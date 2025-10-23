use snafu::prelude::*;
use std::collections::{HashMap, HashSet};
use std::io::Read;
use std::sync::Arc;

use datafusion::arrow;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use datafusion::parquet::arrow::arrow_reader::ArrowReaderOptions;
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::io::{self, BlobStoreType};
use crate::table::{self, LoadedTable, TableLoadOption, TableOptionDelta, TableSource};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to read data into memory: {source}"))]
    ReadBytes { source: std::io::Error },
    #[snafu(display("Failed to create parquet reader builder: {source}"))]
    NewReaderBuilder {
        source: datafusion::parquet::errors::ParquetError,
    },
    #[snafu(display("Failed to build parquet reader: {source}"))]
    BuildReader {
        source: datafusion::parquet::errors::ParquetError,
    },
    #[snafu(display("Failed to collect record batch: {source}"))]
    CollectRecordBatch {
        source: datafusion::arrow::error::ArrowError,
    },
    #[snafu(display("Scheme in table uri not supported for delta table: {uri}"))]
    InvalidUri { uri: String },
    #[snafu(display("Empty Delta table"))]
    EmptyTable {},
    #[snafu(display("Failed to load table schema: {source}"))]
    GetSchema {
        source: deltalake::errors::DeltaTableError,
    },
    #[snafu(display("Failed to convert Delta schema to Arrow schema: {source}"))]
    ConvertSchema {
        source: datafusion::arrow::error::ArrowError,
    },
    #[snafu(display("Failed to open table: {source}"))]
    OpenTable {
        source: deltalake::errors::DeltaTableError,
    },
    #[snafu(display("Failed to load table: {source}"))]
    LoadTable {
        source: deltalake::errors::DeltaTableError,
    },
    #[snafu(display("Failed to update table: {source}"))]
    UpdateTable {
        source: deltalake::errors::DeltaTableError,
    },
}

fn extract_partition_from_path(path: &str) -> HashMap<String, String> {
    let mut partitions = HashMap::new();
    for segment in path.split('/') {
        if let Some((k, v)) = segment.split_once('=') {
            partitions.insert(k.to_string(), v.to_string());
        }
    }
    partitions
}

async fn update_table(
    mut t: deltalake::DeltaTable,
) -> Result<Arc<dyn TableProvider>, table::Error> {
    t
        // TODO: find a way to not do a full table update?
        .update()
        .await
        .context(UpdateTableSnafu)
        .map_err(Box::new)
        .context(table::LoadDeltaSnafu)?;
    Ok(Arc::new(t) as Arc<dyn TableProvider>)
}

pub async fn to_loaded_table(
    t: &TableSource,
    dfctx: &datafusion::execution::context::SessionContext,
) -> Result<LoadedTable, table::Error> {
    let opt = t
        .option
        .clone()
        .unwrap_or_else(|| TableLoadOption::delta(TableOptionDelta::default()));

    let TableOptionDelta { use_memory_table } = opt.as_delta()?;

    let uri_str = t.get_uri_str();
    let delta_table = deltalake::DeltaTableBuilder::from_valid_uri(uri_str)
        .context(OpenTableSnafu)
        .map_err(Box::new)
        .context(table::LoadDeltaSnafu)?
        .with_allow_http(true)
        .load()
        .await
        .context(LoadTableSnafu)
        .map_err(Box::new)
        .context(table::LoadDeltaSnafu)?;
    let parsed_uri = t.parsed_uri()?;
    let url_scheme = parsed_uri.scheme();
    let blob_type = BlobStoreType::try_from(url_scheme).context(table::IoSnafu)?;
    let batch_size = t.batch_size;

    if *use_memory_table {
        let dfctx = dfctx.clone();
        let to_datafusion_table = move || {
            to_mem_table(
                delta_table.clone(),
                blob_type.clone(),
                batch_size,
                dfctx.clone(),
            )
        };
        LoadedTable::new_from_df_table_cb(to_datafusion_table).await
    } else {
        let curr_table = delta_table.clone();
        let df_table = cast_datafusion_table(delta_table, blob_type)?;
        Ok(LoadedTable::new(
            df_table,
            Box::new(move || {
                let next_table = curr_table.clone();
                Box::pin(update_table(next_table))
            }),
        ))
    }
}

fn cast_datafusion_table(
    delta_table: deltalake::DeltaTable,
    blob_type: io::BlobStoreType,
) -> Result<Arc<dyn TableProvider>, table::Error> {
    match blob_type {
        io::BlobStoreType::Azure
        | io::BlobStoreType::S3
        | io::BlobStoreType::GCS
        | io::BlobStoreType::FileSystem => Ok(Arc::new(delta_table)),
        _ => Err(Box::new(Error::InvalidUri {
            uri: delta_table.table_uri().to_string(),
        }))
        .context(table::LoadDeltaSnafu),
    }
}

fn read_partition<R: Read>(mut r: R, batch_size: usize) -> Result<Vec<RecordBatch>, table::Error> {
    let mut buffer = Vec::new();
    r.read_to_end(&mut buffer)
        .context(ReadBytesSnafu)
        .map_err(Box::new)
        .context(table::LoadDeltaSnafu)?;

    let record_batch_reader = ParquetRecordBatchReaderBuilder::try_new_with_options(
        bytes::Bytes::from(buffer),
        ArrowReaderOptions::new().with_skip_arrow_metadata(true),
    )
    .context(NewReaderBuilderSnafu)
    .map_err(|e| table::Error::LoadDelta {
        source: Box::new(e),
    })?
    .with_batch_size(batch_size)
    .build()
    .context(BuildReaderSnafu)
    .map_err(Box::new)
    .context(table::LoadDeltaSnafu)?;

    record_batch_reader
        .into_iter()
        .collect::<arrow::error::Result<Vec<RecordBatch>>>()
        .context(CollectRecordBatchSnafu)
        .map_err(Box::new)
        .context(table::LoadDeltaSnafu)
}

pub async fn to_mem_table(
    delta_table: deltalake::DeltaTable,
    blob_type: io::BlobStoreType,
    batch_size: usize,
    dfctx: datafusion::execution::context::SessionContext,
) -> Result<Arc<dyn TableProvider>, table::Error> {
    let paths = delta_table
        .get_file_uris()
        .context(LoadTableSnafu)
        .map_err(Box::new)
        .context(table::LoadDeltaSnafu)?
        .collect::<Vec<String>>();
    if paths.is_empty() {
        return Err(Box::new(Error::EmptyTable {})).context(table::LoadDeltaSnafu);
    }

    let delta_schema = delta_table
        .get_schema()
        .context(GetSchemaSnafu)
        .map_err(Box::new)
        .context(table::LoadDeltaSnafu)?;

    let mut partition_columns = HashSet::new();
    for path in &paths {
        let partitions = extract_partition_from_path(path);
        for key in partitions.keys() {
            partition_columns.insert(key.clone());
        }
    }

    let arrow_schema: Schema = delta_schema
        .try_into()
        .context(ConvertSchemaSnafu)
        .map_err(Box::new)
        .context(table::LoadDeltaSnafu)?;

    let table_schema = Arc::new(arrow_schema);

    let path_iter = paths.iter().map(|s| s.as_ref());

    let partitions: Vec<Vec<RecordBatch>> = match blob_type {
        io::BlobStoreType::FileSystem => io::fs::partitions_from_iterator(
            path_iter,
            |r| -> Result<Vec<RecordBatch>, table::Error> {
                read_partition::<std::fs::File>(r, batch_size)
            },
        )
        .context(table::IoSnafu)?,
        io::BlobStoreType::S3 | io::BlobStoreType::GCS | io::BlobStoreType::Azure => {
            io::object_store::partitions_from_path_iterator(
                path_iter,
                |r| -> Result<Vec<RecordBatch>, table::Error> {
                    read_partition::<std::io::Cursor<Vec<u8>>>(r, batch_size)
                },
                &dfctx,
            )
            .await
            .context(table::IoSnafu)?
        }
        _ => {
            return Err(Box::new(Error::InvalidUri {
                uri: delta_table.table_uri().to_string(),
            }))
            .context(table::LoadDeltaSnafu);
        }
    };

    let mut updated_partitions = Vec::new();
    for (path, batches) in paths.iter().zip(partitions.into_iter()) {
        let partition_values = extract_partition_from_path(path);
        let mut updated_batches = Vec::new();
        for batch in batches {
            let mut new_columns = Vec::new();
            for partition_key in table_schema.fields().iter().map(|f| f.name()) {
                if partition_columns.contains(partition_key) {
                    let value = partition_values
                        .get(partition_key)
                        .map_or("", |v| v.as_str());
                    let num_rows = batch.num_rows();

                    let field = table_schema.field_with_name(partition_key).map_err(|_| {
                        table::Error::Generic {
                            msg: (format!(
                                "Partition key '{}' not found in final schema",
                                partition_key
                            )),
                        }
                    })?;
                    let array: Arc<dyn arrow::array::Array> = match field.data_type() {
                        arrow::datatypes::DataType::Date32 => {
                            let parsed = chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d")
                                .ok()
                                .map(|d| {
                                    (d - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                                        .num_days() as i32
                                })
                                .unwrap_or(0);
                            Arc::new(arrow::array::Date32Array::from(vec![parsed; num_rows])) as _
                        }

                        arrow::datatypes::DataType::Int32 => {
                            let parsed = value.parse::<i32>().unwrap_or_default();
                            Arc::new(arrow::array::Int32Array::from(vec![parsed; num_rows])) as _
                        }

                        arrow::datatypes::DataType::Int64 => {
                            let parsed = value.parse::<i64>().unwrap_or_default();
                            Arc::new(arrow::array::Int64Array::from(vec![parsed; num_rows])) as _
                        }

                        arrow::datatypes::DataType::Boolean => {
                            let parsed =
                                matches!(value.to_lowercase().as_str(), "true" | "1" | "yes");
                            Arc::new(arrow::array::BooleanArray::from(vec![parsed; num_rows])) as _
                        }

                        _ => {
                            // Default to Utf8
                            Arc::new(arrow::array::StringArray::from(vec![value; num_rows])) as _
                        }
                    };

                    new_columns.push(array);
                } else if let Some(column) = batch.column_by_name(partition_key) {
                    new_columns.push(column.clone());
                } else {
                    return Err(Box::new(Error::CollectRecordBatch {
                        source: arrow::error::ArrowError::SchemaError(format!(
                            "Column '{}' not found in batch schema",
                            partition_key
                        )),
                    }))
                    .context(table::LoadDeltaSnafu);
                }
            }
            let new_batch = RecordBatch::try_new(table_schema.clone(), new_columns)
                .context(CollectRecordBatchSnafu)
                .map_err(Box::new)
                .context(table::LoadDeltaSnafu)?;
            updated_batches.push(new_batch);
        }
        updated_partitions.push(updated_batches);
    }
    Ok(Arc::new(
        datafusion::datasource::MemTable::try_new(table_schema, updated_partitions)
            .map_err(Box::new)
            .context(table::CreateMemTableSnafu)?,
    ))
}

#[cfg(test)]
mod tests {

    use super::*;
    use datafusion::common::stats::Precision;
    use datafusion::datasource::MemTable;
    use datafusion::physical_plan::Statistics;
    use datafusion::prelude::SessionContext;

    use deltalake::DeltaTable;

    use crate::test_util::test_data_path;

    #[tokio::test]
    async fn load_delta_as_memtable() {
        let ctx = SessionContext::new();
        let t = to_loaded_table(
            &TableSource::new("blogs".to_string(), test_data_path("blogs-delta")).with_option(
                TableLoadOption::delta(TableOptionDelta {
                    use_memory_table: true,
                }),
            ),
            &ctx,
        )
        .await
        .unwrap()
        .table;

        validate_statistics(
            t.scan(&ctx.state(), None, &[], None)
                .await
                .unwrap()
                .statistics()
                .unwrap(),
        );

        if t.as_any().downcast_ref::<MemTable>().is_none() {
            panic!("must be of type datafusion::datasource::MemTable");
        }
    }

    #[tokio::test]
    async fn load_delta_as_delta_source() {
        let ctx = SessionContext::new();
        let t = to_loaded_table(
            &TableSource::new("blogs".to_string(), test_data_path("blogs-delta")).with_option(
                TableLoadOption::delta(TableOptionDelta {
                    use_memory_table: false,
                }),
            ),
            &ctx,
        )
        .await
        .unwrap()
        .table;

        match t.as_any().downcast_ref::<DeltaTable>() {
            Some(delta_table) => {
                assert_eq!(delta_table.version(), 0);
            }
            None => panic!("must be of type deltalake::DeltaTable"),
        }
    }

    fn validate_statistics(stats: Statistics) {
        assert_eq!(stats.num_rows, Precision::Exact(500));
        let column_stats = stats.column_statistics;
        assert_eq!(column_stats[0].null_count, Precision::Exact(245));
        assert_eq!(column_stats[1].null_count, Precision::Exact(373));
        assert_eq!(column_stats[2].null_count, Precision::Exact(237));
    }
}
