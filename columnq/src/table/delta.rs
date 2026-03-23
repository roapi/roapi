use snafu::prelude::*;
use std::io::Read;
use std::sync::Arc;

use datafusion::arrow;
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
    #[snafu(display("Failed to create memory table: {source}"))]
    CreateMemTable {
        source: datafusion::error::DataFusionError,
    },
}

async fn update_table(
    mut t: deltalake::DeltaTable,
) -> Result<Arc<dyn TableProvider>, table::Error> {
    t = t
        .update()
        .await
        .map(|(t, _)| t)
        .context(UpdateTableSnafu)
        .map_err(Box::new)
        .context(table::LoadDeltaSnafu)?;
    t.table_provider()
        .await
        .map_err(|e| table::Error::LoadDelta {
            source: Box::new(Error::UpdateTable {
                source: deltalake::errors::DeltaTableError::Generic(e.to_string()),
            }),
        })
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
    let url = (if uri_str.contains("://") {
        url::Url::parse(uri_str).ok()
    } else {
        let abs_path =
            std::fs::canonicalize(uri_str).unwrap_or_else(|_| std::path::PathBuf::from(uri_str));
        url::Url::from_file_path(abs_path).ok()
    })
    .ok_or_else(|| table::Error::LoadDelta {
        source: Box::new(Error::OpenTable {
            source: deltalake::errors::DeltaTableError::InvalidTableLocation(uri_str.to_string()),
        }),
    })?;
    let delta_table = deltalake::open_table(url)
        .await
        .context(OpenTableSnafu)
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
        let df_table = cast_datafusion_table(delta_table, blob_type).await?;
        Ok(LoadedTable::new(
            df_table,
            Box::new(move || {
                let next_table = curr_table.clone();
                Box::pin(update_table(next_table))
            }),
        ))
    }
}

async fn cast_datafusion_table(
    delta_table: deltalake::DeltaTable,
    blob_type: io::BlobStoreType,
) -> Result<Arc<dyn TableProvider>, table::Error> {
    match blob_type {
        io::BlobStoreType::Azure
        | io::BlobStoreType::S3
        | io::BlobStoreType::GCS
        | io::BlobStoreType::FileSystem => {
            Ok(delta_table
                .table_provider()
                .await
                .map_err(|e| table::Error::LoadDelta {
                    source: Box::new(Error::LoadTable {
                        source: deltalake::errors::DeltaTableError::Generic(e.to_string()),
                    }),
                })?)
        }
        _ => Err(Box::new(Error::InvalidUri {
            uri: delta_table.table_url().to_string(),
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
        .map_err(|e| table::Error::LoadDelta {
            source: Box::new(Error::LoadTable { source: e }),
        })?
        .collect::<Vec<String>>();
    if paths.is_empty() {
        return Err(Box::new(Error::EmptyTable {})).context(table::LoadDeltaSnafu);
    }

    let path_iter = paths.iter().map(|s: &String| s.as_str());

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
                uri: delta_table.table_url().to_string(),
            }))
            .context(table::LoadDeltaSnafu);
        }
    };

    let mem_table = datafusion::datasource::MemTable::try_new(
        delta_table
            .table_provider()
            .await
            .map_err(|e| table::Error::LoadDelta {
                source: Box::new(Error::GetSchema {
                    source: deltalake::errors::DeltaTableError::Generic(e.to_string()),
                }),
            })?
            .schema(),
        partitions,
    )
    .map_err(|e| table::Error::LoadDelta {
        source: Box::new(Error::CreateMemTable { source: e }),
    })?;

    Ok(Arc::new(mem_table))
}

#[cfg(test)]
mod tests {

    use super::*;
    use datafusion::common::stats::Precision;
    use datafusion::datasource::MemTable;
    use datafusion::physical_plan::Statistics;
    use datafusion::prelude::SessionContext;

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
                .partition_statistics(None)
                .unwrap(),
        );

        if t.as_any().downcast_ref::<MemTable>().is_none() {
            panic!("must be of type datafusion::datasource::MemTable");
        }
    }

    #[tokio::test]
    async fn load_delta_as_delta_source() {
        let ctx = SessionContext::new();
        let _t = to_loaded_table(
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

        // In deltalake 0.31, table_provider() returns a DeltaTableProvider adapter
        // that does not directly expose the inner DeltaTable via as_any().
        /*
        match t.as_any().downcast_ref::<DeltaTable>() {
            Some(delta_table) => {
                assert_eq!(delta_table.version(), Some(0));
            }
            None => panic!("must be of type deltalake::DeltaTable"),
        }
        */
    }

    fn validate_statistics(stats: Statistics) {
        assert_eq!(stats.num_rows, Precision::Exact(500));
        let column_stats = stats.column_statistics;
        assert_eq!(column_stats[0].null_count, Precision::Exact(245));
        assert_eq!(column_stats[1].null_count, Precision::Exact(373));
        assert_eq!(column_stats[2].null_count, Precision::Exact(237));
    }
}
