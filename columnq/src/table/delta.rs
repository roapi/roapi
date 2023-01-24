use std::convert::{TryFrom, TryInto};
use std::io::Read;
use std::sync::Arc;

use datafusion::arrow;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use datafusion::parquet::arrow::arrow_reader::ArrowReaderOptions;
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::error::ColumnQError;
use crate::io;
use crate::table::{TableLoadOption, TableOptionDelta, TableSource};
use deltalake;

pub async fn to_datafusion_table(t: &TableSource) -> Result<Arc<dyn TableProvider>, ColumnQError> {
    let opt = t
        .option
        .clone()
        .unwrap_or_else(|| TableLoadOption::delta(TableOptionDelta::default()));

    let TableOptionDelta { use_memory_table } = opt.as_delta()?;

    let uri_str = t.get_uri_str();
    let delta_table = deltalake::open_table(uri_str).await?;
    let parsed_uri = t.parsed_uri()?;
    let blob_type = io::BlobStoreType::try_from(parsed_uri.scheme())?;
    let batch_size = t.batch_size;

    if *use_memory_table {
        to_mem_table(delta_table, blob_type, batch_size).await
    } else {
        to_delta_table(delta_table, blob_type).await
    }
}

pub async fn to_delta_table(
    delta_table: deltalake::DeltaTable,
    blob_type: io::BlobStoreType,
) -> Result<Arc<dyn TableProvider>, ColumnQError> {
    match blob_type {
        io::BlobStoreType::FileSystem => Ok(Arc::new(delta_table)),
        io::BlobStoreType::S3 | io::BlobStoreType::GCS => Err(ColumnQError::LoadDelta(format!(
                "object_store for delta table currently only supported in conjunction with `to_memory_table` config: {}",
                delta_table.table_uri(),
            ))),
        _ => {
            Err(ColumnQError::InvalidUri(format!(
                "Scheme in table uri not supported for delta table: {}",
                delta_table.table_uri(),
            )))
        }
    }
}

fn read_partition<R: Read>(mut r: R, batch_size: usize) -> Result<Vec<RecordBatch>, ColumnQError> {
    let mut buffer = Vec::new();
    r.read_to_end(&mut buffer).map_err(|_| {
        ColumnQError::LoadDelta("failed to copy parquet data into memory".to_string())
    })?;

    let record_batch_reader = ParquetRecordBatchReaderBuilder::try_new_with_options(
        bytes::Bytes::from(buffer),
        ArrowReaderOptions::new().with_skip_arrow_metadata(true),
    )?
    .with_batch_size(batch_size)
    .build()?;

    Ok(record_batch_reader
        .into_iter()
        .collect::<arrow::error::Result<Vec<RecordBatch>>>()?)
}

pub async fn to_mem_table(
    delta_table: deltalake::DeltaTable,
    blob_type: io::BlobStoreType,
    batch_size: usize,
) -> Result<Arc<dyn TableProvider>, ColumnQError> {
    if delta_table.get_files().is_empty() {
        return Err(ColumnQError::LoadDelta("empty delta table".to_string()));
    }

    let delta_schema = delta_table.get_schema()?;

    let paths = delta_table.get_file_uris().collect::<Vec<String>>();
    let path_iter = paths.iter().map(|s| s.as_ref());

    let partitions: Vec<Vec<RecordBatch>> = match blob_type {
        io::BlobStoreType::FileSystem => io::fs::partitions_from_iterator(
            path_iter,
            |r| -> Result<Vec<RecordBatch>, ColumnQError> {
                read_partition::<std::fs::File>(r, batch_size)
            },
        )?,
        io::BlobStoreType::S3 | io::BlobStoreType::GCS | io::BlobStoreType::Azure => {
            io::object_store::partitions_from_path_iterator(
                path_iter,
                |r| -> Result<Vec<RecordBatch>, ColumnQError> {
                    read_partition::<std::io::Cursor<Vec<u8>>>(r, batch_size)
                },
            )
            .await?
        }
        _ => {
            return Err(ColumnQError::InvalidUri(format!(
                "Scheme in table uri not supported for delta table: {}",
                delta_table.table_uri(),
            )));
        }
    };

    Ok(Arc::new(datafusion::datasource::MemTable::try_new(
        Arc::new(delta_schema.try_into()?),
        partitions,
    )?))
}

#[cfg(test)]
mod tests {

    use super::*;
    use datafusion::datasource::MemTable;
    use datafusion::physical_plan::Statistics;
    use datafusion::prelude::SessionContext;

    use deltalake::DeltaTable;

    use crate::error::ColumnQError;
    use crate::test_util::test_data_path;

    #[tokio::test]
    async fn load_delta_as_memtable() -> Result<(), ColumnQError> {
        let t = to_datafusion_table(
            &TableSource::new("blogs".to_string(), test_data_path("blogs-delta")).with_option(
                TableLoadOption::delta(TableOptionDelta {
                    use_memory_table: true,
                }),
            ),
        )
        .await?;

        let ctx = SessionContext::new();
        validate_statistics(t.scan(&ctx.state(), &None, &[], None).await?.statistics());

        match t.as_any().downcast_ref::<MemTable>() {
            Some(_) => Ok(()),
            None => panic!("must be of type datafusion::datasource::MemTable"),
        }
    }

    #[tokio::test]
    async fn load_delta_as_delta_source() -> Result<(), ColumnQError> {
        let t = to_datafusion_table(
            &TableSource::new("blogs".to_string(), test_data_path("blogs-delta")).with_option(
                TableLoadOption::delta(TableOptionDelta {
                    use_memory_table: false,
                }),
            ),
        )
        .await?;

        match t.as_any().downcast_ref::<DeltaTable>() {
            Some(delta_table) => {
                assert_eq!(delta_table.version(), 0);
                Ok(())
            }
            None => panic!("must be of type deltalake::DeltaTable"),
        }
    }

    fn validate_statistics(stats: Statistics) {
        assert_eq!(stats.num_rows, Some(500));
        let column_stats = stats.column_statistics.unwrap();
        assert_eq!(column_stats[0].null_count, Some(245));
        assert_eq!(column_stats[1].null_count, Some(373));
        assert_eq!(column_stats[2].null_count, Some(237));
    }
}
