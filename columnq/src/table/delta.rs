use std::convert::{TryFrom, TryInto};
use std::io::Read;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use parquet::arrow::{ArrowReader, ParquetFileArrowReader};
use parquet::file::reader::SerializedFileReader;
use parquet::file::serialized_reader::SliceableCursor;

use crate::error::ColumnQError;
use crate::io;
use crate::table::TableSource;

fn read_partition<R: Read>(mut r: R, batch_size: usize) -> Result<Vec<RecordBatch>, ColumnQError> {
    let mut buffer = Vec::new();
    r.read_to_end(&mut buffer).map_err(|_| {
        ColumnQError::LoadDelta("failed to copy parquet data into memory".to_string())
    })?;

    let file_reader = SerializedFileReader::new(SliceableCursor::new(buffer))
        .map_err(ColumnQError::parquet_file_reader)?;
    let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(file_reader));

    let record_batch_reader = arrow_reader
        .get_record_reader(batch_size)
        .map_err(ColumnQError::parquet_record_reader)?;

    Ok(record_batch_reader
        .into_iter()
        .collect::<arrow::error::Result<Vec<RecordBatch>>>()?)
}

pub async fn to_mem_table(
    t: &TableSource,
) -> Result<datafusion::datasource::MemTable, ColumnQError> {
    // TODO: make batch size configurable
    let batch_size = 1024;

    let delta_table = deltalake::open_table(&t.uri).await?;

    if delta_table.get_files().is_empty() {
        return Err(ColumnQError::LoadDelta("empty delta table".to_string()));
    }

    let delta_schema = delta_table.get_schema()?;
    let uri = t.parsed_uri()?;
    let blob_type = io::BlobStoreType::try_from(uri.scheme())?;

    let paths = delta_table.get_file_paths();
    let path_iter = paths.iter().map(|s| s.as_str());

    let partitions: Vec<Vec<RecordBatch>> = match blob_type {
        io::BlobStoreType::FileSystem => io::fs::partitions_from_iterator(
            path_iter,
            |r| -> Result<Vec<RecordBatch>, ColumnQError> {
                read_partition::<std::fs::File>(r, batch_size)
            },
        )?,
        io::BlobStoreType::S3 => {
            io::s3::partitions_from_path_iterator(
                path_iter,
                |r| -> Result<Vec<RecordBatch>, ColumnQError> {
                    read_partition::<std::io::Cursor<Vec<u8>>>(r, batch_size)
                },
            )
            .await?
        }
        _ => {
            return Err(ColumnQError::InvalidUri(format!(
                "Scheme in table uri not supported for delta table: {:?}",
                t.uri
            )));
        }
    };

    Ok(datafusion::datasource::MemTable::try_new(
        Arc::new(delta_schema.try_into()?),
        partitions,
    )?)
}
