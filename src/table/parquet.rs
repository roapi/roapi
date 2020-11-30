use std::fs::File;
use std::sync::Arc;

use arrow::record_batch::RecordBatchReader;
use parquet::arrow::{ArrowReader, ParquetFileArrowReader};
use parquet::file::reader::SerializedFileReader;

use super::TableSource;

pub fn to_mem_table(t: &TableSource) -> anyhow::Result<datafusion::datasource::MemTable> {
    let batch_size = 1024;

    let file = File::open(&t.uri)?;
    let file_reader = SerializedFileReader::new(file)?;
    let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(file_reader));

    let record_batch_reader = arrow_reader.get_record_reader(batch_size)?;
    let schema_ref = record_batch_reader.schema();

    let partition = record_batch_reader
        .into_iter()
        .collect::<arrow::error::Result<Vec<arrow::record_batch::RecordBatch>>>()?;

    Ok(datafusion::datasource::MemTable::try_new(
        schema_ref,
        vec![partition],
    )?)
}
