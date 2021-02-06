use std::fs::File;
use std::sync::Arc;

use arrow::record_batch::RecordBatchReader;
use parquet::arrow::{ArrowReader, ParquetFileArrowReader};
use parquet::file::reader::SerializedFileReader;

use crate::error::ColumnQError;
use crate::table::TableSource;

pub fn to_mem_table(t: &TableSource) -> Result<datafusion::datasource::MemTable, ColumnQError> {
    let batch_size = 1024;

    let file = File::open(&t.uri).map_err(ColumnQError::open_parquet_file)?;
    let file_reader = SerializedFileReader::new(file).map_err(ColumnQError::parquet_file_reader)?;
    let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(file_reader));

    let record_batch_reader = arrow_reader
        .get_record_reader(batch_size)
        .map_err(ColumnQError::parquet_record_reader)?;
    let schema_ref = record_batch_reader.schema();

    let partition = record_batch_reader
        .into_iter()
        .collect::<arrow::error::Result<Vec<arrow::record_batch::RecordBatch>>>()?;

    Ok(datafusion::datasource::MemTable::try_new(
        schema_ref,
        vec![partition],
    )?)
}
