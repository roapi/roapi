use datafusion::arrow;
use datafusion::parquet;
use datafusion::parquet::errors::ParquetError;

pub fn record_batches_to_bytes(
    batches: &[arrow::record_batch::RecordBatch],
) -> Result<Vec<u8>, ParquetError> {
    let cursor = parquet::file::writer::InMemoryWriteableCursor::default();
    {
        if !batches.is_empty() {
            let schema = batches[0].schema();

            let mut writer = parquet::arrow::ArrowWriter::try_new(cursor.clone(), schema, None)?;
            for batch in batches {
                writer.write(batch)?;
            }
            writer.close()?;
        }
    }

    let result = cursor.into_inner().expect("Should not fail");
    Ok(result)
}
