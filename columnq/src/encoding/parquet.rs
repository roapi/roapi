use datafusion::arrow;
use datafusion::parquet;
use datafusion::parquet::errors::ParquetError;

pub fn record_batches_to_bytes(
    batches: &[arrow::record_batch::RecordBatch],
) -> Result<Vec<u8>, ParquetError> {
    if batches.is_empty() {
        return Ok(vec![]);
    }

    let schema = batches[0].schema();

    let mut writer = parquet::arrow::ArrowWriter::try_new(vec![], schema, None)?;
    for batch in batches {
        writer.write(batch)?;
    }

    Ok(writer.into_inner().expect("Should not fail"))
}
