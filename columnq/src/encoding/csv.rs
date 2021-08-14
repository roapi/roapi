use datafusion::arrow;
use datafusion::arrow::error::ArrowError;

pub fn record_batches_to_bytes(
    batches: &[arrow::record_batch::RecordBatch],
) -> Result<Vec<u8>, ArrowError> {
    let mut cursor = std::io::Cursor::new(Vec::new());
    {
        let mut writer = arrow::csv::Writer::new(&mut cursor);
        for batch in batches {
            writer.write(batch)?;
        }
    }

    Ok(cursor.into_inner().to_vec())
}
