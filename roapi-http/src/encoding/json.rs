pub fn record_batches_to_bytes(
    batches: &[arrow::record_batch::RecordBatch],
) -> Result<Vec<u8>, serde_json::Error> {
    let json_rows = arrow::json::writer::record_batches_to_json_rows(&batches);
    serde_json::to_vec(&json_rows)
}
