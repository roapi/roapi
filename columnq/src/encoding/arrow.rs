use datafusion::arrow;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::ipc::writer::{FileWriter, StreamWriter};

// streaming format spec:
// https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format

pub fn record_batches_to_stream_bytes(
    batches: &[arrow::record_batch::RecordBatch],
) -> Result<Vec<u8>, ArrowError> {
    let mut buf = Vec::new();

    // TODO: write out schema regardless even for empty record batch?
    // see: https://issues.apache.org/jira/browse/ARROW-2119
    if !batches.is_empty() {
        let schema = batches[0].schema();
        let mut writer = StreamWriter::try_new(&mut buf, &schema)?;
        for batch in batches {
            writer.write(batch)?;
        }
    }

    Ok(buf)
}

pub fn record_batches_to_file_bytes(
    batches: &[arrow::record_batch::RecordBatch],
) -> Result<Vec<u8>, ArrowError> {
    let mut buf = Vec::new();

    // TODO: write out schema regardless even for empty record batch?
    // see: https://issues.apache.org/jira/browse/ARROW-2119
    if !batches.is_empty() {
        let schema = batches[0].schema();
        let mut writer = FileWriter::try_new(&mut buf, &schema)?;
        for batch in batches {
            writer.write(batch)?;
        }
        writer.finish()?;
    }

    Ok(buf)
}
