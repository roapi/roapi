use std::io::BufReader;
use std::sync::Arc;

use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::json::reader::{infer_json_schema, Decoder, ValueIter};
use datafusion::arrow::record_batch::RecordBatch;

use crate::error::ColumnQError;
use crate::table::TableSource;

pub async fn to_mem_table(
    t: &TableSource,
) -> Result<datafusion::datasource::MemTable, ColumnQError> {
    let batch_size = t.batch_size;

    let schema_ref: SchemaRef = match &t.schema {
        Some(table_schema) => Arc::new(table_schema.into()),
        // infer schema from data if not provided by user
        None => {
            let inferred_schema: Vec<Schema> =
                partitions_from_table_source!(t, |reader| -> Result<Schema, ColumnQError> {
                    let mut reader = BufReader::new(reader);
                    Ok(infer_json_schema(&mut reader, None)?)
                })?;
            if inferred_schema.is_empty() {
                return Err(ColumnQError::LoadJson("failed to load schema".to_string()));
            }
            Arc::new(Schema::try_merge(inferred_schema)?)
        }
    };

    let decoder = Decoder::new(schema_ref.clone(), batch_size, None);

    let partitions: Vec<Vec<RecordBatch>> =
        partitions_from_table_source!(t, |reader| -> Result<Vec<RecordBatch>, ColumnQError> {
            let mut reader = BufReader::new(reader);
            let mut value_reader = ValueIter::new(&mut reader, None);
            let mut batches = vec![];
            while let Some(batch) = decoder.next_batch(&mut value_reader)? {
                batches.push(batch);
            }
            Ok(batches)
        })?;

    Ok(datafusion::datasource::MemTable::try_new(
        schema_ref, partitions,
    )?)
}
