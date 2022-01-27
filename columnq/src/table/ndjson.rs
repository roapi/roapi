use std::io::{BufReader, Read};
use std::sync::Arc;

use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::json::reader::{infer_json_schema, Decoder, ValueIter};
use datafusion::arrow::record_batch::RecordBatch;

use crate::error::ColumnQError;
use crate::table::TableSource;

fn json_schema_from_reader<R: Read>(r: R) -> Result<Schema, ColumnQError> {
    let mut reader = BufReader::new(r);
    Ok(infer_json_schema(&mut reader, None)?)
}

fn decode_json_from_reader<R: Read>(
    r: R,
    schema_ref: SchemaRef,
    batch_size: usize,
) -> Result<Vec<RecordBatch>, ColumnQError> {
    let decoder = Decoder::new(schema_ref, batch_size, None);
    let mut reader = BufReader::new(r);
    let mut value_reader = ValueIter::new(&mut reader, None);
    let mut batches = vec![];
    while let Some(batch) = decoder.next_batch(&mut value_reader)? {
        batches.push(batch);
    }
    Ok(batches)
}

pub async fn to_mem_table(
    t: &TableSource,
) -> Result<datafusion::datasource::MemTable, ColumnQError> {
    let batch_size = t.batch_size;

    let schema_ref: SchemaRef = match &t.schema {
        Some(table_schema) => Arc::new(table_schema.into()),
        // infer schema from data if not provided by user
        None => {
            let inferred_schema: Vec<Schema> =
                partitions_from_table_source!(t, json_schema_from_reader)?;
            if inferred_schema.is_empty() {
                return Err(ColumnQError::LoadJson("failed to load schema".to_string()));
            }
            Arc::new(Schema::try_merge(inferred_schema)?)
        }
    };

    let partitions: Vec<Vec<RecordBatch>> =
        partitions_from_table_source!(t, |reader| decode_json_from_reader(
            reader,
            schema_ref.clone(),
            batch_size
        ))?;

    Ok(datafusion::datasource::MemTable::try_new(
        schema_ref, partitions,
    )?)
}
