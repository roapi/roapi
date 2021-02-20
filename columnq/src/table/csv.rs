use std::convert::TryFrom;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use uriparse::{Scheme, URIReference};

use crate::error::ColumnQError;
use crate::table::TableSource;

pub async fn to_mem_table(
    t: &TableSource,
) -> Result<datafusion::datasource::MemTable, ColumnQError> {
    // TODO: read csv option from config
    let has_header = true;
    let delimiter = b',';
    let batch_size = 1024;
    let projection = None;

    let uri = URIReference::try_from(t.uri.as_str())
        .map_err(|_| ColumnQError::InvalidUri(t.uri.clone()))?;

    let schema_ref: arrow::datatypes::SchemaRef = match &t.schema {
        Some(s) => Arc::new(s.into()),
        None => {
            let schema = with_reader_from_uri!(
                |mut r| {
                    Ok(arrow::csv::reader::infer_schema_from_reader(
                        &mut r, delimiter, None, has_header,
                    )?
                    .0)
                },
                uri
            )?;
            Arc::new(schema)
        }
    };

    let batches: Vec<RecordBatch> = with_reader_from_uri!(
        |r| -> Result<Vec<RecordBatch>, ColumnQError> {
            let csv_reader = arrow::csv::Reader::new(
                r,
                schema_ref.clone(),
                has_header,
                Some(delimiter),
                batch_size,
                None,
                projection.clone(),
            );

            csv_reader
                .into_iter()
                .map(|batch| Ok(batch?))
                .collect::<Result<Vec<RecordBatch>, ColumnQError>>()
        },
        uri
    )?;

    let partitions = vec![batches];
    Ok(datafusion::datasource::MemTable::try_new(
        schema_ref, partitions,
    )?)
}
