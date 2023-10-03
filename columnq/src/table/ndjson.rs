use std::io::{BufReader, Read};
use std::sync::Arc;

use datafusion::arrow::datatypes::{Schema, SchemaRef};
#[allow(deprecated)]
use datafusion::arrow::json::reader::{infer_json_schema, ReaderBuilder};
use datafusion::arrow::record_batch::RecordBatch;
use snafu::prelude::*;

use crate::table::{self, TableSource};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to infer NDJSON schema: {source}"))]
    InferSchema {
        source: datafusion::arrow::error::ArrowError,
    },
    #[snafu(display("Found empty NDJSON schema"))]
    EmptySchema {},
    #[snafu(display("Failed to build arrow reader: {source}"))]
    BuildReader {
        source: datafusion::arrow::error::ArrowError,
    },
    #[snafu(display("Failed to collect arrow record batches: {source}"))]
    CollectBatches {
        source: datafusion::arrow::error::ArrowError,
    },
}

fn json_schema_from_reader<R: Read>(r: R) -> Result<Schema, table::Error> {
    let mut reader = BufReader::new(r);
    infer_json_schema(&mut reader, None)
        .context(InferSchemaSnafu)
        .context(table::LoadNdJsonSnafu)
}

fn decode_json_from_reader<R: Read>(
    r: R,
    schema_ref: SchemaRef,
    batch_size: usize,
) -> Result<Vec<RecordBatch>, table::Error> {
    let batch_reader = ReaderBuilder::new(schema_ref)
        .with_batch_size(batch_size)
        .build(BufReader::new(r))
        .context(BuildReaderSnafu)
        .context(table::LoadNdJsonSnafu)?;

    let batches = batch_reader
        .collect::<Result<Vec<RecordBatch>, _>>()
        .context(CollectBatchesSnafu)
        .context(table::LoadNdJsonSnafu)?;

    Ok(batches)
}

pub async fn to_mem_table(
    t: &TableSource,
    dfctx: &datafusion::execution::context::SessionContext,
) -> Result<datafusion::datasource::MemTable, table::Error> {
    let batch_size = t.batch_size;

    let schema_ref: SchemaRef = match &t.schema {
        Some(table_schema) => Arc::new(table_schema.into()),
        // infer schema from data if not provided by user
        None => {
            let inferred_schema: Vec<Schema> =
                partitions_from_table_source!(t, json_schema_from_reader, dfctx)
                    .context(table::IoSnafu)?;
            if inferred_schema.is_empty() {
                return Err(Error::EmptySchema {}).context(table::LoadNdJsonSnafu);
            }
            Arc::new(
                Schema::try_merge(inferred_schema)
                    .context(InferSchemaSnafu)
                    .context(table::LoadNdJsonSnafu)?,
            )
        }
    };

    let partitions: Vec<Vec<RecordBatch>> = partitions_from_table_source!(
        t,
        |reader| decode_json_from_reader(reader, schema_ref.clone(), batch_size),
        dfctx
    )
    .context(table::IoSnafu)?;

    datafusion::datasource::MemTable::try_new(schema_ref, partitions)
        .context(table::CreateMemTableSnafu)
}

#[cfg(test)]
mod tests {
    use datafusion::{datasource::TableProvider, prelude::SessionContext};

    use super::*;
    use crate::{table::TableSource, test_util::test_data_path};

    #[tokio::test]
    async fn load_simple_ndjson_file() {
        let ctx = SessionContext::new();
        let t = to_mem_table(
            &TableSource::new(
                "spacex_launches".to_string(),
                test_data_path("spacex_launches.ndjson"),
            ),
            &ctx,
        )
        .await
        .unwrap();

        let schema = t.schema();
        let fields = schema.fields();

        let mut obj_keys = fields.iter().map(|f| f.name()).collect::<Vec<_>>();
        obj_keys.sort();
        let mut expected_obj_keys = vec![
            "fairings",
            "links",
            "static_fire_date_utc",
            "static_fire_date_unix",
            "tbd",
            "net",
            "window",
            "rocket",
            "success",
            "details",
            "crew",
            "ships",
            "capsules",
            "payloads",
            "launchpad",
            "auto_update",
            "failures",
            "flight_number",
            "name",
            "date_unix",
            "date_utc",
            "date_local",
            "date_precision",
            "upcoming",
            "cores",
            "id",
            "launch_library_id",
        ];
        expected_obj_keys.sort();

        assert_eq!(obj_keys, expected_obj_keys);
    }

    #[tokio::test]
    async fn load_simple_jsonl_file() {
        let ctx = SessionContext::new();
        let t = to_mem_table(
            &TableSource::new(
                "spacex_launches".to_string(),
                test_data_path("spacex_launches.jsonl"),
            ),
            &ctx,
        )
        .await
        .unwrap();

        let schema = t.schema();
        let fields = schema.fields();

        let mut obj_keys = fields.iter().map(|f| f.name()).collect::<Vec<_>>();
        obj_keys.sort();
        let mut expected_obj_keys = vec![
            "fairings",
            "links",
            "static_fire_date_utc",
            "static_fire_date_unix",
            "tbd",
            "net",
            "window",
            "rocket",
            "success",
            "details",
            "crew",
            "ships",
            "capsules",
            "payloads",
            "launchpad",
            "auto_update",
            "failures",
            "flight_number",
            "name",
            "date_unix",
            "date_utc",
            "date_local",
            "date_precision",
            "upcoming",
            "cores",
            "id",
            "launch_library_id",
        ];
        expected_obj_keys.sort();

        assert_eq!(obj_keys, expected_obj_keys);
    }
}
