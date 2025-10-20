use std::io::{BufReader, Read};
use std::sync::Arc;

use datafusion::arrow;
use datafusion::arrow::datatypes::Schema;
#[allow(deprecated)]
use datafusion::arrow::json::reader::ReaderBuilder;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use serde_json::value::Value;
use snafu::prelude::*;

use crate::table::{self, LoadedTable, TableLoadOption, TableSchema, TableSource};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to deserialize JSON: {source}"))]
    Deserialize { source: serde_json::Error },
    #[snafu(display("Array encoded option requires manually specified schema"))]
    ArrayEncodedSchemaRequired {},
    #[snafu(display("JSON data is an empty array"))]
    EmptyArray {},
    #[snafu(display("{pointer} points to an empty array"))]
    EmptyArrayPointer { pointer: String },
    #[snafu(display("Invalid json pinter: {pointer}"))]
    InvalidPointer { pointer: String },
    #[snafu(display("{stuff} is not an array"))]
    NotAnArray { stuff: String },
    #[snafu(display("Failed to infer schema: {source}"))]
    InferSchema {
        source: datafusion::arrow::error::ArrowError,
    },
    #[snafu(display("Failed to load schema"))]
    SchemaNotFound {},
    #[snafu(display("Failed build decoder: {source}"))]
    BuildDecoder {
        source: datafusion::arrow::error::ArrowError,
    },
    #[snafu(display("Failed to serialize objects: {source}"))]
    Serialize {
        source: datafusion::arrow::error::ArrowError,
    },
    #[snafu(display("No item found in JSON rows"))]
    NoJsonRow {},
    #[snafu(display("Array encoded JSON row missing column {index:?}: {row:?}"))]
    ArrayEncodingMissingRow { index: usize, row: Value },
}

fn json_value_from_reader<R: Read>(r: R) -> Result<Value, table::Error> {
    let reader = BufReader::new(r);
    serde_json::from_reader(reader)
        .context(DeserializeSnafu)
        .map_err(Box::new)
        .context(table::LoadJsonSnafu)
}

fn json_partition_to_vec(
    json_partition: &Value,
    pointer: Option<&str>,
) -> Result<Vec<Value>, Error> {
    let mut value_ref = json_partition;

    if let Some(p) = pointer {
        match value_ref.pointer(p) {
            Some(v) => value_ref = v,
            None => {
                return Err(Error::InvalidPointer {
                    pointer: p.to_string(),
                })
            }
        }
    }

    match value_ref.as_array() {
        Some(arr) => Ok(arr.to_vec()),
        None => Err(Error::NotAnArray {
            stuff: pointer.unwrap_or("JSON data").to_string(),
        }),
    }
}

fn json_vec_to_partition(
    json_rows: Vec<Value>,
    provided_schema: &Option<TableSchema>,
    batch_size: usize,
    array_encoded: bool,
) -> Result<(arrow::datatypes::Schema, Vec<RecordBatch>), Error> {
    // load schema
    let schema = match provided_schema {
        Some(s) => s.into(),
        None => arrow::json::reader::infer_json_schema_from_iterator(
            json_rows.iter().map(|v| Ok(v.clone())),
        )
        .context(InferSchemaSnafu)?,
    };

    // TODO: batch_size setting here doesn't work because we are invoking serialize directly. might
    // be better to break up the batch ourselives.
    let mut decoder = ReaderBuilder::new(Arc::new(schema.clone()))
        .with_batch_size(batch_size)
        .build_decoder()
        .context(BuildDecoderSnafu)?;

    if array_encoded {
        // convert row array to object based on schema
        // TODO: support array_encoded read in upstream arrow json reader instead
        let objects = json_rows
            .into_iter()
            .map(|json_row| {
                let mut m = serde_json::map::Map::new();
                schema.fields().iter().enumerate().try_for_each(|(i, f)| {
                    match json_row.get(i) {
                        Some(x) => {
                            m.insert(f.name().to_string(), x.clone());
                            Ok(())
                        }
                        None => Err(Error::ArrayEncodingMissingRow {
                            index: i,
                            row: json_row.clone(),
                        }),
                    }
                })?;
                Ok(Value::Object(m))
            })
            .collect::<Result<Vec<Value>, Error>>()?;

        // TODO: avoid unnecessary collection here, update upstream json reader to take an iterator
        // instead of slice
        decoder.serialize(&objects).context(SerializeSnafu)?;
    } else {
        // Note: serialize ignores any batch size setting, and always decodes all rows
        decoder.serialize(&json_rows).context(SerializeSnafu)?;
    };

    let batch = decoder
        .flush()
        .context(SerializeSnafu)?
        .context(NoJsonRowSnafu)?;

    Ok((schema, vec![batch]))
}

async fn to_partitions(
    t: &TableSource,
    dfctx: &datafusion::execution::context::SessionContext,
) -> Result<(Option<Schema>, Vec<Vec<RecordBatch>>), table::Error> {
    let batch_size = t.batch_size;
    let array_encoded = match &t.option {
        Some(TableLoadOption::json { array_encoded, .. }) => array_encoded.unwrap_or(false),
        _ => false,
    };

    if array_encoded && t.schema.is_none() {
        return Err(Box::new(Error::ArrayEncodedSchemaRequired {})).context(table::LoadJsonSnafu);
    }

    let pointer = match &t.option {
        Some(TableLoadOption::json { pointer, .. }) => pointer.to_owned(),
        _ => None,
    };

    let mut merged_schema: Option<Schema> = None;
    let json_partitions: Vec<Value> =
        partitions_from_table_source!(t, json_value_from_reader, dfctx).context(table::IoSnafu)?;

    let partitions = json_partitions
        .iter()
        .map(|json_partition| {
            let json_rows = json_partition_to_vec(json_partition, pointer.as_deref())
                .map_err(Box::new)
                .context(table::LoadJsonSnafu)?;
            if json_rows.is_empty() {
                match &pointer {
                    Some(p) => {
                        return Err(Box::new(Error::EmptyArrayPointer {
                            pointer: p.to_string(),
                        }))
                        .context(table::LoadJsonSnafu);
                    }
                    None => {
                        return Err(Box::new(Error::EmptyArray {})).context(table::LoadJsonSnafu);
                    }
                }
            }

            let (batch_schema, partition) =
                json_vec_to_partition(json_rows, &t.schema, batch_size, array_encoded)
                    .map_err(Box::new)
                    .context(table::LoadJsonSnafu)?;

            merged_schema = Some(match &merged_schema {
                Some(s) if s != &batch_schema => Schema::try_merge(vec![s.clone(), batch_schema])
                    .map_err(Box::new)
                    .context(table::MergeSchemaSnafu)?,
                _ => batch_schema,
            });

            Ok(partition)
        })
        .collect::<Result<Vec<Vec<RecordBatch>>, table::Error>>()?;

    Ok((merged_schema, partitions))
}

pub async fn to_mem_table(
    t: &TableSource,
    dfctx: &datafusion::execution::context::SessionContext,
) -> Result<datafusion::datasource::MemTable, table::Error> {
    let (merged_schema, partitions) = to_partitions(t, dfctx).await?;
    datafusion::datasource::MemTable::try_new(
        Arc::new(
            merged_schema
                .ok_or(Error::SchemaNotFound {})
                .map_err(Box::new)
                .context(table::LoadJsonSnafu)?,
        ),
        partitions,
    )
    .map_err(Box::new)
    .context(table::CreateMemTableSnafu)
}

async fn to_datafusion_table(
    t: TableSource,
    dfctx: datafusion::execution::context::SessionContext,
) -> Result<Arc<dyn TableProvider>, table::Error> {
    Ok(Arc::new(to_mem_table(&t, &dfctx).await?))
}

pub async fn to_loaded_table(
    t: TableSource,
    dfctx: datafusion::execution::context::SessionContext,
) -> Result<LoadedTable, table::Error> {
    LoadedTable::new_from_df_table_cb(move || to_datafusion_table(t.clone(), dfctx.clone())).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    use datafusion::{datasource::TableProvider, prelude::SessionContext};

    use crate::test_util::*;

    #[tokio::test]
    async fn nested_struct_and_lists() {
        let json_content = r#"[
          {
            "foo": [
              {
                "bar": "1234",
                "baz": 1
              }
            ]
          }
        ]"#;

        let tmp_dir = tempfile::TempDir::new().unwrap();
        let tmp_file_path = tmp_dir.path().join("nested.json");
        let mut f = std::fs::File::create(tmp_file_path.clone()).unwrap();
        writeln!(f, "{json_content}").unwrap();

        let ctx = SessionContext::new();
        let t = to_mem_table(
            &TableSource::new(
                "nested_json".to_string(),
                format!("{}", tmp_file_path.to_string_lossy()),
            ),
            &ctx,
        )
        .await
        .unwrap();

        let schema = t.schema();
        let fields = schema.fields();

        let mut obj_keys = fields.iter().map(|f| f.name()).collect::<Vec<_>>();
        obj_keys.sort();
        let mut expected_obj_keys = vec!["foo"];
        expected_obj_keys.sort();

        assert_eq!(obj_keys, expected_obj_keys);
    }

    #[tokio::test]
    async fn spacex_launches() {
        let ctx = SessionContext::new();
        let t = to_mem_table(
            &TableSource::new(
                "spacex_launches".to_string(),
                test_data_path("spacex_launches.json"),
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
    async fn test_multiple_batches() {
        let ctx = SessionContext::new();
        let mut source = TableSource::new(
            "spacex_launches".to_string(),
            test_data_path("spacex_launches.json"),
        );
        source.batch_size = 1;
        let (_, p) = to_partitions(&source, &ctx).await.unwrap();
        assert_eq!(p.len(), 1);
        let batch = &p[0];
        assert_eq!(batch[0].num_rows(), 132);
        assert_eq!(batch.len(), source.batch_size);
    }
}
