use std::io::{BufReader, Read};
use std::sync::Arc;

use datafusion::arrow;
use datafusion::arrow::datatypes::Schema;
#[allow(deprecated)]
use datafusion::arrow::json::reader::ReaderBuilder;
use datafusion::arrow::record_batch::RecordBatch;
use serde_json::value::Value;

use crate::error::ColumnQError;
use crate::table::{TableLoadOption, TableSchema, TableSource};

fn json_value_from_reader<R: Read>(r: R) -> Result<Value, ColumnQError> {
    let reader = BufReader::new(r);
    serde_json::from_reader(reader).map_err(ColumnQError::json_parse)
}

fn json_partition_to_vec(
    json_partition: &Value,
    pointer: Option<&str>,
) -> Result<Vec<Value>, ColumnQError> {
    let mut value_ref = json_partition;

    if let Some(p) = pointer {
        match value_ref.pointer(p) {
            Some(v) => value_ref = v,
            None => return Err(ColumnQError::LoadJson(format!("Invalid json pointer: {p}"))),
        }
    }

    match value_ref.as_array() {
        Some(arr) => Ok(arr.to_vec()),
        None => Err(ColumnQError::LoadJson(format!(
            "{} is not an array",
            pointer.unwrap_or("JSON data")
        ))),
    }
}

fn json_vec_to_partition(
    json_rows: Vec<Value>,
    provided_schema: &Option<TableSchema>,
    batch_size: usize,
    array_encoded: bool,
) -> Result<(arrow::datatypes::Schema, Vec<RecordBatch>), ColumnQError> {
    // load schema
    let schema = match provided_schema {
        Some(s) => s.into(),
        None => arrow::json::reader::infer_json_schema_from_iterator(
            json_rows.iter().map(|v| Ok(v.clone())),
        )
        .map_err(|e| {
            ColumnQError::LoadJson(format!("Failed to infer schema from JSON data: {e}"))
        })?,
    };

    // TODO: batch_size setting here doesn't work because we are invoking serialize directly. might
    // be better to break up the batch ourselives.
    let mut decoder = ReaderBuilder::new(Arc::new(schema.clone()))
        .with_batch_size(batch_size)
        .build_decoder()?;

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
                        None => Err(arrow::error::ArrowError::JsonError(format!(
                            "arry encoded JSON row missing column {i:?} : {json_row:?}"
                        ))),
                    }
                })?;
                Ok(Value::Object(m))
            })
            .collect::<Result<Vec<Value>, ColumnQError>>()?;

        // TODO: avoid unnecessary collection here, update upstream json reader to take an iterator
        // instead of slice
        decoder.serialize(&objects)?;
    } else {
        // Note: serialize ignores any batch size setting, and always decodes all rows
        decoder.serialize(&json_rows)?;
    };

    let batch = decoder
        .flush()?
        .ok_or_else(|| ColumnQError::LoadJson("No item found".to_string()))?;

    Ok((schema, vec![batch]))
}

async fn to_partitions(
    t: &TableSource,
    dfctx: &datafusion::execution::context::SessionContext,
) -> Result<(Option<Schema>, Vec<Vec<RecordBatch>>), ColumnQError> {
    let batch_size = t.batch_size;
    let array_encoded = match &t.option {
        Some(TableLoadOption::json { array_encoded, .. }) => array_encoded.unwrap_or(false),
        _ => false,
    };

    if array_encoded && t.schema.is_none() {
        return Err(ColumnQError::LoadJson(
            "Array encoded option requires manually specified schema".to_string(),
        ));
    }

    let pointer = match &t.option {
        Some(TableLoadOption::json { pointer, .. }) => pointer.to_owned(),
        _ => None,
    };

    let mut merged_schema: Option<Schema> = None;
    let json_partitions: Vec<Value> =
        partitions_from_table_source!(t, json_value_from_reader, dfctx)?;

    let partitions = json_partitions
        .iter()
        .map(|json_partition| {
            let json_rows = json_partition_to_vec(json_partition, pointer.as_deref())?;
            if json_rows.is_empty() {
                match &pointer {
                    Some(p) => {
                        return Err(ColumnQError::LoadJson(format!(
                            "{p} points to an empty array"
                        )));
                    }
                    None => {
                        return Err(ColumnQError::LoadJson(
                            "JSON data is an empty array".to_string(),
                        ));
                    }
                }
            }

            let (batch_schema, partition) =
                json_vec_to_partition(json_rows, &t.schema, batch_size, array_encoded)?;

            merged_schema = Some(match &merged_schema {
                Some(s) if s != &batch_schema => Schema::try_merge(vec![s.clone(), batch_schema])?,
                _ => batch_schema,
            });

            Ok(partition)
        })
        .collect::<Result<Vec<Vec<RecordBatch>>, ColumnQError>>()?;

    Ok((merged_schema, partitions))
}

pub async fn to_mem_table(
    t: &TableSource,
    dfctx: &datafusion::execution::context::SessionContext,
) -> Result<datafusion::datasource::MemTable, ColumnQError> {
    let (merged_schema, partitions) = to_partitions(t, dfctx).await?;
    Ok(datafusion::datasource::MemTable::try_new(
        Arc::new(
            merged_schema
                .ok_or_else(|| ColumnQError::LoadJson("failed to load schema".to_string()))?,
        ),
        partitions,
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    use datafusion::{datasource::TableProvider, prelude::SessionContext};

    use crate::test_util::*;

    #[tokio::test]
    async fn nested_struct_and_lists() -> Result<(), ColumnQError> {
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
        writeln!(f, "{}", json_content).unwrap();

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

        Ok(())
    }

    #[tokio::test]
    async fn spacex_launches() -> Result<(), ColumnQError> {
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

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_batches() -> Result<(), ColumnQError> {
        let ctx = SessionContext::new();
        let mut source = TableSource::new(
            "spacex_launches".to_string(),
            test_data_path("spacex_launches.json"),
        );
        source.batch_size = 1;
        let (_, p) = to_partitions(&source, &ctx).await?;
        assert_eq!(p.len(), 1);
        let batch = &p[0];
        assert_eq!(batch[0].num_rows(), 132);
        assert_eq!(batch.len(), source.batch_size);
        Ok(())
    }
}
