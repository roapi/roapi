use std::io::{BufReader, Read};
use std::sync::Arc;

use datafusion::arrow;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::json::reader::{Decoder, DecoderOptions};
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

    // decode to arrow record batch
    let decoder = Decoder::new(
        Arc::new(schema.clone()),
        DecoderOptions::new().with_batch_size(batch_size),
    );
    let mut batches = vec![];
    {
        // enclose values_iter in its own scope so it won't borrow schema_ref til end of this
        // function
        let mut values_iter: Box<dyn Iterator<Item = arrow::error::Result<Value>>> =
            if array_encoded {
                // convert row array to object based on schema
                // TODO: support array_encoded read in upstream arrow json reader instead
                Box::new(json_rows.into_iter().map(|json_row| {
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
                }))
            } else {
                // no need to convert row since each row is already an object
                Box::new(json_rows.into_iter().map(Ok))
            };

        while let Some(batch) = decoder.next_batch(&mut values_iter).map_err(|e| {
            ColumnQError::LoadJson(format!("Failed decode JSON into Arrow record batch: {e}"))
        })? {
            batches.push(batch);
        }
    }

    Ok((schema, batches))
}

async fn to_partitions(
    t: &TableSource,
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
    let json_partitions: Vec<Value> = partitions_from_table_source!(t, json_value_from_reader)?;

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
) -> Result<datafusion::datasource::MemTable, ColumnQError> {
    let (merged_schema, partitions) = to_partitions(t).await?;
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

    use datafusion::datasource::TableProvider;

    use crate::test_util::*;

    #[tokio::test]
    async fn nested_struct_and_lists() -> Result<(), ColumnQError> {
        let t = to_mem_table(&TableSource::new(
            "spacex_launches".to_string(),
            test_data_path("spacex_launches.json"),
        ))
        .await?;

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
        let mut source = TableSource::new(
            "spacex_launches".to_string(),
            test_data_path("spacex_launches.json"),
        );
        source.batch_size = 1;
        let (_, p) = to_partitions(&source).await?;
        assert_eq!(p.len(), 1);
        assert_eq!(p[0][0].num_rows(), source.batch_size);
        assert_eq!(p[0].len(), 132);
        Ok(())
    }
}
