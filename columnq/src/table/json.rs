use std::convert::TryFrom;
use std::fs;
use std::io::Cursor;
use std::sync::Arc;

use serde_json::value::Value;
use uriparse::{Scheme, URIReference};

use crate::error::ColumnQError;
use crate::table::{TableLoadOption, TableSource};

async fn load_array_by_path(
    uri_s: &str,
    pointer: Option<&str>,
) -> Result<Vec<Value>, ColumnQError> {
    let uri =
        URIReference::try_from(uri_s).map_err(|_| ColumnQError::InvalidUri(uri_s.to_string()))?;

    let payload: Value = with_reader_from_uri!(serde_json::from_reader, uri)?;

    let mut value_ref: &Value = &payload;

    if let Some(p) = pointer {
        match value_ref.pointer(p) {
            Some(v) => value_ref = v,
            None => {
                return Err(ColumnQError::LoadJson(format!(
                    "Invalid json pointer: {}",
                    p
                )))
            }
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

pub async fn to_mem_table(
    t: &TableSource,
) -> Result<datafusion::datasource::MemTable, ColumnQError> {
    let batch_size = 1024;
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

    // load array from file
    let json_rows = load_array_by_path(&t.uri, pointer.as_deref()).await?;

    if json_rows.is_empty() {
        match pointer {
            Some(p) => {
                return Err(ColumnQError::LoadJson(format!(
                    "{} points to an emtpy array",
                    p
                )));
            }
            None => {
                return Err(ColumnQError::LoadJson(
                    "JSON data is an emtpy array".to_string(),
                ));
            }
        }
    }

    // load schema
    let schema_ref: arrow::datatypes::SchemaRef = match &t.schema {
        Some(s) => Arc::new(s.into()),
        None => arrow::json::reader::infer_json_schema_from_iterator(
            json_rows.iter().map(|v| Ok(v.clone())),
        )
        .map_err(|e| {
            ColumnQError::LoadJson(format!("Failed to infer schema from JSON data: {}", e))
        })?,
    };

    // decode to arrow record batch
    let decoder = arrow::json::reader::Decoder::new(schema_ref.clone(), batch_size, None);
    let batch = {
        // enclose values_iter in its own scope so it won't brrow schema_ref til end of this
        // function
        let mut values_iter: Box<dyn Iterator<Item = arrow::error::Result<Value>>>;
        values_iter = if array_encoded {
            // convert row array to object based on schema
            // TODO: support array_encoded read in arrow json reader instead
            Box::new(json_rows.into_iter().map(|json_row| {
                let mut m = serde_json::map::Map::new();
                schema_ref
                    .fields()
                    .iter()
                    .enumerate()
                    .try_for_each(|(i, f)| match json_row.get(i) {
                        Some(x) => {
                            m.insert(f.name().to_string(), x.clone());
                            Ok(())
                        }
                        None => Err(arrow::error::ArrowError::JsonError(format!(
                            "arry encoded JSON row missing column {:?} : {:?}",
                            i, json_row
                        ))),
                    })?;
                Ok(Value::Object(m))
            }))
        } else {
            // no need to convert row since each row is already an object
            Box::new(json_rows.into_iter().map(Ok))
        };

        // decode whole array into single record batch
        decoder
            .next_batch(&mut values_iter)
            .map_err(|e| {
                ColumnQError::LoadJson(format!("Failed decode JSON into Arrow record batch: {}", e))
            })?
            .ok_or_else(|| {
                ColumnQError::LoadJson("JSON data results in empty arrow record batch".to_string())
            })?
    };
    let partitions = vec![vec![batch]];

    Ok(datafusion::datasource::MemTable::try_new(
        schema_ref, partitions,
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::datasource::TableProvider;

    use crate::test_util::*;

    #[tokio::test]
    async fn test_nested_struct_and_lists() -> Result<(), ColumnQError> {
        let t = to_mem_table(&TableSource {
            name: "spacex_launches".to_string(),
            uri: test_data_path("spacex-launches.json"),
            schema: None,
            option: None,
        })
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

        assert_eq!(obj_keys, expected_obj_keys,);

        Ok(())
    }
}
