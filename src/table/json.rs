use bytes::buf::ext::BufExt;
use std::convert::TryFrom;
use std::fs;
use std::io::Cursor;
use std::sync::Arc;

use anyhow::Context;
use serde_json::value::Value;
use uriparse::{Scheme, URIReference};

use crate::table::{TableLoadOption, TableSource};

async fn load_array_by_path(uri_s: &str, pointer: Option<&str>) -> anyhow::Result<Vec<Value>> {
    let uri = URIReference::try_from(uri_s).with_context(|| format!("invalid uri: `{}`", uri_s))?;

    let payload: Value =
        with_reader_from_uri!(serde_json::from_reader, uri).context("Failed to parse JSON data")?;

    let mut value_ref: &Value = &payload;

    if let Some(p) = pointer {
        match value_ref.pointer(p) {
            Some(v) => value_ref = v,
            None => anyhow::bail!("Invalid json pointer: {}", p),
        }
    }

    match value_ref.as_array() {
        Some(arr) => Ok(arr.to_vec()),
        None => {
            anyhow::bail!("{} is not an array", pointer.unwrap_or("JSON data"));
        }
    }
}

pub async fn to_mem_table(t: &TableSource) -> anyhow::Result<datafusion::datasource::MemTable> {
    let batch_size = 1024;
    let array_encoded = match &t.option {
        Some(TableLoadOption::json { array_encoded, .. }) => array_encoded.unwrap_or(false),
        _ => false,
    };

    if array_encoded && t.schema.is_none() {
        anyhow::bail!("Array encoded option requires manually specified schema");
    }

    let pointer = match &t.option {
        Some(TableLoadOption::json { pointer, .. }) => pointer.to_owned(),
        _ => None,
    };

    // load array from file
    let json_rows = load_array_by_path(&t.uri, pointer.as_deref()).await?;

    if json_rows.is_empty() {
        match pointer {
            Some(p) => anyhow::bail!("{} points to an emtpy array", p),
            None => anyhow::bail!("JSON data is an emtpy array"),
        }
    }

    // load schema
    let schema_ref: arrow::datatypes::SchemaRef = match &t.schema {
        Some(s) => Arc::new(s.into()),
        None => arrow::json::reader::infer_json_schema_from_iterator(
            json_rows.iter().map(|v| Ok(v.clone())),
        )
        .context("Failed to infer schema from JSON data")?,
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
            .context("Failed decode JSON into Arrow record batch")?
            .unwrap()
    };
    let partitions = vec![vec![batch]];

    Ok(datafusion::datasource::MemTable::try_new(
        schema_ref, partitions,
    )?)
}
