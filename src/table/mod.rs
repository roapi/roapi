use std::ffi::OsStr;
use std::path::Path;

use anyhow::Context;
use serde_derive::Deserialize;

use crate::error::LoadTableError;

#[derive(Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct TableColumn {
    pub name: String,
    pub data_type: arrow::datatypes::DataType,
    #[serde(default)]
    pub nullable: bool,
}

impl From<&TableColumn> for arrow::datatypes::Field {
    fn from(c: &TableColumn) -> Self {
        arrow::datatypes::Field::new(&c.name, c.data_type.clone(), c.nullable)
    }
}

#[derive(Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct TableSchema {
    pub columns: Vec<TableColumn>,
}

impl From<&TableSchema> for arrow::datatypes::Schema {
    fn from(s: &TableSchema) -> Self {
        arrow::datatypes::Schema::new(
            s.columns
                .iter()
                .map(|c| c.into())
                .collect::<Vec<arrow::datatypes::Field>>(),
        )
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct TableOptionGoogleSpreasheet {
    application_secret_path: String,
    sheet_title: Option<String>,
}

#[allow(non_camel_case_types)]
#[derive(Deserialize, Clone)]
#[serde(tag = "format")]
#[serde(deny_unknown_fields)]
pub enum TableLoadOption {
    json {
        // JSON query pointer following https://tools.ietf.org/html/rfc6901
        pointer: Option<String>,
        array_encoded: Option<bool>,
    },
    csv {},
    parquet {},
    google_spreadsheet(TableOptionGoogleSpreasheet),
}

impl TableLoadOption {
    fn as_google_spreadsheet_opt(&self) -> Result<&TableOptionGoogleSpreasheet, LoadTableError> {
        match self {
            TableLoadOption::google_spreadsheet(opt) => Ok(&opt),
            _ => Err(LoadTableError::ExpectFormatOption(
                "google_spreadsheets".to_string(),
            )),
        }
    }
}

#[derive(Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct TableSource {
    pub name: String,
    pub uri: String,
    pub schema: Option<TableSchema>,
    pub option: Option<TableLoadOption>,
}

pub async fn load(t: &TableSource) -> anyhow::Result<datafusion::datasource::MemTable> {
    // TODO: support reading list of files within directory
    if let Some(opt) = &t.option {
        return Ok(match opt {
            TableLoadOption::json { .. } => json::to_mem_table(t)
                .await
                .context("JSON table load error")?,
            TableLoadOption::csv { .. } => csv::to_mem_table(t).context("CSV table load error")?,
            TableLoadOption::parquet { .. } => {
                parquet::to_mem_table(t).context("Parquet table load error")?
            }
            TableLoadOption::google_spreadsheet(_) => google_spreadsheets::to_mem_table(t)
                .await
                .context("Google spreadsheet load error")?,
        });
    }

    // no format specified explictly, try to guess from file path
    Ok(
        match Path::new(&t.uri).extension().and_then(OsStr::to_str) {
            Some("csv") => csv::to_mem_table(t).context("CSV table load error")?,
            Some("json") => json::to_mem_table(t)
                .await
                .context("JSON table load error")?,
            Some("parquet") => parquet::to_mem_table(t).context("Parquet table load error")?,
            Some(ext) => anyhow::bail!(
                "failed to register `{}` as table `{}`, unsupported table format `{}`",
                t.uri,
                t.name,
                ext,
            ),
            None => anyhow::bail!(
                "failed to register `{}` as table `{}`, cannot detect table format",
                t.uri,
                t.name
            ),
        },
    )
}

pub mod csv;
pub mod google_spreadsheets;
pub mod json;
pub mod parquet;

#[cfg(test)]
pub mod test;
