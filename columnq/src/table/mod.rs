use std::convert::TryFrom;
use std::ffi::OsStr;
use std::path::Path;

use serde_derive::Deserialize;
use uriparse::URIReference;

use crate::error::ColumnQError;

pub mod csv;
pub mod delta;
pub mod google_spreadsheets;
pub mod json;
pub mod ndjson;
pub mod parquet;

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

// Adding new table format:
// * update TableLoadOption enum to add the new variant
// * update TableLoadOption.extension
// * update TableSource.extension
// * update load

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
    ndjson {},
    parquet {},
    google_spreadsheet(TableOptionGoogleSpreasheet),
    delta {},
}

impl TableLoadOption {
    fn as_google_spreadsheet_opt(&self) -> Result<&TableOptionGoogleSpreasheet, ColumnQError> {
        match self {
            TableLoadOption::google_spreadsheet(opt) => Ok(&opt),
            _ => Err(ColumnQError::ExpectFormatOption(
                "google_spreadsheets".to_string(),
            )),
        }
    }

    pub fn extension<'a>(&'a self) -> &'static str {
        match self {
            TableLoadOption::json { .. } => "json",
            TableLoadOption::ndjson { .. } => "ndjson",
            TableLoadOption::csv { .. } => "csv",
            TableLoadOption::parquet { .. } => "parquet",
            TableLoadOption::google_spreadsheet(_) | TableLoadOption::delta { .. } => "",
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

impl TableSource {
    pub fn new(name: String, uri: String) -> Self {
        // TODO: parse table format from uri during initializeion?
        Self {
            name,
            uri,
            schema: None,
            option: None,
        }
    }

    pub fn parsed_uri(&self) -> Result<URIReference, ColumnQError> {
        URIReference::try_from(self.uri.as_str())
            .map_err(|_| ColumnQError::InvalidUri(self.uri.clone()))
    }

    pub fn extension(&self) -> Result<&str, ColumnQError> {
        Ok(match &self.option {
            Some(opt) => opt.extension(),
            None => {
                let ext = Path::new(&self.uri)
                    .extension()
                    .and_then(OsStr::to_str)
                    .ok_or_else(|| {
                        ColumnQError::InvalidUri(format!(
                            "cannot detect table extension from uri: {}",
                            self.uri
                        ))
                    })?;

                match ext {
                    "csv" | "json" | "parquet" | "ndjson" => ext,
                    _ => {
                        return Err(ColumnQError::InvalidUri(format!(
                            "unsupported extension in uri: {}",
                            self.uri
                        )));
                    }
                }
            }
        })
    }
}

pub async fn load(t: &TableSource) -> Result<datafusion::datasource::MemTable, ColumnQError> {
    if let Some(opt) = &t.option {
        return Ok(match opt {
            TableLoadOption::json { .. } => json::to_mem_table(t).await?,
            TableLoadOption::ndjson { .. } => ndjson::to_mem_table(t).await?,
            TableLoadOption::csv { .. } => csv::to_mem_table(t).await?,
            TableLoadOption::parquet { .. } => parquet::to_mem_table(t).await?,
            TableLoadOption::google_spreadsheet(_) => google_spreadsheets::to_mem_table(t).await?,
            TableLoadOption::delta { .. } => delta::to_mem_table(t).await?,
        });
    }

    // no format specified explictly, try to guess from file path
    Ok(
        match Path::new(&t.uri).extension().and_then(OsStr::to_str) {
            Some("csv") => csv::to_mem_table(t).await?,
            Some("json") => json::to_mem_table(t).await?,
            Some("ndjson") => ndjson::to_mem_table(t).await?,
            Some("parquet") => parquet::to_mem_table(t).await?,
            Some(ext) => {
                return Err(ColumnQError::InvalidUri(format!(
                    "failed to register `{}` as table `{}`, unsupported table format `{}`",
                    t.uri, t.name, ext,
                )));
            }
            None => {
                return Err(ColumnQError::InvalidUri(format!(
                    "failed to register `{}` as table `{}`, cannot detect table format",
                    t.uri, t.name
                )));
            }
        },
    )
}
