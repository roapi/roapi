use std::convert::TryFrom;
use std::ffi::OsStr;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;

use datafusion::datasource::TableProvider;

use datafusion::arrow;
use serde::de::{Deserialize, Deserializer};
use serde_derive::Deserialize;
use uriparse::URIReference;

use crate::error::ColumnQError;

pub mod arrow_ipc_file;
pub mod arrow_ipc_stream;
pub mod csv;
pub mod database;
pub mod delta;
pub mod google_spreadsheets;
pub mod json;
pub mod ndjson;
pub mod parquet;
pub mod xlsx;

#[derive(Deserialize, Clone, Debug, Eq, PartialEq)]
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

impl From<TableColumn> for arrow::datatypes::Field {
    fn from(c: TableColumn) -> Self {
        // TODO: update upstream arrow::datatypes::Field::new to support taking owned string as
        // name argument
        arrow::datatypes::Field::new(&c.name, c.data_type, c.nullable)
    }
}

#[derive(Deserialize, Clone, Debug, Eq, PartialEq)]
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

impl From<TableSchema> for arrow::datatypes::Schema {
    fn from(s: TableSchema) -> Self {
        arrow::datatypes::Schema::new(
            s.columns
                .into_iter()
                .map(|c| c.into())
                .collect::<Vec<arrow::datatypes::Field>>(),
        )
    }
}

#[derive(Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct TableOptionGoogleSpreadsheet {
    application_secret_path: String,
    sheet_title: Option<String>,
}

/// CSV table load option
#[derive(Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct TableOptionCsv {
    #[serde(default = "TableOptionCsv::default_has_header")]
    has_header: bool,
    #[serde(default = "TableOptionCsv::default_delimiter")]
    #[serde(deserialize_with = "TableOptionCsv::deserialize_delimiter")]
    delimiter: u8,
    #[serde(default = "TableOptionCsv::default_projection")]
    projection: Option<Vec<usize>>,
    #[serde(default = "TableOptionCsv::default_use_memory_table")]
    use_memory_table: bool,
}

impl TableOptionCsv {
    #[inline]
    pub fn default_has_header() -> bool {
        true
    }

    #[inline]
    pub fn default_delimiter() -> u8 {
        b','
    }

    #[inline]
    pub fn default_projection() -> Option<Vec<usize>> {
        None
    }

    #[inline]
    #[must_use]
    pub fn with_delimiter(mut self, d: u8) -> Self {
        self.delimiter = d;
        self
    }

    #[inline]
    #[must_use]
    pub fn with_has_header(mut self, has_header: bool) -> Self {
        self.has_header = has_header;
        self
    }

    fn deserialize_delimiter<'de, D>(deserializer: D) -> Result<u8, D::Error>
    where
        D: Deserializer<'de>,
    {
        let buf = String::deserialize(deserializer)?;
        match buf.len() {
            1 => Ok(buf.into_bytes()[0]),
            _ => Err(serde::de::Error::custom(
                "CSV delimiter should be a single character",
            )),
        }
    }
    #[inline]
    pub fn default_use_memory_table() -> bool {
        true
    }
}

impl Default for TableOptionCsv {
    fn default() -> Self {
        Self {
            has_header: Self::default_has_header(),
            delimiter: Self::default_delimiter(),
            projection: Self::default_projection(),
            use_memory_table: Self::default_use_memory_table(),
        }
    }
}

#[derive(Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct TableOptionParquet {
    #[serde(default = "TableOptionParquet::default_use_memory_table")]
    use_memory_table: bool,
}

impl TableOptionParquet {
    #[inline]
    pub fn default_use_memory_table() -> bool {
        true
    }
}

impl Default for TableOptionParquet {
    fn default() -> Self {
        Self {
            use_memory_table: Self::default_use_memory_table(),
        }
    }
}

#[derive(Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct TableOptionXlsx {
    sheet_name: Option<String>,
}

#[derive(Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct TableOptionDelta {
    #[serde(default = "TableOptionDelta::default_use_memory_table")]
    use_memory_table: bool,
}

impl TableOptionDelta {
    #[inline]
    pub fn default_use_memory_table() -> bool {
        true
    }
}

impl Default for TableOptionDelta {
    fn default() -> Self {
        Self {
            use_memory_table: Self::default_use_memory_table(),
        }
    }
}

// Adding new table format:
// * update TableLoadOption enum to add the new variant
// * update TableLoadOption.extension
// * update TableSource.extension
// * update load

#[allow(non_camel_case_types)]
#[derive(Deserialize, Clone, Debug, Eq, PartialEq)]
#[serde(tag = "format")]
#[serde(deny_unknown_fields)]
pub enum TableLoadOption {
    json {
        // JSON query pointer following https://tools.ietf.org/html/rfc6901
        pointer: Option<String>,
        array_encoded: Option<bool>,
    },
    csv(TableOptionCsv),
    ndjson {},
    jsonl {},
    parquet(TableOptionParquet),
    google_spreadsheet(TableOptionGoogleSpreadsheet),
    xlsx(TableOptionXlsx),
    delta(TableOptionDelta),
    arrow {},
    arrows {},
    mysql {},
    sqlite {},
    postgres {},
}

impl TableLoadOption {
    fn as_google_spreadsheet(&self) -> Result<&TableOptionGoogleSpreadsheet, ColumnQError> {
        match self {
            Self::google_spreadsheet(opt) => Ok(opt),
            _ => Err(ColumnQError::ExpectFormatOption(
                "google_spreadsheets".to_string(),
            )),
        }
    }

    fn as_xlsx(&self) -> Result<&TableOptionXlsx, ColumnQError> {
        match self {
            Self::xlsx(opt) => Ok(opt),
            _ => Err(ColumnQError::ExpectFormatOption("xlsx".to_string())),
        }
    }

    fn as_csv(&self) -> Result<&TableOptionCsv, ColumnQError> {
        match self {
            Self::csv(opt) => Ok(opt),
            _ => Err(ColumnQError::ExpectFormatOption("csv".to_string())),
        }
    }

    fn as_parquet(&self) -> Result<&TableOptionParquet, ColumnQError> {
        match self {
            Self::parquet(opt) => Ok(opt),
            _ => Err(ColumnQError::ExpectFormatOption("parquet".to_string())),
        }
    }

    fn as_delta(&self) -> Result<&TableOptionDelta, ColumnQError> {
        match self {
            Self::delta(opt) => Ok(opt),
            _ => Err(ColumnQError::ExpectFormatOption("delta".to_string())),
        }
    }

    pub fn extension(&self) -> &'static str {
        match self {
            Self::json { .. } => "json",
            Self::ndjson { .. } => "ndjson",
            Self::jsonl { .. } => "jsonl",
            Self::csv { .. } => "csv",
            Self::parquet { .. } => "parquet",
            Self::google_spreadsheet(_) | Self::delta { .. } => "",
            Self::xlsx { .. } => "xlsx",
            Self::arrow { .. } => "arrow",
            Self::arrows { .. } => "arrows",
            Self::mysql { .. } => "mysql",
            Self::sqlite { .. } => "sqlite",
            Self::postgres { .. } => "postgres",
        }
    }
}

#[derive(Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TableIoSource {
    Uri(String),
    Memory(Vec<u8>),
}

impl<T: Into<String>> From<T> for TableIoSource {
    fn from(s: T) -> Self {
        Self::Uri(s.into())
    }
}

impl TableIoSource {
    pub fn as_memory(&self) -> Result<&[u8], ColumnQError> {
        match self {
            Self::Memory(data) => Ok(data),
            other => Err(ColumnQError::Generic(format!(
                "expect memory IO source, got: {other:?}"
            ))),
        }
    }
}

impl std::fmt::Display for TableIoSource {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            TableIoSource::Uri(uri) => write!(f, "uri({uri})"),
            TableIoSource::Memory(_) => write!(f, "memory"),
        }
    }
}

#[derive(Deserialize, Clone, Debug, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TableSource {
    pub name: String,
    #[serde(flatten)]
    pub io_source: TableIoSource,
    pub schema: Option<TableSchema>,
    pub option: Option<TableLoadOption>,
    #[serde(default = "TableSource::default_batch_size")]
    pub batch_size: usize,
}

impl From<KeyValueSource> for TableSource {
    fn from(kv: KeyValueSource) -> Self {
        Self {
            name: kv.name,
            io_source: kv.io_source,
            schema: kv.schema,
            option: kv.option,
            batch_size: Self::default_batch_size(),
        }
    }
}

impl TableSource {
    pub fn new(name: impl Into<String>, source: impl Into<TableIoSource>) -> Self {
        let io_source = source.into();
        let option = Self::parse_option(&io_source);
        Self {
            name: name.into(),
            io_source,
            schema: None,
            option,
            batch_size: Self::default_batch_size(),
        }
    }

    pub fn new_with_uri(name: impl Into<String>, uri: impl Into<String>) -> Self {
        Self::new(name, uri.into())
    }

    #[inline]
    pub fn default_batch_size() -> usize {
        8192
    }

    #[inline]
    #[must_use]
    pub fn with_option(mut self, option: impl Into<TableLoadOption>) -> Self {
        self.option = Some(option.into());
        self
    }

    #[inline]
    #[must_use]
    pub fn with_schema(mut self, schema: impl Into<TableSchema>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    pub fn get_uri_str(&self) -> &str {
        match &self.io_source {
            TableIoSource::Uri(uri) => uri.as_str(),
            TableIoSource::Memory(_) => "memory",
        }
    }

    pub fn parse_option(source: &TableIoSource) -> Option<TableLoadOption> {
        match source {
            TableIoSource::Uri(uri) => {
                let uri = URIReference::try_from(uri.as_str()).ok()?;
                let scheme = uri.scheme()?;
                match scheme.as_str() {
                    "mysql" => Some(TableLoadOption::mysql {}),
                    "sqlite" => Some(TableLoadOption::sqlite {}),
                    "postgresql" => Some(TableLoadOption::postgres {}),
                    _ => None,
                }
            }
            TableIoSource::Memory(_) => None,
        }
    }

    pub fn parsed_uri(&self) -> Result<URIReference, ColumnQError> {
        match &self.io_source {
            TableIoSource::Uri(uri) => URIReference::try_from(uri.as_str()).map_err(|_| {
                ColumnQError::InvalidUri(format!("{uri}. Make sure it's URI encoded."))
            }),
            TableIoSource::Memory(_) => URIReference::builder()
                .with_scheme(Some(uriparse::Scheme::try_from("memory").map_err(|e| {
                    ColumnQError::Generic(format!(
                        "failed to create uri scheme for memory IO source: {e:?}"
                    ))
                })?))
                .with_path(uriparse::Path::try_from("data").map_err(|e| {
                    ColumnQError::Generic(format!(
                        "failed to create uri path for memory IO source: {e:?}"
                    ))
                })?)
                .build()
                .map_err(|e| {
                    ColumnQError::Generic(format!(
                        "failed to create uri for memory IO source: {e:?}"
                    ))
                }),
        }
    }

    pub fn extension(&self) -> Result<&str, ColumnQError> {
        Ok(match (&self.option, &self.io_source) {
            (Some(opt), _) => opt.extension(),
            (None, TableIoSource::Uri(uri)) => {
                match Path::new(uri).extension().and_then(OsStr::to_str) {
                    Some(ext) => match ext {
                        "csv" | "json" | "ndjson" | "jsonl" | "parquet" | "arrow" | "arrows"
                        | "xlsx" => ext,
                        "sqlite" | "sqlite3" | "db" => "sqlite",
                        _ => {
                            return Err(ColumnQError::InvalidUri(format!(
                                "unsupported extension in uri: {uri}"
                            )));
                        }
                    },
                    None => {
                        // database sources doesn't have suffix extension, parse scheme instead
                        match TableSource::parse_option(&self.io_source) {
                            Some(TableLoadOption::mysql {}) => "mysql",
                            Some(TableLoadOption::sqlite {}) => "sqlite",
                            Some(TableLoadOption::postgres {}) => "postgres",
                            _ => {
                                return Err(ColumnQError::InvalidUri(format!(
                                    "unsupported extension in uri: {uri}"
                                )));
                            }
                        }
                    }
                }
            }
            (None, TableIoSource::Memory(_)) => {
                return Err(ColumnQError::Generic(
                    "cannot detect table extension from memory IO source, please specify a format option".to_string()
                ));
            }
        })
    }
}

pub async fn load(
    t: &TableSource,
    dfctx: &datafusion::execution::context::SessionContext,
) -> Result<Arc<dyn TableProvider>, ColumnQError> {
    if let Some(opt) = &t.option {
        Ok(match opt {
            TableLoadOption::json { .. } => Arc::new(json::to_mem_table(t).await?),
            TableLoadOption::ndjson { .. } | TableLoadOption::jsonl { .. } => {
                Arc::new(ndjson::to_mem_table(t).await?)
            }
            TableLoadOption::csv { .. } => csv::to_datafusion_table(t, dfctx).await?,
            TableLoadOption::parquet { .. } => parquet::to_datafusion_table(t, dfctx).await?,
            TableLoadOption::google_spreadsheet(_) => {
                Arc::new(google_spreadsheets::to_mem_table(t).await?)
            }
            TableLoadOption::xlsx { .. } => Arc::new(xlsx::to_mem_table(t).await?),
            TableLoadOption::delta { .. } => delta::to_datafusion_table(t).await?,
            TableLoadOption::arrow { .. } => Arc::new(arrow_ipc_file::to_mem_table(t).await?),
            TableLoadOption::arrows { .. } => Arc::new(arrow_ipc_stream::to_mem_table(t).await?),
            TableLoadOption::mysql { .. } => {
                Arc::new(database::DatabaseLoader::MySQL.to_mem_table(t)?)
            }
            TableLoadOption::sqlite { .. } => {
                Arc::new(database::DatabaseLoader::SQLite.to_mem_table(t)?)
            }
            TableLoadOption::postgres { .. } => {
                Arc::new(database::DatabaseLoader::Postgres.to_mem_table(t)?)
            }
        })
    } else {
        let t: Arc<dyn TableProvider> = match t.extension()? {
            "csv" => csv::to_datafusion_table(t, dfctx).await?,
            "json" => Arc::new(json::to_mem_table(t).await?),
            "ndjson" | "jsonl" => Arc::new(ndjson::to_mem_table(t).await?),
            "parquet" => parquet::to_datafusion_table(t, dfctx).await?,
            "arrow" => Arc::new(arrow_ipc_file::to_mem_table(t).await?),
            "arrows" => Arc::new(arrow_ipc_stream::to_mem_table(t).await?),
            "mysql" => Arc::new(database::DatabaseLoader::MySQL.to_mem_table(t)?),
            "sqlite" => Arc::new(database::DatabaseLoader::SQLite.to_mem_table(t)?),
            "postgresql" => Arc::new(database::DatabaseLoader::Postgres.to_mem_table(t)?),
            ext => {
                return Err(ColumnQError::InvalidUri(format!(
                    "failed to register `{}` as table `{}`, unsupported table format `{}`",
                    t.io_source, t.name, ext,
                )));
            }
        };

        Ok(t)
    }
}

/// For parsing table URI arg in CLI
pub fn parse_table_uri_arg(uri_arg: &str) -> Result<TableSource, ColumnQError> {
    // separate uri from table load options
    let mut uri_args = uri_arg.split(',');

    let uri = uri_args
        .next()
        .ok_or_else(|| ColumnQError::Generic(format!("invalid table URI argument: {uri_arg}")))?;
    let split = uri.splitn(2, '=').collect::<Vec<&str>>();

    let (table_name, uri) = match split.len() {
        1 => {
            let uri = split[0];
            let table_name = match Path::new(uri).file_stem() {
                Some(s) => Ok(s),
                None => Path::new(uri)
                    .file_name()
                    .ok_or_else(|| ColumnQError::Generic(format!("invalid table URI: {uri}"))),
            }?
            .to_str()
            .ok_or_else(|| ColumnQError::Generic(format!("invalid table URI string: {uri}")))?;

            (table_name, uri)
        }
        2 => (split[0], split[1]),
        _ => unreachable!(),
    };

    let t = if uri == "stdin" {
        let mut buffer = Vec::new();
        std::io::stdin().read_to_end(&mut buffer).map_err(|e| {
            ColumnQError::Generic(format!("Failed to read table data from stdin: {e:?}"))
        })?;
        TableSource::new(table_name, TableIoSource::Memory(buffer))
    } else {
        TableSource::new(table_name, uri.to_string())
    };

    // parse extra options from table uri
    let mut option_json = serde_json::map::Map::new();
    for opt_str in uri_args {
        let mut parts = opt_str.splitn(2, '=');
        let opt_key = parts
            .next()
            .ok_or_else(|| ColumnQError::Generic(format!("invalid table option: {opt_str:?}")))?;
        let opt_value = parts
            .next()
            .ok_or_else(|| ColumnQError::Generic(format!("invalid table option: {opt_str:?}")))?;
        option_json.insert(
            opt_key.to_string(),
            serde_json::from_str(opt_value).unwrap_or_else(|_| opt_value.into()),
        );
    }

    if !option_json.is_empty() {
        let opt: TableLoadOption = serde_json::from_value(serde_json::Value::Object(option_json))
            .map_err(|e| {
            ColumnQError::Generic(format!("Failed to parse table option: {e:?}"))
        })?;
        Ok(t.with_option(opt))
    } else {
        Ok(t)
    }
}

#[derive(Deserialize, Clone, Debug, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct KeyValueSource {
    pub name: String,
    pub key: String,
    pub value: String,
    #[serde(flatten)]
    pub io_source: TableIoSource,
    pub schema: Option<TableSchema>,
    pub option: Option<TableLoadOption>,
}

impl KeyValueSource {
    pub fn new(
        name: impl Into<String>,
        source: impl Into<TableIoSource>,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            key: key.into(),
            value: value.into(),
            io_source: source.into(),
            schema: None,
            option: None,
        }
    }

    #[must_use]
    pub fn with_option(mut self, option: impl Into<TableLoadOption>) -> Self {
        self.option = Some(option.into());
        self
    }

    #[must_use]
    pub fn with_schema(mut self, schema: impl Into<TableSchema>) -> Self {
        self.schema = Some(schema.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn uri_deserialization() -> anyhow::Result<()> {
        let table_source: TableSource = serde_yaml::from_str(
            r#"
name: "ubuntu_ami"
uri: "test_data/ubuntu-ami.json"
option:
  format: "json"
  pointer: "/aaData"
  array_encoded: true
schema:
  columns:
    - name: "zone"
      data_type: "Utf8"
    - name: "name"
      data_type: "Utf8"
"#,
        )?;

        assert_eq!(
            table_source.io_source,
            TableIoSource::Uri("test_data/ubuntu-ami.json".to_string())
        );

        Ok(())
    }

    #[test]
    fn batch_size_deserialisation() -> anyhow::Result<()> {
        let table_source: TableSource = serde_yaml::from_str(
            r#"
name: "ubuntu_ami"
uri: "test_data/ubuntu-ami.json"
batch_size: 512
"#,
        )?;

        assert_eq!(table_source.batch_size, 512);

        Ok(())
    }

    #[test]
    fn test_parse_table_uri() {
        let t = parse_table_uri_arg("t=a/b/c").unwrap();
        assert_eq!(TableSource::new("t", "a/b/c"), t);

        let t = parse_table_uri_arg("t=s3://a/b/c,format=csv,has_header=true").unwrap();
        assert_eq!(
            TableSource::new("t", "s3://a/b/c").with_option(TableLoadOption::csv(
                TableOptionCsv::default().with_has_header(true),
            )),
            t
        );

        let t = parse_table_uri_arg("s3://a/b/foo.csv").unwrap();
        assert_eq!(TableSource::new("foo", "s3://a/b/foo.csv"), t);

        let t = parse_table_uri_arg("s3://a/b/foo.csv,format=csv").unwrap();
        assert_eq!(
            TableSource::new("foo", "s3://a/b/foo.csv")
                .with_option(TableLoadOption::csv(TableOptionCsv::default())),
            t
        );
    }

    #[cfg(feature = "database-sqlite")]
    #[tokio::test]
    async fn test_load_sqlite_table() -> anyhow::Result<()> {
        let t = TableSource::new("uk_cities", "sqlite://../test_data/sqlite/sample.db");
        let ctx = datafusion::prelude::SessionContext::new();
        let table = load(&t, &ctx).await?;
        let stats = table
            .scan(&ctx.state(), &None, &[], None)
            .await?
            .statistics();
        assert_eq!(stats.num_rows, Some(37));

        Ok(())
    }

    #[cfg(feature = "database-sqlite")]
    #[tokio::test]
    async fn test_load_sqlite_table_with_config() -> anyhow::Result<()> {
        for ext in vec!["db", "sqlite", "sqlite3"] {
            let t: TableSource = serde_yaml::from_str(&format!(
                r#"
name: "uk_cities"
uri: "sqlite://../test_data/sqlite/sample.{}"
"#,
                ext
            ))?;
            let ctx = datafusion::prelude::SessionContext::new();
            let table = load(&t, &ctx).await?;
            let stats = table
                .scan(&ctx.state(), &None, &[], None)
                .await?
                .statistics();
            assert_eq!(stats.num_rows, Some(37));
        }
        Ok(())
    }
}
