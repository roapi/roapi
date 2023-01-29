use std::fmt;

use datafusion::error::DataFusionError;
use datafusion::{arrow, parquet};
use uriparse::uri_reference::URIReferenceError;

#[derive(thiserror::Error, Debug)]
pub enum ColumnQError {
    #[error("Invalid table URI: {0}")]
    InvalidUri(String),

    #[error("Missing required table option config")]
    MissingOption,

    #[error("Invalid format specified, expect: {0}")]
    ExpectFormatOption(String),

    #[error("Unexpected Google Spreadsheets error: {0}")]
    GoogleSpreadsheets(String),

    #[error("Error loading JSON: {0}")]
    LoadJson(String),

    #[error("Error loading CSV: {0}")]
    LoadCsv(String),

    #[error("Error loading Parquet: {0}")]
    LoadParquet(String),

    #[error("Error loading Delta table: {0}")]
    LoadDelta(String),

    #[error("Error loading Xlsx table: {0}")]
    LoadXlsx(String),

    #[error("Error loading data from HTTP store: {0}")]
    HttpStore(String),

    #[error("Error loading data from file store: {0}")]
    FileStore(String),

    #[error("Error loading data from S3 store: {0}")]
    S3Store(String),

    #[error("DataFusion error: {source}")]
    ObjectStore {
        #[from]
        source: object_store::Error,
    },

    #[error("DeltaTable error: {source}")]
    DeltaTable {
        #[from]
        source: deltalake::DeltaTableError,
    },

    #[error("Arrow error: {source}")]
    Arrow {
        #[from]
        source: arrow::error::ArrowError,
    },

    #[error("DataFusion error: {source}")]
    DataFusion {
        #[from]
        source: datafusion::error::DataFusionError,
    },

    #[error("Parquet error: {source}")]
    Parquet {
        #[from]
        source: datafusion::parquet::errors::ParquetError,
    },

    #[error("Serde error: {source}")]
    Serde {
        #[from]
        source: serde_json::Error,
    },

    #[error("Generic error: {0}")]
    Generic(String),

    #[error("Database error: {0}")]
    Database(String),
}

impl ColumnQError {
    pub fn open_parquet_file(e: std::io::Error) -> Self {
        Self::LoadParquet(format!("Failed to open file: {e}"))
    }

    pub fn parquet_record_reader(e: parquet::errors::ParquetError) -> Self {
        Self::LoadParquet(format!("Failed to create record reader: {e}"))
    }

    pub fn parquet_file_reader(e: parquet::errors::ParquetError) -> Self {
        Self::LoadParquet(format!("Failed to create file reader: {e}"))
    }

    pub fn json_parse(e: serde_json::Error) -> Self {
        Self::LoadJson(format!("Failed to parse JSON data: {e}"))
    }

    pub fn s3_obj_missing_key() -> Self {
        Self::S3Store("Missing key in S3 object list item".to_string())
    }

    pub fn invalid_kv_key_type() -> Self {
        Self::Generic("keyvalue store key datatype should be a string".to_string())
    }
}

impl From<URIReferenceError> for ColumnQError {
    fn from(e: URIReferenceError) -> Self {
        ColumnQError::InvalidUri(e.to_string())
    }
}

#[derive(thiserror::Error, Debug)]
pub struct QueryError {
    pub error: String,
    pub message: String,
}

impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}: {}", self.error, self.message)
    }
}

impl QueryError {
    pub fn plan_sql(error: DataFusionError) -> Self {
        Self {
            error: "plan_sql".to_string(),
            message: format!("Failed to plan execution from SQL query: {error}"),
        }
    }

    pub fn invalid_sort(error: DataFusionError) -> Self {
        Self {
            error: "invalid_sort".to_string(),
            message: format!("Failed to apply sort operator: {error}"),
        }
    }

    pub fn invalid_filter(error: DataFusionError) -> Self {
        Self {
            error: "invalid_filter".to_string(),
            message: format!("Failed to apply filter operator: {error}"),
        }
    }

    pub fn invalid_limit(error: DataFusionError) -> Self {
        Self {
            error: "invalid_limit".to_string(),
            message: format!("Failed to apply limit operator: {error}"),
        }
    }

    pub fn invalid_projection(error: DataFusionError) -> Self {
        Self {
            error: "invalid_projection".to_string(),
            message: format!("Failed to apply projection operator: {error}"),
        }
    }

    pub fn query_exec(error: DataFusionError) -> Self {
        Self {
            error: "query_execution".to_string(),
            message: format!("Failed to execute query: {error}"),
        }
    }

    pub fn invalid_table(error: DataFusionError, table_name: &str) -> Self {
        Self {
            error: "invalid_table".to_string(),
            message: format!("Failed to load table {table_name}: {error}"),
        }
    }

    pub fn invalid_kv_name(kv_name: &str) -> Self {
        Self {
            error: "invalid_kv_name".to_string(),
            message: format!("keyvalue store name `{kv_name}` doesn't exist"),
        }
    }
}
