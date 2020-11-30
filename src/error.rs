use std::fmt;

use actix_web::{http, HttpResponse};
use datafusion::error::DataFusionError;
use serde::Serializer;
use serde_derive::Serialize;
use uriparse::uri_reference::URIReferenceError;

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
            message: format!(
                "Failed to plan execution from SQL query: {}",
                error.to_string()
            ),
        }
    }

    pub fn invalid_sort(error: DataFusionError) -> Self {
        Self {
            error: "invalid_sort".to_string(),
            message: format!("Failed to apply sort operator: {}", error.to_string()),
        }
    }

    pub fn invalid_filter(error: DataFusionError) -> Self {
        Self {
            error: "invalid_filter".to_string(),
            message: format!("Failed to apply filter operator: {}", error.to_string()),
        }
    }

    pub fn invalid_limit(error: DataFusionError) -> Self {
        Self {
            error: "invalid_limit".to_string(),
            message: format!("Failed to apply limit operator: {}", error.to_string()),
        }
    }

    pub fn invalid_projection(error: DataFusionError) -> Self {
        Self {
            error: "invalid_projection".to_string(),
            message: format!("Failed to apply projection operator: {}", error.to_string()),
        }
    }

    pub fn query_exec(error: DataFusionError) -> Self {
        Self {
            error: "query_execution".to_string(),
            message: format!("Failed to execute query: {}", error.to_string()),
        }
    }

    pub fn invalid_table(error: DataFusionError, table_name: &str) -> Self {
        Self {
            error: "invalid_table".to_string(),
            message: format!("Failed to load table {}: {}", table_name, error.to_string()),
        }
    }
}

#[derive(Serialize, thiserror::Error, Debug)]
pub struct ApiErrResp {
    #[serde(serialize_with = "serialize_statuscode")]
    pub code: http::StatusCode,
    pub error: String,
    pub message: String,
}

impl ApiErrResp {
    pub fn json_serialization(_: serde_json::Error) -> Self {
        Self {
            code: http::StatusCode::INTERNAL_SERVER_ERROR,
            error: "json_serialization".to_string(),
            message: "Failed to serialize record batches into json".to_string(),
        }
    }

    pub fn arrow_stream_serialization(_: arrow::error::ArrowError) -> Self {
        Self {
            code: http::StatusCode::INTERNAL_SERVER_ERROR,
            error: "arrow_stream_serialization".to_string(),
            message: "Failed to serialize record batches into arrow stream".to_string(),
        }
    }

    pub fn read_query(error: std::str::Utf8Error) -> Self {
        Self {
            code: http::StatusCode::BAD_REQUEST,
            error: "read_query".to_string(),
            message: format!("Failed to decode utf-8 query: {}", error.to_string()),
        }
    }
}

impl From<QueryError> for ApiErrResp {
    fn from(e: QueryError) -> Self {
        ApiErrResp {
            error: e.error,
            message: e.message,
            code: http::StatusCode::BAD_REQUEST,
        }
    }
}

fn serialize_statuscode<S>(x: &http::StatusCode, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    s.serialize_u16(x.as_u16())
}

impl fmt::Display for ApiErrResp {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[{}]({}): {}", self.code, self.error, self.message)
    }
}

impl actix_web::error::ResponseError for ApiErrResp {
    fn status_code(&self) -> http::StatusCode {
        self.code
    }

    fn error_response(&self) -> HttpResponse {
        HttpResponse::build(self.code).json(self)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum LoadTableError {
    #[error("Invalid table URI: {0}")]
    InvalidUri(String),

    #[error("Missing required table option config")]
    MissingOption,

    #[error("Invalid format specified, expect: {0}")]
    ExpectFormatOption(String),

    #[error("Unexpected Google Spreadsheets error: {0}")]
    GoogleSpeadsheets(String),

    #[error("Failed to parse source into arrow format: {source}")]
    Arrow {
        #[from]
        source: arrow::error::ArrowError,
    },

    #[error("Failed to convert into DataFusion table: {source}")]
    DataFusion {
        #[from]
        source: datafusion::error::DataFusionError,
    },
}

impl From<URIReferenceError> for LoadTableError {
    fn from(e: URIReferenceError) -> Self {
        LoadTableError::InvalidUri(e.to_string())
    }
}
