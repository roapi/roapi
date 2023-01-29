use std::fmt;

use axum::http;
use axum::http::Response;
use columnq::datafusion::arrow;
use columnq::datafusion::parquet;
use columnq::error::ColumnQError;
use columnq::error::QueryError;
use serde::Serializer;
use serde_derive::Serialize;

#[derive(Serialize, thiserror::Error, Debug)]
pub struct ApiErrResp {
    #[serde(serialize_with = "serialize_statuscode")]
    pub code: http::StatusCode,
    pub error: String,
    pub message: String,
}

fn serialize_statuscode<S>(x: &http::StatusCode, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    s.serialize_u16(x.as_u16())
}

impl ApiErrResp {
    pub fn not_found(message: impl Into<String>) -> Self {
        Self {
            code: http::StatusCode::NOT_FOUND,
            error: "not_found".to_string(),
            message: message.into(),
        }
    }

    pub fn json_serialization(_: columnq::error::ColumnQError) -> Self {
        Self {
            code: http::StatusCode::INTERNAL_SERVER_ERROR,
            error: "json_serialization".to_string(),
            message: "Failed to serialize payload into JSON".to_string(),
        }
    }

    pub fn csv_serialization(_: arrow::error::ArrowError) -> Self {
        Self {
            code: http::StatusCode::INTERNAL_SERVER_ERROR,
            error: "csv_serialization".to_string(),
            message: "Failed to serialize record batches into CSV".to_string(),
        }
    }

    pub fn arrow_file_serialization(_: arrow::error::ArrowError) -> Self {
        Self {
            code: http::StatusCode::INTERNAL_SERVER_ERROR,
            error: "arrow_file_serialization".to_string(),
            message: "Failed to serialize record batches into arrow file".to_string(),
        }
    }

    pub fn arrow_stream_serialization(_: arrow::error::ArrowError) -> Self {
        Self {
            code: http::StatusCode::INTERNAL_SERVER_ERROR,
            error: "arrow_stream_serialization".to_string(),
            message: "Failed to serialize record batches into arrow stream".to_string(),
        }
    }

    pub fn parquet_serialization(_: parquet::errors::ParquetError) -> Self {
        Self {
            code: http::StatusCode::INTERNAL_SERVER_ERROR,
            error: "parquet_serialization".to_string(),
            message: "Failed to serialize record batches into parquet".to_string(),
        }
    }

    pub fn read_query(error: std::str::Utf8Error) -> Self {
        Self {
            code: http::StatusCode::BAD_REQUEST,
            error: "read_query".to_string(),
            message: format!("Failed to decode utf-8 query: {error}"),
        }
    }

    pub fn register_table(error: String) -> Self {
        Self {
            code: http::StatusCode::INTERNAL_SERVER_ERROR,
            error: "register_table".to_string(),
            message: error,
        }
    }

    pub fn read_only_mode() -> Self {
        Self {
            code: http::StatusCode::FORBIDDEN,
            error: "read_only_mode".to_string(),
            message: "Write operation is not allowed in read-only mode".to_string(),
        }
    }

    pub fn load_table(error: ColumnQError) -> Self {
        Self {
            code: http::StatusCode::INTERNAL_SERVER_ERROR,
            error: "load_table".to_string(),
            message: error.to_string(),
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

impl From<http::Error> for ApiErrResp {
    fn from(e: http::Error) -> Self {
        ApiErrResp {
            error: "http_error".to_string(),
            message: e.to_string(),
            code: http::StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl fmt::Display for ApiErrResp {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[{}]({}): {}", self.code, self.error, self.message)
    }
}

impl axum::response::IntoResponse for ApiErrResp {
    fn into_response(self) -> axum::response::Response {
        let payload = serde_json::to_string(&self).unwrap();
        let body = axum::body::boxed(axum::body::Full::from(payload));

        Response::builder().status(self.code).body(body).unwrap()
    }
}
