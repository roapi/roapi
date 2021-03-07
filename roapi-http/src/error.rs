use std::fmt;

use actix_web::{http, HttpResponse};
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

impl ApiErrResp {
    pub fn not_found(message: &str) -> Self {
        Self {
            code: http::StatusCode::NOT_FOUND,
            error: "not_found".to_string(),
            message: message.to_string(),
        }
    }

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
