use std::convert::TryFrom;

use axum::body::Body;
use axum::http::header;
use axum::http::Response;
use axum::response::IntoResponse;
use columnq::datafusion::arrow;
use columnq::encoding;

use crate::error::ApiErrResp;

#[inline]
pub fn bytes_to_resp(bytes: Vec<u8>, content_type: &'static str) -> impl IntoResponse {
    let mut res = Response::new(Body::from(bytes));
    res.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static(content_type),
    );
    res
}

#[inline]
pub fn bytes_to_json_resp(bytes: Vec<u8>) -> impl IntoResponse {
    bytes_to_resp(bytes, "application/json")
}

pub fn encode_type_from_hdr(
    headers: header::HeaderMap,
    response_format: encoding::ContentType,
) -> encoding::ContentType {
    match headers.get(header::ACCEPT) {
        None => response_format,
        Some(hdr_value) => {
            encoding::ContentType::try_from(hdr_value.as_bytes()).unwrap_or(response_format)
        }
    }
}

pub fn encode_record_batches(
    content_type: encoding::ContentType,
    batches: &[arrow::record_batch::RecordBatch],
) -> Result<impl IntoResponse, ApiErrResp> {
    let payload = match content_type {
        encoding::ContentType::Json => encoding::json::record_batches_to_bytes(batches)
            .map_err(ApiErrResp::json_serialization)?,
        encoding::ContentType::Csv => encoding::csv::record_batches_to_bytes(batches)
            .map_err(ApiErrResp::csv_serialization)?,
        encoding::ContentType::ArrowFile => encoding::arrow::record_batches_to_file_bytes(batches)
            .map_err(ApiErrResp::arrow_file_serialization)?,
        encoding::ContentType::ArrowStream => {
            encoding::arrow::record_batches_to_stream_bytes(batches)
                .map_err(ApiErrResp::arrow_stream_serialization)?
        }
        encoding::ContentType::Parquet => encoding::parquet::record_batches_to_bytes(batches)
            .map_err(ApiErrResp::parquet_serialization)?,
    };

    Ok(bytes_to_resp(payload, content_type.to_str()))
}

pub mod graphql;
pub mod kv;
pub mod register;
pub mod rest;
pub mod routes;
pub mod schema;
pub mod sql;

pub use routes::register_app_routes;
