use axum::body::Bytes;
use axum::extract;
use axum::http::header::HeaderMap;
use axum::response::IntoResponse;
use std::sync::Arc;

use crate::api::{encode_record_batches, encode_type_from_hdr};
use crate::error::ApiErrResp;

use super::HandlerCtx;

pub async fn post<H: HandlerCtx>(
    state: extract::Extension<Arc<H>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, ApiErrResp> {
    let ctx = state.0;
    let encode_type = encode_type_from_hdr(headers);
    let sql = std::str::from_utf8(&body).map_err(ApiErrResp::read_query)?;
    let batches = ctx.query_sql(sql).await?;
    encode_record_batches(encode_type, &batches)
}
