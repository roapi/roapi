use std::sync::Arc;

use axum::body::Body;
use axum::body::Bytes;
use axum::extract;
use axum::http::header::HeaderMap;
use axum::http::Response;

use crate::api::{encode_record_batches, encode_type_from_hdr, HandlerContext};
use crate::error::ApiErrResp;

pub async fn post(
    state: extract::Extension<Arc<HandlerContext>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response<Body>, ApiErrResp> {
    let ctx = state.0;
    let encode_type = encode_type_from_hdr(headers);
    let sql = std::str::from_utf8(&body).map_err(ApiErrResp::read_query)?;
    let batches = ctx.cq.query_sql(sql).await?;
    encode_record_batches(encode_type, &batches)
}
