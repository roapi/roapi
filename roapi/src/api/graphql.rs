use axum::body::Bytes;
use axum::extract;
use axum::http::header::HeaderMap;
use axum::response::IntoResponse;
use std::sync::Arc;

use crate::api::{encode_record_batches, encode_type_from_hdr};
use crate::context::RoapiContext;
use crate::error::ApiErrResp;

pub async fn post<H: RoapiContext>(
    state: extract::Extension<Arc<H>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, ApiErrResp> {
    let ctx = state.0;
    let response_format = H::get_response_format(&ctx).await;
    let encode_type = encode_type_from_hdr(headers, response_format);
    let graphq = std::str::from_utf8(&body).map_err(ApiErrResp::read_query)?;
    let batches = ctx.query_graphql(graphq).await?;
    encode_record_batches(encode_type, &batches)
}
