use crate::api::{encode_record_batches, encode_type_from_hdr};
use crate::error::ApiErrResp;
use axum::extract::{self, Extension};
use axum::http::header::HeaderMap;
use axum::response::IntoResponse;
use std::collections::HashMap;
use std::sync::Arc;

use super::HandlerCtx;

pub async fn get_table<H: HandlerCtx>(
    Extension(ctx): extract::Extension<Arc<H>>,
    headers: HeaderMap,
    extract::Path(table_name): extract::Path<String>,
    extract::Query(params): extract::Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, ApiErrResp> {
    let encode_type = encode_type_from_hdr(headers);
    let batches = ctx.query_rest_table(&table_name, &params).await?;
    encode_record_batches(encode_type, &batches)
}
