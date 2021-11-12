use std::collections::HashMap;
use std::sync::Arc;

use axum::body::Body;
use axum::extract;
use axum::http::header::HeaderMap;
use axum::http::Response;

use crate::api::HandlerContext;
use crate::api::{encode_record_batches, encode_type_from_hdr};
use crate::error::ApiErrResp;

pub async fn get_table(
    state: extract::Extension<Arc<HandlerContext>>,
    headers: HeaderMap,
    extract::Path(table_name): extract::Path<String>,
    extract::Query(params): extract::Query<HashMap<String, String>>,
) -> Result<Response<Body>, ApiErrResp> {
    let ctx = &state.0;
    let encode_type = encode_type_from_hdr(headers);
    let batches = ctx.cq.query_rest_table(&table_name, &params).await?;
    encode_record_batches(encode_type, &batches)
}
