use crate::api::HandlerContext;
use crate::api::{encode_record_batches, encode_type_from_hdr};
use crate::error::ApiErrResp;
use axum::extract::{self, Extension};
use axum::http::header::HeaderMap;
use axum::response::IntoResponse;
use tokio::sync::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

pub async fn get_table(
    Extension(state): extract::Extension<Arc<RwLock<HandlerContext>>>,
    headers: HeaderMap,
    extract::Path(table_name): extract::Path<String>,
    extract::Query(params): extract::Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, ApiErrResp> {
    let ctx = &state.read().await;
    let encode_type = encode_type_from_hdr(headers);
    let batches = ctx.cq.query_rest_table(&table_name, &params).await?;
    encode_record_batches(encode_type, &batches)
}
