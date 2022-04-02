use crate::error::ApiErrResp;
use axum::extract::{self, Extension};
use axum::response::IntoResponse;
use std::sync::Arc;

use super::HandlerCtx;

pub async fn get<H: HandlerCtx>(
    Extension(ctx): extract::Extension<Arc<H>>,
    extract::Path((kv_name, key)): extract::Path<(String, String)>,
) -> Result<impl IntoResponse, ApiErrResp> {
    ctx.kv_get(&kv_name, &key)
        .await?
        .ok_or_else(|| ApiErrResp::not_found(format!("key {} not found", key)))
}
