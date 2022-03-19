use crate::api::bytes_to_json_resp;
use crate::error::ApiErrResp;
use axum::extract;
use axum::response::IntoResponse;
use std::sync::Arc;

use super::HandlerCtx;

pub async fn schema<H: HandlerCtx>(
    state: extract::Extension<Arc<H>>,
) -> Result<impl IntoResponse, ApiErrResp> {
    let ctx = state.0;
    let payload = ctx.schemas_json_bytes().await?;
    Ok(bytes_to_json_resp(payload))
}

pub async fn get_by_table_name<H: HandlerCtx>(
    state: extract::Extension<Arc<H>>,
    extract::Path(table_name): extract::Path<&str>,
) -> Result<impl IntoResponse, ApiErrResp> {
    let ctx = state.0;
    let payload = ctx.table_schema_json_bytes(table_name).await?;
    Ok(bytes_to_json_resp(payload))
}
