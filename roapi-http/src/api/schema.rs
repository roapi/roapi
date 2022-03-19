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
    let schema = ctx.schema_map().await;
    let payload = serde_json::to_vec(&schema)
        .map_err(columnq::error::ColumnQError::from)
        .map_err(ApiErrResp::json_serialization)?;
    Ok(bytes_to_json_resp(payload))
}

pub async fn get_by_table_name<H: HandlerCtx>(
    state: extract::Extension<Arc<H>>,
    extract::Path(table_name): extract::Path<String>,
) -> Result<impl IntoResponse, ApiErrResp> {
    let ctx = state.0;
    let payload = serde_json::to_vec(
        ctx.schema_map()
            .await
            .get(&table_name)
            .ok_or_else(|| ApiErrResp::not_found("invalid table name"))?
            .as_ref(),
    )
    .map_err(columnq::error::ColumnQError::from)
    .map_err(ApiErrResp::json_serialization)?;
    Ok(bytes_to_json_resp(payload))
}
