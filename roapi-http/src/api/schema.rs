use crate::api::{bytes_to_json_resp, HandlerContext};
use crate::error::ApiErrResp;
use axum::extract;
use axum::response::IntoResponse;
use std::sync::Arc;

pub async fn schema(
    state: extract::Extension<Arc<HandlerContext>>,
) -> Result<impl IntoResponse, ApiErrResp> {
    let ctx = state.0;
    let schema = ctx.cq.schema_map();
    let payload = serde_json::to_vec(&schema)
        .map_err(columnq::error::ColumnQError::from)
        .map_err(ApiErrResp::json_serialization)?;
    Ok(bytes_to_json_resp(payload))
}

pub async fn get_by_table_name(
    state: extract::Extension<Arc<HandlerContext>>,
    extract::Path(table_name): extract::Path<String>,
) -> Result<impl IntoResponse, ApiErrResp> {
    let ctx = state.0;
    let payload = serde_json::to_vec(
        ctx.cq
            .schema_map()
            .get(&table_name)
            .ok_or_else(|| ApiErrResp::not_found("invalid table name"))?
            .as_ref(),
    )
    .map_err(columnq::error::ColumnQError::from)
    .map_err(ApiErrResp::json_serialization)?;
    Ok(bytes_to_json_resp(payload))
}
