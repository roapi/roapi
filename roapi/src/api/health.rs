use crate::api::bytes_to_json_resp;
use crate::error::ApiErrResp;
use axum::response::IntoResponse;

pub async fn health() -> Result<impl IntoResponse, ApiErrResp> {
    Ok(bytes_to_json_resp("OK".into()))
}
