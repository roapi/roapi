use actix_web::{web, HttpRequest, HttpResponse};

use crate::api::HandlerContext;
use crate::error::ApiErrResp;

pub async fn get(
    data: web::Data<HandlerContext>,
    _req: HttpRequest,
    _query: web::Bytes,
) -> Result<HttpResponse, ApiErrResp> {
    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .body(serde_json::to_vec(&data.schema_map).map_err(ApiErrResp::json_serialization)?))
}
