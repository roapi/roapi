use std::collections::HashMap;

use actix_web::{web, HttpRequest, HttpResponse};
use serde_derive::Deserialize;

use crate::api::HandlerContext;
use crate::error::ApiErrResp;

pub async fn get(
    data: web::Data<HandlerContext>,
    _req: HttpRequest,
    _query: web::Bytes,
) -> Result<HttpResponse, ApiErrResp> {
    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .body(serde_json::to_vec(data.cq.schema_map()).map_err(ApiErrResp::json_serialization)?))
}

#[derive(Deserialize)]
pub struct SchemaTablePath {
    table_name: String,
}

pub async fn get_by_table_name(
    data: web::Data<HandlerContext>,
    path: web::Path<SchemaTablePath>,
    _req: HttpRequest,
    _query: web::Query<HashMap<String, String>>,
) -> Result<HttpResponse, ApiErrResp> {
    Ok(HttpResponse::Ok().content_type("application/json").body(
        serde_json::to_vec(
            data.cq
                .schema_map()
                .get(&path.table_name)
                .ok_or_else(|| ApiErrResp::not_found("invalid table name"))?,
        )
        .map_err(ApiErrResp::json_serialization)?,
    ))
}
