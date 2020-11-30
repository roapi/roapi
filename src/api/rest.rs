use std::collections::HashMap;

use actix_web::{web, HttpRequest, HttpResponse};
use serde_derive::Deserialize;

use crate::api::{encode_record_batches, encode_type_from_req, HandlerContext};
use crate::error::ApiErrResp;
use crate::query;

#[derive(Deserialize)]
pub struct RestTablePath {
    table_name: String,
}

pub async fn get_table(
    data: web::Data<HandlerContext>,
    path: web::Path<RestTablePath>,
    req: HttpRequest,
    query: web::Query<HashMap<String, String>>,
) -> Result<HttpResponse, ApiErrResp> {
    let encode_type = encode_type_from_req(req)?;

    let batches =
        query::rest::query_table(&data.dfctx, &path.table_name, &query.into_inner()).await?;

    encode_record_batches(encode_type, &batches)
}
