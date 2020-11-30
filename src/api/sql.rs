use actix_web::{web, HttpRequest, HttpResponse};

use crate::api::{encode_record_batches, encode_type_from_req, HandlerContext};
use crate::error::ApiErrResp;
use crate::query;

pub async fn post(
    data: web::Data<HandlerContext>,
    req: HttpRequest,
    query: web::Bytes,
) -> Result<HttpResponse, ApiErrResp> {
    let encode_type = encode_type_from_req(req)?;

    let sql = std::str::from_utf8(&query).map_err(ApiErrResp::read_query)?;
    let batches = query::sql::query(&data.dfctx, &sql).await?;

    encode_record_batches(encode_type, &batches)
}
