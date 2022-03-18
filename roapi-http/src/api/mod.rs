use std::collections::HashMap;
use std::convert::TryFrom;

use axum::body::Body;
use axum::http::header;
use axum::http::Response;
use axum::response::IntoResponse;
use columnq::datafusion::arrow;
use columnq::encoding;
use columnq::encoding::ContentType;
use columnq::ColumnQ;
use columnq::error::ColumnQError;
use columnq::error::QueryError;
use columnq::table::TableSource;
use log::info;
use tokio::sync::RwLock;

use crate::config::Config;
use crate::error::ApiErrResp;

pub struct HandlerContext {
    pub cq: ColumnQ,
    // TODO: store pre serialized schema in handler context
}

impl HandlerContext {
    pub async fn new(config: &Config) -> anyhow::Result<Self> {
        let mut cq = ColumnQ::new();

        if config.tables.is_empty() {
            anyhow::bail!("No table found in tables config");
        }

        for t in config.tables.iter() {
            info!("loading `{}` as table `{}`", t.io_source, t.name);
            cq.load_table(t).await?;
            info!("registered `{}` as table `{}`", t.io_source, t.name);
        }

        Ok(Self { cq })
    }
}

pub enum HandlerCtxType {
    RwLock(RwLock<HandlerContext>),
    NoLock(HandlerContext),
}

impl HandlerCtxType {
    pub async fn load_table(&self, table: &TableSource) -> Result<(), ColumnQError> {
        match self {
            HandlerCtxType::RwLock(ctx) => {
                let mut ctx = ctx.write().await;
                ctx.cq.load_table(table).await
            }
            _ => Err(ColumnQError::Generic("Current is read only".to_string())),
        }
    }

    pub async fn schema_map(&self) -> HashMap<String, arrow::datatypes::SchemaRef> {
        match self {
            HandlerCtxType::RwLock(ctx) => {
                let ctx = ctx.read().await;
                ctx.cq.schema_map().clone()
            }
            HandlerCtxType::NoLock(ctx) => ctx.cq.schema_map().clone(),
        }
    }

    pub async fn query_graphql(
        &self,
        query: &str,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>, QueryError> {
        match self {
            HandlerCtxType::RwLock(ctx) => {
                let ctx = ctx.read().await;
                ctx.cq.query_graphql(query).await
            }
            HandlerCtxType::NoLock(ctx) => ctx.cq.query_graphql(query).await,
        }
    }

    pub async fn query_sql(
        &self,
        query: &str,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>, QueryError> {
        match self {
            HandlerCtxType::RwLock(ctx) => {
                let ctx = ctx.read().await;
                ctx.cq.query_sql(query).await
            }
            HandlerCtxType::NoLock(ctx) => ctx.cq.query_sql(query).await,
        }
    }

    pub async fn query_rest_table(
        &self,
        table_name: &str,
        params: &HashMap<String, String>,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>, QueryError> {
        match self {
            HandlerCtxType::RwLock(ctx) => {
                let ctx = ctx.read().await;
                ctx.cq.query_rest_table(table_name, params).await
            }
            HandlerCtxType::NoLock(ctx) => ctx.cq.query_rest_table(table_name, params).await,
        }
    }
}

#[inline]
pub fn bytes_to_resp(bytes: Vec<u8>, content_type: &'static str) -> impl IntoResponse {
    let mut res = Response::new(Body::from(bytes));
    res.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static(content_type),
    );
    res
}

#[inline]
pub fn bytes_to_json_resp(bytes: Vec<u8>) -> impl IntoResponse {
    bytes_to_resp(bytes, "application/json")
}

pub fn encode_type_from_hdr(headers: header::HeaderMap) -> encoding::ContentType {
    match headers.get(header::ACCEPT) {
        None => encoding::ContentType::Json,
        Some(hdr_value) => {
            encoding::ContentType::try_from(hdr_value.as_bytes()).unwrap_or(ContentType::Json)
        }
    }
}

pub fn encode_record_batches(
    content_type: encoding::ContentType,
    batches: &[arrow::record_batch::RecordBatch],
) -> Result<impl IntoResponse, ApiErrResp> {
    let payload = match content_type {
        encoding::ContentType::Json => encoding::json::record_batches_to_bytes(batches)
            .map_err(ApiErrResp::json_serialization)?,
        encoding::ContentType::Csv => encoding::csv::record_batches_to_bytes(batches)
            .map_err(ApiErrResp::csv_serialization)?,
        encoding::ContentType::ArrowFile => encoding::arrow::record_batches_to_file_bytes(batches)
            .map_err(ApiErrResp::arrow_file_serialization)?,
        encoding::ContentType::ArrowStream => {
            encoding::arrow::record_batches_to_stream_bytes(batches)
                .map_err(ApiErrResp::arrow_stream_serialization)?
        }
        encoding::ContentType::Parquet => encoding::parquet::record_batches_to_bytes(batches)
            .map_err(ApiErrResp::parquet_serialization)?,
    };

    Ok(bytes_to_resp(payload, content_type.to_str()))
}

pub mod graphql;
pub mod rest;
pub mod routes;
pub mod schema;
pub mod sql;
pub mod register;

pub use routes::register_app_routes;
