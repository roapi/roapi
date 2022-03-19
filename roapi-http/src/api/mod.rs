use std::collections::HashMap;
use std::convert::TryFrom;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::header;
use axum::http::Response;
use axum::response::IntoResponse;
use columnq::datafusion::arrow;
use columnq::encoding;
use columnq::encoding::ContentType;
use columnq::error::ColumnQError;
use columnq::error::QueryError;
use columnq::table::TableSource;
use columnq::ColumnQ;
use log::info;
use tokio::sync::RwLock;

use crate::config::Config;
use crate::error::ApiErrResp;

pub struct RawHandlerContext {
    pub cq: ColumnQ,
    // TODO: store pre serialized schema in handler context
}

impl RawHandlerContext {
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

pub type ConcurrentHandlerContext = RwLock<RawHandlerContext>;

#[async_trait]
pub trait HandlerCtx: Send + Sync + 'static {
    fn read_only_mode() -> bool;

    async fn load_table(&self, table: &TableSource) -> Result<(), ColumnQError>;
    // FIXME: avoid clone
    async fn schema_map(&self) -> HashMap<String, arrow::datatypes::SchemaRef>;
    async fn query_graphql(
        &self,
        query: &str,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>, QueryError>;
    async fn query_sql(
        &self,
        query: &str,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>, QueryError>;
    async fn query_rest_table(
        &self,
        table_name: &str,
        params: &HashMap<String, String>,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>, QueryError>;
}

#[async_trait]
impl HandlerCtx for RawHandlerContext {
    #[inline]
    fn read_only_mode() -> bool {
        true
    }

    #[inline]
    async fn load_table(&self, _table: &TableSource) -> Result<(), ColumnQError> {
        Err(ColumnQError::Generic(
            "Table update not supported in read only mode".to_string(),
        ))
    }

    #[inline]
    async fn schema_map(&self) -> HashMap<String, arrow::datatypes::SchemaRef> {
        self.cq.schema_map().clone()
    }

    #[inline]
    async fn query_graphql(
        &self,
        query: &str,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>, QueryError> {
        self.cq.query_graphql(query).await
    }

    #[inline]
    async fn query_sql(
        &self,
        query: &str,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>, QueryError> {
        self.cq.query_sql(query).await
    }

    #[inline]
    async fn query_rest_table(
        &self,
        table_name: &str,
        params: &HashMap<String, String>,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>, QueryError> {
        self.cq.query_rest_table(table_name, params).await
    }
}

#[async_trait]
impl HandlerCtx for ConcurrentHandlerContext {
    #[inline]
    fn read_only_mode() -> bool {
        false
    }

    #[inline]
    async fn load_table(&self, table: &TableSource) -> Result<(), ColumnQError> {
        let mut ctx = self.write().await;
        ctx.cq.load_table(table).await
    }

    #[inline]
    async fn schema_map(&self) -> HashMap<String, arrow::datatypes::SchemaRef> {
        let ctx = self.read().await;
        ctx.cq.schema_map().clone()
    }

    #[inline]
    async fn query_graphql(
        &self,
        query: &str,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>, QueryError> {
        let ctx = self.read().await;
        ctx.cq.query_graphql(query).await
    }

    #[inline]
    async fn query_sql(
        &self,
        query: &str,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>, QueryError> {
        let ctx = self.read().await;
        ctx.cq.query_sql(query).await
    }

    #[inline]
    async fn query_rest_table(
        &self,
        table_name: &str,
        params: &HashMap<String, String>,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>, QueryError> {
        let ctx = self.read().await;
        ctx.cq.query_rest_table(table_name, params).await
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
pub mod register;
pub mod rest;
pub mod routes;
pub mod schema;
pub mod sql;

pub use routes::register_app_routes;
