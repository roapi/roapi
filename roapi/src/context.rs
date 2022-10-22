use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use columnq::datafusion::arrow;
use columnq::datafusion::dataframe::DataFrame;
use columnq::datafusion::error::DataFusionError;
use columnq::encoding;
use columnq::error::ColumnQError;
use columnq::error::QueryError;
use columnq::table::TableSource;
use columnq::ColumnQ;
use log::info;
use tokio::sync::RwLock;

use crate::config::Config;
use crate::error::ApiErrResp;

pub struct RawRoapiContext {
    pub cq: ColumnQ,
    pub response_format: encoding::ContentType,
    // TODO: store pre serialized schema in handler context
}

impl RawRoapiContext {
    pub async fn new(config: &Config) -> anyhow::Result<Self> {
        let mut cq = ColumnQ::new();

        if config.tables.is_empty() && config.kvstores.is_empty() {
            anyhow::bail!("No table nor kvstore found in config");
        }

        for t in config.tables.iter() {
            info!("loading `{}` as table `{}`", t.io_source, t.name);
            cq.load_table(t).await?;
            info!("registered `{}` as table `{}`", t.io_source, t.name);
        }

        for k in config.kvstores.iter() {
            info!("loading `{}` as kv store `{}`", k.io_source, k.name);
            cq.load_kv(k.clone()).await?;
            info!("registered `{}` as kv store `{}`", k.io_source, k.name);
        }

        Ok(Self {
            cq,
            response_format: config.response_format,
        })
    }
}

pub type ConcurrentRoapiContext = RwLock<RawRoapiContext>;

#[async_trait]
pub trait RoapiContext: Send + Sync + 'static {
    fn read_only_mode() -> bool;

    async fn load_table(&self, table: &TableSource) -> Result<(), ColumnQError>;

    async fn schemas_json_bytes(&self) -> Result<Vec<u8>, ApiErrResp>;

    async fn table_schema_json_bytes(&self, table_name: &str) -> Result<Vec<u8>, ApiErrResp>;

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

    async fn kv_get(&self, kv_name: &str, key: &str) -> Result<Option<String>, QueryError>;

    async fn sql_to_df(&self, query: &str) -> Result<Arc<DataFrame>, DataFusionError>;

    async fn get_response_format(&self) -> encoding::ContentType;
}

#[async_trait]
impl RoapiContext for RawRoapiContext {
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
    async fn schemas_json_bytes(&self) -> Result<Vec<u8>, ApiErrResp> {
        serde_json::to_vec(self.cq.schema_map())
            .map_err(columnq::error::ColumnQError::from)
            .map_err(ApiErrResp::json_serialization)
    }

    #[inline]
    async fn table_schema_json_bytes(&self, table_name: &str) -> Result<Vec<u8>, ApiErrResp> {
        serde_json::to_vec(
            self.cq
                .schema_map()
                .get(table_name)
                .ok_or_else(|| ApiErrResp::not_found("invalid table name"))?
                .as_ref(),
        )
        .map_err(columnq::error::ColumnQError::from)
        .map_err(ApiErrResp::json_serialization)
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

    #[inline]
    async fn kv_get(&self, kv_name: &str, key: &str) -> Result<Option<String>, QueryError> {
        Ok(self.cq.kv_get(kv_name, key)?.cloned())
    }

    #[inline]
    async fn sql_to_df(&self, query: &str) -> Result<Arc<DataFrame>, DataFusionError> {
        self.cq.dfctx.sql(query).await
    }

    #[inline]
    async fn get_response_format(&self) -> encoding::ContentType {
        self.response_format
    }
}

#[async_trait]
impl RoapiContext for ConcurrentRoapiContext {
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
    async fn schemas_json_bytes(&self) -> Result<Vec<u8>, ApiErrResp> {
        let ctx = self.read().await;
        serde_json::to_vec(ctx.cq.schema_map())
            .map_err(columnq::error::ColumnQError::from)
            .map_err(ApiErrResp::json_serialization)
    }

    #[inline]
    async fn table_schema_json_bytes(&self, table_name: &str) -> Result<Vec<u8>, ApiErrResp> {
        let ctx = self.read().await;
        serde_json::to_vec(
            ctx.cq
                .schema_map()
                .get(table_name)
                .ok_or_else(|| ApiErrResp::not_found("invalid table name"))?
                .as_ref(),
        )
        .map_err(columnq::error::ColumnQError::from)
        .map_err(ApiErrResp::json_serialization)
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

    #[inline]
    async fn kv_get(&self, kv_name: &str, key: &str) -> Result<Option<String>, QueryError> {
        let ctx = self.read().await;
        Ok(ctx.cq.kv_get(kv_name, key)?.cloned())
    }

    #[inline]
    async fn sql_to_df(&self, query: &str) -> Result<Arc<DataFrame>, DataFusionError> {
        let ctx = self.read().await;
        ctx.cq.dfctx.sql(query).await
    }

    #[inline]
    async fn get_response_format(&self) -> encoding::ContentType {
        let ctx = self.read().await;
        ctx.response_format
    }
}
