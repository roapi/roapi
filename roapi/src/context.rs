use std::collections::HashMap;

use async_trait::async_trait;
use columnq::arrow::record_batch::RecordBatch;
use columnq::datafusion::arrow;
use columnq::datafusion::dataframe::DataFrame;
use columnq::datafusion::error::DataFusionError;
use columnq::datafusion::prelude::SessionContext;
use columnq::encoding;
use columnq::error::ColumnQError;
use columnq::error::QueryError;
use columnq::table::TableSource;
use columnq::ColumnQ;
use jsonwebtoken::{decode, DecodingKey, Validation, Algorithm, TokenData, errors::Error as JwtError};
use log::info;
use snafu::prelude::*;
use tokio::sync::RwLock;

use crate::config::Config;
use crate::error::ApiErrResp;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("No table nor kvstore found in config"))]
    NoData,
    #[snafu(display("Failed to load table: {source}"))]
    LoadTable { source: ColumnQError },
    #[snafu(display("Failed to load kvstore: {source}"))]
    LoadKvstore { source: ColumnQError },
}

pub struct RawRoapiContext {
    pub cq: ColumnQ,
    pub response_format: encoding::ContentType,
    pub jwt_secret: String,
    // TODO: store pre serialized schema in handler context
}

impl RawRoapiContext {
    pub async fn new(config: &Config, read_only: bool, jwt_secret: String) -> Result<Self, Error> {
        let mut cq = match config.get_datafusion_config() {
            Ok(df_cfg) => ColumnQ::new_with_config(df_cfg, read_only, config.reload_interval),
            _ => ColumnQ::new_with_read_only(read_only, config.reload_interval),
        };

        if config.tables.is_empty() && config.kvstores.is_empty() {
            return Err(Error::NoData);
        }

        for t in config.tables.iter() {
            info!("loading `{}` as table `{}`", t.io_source, t.name);
            cq.load_table(t).await.context(LoadTableSnafu)?;
            info!("registered `{}` as table `{}`", t.io_source, t.name);
        }

        for k in config.kvstores.iter() {
            info!("loading `{}` as kv store `{}`", k.io_source, k.name);
            cq.load_kv(k.clone()).await.context(LoadKvstoreSnafu)?;
            info!("registered `{}` as kv store `{}`", k.io_source, k.name);
        }

        Ok(Self {
            cq,
            response_format: config.response_format,
            jwt_secret,
        })
    }

    pub fn authorize_jwt(&self, token: &str) -> Result<TokenData<Claims>, JwtError> {
        decode::<Claims>(
            token,
            &DecodingKey::from_secret(self.jwt_secret.as_ref()),
            &Validation::new(Algorithm::HS256),
        )
    }
}

pub type ConcurrentRoapiContext = RwLock<RawRoapiContext>;

#[async_trait]
pub trait RoapiContext: Send + Sync + 'static {
    fn read_only_mode() -> bool;

    async fn load_table(&self, table: &TableSource) -> Result<(), ColumnQError>;

    async fn drop_table(&self, table: &TableSource) -> Result<(), ColumnQError>;

    async fn schemas(&self) -> Result<Vec<(String, arrow::datatypes::SchemaRef)>, ApiErrResp>;

    async fn schemas_json_bytes(&self) -> Result<Vec<u8>, ApiErrResp>;

    async fn table_names(&self) -> Vec<String>;

    async fn table_schema(
        &self,
        table_name: &str,
    ) -> Result<arrow::datatypes::SchemaRef, ApiErrResp>;

    async fn table_schema_json_bytes(&self, table_name: &str) -> Result<Vec<u8>, ApiErrResp>;

    async fn query_graphql(&self, query: &str) -> Result<Vec<RecordBatch>, QueryError>;

    async fn query_sql(&self, query: &str) -> Result<Vec<RecordBatch>, QueryError>;

    async fn query_rest_table(
        &self,
        table_name: &str,
        params: &HashMap<String, String>,
    ) -> Result<Vec<RecordBatch>, QueryError>;

    async fn kv_get(&self, kv_name: &str, key: &str) -> Result<Option<String>, QueryError>;

    async fn sql_to_df(&self, query: &str) -> Result<DataFrame, DataFusionError>;

    async fn get_response_format(&self) -> encoding::ContentType;

    async fn get_dfctx(&self) -> SessionContext;

    async fn refresh_tables(&self) -> Result<(), ColumnQError>;
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
    async fn drop_table(&self, _table: &TableSource) -> Result<(), ColumnQError> {
        Err(ColumnQError::Generic(
            "Table update not supported in read only mode".to_string(),
        ))
    }

    #[inline]
    async fn schemas(&self) -> Result<Vec<(String, arrow::datatypes::SchemaRef)>, ApiErrResp> {
        Ok(self
            .cq
            .schema_map()
            .iter()
            .map(|(table_name, schema)| (table_name.to_string(), schema.clone()))
            .collect())
    }

    #[inline]
    async fn schemas_json_bytes(&self) -> Result<Vec<u8>, ApiErrResp> {
        serde_json::to_vec(self.cq.schema_map())
            .map_err(columnq::error::ColumnQError::from)
            .map_err(ApiErrResp::json_serialization)
    }

    #[inline]
    async fn table_names(&self) -> Vec<String> {
        self.cq.schema_map().keys().map(|s| s.to_string()).collect()
    }

    #[inline]
    async fn table_schema(
        &self,
        table_name: &str,
    ) -> Result<arrow::datatypes::SchemaRef, ApiErrResp> {
        Ok(self
            .cq
            .schema_map()
            .get(table_name)
            .ok_or_else(|| ApiErrResp::not_found("invalid table name"))?
            .clone())
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
    async fn query_graphql(&self, query: &str) -> Result<Vec<RecordBatch>, QueryError> {
        self.cq.query_graphql(query).await
    }

    #[inline]
    async fn query_sql(&self, query: &str) -> Result<Vec<RecordBatch>, QueryError> {
        self.cq.query_sql(query).await
    }

    #[inline]
    async fn query_rest_table(
        &self,
        table_name: &str,
        params: &HashMap<String, String>,
    ) -> Result<Vec<RecordBatch>, QueryError> {
        self.cq.query_rest_table(table_name, params).await
    }

    #[inline]
    async fn kv_get(&self, kv_name: &str, key: &str) -> Result<Option<String>, QueryError> {
        Ok(self.cq.kv_get(kv_name, key)?.cloned())
    }

    #[inline]
    async fn sql_to_df(&self, query: &str) -> Result<DataFrame, DataFusionError> {
        self.cq.dfctx.sql(query).await
    }

    #[inline]
    async fn get_response_format(&self) -> encoding::ContentType {
        self.response_format
    }

    #[inline]
    async fn get_dfctx(&self) -> SessionContext {
        self.cq.dfctx.clone()
    }

    #[inline]
    async fn refresh_tables(&self) -> Result<(), ColumnQError> {
        Err(ColumnQError::Generic(
            "Table refresh not supported in read only mode".to_string(),
        ))
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
    async fn drop_table(&self, table: &TableSource) -> Result<(), ColumnQError> {
        let mut ctx = self.write().await;
        ctx.cq.drop_table(table).await
    }

    #[inline]
    async fn table_names(&self) -> Vec<String> {
        let ctx = self.read().await;
        ctx.cq.schema_map().keys().map(|s| s.to_string()).collect()
    }

    #[inline]
    async fn table_schema(
        &self,
        table_name: &str,
    ) -> Result<arrow::datatypes::SchemaRef, ApiErrResp> {
        let ctx = self.read().await;
        Ok(ctx
            .cq
            .schema_map()
            .get(table_name)
            .ok_or_else(|| ApiErrResp::not_found("invalid table name"))?
            .clone())
    }

    #[inline]
    async fn schemas(&self) -> Result<Vec<(String, arrow::datatypes::SchemaRef)>, ApiErrResp> {
        let ctx = self.read().await;
        Ok(ctx
            .cq
            .schema_map()
            .iter()
            .map(|(table_name, schema)| (table_name.to_string(), schema.clone()))
            .collect())
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
    async fn query_graphql(&self, query: &str) -> Result<Vec<RecordBatch>, QueryError> {
        let ctx = self.read().await;
        ctx.cq.query_graphql(query).await
    }

    #[inline]
    async fn query_sql(&self, query: &str) -> Result<Vec<RecordBatch>, QueryError> {
        let ctx = self.read().await;
        ctx.cq.query_sql(query).await
    }

    #[inline]
    async fn query_rest_table(
        &self,
        table_name: &str,
        params: &HashMap<String, String>,
    ) -> Result<Vec<RecordBatch>, QueryError> {
        let ctx = self.read().await;
        ctx.cq.query_rest_table(table_name, params).await
    }

    #[inline]
    async fn kv_get(&self, kv_name: &str, key: &str) -> Result<Option<String>, QueryError> {
        let ctx = self.read().await;
        Ok(ctx.cq.kv_get(kv_name, key)?.cloned())
    }

    #[inline]
    async fn sql_to_df(&self, query: &str) -> Result<DataFrame, DataFusionError> {
        let ctx = self.read().await;
        ctx.cq.dfctx.sql(query).await
    }

    #[inline]
    async fn get_response_format(&self) -> encoding::ContentType {
        let ctx = self.read().await;
        ctx.response_format
    }

    #[inline]
    async fn get_dfctx(&self) -> SessionContext {
        let ctx = self.read().await;
        ctx.cq.dfctx.clone()
    }

    #[inline]
    async fn refresh_tables(&self) -> Result<(), ColumnQError> {
        let mut ctx = self.write().await;
        ctx.cq.refresh_tables().await
    }
}
