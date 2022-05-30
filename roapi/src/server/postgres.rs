use async_trait::async_trait;
use std::sync::Arc;

use columnq::datafusion::dataframe::DataFrame;
use columnq::datafusion::error::DataFusionError;
use convergence::engine::{Engine, Portal};
use convergence::protocol::{ErrorResponse, FieldDescription, SqlState};
use convergence::protocol_ext::DataRowBatch;
use convergence_arrow::table::{record_batch_to_rows, schema_to_field_desc};
use sqlparser::ast::Statement;
use tokio::net::TcpListener;

use crate::config::Config;
use crate::context::RoapiContext;
use crate::server::RunnableServer;

fn df_err_to_sql(err: DataFusionError) -> ErrorResponse {
    ErrorResponse::error(SqlState::DATA_EXCEPTION, err.to_string())
}

/// A portal built using a logical DataFusion query plan.
pub struct DataFusionPortal {
    df: Arc<DataFrame>,
}

#[async_trait]
impl Portal for DataFusionPortal {
    async fn fetch(&mut self, batch: &mut DataRowBatch) -> Result<(), ErrorResponse> {
        for arrow_batch in self.df.collect().await.map_err(df_err_to_sql)? {
            record_batch_to_rows(&arrow_batch, batch)?;
        }
        Ok(())
    }
}

pub struct RoapiContextEngine<H: RoapiContext> {
    pub ctx: Arc<H>,
}

#[async_trait]
impl<H: RoapiContext> Engine for RoapiContextEngine<H> {
    type PortalType = DataFusionPortal;

    async fn prepare(
        &mut self,
        statement: &Statement,
    ) -> Result<Vec<FieldDescription>, ErrorResponse> {
        let query = statement.to_string();
        let df = self.ctx.sql_to_df(&query).await.map_err(df_err_to_sql)?;
        schema_to_field_desc(&df.schema().clone().into())
    }

    async fn create_portal(
        &mut self,
        statement: &Statement,
    ) -> Result<Self::PortalType, ErrorResponse> {
        let query = statement.to_string();
        let df = self.ctx.sql_to_df(&query).await.map_err(df_err_to_sql)?;
        Ok(DataFusionPortal { df })
    }
}

pub struct PostgresServer<H: RoapiContext> {
    pub ctx: Arc<H>,
    pub port: u16,
    pub listener: TcpListener,
}

impl<H: RoapiContext> PostgresServer<H> {
    pub async fn new(ctx: Arc<H>, config: &Config, default_host: String) -> Self {
        let default_addr = format!("{}:5432", default_host);

        let addr = config
            .addr
            .postgres
            .clone()
            .unwrap_or_else(|| default_addr.to_string());

        let listener = TcpListener::bind(addr)
            .await
            .expect("Failed to bind address for Postgres server");
        Self {
            ctx,
            port: listener
                .local_addr()
                .expect("Failed to get address from listener")
                .port(),
            listener,
        }
    }
}

#[async_trait]
impl<H: RoapiContext> RunnableServer for PostgresServer<H> {
    fn port(&self) -> u16 {
        self.port
    }

    async fn run(&self) -> anyhow::Result<()> {
        use convergence::connection::Connection;

        loop {
            let (stream, _) = self.listener.accept().await?;
            let engine = RoapiContextEngine {
                ctx: self.ctx.clone(),
            };
            tokio::spawn(async move {
                let mut conn = Connection::new(engine);
                conn.run(stream).await.unwrap();
            });
        }
    }
}
