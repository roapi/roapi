// Wire protocol reference:
// https://www.postgresql.org/docs/current/protocol-message-formats.html
// https://beta.pgcon.org/2014/schedule/attachments/330_postgres-for-the-wire.pdf

use async_trait::async_trait;
use std::sync::Arc;

use columnq::datafusion::dataframe::DataFrame;
use columnq::datafusion::error::DataFusionError;
use columnq::sqlparser::ast::Statement;
use convergence::engine::{Engine, Portal};
use convergence::protocol::{ErrorResponse, FieldDescription, SqlState};
use convergence::protocol_ext::DataRowBatch;
use convergence_arrow::table::{record_batch_to_rows, schema_to_field_desc};
use log::info;
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

impl<H: RoapiContext> RoapiContextEngine<H> {
    fn ignored_statement(statement: &Statement) -> bool {
        !matches!(
            statement,
            Statement::Query { .. }
                | Statement::Analyze { .. }
                | Statement::Fetch { .. }
                | Statement::ShowFunctions { .. }
                | Statement::ShowVariable { .. }
                | Statement::ShowVariables { .. }
                | Statement::ShowCollation { .. }
                | Statement::Assert { .. }
                | Statement::ExplainTable { .. }
                | Statement::Explain { .. }
                | Statement::ShowColumns { .. }
                | Statement::ShowTables { .. }
        )
    }
}

#[async_trait]
impl<H: RoapiContext> Engine for RoapiContextEngine<H> {
    type PortalType = DataFusionPortal;

    async fn prepare(
        &mut self,
        statement: &Statement,
    ) -> Result<Vec<FieldDescription>, ErrorResponse> {
        if RoapiContextEngine::<H>::ignored_statement(statement) {
            return Ok(vec![]);
        }
        let query = statement.to_string();
        info!("preparing query: {}", &query);
        let df = self.ctx.sql_to_df(&query).await.map_err(df_err_to_sql)?;
        schema_to_field_desc(&df.schema().clone().into())
    }

    async fn create_portal(
        &mut self,
        statement: &Statement,
    ) -> Result<Self::PortalType, ErrorResponse> {
        if RoapiContextEngine::<H>::ignored_statement(statement) {
            Ok(DataFusionPortal {
                df: self
                    .ctx
                    .sql_to_df("SELECT 1 WHERE 1 = 2")
                    .await
                    .map_err(df_err_to_sql)?,
            })
        } else {
            let query = statement.to_string();
            let df = self.ctx.sql_to_df(&query).await.map_err(df_err_to_sql)?;
            Ok(DataFusionPortal { df })
        }
    }
}

pub struct PostgresServer<H: RoapiContext> {
    pub ctx: Arc<H>,
    pub addr: std::net::SocketAddr,
    pub listener: TcpListener,
}

impl<H: RoapiContext> PostgresServer<H> {
    pub async fn new(ctx: Arc<H>, config: &Config, default_host: String) -> Self {
        let default_addr = format!("{default_host}:5432");

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
            addr: listener
                .local_addr()
                .expect("Failed to get address from listener"),
            listener,
        }
    }
}

#[async_trait]
impl<H: RoapiContext> RunnableServer for PostgresServer<H> {
    fn addr(&self) -> std::net::SocketAddr {
        self.addr
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
