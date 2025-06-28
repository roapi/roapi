use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use columnq::table::TableSource;
use log::{error, info};
use snafu::prelude::*;
use tokio::sync::{Mutex, RwLock};
use tokio::time;

use crate::config::Config;
use crate::context::RawRoapiContext;
use crate::context::{ConcurrentRoapiContext, RoapiContext};
use crate::server;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to build HTTP server: {source}"))]
    BuildHttpServer { source: server::http::Error },
    #[snafu(display("Failed to build FlightSQL server: {source}"))]
    BuildFlightSqlServer { source: server::flight_sql::Error },
    #[snafu(display("Failed to build context: {source}"))]
    BuildContext { source: crate::context::Error },
}

pub struct Application {
    http_addr: std::net::SocketAddr,
    http_server: server::http::HttpApiServe,
    postgres_server: Box<dyn server::RunnableServer>,
    flight_sql_server: Box<dyn server::RunnableServer>,
}

impl Application {
    pub async fn build(config: Config) -> Result<Self, Error> {
        let default_host = std::env::var("HOST").unwrap_or_else(|_| "127.0.0.1".to_string());

        let handler_ctx = RawRoapiContext::new(&config, !config.disable_read_only)
            .await
            .context(BuildContextSnafu)?;

        let tables = config
            .tables
            .iter()
            .map(|t| (t.name.clone(), t.clone()))
            .collect::<HashMap<String, TableSource>>();
        let tables = Arc::new(Mutex::new(tables));

        if config.disable_read_only {
            info!("Read-only mode disabled.");
            let ctx_ext: Arc<ConcurrentRoapiContext> = Arc::new(RwLock::new(handler_ctx));
            let postgres_server = Box::new(
                server::postgres::PostgresServer::new(
                    ctx_ext.clone(),
                    &config,
                    default_host.clone(),
                )
                .await,
            );

            let flight_sql_server = Box::new(
                server::flight_sql::RoapiFlightSqlServer::new(
                    ctx_ext.clone(),
                    &config,
                    default_host.clone(),
                )
                .await
                .context(BuildFlightSqlServerSnafu)?,
            );

            let (http_server, http_addr) =
                server::http::build_http_server(ctx_ext.clone(), tables, &config, default_host)
                    .await
                    .context(BuildHttpServerSnafu)?;

            let _handle = tokio::task::spawn(async move {
                loop {
                    if let Err(e) = ctx_ext.refresh_tables().await {
                        error!("Failed to refresh table: {e:?}");
                    }
                    time::sleep(Duration::from_millis(1000)).await;
                }
            });

            Ok(Self {
                http_addr,
                http_server,
                postgres_server,
                flight_sql_server,
            })
        } else {
            info!("Running in read-only mode.");
            let ctx_ext = Arc::new(handler_ctx);
            let postgres_server = Box::new(
                server::postgres::PostgresServer::new(
                    ctx_ext.clone(),
                    &config,
                    default_host.clone(),
                )
                .await,
            );
            let flight_sql_server = Box::new(
                server::flight_sql::RoapiFlightSqlServer::new(
                    ctx_ext.clone(),
                    &config,
                    default_host.clone(),
                )
                .await
                .context(BuildFlightSqlServerSnafu)?,
            );
            let (http_server, http_addr) = server::http::build_http_server::<RawRoapiContext>(
                ctx_ext,
                tables,
                &config,
                default_host,
            )
            .await
            .context(BuildHttpServerSnafu)?;

            Ok(Self {
                http_addr,
                http_server,
                postgres_server,
                flight_sql_server,
            })
        }
    }

    pub fn http_addr(&self) -> std::net::SocketAddr {
        self.http_addr
    }

    pub fn postgres_addr(&self) -> std::net::SocketAddr {
        self.postgres_server.addr()
    }

    pub fn flight_sql_addr(&self) -> std::net::SocketAddr {
        self.flight_sql_server.addr()
    }

    pub async fn run_until_stopped(self) -> Result<(), Error> {
        let mut handles = vec![];

        let postgres_server = self.postgres_server;
        info!(
            "🚀 Listening on {} for Postgres traffic...",
            postgres_server.addr()
        );
        handles.push(tokio::spawn(async move {
            postgres_server
                .run()
                .await
                .expect("Failed to run postgres server");
        }));

        let flight_sql_server = self.flight_sql_server;
        info!(
            "🚀 Listening on {} for FlightSQL traffic...",
            flight_sql_server.addr()
        );
        handles.push(tokio::spawn(async move {
            flight_sql_server
                .run()
                .await
                .expect("Failed to run FlightSQL server");
        }));

        info!("🚀 Listening on {} for HTTP traffic...", self.http_addr);
        #[cfg(feature = "ui")]
        info!("🚀 Serving web UI through {}/ui", self.http_addr);
        handles.push(tokio::spawn(async move {
            self.http_server.await.expect("Failed to start HTTP server");
        }));

        futures::future::join_all(handles).await;

        Ok(())
    }
}
