use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use columnq::{error::ColumnQError, table::TableSource};
use log::info;
use tokio::sync::{Mutex, RwLock};
use tokio::{task, time};

use crate::config::Config;
use crate::context::{ConcurrentRoapiContext, RoapiContext};
use crate::context::RawRoapiContext;
use crate::error::ApiErrResp;
use crate::server;

pub struct Application {
    http_addr: std::net::SocketAddr,
    http_server: server::http::HttpApiServer,
    postgres_server: Box<dyn server::RunnableServer>,
    max_age: Option<Duration>,
    tables: Arc<Mutex<HashMap<String, TableSource>>>,
    handler_ctx: Option<Arc<RwLock<RawRoapiContext>>>,
}

impl Application {
    pub async fn build(config: Config) -> anyhow::Result<Self> {
        let default_host = std::env::var("HOST").unwrap_or_else(|_| "127.0.0.1".to_string());

        let handler_ctx = RawRoapiContext::new(&config)
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        let tables = config
            .tables
            .iter()
            .map(|t| (t.name.clone(), t.clone()))
            .collect::<HashMap<String, TableSource>>();
        let tables = Arc::new(Mutex::new(tables));

        if config.disable_read_only {
            let ctx_ext = Arc::new(RwLock::new(handler_ctx.clone()));
            let postgres_server = Box::new(
                server::postgres::PostgresServer::new(
                    ctx_ext.clone(),
                    &config,
                    default_host.clone(),
                )
                .await,
            );
            let (http_server, http_addr) = server::http::build_http_server::<ConcurrentRoapiContext>(
                ctx_ext.clone(),
                tables.clone(),
                &config,
                default_host,
            )?;

            Ok(Self {
                http_addr,
                http_server,
                postgres_server,
                max_age: config.max_age,
                tables: tables.clone(),
                handler_ctx: Some(ctx_ext.clone()),
            })
        } else {
            let ctx_ext = Arc::new(handler_ctx.clone());
            let postgres_server = Box::new(
                server::postgres::PostgresServer::new(
                    ctx_ext.clone(),
                    &config,
                    default_host.clone(),
                )
                .await,
            );
            let (http_server, http_addr) = server::http::build_http_server::<RawRoapiContext>(
                ctx_ext.clone(),
                tables.clone(),
                &config,
                default_host,
            )?;

            Ok(Self {
                http_addr,
                http_server,
                postgres_server,
                max_age: config.max_age,
                tables: tables.clone(), 
                handler_ctx: None,
            })
        }
    }

    pub fn http_addr(&self) -> std::net::SocketAddr {
        self.http_addr
    }

    pub fn postgres_addr(&self) -> std::net::SocketAddr {
        self.postgres_server.addr()
    }

    pub async fn run_until_stopped(self) -> anyhow::Result<()> {
        let postgres_server = self.postgres_server;
        info!(
            "🚀 Listening on {} for Postgres traffic...",
            postgres_server.addr()
        );
        tokio::spawn(async move {
            postgres_server
                .run()
                .await
                .expect("Failed to run postgres server");
        });
        if self.max_age.is_some() && self.handler_ctx.is_some() {
            let duration = self.max_age.unwrap();
            let tables = self.tables.clone();
            let ctx = self.handler_ctx.unwrap();
            let _ = task::spawn(async move  {
                let mut interval = time::interval(duration);
                let tabs = tables.lock().await;
                loop {
                    interval.tick().await;
                    for t in tabs.iter() {
                        if let Some(table) = tabs.get(&t.1.name) {
                            let res = ctx.load_table(table)
                                .await
                                .map_err(ColumnQError::from)
                                .map_err(ApiErrResp::load_table);
                            println!("reloaded table {} with status {:?}", &t.1.name, res);
                        }
                    }
                }
            });
        }
        info!("🚀 Listening on {} for HTTP traffic...", self.http_addr);
        Ok(self.http_server.await?)
    }
}
