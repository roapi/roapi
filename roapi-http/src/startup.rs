use axum::extract::Extension;
use axum::http::Method;
use columnq::table::TableSource;
use std::collections::HashMap;
use std::net::TcpListener;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

use crate::api;
use crate::api::ConcurrentHandlerContext;
use crate::api::RawHandlerContext;
use crate::config::Config;
use crate::layers::HttpLoggerLayer;

pub struct Application {
    port: u16,
    server: axum::Server<
        hyper::server::conn::AddrIncoming,
        axum::routing::IntoMakeService<axum::Router>,
    >,
}

impl Application {
    pub async fn build(config: Config) -> anyhow::Result<Self> {
        let default_host = std::env::var("HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
        let default_port = std::env::var("PORT").unwrap_or_else(|_| "8080".to_string());
        let default_addr = [default_host.to_string(), default_port.to_string()].join(":");
        let addr = (config.addr)
            .clone()
            .unwrap_or_else(|| default_addr.to_string());
        let listener = TcpListener::bind(addr)?;
        let port = listener.local_addr().unwrap().port();
        let server = axum::Server::from_tcp(listener).unwrap();

        let handler_ctx = RawHandlerContext::new(&config)
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        let cors = tower_http::cors::CorsLayer::new()
            .allow_methods(vec![Method::GET, Method::POST, Method::OPTIONS])
            .allow_origin(tower_http::cors::Any)
            .allow_credentials(false);
        let tables = config
            .tables
            .iter()
            .map(|t| (t.name.clone(), t.clone()))
            .collect::<HashMap<String, TableSource>>();

        let mut app = if config.disable_read_only {
            let ctx_ext = Arc::new(RwLock::new(handler_ctx));
            let routes = api::routes::register_app_routes::<ConcurrentHandlerContext>();
            routes.layer(Extension(ctx_ext))
        } else {
            let ctx_ext = Arc::new(handler_ctx);
            let routes = api::routes::register_app_routes::<RawHandlerContext>();
            routes.layer(Extension(ctx_ext))
        };

        app = app
            .layer(Extension(Arc::new(Mutex::new(tables))))
            .layer(cors);

        if log::log_enabled!(log::Level::Info) {
            // only add logger layer if level >= INFO
            app = app.layer(HttpLoggerLayer::new());
        }

        let server = server.serve(app.into_make_service());
        Ok(Self { port, server })
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub async fn run_until_stopped(self) -> anyhow::Result<()> {
        Ok(self.server.await?)
    }
}
