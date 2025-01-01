use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::Extension;
use axum::http::Method;
use columnq::table::TableSource;
use snafu::prelude::*;
use tokio::sync::Mutex;

pub mod layers;

use crate::api;
use crate::config::Config;
use crate::context::RoapiContext;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Could bind TCP address: {source}"))]
    BindTcp { source: std::io::Error },
}

pub type HttpApiServe = axum::serve::Serve<axum::Router, axum::Router>;

pub async fn build_http_server<H: RoapiContext>(
    ctx_ext: Arc<H>,
    tables: Arc<Mutex<HashMap<String, TableSource>>>,
    config: &Config,
    default_host: String,
) -> Result<(HttpApiServe, std::net::SocketAddr), Error> {
    let default_http_port = std::env::var("PORT").unwrap_or_else(|_| "8080".to_string());
    let default_http_addr = [default_host, default_http_port].join(":");
    let http_addr = config
        .addr
        .http
        .clone()
        .unwrap_or_else(|| default_http_addr.to_string());

    let routes = api::routes::register_app_routes::<H>();
    let mut app = routes.layer(Extension(ctx_ext));

    let cors = tower_http::cors::CorsLayer::new()
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
        .allow_origin(tower_http::cors::Any)
        .allow_credentials(false);

    app = app.layer(Extension(tables)).layer(cors);

    if log::log_enabled!(log::Level::Info) {
        // only add logger layer if level >= INFO
        app = app.layer(layers::HttpLoggerLayer::new());
    }

    let listener = tokio::net::TcpListener::bind(http_addr)
        .await
        .context(BindTcpSnafu)?;
    let addr = listener
        .local_addr()
        .expect("Failed to get address from listener");

    let serve = axum::serve(listener, app);
    Ok((serve, addr))
}
