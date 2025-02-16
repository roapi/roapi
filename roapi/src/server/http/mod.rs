use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::Extension;
use axum::http::Method;
use axum::response::IntoResponse;
use axum::routing::get;
use columnq::table::TableSource;
use snafu::prelude::*;
use tokio::sync::Mutex;

pub mod layers;
#[cfg(feature = "ui")]
mod ui;

use crate::api;
use crate::config::Config;
use crate::context::RoapiContext;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Could bind TCP address: {source}"))]
    BindTcp { source: std::io::Error },
}

pub type HttpApiServe = axum::serve::Serve<axum::Router, axum::Router>;

pub async fn health() -> Result<impl IntoResponse, crate::error::ApiErrResp> {
    Ok(api::bytes_to_resp("OK".into(), "text/plain"))
}

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

    let api_routes = api::routes::register_api_routes::<H>();
    let routes = api_routes.route("/health", get(health));

    #[cfg(feature = "ui")]
    let routes = routes.nest("/ui", ui::ui_router());
    #[cfg(not(feature = "ui"))]
    let routes = {
        async fn ui() -> Result<impl IntoResponse, crate::error::ApiErrResp> {
            Ok("ROAPI is not built with UI support".into_response())
        }
        routes.route("/ui", get(ui))
    };

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
