use axum::response::IntoResponse;
use axum::{
    routing::{get, post},
    Router,
};

use crate::api;
use crate::context::RoapiContext;

pub async fn version() -> Result<impl IntoResponse, crate::error::ApiErrResp> {
    Ok(api::bytes_to_json_resp(
        format!("\"{}\"", env!("CARGO_PKG_VERSION")).into(),
    ))
}

pub fn register_api_routes<H: RoapiContext>() -> Router {
    let mut api_routes = Router::new()
        .route("/version", get(version))
        .route("/tables/:table_name", get(api::rest::get_table::<H>))
        .route("/sql", post(api::sql::post::<H>))
        .route("/kv/:kv_name/:key", get(api::kv::get::<H>))
        .route("/graphql", post(api::graphql::post::<H>))
        .route("/schema", get(api::schema::schema::<H>))
        .route(
            "/schema/:table_name",
            get(api::schema::get_by_table_name::<H>),
        );

    if H::read_only_mode() {
        api_routes = api_routes
            .route("/table", post(api::register::register_table_read_only))
            .route("/tables/drop", post(api::drop::drop_table_read_only));
    } else {
        api_routes = api_routes
            .route("/table", post(api::register::register_table::<H>))
            .route("/tables/drop", post(api::drop::drop_table::<H>));
    }

    Router::new().nest("/api", api_routes)
}
