use crate::api;

use axum::{
    routing::{get, post},
    Router,
};

pub fn register_app_routes() -> Router {
    Router::new()
        .route("/api/tables/:table_name", get(api::rest::get_table))
        .route("/api/sql", post(api::sql::post))
        .route("/api/graphql", post(api::graphql::post))
        .route("/api/schema", get(api::schema::schema))
        .route("/api/table", post(api::register::register_table))
}
