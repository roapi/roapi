use axum::{
    routing::{get, post},
    Router,
};

use crate::api;
use crate::context::RoapiContext;

pub fn register_app_routes<H: RoapiContext>() -> Router {
    let mut router = Router::new()
        .route("/health", get(api::health::health))
        .route("/api/tables/:table_name", get(api::rest::get_table::<H>))
        .route("/api/sql", post(api::sql::post::<H>))
        .route("/api/kv/:kv_name/:key", get(api::kv::get::<H>))
        .route("/api/graphql", post(api::graphql::post::<H>))
        .route("/api/schema", get(api::schema::schema::<H>))
        .route(
            "/api/schema/:table_name",
            get(api::schema::get_by_table_name::<H>),
        )
        .route("/api/jwt/authorize", post(api::jwt::authorize::<H>));

    if H::read_only_mode() {
        router = router
            .route("/api/table", post(api::register::register_table_read_only))
            .route("/api/tables/drop", post(api::drop::drop_table_read_only));
    } else {
        router = router
            .route("/api/table", post(api::register::register_table::<H>))
            .route("/api/tables/drop", post(api::drop::drop_table::<H>));
    }

    router
}
