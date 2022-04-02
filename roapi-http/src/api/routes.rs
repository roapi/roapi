use axum::{
    routing::{get, post},
    Router,
};

use crate::api::{self, HandlerCtx};

pub fn register_app_routes<H: HandlerCtx>() -> Router {
    let mut router = Router::new()
        .route("/api/tables/:table_name", get(api::rest::get_table::<H>))
        .route("/api/sql", post(api::sql::post::<H>))
        .route("/api/kv/:kv_name/:key", get(api::kv::get::<H>))
        .route("/api/graphql", post(api::graphql::post::<H>))
        .route("/api/schema", get(api::schema::schema::<H>));

    if H::read_only_mode() {
        router = router.route("/api/table", post(api::register::register_table_read_only));
    } else {
        router = router.route("/api/table", post(api::register::register_table::<H>));
    }

    router
}
