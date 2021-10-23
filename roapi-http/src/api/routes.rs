use actix_http::body::MessageBody;
use actix_service::ServiceFactory;
use actix_web::dev::{ServiceRequest, ServiceResponse};
use actix_web::{web, App, Error};

use crate::api;

pub fn register_app_routes<T, B>(app: App<T, B>) -> App<T, B>
where
    B: MessageBody,
    T: ServiceFactory<
        ServiceRequest,
        Config = (),
        Response = ServiceResponse<B>,
        Error = Error,
        InitError = (),
    >,
{
    app.route(
        "/api/tables/{table_name}",
        web::get().to(api::rest::get_table),
    )
    .route("/api/sql", web::post().to(api::sql::post))
    .route("/api/graphql", web::post().to(api::graphql::post))
    .route("/api/schema", web::get().to(api::schema::get))
    .route(
        "/api/schema/{table_name}",
        web::get().to(api::schema::get_by_table_name),
    )
}
