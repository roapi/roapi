use std::fs;

use actix_cors::Cors;
use actix_http::body::MessageBody;
use actix_service::ServiceFactory;
use actix_web::dev::{ServiceRequest, ServiceResponse};
use actix_web::{middleware, web, App, Error, HttpServer};
use anyhow::Context;

use roapi::api;
use roapi::api::HandlerContext;
use roapi::config::Config;

fn register_app_routes<T, B>(app: App<T, B>) -> App<T, B>
where
    B: MessageBody,
    T: ServiceFactory<
        Config = (),
        Request = ServiceRequest,
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
    .route("/api/graphsql", web::post().to(api::graphql::post))
    .route("/api/schema", web::get().to(api::schema::get))
}

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let matches = clap::App::new("roapi")
        .version(env!("CARGO_PKG_VERSION"))
        .setting(clap::AppSettings::ArgRequiredElseHelp)
        .args(&[clap::Arg::new("config")
            .about("config file path")
            .required(true)
            .takes_value(true)
            .long("config")
            .short('c')])
        .get_matches();

    let config_path = matches.value_of("config").unwrap();
    let config_content = fs::read_to_string(config_path)
        .with_context(|| format!("Failed to read config file: {}", config_path))?;
    let config: Config =
        serde_yaml::from_str(&config_content).context("Failed to parse YAML config")?;

    let ctx = web::Data::new(
        HandlerContext::new(&config)
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?,
    );

    // Start http server
    HttpServer::new(move || {
        let app = App::new()
            .app_data(ctx.clone())
            .wrap(middleware::Logger::default())
            .wrap(
                Cors::new()
                    .allowed_methods(vec!["POST", "GET"])
                    .supports_credentials()
                    .max_age(3600)
                    .finish(),
            );

        register_app_routes(app)
    })
    .bind(config.addr.unwrap_or_else(|| "127.0.0.1:8080".to_string()))?
    .run()
    .await?;

    Ok(())
}

#[cfg(test)]
#[path = "./main_test.rs"]
mod main_test;
