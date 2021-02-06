use std::fs;

use actix_cors::Cors;
use actix_web::{middleware, web, App, HttpServer};
use anyhow::Context;
use columnq::table::TableSource;

use roapi_http::api;
use roapi_http::api::HandlerContext;
use roapi_http::config::Config;

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let matches = clap::App::new("roapi-http")
        .version(env!("CARGO_PKG_VERSION"))
        .author("QP Hou")
        .about(
            "Create full-fledged APIs for static datasets without writing a single line of code.",
        )
        .setting(clap::AppSettings::ArgRequiredElseHelp)
        .args(&[
            clap::Arg::new("addr")
                .about("bind address")
                .required(false)
                .takes_value(true)
                .value_name("IP:PORT")
                .long("addr")
                .short('a'),
            clap::Arg::new("config")
                .about("config file path")
                .required(false)
                .takes_value(true)
                .long("config")
                .short('c'),
            clap::Arg::new("table")
                .about("table sources to load")
                .takes_value(true)
                .required(false)
                .number_of_values(1)
                .multiple(true)
                .value_name("table_name:uri")
                .long("table")
                .short('t'),
        ])
        .get_matches();

    let mut config: Config = match matches.value_of("config") {
        None => Config::default(),
        Some(config_path) => {
            let config_content = fs::read_to_string(config_path)
                .with_context(|| format!("Failed to read config file: {}", config_path))?;

            serde_yaml::from_str(&config_content).context("Failed to parse YAML config")?
        }
    };

    if let Some(tables) = matches.values_of("table") {
        for v in tables {
            let mut split = v.splitn(2, ':');
            let table_name = split
                .next()
                .ok_or_else(|| anyhow::anyhow!("invalid table config: {}", v))?;
            let uri = split
                .next()
                .ok_or_else(|| anyhow::anyhow!("invalid table config: {}", v))?;
            config
                .tables
                .push(TableSource::new(table_name.to_string(), uri.to_string()));
        }
    }

    if let Some(addr) = matches.value_of("addr") {
        config.addr = Some(addr.to_string());
    }

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
                Cors::default()
                    .allowed_methods(vec!["POST", "GET"])
                    .supports_credentials()
                    .max_age(3600),
            );

        api::register_app_routes(app)
    })
    .bind(config.addr.unwrap_or_else(|| "127.0.0.1:8080".to_string()))?
    .run()
    .await?;

    Ok(())
}
