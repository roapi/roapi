use crate::api;
use crate::api::HandlerContext;
use crate::config::Config;
use actix_cors::Cors;
use actix_web::dev::Server;
use actix_web::{middleware, web, App, HttpServer};
use std::net::TcpListener;

pub struct Application {
    port: u16,
    server: Server,
}

impl Application {
    pub async fn build(config: Config) -> Result<Self, std::io::Error> {
        let addr = (config.addr)
            .clone()
            .unwrap_or_else(|| "127.0.0.1:8080".to_string());
        let listener = TcpListener::bind(addr)?;
        let port = listener.local_addr().unwrap().port();

        let ctx = web::Data::new(
            HandlerContext::new(&config)
                .await
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?,
        );

        let server = HttpServer::new(move || {
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
        .listen(listener)?
        .run();

        Ok(Self { port, server })
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub async fn run_until_stopped(self) -> Result<(), std::io::Error> {
        self.server.await
    }
}
