#![deny(warnings)]

use roapi::config::get_configuration;
use roapi::startup::Application;
use snafu::{whatever, Whatever};

#[cfg(snmalloc)]
#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[tokio::main]
async fn main() -> Result<(), Whatever> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let config = whatever!(get_configuration(), "Failed to load configuration");
    let application = Application::build(config)
        .await
        .expect("Failed to build App");
    application
        .run_until_stopped()
        .await
        .expect("Failed to run App");
    Ok(())
}
