#![deny(warnings)]

use roapi::config::{get_cmd, get_configuration};
use roapi::startup::Application;

#[cfg(feature = "snmalloc")]
#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[tokio::main]
async fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let mut cmd = get_cmd();

    let re: Result<(), Box<dyn std::error::Error>> = async {
        let cmd = cmd.clone();
        let config = get_configuration(cmd)?;
        let application = Application::build(config).await?;
        application.run_until_stopped().await?;
        Ok(())
    }
    .await;
    if let Err(e) = re {
        cmd.error(clap::error::ErrorKind::Io, format!("{e}")).exit();
    }
}
