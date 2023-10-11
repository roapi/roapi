use async_trait::async_trait;
use snafu::Whatever;

#[async_trait]
pub trait RunnableServer: Send + Sync {
    fn addr(&self) -> std::net::SocketAddr;

    async fn run(&self) -> Result<(), Whatever>;
}

pub mod flight_sql;
pub mod http;
pub mod postgres;
