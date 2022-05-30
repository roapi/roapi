use async_trait::async_trait;

#[async_trait]
pub trait RunnableServer: Send + Sync {
    fn addr(&self) -> std::net::SocketAddr;

    async fn run(&self) -> anyhow::Result<()>;
}

pub mod http;
pub mod postgres;
