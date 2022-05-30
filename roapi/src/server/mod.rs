use async_trait::async_trait;

#[async_trait]
pub trait RunnableServer: Send + Sync {
    fn port(&self) -> u16;

    async fn run(&self) -> anyhow::Result<()>;
}

pub mod http;
pub mod postgres;
