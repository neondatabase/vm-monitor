use anyhow::Result;
use cfg_if::cfg_if;
use clap::Parser;
use tokio::net::TcpListener;
use tracing::info;
use tracing_subscriber::fmt::format::Pretty;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::{filter::LevelFilter, fmt, prelude::*};
use vm_monitor::monitor::Monitor;
use vm_monitor::Args;

#[tokio::main]
async fn main() -> Result<()> {
    let subscriber = tracing_subscriber::fmt::Subscriber::builder();
    cfg_if! {
        if #[cfg(debug_assertions)] {
            let subscriber = subscriber.pretty();
        } else {
            let subscriber = subscriber.json();
        }
    };
    let subscriber = subscriber
        .with_env_filter(EnvFilter::from_default_env())
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let addr = "127.0.0.1:10369";
    let listener = TcpListener::bind(addr).await?;

    let (informant, _) = listener.accept().await?;
    info!("Connected to informant on {addr}");

    let args = Args::parse();
    let mut monitor = Monitor::new(Default::default(), args, informant).await?;
    monitor.run().await
}
