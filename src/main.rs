use anyhow::Result;
use clap::Parser;
use vm_monitor::monitor::Monitor;
use vm_monitor::Args;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let addr = "127.0.0.1:10369";
    let listener = TcpListener::bind(addr).await?;

    let (informant, _) = listener.accept().await?;

    let args = Args::parse();
    let mut monitor = Monitor::new(Default::default(), args, informant).await?;
    monitor.run().await
}
