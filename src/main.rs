use clap::Parser;
use tokio::net::TcpListener;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;
use vm_monitor::monitor::Monitor;
use vm_monitor::Args;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let subscriber = tracing_subscriber::fmt::Subscriber::builder()
        .json()
        .with_file(true)
        .with_line_number(true)
        .with_span_list(true)
        .with_env_filter(EnvFilter::from_default_env())
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let addr = "127.0.0.1:10369";
    let listener = TcpListener::bind(addr).await?;

    // Wait for a connection
    let (informant, _) = listener.accept().await?;
    info!("Connected to informant on {addr}");

    let args = Args::parse();
    let mut monitor = Monitor::new(Default::default(), args, informant).await?;
    match monitor.run().await {
        Ok(_) => {
            error!(error = "Monitor stopped running but returned Ok(())");
            unreachable!("Monitor stopped running but returned Ok(())")
        }
        Err(e) => {
            error!(error = ?e, "Monitor terminated on {e}");
            Err(e)
        }
    }
}
