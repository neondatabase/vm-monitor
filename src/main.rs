use anyhow::Result;
use cfg_if::cfg_if;
use clap::Parser;
use tokio::net::TcpListener;
use tracing::{error, info, trace};
use tracing_subscriber::EnvFilter;
use vm_monitor::monitor::Monitor;
use vm_monitor::timer::Timer;
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

    tokio::spawn(async {
        let mut timer = Timer::new(5000);
        loop {
            timer.await;
            trace!("heartbeat: running");
            timer = Timer::new(5000);
        }
    });

    let addr = "127.0.0.1:10369";
    let listener = TcpListener::bind(addr).await?;

    let (informant, _) = listener.accept().await?;
    info!("Connected to informant on {addr}");

    let args = Args::parse();
    let mut monitor = Monitor::new(Default::default(), args, informant).await?;
    match monitor.run().await {
        Ok(_) => {
            error!("Monitor stopped running but returned Ok(())");
            unreachable!("Monitor stopped running but returned Ok(())")
        }
        Err(e) => {
            error!("Monitor terminated on {e}");
            return Err(e);
        }
    }
}
