use anyhow::Result;
use clap::Parser;
use compute_ctl::Args;
use compute_ctl::monitor::Monitor;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();
    let _monitor = Monitor::new(Default::default(), args).await?;
    loop {} // Let the monitor run in the background
}
