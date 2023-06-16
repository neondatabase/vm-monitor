use anyhow::Result;
use clap::Parser;
use compute_ctl::bridge::handle_connection;
use compute_ctl::Args;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let addr = "127.0.0.1:3333";
    let listener = TcpListener::bind(&addr).await?;

    loop {
        let (stream, _) = listener.accept().await?;
        tokio::spawn(handle_connection(stream)).await??;
    }
}
