use anyhow::Context;
use axum::{routing::get, Router, Server};
use clap::Parser;
use tracing::info;
use tracing_subscriber::EnvFilter;
use vm_monitor::ws_handler;
use vm_monitor::Args;
use vm_monitor::ServerState;

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

    let args: &'static Args = Box::leak(Box::new(Args::parse()));

    // This channel is used to close old connections. When a new connection is
    // made, we send a message signalling to the old connection to close.
    let (sender, _) = tokio::sync::broadcast::channel::<()>(1);

    let app = Router::new()
        // This route gets upgraded to a websocket connection. We only support
        // connection at a time. We enforce this using global app state.
        // True indicates we are connected to someone and False indicates we can
        // receive connections.
        .route("/monitor", get(ws_handler))
        .with_state(ServerState { sender, args });

    let addr = args.addr();
    let bound = Server::try_bind(&addr.parse().expect("parsing address should not fail"))
        .with_context(|| format!("failed to bind to {addr}"))?;

    info!(addr, "server bound");

    bound
        .serve(app.into_make_service())
        .await
        .context("server exited")?;

    Ok(())
}
