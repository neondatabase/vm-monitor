// REVIEW: is there a reason why src/main.rs is separate from src/lib.rs?
// IMO it seems like unnecessary complication.
use std::{sync::OnceLock, time::Duration};

use anyhow::Context;
use axum::{
    extract::{ws::WebSocket, State, WebSocketUpgrade},
    response::Response,
    routing::get,
    Router, Server,
};
use clap::Parser;
use tokio::sync::broadcast;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;
use vm_monitor::monitor::Monitor;
use vm_monitor::Args;

static ARGS: OnceLock<Args> = OnceLock::new();

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

    ARGS.get_or_init(Args::parse);

    // This channel is used to close old connections. When a new connection is
    // made, we send a message signalling to the old connection to close.
    let (sender, _) = tokio::sync::broadcast::channel::<()>(1);

    let app = Router::new()
        // This route gets upgraded to a websocket connection. We only support
        // connection at a time. We enforce this using global app state.
        // True indicates we are connected to someone and False indicates we can
        // receive connections.
        .route("/monitor", get(ws_handler))
        .with_state(sender);

    let args = Args::parse();
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

/// Handles incoming websocket connections.
///
/// If we are already to connected to an informant, we kill that old connection
/// and accept the new one.
#[tracing::instrument(name = "/monitor", skip(ws))]
async fn ws_handler(ws: WebSocketUpgrade, State(sender): State<broadcast::Sender<()>>) -> Response {
    // Kill the old monitor
    // TODO: this comment is a little jank, as on the first connection to the server,
    // we're not actually connected to anyone. We cn probaly drop the first receiver
    // and then look at the receiver count to determine if this if the first connection.
    warn!("already connected to an informant over websocket -> closing old connection");
    let _ = sender.send(());

    // Start the new one. Wow, the cycle of death and rebirth
    let closer = sender.subscribe();
    ws.on_upgrade(|ws| start_monitor(ws, closer))
}

// TODO: should these warns be hard errors? In theory they can happen in normal
// operation if we're switching agents
/// Starts the monitor. If startup fails or the monitor exits, and error will
/// be logged and our internal state will be reset to allow for new connections.
#[tracing::instrument(skip_all)]
async fn start_monitor(ws: WebSocket, kill: broadcast::Receiver<()>) {
    info!("accepted new websocket connection -> starting monitor");
    let monitor = tokio::time::timeout(
        Duration::from_secs(2),
        Monitor::new(Default::default(), ARGS.get().unwrap(), ws, kill),
    )
    .await;
    let mut monitor = match monitor {
        Ok(Ok(monitor)) => monitor,
        Ok(Err(error)) => {
            error!(?error, "failed to create monitor");
            return;
        }
        Err(elapsed) => {
            error!(
                ?elapsed,
                "creating monitor timed out (probably waiting to receive protocol range)"
            );
            return;
        }
    };
    info!("connected to informant");

    match monitor.run().await {
        Ok(()) => info!("monitor was killed due to new connection"),
        Err(e) => error!(error = ?e, "monitor terminated by itself"),
    }
}
