use std::time::Duration;

use anyhow::Context;
use axum::{
    body::Full,
    extract::{ws::WebSocket, State, WebSocketUpgrade},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{any, get},
    Router, Server,
};
use clap::Parser;
use tokio::sync::broadcast;
use tracing::{error, info, warn};
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

    // This channel is used to close old connections. When a new connection is
    // made, we send a message signalling to the old connection to close.
    let (sender, _) = tokio::sync::broadcast::channel::<()>(1);

    let app = Router::new()
        // Current, the informant exposes a /register endpoint. At the time of
        // writing, we need to support both agent->informant->monitor and
        // agent->monitor systems due to old vms. For new deploys with just the
        // monitor, the agent can still hit the /register endpoint. If it gets
        // a 404 back, it'll know to git the /monitor endpoint and normal
        // functioning can follow.
        .route("/register", any(register))
        // This route gets upgraded to a websocket connection. We only support
        // connection at a time. We enforce this using global app state.
        // True indicates we are connected to someone and False indicates we can
        // receive connections.
        .route("/monitor", get(ws_handler))
        .with_state(sender);

    let args = Args::parse();
    let addr = args.addr();
    let server = Server::try_bind(
        &addr
            .parse()
            .with_context(|| format!("failed to parse address: '{addr}'"))?,
    )
    .with_context(|| format!("failed to bind to {addr}"))?;

    info!("server listening on {addr}");

    server
        .serve(app.into_make_service())
        .await
        .context("server exited")?;

    Ok(())
}

/// Always returns a 404 to signal to agents that they should connect on the
/// /monitor endpoint
#[tracing::instrument(name = "/register")]
async fn register() -> Response {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(Full::from("connect to the monitor via ws on /monitor"))
        .unwrap()
        .into_response()
}

/// Handles incoming websocket connections. If we are already to connected to
/// an informant, we stop the monitor for that connection (thus killing it),
/// and create a new monitor for the new connection.
#[tracing::instrument(name = "/monitor", skip(ws))]
async fn ws_handler(ws: WebSocketUpgrade, State(sender): State<broadcast::Sender<()>>) -> Response {
    // Kill the old monitor
    warn!("already connected to an informant over websocket; closing old connection");
    let _ = sender.send(());

    // Start the new one. Wow, the cycle of death and rebirth
    let closer = sender.subscribe();
    ws.on_upgrade(|ws| start_monitor(ws, closer))
}

// TODO: should these warns be hard errors? In theory they can happen in normal
// operation if we're switching agents
/// Starts the monitor. If startup fails or the monitor exits, and error will
/// be logged and our internal state will be reset to allow for new connections.
async fn start_monitor(ws: WebSocket, kill: broadcast::Receiver<()>) {
    let args = Args::parse();
    let monitor = tokio::time::timeout(
        Duration::from_secs(2),
        Monitor::new(Default::default(), args, ws, kill),
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
