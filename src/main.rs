use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use anyhow::Context;
use axum::{
    body::Full,
    extract::{ws::WebSocket, State, WebSocketUpgrade},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Router, Server,
};
use clap::Parser;
use tracing::{info, warn};
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

    let app = Router::new()
        // Current, the informant exposes a /register endpoint. At the time of
        // writing, we need to support both agent->informant->monitor and
        // agent->monitor systems due to old vms. For new deploys with just the
        // monitor, the agent can still hit the /register endpoint. If it gets
        // a 404 back, it'll know to git the /monitor endpoint and normal
        // functioning can follow.
        .route("/register", get(register))
        // This route gets upgraded to a websocket connection. We only support
        // connection at a time. We enforce this using global app state.
        // True indicates we are connected to someone and False indicates we can
        // receive connections.
        .route("/monitor", get(ws_handler))
        .with_state(Arc::new(AtomicBool::new(false)));

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
/// an informant, returns a 409 (conflict) response, as only one informant can
/// be instructing is on what do at a time. Otherwise, starts the monitor.
#[tracing::instrument(name = "/monitor", skip(ws))]
async fn ws_handler(ws: WebSocketUpgrade, State(state): State<Arc<AtomicBool>>) -> Response {
    if !state.fetch_or(true, Ordering::AcqRel) {
        info!("receiving websocket connection");
        ws.on_upgrade(|ws| start_monitor(ws, state))
    } else {
        warn!("already connected to an informant over websocket; sending 409");
        Response::builder()
            .status(StatusCode::CONFLICT)
            .body(Full::from(
                "monitor may only be connected to one informant at a time",
            ))
            .expect("making a body from a string literal should never fail")
            .into_response()
    }
}

// TODO: should these warns be hard errors? In theory they can happen in normal
// operation if we're switching agents
/// Starts the monitor. If startup fails or the monitor exits, and error will
/// be logged and our internal state will be reset to allow for new connections.
#[tracing::instrument(skip(ws))]
async fn start_monitor(ws: WebSocket, state: Arc<AtomicBool>) {
    let args = Args::parse();
    let mut monitor = match Monitor::new(Default::default(), args, ws).await {
        Ok(monitor) => monitor,
        Err(e) => {
            warn!(error = ?e, "failed to create monitor");
            state.fetch_and(false, Ordering::AcqRel);
            return;
        }
    };
    info!("connected to informant");
    match monitor.run().await {
        Ok(_) => {
            unreachable!("Monitor stopped running but returned Ok(())")
        }
        Err(e) => {
            warn!(error = ?e, "monitor terminated");
            state.fetch_and(false, Ordering::AcqRel);
        }
    }
}
