// REVIEW: is there a reason why src/main.rs is separate from src/lib.rs?
// IMO it seems like unnecessary complication.
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
        // REVIEW [deletion]: Do we actually need the /register endpoint explicitly?
        // Or can we get away with having nothing, because we should get 404s for
        // routes that don't exist (?) Not sure - an idea to check.
        // ---
        // This route gets upgraded to a websocket connection. We only support
        // connection at a time. We enforce this using global app state.
        // True indicates we are connected to someone and False indicates we can
        // receive connections.
        .route("/monitor", get(ws_handler))
        // REVIEW: Just using a `AtomicBool` means there's no name behind it. It
        // makes it harder to tell what the purpose is behind it.
        // But, we already talked about making this a global instead of state.
        // There's multiple options :)
        .with_state(Arc::new(AtomicBool::new(false)));

    let args = Args::parse();
    let addr = args.addr();
    Server::try_bind(&addr.parse().expect("parsing address should not fail"))
        .with_context(|| format!("failed to bind to {addr}"))?
        // REVIEW: Would be good to separate "bind" from "serve", so that we have a
        // log line about what address/port we've bound to.
        .serve(app.into_make_service())
        .await
        .context("server exited")?;

    Ok(())
}

/// Always returns a 404 to signal to agents that they should connect on the
/// /monitor endpoint
#[tracing::instrument(name = "/register")]
async fn register() -> Response {
    // REVIEW: Can just return (StatusCode, &'static str) - see more below.
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
        // REVIEW: thing I mentioned previously about responding with 409 to a new
        // informant vs dropping the old one.
        warn!("already connected to an informant over websocket; sending 409");
        // REVIEW: You can just return a `Result<Response, (http::StatusCode, &'static str)>`
        // and that should allow you to write
        //
        //   Err((StatusCode::Conflict, "monitor may only ..."))
        //
        // Instead of manually building the Response object
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
    // REVIEW: it'd be good to have a log message here about accepting a websocket
    // connection.
    // REVIEW: Why are we re-parsing the arguments every time we accept a weboscket
    // connection?
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
