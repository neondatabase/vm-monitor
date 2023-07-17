use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use axum::{
    body::Full,
    extract::{ws::WebSocket, State, WebSocketUpgrade},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Router,
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
        .route(
            "/register",
            get(|| async {
                Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Full::from("connect to the monitor via ws on /monitor"))
                    .unwrap()
            }),
        )
        .route("/monitor", get(ws_handler))
        .with_state(Arc::new(AtomicBool::new(false)));

    axum::Server::bind(&"0.0.0.0:10369".parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();

    Ok(())
}

#[tracing::instrument(skip(ws))]
async fn ws_handler(ws: WebSocketUpgrade, State(state): State<Arc<AtomicBool>>) -> Response {
    if !state.fetch_or(true, Ordering::AcqRel) {
        info!("receiving websocket connection");
        ws.on_upgrade(start_monitor)
    } else {
        warn!("already connected to an informant over websocket; sending 409");
        Response::builder()
            .status(StatusCode::CONFLICT)
            .body(Full::from(
                "monitor may only be connected to one informant at a time",
            ))
            .unwrap()
            .into_response()
    }
}

#[tracing::instrument(skip(ws))]
async fn start_monitor(ws: WebSocket) {
    let args = Args::parse();
    let mut monitor = match Monitor::new(Default::default(), args, ws).await {
        Ok(monitor) => monitor,
        Err(e) => panic!("Failed to create monitor: {e}"),
    };
    info!("connected to informant");
    match monitor.run().await {
        Ok(_) => {
            unreachable!("Monitor stopped running but returned Ok(())")
        }
        Err(e) => {
            panic!("Monitor terminated on {e}");
        }
    }
}
