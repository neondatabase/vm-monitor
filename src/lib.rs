use axum::{
    extract::{ws::WebSocket, State, WebSocketUpgrade},
    response::Response,
};
use clap::Parser;
use std::{fmt::Debug, sync::OnceLock, time::Duration};
use sysinfo::{RefreshKind, System, SystemExt};
use tokio::sync::broadcast;
use tracing::{error, info};

use crate::runner::Runner;

// Code that interfaces with agent
pub mod dispatcher;
pub mod protocol;

pub mod cgroup;
pub mod filecache;
pub mod runner;

/// The CLI arguments
///
/// This is in a static so we only have to parse it once and then the entire
/// binary can see it.
pub static ARGS: OnceLock<Args> = OnceLock::new();

#[derive(Parser, Debug, Clone)]
pub struct Args {
    #[arg(short, long)]
    cgroup: Option<String>,
    #[arg(short, long)]
    file_cache_conn_str: Option<String>,
    #[arg(short, long, default_value_t = String::from("127.0.0.1:10369"))]
    pub addr: String,
}

impl Args {
    pub fn addr(&self) -> &str {
        &self.addr
    }
}

/// The number of bytes in one mebibyte.
#[allow(non_upper_case_globals)]
const MiB: u64 = 1 << 20;

/// Convert a quantity in bytes to a quantity in mebibytes, generally for display
/// purposes. (Most calculations in this crate use bytes directly)
pub fn bytes_to_mebibytes(bytes: u64) -> f32 {
    (bytes as f32) / (MiB as f32)
}

pub fn get_total_system_memory() -> u64 {
    System::new_with_specifics(RefreshKind::new().with_memory()).total_memory()
}

/// Handles incoming websocket connections.
///
/// If we are already to connected to an informant, we kill that old connection
/// and accept the new one.
#[tracing::instrument(name = "/monitor", skip(ws))]
pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(sender): State<broadcast::Sender<()>>,
) -> Response {
    // Kill the old monitor
    // TODO: this log is a little jank, as on the first connection to the server,
    // we're not actually connected to anyone. We can probaly drop the first receiver
    // and then look at the receiver count to determine if this if the first connection.
    info!("already connected to an informant over websocket -> closing old connection");
    let _ = sender.send(());

    // Start the new one. Wow, the cycle of death and rebirth
    let closer = sender.subscribe();
    ws.on_upgrade(|ws| start_monitor(ws, closer))
}

/// Starts the monitor. If startup fails or the monitor exits, an error will
/// be logged and our internal state will be reset to allow for new connections.
#[tracing::instrument(skip_all)]
async fn start_monitor(ws: WebSocket, kill: broadcast::Receiver<()>) {
    info!("accepted new websocket connection -> starting monitor");
    let monitor = tokio::time::timeout(
        Duration::from_secs(2),
        // Unwrap is safe because we initialize at the beginning of main
        Runner::new(Default::default(), ARGS.get().unwrap(), ws, kill),
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
