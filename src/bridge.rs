//! # bridge
//!
//! Contains types that manage the interaction (not data intercahnge, see
//! `transport`) between informant and monitor. The `Dispatcher` is a handy
//! way to process and send packets in a straightforward way.

use anyhow::Context;
use async_std::channel::{Receiver, Sender};
use axum::extract::ws::{Message, WebSocket};
use futures_util::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use tokio::sync::oneshot;
use tracing::{debug, info};

use crate::{
    protocol::{Allocation, MonitorMessage},
    protocol::{
        ProtocolBounds, ProtocolResponse, ProtocolVersion, PROTOCOL_MAX_VERSION,
        PROTOCOL_MIN_VERSION,
    },
};

#[derive(Debug)]
pub struct Dispatcher {
    pub(crate) source: SplitStream<WebSocket>,
    sink: SplitSink<WebSocket, Message>,

    pub(crate) notify_upscale_events: Sender<(Allocation, oneshot::Sender<()>)>,
    pub(crate) request_upscale_events: Receiver<oneshot::Sender<()>>, // TODO: if needed, make state some arc mutex thing or an atomic
    pub(crate) proto_version: ProtocolVersion,
}

impl Dispatcher {
    pub async fn new(
        stream: WebSocket,
        notify_upscale_events: Sender<(Allocation, oneshot::Sender<()>)>,
        request_upscale_events: Receiver<oneshot::Sender<()>>,
    ) -> anyhow::Result<Self> {
        let (mut sink, mut source) = stream.split();

        // Figure out what protocol to use
        info!("waiting for informant to send protocol range");
        let proto_bounds = if let Some(bounds) = source.next().await {
            let bounds = bounds.context("failed to read bounds off connection")?;
            if let Message::Text(bounds) = bounds {
                info!(bounds, "received bounds message");
                assert!(PROTOCOL_MIN_VERSION <= PROTOCOL_MAX_VERSION);
                // Safe to unwrap because of the assert
                let monitor_bounds: ProtocolBounds =
                    ProtocolBounds::new(PROTOCOL_MIN_VERSION, PROTOCOL_MAX_VERSION).unwrap();
                let informant_bounds: ProtocolBounds =
                    serde_json::from_str(&bounds).context("failed to deserialize bounds")?;
                match monitor_bounds.highest_shared_version(&informant_bounds) {
                    Ok(version) => {
                        sink.send(Message::Text(
                            serde_json::to_string(&ProtocolResponse::version(version)).unwrap(),
                        ))
                        .await
                        .context("failed to notify informant of negotiated protocol version")?;
                        version
                    }
                    Err(e) => {
                        sink.send(Message::Text(
                            serde_json::to_string(&ProtocolResponse::error(format!(
                                "Received range {} which does not overlap with {}",
                                informant_bounds, monitor_bounds
                            )))
                            .unwrap(),
                        ))
                        .await
                        .context(
                            "failed to notify informant of no overlap between protocol ranges",
                        )?;
                        Err(e).context("error determining suitable protocol bounds")?
                    }
                }
            } else {
                // See nhooyr/websocket's implementation of wsjson.Write
                unreachable!("informant never sends non-text message")
            }
        } else {
            anyhow::bail!("connection closed while doing protocol handshake")
        };

        Ok(Self {
            sink,
            source,
            notify_upscale_events,
            request_upscale_events,
            proto_version: proto_bounds,
        })
    }

    /// Notify the cgroup manager that we have received upscale. Returns a Receiver
    /// that the cgroup will send to as a form of acknowledging the upscale.
    pub async fn notify_upscale(
        &self,
        resources: Allocation,
    ) -> anyhow::Result<oneshot::Receiver<()>> {
        let (tx, rx) = oneshot::channel();
        self.notify_upscale_events
            .send((resources, tx))
            .await
            .context("failed to send resources and oneshot sender across channel")?;
        Ok(rx)
    }

    /// Mainly here so we only send actual data. Otherwise, it would be easy to
    /// accidentally serialize something else and send it.
    #[tracing::instrument(skip(self))]
    pub async fn send(&mut self, message: MonitorMessage) -> anyhow::Result<()> {
        debug!(?message, action = "sending packet");
        let json = serde_json::to_string(&message).context("failed to serialize packet")?;
        self.sink
            .send(Message::Text(json))
            .await
            .context("stream error sending message")
    }
}
