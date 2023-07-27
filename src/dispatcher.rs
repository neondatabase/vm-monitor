//! # bridge
//!
//! The dispatcher manages many of the signals in the monitor. It types that
//! manage the interaction (not data intercahnge, see `protocol`) between
//! informant and monitor, allow us to to process and send packets in a
//! straightforward way. The dispatcher also manages that signals that come from
//! the cgroup (requesting upscale), and the signals that go to the cgroup (
//! notifying it of upscale).

use anyhow::Context;
use async_std::channel::{Receiver, Sender};
use axum::extract::ws::{Message, WebSocket};
use futures_util::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use tokio::sync::oneshot;
use tracing::info;

use crate::{
    protocol::{Allocation, MonitorMessage},
    protocol::{
        ProtocolRange, ProtocolResponse, ProtocolVersion, PROTOCOL_MAX_VERSION,
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
    /// Creates a new dispatcher using the passed-in connection. Performs a
    /// negotiation with the informant to determine a suitable protocol range.
    /// This consists of two steps:
    /// 1. Wait for the informant to sent the range of protocols it supports.
    /// 2. Send a protocol version that works for us as well or an error if there
    ///    is no compatible version.
    pub async fn new(
        stream: WebSocket,
        notify_upscale_events: Sender<(Allocation, oneshot::Sender<()>)>,
        request_upscale_events: Receiver<oneshot::Sender<()>>,
    ) -> anyhow::Result<Self> {
        let (mut sink, mut source) = stream.split();

        // Figure out what protocol to use
        info!("waiting for informant to send protocol range");
        let proto_range = if let Some(range) = source.next().await {
            let range = range.context("failed to read range off connection")?;
            let Message::Text(range) = range
                else {
                // See nhooyr/websocket's implementation of wsjson.Write
                unreachable!("informant never sends non-text message")
            };

            assert!(PROTOCOL_MIN_VERSION <= PROTOCOL_MAX_VERSION);
            // Safe to unwrap because of the assert
            let monitor_range: ProtocolRange =
                ProtocolRange::new(PROTOCOL_MIN_VERSION, PROTOCOL_MAX_VERSION).unwrap();

            let informant_range: ProtocolRange =
                serde_json::from_str(&range).context("failed to deserialize range")?;

            info!(range = ?informant_range, "received protocol range");

            match monitor_range.highest_shared_version(&informant_range) {
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
                            informant_range, monitor_range
                        )))
                        .unwrap(),
                    ))
                    .await
                    .context("failed to notify informant of no overlap between protocol ranges")?;
                    Err(e).context("error determining suitable protocol range")?
                }
            }
        } else {
            anyhow::bail!("connection closed while doing protocol handshake")
        };

        Ok(Self {
            sink,
            source,
            notify_upscale_events,
            request_upscale_events,
            proto_version: proto_range,
        })
    }

    /// Notify the cgroup manager that we have received upscale and wait for
    /// the acknowledgement.
    #[tracing::instrument(skip(self))]
    pub async fn notify_upscale(&self, resources: Allocation) -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.notify_upscale_events
            .send((resources, tx))
            .await
            .context("failed to send resources and oneshot sender across channel")?;
        rx.await
            .context("failed to get receipt of upscale from cgroup")
    }

    /// Mainly here so we only send actual data. Otherwise, it would be easy to
    /// accidentally serialize something else and send it.
    #[tracing::instrument(skip(self))]
    pub async fn send(&mut self, message: MonitorMessage) -> anyhow::Result<()> {
        info!(?message, "sending message to agent");
        let json = serde_json::to_string(&message).context("failed to serialize packet")?;
        self.sink
            .send(Message::Text(json))
            .await
            .context("stream error sending message")
    }
}
