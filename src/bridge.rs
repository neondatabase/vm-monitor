//! # bridge
//!
//! Contains types that manage the interaction (not data intercahnge, see
//! `transport`) between informant and monitor. The `Dispatcher` is a handy
//! way to process and send packets in a straightforward way.

use anyhow::Result;
use async_std::channel::{Receiver, Sender};
use futures_util::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::oneshot,
};
use tokio_tungstenite::{accept_async, tungstenite::Message, WebSocketStream};
use tracing::debug;

use crate::{transport::*, LogContext};

#[derive(Debug)]
pub struct Dispatcher<S> {
    pub(crate) source: SplitStream<WebSocketStream<S>>,
    sink: SplitSink<WebSocketStream<S>, Message>,

    pub(crate) notify_upscale_events: Sender<(Resources, oneshot::Sender<()>)>,
    pub(crate) request_upscale_events: Receiver<oneshot::Sender<()>>, // TODO: if needed, make state some arc mutex thing or an atomic
}

impl<S> Dispatcher<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    pub async fn new(
        stream: S,
        notify_upscale_events: Sender<(Resources, oneshot::Sender<()>)>,
        request_upscale_events: Receiver<oneshot::Sender<()>>,
    ) -> Result<Self> {
        let (sink, source) = accept_async(stream)
            .await
            .tee("failed to connect to stream")?
            .split();
        Ok(Self {
            sink,
            source,
            notify_upscale_events,
            request_upscale_events,
        })
    }

    /// Notify the cgroup manager that we have received upscale. Returns a Receiver
    /// that the cgroup will send to as a form of acknowledging the upscale.
    pub async fn notify_upscale(&self, resources: Resources) -> Result<oneshot::Receiver<()>> {
        let (tx, rx) = oneshot::channel();
        self.notify_upscale_events
            .send((resources, tx))
            .await
            .tee("failed to send resources and oneshot sender across channel")?;
        Ok(rx)
    }

    /// Mainly here so we only send actual data. Otherwise, it would be easy to
    /// accidentally serialize something else and send it.
    #[tracing::instrument(skip(self))]
    pub async fn send(&mut self, p: Packet) -> Result<()> {
        debug!(packet = ?p, action = "sending packet");
        let json = serde_json::to_string(&p).tee("failed to serialize packet")?;
        Ok(self
            .sink
            .send(Message::Text(json))
            .await
            .tee("stream error sending message")?)
    }
}
