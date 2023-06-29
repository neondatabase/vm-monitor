/// # bridge
///
/// The bridge module mediates the follow exchanges between the monitor and
/// informant.
///
/// Monitor: RequestUpscale
/// Informant: Returns No Data
///
/// Informant: TryDownscale
/// Monitor: Returns DownscaleResult
///
/// Informant: ResourceMessage
/// Monitor: Returns No Data
///
/// Note: messages don't need to carry uuid's because the monitor and informant
/// are always linked. The monitor has no knowledge of autoscaler-agents
///
/// The monitor and informant are connected via websocket on port 10369
///
use anyhow::{Context, Result};
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

use crate::transport::*;

#[derive(Debug)]
pub struct Dispatcher<S> {
    pub(crate) source: SplitStream<WebSocketStream<S>>,
    pub(crate) sink: SplitSink<WebSocketStream<S>, Message>,

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
        let (sink, source) = accept_async(stream).await?.split();
        Ok(Self {
            sink,
            source,
            notify_upscale_events,
            request_upscale_events,
        })
    }

    pub async fn send(&mut self, p: Packet) -> Result<()> {
        debug!("Sending packet {p:?}");
        let json = serde_json::to_string(&p).context("failed to serialize packet")?;
        self.sink
            .send(Message::Text(json))
            .await
            .map_err(|e| e.into()) // Sink returns some weird error that we need to convert
    }
}
