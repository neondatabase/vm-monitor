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
use anyhow::Result;
use futures_util::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use serde::Serialize;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
};
use tokio_tungstenite::{accept_async, tungstenite::Message, WebSocketStream};
use crate::transport::*;

use crate::cgroup::CgroupState;

pub async fn handle_connection(stream: TcpStream) -> Result<()> {
    let dispatcher = Dispatcher::new(stream, 0, handler).await?;
    dispatcher.run().await
}

fn handler(seqnum: &mut usize, msg: Message) -> Option<Packet> {
    let decoded: Packet = match msg {
        Message::Text(text) => serde_json::from_str(&text).unwrap(),
        Message::Binary(bytes) => serde_json::from_slice(&bytes).unwrap(),
        _ => panic!("non text/binary packet"),
    };
    *seqnum += 1;
    match decoded.stage {
        Stage::Request(req) => {
            let res = match req {
                Request::RequestUpscale { cpu, mem } => {
                    unreachable!("Informant should never send a Request::RequestUpscale")
                }
                Request::NotifyUpscale { cpu, mem } => Response::ResourceConfirmation {},
                Request::TryDownscale { cpu, mem } => Response::DownscaleResult {
                    ok: true,
                    status: String::from("everything is ok"),
                },
            };
            Some(Packet {
                stage: Stage::Response(res),
                seqnum: *seqnum,
            })
        }
        Stage::Response(res) => Some(Packet {
            stage: Stage::Done,
            seqnum: *seqnum,
        }),
        Stage::Done => None,
    }
}

struct Dispatcher<T, U, S> {
    // conn: WebSocketStream<S>,
    source: SplitStream<WebSocketStream<S>>,
    sink: SplitSink<WebSocketStream<S>, Message>,
    state: U,
    handler: fn(&mut U, Message) -> T,
}

impl<T, U, S> Dispatcher<T, U, S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    T: Serialize,
{
    pub async fn new(stream: S, state: U, handler: fn(&mut U, Message) -> T) -> Result<Self> {
        let (sink, source) = accept_async(stream).await?.split();
        Ok(Self {
            sink,
            source,
            state,
            handler,
        })
    }

    pub async fn run(mut self) -> Result<()> {
        while let Some(msg) = self.source.next().await {
            println!("Received: {msg:?}");
            // Maybe have another thread do this work? Can lead to out of order
            match msg {
                Ok(msg) => {
                    let ret = self.handle(msg);
                    let json = serde_json::to_string(&ret).unwrap();
                    self.sink.send(Message::Text(json)).await.unwrap();
                }
                Err(e) => println!("{e}"),
            }
        }
        Ok(())
    }

    fn handle(&mut self, msg: Message) -> T {
        (self.handler)(&mut self.state, msg)
    }
}

// TODO: implement methods here to make clear that these methods interface with
// informant?
impl CgroupState {
    #[tracing::instrument]
    pub async fn request_upscale(&self) -> Result<()> {
        Ok(())
    }

    #[tracing::instrument]
    pub async fn try_downscale(&self) -> Result<()> {
        Ok(())
    }
}

