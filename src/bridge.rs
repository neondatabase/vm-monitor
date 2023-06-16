use anyhow::Result;
use futures_util::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{TcpListener, TcpStream},
};
use tokio_tungstenite::{accept_async, tungstenite::Message, WebSocketStream};
use uuid::Uuid;

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
        Stage::Request => Some(Packet {
            stage: Stage::Response,
            seqnum: *seqnum,
        }),
        Stage::Response => Some(Packet {
            stage: Stage::Response,
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

#[derive(Serialize, Deserialize, Debug)]
enum Stage {
    Request = 0,
    Response = 1,
    Done = 2, // Abort
              // Proceed
}

#[derive(Serialize, Deserialize, Debug)]
struct Packet {
    stage: Stage,
    seqnum: usize,
}

enum MessageType {
    ResourceMessage { agent_id: Uuid, cpu: u64, mem: u64 },

    RequestUpscale { cpu: u64, mem: u64 },
    TryDownscale { agent_id: Uuid, cpu: u64, mem: u64 },
}
