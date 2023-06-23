// TODO: add transaction ids to packets


use anyhow::Result;
use clap::Parser;
use compute_ctl::monitor::Monitor;
use compute_ctl::Args;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let addr = "127.0.0.1:10369";
    let listener = TcpListener::bind(addr).await?;

    let (informant, _) = listener.accept().await?;

    let args = Args::parse();
    let mut monitor = Monitor::new(Default::default(), args, informant).await?;

    // let handler = move |msg| {
    //     let decoded: Packet = match msg {
    //         Message::Text(text) => serde_json::from_str(&text).unwrap(),
    //         Message::Binary(bytes) => serde_json::from_slice(&bytes).unwrap(),
    //         _ => panic!("non text/binary packet"),
    //     };
    //
    //     match decoded.stage {
    //         Stage::Request(req) => {
    //             let res = match req {
    //                 Request::NotifyUpscale(resources) => Response::ResourceConfirmation,
    //                 Request::TryDownscale(resources) => {
    //                     // FIXME: manually calling block on is a little sus
    //                     Response::DownscaleResult(match block_on(monitor.try_downscale(resources)) {
    //                         Ok(result) => result,
    //                         Err(e) => panic!("Error attempting to downscale: {}", e),
    //                     })
    //                 }
    //                 Request::RequestUpscale { .. } => {
    //                     unreachable!("Informant should never send a Request::RequestUpscale")
    //                 }
    //             };
    //             Some(Packet {
    //                 stage: Stage::Response(res),
    //                 seqnum: 0, // FIXME
    //             })
    //         }
    //
    //         Stage::Response(res) => match res {
    //             Response::UpscaleResult => Some(Packet {
    //                 stage: Stage::Done,
    //                 seqnum: 0, // FIXME
    //             }),
    //             Response::ResourceConfirmation => {
    //                 unreachable!("Monitor should never receive a Response::ResourceConfirmation")
    //             }
    //             Response::DownscaleResult { .. } => {
    //                 unreachable!("Monitor should never receive a Response::DownscaleResult")
    //             }
    //         },
    //         Stage::Done => None, // Yay!! :)
    //     }
    // };
    monitor.run().await
    // loop {} // Let the monitor run in the background
}
