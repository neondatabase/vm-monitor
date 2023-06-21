/// The data interchange between monitor and informant is sufficiently complex
/// that it warrants its own module. Careful synchronization must be maintained
/// between these datatypes and the Go serialization layer so everything doesn't
/// explode. It's a little fragile :(
///
/// The pervasive use of `#[serde(tag = "stage")]` allows Go code to read the
/// tag and then choose how to act based on the contained fields. It will try
/// to serialize out all possible fields carried by any request/response, but
/// will only access the valid ones because it can check the "type".
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct Packet {
    pub stage: Stage,
    pub seqnum: usize,
}

/// Communication between monitor and informant happens in three steps.
/// 1. One party sends the other a `Packet` containing a `Request`
/// 2. The other party performs some action to generate a `Response`, whic
///    it sends back.
/// 3. The originial sender sends back a `Done` upon receiving the response.
///
/// TODO: in the future more stages might be added to accomodate handling
/// (or not handling) packets based on their sequence numbers. This will
/// allow us to detect racy behaviour and unfavorable interleavings.
#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "stage")]
pub enum Stage {
    Request(Request),
    Response(Response), // Maybe make this option<response> to signal cancellation
    Done,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
pub enum Request {
    // Monitor initiated
    RequestUpscale { cpu: u64, mem: u64 },

    // Informant initiated
    NotifyUpscale { cpu: u64, mem: u64 },
    TryDownscale { cpu: u64, mem: u64 },
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
pub enum Response {
    // Informant sent
    UpscaleResult,

    // Monitor sent
    ResourceConfirmation,
    DownscaleResult { ok: bool, status: String },
}
