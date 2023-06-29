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
    pub id: usize,
}

impl Packet {
    pub fn new(stage: Stage, id: usize) -> Self {
        Self { stage, id }
    }
}

/// Communication between monitor and informant happens in three steps.
/// 1. One party sends the other a `Packet` containing a `Request`
/// 2. The other party performs some action to generate a `Response`, which
///    it sends back.
/// 3. The original sender sends back a `Done` upon receiving the response.
///
/// TODO: in the future more stages might be added to accomodate handling
/// (or not handling) packets based on their sequence numbers. This will
/// allow us to detect racy behaviour and unfavorable interleavings.
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub enum Stage {
    Request(Request),
    Response(Response), // Maybe make this option<response> to signal cancellation
    Done {},            // Because of the way go serializes struct{}
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub enum Request {
    // Monitor initiated
    RequestUpscale {}, // We need {} because of the way go serializes struct{}

    // Informant initiated
    NotifyUpscale(Resources),
    TryDownscale(Resources),
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub enum Response {
    // Informant sent
    UpscaleResult(Resources),

    // Monitor sent
    ResourceConfirmation {}, // We need {} because of the way go serializes struct{}
    DownscaleResult(DownscaleStatus),
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct Resources {
    pub(crate) cpu: u64,
    pub(crate) mem: u64,
}

impl Resources {
    pub fn new(cpu: u64, mem: u64) -> Self {
        Self { cpu, mem }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DownscaleStatus {
    ok: bool,
    status: String,
}

impl DownscaleStatus {
    pub fn new(ok: bool, status: String) -> Self {
        Self { ok, status }
    }
}
