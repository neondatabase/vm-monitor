//! # transport
//!
//! The data interchange between monitor and informant is sufficiently complex
//! that it warrants its own module. Each type in this module contains a sibling
//! in the informant that should be serialized identically or in a compatible way.
//! Careful synchronization must be maintained between these datatypes and the Go
//! serialization layer so everything doesn't explode. It's a little fragile :(
//!
//! The transport module types mediate the following exchanges between the monitor and
//! informant:
//!
//! Monitor: RequestUpscale
//! Informant: Returns Resources
//!
//! Informant: TryDownscale
//! Monitor: Returns DownscaleResult
//!
//! Informant: ResourceMessage
//! Monitor: Returns No Data
//!
//! Note: messages don't need to carry uuid's because the monitor and informant
//! are always linked. The monitor has no knowledge of autoscaler-agents.
//!
//! The pervasive use of `#[serde(tag = "stage")]` allows Go code to read the
//! tag and then choose how to act based on the contained fields. It will try
//! to serialize out all possible fields carried by any request/response, but
//! will only access the valid ones because it can check the "type".

use serde::{Deserialize, Serialize};

/// The `Packet` is the singular message typed passed over the websocket connection
/// to the informant.
#[derive(Serialize, Deserialize, Debug)]
pub struct Packet {
    pub stage: Stage,

    /// Used to identify packets that are part of the same transaction. If the
    /// informant sends a TryDownscale, the monitor should return a response with
    /// the same id so the informant knows when the response to that particular
    /// downscale request has been made.
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
/// *Note*: there is one special case: when the monitor sends a `RequestUpscale`.
/// The informant will immediately respond with a `Done`. If the agent decides to
/// upscale, we will get notified with an UpscaleResult.
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub enum Stage {
    /// The initial communication in an interaction between informant/monitor.
    Request(Request),

    /// The second interaction in any communication.
    Response(Response),

    /// Final acnowledgement that a transaction is complete. Mostly for debugging
    /// purposes. Kept as a struct variant because on the go side it's represented
    /// as a struct{}, which go serializes as `{}`.
    Done {},
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

/// Represents a specification for a VM's usage - whether it be its allowed
/// allocation of resources or a desired allocation it should attempt to downscale
/// to.
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

/// The status returned after handling a TryDownscale request
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
