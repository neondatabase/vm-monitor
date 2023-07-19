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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MonitorMessage {
    #[serde(flatten)]
    pub(crate) inner: MonitorMessageInner,
    pub(crate) id: usize,
}

impl MonitorMessage {
    pub fn new(inner: MonitorMessageInner, id: usize) -> Self {
        Self { inner, id }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
pub enum MonitorMessageInner {
    InvalidMessage {
        error: String,
    },
    InternalError {
        error: String,
    },
    // This is a struct variant because of the way go serializes struct{}
    UpscaleConfirmation {},
    // This is a struct variant because of the way go serializes struct{}
    UpscaleRequest {},

    // FIXME for the future (once the informant is deprecated)
    // As of the time of writing, the informant also uses a struct on the go
    // side called DownscaleResult. This struct has uppercase fields which are
    // serialized as such. Thus, we serialize using uppercase names so we don't
    // have to make a breaking change to the agent<->informant protocol. Once
    // the informant has been superseded by the monitor, this can be changed back.
    DownscaleResult {
        #[serde(rename = "Ok")]
        ok: bool,
        #[serde(rename = "Status")]
        status: String,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct InformantMessage {
    #[serde(flatten)]
    pub(crate) inner: InformantMessageInner,
    pub(crate) id: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type", content = "content")]
pub enum InformantMessageInner {
    InvalidMessage { error: String },
    InternalError { error: String },
    UpscaleNotification { granted: Allocation },
    DownscaleRequest { target: Allocation },
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct Allocation {
    pub(crate) cpu: f64,
    pub(crate) mem: u64,
}

impl Allocation {
    pub fn new(cpu: f64, mem: u64) -> Self {
        Self { cpu, mem }
    }
}
