//! Module for types that interface with the informant - types representing
//! protocols and types for actual messages sent to the informant.
//!
//! The pervasive use of serde modifiers throughout this module is to ease
//! serialization on the go side. Because go does not have enums (which model
//! messages well), it is harder to model messages, and we accomodate that with
//! serde.
//!
//! *Note*: the informant sends and receives messages in different ways.
//!
//! The informant serializes messages in the form and then sends them. The use
//! of `#[serde(tag = "type", content = "content")]` allows us to use `Type`
//! to determine how to deserialize `Content`.
//! ```
//! struct {
//!     Content any
//!     Type    string
//!     Id      uint64
//! }
//! ```
//! and receives messages in the form:
//! ```
//! struct {
//!     {fields embedded}
//!     Type string
//!     Id   uint64
//! }
//! ```
//! After reading the type field, the informant will decode the entire message
//! again, this time into the correct type using the embedded fields.
//! Because the informant cannot just extract the json contained in a certain field
//! (it initially deserializes to `map[string]interface{}`), we keep the fields
//! at the top level, so the entire piece of json can be deserialized into a struct,
//! such as a `DownscaleResult`, with the `Type` and `Id` fields ignored.

use core::fmt;
use std::cmp;

use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

/// A Message we send to the informant.
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

/// The different underlying message types we can send to the informant.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
pub enum MonitorMessageInner {
    /// Indicates that the informant sent an invalid message, i.e, we couldn't
    /// properly deserialize it.
    InvalidMessage { error: String },
    /// Indicates that we experienced an internal error while processing a message.
    /// For example, if a cgroup operation fails while trying to handle an upscale,
    /// we return `InternalError`.
    InternalError { error: String },
    /// Returned to the informant once we have finished handling an upscale. If the
    /// handling was unsuccessful, an `InternalError` will get returned instead.
    /// *Note*: this is a struct variant because of the way go serializes struct{}
    UpscaleConfirmation {},
    /// Indicates to the monitor that we are urgently requesting resources.
    /// *Note*: this is a struct variant because of the way go serializes struct{}
    UpscaleRequest {},
    /// Returned to the informant once we have finished attempting to downscale. If
    /// an error occured trying to do so, an `InternalError` will get returned instead.
    /// However, if we are simply unsuccessful (for example, do to needing the resources),
    /// that gets included in the `DownscaleResult`.
    DownscaleResult {
        // FIXME for the future (once the informant is deprecated)
        // As of the time of writing, the informant also uses a struct on the go
        // side called DownscaleResult. This struct has uppercase fields which are
        // serialized as such. Thus, we serialize using uppercase names so we don't
        // have to make a breaking change to the agent<->informant protocol. Once
        // the informant has been superseded by the monitor, this can be changed back.
        #[serde(rename = "Ok")]
        ok: bool,
        #[serde(rename = "Status")]
        status: String,
    },
    /// Part of the bidirectional heartbeat. The heartbeat is initiated by the
    /// informant.
    /// *Note*: this is a struct variant because of the way go serializes struct{}
    HealthCheck {}
}

/// A message received form the informant.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct InformantMessage {
    #[serde(flatten)]
    pub(crate) inner: InformantMessageInner,
    pub(crate) id: usize,
}

/// The different underlying message types we can receive from the informant.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type", content = "content")]
pub enum InformantMessageInner {
    /// Indicates that the we sent an invalid message, i.e, we couldn't
    /// properly deserialize it.
    InvalidMessage { error: String },
    /// Indicates that the informan experienced an internal error while processing
    /// a message. For example, if it failed to request upsacle from the agent, it
    /// would return an `InternalError`.
    InternalError { error: String },
    /// Indicates to us that we have been granted more resources. We should respond
    /// with an `UpscaleConfirmation` when done handling the resources (increasins
    /// file cache size, cgorup memory limits).
    UpscaleNotification { granted: Allocation },
    /// A request to reduce resource usage. We should response with a `DownscaleResult`,
    /// when done.
    DownscaleRequest { target: Allocation },
    /// Part of the bidirectional heartbeat. The heartbeat is initiated by the
    /// informant.
    /// *Note*: this is a struct variant because of the way go serializes struct{}
    HealthCheck {}
}

/// Represents the resources granted to a VM.
#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct Allocation {
    /// Number of vCPUs
    pub(crate) cpu: f64,
    /// Bytes of memory
    pub(crate) mem: u64,
}

impl Allocation {
    pub fn new(cpu: f64, mem: u64) -> Self {
        Self { cpu, mem }
    }
}

pub const PROTOCOL_MIN_VERSION: ProtocolVersion = ProtocolVersion::V1_0;
pub const PROTOCOL_MAX_VERSION: ProtocolVersion = ProtocolVersion::V1_0;

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Ord, Eq, Serialize_repr, Deserialize_repr)]
#[repr(u8)]
pub enum ProtocolVersion {
    /// Represents v1.0 of the informant<-> monitor protocol - the initial version
    ///
    /// Currently the latest version.
    V1_0 = 1,
}

impl fmt::Display for ProtocolVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProtocolVersion::V1_0 => f.write_str("v1.0"),
        }
    }
}

/// A set of protocol bounds that determines what we are speaking. An invariant
/// that must be maintained is that min <= max. These bounds are inclusive.
#[derive(Deserialize, Debug)]
pub struct ProtocolRange {
    min: ProtocolVersion,
    max: ProtocolVersion,
}

impl fmt::Display for ProtocolRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.min == self.max {
            f.write_fmt(format_args!("{}", self.max))
        } else {
            f.write_fmt(format_args!("{} to {}", self.min, self.max))
        }
    }
}

impl ProtocolRange {
    /// Create a new `ProtocolBounds`. Returns None if min > max
    pub fn new(min: ProtocolVersion, max: ProtocolVersion) -> Option<Self> {
        if min > max {
            None
        } else {
            Some(Self { min, max })
        }
    }

    /// Merge to `ProtocolBounds` to create a range that suitable for both of them.
    pub fn highest_shared_version(&self, other: &Self) -> anyhow::Result<ProtocolVersion> {
        // We first have to make sure the ranges are overlapping. Once we know
        // this, we can merge the ranges by taking the max of the mins and the
        // mins of the maxes.
        if self.min > other.max {
            anyhow::bail!(
                "Non-overlapping bounds: other.max = {} was less than self.min = {}",
                other.max,
                self.min,
            )
        } else if self.max < other.min {
            anyhow::bail!(
                "Non-overlappinng bounds: self.max = {} was less than other.min = {}",
                self.max,
                other.min
            )
        } else {
            Ok(cmp::min(self.max, other.max))
        }
    }
}

/// An enum in disguise for returning the settled on protocol with the informant.
/// If error is None, version should be Some and vice versa. It's set up this way
/// to ease usage on the go side.
#[derive(Serialize, Debug)]
pub struct ProtocolResponse {
    error: Option<String>,
    version: Option<ProtocolVersion>,
}

impl ProtocolResponse {
    pub fn version(version: ProtocolVersion) -> Self {
        Self {
            error: None,
            version: Some(version),
        }
    }

    pub fn error(error: String) -> Self {
        Self {
            error: Some(error),
            version: None,
        }
    }
}
