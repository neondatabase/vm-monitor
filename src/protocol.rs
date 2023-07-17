use core::fmt;
use std::cmp;

use serde::{Deserialize, Serialize};

pub const PROTOCOL_MIN_VERSION: ProtocolVersion = ProtocolVersion::V1_0;
pub const PROTOCOL_MAX_VERSION: ProtocolVersion = ProtocolVersion::V1_0;

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Ord, Eq, Serialize, Deserialize)]
pub enum ProtocolVersion {
    /// Represents v1.0 of the informant<-> monitor protocol - the initial version
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
/// that must be maintained is that min <= max
#[derive(Deserialize, Debug)]
pub struct ProtocolBounds {
    min: ProtocolVersion,
    max: ProtocolVersion,
}

impl fmt::Display for ProtocolBounds {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.min == self.max {
            f.write_fmt(format_args!("{}", self.max))
        } else {
            f.write_fmt(format_args!("{} to {}", self.min, self.max))
        }
    }
}

impl ProtocolBounds {
    /// Create a new `ProtoBounds`. Returns None if min > max
    pub fn new(min: ProtocolVersion, max: ProtocolVersion) -> Option<Self> {
        if min > max {
            None
        } else {
            Some(Self { min, max })
        }
    }

    /// Merge to `ProtoBounds` to create a range that suitable for both of them.
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
