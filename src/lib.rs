use anyhow::{Context, Result};
use clap::Parser;
use std::fmt::{Debug, Display};
use sysinfo::{RefreshKind, System, SystemExt};
use tap::TapFallible;
use tracing::{error, error_span, span, Level, Span};

#[derive(Parser, Debug)]
pub struct Args {
    #[arg(short, long)]
    cgroup: Option<String>,
    #[arg(short, long)]
    file_cache_conn_str: Option<String>,
}

#[allow(non_upper_case_globals)]
const MiB: u64 = 1 << 20;

/// Convert a quantity in bytes to a quantity in mebibytes, generally for display
/// purposes. (Most calculations in this crate use bytes directly)
pub fn mib(bytes: u64) -> f32 {
    (bytes as f32) / (MiB as f32)
}

pub fn get_total_system_memory() -> u64 {
    System::new_with_specifics(RefreshKind::new().with_memory()).total_memory()
}
/// A trait meant to be implemented for error types that attaches a context to
/// an error as well as logging it out.
pub trait LogContext<T, E>: TapFallible + Context<T, E>
where
    Self::Err: Debug,
{
    /// Log out an error with associated context and attach that context to the
    /// error - thus "tee"ing the context to the logs and the error.
    ///
    // PS: sadly this does not allow attaching additional fields to the log.
    fn tee(self, msg: &'static str) -> Result<T> {
        self.tap_err(|e| error!(error=?e, msg)).context(msg)
    }

    /// Similar to `tee`, but evaluates the provided closure to provide context.
    fn with_tee<F, C>(self, msg: F) -> Result<T>
    where
        C: Display + Send + Sync + 'static,
        F: FnOnce() -> C,
    {
        let ctx = msg();
        self.tap_err(|e| error!(error=?e, "{}", ctx)).context(ctx)
    }
}

impl<T, E> LogContext<T, E> for anyhow::Result<T, E>
where
    <Self as TapFallible>::Err: Debug,
    Self: Context<T, E>,
{
}

/// Code that interfaces with agent
pub mod bridge;
pub mod transport;

pub mod cgroup;
pub mod filecache;
pub mod manager;
pub mod monitor;
pub mod timer;
