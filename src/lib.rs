use clap::Parser;
use std::fmt::Debug;
use sysinfo::{RefreshKind, System, SystemExt};

#[derive(Parser, Debug, Clone)]
pub struct Args {
    #[arg(short, long)]
    cgroup: Option<String>,
    #[arg(short, long)]
    file_cache_conn_str: Option<String>,
    #[arg(short, long, default_value_t = String::from("127.0.0.1:10369"))]
    pub addr: String,
}

impl Args {
    pub fn addr(&self) -> &str {
        &self.addr
    }
}

#[allow(non_upper_case_globals)]
const MiB: u64 = 1 << 20;

// REVIEW: `mib` does not make it clear what the function does. Consider a name that
// describes the "function" of the function?
// Or, perhaps use distinct types to represent "number of bytes" vs "number of
// mebibytes", and then the conversion can be a method (e.g. `to_mib`).
/// Convert a quantity in bytes to a quantity in mebibytes, generally for display
/// purposes. (Most calculations in this crate use bytes directly)
pub fn mib(bytes: u64) -> f32 {
    (bytes as f32) / (MiB as f32)
}

pub fn get_total_system_memory() -> u64 {
    System::new_with_specifics(RefreshKind::new().with_memory()).total_memory()
}

// Code that interfaces with agent
pub mod dispatcher;
pub mod protocol;

pub mod cgroup;
pub mod filecache;
pub mod monitor;
