use clap::Parser;
use sysinfo::{RefreshKind, System, SystemExt};

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

/// Code that interfaces with agent
pub mod bridge;
pub mod cgroup;
pub mod filecache;
pub mod manager;
pub mod monitor;
pub mod timer;
