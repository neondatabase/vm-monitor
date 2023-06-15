use std::sync::Arc;

use anyhow::{bail, Context, Result};
use tracing::info;

use crate::{
    cgroup::CgroupState,
    filecache::{FileCacheConfig, FileCacheState},
    get_total_system_memory,
    manager::Manager,
    mib, Args, MiB,
};

pub struct Monitor {
    config: MonitorConfig,
    // TODO: flip it inside out to Arc<Option>?
    cgroup: Option<Arc<CgroupState>>,
    filecache: Option<FileCacheState>,

    // TODO: is this comment still applicable
    // file_cache_reserved_bytes stores the amount of memory that's currently reserved for the file
    // cache.
    //
    // This field is mostly used during initialization, where it allows us to pass state from the
    // file cache's startup hook to the cgroup's hook.
    //
    // There's definitely better ways of doing this, but the solution we have will work for now.
    file_cache_reserved_bytes: u64,
}

pub struct MonitorConfig {
    /// `sys_buffer_bytes` gives the estimated amount of memory, in bytes, that the kernel uses before
    /// handing out the rest to userspace. This value is the estimated difference between the
    /// *actual* physical memory and the amount reported by `grep MemTotal /proc/meminfo`.
    ///
    /// For more information, refer to `man 5 proc`, which defines MemTotal as "Total usable RAM
    /// (i.e., physical RAM minus a few reserved bits and the kernel binary code)".
    ///
    /// We only use `sys_buffer_bytes` when calculating the system memory from the *external* memory
    /// size, rather than the self-reported memory size, according to the kernel.
    ///
    /// TODO: this field is only necessary while we still have to trust the autoscaler-agent's
    /// upscale resource amounts (because we might not *actually* have been upscaled yet). This field
    /// should be removed once we have a better solution there.
    sys_buffer_bytes: u64,
}

impl Default for MonitorConfig {
    fn default() -> Self {
        Self {
            sys_buffer_bytes: 100 * MiB,
        }
    }
}

impl Monitor {
    pub async fn new(config: MonitorConfig, args: Args) -> Result<Self> {
        if config.sys_buffer_bytes == 0 {
            bail!("invalid MonitorConfig: ssy_buffer_bytes cannot be 0")
        }

        let mut state = Monitor {
            config,
            cgroup: None,
            filecache: None,
            file_cache_reserved_bytes: 0,
        };

        let mem = get_total_system_memory();

        // We need to process file cache initialization before cgroup initialization, so that the memory
        // allocated to the file cache is appropriately taken into account when we decide the cgroup's
        // memory limits.
        if let Some(connstr) = args.file_cache_conn_str {
            let config: FileCacheConfig = Default::default();
            if !config.in_memory {
                panic!("file cache not in-memory implemented")
            }

            let mut file_cache = FileCacheState::new(&connstr, config)?;
            let size = file_cache
                .get_file_cache_size()
                .context("Error getting file cache size")?;

            let new_size = file_cache.config.calculate_cache_size(mem);
            info!(
                "Initial file cache size: {}, setting to {}",
                mib(size),
                mib(new_size)
            );

            // note: even if size == new_size, we want to explicitly set it, just
            // to make sure that we have the permissions to do so
            let actual_size = file_cache.set_file_cache_size(new_size)?;
            state.file_cache_reserved_bytes = actual_size;

            state.filecache = Some(file_cache);
        }

        if let Some(name) = args.cgroup {
            let manager = Manager::new(name).await?;
            let config = Default::default();
            let mut cgroup_state = CgroupState::new(manager, config);
            let available = mem - state.file_cache_reserved_bytes;

            cgroup_state.set_memory_limits(available).await?;

            let cgroup_state = Arc::new(cgroup_state);
            let clone = Arc::clone(&cgroup_state);
            state.cgroup = Some(cgroup_state);
            tokio::spawn(async move { clone.handle_cgroup_signals_loop().await });
        }

        Ok(state)
    }
}
