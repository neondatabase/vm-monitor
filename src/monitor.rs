use std::{fmt::Debug, mem};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use async_std::channel;
use futures_util::StreamExt;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::oneshot,
};
use tracing::{info, debug, warn};

use crate::{
    bridge::Dispatcher,
    cgroup::CgroupState,
    filecache::{FileCacheConfig, FileCacheState},
    get_total_system_memory,
    manager::{Manager, MemoryLimits},
    mib,
    transport::{DownscaleStatus, Packet, Request, Resources, Response, Stage},
    Args, MiB,
};

#[derive(Debug)]
pub struct Monitor<S> {
    config: MonitorConfig,
    filecache: Option<FileCacheState>,
    // TODO: flip it inside out to Arc<Option>?
    cgroup: Option<Arc<CgroupState>>,
    dispatcher: Dispatcher<S>,
}

#[derive(Debug)]
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

impl<S> Monitor<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Debug + 'static,
{
    #[tracing::instrument]
    pub async fn new(config: MonitorConfig, args: Args, stream: S) -> Result<Self> {
        if config.sys_buffer_bytes == 0 {
            bail!("invalid MonitorConfig: ssy_buffer_bytes cannot be 0")
        }
        // TODO: make these bounded? Probably
        let (notified_send, notified_recv) = channel::unbounded();
        let (requesting_send, requesting_recv) = channel::unbounded();

        let dispatcher = Dispatcher::new(stream, notified_send, requesting_recv).await?;

        let mut state = Monitor {
            config,
            filecache: None,
            cgroup: None,
            dispatcher,
        };

        let mut file_cache_reserved_bytes = 0;
        let mem = get_total_system_memory();

        // We need to process file cache initialization before cgroup initialization, so that the memory
        // allocated to the file cache is appropriately taken into account when we decide the cgroup's
        // memory limits.
        if let Some(connstr) = args.file_cache_conn_str {
            info!("Initializing file cache.");
            let config: FileCacheConfig = Default::default();
            if !config.in_memory {
                panic!("file cache not in-memory implemented")
            }

            let file_cache = FileCacheState::new(&connstr, config)
                .await
                .context("Failed to create file cache")?;

            let size = file_cache
                .get_file_cache_size()
                .await
                .context("Error getting file cache size")?;

            let new_size = file_cache.config.calculate_cache_size(mem);
            info!(
                "Initial file cache size: {}, setting to {}",
                mib(size),
                mib(new_size)
            );

            // note: even if size == new_size, we want to explicitly set it, just
            // to make sure that we have the permissions to do so
            let actual_size = file_cache
                .set_file_cache_size(new_size)
                .await
                .context("Failed to set file cache size")?;
            file_cache_reserved_bytes = actual_size;

            state.filecache = Some(file_cache);
        }

        if let Some(name) = args.cgroup {
            info!("Creating manager");
            let manager = Manager::new(name)
                .await
                .context("Failed to create new manager")?;
            let config = Default::default();

            info!("Creating cgroup state");
            let mut cgroup_state =
                CgroupState::new(manager, config, notified_recv, requesting_send);

            let available = mem - file_cache_reserved_bytes;

            cgroup_state.set_memory_limits(available).await?;

            let cgroup_state = Arc::new(cgroup_state);
            let clone = Arc::clone(&cgroup_state);
            state.cgroup = Some(cgroup_state);

            tokio::spawn(async move { clone.handle_cgroup_signals_loop().await });
        } else {
            // We need to forget the sender so that its drop impl does not get ran.
            // This allows us to poll it in `Monitor::run` regardless of whether we
            // are managing a cgroup or not. If we don't forget it, all receives will
            // immediately return an error because the sender is droped and it will
            // claim all select! statements, effectively turning `Monitor::run` into
            // `loop { fail to receive }`.
            mem::forget(requesting_send);
        }

        Ok(state)
    }

    /// Attempt to downscale filecache + cgroup
    #[tracing::instrument(skip(self))]
    pub async fn try_downscale(&self, target: Resources) -> Result<DownscaleStatus> {
        info!("Attempting to downscale to {target:?}");

        // Nothing to adjust
        if self.cgroup.is_none() && self.filecache.is_none() {
            info!("No action needed for downscale (no cgroup or file cache enabled)");
            return Ok(DownscaleStatus::new(
                true,
                "monitor is not managing cgroup or file cache".to_string(),
            ));
        }

        let requested_mem = target.mem;
        let usable_system_memory = requested_mem.saturating_sub(self.config.sys_buffer_bytes);
        let expected_file_cache_mem_usage = if let Some(file_cache) = &self.filecache {
            file_cache.config.calculate_cache_size(usable_system_memory)
        } else {
            0
        };

        let mut new_cgroup_mem_high = 0;
        if let Some(cgroup) = &self.cgroup {
            new_cgroup_mem_high = cgroup
                .config
                .calculate_memory_high_value(usable_system_memory - expected_file_cache_mem_usage);

            let current = cgroup
                .get_current_memory()
                .context("Failed to fetch cgroup memory")?;

            if new_cgroup_mem_high < current + cgroup.config.memory_high_buffer_bytes {
                let status = format!(
                    "{}: {} MiB (new high) < {} (current usage) + {} (buffer)",
                    "Calculated memory.high too low",
                    mib(new_cgroup_mem_high),
                    mib(current),
                    mib(cgroup.config.memory_high_buffer_bytes)
                );

                info!("Discontinuing downscale: {status}");

                return Ok(DownscaleStatus::new(false, status));
            }
        }

        // The downscaling has been approved. Downscale the file cache, then the cgroup.
        let mut status = vec![];
        let mut file_cache_mem_usage = 0;
        if let Some(file_cache) = &self.filecache {
            if !file_cache.config.in_memory {
                panic!("file cache not in-memory unimplemented")
            }

            let actual_usage = file_cache
                .set_file_cache_size(expected_file_cache_mem_usage)
                .await
                .context("failed to set file cache size")?;
            file_cache_mem_usage = actual_usage;
            status.push(format!("Set file cache size to {} MiB", mib(actual_usage)));
        }

        if let Some(cgroup) = &self.cgroup {
            let available_memory = usable_system_memory - file_cache_mem_usage;

            if file_cache_mem_usage != expected_file_cache_mem_usage {
                new_cgroup_mem_high = cgroup.config.calculate_memory_high_value(available_memory);
            }

            let limits = MemoryLimits::new(
                // new_cgroup_mem_high is initialized to 0 but it is guaranteed to not be here
                // since it is properly initialized in the previous cgroup if let block
                new_cgroup_mem_high,
                available_memory,
            );
            cgroup
                .manager
                .set_limits(limits)
                .context("Failed to set cgroup memory limits")?;

            status.push(format!(
                "Set cgroup memory.high to {} MiB, of new max {} MiB",
                mib(new_cgroup_mem_high),
                mib(available_memory)
            ));
        }

        // TODO: make this status thing less jank
        info!("Downscale successful: {}", status.join("; "));

        Ok(DownscaleStatus::new(true, status.join("; ")))
    }

    /// Handle new resources
    #[tracing::instrument(skip(self))]
    pub async fn handle_upscale(&self, resources: Resources) -> Result<()> {
        info!("Handling agent-granted upscale to {resources:?}");

        if self.filecache.is_none() && self.cgroup.is_none() {
            info!("No action needed for upscale (no cgroup or file cache enabled)");
            return Ok(());
        }

        let new_mem = resources.mem;
        let usable_system_memory = new_mem.saturating_sub(self.config.sys_buffer_bytes);

        // Get the file cache's expected contribution to the memory usage
        let mut file_cache_mem_usage = 0;
        if let Some(file_cache) = &self.filecache {
            if !file_cache.config.in_memory {
                panic!("File cache not in-memory unimplemented");
            }

            let expected_usage = file_cache.config.calculate_cache_size(usable_system_memory);
            info!(
                "Updating file cache size, target = {} MiB, total_memory = {} MiB",
                mib(expected_usage),
                mib(new_mem)
            );

            let actual_usage = file_cache.set_file_cache_size(expected_usage).await?;

            if actual_usage != expected_usage {
                warn!(
                    "File cache was set to a different size that we wanted: target = {} Mib, actual= {} Mib",
                    mib(expected_usage),
                    mib(actual_usage)
                )
            }
            file_cache_mem_usage = actual_usage;
        }

        if let Some(cgroup) = &self.cgroup {
            info!("cgroup: {}", cgroup.manager.name);

            let available_memory = usable_system_memory - file_cache_mem_usage;
            let new_cgroup_mem_high = cgroup.config.calculate_memory_high_value(available_memory);
            info!(
                "Updating cgroup memory.high: target = {} MiB, total_memory = {} MiB",
                mib(new_cgroup_mem_high),
                mib(new_mem)
            );
            let limits = MemoryLimits::new(new_cgroup_mem_high, available_memory);
            cgroup.manager.set_limits(limits)?;
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn process_packet(&mut self, packet: Packet) -> Result<Option<Packet>> {
        let id = packet.id;
        match packet.stage {
            Stage::Request(req) => match req {
                Request::RequestUpscale {} => {
                    unreachable!("Informant should never send a Request::RequestUpscale")
                }
                Request::NotifyUpscale(resources) => {
                    let (tx, rx) = oneshot::channel();
                    self.handle_upscale(resources).await?;
                    self.dispatcher
                        .notify_upscale_events
                        .send((resources, tx))
                        .await?;
                    rx.await?;
                    info!("Confirmed receipt of upscale by cgroup manager");
                    Ok(Some(Packet::new(
                        Stage::Response(Response::ResourceConfirmation {}),
                        id,
                    )))
                }
                Request::TryDownscale(resources) => Ok(Some(Packet::new(
                    Stage::Response(Response::DownscaleResult(
                        self.try_downscale(resources).await?,
                    )),
                    id,
                ))),
            },
            Stage::Response(res) => match res {
                Response::UpscaleResult(resources) => {
                    let (tx, rx) = oneshot::channel();
                    self.handle_upscale(resources).await?;
                    self.dispatcher
                        .notify_upscale_events
                        .send((resources, tx))
                        .await?;
                    rx.await?;
                    info!("Confirmed receipt of requested upscale by cgroup manager");
                    Ok(Some(Packet::new(
                        Stage::Done {},
                        0, // FIXME
                    )))
                }
                Response::ResourceConfirmation {} => {
                    unreachable!("Informant should never receive a Response::ResourceConfirmation")
                }
                Response::DownscaleResult(_) => {
                    unreachable!("Informant should never receive a Response::DownscaleResult")
                }
            },
            Stage::Done {} => Ok(None), // yay! :)
        }
    }

    // TODO: don't propagate errors, probably just warn!?
    #[tracing::instrument(skip(self))]
    pub async fn run(&mut self) -> Result<()> {
        info!("Starting dispatcher.");
        loop {
            // TODO: refactor this
            // check if we need to propagate a request
            let msg = tokio::select! {
                sender = self.dispatcher.request_upscale_events.recv() => {
                    match sender {
                        Ok(sender) => {
                            info!("cgroup asking for upscale. Forwarding request.");
                            self.dispatcher.send(
                                Packet::new(Stage::Request(Request::RequestUpscale {}), 0)
                            ).await?;
                            if let Ok(_) = sender.send(()) {
                                warn!("Error sending confirmation of upscale request to cgroup");
                            };
                        },
                        Err(e) => warn!("Error receiving upscale event: {e}")
                    }
                    continue
                }
                msg = self.dispatcher.source.next() => {
                    msg
                }
            };
            if let Some(msg) = msg {
                debug!("Received packet: {msg:?}");
                // Maybe have another thread do this work? Can lead to out of order?
                match msg {
                    Ok(msg) => {
                        // Inlined for borrow checker reasons:
                        // We only want to borrow self for as short a time as possible
                        // and a closure borrows self for it's entire lifetime - which
                        // is too long and prevents reading/writing the stream.
                        let packet: Packet = match msg {
                            tokio_tungstenite::tungstenite::Message::Text(text) => {
                                serde_json::from_str(&text).unwrap()
                            }
                            tokio_tungstenite::tungstenite::Message::Binary(bin) => {
                                serde_json::from_slice(&bin).unwrap()
                            }
                            _ => continue,
                        };

                        let Some(out) = self.process_packet(packet).await? else {
                            continue
                        };

                        // Technically doesn't need a block, but just putting one for
                        // clarity since the borrowing in this section is tricky
                        {
                            self.dispatcher.send(out).await?;
                        }
                    }
                    Err(e) => warn!("{e}"),
                }
            } else {
                bail!("connection closed")
            }
        }
    }
}
