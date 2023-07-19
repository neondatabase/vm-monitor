//! # `monitor`
//!
//! Where the action happens: starting monitor, handling packets received from
//! informant and sending upscale requests

use std::sync::Arc;
use std::{fmt::Debug, mem};

use anyhow::Context;
use async_std::channel;
use axum::extract::ws::{Message, WebSocket};
use futures_util::{StreamExt, TryFutureExt};
use tracing::{debug, info, warn};

use crate::{
    cgroup::CgroupState,
    dispatcher::Dispatcher,
    filecache::{FileCacheConfig, FileCacheState},
    get_total_system_memory,
    manager::{Manager, MemoryLimits},
    mib,
    protocol::{
        Allocation, InformantMessage, InformantMessageInner, MonitorMessage, MonitorMessageInner,
    },
    Args, MiB,
};

/// Central struct that interacts with informant, dispatcher, and cgroup to handle
/// signals from the informant.
#[derive(Debug)]
pub struct Monitor {
    config: MonitorConfig,
    filecache: Option<FileCacheState>,
    cgroup: Option<Arc<CgroupState>>,
    dispatcher: Dispatcher,

    /// We "mint" new package ids by incrementing this counter and taking the value.
    counter: usize,
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

impl Monitor {
    /// Create a new monitor.
    #[tracing::instrument(skip(ws))]
    pub async fn new(config: MonitorConfig, args: Args, ws: WebSocket) -> anyhow::Result<Self> {
        anyhow::ensure!(
            config.sys_buffer_bytes != 0,
            "invalid MonitorConfig: ssy_buffer_bytes cannot be 0"
        );
        // TODO: make these bounded? Probably
        //
        // *NOTE*: the dispatcher and cgroup manager talk through these channels
        // so make sure they each get the correct half, nothing is droppped, etc.
        let (notified_send, notified_recv) = channel::unbounded();
        let (requesting_send, requesting_recv) = channel::unbounded();

        let dispatcher = Dispatcher::new(ws, notified_send, requesting_recv)
            .await
            .context("error creating new dispatcher")?;

        let mut state = Monitor {
            config,
            filecache: None,
            cgroup: None,
            dispatcher,
            counter: 0,
        };

        let mut file_cache_reserved_bytes = 0;
        let mem = get_total_system_memory();

        // We need to process file cache initialization before cgroup initialization, so that the memory
        // allocated to the file cache is appropriately taken into account when we decide the cgroup's
        // memory limits.
        if let Some(connstr) = args.file_cache_conn_str {
            info!("initializing file cache");
            let config: FileCacheConfig = Default::default();
            if !config.in_memory {
                panic!("file cache not in-memory implemented")
            }

            let file_cache = FileCacheState::new(&connstr, config)
                .await
                .context("failed to create file cache")?;

            let size = file_cache
                .get_file_cache_size()
                .await
                .context("error getting file cache size")?;

            let new_size = file_cache.config.calculate_cache_size(mem);
            info!(
                initial = mib(size),
                new = mib(new_size),
                "setting initial file cache size",
            );

            // note: even if size == new_size, we want to explicitly set it, just
            // to make sure that we have the permissions to do so
            let actual_size = file_cache
                .set_file_cache_size(new_size)
                .await
                .context("failed to set file cache size, possibly due to inadequate permissions")?;
            if actual_size != new_size {
                info!("file cache size actually got set to {actual_size}")
            }
            file_cache_reserved_bytes = actual_size;

            state.filecache = Some(file_cache);
        }

        if let Some(name) = args.cgroup {
            info!("creating manager");
            let manager = Manager::new(name)
                .await
                .context("failed to create new manager")?;
            let config = Default::default();

            let mut cgroup_state =
                CgroupState::new(manager, config, notified_recv, requesting_send);

            let available = mem - file_cache_reserved_bytes;

            cgroup_state
                .set_memory_limits(available)
                .await
                .context("failed to set cgroup memory limits")?;

            let cgroup_state = Arc::new(cgroup_state);
            let clone = Arc::clone(&cgroup_state);
            state.cgroup = Some(cgroup_state);

            tokio::spawn(async move { clone.handle_cgroup_signals_loop().await });
        } else {
            // *NOTE*: We need to forget the sender so that its drop impl does not get ran.
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
    pub async fn try_downscale(&self, target: Allocation) -> anyhow::Result<(bool, String)> {
        // Nothing to adjust
        if self.cgroup.is_none() && self.filecache.is_none() {
            info!("no action needed for downscale (no cgroup or file cache enabled)");
            return Ok((
                true,
                "monitor is not managing cgroup or file cache".to_string(),
            ));
        }

        let requested_mem = target.mem;
        let usable_system_memory = requested_mem.saturating_sub(self.config.sys_buffer_bytes);
        let expected_file_cache_mem_usage = self
            .filecache
            .as_ref()
            .map(|file_cache| file_cache.config.calculate_cache_size(usable_system_memory))
            .unwrap_or(0);
        let mut new_cgroup_mem_high = 0;
        if let Some(cgroup) = &self.cgroup {
            new_cgroup_mem_high = cgroup
                .config
                .calculate_memory_high_value(usable_system_memory - expected_file_cache_mem_usage);

            let current = cgroup
                .get_current_memory()
                .context("failed to fetch cgroup memory")?;

            if new_cgroup_mem_high < current + cgroup.config.memory_high_buffer_bytes {
                let status = format!(
                    "{}: {} MiB (new high) < {} (current usage) + {} (buffer)",
                    "calculated memory.high too low",
                    mib(new_cgroup_mem_high),
                    mib(current),
                    mib(cgroup.config.memory_high_buffer_bytes)
                );

                info!(status, "discontinuing downscale");

                return Ok((false, status));
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
            let message = format!("set file cache size to {} MiB", mib(actual_usage));
            info!("downscale: {message}");
            status.push(message);
        }

        if let Some(cgroup) = &self.cgroup {
            let available_memory = usable_system_memory - file_cache_mem_usage;

            if file_cache_mem_usage != expected_file_cache_mem_usage {
                new_cgroup_mem_high = cgroup.config.calculate_memory_high_value(available_memory);
            }

            let limits = MemoryLimits::new(
                // new_cgroup_mem_high is initialized to 0 but it is guarancontextd to not be here
                // since it is properly initialized in the previous cgroup if let block
                new_cgroup_mem_high,
                available_memory,
            );
            cgroup
                .manager
                .set_limits(&limits)
                .context("failed to set cgroup memory limits")?;

            let message = format!(
                "set cgroup memory.high to {} MiB, of new max {} MiB",
                mib(new_cgroup_mem_high),
                mib(available_memory)
            );
            info!("downscale: message");
            status.push(message);
        }

        // TODO: make this status thing less jank
        let status = status.join("; ");
        Ok((true, status))
    }

    /// Handle new resources
    #[tracing::instrument(skip(self))]
    pub async fn handle_upscale(&self, resources: Allocation) -> anyhow::Result<()> {
        if self.filecache.is_none() && self.cgroup.is_none() {
            info!("no action needed for upscale (no cgroup or file cache enabled)");
            return Ok(());
        }

        let new_mem = resources.mem;
        let usable_system_memory = new_mem.saturating_sub(self.config.sys_buffer_bytes);

        // Get the file cache's expected contribution to the memory usage
        let mut file_cache_mem_usage = 0;
        if let Some(file_cache) = &self.filecache {
            if !file_cache.config.in_memory {
                panic!("file cache not in-memory unimplemented");
            }

            let expected_usage = file_cache.config.calculate_cache_size(usable_system_memory);
            info!(
                target = mib(expected_usage),
                total = mib(new_mem),
                "updating file cache size",
            );

            let actual_usage = file_cache
                .set_file_cache_size(expected_usage)
                .await
                .context("failed to set file cache size")?;

            if actual_usage != expected_usage {
                warn!(
                    "file cache was set to a different size that we wanted: target = {} Mib, actual= {} Mib",
                    mib(expected_usage),
                    mib(actual_usage)
                )
            }
            file_cache_mem_usage = actual_usage;
        }

        if let Some(cgroup) = &self.cgroup {
            let available_memory = usable_system_memory - file_cache_mem_usage;
            let new_cgroup_mem_high = cgroup.config.calculate_memory_high_value(available_memory);
            info!(
                target = mib(new_cgroup_mem_high),
                total = mib(new_mem),
                name = cgroup.manager.name,
                "updating cgroup memory.high",
            );
            let limits = MemoryLimits::new(new_cgroup_mem_high, available_memory);
            cgroup
                .manager
                .set_limits(&limits)
                .context("failed to set file cache size")?;
        }

        Ok(())
    }

    /// Take in a packet and perform some action, such as downscaling or upscaling,
    /// and return a packet to be send back.
    #[tracing::instrument(skip(self))]
    pub async fn process_packet(
        &mut self,
        InformantMessage { inner, id }: InformantMessage,
    ) -> anyhow::Result<Option<MonitorMessage>> {
        match inner {
            InformantMessageInner::UpscaleNotification { granted } => {
                // See the TryFutureExt
                // trait wonderfully located
                // in the futures crate
                self.handle_upscale(granted)
                    .map_err(|e| e.context("failed to handle upscale"))
                    .and_then(|_| self.dispatcher.notify_upscale(granted))
                    .map_err(|e| e.context("failed to notify cgroup of upscale event"))
                    .await
                    .map(|_| {
                        Some(MonitorMessage::new(
                            MonitorMessageInner::UpscaleConfirmation {},
                            id,
                        ))
                    })
            }
            InformantMessageInner::DownscaleRequest { target } => self
                .try_downscale(target)
                .await
                .context("failed to downscale")
                .map(|(ok, status)| {
                    Some(MonitorMessage::new(
                        MonitorMessageInner::DownscaleResult { ok, status },
                        id,
                    ))
                }),
            InformantMessageInner::InvalidMessage { error } => {
                warn!(
                    error,
                    id, "received notification of an invalid message we sent"
                );
                Ok(None)
            }
            InformantMessageInner::InternalError { error } => {
                warn!(error, id, "informant experienced an internal error");
                Ok(None)
            }
        }
    }

    // TODO: don't propagate errors, probably just warn!?
    #[tracing::instrument(skip(self))]
    pub async fn run(&mut self) -> anyhow::Result<()> {
        info!("starting dispatcher");
        loop {
            tokio::select! {
                // we need to propagate an upscale request
                sender = self.dispatcher.request_upscale_events.recv() => {
                    match sender {
                        Ok(sender) => {
                            info!("cgroup asking for upscale; forwarding request");
                            self.counter += 1;
                            self.dispatcher
                                .send(MonitorMessage::new(MonitorMessageInner::UpscaleRequest {}, self.counter))
                                .await
                                .context("failed to send packet")?;
                            if sender.send(()).is_err() {
                                warn!("error sending confirmation of upscale request to cgroup");
                            };
                        },
                        Err(e) => warn!("error receiving upscale event: {e}")
                    }
                }
                // there is a packet from the informant
                msg = self.dispatcher.source.next() => {
                    if let Some(msg) = msg {
                        debug!(packet = ?msg, "receiving packet");
                        // TODO: do we need to offload this work to another thread?
                        match msg {
                            Ok(msg) => {
                                let packet: InformantMessage = match msg {
                                    Message::Text(text) => {
                                        serde_json::from_str(&text).context("failed to deserialize text message")?
                                    }
                                    other => {
                                        warn!(
                                            message=?other,
                                            "informant should only send text messages but received different type"
                                        );
                                        continue
                                    },
                                };

                                let out = match self.process_packet(packet.clone()).await {
                                    Ok(Some(out)) => out,
                                    Ok(None) => continue,
                                    Err(e) => {
                                        warn!(error = &e.to_string(), "error handling packet");
                                        MonitorMessage::new(
                                            MonitorMessageInner::InternalError {
                                                error: e.to_string()
                                            },
                                            packet.id
                                        )
                                    }
                                };

                                self.dispatcher
                                    .send(out)
                                    .await
                                    .context("failed to send packet")?;
                            }
                            Err(e) => warn!("{e}"),
                        }
                    } else {
                        anyhow::bail!("dispatcher connection closed")
                    }
                }
            }
        }
    }
}
