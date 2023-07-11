//! # `monitor`
//!
//! Where the action happens: starting monitor, handling packets received from
//! informant and sending upscale requests

use std::sync::Arc;
use std::{fmt::Debug, mem};

use async_std::channel;
use futures_util::StreamExt;
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::{debug, info, warn};

use crate::{
    bridge::Dispatcher,
    cgroup::CgroupState,
    filecache::{FileCacheConfig, FileCacheState},
    get_total_system_memory,
    manager::{Manager, MemoryLimits},
    mib,
    transport::{DownscaleStatus, Packet, Request, Resources, Response, Stage},
    Args, LogContext, MiB,
};

/// Central struct that interacts with informant, dispatcher, and cgroup to handle
/// signals from the informant.
#[derive(Debug)]
pub struct Monitor<S> {
    config: MonitorConfig,
    filecache: Option<FileCacheState>,
    // TODO: flip it inside out to Arc<Option>?
    cgroup: Option<Arc<CgroupState>>,
    dispatcher: Dispatcher<S>,
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

impl<S> Monitor<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Debug + 'static,
{
    /// Create a new monitor.
    #[tracing::instrument]
    pub async fn new(config: MonitorConfig, args: Args, stream: S) -> anyhow::Result<Self> {
        if config.sys_buffer_bytes == 0 {
            anyhow::bail!("invalid MonitorConfig: ssy_buffer_bytes cannot be 0")
        }
        // TODO: make these bounded? Probably
        //
        // *NOTE*: the dispatcher and cgroup manager talk through these channels
        // so make sure they each get the correct half, nothing is droppped, etc.
        let (notified_send, notified_recv) = channel::unbounded();
        let (requesting_send, requesting_recv) = channel::unbounded();

        let dispatcher = Dispatcher::new(stream, notified_send, requesting_recv)
            .await
            .tee("error creating new dispatcher")?;

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
            info!(action = "initializing file cache");
            let config: FileCacheConfig = Default::default();
            if !config.in_memory {
                panic!("file cache not in-memory implemented")
            }

            let file_cache = FileCacheState::new(&connstr, config)
                .await
                .tee("failed to create file cache")?;

            let size = file_cache
                .get_file_cache_size()
                .await
                .tee("error getting file cache size")?;

            let new_size = file_cache.config.calculate_cache_size(mem);
            info!(
                initial = mib(size),
                new = mib(new_size),
                action = "setting initial file cache size",
            );

            // note: even if size == new_size, we want to explicitly set it, just
            // to make sure that we have the permissions to do so
            let actual_size = file_cache
                .set_file_cache_size(new_size)
                .await
                .tee("failed to set file cache size, possibly due to inadequate permissions")?;
            file_cache_reserved_bytes = actual_size;

            state.filecache = Some(file_cache);
        }

        if let Some(name) = args.cgroup {
            info!(action = "creating manager");
            let manager = Manager::new(name)
                .await
                .tee("failed to create new manager")?;
            let config = Default::default();

            info!(action = "creating cgroup state");
            let mut cgroup_state =
                CgroupState::new(manager, config, notified_recv, requesting_send);

            let available = mem - file_cache_reserved_bytes;

            cgroup_state
                .set_memory_limits(available)
                .await
                .tee("failed to set cgroup memory limits")?;

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
    pub async fn try_downscale(&self, target: Resources) -> anyhow::Result<DownscaleStatus> {
        info!(?target, action = "attempting to downscale");

        // Nothing to adjust
        if self.cgroup.is_none() && self.filecache.is_none() {
            info!("no action needed for downscale (no cgroup or file cache enabled)");
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
                .tee("failed to fetch cgroup memory")?;

            if new_cgroup_mem_high < current + cgroup.config.memory_high_buffer_bytes {
                let status = format!(
                    "{}: {} MiB (new high) < {} (current usage) + {} (buffer)",
                    "calculated memory.high too low",
                    mib(new_cgroup_mem_high),
                    mib(current),
                    mib(cgroup.config.memory_high_buffer_bytes)
                );

                info!(status, action = "discontinuing downscale");

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
                .tee("failed to set file cache size")?;
            file_cache_mem_usage = actual_usage;
            status.push(format!("set file cache size to {} MiB", mib(actual_usage)));
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
                .set_limits(&limits)
                .tee("failed to set cgroup memory limits")?;

            status.push(format!(
                "set cgroup memory.high to {} MiB, of new max {} MiB",
                mib(new_cgroup_mem_high),
                mib(available_memory)
            ));
        }

        // TODO: make this status thing less jank
        let status = status.join("; ");

        info!(status, "downscale successful");

        Ok(DownscaleStatus::new(true, status))
    }

    /// Handle new resources
    #[tracing::instrument(skip(self))]
    pub async fn handle_upscale(&self, resources: Resources) -> anyhow::Result<()> {
        info!(?resources, action = "handling agent-granted upscale");

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
                action = "npdating file cache size",
            );

            let actual_usage = file_cache
                .set_file_cache_size(expected_usage)
                .await
                .tee("failed to set file cache size")?;

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
                action = "updating cgroup memory.high",
            );
            let limits = MemoryLimits::new(new_cgroup_mem_high, available_memory);
            cgroup
                .manager
                .set_limits(&limits)
                .tee("failed to set file cache size")?;
        }

        info!("upscale handling successful");

        Ok(())
    }

    /// Take in a pack and perform some action, such as downscaling or upscaling,
    /// and return a packet to be send back.
    #[tracing::instrument(skip(self))]
    pub async fn process_packet(
        &mut self,
        Packet { id, stage }: Packet,
    ) -> anyhow::Result<Option<Packet>> {
        match stage {
            Stage::Request(req) => match req {
                Request::RequestUpscale {} => {
                    unreachable!("informant should never send a Request::RequestUpscale")
                }
                Request::NotifyUpscale(resources) => {
                    self.handle_upscale(resources)
                        .await
                        .tee("failed to handle upscale")?;
                    let rx = self
                        .dispatcher
                        .notify_upscale(resources)
                        .await
                        .tee("failed to notify cgroup of upscale event")?;
                    rx.await
                        .tee("failed to confirm receipt of upscale by cgroup manager")?;
                    info!("confirmed receipt of upscale by cgroup manager");
                    Ok(Some(Packet::new(
                        Stage::Response(Response::ResourceConfirmation {}),
                        id,
                    )))
                }
                Request::TryDownscale(resources) => Ok(Some(Packet::new(
                    Stage::Response(Response::DownscaleResult(
                        self.try_downscale(resources)
                            .await
                            .tee("failed to downscale")?,
                    )),
                    id,
                ))),
            },
            Stage::Response(res) => match res {
                Response::UpscaleResult(resources) => {
                    self.handle_upscale(resources)
                        .await
                        .tee("failed to handle upscale")?;
                    let rx = self
                        .dispatcher
                        .notify_upscale(resources)
                        .await
                        .tee("failed to notify cgroup of upscale event")?;
                    rx.await
                        .tee("confirmed receipt of upscale by cgroup manager")?;
                    info!("confirmed receipt of requested upscale by cgroup manager");
                    Ok(Some(Packet::new(Stage::Done {}, id)))
                }
                Response::ResourceConfirmation {} => {
                    unreachable!("informant should never receive a Response::ResourceConfirmation")
                }
                Response::DownscaleResult(_) => {
                    unreachable!("informant should never receive a Response::DownscaleResult")
                }
            },
            Stage::Done {} => Ok(None), // yay! :)
        }
    }

    // TODO: don't propagate errors, probably just warn!?
    #[tracing::instrument(skip(self))]
    pub async fn run(&mut self) -> anyhow::Result<()> {
        info!(action = "starting dispatcher");
        loop {
            tokio::select! {
                // we need to propagate an upscale request
                sender = self.dispatcher.request_upscale_events.recv() => {
                    match sender {
                        Ok(sender) => {
                            info!("cgroup asking for upscale; forwarding request");
                            self.counter += 1;
                            self.dispatcher
                                .send(Packet::new(Stage::Request(Request::RequestUpscale {}), self.counter))
                                .await
                                .tee("failed to send packet")?;
                            if let Err(_) = sender.send(()) {
                                warn!("error sending confirmation of upscale request to cgroup");
                            };
                        },
                        Err(e) => warn!("error receiving upscale event: {e}")
                    }
                }
                // there is a packet from the informant
                msg = self.dispatcher.source.next() => {
                    if let Some(msg) = msg {
                        debug!(packet = ?msg, action = "receiving packet");
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

                                let Some(out) = self
                                    .process_packet(packet)
                                    .await
                                    .tee("failed to process packet")?
                                else {
                                    continue
                                };

                                // Technically doesn't need a block, but just putting one for
                                // clarity since the borrowing in this section is tricky
                                {
                                    self.dispatcher
                                        .send(out)
                                        .await
                                        .tee("failed to send packet")?;
                                }
                            }
                            Err(e) => warn!("{e}"),
                        }
                    } else {
                        anyhow::bail!("connection closed")
                    }
                }
            }
        }
    }
}
