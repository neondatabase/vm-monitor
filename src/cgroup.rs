use std::{future, sync::Arc, time::Instant};

use anyhow::{bail, Result};
use async_std::channel::{Receiver, Sender, TryRecvError};
use tokio::sync::oneshot;
use tracing::info;

use crate::{
    manager::{Manager, MemoryLimits},
    mib,
    timer::Timer,
    transport::Resources,
    LogContext, MiB,
};

#[derive(Debug)]
pub struct CgroupState {
    pub(crate) manager: Manager,
    pub(crate) config: CgroupConfig,

    // For communication with dispatcher
    notify_upscale_events: Receiver<(Resources, oneshot::Sender<()>)>,
    request_upscale_events: Sender<oneshot::Sender<()>>,
}

#[derive(Debug)]
pub struct CgroupConfig {
    // oom_buffer_bytes gives the target difference between the total memory reserved for the cgroup
    // and the value of the cgroup's memory.high.
    //
    // In other words, memory.high + OOMBufferBytes will equal the total memory that the cgroup may
    // use (equal to system memory, minus whatever's taken out for the file cache).
    oom_buffer_bytes: u64,

    // memory_high_buffer_bytes gives the amount of memory, in bytes, below a proposed new value for
    // memory.high that the cgroup's memory usage must be for us to downscale
    //
    // In other words, we can downscale only when:
    //
    //   memory.current + memory_high_buffer_bytes < (proposed) memory.high
    //
    // TODO: there's some minor issues with this approach -- in particular, that we might have
    // memory in use by the kernel's page cache that we're actually ok with getting rid of.
    pub(crate) memory_high_buffer_bytes: u64,

    // max_upscale_wait_millis gives the maximum duration, in milliseconds, that we're allowed to pause
    // the cgroup for while waiting for the autoscaler-agent to upscale us
    max_upscale_wait_millis: u64,

    // do_not_freeze_more_often_than_millis gives a required minimum time, in milliseconds, that we must
    // wait before re-freezing the cgroup while waiting for the autoscaler-agent to upscale us.
    do_not_freeze_more_often_than_millis: u64,

    // memory_high_increase_by_bytes gives the amount of memory, in bytes, that we should periodically
    // increase memory.high by while waiting for the autoscaler-agent to upscale us.
    //
    // This exists to avoid the excessive throttling that happens when a cgroup is above its
    // memory.high for too long. See more here:
    // https://github.com/neondatabase/autoscaling/issues/44#issuecomment-1522487217
    memory_high_increase_by_bytes: u64,

    // memory_high_increase_every_millis gives the period, in milliseconds, at which we should
    // repeatedly increase the value of the cgroup's memory.high while we're waiting on upscaling
    // and memory.high is still being hit.
    //
    // Technically speaking, this actually serves as a rate limit to moderate responding to
    // memory.high events, but these are roughly equivalent if the process is still allocating
    // memory.
    memory_high_increase_every_millis: u64,
}

impl Default for CgroupConfig {
    fn default() -> Self {
        Self {
            oom_buffer_bytes: 100 * MiB,
            memory_high_buffer_bytes: 100 * MiB,
            // while waiting for upscale, don't freeze for more than 20ms every 1s
            max_upscale_wait_millis: 20, // 20ms
            do_not_freeze_more_often_than_millis: 1000,
            // while waiting for upscale, increase memory.high by 10MiB every 25ms
            memory_high_increase_by_bytes: 10 * MiB,
            memory_high_increase_every_millis: 25, // 25 ms
        }
    }
}

impl CgroupConfig {
    // Calculate the new value for the cgroups memory.high based on system memory
    pub fn calculate_memory_high_value(&self, total_system_mem: u64) -> u64 {
        total_system_mem.saturating_sub(self.oom_buffer_bytes)
    }
}

impl CgroupState {
    pub fn new(
        manager: Manager,
        config: CgroupConfig,
        notify_upscale_events: Receiver<(Resources, oneshot::Sender<()>)>,
        request_upscale_events: Sender<oneshot::Sender<()>>,
    ) -> Self {
        Self {
            manager,
            config,
            notify_upscale_events,
            request_upscale_events,
        }
    }

    pub fn get_current_memory(&self) -> Result<u64> {
        self.manager.current_memory_usage()
    }

    #[tracing::instrument(skip(self))]
    pub async fn set_memory_limits(&mut self, available_memory: u64) -> Result<()> {
        info!(name = self.manager.name, "setting memory limits for cgroup");
        let new_high = self.config.calculate_memory_high_value(available_memory);
        info!(
            self.manager.name,
            memory = mib(new_high),
            "Setting cgroup memory.",
        );

        // We don't want to block here, just clear any outstanding event if there is one
        if let Err(TryRecvError::Closed) = self.manager.highs.try_recv() {
            bail!("failed to clear memory.high events due to channel being closed")
        }

        let limits = MemoryLimits::new(new_high, available_memory);

        self.manager
            .set_limits(&limits)
            .tee("failed to set cgroup memory limits")?;

        info!(self.manager.name, "successfully set cgroup memory limits");
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn handle_cgroup_signals_loop(self: &Arc<Self>) {
        // FIXME: we should have "proper" error handling instead of just panicking. It's hard to
        // determine what the correct behavior should be if a cgroup operation fails, though.
        let state = Arc::clone(self);
        info!(state.manager.name, "starting main signals loop");
        tokio::spawn(async move {
            let mut waiting_on_upscale = false;
            let mut wait_to_increase_memory_high = Timer::new(0);
            let mut wait_to_freeze = Timer::new(0);
            loop {
                // Wait for a new signal
                tokio::select! {
                    // Just panic on errors for now, see FIXME
                    err = state.manager.errors.recv() => {
                        match err {
                            Ok(err) => panic!("error listening for cgroup signals {err}"),
                            Err(e) => panic!("error channel was unexpectedly closed: {e}")
                        }
                    }

                    bundle = state.notify_upscale_events.recv() => {
                        info!("received upscale event");
                        match bundle {
                            Ok((resources, tx)) => {
                                info!(?resources, "cgroup manager confirming upscale");
                                // Report back that we're done handling the event.
                                // This is *important* as the dispatcher is waiting on hearing back!
                                if let Err(_) = tx.send(()) {
                                    panic!("error confirming receipt of upscale");
                                }
                            }
                            Err(e) => panic!("error listening for upscales, {e}")
                        }
                        let _ = state.manager.highs.recv().await;
                    }

                    _ = state.manager.highs.recv() => {
                        tokio::select! {
                            biased;
                            _ = &mut wait_to_freeze => {
                                match state.handle_memory_high_event().await {
                                     Ok(b) => {
                                        waiting_on_upscale = b;
                                        wait_to_freeze = Timer::new(state.config.do_not_freeze_more_often_than_millis);
                                     },
                                     Err(e) => panic!("error handling memory.high event {e}")
                                }
                            }
                            _ = future::ready(()) => {
                                if !waiting_on_upscale {
                                    info!("received memory.high event, but too soon to re-freeze. Requesting upscale.");

                                    tokio::select! {
                                        biased;
                                        bundle = state.notify_upscale_events.recv() => {
                                            info!("no need to request upscaling because we were already upscaled");
                                            match bundle {
                                                Ok((_, tx)) => {
                                                    // Report back that we're done handling the event.
                                                    // This is *important* as the dispatcher is waiting on hearing back!
                                                    if let Err(_) = tx.send(()) {
                                                        panic!("error confirming receipt of upscale");
                                                    }
                                                }
                                                Err(e) => panic!("error listening for upscales, {e}")
                                            }
                                        }
                                        _ = future::ready(()) => {
                                            // TODO: could just unwrap
                                            info!("requesting upscale.");
                                            match state.request_upscale().await {
                                                Ok(_) => {},
                                                Err(e) => panic!("error requesting upscale {e}")
                                            }
                                        }
                                    }
                                } else {
                                    tokio::select! {
                                        biased;
                                        _ = &mut wait_to_increase_memory_high => {
                                            info!("received memory.high event, too soon to re-freeze, but increasing memory.high");
                                            tokio::select! {
                                                biased;
                                                bundle = state.notify_upscale_events.recv() => {
                                                    info!("no need to request upscaling because we were already upscaled");
                                                    match bundle {
                                                        Ok((_, tx)) => {
                                                            // Report back that we're done handling the event.
                                                            // This is *important* as the dispatcher is waiting on hearing back!
                                                            if let Err(_) = tx.send(()) {
                                                                panic!("error confirming receipt of upscale");
                                                            }
                                                        }
                                                        Err(e) => panic!("error listening for upscales, {e}")
                                                    }
                                                        return;
                                                    }
                                                _ = future::ready(()) => {
                                                    info!("requesting upscale.");
                                                    // TODO: could just unwrap
                                                    match state.request_upscale().await {
                                                        Ok(_) => {},
                                                        Err(e) => panic!("error requesting upscale {e}")
                                                    }
                                                }
                                            };

                                            let mem_high = match state.manager.get_high_bytes() {
                                                Ok(high) => high,
                                                Err(e) => panic!("error fetching memory.high {}", e)
                                            };

                                            let new_high = mem_high + state.config.memory_high_increase_by_bytes;
                                            info!(
                                                 old = mib(mem_high),
                                                 new = mib(new_high),
                                                "updating memory.high (MiB)",
                                            );

                                            if let Err(e) = state.manager.set_high_bytes(new_high) {
                                                panic!("error setting memory limits: {e}")
                                            }

                                            wait_to_increase_memory_high = Timer::new(state.config.memory_high_increase_every_millis)
                                        }
                                        _ = future::ready(()) => {
                                            // Can't do anything
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    #[tracing::instrument(skip(self))]
    pub async fn handle_memory_high_event(&self) -> Result<bool> {
        tokio::select! {
            biased;

            bundle = self.notify_upscale_events.recv() => {
                info!("skipping memory.high event because there was an upscale event");
                match bundle {
                    Ok((_, tx)) => {
                        // Report back that we're done handling the event.
                        // This is *important* as the dispatcher is waiting on hearing back!
                        if let Err(_) = tx.send(()) {
                            panic!("error confirming receipt of upscale");
                        }
                    }
                    Err(e) => panic!("error listening for upscales, {e}")
                }
                return Ok(false);
            },

            _ = future::ready(()) => {}
        };

        info!("received memory.high event. Freezing cgroup.");

        self.manager
            .freeze()
            .with_tee(|| format!("failed to freeze cgroup {}", self.manager.name))?;

        let start = Instant::now();

        let must_thaw = Timer::new(self.config.max_upscale_wait_millis);

        info!(
            wait = self.config.max_upscale_wait_millis,
            "sending request for immediate upscaling. Waiting.",
        );

        self.request_upscale()
            .await
            .tee("failed to request upscale")?;

        let mut upscaled = false;
        let total_wait;

        tokio::select! {
            bundle = self.notify_upscale_events.recv() => {
                total_wait = start.elapsed();
                info!(
                    wait = total_wait.as_millis(),
                    "received notification that upscale occured after {total_wait:?} ms. Thawing cgroup.",
                );
                match bundle {
                    Ok((_, tx)) => {
                        // Report back that we're done handling the event.
                        // This is *important* as the dispatcher is waiting on hearing back!
                        if let Err(_) = tx.send(()) {
                            panic!("error confirming receipt of upscale");
                        }
                    }
                    Err(e) => panic!("error listening for upscales, {e}")
                }
                upscaled = false;
            }
            _ = must_thaw => {
                total_wait = start.elapsed();
                info!(
                    wait = total_wait.as_millis(),
                    "timeout after {total_wait:?} ms waiting for upscale. Thawing cgroup.",
                )
            }
        };

        self.manager
            .thaw()
            .with_tee(|| format!("failed to thaw cgroup {}", self.manager.name))?;

        // We don't want to block here, just clear any outstanding event if there is one
        if let Err(TryRecvError::Closed) = self.manager.highs.try_recv() {
            bail!("failed to clear memory.high events due to channel being closed")
        }

        return Ok(!upscaled);
    }

    #[tracing::instrument(skip(self))]
    pub async fn request_upscale(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.request_upscale_events
            .send(tx)
            .await
            .tee("failed to send upscale request across channel")?;
        rx.await
            .tee("failed to read confirmation of receipt of upscale request")?;
        Ok(())
    }
}
