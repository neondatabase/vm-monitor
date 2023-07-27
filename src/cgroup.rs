// REVIEW: "this module" is redundant
//! This module manages the actual cgroup operations we need to perform to
//! upscale and downscale. It also handles sending upscale requests to the
//! dispatcher.

use std::{future, sync::Arc, time::Duration};

use anyhow::Context;
use async_std::channel::{Receiver, Sender};
use tokio::{sync::oneshot, time::Instant};
use tracing::info;

use crate::{
    manager::{Manager, MemoryLimits},
    mib,
    protocol::Allocation,
    MiB,
};

#[derive(Debug)]
pub struct CgroupState {
    pub(crate) manager: Manager,
    pub(crate) config: CgroupConfig,

    // For communication with dispatcher
    notify_upscale_events: Receiver<(Allocation, oneshot::Sender<()>)>,
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

    // max_upscale_wait gives the maximum duration, in milliseconds, that we're allowed to pause
    // the cgroup for while waiting for the autoscaler-agent to upscale us
    max_upscale_wait: Duration,

    // do_not_freeze_more_often_than gives a required minimum time, in milliseconds, that we must
    // wait before re-freezing the cgroup while waiting for the autoscaler-agent to upscale us.
    do_not_freeze_more_often_than: Duration,

    // memory_high_increase_by_bytes gives the amount of memory, in bytes, that we should periodically
    // increase memory.high by while waiting for the autoscaler-agent to upscale us.
    //
    // This exists to avoid the excessive throttling that happens when a cgroup is above its
    // memory.high for too long. See more here:
    // https://github.com/neondatabase/autoscaling/issues/44#issuecomment-1522487217
    memory_high_increase_by_bytes: u64,

    // memory_high_increase_every gives the period, in milliseconds, at which we should
    // repeatedly increase the value of the cgroup's memory.high while we're waiting on upscaling
    // and memory.high is still being hit.
    //
    // Technically speaking, this actually serves as a rate limit to moderate responding to
    // memory.high events, but these are roughly equivalent if the process is still allocating
    // memory.
    memory_high_increase_every: Duration,
}

impl Default for CgroupConfig {
    fn default() -> Self {
        Self {
            oom_buffer_bytes: 100 * MiB,
            memory_high_buffer_bytes: 100 * MiB,
            // while waiting for upscale, don't freeze for more than 20ms every 1s
            max_upscale_wait: Duration::from_millis(20),
            do_not_freeze_more_often_than: Duration::from_millis(1000),
            // while waiting for upscale, increase memory.high by 10MiB every 25ms
            memory_high_increase_by_bytes: 10 * MiB,
            memory_high_increase_every: Duration::from_millis(25),
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
        notify_upscale_events: Receiver<(Allocation, oneshot::Sender<()>)>,
        request_upscale_events: Sender<oneshot::Sender<()>>,
    ) -> Self {
        Self {
            manager,
            config,
            notify_upscale_events,
            request_upscale_events,
        }
    }

    pub fn get_current_memory(&self) -> anyhow::Result<u64> {
        self.manager.current_memory_usage()
    }

    #[tracing::instrument(skip(self))]
    pub async fn set_memory_limits(&mut self, available_memory: u64) -> anyhow::Result<()> {
        let new_high = self.config.calculate_memory_high_value(available_memory);
        self.manager.flush_high_event()?;
        let limits = MemoryLimits::new(new_high, available_memory);
        info!(
            name = self.manager.name,
            memory = ?limits,
            "setting cgroup memory",
        );
        self.manager
            .set_limits(&limits)
            .context("failed to set cgroup memory limits")?;
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub fn handle_cgroup_signals_loop(self: &Arc<Self>) {
        // FIXME: we should have "proper" error handling instead of just panicking. It's hard to
        // determine what the correct behavior should be if a cgroup operation fails, though.
        let state = Arc::clone(self);
        info!(name = state.manager.name, "starting main signals loop");
        tokio::spawn(async move {
            // REVIEW: explain what these are for?
            let mut waiting_on_upscale = false;
            let wait_to_increase_memory_high = tokio::time::sleep(Duration::ZERO);
            let wait_to_freeze = tokio::time::sleep(Duration::ZERO);
            tokio::pin!(wait_to_increase_memory_high);
            tokio::pin!(wait_to_freeze);
            // REVIEW: yikes. this is... not good. This does not translate well from
            // Go to Rust. It's nigh-impossible to tell what's actually happening
            // here, with all the varying types of indentation and control flow, and
            // the minimal comments.
            //
            // I would recommend finding a way to restructure this before shipping.
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
                                if tx.send(()).is_err() {
                                    panic!("error confirming receipt of upscale");
                                }
                            }
                            Err(e) => panic!("error listening for upscales, {e}")
                        }

                        state.manager.flush_high_event().unwrap();
                    }

                    _ = state.manager.highs.recv() => {
                        tokio::select! {
                            biased;
                            // Is this is suitable use case for now_or_never?
                            _ = &mut wait_to_freeze => {
                                match state.handle_memory_high_event().await {
                                     Ok(b) => {
                                        waiting_on_upscale = b;
                                        wait_to_freeze
                                            .as_mut()
                                            .reset(Instant::now() + state.config.do_not_freeze_more_often_than);
                                     },
                                     Err(e) => panic!("error handling memory.high event {e}")
                                }
                            }
                            _ = future::ready(()) => {
                                if !waiting_on_upscale {
                                    info!(
                                        "received memory.high event, but too soon to re-freeze; requesting upscale",
                                    );

                                    tokio::select! {
                                        biased;
                                        bundle = state.notify_upscale_events.recv() => {
                                            info!("no need to request upscaling because we were already upscaled");
                                            match bundle {
                                                Ok((_, tx)) => {
                                                    // Report back that we're done handling the event.
                                                    // This is *important* as the dispatcher is waiting on hearing back!
                                                    if tx.send(()).is_err() {
                                                        panic!("error confirming receipt of upscale");
                                                    }
                                                }
                                                Err(e) => panic!("error listening for upscales, {e}")
                                            }
                                        }
                                        _ = future::ready(()) => {
                                            // TODO: could just unwrap
                                            info!("requesting upscale");
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
                                            info!(
                                                "received memory.high event, but too soon to refreeze and waiting on upscale; increasing memory.high"
                                            );
                                            tokio::select! {
                                                biased;
                                                bundle = state.notify_upscale_events.recv() => {
                                                    info!("no need to request upscaling because we were already upscaled");
                                                    match bundle {
                                                        Ok((_, tx)) => {
                                                            // Report back that we're done handling the event.
                                                            // This is *important* as the dispatcher is waiting on hearing back!
                                                            if tx.send(()).is_err() {
                                                                panic!("error confirming receipt of upscale");
                                                            }
                                                        }
                                                        Err(e) => panic!("error listening for upscales, {e}")
                                                    }
                                                        return;
                                                    }
                                                _ = future::ready(()) => {
                                                    info!("requesting upscale");
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

                                            wait_to_increase_memory_high
                                                .as_mut()
                                                .reset(Instant::now() + state.config.memory_high_increase_every)

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

    // REVIEW: this function can't just return a bool without any comment or anything
    // explaining what the bool means. Either (a) make a bespoke enum, or (b) add
    // some comments.
    #[tracing::instrument(skip(self))]
    pub async fn handle_memory_high_event(&self) -> anyhow::Result<bool> {
        // REVIEW: candidate for now_or_never. You can also just `if let Poll::Ready`.
        tokio::select! {
            biased;
            bundle = self.notify_upscale_events.recv() => {
                info!("skipping memory.high event because there was an upscale event");
                match bundle {
                    Ok((_, tx)) => {
                        // Report back that we're done handling the event.
                        // This is *important* as the dispatcher is waiting on hearing back!
                        if tx.send(()).is_err() {
                            panic!("error confirming receipt of upscale");
                        }
                    }
                    Err(e) => panic!("error listening for upscales, {e}")
                }
                return Ok(false);
            },
            _ = future::ready(()) => {}
        };

        info!("received memory.high event; freezing cgroup");
        self.manager
            .freeze()
            .with_context(|| format!("failed to freeze cgroup {}", self.manager.name))?;

        // REVIEW: comments explaining this?
        let start = Instant::now();
        let must_thaw = tokio::time::sleep(self.config.max_upscale_wait);

        info!(
            wait = ?self.config.max_upscale_wait,
            "sending request for immediate upscaling; waiting",
        );
        self.request_upscale()
            .await
            .context("failed to request upscale")?;

        let mut upscaled = false;
        let total_wait;
        // REVIEW: comment explaining what we're doing?
        tokio::select! {
            bundle = self.notify_upscale_events.recv() => {
                total_wait = start.elapsed();
                // REVIEW: duplicated information in the log
                info!(
                    wait = total_wait.as_millis(), // REVIEW: log key should have units
                    "received notification that upscale occured after {total_wait:?} ms; thawing cgroup",
                );
                match bundle {
                    Ok((_, tx)) => {
                        // Report back that we're done handling the event.
                        // This is *important* as the dispatcher is waiting on hearing back!
                        if tx.send(()).is_err() {
                            panic!("error confirming receipt of upscale");
                        }
                    }
                    Err(e) => panic!("error listening for upscales, {e}")
                }
                upscaled = false;
            }
            _ = must_thaw => {
                total_wait = start.elapsed();
                // REVIEW: duplicated information in the log
                info!(
                    wait = total_wait.as_millis(), // REVIEW: log key should have units
                    "timeout after {total_wait:?} ms waiting for upscale; thawing cgroup",
                )
            }
        };
        self.manager
            .thaw()
            .with_context(|| format!("failed to thaw cgroup {}", self.manager.name))?;
        self.manager.flush_high_event()?;
        Ok(!upscaled)
    }

    #[tracing::instrument(skip(self))]
    pub async fn request_upscale(&self) -> anyhow::Result<()> {
        info!("requesting upscale");
        let (tx, rx) = oneshot::channel();
        self.request_upscale_events
            .send(tx)
            .await
            .context("failed to send upscale request across channel")?;
        rx.await
            .context("failed to read confirmation of receipt of upscale request")?;
        Ok(())
    }
}
