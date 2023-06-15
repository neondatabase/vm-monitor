// TODO: standardize using 'memory high event' or 'memory.high event'
// TODO: logging; make sure to assess how control flow like ? affects it.

use std::{sync::Arc, time::Instant, future};

use crate::{
    manager::{Manager, MemoryLimits},
    mib,
    timer::Timer,
    MiB,
};
use anyhow::Result;
use async_std::channel::{self, Receiver, Sender};
use tracing::info;

pub struct CgroupState {
    manager: Manager,
    config: CgroupConfig,
    upscale_events_sender: Sender<()>,
    upscale_events_receiver: Receiver<()>,
}

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
    memory_high_buffer_bytes: u64,

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
    pub fn new(manager: Manager, config: CgroupConfig) -> Self {
        let (upscale_events_sender, upscale_events_receiver) = channel::bounded(1);
        Self {
            manager,
            config,
            upscale_events_sender,
            upscale_events_receiver,
        }
    }

    pub fn get_current_memory(&self) -> Result<u64> {
        self.manager.current_memory_usage()
    }

    pub async fn received_upscale(&self) {
        // The receiver will not be dropped/closed it is also contained in self,
        // so the unwrap is safe
        self.upscale_events_sender.send(()).await.unwrap();
    }

    pub async fn set_memory_limits(&mut self, available_memory: u64) -> Result<()> {
        let new_high = self.config.calculate_memory_high_value(available_memory);
        info!(
            "Total memory available for cgroup {} is {} MiB. Setting cgroup memory.",
            self.manager.name,
            mib(new_high)
        );

        self.manager.highs.recv().await?;

        let limits = MemoryLimits::new(new_high, available_memory);

        self.manager.set_limits(limits)?;

        info!(
            "Successfully set cgroup {} memory limits",
            self.manager.name
        );
        Ok(())
    }

    pub async fn handle_cgroup_signals_loop(self: &Arc<Self>) {
        // FIXME: we should have "proper" error handling instead of just panicking. It's hard to
        // determine what the correct behavior should be if a cgroup operation fails, though.
        let state = Arc::clone(self);
        // let errors = state.manager.errors.clone();
        // let highs = state.manager.highs.clone();
        // let upscale_events_receiver = state.upscale_events_receiver.clone();
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
                            Ok(err) => panic!("Error listening for cgroup signals {err}"),
                            Err(e) => panic!("Error channel was unexpectedly closed: {e}")
                        }
                    }

                    _ = state.upscale_events_receiver.recv() => {
                        info!("Received upscale event");
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
                                     Err(e) => panic!("Error handling memory high event {e}")
                                }
                            }
                            _ = future::ready(()) => {
                                if !waiting_on_upscale {
                                    info!("Received memory.high event, but too soon to re-freeze. Requesting upscale.");

                                    tokio::select! {
                                        biased;
                                        _ = state.upscale_events_receiver.recv() => {
                                            info!("No need to request upscaling because we were already upscaled");
                                            return;
                                        }
                                        _ = future::ready(()) => {
                                            // TODO: could just unwrap
                                            match state.request_upscale().await {
                                                Ok(_) => {},
                                                Err(e) => panic!("Error requesting upscale {e}")
                                            }
                                        }
                                    }
                                } else {
                                    tokio::select! {
                                        biased;
                                        _ = &mut wait_to_increase_memory_high => {
                                            tokio::select! {
                                                biased;
                                                _ = state.upscale_events_receiver.recv() => {
                                                    info!("No need to request upscaling because we were already upscaled");
                                                    return;
                                                }
                                                _ = future::ready(()) => {
                                                    // TODO: could just unwrap
                                                    match state.request_upscale().await {
                                                        Ok(_) => {},
                                                        Err(e) => panic!("Error requesting upscale {e}")
                                                    }
                                                }
                                            };

                                            let mem_high = match state.manager.get_high_bytes() {
                                                Ok(high) => high,
                                                Err(e) => panic!("Error fetching memory.high {}", e)
                                            };

                                            let new_high = mem_high + state.config.memory_high_increase_by_bytes;
                                            info!("Updating memory.high from {} -> {} Mib",
                                                  mib(mem_high),
                                                  mib(new_high)
                                            );

                                            if let Err(e) = state.manager.set_high_bytes(new_high) {
                                                panic!("Error setting memory limits: {e}")
                                            }

                                            wait_to_increase_memory_high = Timer::new(state.config.memory_high_increase_every_millis)
                                        }
                                        _ = future::ready(()) => {
                                            // Can't do anything
                                        }
                                    }
                                    info!("Received memory.high event, too soon to re-freeze, but increasing memory.high");

                                }
                            }
                        }
                    }

                }
            }
        });
    }

    pub async fn handle_memory_high_event(&self) -> Result<bool> {
        tokio::select! {
            biased;

            _ = self.upscale_events_receiver.recv() => {
                info!("Skipping memory.high event because there was an upscale vent");
                return Ok(false);
            },

            _ = future::ready(()) => {}
        };

        info!("Received memory high event. Freezing cgroup.");

        self.manager.freeze()?;

        let start = Instant::now();

        let must_thaw = Timer::new(self.config.max_upscale_wait_millis);

        info!(
            "Sending request for immediate upscaling, waiting for at most {}",
            self.config.max_upscale_wait_millis
        );

        self.request_upscale().await?;

        let mut upscaled = false;
        let total_wait;

        tokio::select! {
            _ = self.upscale_events_receiver.recv() => {
                total_wait = start.elapsed();
                info!("Received notification that upscale occured after {total_wait:?}. Thawing cgroup.");
                upscaled = false;
            }
            _ = must_thaw => {
                total_wait = start.elapsed();
                info!("Time out after {total_wait:?} waiting for upscale. Thawing cgroup.")
            }
        };

        self.manager.thaw()?;

        self.manager.highs.recv().await?;

        return Ok(!upscaled);
    }

    pub async fn request_upscale(&self) -> Result<()> {
        todo!()
    }
}
