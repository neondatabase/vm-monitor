use crate::manager::{Manager, MemoryLimits};
use anyhow::Result;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tracing::info;

#[allow(non_upper_case_globals)]
const MiB: u64 = 1 << 20;

pub struct CgroupState {
    // TODO: quite possibel we should just own manager
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
            oom_buffer_bytes: 100 * MiB,         // 100 MiB
            memory_high_buffer_bytes: 100 * MiB, // 100 MiB
            // while waiting for upscale, don't freeze for more than 20ms every 1s
            max_upscale_wait_millis: 20,                // 20ms
            do_not_freeze_more_often_than_millis: 1000, // 1s
            // while waiting for upscale, increase memory.high by 10MiB every 25ms
            memory_high_increase_by_bytes: 10 * MiB, // 10 MiB
            memory_high_increase_every_millis: 25,   // 25 ms
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
            (new_high as f32) / (MiB as f32)
        );

        self.manager.highs.recv().await;

        let limits = MemoryLimits::new(new_high, available_memory);

        self.manager.set_limits(limits).map(|_| {
            info!(
                "Successfully set cgroup {} memory limits",
                self.manager.name
            )
        })
    }

    pub async fn handle_cgroup_signals_loop(&self, config: CgroupConfig) {
        let mut waiting_on_upscale = false;
        todo!()
    }
}
