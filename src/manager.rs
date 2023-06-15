// Need to think about who is releasing the cgroup
// Instead of channels, could use condvars and callback?

use std::{
    fmt::Display,
    fs, future, mem,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use anyhow::{anyhow, bail, Error, Result};
use cgroups_rs::{
    cgroup::{Cgroup, UNIFIED_MOUNTPOINT},
    freezer::{FreezerController, FreezerState},
    hierarchies::{self, is_cgroup2_unified_mode},
    memory::MemController,
    MaxValue,
    Subsystem::{Freezer, Mem},
};
use notify::Event;
use tokio::time;

use async_std::channel::{self, Receiver};

use futures_util::StreamExt;

use inotify::{Inotify, WatchMask};

use tracing::{info, warn};

use crate::timer::Timer;
pub struct Manager {
    /// Receives updates on memory high events
    pub(crate) highs: Receiver<u64>,
    pub(crate) errors: Receiver<Error>,
    pub(crate) name: String,
    // TODO: might need to put a rwlock around this?
    pub(crate) cgroup: Cgroup,
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum MemoryEvent {
    Low,
    High,
    Max,
    Oom,
    OomKill,
    OomGroupKill,
}

impl Display for MemoryEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MemoryEvent::Low => f.write_str("low"),
            MemoryEvent::High => f.write_str("high"),
            MemoryEvent::Max => f.write_str("max"),
            MemoryEvent::Oom => f.write_str("oom"),
            MemoryEvent::OomKill => f.write_str("oom_kill"),
            MemoryEvent::OomGroupKill => f.write_str("oom_group_kill"),
        }
    }
}

pub struct MemoryLimits {
    high: u64,
    max: u64,
}

impl MemoryLimits {
    pub fn new(high: u64, max: u64) -> Self {
        return Self { max, high };
    }
}

impl Manager {
    /// Load the cgroup named `name`. This should just be the name of the cgroup,
    /// and not include anything like /sys/fs/cgroups
    pub async fn new(name: String) -> Result<Self> {
        // TODO: check for cgroup mode
        if !is_cgroup2_unified_mode() {
            bail!("cgroups v2 not supported");
        }

        let cgroup = Cgroup::load(hierarchies::auto(), &name);

        // Set up a watcher that notifies on changes to memory.events
        let path = format!("{}/{}/memory.events", UNIFIED_MOUNTPOINT, &name);
        let inotify = Inotify::init()?;
        inotify.watches().add(&path, WatchMask::MODIFY)?;

        // These are effectively just signals
        let (event_tx, event_rx) = channel::bounded(1);
        let (error_tx, error_rx) = channel::bounded(1);

        let min_wait = 1000; // 1000ms = 1s
        let mut waiter = Timer::new(min_wait);
        let high_count = AtomicU64::new(0);
        let name_clone = name.clone();

        // Long running background task for getting memory.events updates
        tokio::spawn(async move {
            // TODO: how big do we want buffer to be?
            let mut events = inotify
                .into_event_stream([0u8; 10 * mem::size_of::<Result<Event, Error>>()])
                .expect("failed to start memory event stream");

            loop {
                // Make sure restarts of the listener are separated by min_wait
                // The first branch will always be taken immediately on the first
                // iteration because of how waiter is initialized.
                tokio::select! {
                    biased;
                    _ = waiter => (),
                    _ = future::ready(()) => {
                        info!("Respecting minimum wait of {min_wait:?} before restarting memory.events listener");
                        tokio::spawn(time::sleep(Duration::from_secs(0))).await.unwrap();
                        info!("Restarting memory.events listener")
                    }
                };

                waiter = Timer::new(min_wait);

                // Read memory.events and send an update down the channel if the number of high events
                // has increased
                if let Some(val) = events.next().await {
                    match val {
                        Ok(_) => {
                            if let Ok(high) = Self::get_event_count(&name_clone, MemoryEvent::High)
                            {
                                if high_count.fetch_max(high, Ordering::SeqCst) < high {
                                    event_tx.send(high).await.unwrap()
                                }
                            } else {
                                warn!("Failed to read high events count from memory.events")
                            }
                        }
                        Err(error) => {
                            error_tx.send(Error::from(error)).await.unwrap();
                            return;
                        }
                    }
                }
            }
        });

        // Log out an initial memory.events summary
        match Self::get_event_count(&name, MemoryEvent::High) {
            // TODO: change this to general memory information in the future?
            Ok(high) => info!("The current number of memory high events: {high}"),
            Err(e) => {
                return Err(
                    e.context("Failed to extract number of memory high events from memory.events")
                )
            }
        }

        // Ignore the first set of events. We don't actually want to be notified
        // on startup since some processes might already be running.
        let _ = event_rx.recv().await;

        Ok(Self {
            highs: event_rx,
            errors: error_rx,
            name,
            cgroup,
        })
    }

    /// Read memory.events for the desired event type.
    fn get_event_count(name: &str, event: MemoryEvent) -> Result<u64> {
        let path = format!("{}/{}/memory.events", UNIFIED_MOUNTPOINT, &name);
        let contents = fs::read_to_string(&path).expect("failed to read memory events info");
        contents
            .lines()
            .filter_map(|s| s.split_once(' '))
            .find(|(e, _)| *e == event.to_string())
            .map(|(_, count)| count.parse::<u64>())
            .ok_or(anyhow!(
                "failed to find entry for memory high events in {path}"
            ))?
            .map_err(|e| e.into())
    }

    pub fn state(&self) -> Result<FreezerState> {
        self.freezer()?.state().map_err(|e| e.into())
    }

    fn freezer(&self) -> Result<&FreezerController> {
        if let Some(Freezer(freezer)) = self
            .cgroup
            .subsystems()
            .iter()
            .find(|sub| matches!(sub, Freezer(_)))
        {
            Ok(freezer)
        } else {
            bail!("could not find freezer subsystem")
        }
    }

    pub fn freeze(&self) -> Result<()> {
        Ok(self.freezer()?.freeze()?)
    }

    pub fn thaw(&self) -> Result<()> {
        Ok(self.freezer()?.thaw()?)
    }

    fn memory(&self) -> Result<&MemController> {
        if let Some(Mem(memory)) = self
            .cgroup
            .subsystems()
            .iter()
            .find(|sub| matches!(sub, Mem(_)))
        {
            Ok(memory)
        } else {
            bail!("could not find memory subsystem")
        }
    }

    pub fn current_memory_usage(&self) -> Result<u64> {
        Ok(self.memory()?.memory_stat().usage_in_bytes)
    }

    pub fn set_high_bytes(&self, bytes: u64) -> Result<()> {
        Ok(self.memory()?.set_mem(cgroups_rs::memory::SetMemory {
            low: None,
            high: Some(MaxValue::Value(bytes.max(i64::MAX as u64) as i64)),
            min: None,
            max: None,
        })?)
    }

    pub fn set_limits(&self, limits: MemoryLimits) -> Result<()> {
        Ok(self.memory()?.set_mem(cgroups_rs::memory::SetMemory {
            low: Some(MaxValue::Value(limits.max.max(i64::MAX as u64) as i64)),
            high: Some(MaxValue::Value(limits.high.max(i64::MAX as u64) as i64)),
            min: None,
            max: None,
        })?)
    }

    pub fn get_high_bytes(&self) -> Result<u64> {
        let high = self.memory()?.get_mem().map(|mem| mem.high)?;
        match high {
            Some(MaxValue::Max) => Ok(i64::MAX as u64),
            Some(MaxValue::Value(high)) => Ok(high as u64),
            None => bail!("failed to read memory.high from memory subsystem"),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}
