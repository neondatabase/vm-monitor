// Need to think about who is releasing/initing the cgroup
// Instead of channels, could use condvars and callback?
// TODO: is it ok to just unwrap channel errors? How could we handle them?

use std::{
    fmt::Display,
    fs, future, mem,
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex,
    },
};

use anyhow::{anyhow, bail, Error, Result};
use async_std::channel::{self, Receiver, TryRecvError};
use cgroups_rs::{
    cgroup::{Cgroup, UNIFIED_MOUNTPOINT},
    freezer::{FreezerController, FreezerState},
    hierarchies::{self, is_cgroup2_unified_mode},
    memory::MemController,
    MaxValue,
    Subsystem::{Freezer, Mem},
};
use futures_util::StreamExt;
use inotify::{Inotify, WatchMask};
use notify::Event;
use tracing::{info, warn};

use crate::{timer::Timer, LogContext};

#[derive(Debug)]
pub struct Manager {
    /// Receives updates on memory.high events
    ///
    /// Note: this channel's methods should be cancellation safe, refer to the
    /// async-std source code.
    pub(crate) highs: Receiver<u64>,

    /// Receives errors retrieving cgroup statistics
    ///
    /// Note: this channel's methods should be cancellation safe, refer to the
    /// async-std source code.
    pub(crate) errors: Receiver<Error>,

    pub(crate) name: String,
    pub(crate) cgroup: Cgroup,

    /// # Safety
    /// This lock must be held while while performing IO on cgroup "files"
    /// like memory.high, memory.current, etc
    ///
    /// A normal Mutex is appropriate since we never lock the mutex in async
    /// functions (although an async function may call a sync function that acceses
    /// the mutex), so it is guaranteed to never be held across await points.
    ///
    /// Design note: perhaps we could make a new struct combining
    memory_update_lock: Mutex<()>,
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

#[derive(Debug)]
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
    #[tracing::instrument]
    pub async fn new(name: String) -> Result<Self> {
        // TODO: check for cgroup mode
        if !is_cgroup2_unified_mode() {
            bail!("cgroups v2 not supported");
        }

        let cgroup = Cgroup::load(hierarchies::auto(), &name);

        info!("creating file watcher for memory.high events");

        // Set up a watcher that notifies on changes to memory.events
        let path = format!("{}/{}/memory.events", UNIFIED_MOUNTPOINT, &name);
        let inotify = Inotify::init().tee("failed to initialize file watcher")?;
        inotify
            .watches()
            .add(&path, WatchMask::MODIFY)
            .with_tee(|| format!("failed to start watching {path}"))?;

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
                    _ = &mut waiter => (),
                    _ = future::ready(()) => {
                        info!(
                            wait = min_wait,
                            "respecting minimum wait of {min_wait:?} ms before restarting memory.events listener",
                        );
                        (&mut waiter).await;
                        info!("restarting memory.events listener")
                    }
                };

                waiter = Timer::new(min_wait);

                // Read memory.events and send an update down the channel if the number of high events
                // has increased
                if let Some(val) = events.next().await {
                    info!("got memory.high event");
                    match val {
                        Ok(_) => {
                            if let Ok(high) = Self::get_event_count(&name_clone, MemoryEvent::High)
                            {
                                if high_count.fetch_max(high, Ordering::SeqCst) < high {
                                    event_tx.send(high).await.unwrap()
                                }
                            } else {
                                warn!("failed to read high events count from memory.events")
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
        // TODO: change this to general memory information in the future?
        let high = Self::get_event_count(&name, MemoryEvent::High)
            .tee("failed to extract number of memory.high events from memory.events")?;
        info!(
            events = high,
            "the current number of memory.high events: {high}"
        );

        // Ignore the first set of events. We don't actually want to be notified
        // on startup since some processes might already be running.
        // We don't want to block here as that would interrupt the rest of
        // startup, so we use try_recv
        if let Err(TryRecvError::Closed) = event_rx.try_recv() {
            bail!(
                "failed to clear initial memory.high event count due to event channel being closed"
            )
        };

        Ok(Self {
            highs: event_rx,
            errors: error_rx,
            name,
            cgroup,
            memory_update_lock: Mutex::new(()),
        })
    }

    /// Read memory.events for the desired event type.
    fn get_event_count(name: &str, event: MemoryEvent) -> Result<u64> {
        let path = format!("{}/{}/memory.events", UNIFIED_MOUNTPOINT, &name);
        let contents = fs::read_to_string(&path).expect("failed to read memory events info");
        Ok(contents
            .lines()
            .filter_map(|s| s.split_once(' '))
            .find(|(e, _)| *e == event.to_string())
            .map(|(_, count)| count.parse::<u64>())
            .ok_or(anyhow!("error getting memory.high event count"))
            .with_tee(|| format!("failed to find entry for memory.high events in {path}"))?
            .tee("failed to parse memory.high as u64")?)
    }

    pub fn state(&self) -> Result<FreezerState> {
        Ok(self
            .freezer()
            .tee("failed to get freezer subsystem while attempting to get freezer state")?
            .state()
            .tee("failed to get freezer state")?)
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
        Ok(self
            .freezer()
            .tee("failed to get freezer subsystem")?
            .freeze()
            .tee("failed to freeze")?)
    }

    pub fn thaw(&self) -> Result<()> {
        Ok(self
            .freezer()
            .tee("failed to get freezer subsystem")?
            .thaw()
            .tee("failed to thaw")?)
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
        Ok(self
            .memory()
            .tee("failed to get memory subsystem")?
            .memory_stat()
            .usage_in_bytes)
    }

    pub fn set_high_bytes(&self, bytes: u64) -> Result<()> {
        let _ = self.memory_update_lock.lock();
        Ok(self
            .memory()
            .tee("failed to get memory subsystem")?
            .set_mem(cgroups_rs::memory::SetMemory {
                low: None,
                high: Some(MaxValue::Value(bytes.max(i64::MAX as u64) as i64)),
                min: None,
                max: None,
            })
            .tee("failed to set memory.high")?)
    }

    pub fn set_limits(&self, limits: &MemoryLimits) -> Result<()> {
        let _ = self.memory_update_lock.lock();
        info!(limits.high, limits.max, "writing new memory limits",);
        Ok(self
            .memory()
            .tee("failed to get memory subsystem while setting memory limits")?
            .set_mem(cgroups_rs::memory::SetMemory {
                min: None,
                low: None,
                high: Some(MaxValue::Value(
                    u64::max(limits.high, i64::MAX as u64) as i64
                )),
                max: Some(MaxValue::Value(u64::max(limits.max, i64::MAX as u64) as i64)),
            })
            .tee("failed to set memory limits")?)
    }

    pub fn get_high_bytes(&self) -> Result<u64> {
        let _ = self.memory_update_lock.lock();
        let high = self
            .memory()
            .tee("failed to get memory subsystem while getting memory statistics")?
            .get_mem()
            .map(|mem| mem.high)
            .tee("failed to get memory statistics from subsystem")?;
        match high {
            Some(MaxValue::Max) => Ok(i64::MAX as u64),
            Some(MaxValue::Value(high)) => Ok(high as u64),
            None => bail!("failed to read memory.high from memory subsystem"),
        }
    }
}
