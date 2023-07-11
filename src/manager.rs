//! # `manager`
//!
//! `manager` exposes a `Manager` type that represents a managed cgroup with all
//! the functionality we need.

// Need to think about who is releasing/initing the cgroup
// Instead of channels, could use condvars and callback?
// TODO: is it ok to just unwrap channel errors? How could we handle them?
use std::{
    fmt::Display,
    fs, future, mem,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    thread,
    time::Duration,
};

use anyhow::anyhow;
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
use tokio::time::Instant;
use tracing::{info, trace, warn};

use crate::LogContext;

/// `Manager` basically represents a cgroup. Its methods cover the behaviour we
/// want from said cgroup, such as increasing and decreasing memory.{max,high},
/// monitoring the cgroup usage, and freezing and thawing the cgroup.
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
    pub(crate) errors: Receiver<anyhow::Error>,

    pub(crate) name: String,

    /// The underlying cgroup that we are managing.
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
    ///
    /// TODO: this should only be an Arc as long as we have the deadlock checker
    /// (hopefully only  in debug). Otherwise we don't need the sharing.
    memory_update_lock: Arc<Mutex<()>>,
}

/// A memory event type reported in memory.events.
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

/// Represents a set of limits we apply to a cgroup to control memory usage. Setting
/// these values also affects the thresholds for receiving usage alerts.
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
    /// and not include anything like /sys/fs/cgroups. Starts a listener that
    /// sends events to self.highs and self.errors.
    #[tracing::instrument]
    pub async fn new(name: String) -> anyhow::Result<Self> {
        // Make sure cgroups v2 are supported
        if !is_cgroup2_unified_mode() {
            anyhow::bail!("cgroups v2 not supported");
        }

        let cgroup = Cgroup::load(hierarchies::auto(), &name);

        info!(action = "creating file watcher for memory.high events");

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

        // The duration to separate restarts of the listener by.
        // 1000ms = 1s
        let min_wait = Duration::from_millis(1000);

        let timer = tokio::time::sleep(min_wait);
        let high_count = AtomicU64::new(0);
        let name_clone = name.clone();

        // Long running background task for getting memory.events updates
        tokio::spawn(async move {
            tokio::pin!(timer);
            // TODO: how big do we want buffer to be?
            let mut events = inotify
                .into_event_stream(
                    [0u8; 10 * mem::size_of::<anyhow::Result<Event, anyhow::Error>>()],
                )
                .expect("failed to start memory event stream");

            loop {
                // Make sure restarts of the listener are separated by min_wait
                // The first branch will always be taken immediately on the first
                // iteration because of how waiter is initialized.
                tokio::select! {
                    biased;
                    // If the time has elapsed, continue
                    _ = &mut timer => (),

                    // Otherwise, be explicit about waiting it out
                    _ = future::ready(()) => {
                        info!(
                            wait = ?min_wait,
                            action = "respecting minimum wait of {min_wait:?} ms before restarting memory.events listener",
                        );
                        timer.as_mut().await;
                        info!(action = "restarting memory.events listener")
                    }
                };

                // Start the timer in the background
                timer.as_mut().reset(Instant::now() + min_wait);

                // Read memory.events and send an update down the channel if the number of high events
                // has increased
                if let Some(val) = events.next().await {
                    info!("got memory.high event");
                    match val {
                        Ok(_) => {
                            if let Ok(high) = Self::get_event_count(&name_clone, MemoryEvent::High)
                            {
                                // Only send an event if the high count is higher.
                                if high_count.fetch_max(high, Ordering::SeqCst) < high {
                                    event_tx.send(high).await.unwrap()
                                }
                            } else {
                                warn!("failed to read high events count from memory.events")
                            }
                        }
                        Err(error) => {
                            error_tx.send(anyhow!(error)).await.unwrap();
                            return;
                        }
                    }
                }
            }
        });

        // Log out an initial memory.high event count
        //
        // Note: in theory, this is a little risky because in we guard
        // every acces to cgroup files with a lock. In reality, writing to the
        // "file" should be very fast, there should be very little contention,
        // and the only other thing that could access the file is the the thread
        // we just spawned. Therefore, the likelihood of a race is very small.
        let high = Self::get_event_count(&name, MemoryEvent::High)
            .tee("failed to extract number of memory.high events from memory.events")?;
        info!(
            events = high,
            "the current number of memory.high events: {high}"
        );

        // Ignore the first set of events. We don't actually want to be notified
        // on startup since some processes might already be running.
        // We don't want to block here as that would interrupt the rest of
        // startup, so we use try_recv to flush a possible event.
        if let Err(TryRecvError::Closed) = event_rx.try_recv() {
            anyhow::bail!(
                "failed to clear initial memory.high event count due to event channel being closed"
            )
        };

        let memory_update_lock = Arc::new(Mutex::new(()));
        let clone = Arc::clone(&memory_update_lock);
        // Start deadlock checker
        thread::spawn(move || loop {
            trace!(action = "waiting 1 second to take memory update lock");
            std::thread::sleep(Duration::from_millis(1000));
            let _lock = clone.lock().unwrap();
            trace!("memory update lock taken and released")
        });

        Ok(Self {
            highs: event_rx,
            errors: error_rx,
            name,
            cgroup,
            memory_update_lock,
        })
    }

    /// Clear a memory.high event is there is one in a non-blocking way.
    ///
    /// This is mainly called when we are upscaled - as the upscale presumably
    /// deals with the outstanding event.
    ///
    /// Retrospective: there was an error during inital development of the monitor
    /// where cancelling an upscale event was done with a simple `self.highs.recv().await`,
    /// thus blocking the manager until a high event was received if there was none
    /// at the time of the call. Lesson: it is crucial that this be non-blocking.
    pub fn flush_high_event(&self) -> anyhow::Result<()> {
        match self.highs.try_recv() {
            Ok(high) => {
                info!(high, action = "flushed memory.high event");
                Ok(())
            }
            Err(TryRecvError::Empty) => Ok(()), // Nothing to flush, all good
            Err(TryRecvError::Closed) => {
                anyhow::bail!(
                    "failed to flush possible outstanding high event due to closed channel"
                )
            }
        }
    }

    /// Read memory.events for the desired event type.
    fn get_event_count(name: &str, event: MemoryEvent) -> anyhow::Result<u64> {
        let path = format!("{}/{}/memory.events", UNIFIED_MOUNTPOINT, &name);
        let contents = fs::read_to_string(&path).expect("failed to read memory events info");

        // Then contents of the file look like:
        // low 42
        // high 101
        // ...
        Ok(contents
            .lines()
            .filter_map(|s| s.split_once(' '))
            .find(|(e, _)| *e == event.to_string())
            .map(|(_, count)| count.parse::<u64>())
            .ok_or(anyhow!("error getting memory.high event count"))
            .with_tee(|| format!("failed to find entry for memory.high events in {path}"))?
            .tee("failed to parse memory.high as u64")?)
    }

    /// Retrieve whether cgroup is frozen or thawed.
    pub fn state(&self) -> anyhow::Result<FreezerState> {
        Ok(self
            .freezer()
            .tee("failed to get freezer subsystem while attempting to get freezer state")?
            .state()
            .tee("failed to get freezer state")?)
    }

    /// Get a handle on the freezer subsystem.
    fn freezer(&self) -> anyhow::Result<&FreezerController> {
        if let Some(Freezer(freezer)) = self
            .cgroup
            .subsystems()
            .iter()
            .find(|sub| matches!(sub, Freezer(_)))
        {
            Ok(freezer)
        } else {
            anyhow::bail!("could not find freezer subsystem")
        }
    }

    /// Attempt to freeze the cgroup.
    pub fn freeze(&self) -> anyhow::Result<()> {
        Ok(self
            .freezer()
            .tee("failed to get freezer subsystem")?
            .freeze()
            .tee("failed to freeze")?)
    }

    /// Attempt to thaw the cgroup.
    pub fn thaw(&self) -> anyhow::Result<()> {
        Ok(self
            .freezer()
            .tee("failed to get freezer subsystem")?
            .thaw()
            .tee("failed to thaw")?)
    }

    /// Get a handle on the memory subsystem.
    ///
    /// Note: this method does not require `self.memory_update_lock` because
    /// getting a handle to the subsystem does not access any of the files we
    /// care about, such as memory.high and memory.events
    fn memory(&self) -> anyhow::Result<&MemController> {
        if let Some(Mem(memory)) = self
            .cgroup
            .subsystems()
            .iter()
            .find(|sub| matches!(sub, Mem(_)))
        {
            Ok(memory)
        } else {
            anyhow::bail!("could not find memory subsystem")
        }
    }

    /// Get cgroup current memory usage.
    pub fn current_memory_usage(&self) -> anyhow::Result<u64> {
        let _lock = self.memory_update_lock.lock().unwrap();
        info!("acquired lock on cgroup memory.* files");
        Ok(self
            .memory()
            .tee("failed to get memory subsystem")?
            .memory_stat()
            .usage_in_bytes)
    }

    /// Set cgroup memory.high threshold.
    pub fn set_high_bytes(&self, bytes: u64) -> anyhow::Result<()> {
        let _lock = self.memory_update_lock.lock().unwrap();
        info!("acquired lock on cgroup memory.* files");
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

    /// Set cgroup memory.high and memory.max.
    pub fn set_limits(&self, limits: &MemoryLimits) -> anyhow::Result<()> {
        let _lock = self.memory_update_lock.lock().unwrap();
        info!("acquired lock on cgroup memory.* files");
        info!(
            limits.high,
            limits.max,
            action = "writing new memory limits",
        );
        Ok(self
            .memory()
            .tee("failed to get memory subsystem while setting memory limits")?
            .set_mem(cgroups_rs::memory::SetMemory {
                min: None,
                low: None,
                high: Some(MaxValue::Value(
                    u64::min(limits.high, i64::MAX as u64) as i64
                )),
                max: Some(MaxValue::Value(u64::min(limits.max, i64::MAX as u64) as i64)),
            })
            .tee("failed to set memory limits")?)
    }

    /// Get memory.high threshold.
    pub fn get_high_bytes(&self) -> anyhow::Result<u64> {
        let _ = self.memory_update_lock.lock();
        info!("acquired lock on cgroup memory.* files");
        let high = self
            .memory()
            .tee("failed to get memory subsystem while getting memory statistics")?
            .get_mem()
            .map(|mem| mem.high)
            .tee("failed to get memory statistics from subsystem")?;
        match high {
            Some(MaxValue::Max) => Ok(i64::MAX as u64),
            Some(MaxValue::Value(high)) => Ok(high as u64),
            None => anyhow::bail!("failed to read memory.high from memory subsystem"),
        }
    }
}
