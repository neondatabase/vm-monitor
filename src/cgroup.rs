use std::{
    fmt::{Debug, Display},
    fs,
    pin::Pin,
    sync::atomic::{AtomicU64, Ordering},
    task::{self, Poll},
};

use anyhow::{anyhow, Context};
use async_std::channel::{Receiver, Sender};
use cgroups_rs::{
    freezer::FreezerController,
    hierarchies::{self, is_cgroup2_unified_mode, UNIFIED_MOUNTPOINT},
    memory::MemController,
    MaxValue,
    Subsystem::{Freezer, Mem},
};
use futures_util::{stream::BoxStream, Future, FutureExt, Stream};
use inotify::{EventStream, Inotify, WatchMask};
use tokio::{
    sync::Mutex,
    time::{Duration, Instant},
};
use tokio_stream::StreamExt;
use tracing::{info, warn};

use crate::protocol::Resources;
use crate::MiB;

/// Monotonically increasing counter of the number of memory.high events
/// the cgroup has experienced.
///
/// We use this to determine if a modification to the `memory.events` file actually
/// changed the `high` field. If not, we don't care about the change. When we
/// read the file, we check the `high` field in the file against `MEMORY_EVENT_COUNT`
/// to see if it changed since last time.
pub static MEMORY_EVENT_COUNT: AtomicU64 = AtomicU64::new(0);

/// Monotonically increasing counter that gives each cgroup event a unique id.
///
/// This allows us to answer questions like "did this upscale arrive before this
/// memory.high?". This static is also used by the `Sequenced` type to "tag" values
/// with a sequence number. As such, prefer to used the `Sequenced` type rather
/// than this static directly.
static EVENT_SEQUENCE_NUMBER: AtomicU64 = AtomicU64::new(0);

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

impl MemoryEvent {
    fn as_str(&self) -> &str {
        match self {
            MemoryEvent::Low => "low",
            MemoryEvent::High => "high",
            MemoryEvent::Max => "max",
            MemoryEvent::Oom => "oom",
            MemoryEvent::OomKill => "oom_kill",
            MemoryEvent::OomGroupKill => "oom_group_kill",
        }
    }
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

/// An event that the `CgroupWatcher` responds to.
///
/// The `CroupWatcher`'s job primarily consists of reacting to these events.
/// See the `CgroupWatcher` docs and associated impl items for more information.
#[derive(Debug, Clone)]
enum EventKind {
    /// We were upscaling to the contained allocation
    Upscale(Resources),

    /// Number of memory.highs
    MemoryHigh(u64),
}

/// An extension to a stream that makes it peekable.
///
/// Note: this should probably be replaced using futures::StreamExt::Peekable
struct PeekableStream<T> {
    stream: BoxStream<'static, T>,
    peek: Option<T>,
}

impl<T> Debug for PeekableStream<T>
where
    T: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeekableStream")
            .field("stream", &"{ .. }")
            .field("peek", &self.peek)
            .finish()
    }
}

impl<T> PeekableStream<T> {
    pub fn new<S>(stream: S) -> Self
    where
        S: Stream<Item = T> + Send + 'static,
    {
        Self {
            stream: futures_util::StreamExt::boxed(stream),
            peek: None,
        }
    }

    /// Peek the stream, but don't await a new element if there isn't currently
    /// one in that we've already peeked.
    pub fn peek_eager(&mut self) -> Option<&T> {
        self.peek.as_ref()
    }

    /// Peek the stream.
    pub async fn peek(&mut self) -> Option<&T> {
        if self.peek.is_none() {
            self.peek = self.stream.next().await;
        }
        return self.peek.as_ref();
    }
}

impl<T> Stream for PeekableStream<T>
where
    T: Unpin,
{
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Option<Self::Item>> {
        // first check if there's something we've already pulled from the stream
        // in the peek
        if let Some(data) = self.as_mut().peek.take() {
            return Poll::Ready(Some(data));
        }
        // otherwise we actually need to pull something from the stream
        let next = self.stream.next();
        tokio::pin!(next);
        Future::poll(Pin::new(&mut next), cx)
    }
}

/// Configuration for a `CgroupWatcher`
#[derive(Debug, Clone)]
pub struct Config {
    // The target difference between the total memory reserved for the cgroup
    // and the value of the cgroup's memory.high.
    //
    // In other words, memory.high + oom_buffer_bytes will equal the total memory that the cgroup may
    // use (equal to system memory, minus whatever's taken out for the file cache).
    oom_buffer_bytes: u64,

    // The amount of memory, in bytes, below a proposed new value for
    // memory.high that the cgroup's memory usage must be for us to downscale
    //
    // In other words, we can downscale only when:
    //
    //   memory.current + memory_high_buffer_bytes < (proposed) memory.high
    //
    // TODO: there's some minor issues with this approach -- in particular, that we might have
    // memory in use by the kernel's page cache that we're actually ok with getting rid of.
    pub(crate) memory_high_buffer_bytes: u64,

    // The maximum duration, in milliseconds, that we're allowed to pause
    // the cgroup for while waiting for the autoscaler-agent to upscale us
    max_upscale_wait: Duration,

    // The required minimum time, in milliseconds, that we must wait before re-freezing
    // the cgroup while waiting for the autoscaler-agent to upscale us.
    do_not_freeze_more_often_than: Duration,

    // The amount of memory, in bytes, that we should periodically increase memory.high
    // by while waiting for the autoscaler-agent to upscale us.
    //
    // This exists to avoid the excessive throttling that happens when a cgroup is above its
    // memory.high for too long. See more here:
    // https://github.com/neondatabase/autoscaling/issues/44#issuecomment-1522487217
    memory_high_increase_by_bytes: u64,

    // The period, in milliseconds, at which we should repeatedly increase the value
    // of the cgroup's memory.high while we're waiting on upscaling and memory.high
    // is still being hit.
    //
    // Technically speaking, this actually serves as a rate limit to moderate responding to
    // memory.high events, but these are roughly equivalent if the process is still allocating
    // memory.
    memory_high_increase_every: Duration,
}

impl Config {
    /// Calculate the new value for the cgroups memory.high based on system memory
    pub fn calculate_memory_high_value(&self, total_system_mem: u64) -> u64 {
        total_system_mem.saturating_sub(self.oom_buffer_bytes)
    }
}

impl Default for Config {
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

/// Used to represent data that is associated with a certain point in time, such
/// as an upscale request or memory.high event.
///
/// Internally, creating a `Sequenced` uses a static atomic counter to obtain
/// a unique sequence number. Sequence numbers are monotonically increasing,
/// allowing us to answer questions like "did this upscale happen after this
/// memory.high event?" by comparing the sequence numbers of the two events.
#[derive(Debug, Clone)]
pub struct Sequenced<T> {
    seqnum: u64,
    data: T,
}

impl<T> Sequenced<T> {
    pub fn new(data: T) -> Self {
        Self {
            seqnum: EVENT_SEQUENCE_NUMBER.fetch_add(1, Ordering::AcqRel),
            data,
        }
    }
}

/// Responds to `MonitorEvents` to manage the cgroup: preventing it from being
/// OOM killed or throttling.
///
/// The `CgroupWatcher` primarily achieves this by reading from a stream of
/// `MonitorEvent`s. See `main_signals_loop` for details on how to keep the
/// cgroup happy.
#[derive(Debug)]
pub struct CgroupWatcher {
    pub config: Config,

    /// The event stream that informs cgroup actions. Consists of upscale requests
    /// and `memory.high` events.
    ///
    /// Note: this is wrapped in a mutex because otherwise we need `&mut self` to
    /// do anything useful. This is a problem because `main_signals_loop` never
    /// (or should never) return, so it effectively takes ownership of the
    /// `Cgroup`. However, the monitor still needs to be able to do things like
    /// get/set memory. Therefore we wrap the stream in a Mutex. It's a little
    /// ugly, but the Mutex is never contended so this shouldn't have any ill effects.
    events: Mutex<PeekableStream<Sequenced<EventKind>>>,

    /// The sequence number of the last upscale.
    ///
    /// If we receive a memory.high event that has a _lower_ sequence number than
    /// `last_upscale_seqnum`, then we know it occured before the upscale, and we
    /// can safely ignore it.
    ///
    /// Note: Like the `events` field, this doesn't _need_ interior mutability but we
    /// use it anyways so that methods take `&self`, not `&mut self`.
    last_upscale_seqnum: AtomicU64,

    /// The name of the cgroup we are managing
    // Won't be necessary if/once https://github.com/kata-containers/cgroups-rs/pull/120
    // is merged
    name: String,

    /// A channel on which we send messages to request upscale from the dispatcher.
    upscale_requester: Sender<()>,

    /// The actual cgroup we are watching and managing.
    cgroup: cgroups_rs::Cgroup,

    /// The direct source of upscale events, for if we need to listen for an
    /// upscale event _in particular_.
    ///
    /// All events from this channel are also piped into `self.events`. Thus,
    /// when awaiting upscales directly, first check if an upscale has already
    /// been peeked and is held in events, then directly await this channel.
    upscale_receiver: Receiver<Sequenced<Resources>>,
}

/// Read memory.events for the desired event type.
///
/// `path` specifies the path to the desired `memory.events` file.
/// For more info, see the `memory.events` section of the [kernel docs]
/// (https://docs.kernel.org/admin-guide/cgroup-v2.html#memory-interface-files)
fn get_event_count(path: &str, event: MemoryEvent) -> anyhow::Result<u64> {
    let contents = fs::read_to_string(path)
        .with_context(|| format!("failed to read memory.events from {path}"))?;

    // Then contents of the file look like:
    // low 42
    // high 101
    // ...
    contents
        .lines()
        .filter_map(|s| s.split_once(' '))
        .find(|(e, _)| *e == event.as_str())
        .ok_or_else(|| anyhow!("failed to find entry for memory.{event} events in {path}"))
        .and_then(|(_, count)| {
            count
                .parse::<u64>()
                .with_context(|| format!("failed to parse memory.{event} as u64"))
        })
}

/// Create an event stream that produces events whenever the file at the provided
/// path is modified.
fn create_file_watcher(path: &str) -> anyhow::Result<EventStream<[u8; 1024]>> {
    info!("creating file watcher for {path}");
    let inotify = Inotify::init().context("failed to initialize file watcher")?;
    inotify
        .watches()
        .add(path, WatchMask::MODIFY)
        .with_context(|| format!("failed to start watching {path}"))?;
    inotify
        // The inotify docs use [0u8; 1024] so we'll just copy them. We only need
        // to store one event at a time - if the event gets written over, that's
        // ok. We still see that there is an event. For more information, see:
        // https://man7.org/linux/man-pages/man7/inotify.7.html
        .into_event_stream([0u8; 1024])
        .context("failed to start inotify event stream")
}

impl CgroupWatcher {
    #[tracing::instrument]
    /// Create a new `CgroupWatcher`.
    pub fn new(
        name: String,
        // A channel on which upscale notifications will be sent
        upscale_notifier: Receiver<Sequenced<Resources>>,
        // A channel on which to send upscale requests
        upscale_requester: Sender<()>,
    ) -> anyhow::Result<Self> {
        // TODO: clarify exactly why we need v2
        // Make sure cgroups v2 (aka unified) are supported
        if !is_cgroup2_unified_mode() {
            anyhow::bail!("cgroups v2 not supported");
        }
        let cgroup = cgroups_rs::Cgroup::load(hierarchies::auto(), &name);

        // Start monitoring the cgroup for memory events. In general, for
        // cgroups v2 (aka unified), metrics are reported in files like
        // > `/sys/fs/cgroup/{name}/{metric}`
        // We are looking for `memory.high` events, which are stored in the
        // file `memory.events`. For more info, see the `memory.events` section
        // of https://docs.kernel.org/admin-guide/cgroup-v2.html#memory-interface-files
        let path = format!("{}/{}/memory.events", UNIFIED_MOUNTPOINT, &name);
        let memory_events = create_file_watcher(&path)
            .with_context(|| format!("failed to create event watcher for {path}"))?
            // This would be nice with with .inspect_err followed by .ok
            .filter_map(move |_| match get_event_count(&path, MemoryEvent::High) {
                Ok(high) => Some(high),
                Err(error) => {
                    // TODO: Might want to just panic here
                    warn!(?error, "failed to read high events count from {}", &path);
                    None
                }
            })
            // Only report the event if the memory.high count increased
            .filter_map(|high| {
                if MEMORY_EVENT_COUNT.fetch_max(high, Ordering::AcqRel) < high {
                    Some(high)
                } else {
                    None
                }
            })
            .map(|e| Sequenced::new(EventKind::MemoryHigh(e)));

        let initial_count = get_event_count(
            &format!("{}/{}/memory.events", UNIFIED_MOUNTPOINT, &name),
            MemoryEvent::High,
        )?;

        info!(initial_count, "initial memory.high event count");

        // Hard update `MEMORY_EVENT_COUNT` since there could have been processes
        // running in the cgroup before that caused it to be non-zero.
        MEMORY_EVENT_COUNT.fetch_max(initial_count, Ordering::AcqRel);

        // Add the upscale requests into the stream
        let all_events =
            memory_events.merge(upscale_notifier.clone().map(|Sequenced { seqnum, data }| {
                Sequenced {
                    seqnum,
                    data: EventKind::Upscale(data),
                }
            }));

        Ok(Self {
            name,
            cgroup,
            upscale_requester,
            events: Mutex::new(PeekableStream::new(all_events)),
            upscale_receiver: upscale_notifier,
            last_upscale_seqnum: AtomicU64::new(0),
            config: Default::default(),
        })
    }

    /// The entrypoint for the `CgroupWatcher`.
    #[tracing::instrument(skip(self))]
    pub async fn watch(&self) -> anyhow::Result<()> {
        // There are several actions might do when receiving a `memory.high`,
        // such as freezing the cgroup, or increasing its `memory.high`. We don't
        // want to do these things too often (because postgres needs to run, and
        // we only have so much memory). These timers serve as rate limits for this.
        let wait_to_freeze = tokio::time::sleep(Duration::ZERO);
        let wait_to_increase_memory_high = tokio::time::sleep(Duration::ZERO);

        tokio::pin!(wait_to_freeze, wait_to_increase_memory_high);

        // Are we waiting to be upscaled? Could be true if we request upscale due
        // to a memory.high event and it does not arrive in time.
        let mut waiting_on_upscale = false;

        while let Some(Sequenced { seqnum, data }) = {
            // Get the next element in a block to avoid holding the lock for the
            // entire loop body - this will cause deadlocks
            let mut stream = self.events.lock().await;
            let next = stream.next().await;
            drop(stream);
            next
        } {
            match data {
                EventKind::Upscale(_) => self.last_upscale_seqnum.store(seqnum, Ordering::Release),
                EventKind::MemoryHigh(_) => {
                    // The memory.high came before our last upscale, so we consider
                    // it resolved
                    if self.last_upscale_seqnum.fetch_max(seqnum, Ordering::AcqRel) > seqnum {
                        info!(
                            "received memory.high event, but it came before our last upscale -> ignoring it"
                        );
                        continue;
                    }

                    // The memory.high came after our latest upscale. We don't
                    // want to do anything yet, so peek the next event in hopes
                    // that it's an upscale.
                    if let Some(Sequenced {
                        seqnum: peeknum,
                        data: EventKind::Upscale(_),
                    }) = self.events.lock().await.peek_eager()
                    {
                        if *peeknum > seqnum {
                            info!(
                                "received memory.high event, but it came before our last upscale -> ignoring it"
                            );
                            continue;
                        }
                    }

                    // If it's been long enough since we last froze, freeze the
                    // cgroup and request upscale
                    if wait_to_freeze.is_elapsed() {
                        info!("received memory.high event -> requesting upscale");
                        waiting_on_upscale = self
                            .handle_memory_high_event()
                            .await
                            .context("failed to handle upscale")?;
                        wait_to_freeze
                            .as_mut()
                            .reset(Instant::now() + self.config.do_not_freeze_more_often_than);
                        continue;
                    }

                    // Ok, we can't freeze, just request upscale
                    if !waiting_on_upscale {
                        info!("received memory.high event, but too soon to refreeze -> requesting upscale");

                        // Make check to make sure we haven't been upscaled in the
                        // meantine (can happen if the agent independently decides
                        // to upscale us again)
                        if self.upscale_receiver.recv().now_or_never().is_some() {
                            info!("no need to request upscaling because we got upscaled");
                            continue;
                        }
                        self.upscale_requester
                            .send(())
                            .await
                            .context("failed to request upscale")?;
                        continue;
                    }

                    // Shoot, we can't freeze or and we're still waiting on upscale,
                    // increase memory.high to reduce throttling
                    if wait_to_increase_memory_high.is_elapsed() {
                        info!(
                            "received memory.high event, \
                            but too soon to refreeze and already requested upscale \
                            -> increasing memory.high"
                        );

                        // Make check to make sure we haven't been upscaled in the
                        // meantine (can happen if the agent independently decides
                        // to upscale us again)
                        if self.upscale_receiver.recv().now_or_never().is_some() {
                            info!("no need to increase memory.high because got upscaled");
                            continue;
                        }

                        // Request upscale anyways (the agent will handle deduplicating
                        // requests)
                        self.upscale_requester
                            .send(())
                            .await
                            .context("failed to request upscale")?;

                        let memory_high =
                            self.get_high_bytes().context("failed to get memory.high")?;
                        let new_high = memory_high + self.config.memory_high_increase_by_bytes;
                        info!(
                            current_high_bytes = memory_high,
                            new_high_bytes = new_high,
                            "updating memory.high"
                        );
                        self.set_high_bytes(new_high)
                            .context("failed to set memory.high")?;
                        wait_to_increase_memory_high
                            .as_mut()
                            .reset(Instant::now() + self.config.memory_high_increase_every)
                    }

                    // we can't do anything
                }
            };
        }
        Ok(())
    }

    /// Handle a `memory.high`, returning whether we are still waiting on upscale
    /// by the time the function returns.
    ///
    /// The general plan for handling a `memory.high` event is as follows:
    /// 1. Freeze the cgroup
    /// 2. Start a timer for `self.config.max_upscale_wait`
    /// 3. Request upscale
    /// 4. After the timer elapses or we receive upscale, thaw the cgroup.
    /// 5. Return whether or not we are still waiting for upscale. If we are,
    ///    we'll increase the cgroups memory.high to avoid getting oom killed
    #[tracing::instrument(skip(self))]
    pub async fn handle_memory_high_event(&self) -> anyhow::Result<bool> {
        // Immediately freeze the cgroup before doing anything else.
        info!("received memory.high event -> freezing cgroup");
        self.freeze().context("failed to freeze cgroup")?;

        // We'll use this for logging durations
        let start_time = Instant::now();

        // Start a timer for the maximum time we'll leave the cgroup frozen for:
        let thaw_deadline = tokio::time::sleep(self.config.max_upscale_wait);

        // Request the upscale
        info!(
            wait = ?self.config.max_upscale_wait,
            "sending request for immediate upscaling",
        );
        self.upscale_requester
            .send(())
            .await
            .context("failed to request upscale")?;

        let mut waiting_on_upscale = true;
        tokio::select! {
            // Deadline passed without us receiving upscale
            _ = thaw_deadline => {
                info!(elapsed = ?start_time.elapsed(), "timed out waiting for upscale");
            }

            // Upscaled :)
            res = self.await_upscale() => {
                res.context("failed to await upscale")?;
                info!(elapsed = ?start_time.elapsed(), "received upscale in time");
                waiting_on_upscale = false;
            }
        }

        info!("thawing cgroup");
        self.thaw().context("failed to thaw cgroup")?;

        Ok(waiting_on_upscale)
    }

    /// Await an upscale event, discarding any `memory.high` events received in
    /// the process.
    ///
    /// This is used in `handle_memory_high_event`, where we need to listen
    /// for upscales in particular so we know if we can thaw the cgroup early.
    #[tracing::instrument(skip(self))]
    async fn await_upscale(&self) -> anyhow::Result<()> {
        // First check if we've already peeked the upscale.
        //
        // Take the lock in a block so that it gets dropped immediately after.
        // If we take the lock in the <expr> section of if let <pat> = <expr>,
        // it'll be help for the whole block - deadlocking when we take the lock
        // again and call next to clear out the peek.
        let peek = {
            let mut guard = self.events.lock().await;
            guard.peek_eager().cloned()
        };

        if let Some(Sequenced {
            seqnum,
            data: EventKind::Upscale(_),
        }) = peek
        {
            // Clear out the peek
            let _ = self.events.lock().await.next().await;
            self.last_upscale_seqnum.store(seqnum, Ordering::Release);
            return Ok(());
        }

        // If we haven't peeked the upscale, directly await it
        let Sequenced { seqnum, .. } = self
            .upscale_receiver
            .recv()
            .await
            .context("error listening for upscales")?;

        self.last_upscale_seqnum.store(seqnum, Ordering::Release);
        Ok(())
    }

    /// Get the cgroup's name.
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Represents a set of limits we apply to a cgroup to control memory usage.
///
/// Setting these values also affects the thresholds for receiving usage alerts.
#[derive(Debug)]
pub struct MemoryLimits {
    high: u64,
    max: u64,
}

impl MemoryLimits {
    pub fn new(high: u64, max: u64) -> Self {
        Self { max, high }
    }
}

// Methods for manipulating the actual cgroup
impl CgroupWatcher {
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
        self.freezer()
            .context("failed to get freezer subsystem")?
            .freeze()
            .context("failed to freeze")
    }

    /// Attempt to thaw the cgroup.
    pub fn thaw(&self) -> anyhow::Result<()> {
        self.freezer()
            .context("failed to get freezer subsystem")?
            .thaw()
            .context("failed to thaw")
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
        Ok(self
            .memory()
            .context("failed to get memory subsystem")?
            .memory_stat()
            .usage_in_bytes)
    }

    /// Set cgroup memory.high threshold.
    pub fn set_high_bytes(&self, bytes: u64) -> anyhow::Result<()> {
        self.memory()
            .context("failed to get memory subsystem")?
            .set_mem(cgroups_rs::memory::SetMemory {
                low: None,
                high: Some(MaxValue::Value(bytes.min(i64::MAX as u64) as i64)),
                min: None,
                max: None,
            })
            .context("failed to set memory.high")
    }

    /// Set cgroup memory.high and memory.max.
    pub fn set_limits(&self, limits: &MemoryLimits) -> anyhow::Result<()> {
        info!(limits.high, limits.max, "writing new memory limits",);
        self.memory()
            .context("failed to get memory subsystem while setting memory limits")?
            .set_mem(cgroups_rs::memory::SetMemory {
                min: None,
                low: None,
                high: Some(MaxValue::Value(limits.high.min(i64::MAX as u64) as i64)),
                max: Some(MaxValue::Value(limits.max.min(i64::MAX as u64) as i64)),
            })
            .context("failed to set memory limits")
    }

    /// Given some amount of available memory, set the desired cgroup memory limits
    pub fn set_memory_limits(&mut self, available_memory: u64) -> anyhow::Result<()> {
        let new_high = self.config.calculate_memory_high_value(available_memory);
        let limits = MemoryLimits::new(new_high, available_memory);
        info!(
            name = self.name,
            memory = ?limits,
            "setting cgroup memory",
        );
        self.set_limits(&limits)
            .context("failed to set cgroup memory limits")?;
        Ok(())
    }

    /// Get memory.high threshold.
    pub fn get_high_bytes(&self) -> anyhow::Result<u64> {
        let high = self
            .memory()
            .context("failed to get memory subsystem while getting memory statistics")?
            .get_mem()
            .map(|mem| mem.high)
            .context("failed to get memory statistics from subsystem")?;
        match high {
            Some(MaxValue::Max) => Ok(i64::MAX as u64),
            Some(MaxValue::Value(high)) => Ok(high as u64),
            None => anyhow::bail!("failed to read memory.high from memory subsystem"),
        }
    }
}
