//! The `{output log, subscribers}` pair and the live-subscriber handle over
//! it -- docs/03-pty.md#reader-loop's "steps 2-4 happen under one
//! short mutex that also guards subscriber registration". See the parent
//! module doc (`session/mod.rs`) for the design this implements; `replay.rs`
//! is the other half of the story (catch-up reads off this same mutex).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::{mpsc, Semaphore};

use crate::log::{LogEvent, OutputLog};

use super::types::Chunk;

/// Queue bound per subscriber: a memory budget, in bytes
/// (docs/03-pty.md#backpressure).
///
/// `pub` so fixtures can size a burst against the real bound rather than a
/// number copied out of here that goes stale;
/// [`replay::LIVE_GAP_BYTES`](super::replay::LIVE_GAP_BYTES) is derived from
/// it too.
pub const MAX_QUEUE_BYTES: usize = 8 * 1024 * 1024;

/// What one queued chunk costs a subscriber *besides* its payload: a channel
/// slot (`Chunk` is a `u64` plus an `Arc` pointer, ~32 B with tokio's slot
/// state) plus that queue's share of the shared `Arc<[u8]>` header. Charged
/// alongside the payload so the budget bounds real memory instead of just
/// payload bytes -- a deliberate over-estimate, not a measured allocator
/// figure.
///
/// This is what the old `MAX_QUEUE_CHUNKS = 256` was really for, and getting
/// it wrong is teleport #N5: a count bound
/// calibrated for Linux's large reads let macOS, where a pty read averages
/// 14 bytes, disconnect a subscriber after ~3.5 KiB against a design that
/// promises 8 MiB.
const CHUNK_OVERHEAD: usize = 64;

/// Backstop only. Every chunk costs at least `CHUNK_OVERHEAD` permits, so
/// the budget above is always the binding half and this capacity cannot trip
/// first -- it exists because `mpsc::channel` requires a number, not as a
/// second bound with its own calibration.
const MAX_QUEUE_CHUNKS: usize = MAX_QUEUE_BYTES / CHUNK_OVERHEAD;

/// Permits one chunk costs: its payload plus the fixed per-chunk overhead.
/// `publish` acquires this before the chunk is visible and
/// `Subscription::recv` returns exactly it, which is the symmetry the budget
/// depends on.
fn queue_cost(len: usize) -> usize {
    len + CHUNK_OVERHEAD
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct SubscriberId(u64);

#[derive(Debug)]
pub(super) struct SubscriberSlot {
    id: SubscriberId,
    tx: mpsc::Sender<Chunk>,
    /// Memory budget for this subscriber's queue, shared with the matching
    /// `Subscription` so the receive side can return it as it drains.
    /// Starts at `MAX_QUEUE_BYTES` permits; `publish` acquires
    /// `queue_cost(len)` *before* the chunk is made visible via `try_send`,
    /// so a chunk can never reach `Subscription::recv` before it is already
    /// accounted for -- a plain counter bumped *after* `try_send` can't
    /// promise that (the receiver can drain and give the budget back
    /// first). This is the whole bound: the channel's capacity is a
    /// backstop that cannot trip first (`MAX_QUEUE_CHUNKS`).
    budget: Arc<Semaphore>,
    /// Set when `publish` drops this slot for exceeding its budget, and
    /// never otherwise. Shared with the matching `Subscription` so a
    /// consumer can tell *why* its stream ended: a slot closed at EOF and a
    /// slot dropped for falling behind both surface as `recv() -> None`,
    /// and those two mean opposite things to a client (docs/04-protocol.md).
    overflowed: Arc<AtomicBool>,
}

/// The `{output log, subscribers}` pair, guarded by one short-held mutex
/// (docs/03-pty.md#reader-loop: "steps 2-4 happen under one short
/// mutex that also guards subscriber registration"). The log carries
/// `next_offset`, so appending to the file and advancing the offset cannot
/// drift apart, and neither can drift from subscriber registration.
///
/// `pub(super)`: constructed in `manager.rs`'s `create()`, held by
/// `mod.rs`'s `Session` and by `replay.rs`'s `Replay`, so the whole
/// `session` subtree needs to see it -- no wider.
#[derive(Debug)]
pub(super) struct Fanout {
    pub(super) log: OutputLog,
    pub(super) subscribers: Vec<SubscriberSlot>,
    next_subscriber_id: u64,
}

impl Fanout {
    pub(super) fn new(log: OutputLog) -> Self {
        Self {
            log,
            subscribers: Vec::new(),
            next_subscriber_id: 0,
        }
    }

    /// Runs on the PTY reader thread via the `on_output` closure passed to
    /// `pty::spawn`. Must never block: the append is a `write_all` into the
    /// page cache -- the periodic `fsync` is `LogSyncer`'s thread, never this
    /// one -- every send is `try_send`, budget acquisition is `try_acquire`,
    /// and a subscriber that would exceed either bound is dropped instead of
    /// waited on (docs/03-pty.md#the-rule).
    ///
    /// Returns whatever the log wants recorded; the caller traces it off the
    /// hot path rather than this method reaching for a logger under the lock.
    pub(super) fn publish(&mut self, bytes: &[u8]) -> Vec<LogEvent> {
        let appended = self.log.append(bytes);
        let start = appended.start;

        if self.subscribers.is_empty() {
            return appended.events; //  zero subscribers is fine, indefinitely.
        }

        let payload: Arc<[u8]> = Arc::from(bytes);
        self.subscribers.retain(|sub| {
            // Chunks are bounded by pty.rs's READ_BUFFER_SIZE (64 KiB), so
            // this always fits u32; MAX_QUEUE_BYTES itself fits comfortably
            // under Semaphore's permit ceiling.
            let cost = u32::try_from(queue_cost(payload.len()))
                .expect("a chunk cannot exceed pty.rs's READ_BUFFER_SIZE");
            let Ok(permit) = sub.budget.try_acquire_many(cost) else {
                // Bound tripped -- disconnect, don't wait. Record *why*, so
                // the stream closes without an exit frame.
                sub.overflowed.store(true, Ordering::Release);
                return false;
            };
            let chunk = Chunk {
                offset: start,
                bytes: Arc::clone(&payload),
            };
            match sub.tx.try_send(chunk) {
                Ok(()) => {
                    permit.forget(); // returned by Subscription::recv once this chunk is drained
                    true
                }
                // The Subscription was dropped (or, unreachably, the backstop
                // capacity filled). `permit` drops here too, returning the
                // budget it reserved. Flagged the same way: if a receiver is
                // still there to read it, this was the backstop, not an EOF.
                Err(_) => {
                    sub.overflowed.store(true, Ordering::Release);
                    false
                }
            }
        });

        appended.events
    }

    /// Registers a subscriber slot. Split out of `Session::subscribe` so
    /// `Replay::next_round` can register *and* read the replay boundary under
    /// one acquisition of the lock -- that atomicity is the whole attach-race
    /// fix (docs/04-protocol.md).
    pub(super) fn register(&mut self, fanout: &Arc<Mutex<Fanout>>) -> Subscription {
        let (tx, rx) = mpsc::channel(MAX_QUEUE_CHUNKS);
        let budget = Arc::new(Semaphore::new(MAX_QUEUE_BYTES));
        let overflowed = Arc::new(AtomicBool::new(false));

        let id = SubscriberId(self.next_subscriber_id);
        self.next_subscriber_id += 1;
        self.subscribers.push(SubscriberSlot {
            id,
            tx,
            budget: Arc::clone(&budget),
            overflowed: Arc::clone(&overflowed),
        });

        Subscription {
            id,
            rx,
            budget,
            overflowed,
            fanout: Arc::clone(fanout),
        }
    }
}

/// A live subscription to a session's output. Dropping it unregisters from
/// the session's `Fanout` -- an idle session does not accumulate dead slots
/// waiting for the next chunk to notice them.
#[derive(Debug)]
pub struct Subscription {
    id: SubscriberId,
    rx: mpsc::Receiver<Chunk>,
    budget: Arc<Semaphore>,
    overflowed: Arc<AtomicBool>,
    fanout: Arc<Mutex<Fanout>>,
}

impl Subscription {
    /// Waits for the next chunk. `None` means the session's fan-out closure
    /// (and therefore the reader thread's handle to it) is gone.
    pub async fn recv(&mut self) -> Option<Chunk> {
        let chunk = self.rx.recv().await?;
        self.budget.add_permits(queue_cost(chunk.bytes.len()));
        Some(chunk)
    }

    /// Whether this subscription ended because it fell behind its budget,
    /// as opposed to the session reaching EOF. Only meaningful once `recv`
    /// has returned `None`.
    ///
    /// Explicit because it cannot be inferred: `Session::close_subscribers`
    /// (EOF) and `publish` (overflow) both drop the slot, and the session's
    /// `state` is written by a *different* thread than the one that closes
    /// the subscribers, so reading it here races.
    pub fn overflowed(&self) -> bool {
        self.overflowed.load(Ordering::Acquire)
    }

    /// Non-blocking drain, used only when finalizing an `exit` frame: even
    /// with the bounded eof/grace wait in `ws.rs`, a chunk can become ready
    /// in the same poll that decides to finalize, so this mops it up before
    /// `final_offset` is read. `None` covers both "nothing queued right now"
    /// and "the fan-out is gone" -- finalizing treats them the same.
    pub fn try_recv(&mut self) -> Option<Chunk> {
        let chunk = self.rx.try_recv().ok()?;
        self.budget.add_permits(queue_cost(chunk.bytes.len()));
        Some(chunk)
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        let mut fanout = self.fanout.lock();
        fanout.subscribers.retain(|s| s.id != self.id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::LogLimits;
    use std::path::PathBuf;

    /// A `Fanout` over a real log in a throwaway directory -- these tests
    /// exercise fan-out mechanics, not PTY spawning.
    fn scratch_fanout(dir: &std::path::Path) -> Arc<Mutex<Fanout>> {
        let log = OutputLog::open(dir, LogLimits::default(), None).expect("open log");
        Arc::new(Mutex::new(Fanout::new(log)))
    }

    fn scratch_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "anchor-session-unit-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    /// Dropping a `Subscription` unregisters it immediately, not lazily on
    /// the next chunk -- an idle session must not accumulate dead slots.
    /// Needs `Fanout::subscribers` (private), so this lives here rather than
    /// in `daemon/tests/`.
    #[tokio::test]
    async fn dropped_subscription_is_unregistered_immediately() {
        let dir = scratch_dir("drop");
        let fanout = scratch_fanout(&dir);

        let sub = fanout.lock().register(&fanout);
        assert_eq!(fanout.lock().subscribers.len(), 1);

        drop(sub);
        assert_eq!(
            fanout.lock().subscribers.len(),
            0,
            "Drop must remove the slot without waiting for output"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// teleport #N5: the bound is a memory budget,
    /// so a stream of tiny chunks gets the headroom the design promises rather
    /// than the 256 slots an `mpsc::channel` capacity used to impose. macOS
    /// pty reads average 14 bytes; under the old count bound a subscriber was
    /// disconnected after 256 of them (~3.5 KiB) against a documented 8 MiB.
    ///
    /// Both halves matter and neither alone is the fix: it must admit *far*
    /// more than 256, and it must still be bounded.
    #[tokio::test]
    async fn tiny_chunks_get_the_budget_not_a_slot_count() {
        let dir = scratch_dir("tiny-chunks");
        let fanout = scratch_fanout(&dir);
        let _sub = fanout.lock().register(&fanout);

        // A 14-byte chunk: the measured macOS mean.
        let chunk = [b'x'; 14];
        let mut admitted = 0usize;
        while !fanout.lock().subscribers.is_empty() {
            fanout.lock().publish(&chunk);
            admitted += 1;
            assert!(
                admitted <= MAX_QUEUE_CHUNKS,
                "the queue is unbounded: {admitted} chunks accepted with nobody draining"
            );
        }

        let ceiling = MAX_QUEUE_BYTES / queue_cost(chunk.len());
        assert_eq!(
            admitted,
            ceiling + 1,
            "a never-drained subscriber must take exactly the budget, then be dropped"
        );
        assert!(
            admitted > 256 * 100,
            "still effectively slot-bound at {admitted} chunks -- N5 has regressed"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The ordering rule, checked from the outside: by the time a chunk is
    /// visible to a subscriber, its bytes are already readable at the offset
    /// it was tagged with (docs/03-pty.md#reader-loop). Needs private
    /// `Fanout`, so it lives here.
    #[tokio::test]
    async fn a_published_offset_is_already_on_disk() {
        let dir = scratch_dir("ordering");
        let fanout = scratch_fanout(&dir);
        let mut sub = fanout.lock().register(&fanout);

        fanout.lock().publish(b"abc");
        fanout.lock().publish(b"defgh");

        let path = dir.join(crate::log::LOG_FILE_NAME);
        for expected_offset in [0u64, 3] {
            let chunk = sub.recv().await.expect("chunk");
            assert_eq!(chunk.offset, expected_offset);
            let on_disk = std::fs::read(&path).expect("read log");
            assert!(
                on_disk.len() as u64 >= chunk.offset + chunk.bytes.len() as u64,
                "chunk at offset {} is {} bytes, but only {} are on disk",
                chunk.offset,
                chunk.bytes.len(),
                on_disk.len()
            );
            let offset =
                usize::try_from(chunk.offset).expect("checked against on_disk.len() above");
            assert_eq!(&on_disk[offset..][..chunk.bytes.len()], &*chunk.bytes);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
