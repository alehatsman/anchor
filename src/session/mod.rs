//! Session ownership: the PTY, its output log, and the subscriber fan-out.
//!
//! See docs/03-pty.md for the rules this implements. Two of them shape
//! everything here:
//!
//! - **The PTY reader must never wait for a subscriber.** The reader thread's
//!   `on_output` closure appends, advances the offset and fans out under one
//!   short mutex, and every send on it is a `try_send`.
//! - **`exited` fires off the reaper's `wait()`, never off reader EOF.** A
//!   descendant holding the master open can delay EOF long after the child is
//!   gone. Both signals are consumed, on their own threads, for different
//!   purposes.
//!
//! What teleport keeps here and anchor deliberately does not: attention
//! signals (idle, bell), OSC title scanning and Claude resume ids, and the
//! preset/kind metadata. Those are client concerns under docs/01-scope.md —
//! labels carry whatever a client needs instead.

pub mod control;
pub mod fanout;
pub mod manager;
pub mod persist;
pub mod replay;
pub mod types;

pub use fanout::{Subscription, MAX_QUEUE_BYTES};
pub use manager::{CreateError, SessionManager};
pub use replay::{
    Attach, AttachError, HistoryReplay, Replay, ReplayStep, LIVE_GAP_BYTES, REPLAY_ROUND_BYTES,
};
pub use types::Chunk;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use parking_lot::Mutex;
use tokio::sync::watch;

use crate::log::SyncHandle;
use crate::model::{SessionId, SpawnSpec, State};
use crate::pty::{PtySession, TerminalSession};

use fanout::Fanout;

/// Mutable per-session state that is not the PTY's own.
#[derive(Debug)]
pub(crate) struct Runtime {
    pub(crate) state: State,
    pub(crate) exit_code: Option<i32>,
    pub(crate) lost_reason: Option<&'static str>,
    pub(crate) exited_at: Option<i64>,
    pub(crate) cols: u16,
    pub(crate) rows: u16,
}

/// At most one resize reaches the PTY per this window, and it is always the
/// latest one asked for. A browser drag emits a resize per frame; each one
/// is a `TIOCSWINSZ` plus a `SIGWINCH` the child has to handle, and a shell
/// redrawing on every frame of a drag is the visible cost.
const RESIZE_COALESCE_WINDOW: Duration = Duration::from_millis(100);

/// Resize coalescing state. `pending` is what has been asked for but not yet
/// applied; the flusher applies it when the window closes.
#[derive(Debug)]
pub(crate) struct ResizeState {
    pub(crate) last_applied: Instant,
    pub(crate) pending: Option<(u16, u16)>,
    /// True while a flusher is scheduled, so a burst spawns one, not one per
    /// resize.
    pub(crate) flushing: bool,
}

/// Who may write input and resize. See `control.rs`.
#[derive(Debug, Default)]
pub(crate) struct ControlLease {
    pub(crate) holder: Option<String>,
    pub(crate) epoch: u64,
}

/// One session: the PTY primitive plus the fan-out layered over it.
///
/// No lock around `pty`. `TerminalSession`'s `&self` methods already let
/// `write`, `resize` and `terminate` run independently, each blocking only as
/// long as its own PTY thread does. A shared mutex here would serialize them
/// again and wedge `terminate` behind a stuck write.
#[derive(Debug)]
pub struct Session {
    /// This session's identity — a ULID.
    pub id: SessionId,
    /// What was spawned and how. Fixed at creation.
    pub spec: SpawnSpec,
    /// Caller-owned key/value. anchor never interprets a label.
    pub labels: Mutex<std::collections::BTreeMap<String, String>>,
    pty: PtySession,
    pid: Option<u32>,
    fanout: Arc<Mutex<Fanout>>,
    runtime: Mutex<Runtime>,
    resize: Mutex<ResizeState>,
    /// Queues the applied size for persistence, off this thread.
    persist: Option<crate::session::persist::OutputPersister>,
    pub(crate) control: Mutex<ControlLease>,
    created_at: i64,
    /// `true` once the exit listener has recorded a final state. A `watch`,
    /// not a one-shot: "has this exited yet" is a level, and a client that
    /// attaches after the exit must still see it.
    exited: watch::Receiver<bool>,
    exited_tx: watch::Sender<bool>,
    /// `true` once the reader thread saw real EOF on the master. Not a state
    /// signal — it exists so a caller can bound how long it waits for
    /// trailing output after `exited` fires.
    eof: watch::Receiver<bool>,
    eof_tx: watch::Sender<bool>,
    /// Flushes the log without touching `fanout`. Deliberately not reached
    /// through that lock: an fsync held under the mutex the reader thread
    /// takes would stall the PTY behind disk latency.
    sync: SyncHandle,
}

impl Session {
    /// Registers a subscriber with no replay. Bounded to
    /// [`MAX_QUEUE_BYTES`]; a subscriber that falls behind is disconnected
    /// rather than waited on.
    ///
    /// Use [`attach`](Self::attach) for anything that needs history — this
    /// has no defined starting offset.
    pub fn subscribe(&self) -> Subscription {
        let mut fanout = self.fanout.lock();
        fanout.register(&self.fanout)
    }

    /// Begins an attach: opens a read handle and establishes the replay
    /// boundary **without** registering a subscriber.
    ///
    /// Registration happens in the catch-up round that finds the remaining
    /// gap small enough, under the same lock that reads the boundary. That is
    /// what closes the attach race, and it is late enough that a subscriber's
    /// queue is never asked to hold history and live output at once.
    pub fn attach(&self, from: u64) -> Result<Replay, AttachError> {
        let fanout = self.fanout.lock();

        let next_offset = fanout.log.next_offset();
        if from > next_offset {
            return Err(AttachError::OffsetAhead {
                requested: from,
                next_offset,
            });
        }
        let log_capped_at = fanout.log.log_capped_at();
        let readable_end = fanout.log.readable_end();
        let reader = fanout.log.reader().map_err(AttachError::Open)?;
        drop(fanout);

        // Bytes between a cap and `next_offset` were streamed live and are
        // gone. A client asking for them is told where the stream resumes,
        // rather than served whatever sits at that file position.
        let replay_from = if from >= next_offset.min(readable_end) {
            next_offset
        } else {
            from
        };

        Ok(Replay {
            replay_from,
            next_offset,
            log_capped_at,
            fanout: Arc::clone(&self.fanout),
            reader,
            cursor: replay_from,
            previous_gap: u64::MAX,
            stalled_rounds: 0,
            total_rounds: 0,
        })
    }

    /// The authoritative output offset.
    pub fn next_offset(&self) -> u64 {
        self.fanout.lock().log.next_offset()
    }

    /// Offset at which persistence stopped, or `None`.
    pub fn log_capped_at(&self) -> Option<u64> {
        self.fanout.lock().log.log_capped_at()
    }

    /// This session's `output.vt` path.
    pub fn log_path(&self) -> PathBuf {
        self.fanout.lock().log.path().to_path_buf()
    }

    /// The child's OS pid.
    pub fn pid(&self) -> Option<u32> {
        self.pid
    }

    /// The current lifecycle state.
    pub fn state(&self) -> State {
        self.runtime.lock().state
    }

    /// The effective PTY size.
    pub fn size(&self) -> (u16, u16) {
        let runtime = self.runtime.lock();
        (runtime.cols, runtime.rows)
    }

    /// Writes input to the PTY. Rejected once the session is closing.
    pub fn write(&self, bytes: &[u8]) -> Result<()> {
        self.pty.write(bytes)
    }

    /// Applies a new PTY size, coalesced to at most one per
    /// [`RESIZE_COALESCE_WINDOW`], keeping the latest.
    ///
    /// There is exactly one effective size per session; arbitrating between
    /// clients is the lease's job, not a negotiation between them.
    ///
    /// A resize inside the window is remembered and applied when it closes,
    /// so the last size asked for always wins — dropping it would leave the
    /// terminal at the wrong width after a drag ends.
    pub fn resize(self: &Arc<Self>, cols: u16, rows: u16) -> Result<()> {
        let schedule_in = {
            let mut resize = self.resize.lock();
            let elapsed = resize.last_applied.elapsed();
            if elapsed >= RESIZE_COALESCE_WINDOW {
                resize.last_applied = Instant::now();
                resize.pending = None;
                drop(resize);
                return self.apply_resize(cols, rows);
            }
            resize.pending = Some((cols, rows));
            if resize.flushing {
                None // a flusher is already scheduled for this burst
            } else {
                resize.flushing = true;
                Some(RESIZE_COALESCE_WINDOW - elapsed)
            }
        };

        if let Some(delay) = schedule_in {
            // Without a runtime there is nothing to schedule on — apply now
            // rather than silently dropping the resize.
            match tokio::runtime::Handle::try_current() {
                Ok(handle) => {
                    let session = Arc::clone(self);
                    handle.spawn(async move {
                        tokio::time::sleep(delay).await;
                        session.flush_pending_resize();
                    });
                }
                Err(_) => {
                    self.resize.lock().flushing = false;
                    self.flush_pending_resize();
                }
            }
        }
        Ok(())
    }

    /// Applies whatever the last resize asked for, once the window closes.
    fn flush_pending_resize(&self) {
        let pending = {
            let mut resize = self.resize.lock();
            resize.flushing = false;
            resize.last_applied = Instant::now();
            resize.pending.take()
        };
        if let Some((cols, rows)) = pending {
            if let Err(e) = self.apply_resize(cols, rows) {
                tracing::debug!(session = %self.id, error = %e, "deferred resize failed");
            }
        }
    }

    /// The size actually reaching the PTY.
    fn apply_resize(&self, cols: u16, rows: u16) -> Result<()> {
        self.pty.resize(cols, rows)?;
        {
            let mut runtime = self.runtime.lock();
            runtime.cols = cols;
            runtime.rows = rows;
        }
        // Persisted off this thread, so a restart reports the size the
        // session actually has rather than the one it was created at.
        if let Some(persist) = &self.persist {
            persist.note_size(&self.id, cols, rows);
        }
        Ok(())
    }

    /// Runs the termination policy: graceful signal, bounded wait, hard kill.
    /// Blocks for up to ~7s. Idempotent.
    ///
    /// Does not remove the session from its manager — a terminated session
    /// stays listed as `exited` until an explicit purge.
    pub fn terminate(&self) -> Result<()> {
        {
            let mut runtime = self.runtime.lock();
            if runtime.state == State::Running {
                runtime.state = State::Closing;
            }
        }
        self.pty.terminate()
    }

    /// fsyncs the output log. Never called under the fan-out lock.
    pub fn sync_log(&self) {
        if let Err(e) = self.sync.sync() {
            tracing::warn!(session = %self.id, error = %e, "fsync of the output log failed");
        }
    }

    /// Resolves once the session has exited. A late caller still sees it.
    pub async fn wait_exited(&self) {
        let mut exited = self.exited.clone();
        while !*exited.borrow() {
            if exited.changed().await.is_err() {
                return;
            }
        }
    }

    /// Resolves once the reader thread has seen EOF on the master. Not an
    /// exit signal — use it only to bound a wait for trailing output.
    pub async fn wait_eof(&self) {
        let mut eof = self.eof.clone();
        while !*eof.borrow() {
            if eof.changed().await.is_err() {
                return;
            }
        }
    }

    /// Replaces this session's labels wholesale.
    pub fn set_labels(&self, labels: std::collections::BTreeMap<String, String>) {
        *self.labels.lock() = labels;
    }

    /// A point-in-time row for the store and the wire.
    pub fn snapshot(&self) -> crate::model::Session {
        let runtime = self.runtime.lock();
        let (output_bytes, log_capped_at) = {
            let fanout = self.fanout.lock();
            (fanout.log.next_offset(), fanout.log.log_capped_at())
        };
        crate::model::Session {
            id: self.id.clone(),
            labels: self.labels.lock().clone(),
            spec: self.spec.clone(),
            state: runtime.state,
            pid: self.pid,
            exit_code: runtime.exit_code,
            lost_reason: runtime.lost_reason.map(str::to_string),
            cols: runtime.cols,
            rows: runtime.rows,
            output_bytes,
            log_capped_at,
            created_at: self.created_at,
            exited_at: runtime.exited_at,
        }
    }
}
