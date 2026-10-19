//! Append-only output log -- `<data_dir>/sessions/<id>/output.vt`.
//!
//! See docs/02-model.md#storage for the durability rules. Raw PTY bytes,
//! appended, never rewritten: no framing, no headers, no escaping. The
//! file's length *is* the replay index.
//!
//! The invariant every other module may rely on:
//!
//! ```text
//! file_length == min(next_offset, log_capped_at)   # cap = infinity when null
//! ```
//!
//! This module owns `next_offset`. That is deliberate: docs/03-pty.md's
//! reader loop requires **persist, then advance the offset, then fan out**, so
//! the counter has to live behind the same call that does the write, or the
//! ordering is an unenforced comment. The session layer holds an `OutputLog`
//! inside its fan-out mutex and reads the start offset back out of `append`.
//!
//! **Not in scope here:** bounded attach (`tail` / `max_replay_bytes`) lives
//! in `session/replay.rs` -- this module serves whatever byte range it is
//! asked for, clamped only to what actually exists. Keeping the bound one
//! layer up is what lets a VT state snapshot replace a byte range later
//! without a protocol change (docs/04-protocol.md). [`LogEvent`]s are
//! returned by `append`, not recorded anywhere; the caller logs them.
//!
//! **Nothing on the append path ever calls `fsync`.** The periodic sync
//! docs/02-model.md asks for runs on [`LogSyncer`]'s own thread against
//! a [`SyncHandle`] -- a second `Arc` on the same open file -- so it holds no
//! lock the PTY reader thread needs, and a slow disk stalls the syncer rather
//! than the terminal.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::sync::{Arc, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use tracing::warn;

/// Filename every session's log is opened under, inside its own session
/// directory (docs/02-model.md#storage).
pub const LOG_FILE_NAME: &str = "output.vt";

/// Defaults from docs/02-model.md#the-size-cap. `config.toml` does not
/// exist yet; when it does, it overrides these rather than replacing them.
pub const DEFAULT_LOG_WARN_BYTES: u64 = 256 * 1024 * 1024;
/// See [`DEFAULT_LOG_WARN_BYTES`].
pub const DEFAULT_LOG_MAX_BYTES: u64 = 1024 * 1024 * 1024;
/// Never `fsync` per chunk -- that couples PTY drain rate to disk latency
/// (docs/02-model.md#storage). Sync on this interval instead.
pub const DEFAULT_SYNC_INTERVAL: Duration = Duration::from_secs(2);

/// Size/sync thresholds for one session's log (docs/02-model.md#the-size-cap).
#[derive(Debug, Clone, Copy)]
pub struct LogLimits {
    /// Emit [`LogEvent::Warned`] once the log crosses this size.
    pub warn_bytes: u64,
    /// Stop growing the file once the log reaches this size
    /// ([`LogEvent::Capped`]); the offset stream keeps advancing regardless.
    pub max_bytes: u64,
    /// How often the background syncer flushes this log's file.
    pub sync_interval: Duration,
}

impl Default for LogLimits {
    fn default() -> Self {
        Self {
            warn_bytes: DEFAULT_LOG_WARN_BYTES,
            max_bytes: DEFAULT_LOG_MAX_BYTES,
            sync_interval: DEFAULT_SYNC_INTERVAL,
        }
    }
}

/// Something worth an operator-visible log line. Returned rather than
/// written, because `append` runs on the PTY reader thread and nothing on
/// that thread may touch SQLite.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogEvent {
    /// Crossed `log_warn_bytes`. Emitted once per log.
    Warned {
        /// The log's total size at the moment it crossed the threshold.
        output_bytes: u64,
    },
    /// Hit `log_max_bytes`. The file stops growing here; `next_offset` keeps
    /// advancing and live streaming continues.
    Capped {
        /// The offset the file was capped at.
        at: u64,
    },
    /// A write failed. Persistence stops and the log is capped where it
    /// stopped; the session keeps running. `log_capped_at` is the durable
    /// signal -- no `lost_reason` records this.
    IoError {
        /// The offset the write/sync was attempted at.
        at: u64,
        /// The `io::Error`'s display text.
        error: String,
    },
}

/// The result of one append. `start` is the offset of the chunk's first byte
/// -- the value the subscriber fan-out tags the chunk with.
#[derive(Debug)]
pub struct Appended {
    /// Offset of the chunk's first byte.
    pub start: u64,
    /// Anything worth recording that this append triggered.
    pub events: Vec<LogEvent>,
}

/// The append side of one session's log. Single-writer by construction:
/// The session layer keeps it inside the mutex the reader loop takes.
#[derive(Debug)]
pub struct OutputLog {
    path: PathBuf,
    /// Shared with every [`SyncHandle`] handed out for this log, so the
    /// background syncer flushes the same open file without taking the
    /// caller's lock. Writes go through `&File`, which is `Write`.
    file: Arc<File>,
    limits: LogLimits,
    /// Authoritative output offset. Never rewinds -- not on a cap, not across
    /// a restart (docs/02-model.md#startup-sequence).
    next_offset: u64,
    /// Bytes actually on disk. Equal to `min(next_offset, log_capped_at)`
    /// except after an I/O error, where it is the honest, smaller truth.
    file_len: u64,
    log_capped_at: Option<u64>,
    /// Sticky: once a write fails we stop persisting rather than interleave
    /// a hole into a byte stream whose offsets are an index.
    io_failed: bool,
    warned: bool,
}

impl OutputLog {
    /// Opens (creating if needed) `<dir>/output.vt` in append mode.
    ///
    /// The stream resumes at the file's length, capped if the file already
    /// holds `max_bytes`. This is only ever a fresh open: a restart marks
    /// every live session `lost` and never reopens its log for writing, so
    /// the recovery rule for a stream that ran ahead of a capped file lives
    /// in one place, `Store::reconcile_output_bytes`.
    pub fn open(dir: &Path, limits: LogLimits) -> Result<Self> {
        create_dir_owner_only(dir)?;
        let path = dir.join(LOG_FILE_NAME);
        let mut options = OpenOptions::new();
        options.create(true).append(true);
        // The log is a verbatim terminal transcript, credentials included.
        // Owner-only on the file itself, not just the directory around it.
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options
            .open(&path)
            .with_context(|| format!("opening {}", path.display()))?;
        let file_len = file
            .metadata()
            .with_context(|| format!("stat {}", path.display()))?
            .len();

        Ok(Self {
            path,
            file: Arc::new(file),
            limits,
            next_offset: file_len,
            file_len,
            log_capped_at: (file_len >= limits.max_bytes).then_some(file_len),
            io_failed: false,
            warned: file_len >= limits.warn_bytes,
        })
    }

    /// Persists `bytes`, then advances the offset. Runs on the PTY reader
    /// thread; never blocks on anything but the write itself, and never
    /// fails upward -- a log that cannot be written must not kill a running
    /// child (docs/02-model.md).
    pub fn append(&mut self, bytes: &[u8]) -> Appended {
        let start = self.next_offset;
        let mut events = Vec::new();

        if !self.io_failed && self.log_capped_at.is_none() {
            // Fill the budget exactly, then stop: `log_capped_at` is always
            // `max_bytes` for a log that got there by growing.
            let room = self.limits.max_bytes.saturating_sub(self.file_len);
            // `usize::MAX` as the fallback, not 0: room clamped that high
            // means "no meaningful cap on a machine with a smaller usize",
            // so the `.min(bytes.len())` right after is the only bound that
            // actually applies.
            let room = usize::try_from(room).unwrap_or(usize::MAX);
            let fits = room.min(bytes.len());

            if fits > 0 {
                match (&*self.file).write_all(&bytes[..fits]) {
                    Ok(()) => self.file_len += fits as u64,
                    Err(e) => self.fail(&mut events, &e),
                }
            }

            if !self.io_failed {
                if !self.warned && self.file_len >= self.limits.warn_bytes {
                    self.warned = true;
                    events.push(LogEvent::Warned {
                        output_bytes: self.file_len,
                    });
                }
                if fits < bytes.len() {
                    self.log_capped_at = Some(self.file_len);
                    events.push(LogEvent::Capped { at: self.file_len });
                }
            }
        }

        // Persist first, advance second -- a subscriber must never see an
        // offset for bytes that are not yet readable
        // (docs/03-pty.md#reader-loop-ordering--not-negotiable).
        self.next_offset = start + bytes.len() as u64;

        Appended { start, events }
    }

    /// The authoritative output offset. Bytes below it have been handed out;
    /// bytes at or above it do not exist yet.
    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }

    /// Offset at which persistence stopped, or `None` while the whole stream
    /// is still on disk.
    pub fn log_capped_at(&self) -> Option<u64> {
        self.log_capped_at
    }

    /// One past the last byte a reader can actually get. Every range read
    /// clamps to this; it is the file length, so it is true under a cap and
    /// under an I/O error alike.
    pub fn readable_end(&self) -> u64 {
        self.file_len
    }

    /// This log's `output.vt` path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// A fresh read handle. Independent of the append handle -- replay seeks
    /// and reads without touching the writer's file position.
    pub fn reader(&self) -> io::Result<LogReader> {
        LogReader::open(&self.path)
    }

    /// A handle for flushing this log to disk from somewhere that is not the
    /// reader thread -- see [`LogSyncer`]. Cheap; hand one to whoever needs
    /// to sync on close.
    pub fn sync_handle(&self) -> SyncHandle {
        SyncHandle {
            file: Arc::clone(&self.file),
            path: self.path.clone(),
        }
    }

    /// Persistence stopped and it is not coming back for this log. Setting
    /// `log_capped_at` here is what keeps the module's invariant true and
    /// keeps the hole *visible*: without it a client would be handed a
    /// replay range that silently stops short of `next_offset` and live
    /// chunks resuming past a gap it was never told about
    /// (docs/02-model.md#the-size-cap).
    fn fail(&mut self, events: &mut Vec<LogEvent>, error: &io::Error) {
        if self.io_failed {
            return;
        }
        self.io_failed = true;
        // `write_all` loops over `write`, so a full disk can land k bytes and
        // then fail: the file is longer than `file_len` says. Cap at what is
        // really there, or the cap understates the file and `file_length ==
        // min(next_offset, log_capped_at)` is false on exactly the failure
        // this module exists to handle.
        self.file_len = self.file.metadata().map_or(self.file_len, |m| m.len());
        self.log_capped_at = Some(self.file_len);
        events.push(LogEvent::IoError {
            at: self.file_len,
            error: error.to_string(),
        });
    }
}

/// A second reference to a log's open file, used only to `fsync` it. Exists
/// so the flush never happens on the PTY reader thread or under the fan-out
/// mutex (docs/02-model.md#storage).
#[derive(Debug, Clone)]
pub struct SyncHandle {
    file: Arc<File>,
    path: PathBuf,
}

impl SyncHandle {
    /// `fsync`s the underlying file.
    pub fn sync(&self) -> io::Result<()> {
        self.file.sync_data()
    }

    /// This log's `output.vt` path.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// One thread for the whole daemon that flushes every registered log on
/// `sync_interval` -- the "`fsync` on a 2-second timer while running" half of
/// docs/02-model.md#storage.
///
/// One thread, not one per session: the work is a handful of `fsync`s and the
/// per-session thread budget is already spent on the four `pty.rs` needs.
/// Registrations are `Weak`, so a log whose session is gone is pruned on the
/// next tick rather than kept alive by the syncer.
#[derive(Debug)]
pub struct LogSyncer {
    tx: Option<Sender<(Weak<File>, PathBuf)>>,
    thread: Option<JoinHandle<()>>,
}

impl LogSyncer {
    /// Starts the syncer thread, flushing every registered log every
    /// `interval`.
    pub fn new(interval: Duration) -> Self {
        let (tx, rx) = mpsc::channel::<(Weak<File>, PathBuf)>();
        let thread = std::thread::Builder::new()
            .name("log-syncer".into())
            .spawn(move || {
                let mut registered: Vec<(Weak<File>, PathBuf)> = Vec::new();
                let mut last = Instant::now();
                loop {
                    // A registration must not reset the timer, or a daemon
                    // creating sessions steadily would never sync at all.
                    let wait = interval.saturating_sub(last.elapsed());
                    match rx.recv_timeout(wait) {
                        Ok(entry) => registered.push(entry),
                        Err(RecvTimeoutError::Timeout) => {}
                        Err(RecvTimeoutError::Disconnected) => {
                            sync_all(&mut registered);
                            return;
                        }
                    }
                    if last.elapsed() >= interval {
                        sync_all(&mut registered);
                        last = Instant::now();
                    }
                }
            })
            .expect("spawning the log syncer thread");
        Self {
            tx: Some(tx),
            thread: Some(thread),
        }
    }

    /// Starts flushing `handle`'s log on the timer. Dropping every
    /// `SyncHandle` and `OutputLog` for that file unregisters it.
    pub fn register(&self, handle: &SyncHandle) {
        if let Some(tx) = &self.tx {
            let _ = tx.send((Arc::downgrade(&handle.file), handle.path.clone()));
        }
    }
}

impl Drop for LogSyncer {
    /// Dropping the sender is the stop signal; the thread does one last pass
    /// so a daemon shutting down does not lose the tail of every live log.
    fn drop(&mut self) {
        self.tx = None;
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn sync_all(registered: &mut Vec<(Weak<File>, PathBuf)>) {
    registered.retain(|(weak, path)| match weak.upgrade() {
        Some(file) => {
            if let Err(e) = file.sync_data() {
                warn!(path = %path.display(), error = %e, "periodic log fsync failed");
            }
            true
        }
        None => false, // the session is gone; stop tracking it.
    });
}

/// The read side. Cheap to create, one per replay.
#[derive(Debug)]
pub struct LogReader {
    file: File,
}

impl LogReader {
    /// Opens `path` for reading. Independent of any writer -- see the
    /// struct doc.
    pub fn open(path: &Path) -> io::Result<Self> {
        Ok(Self {
            file: File::open(path)?,
        })
    }

    /// Reads `[from, to)`, clamped to what the file actually holds. Callers
    /// clamp `to` to [`OutputLog::readable_end`] under the fan-out lock, so a
    /// short read here means someone read past what they accounted for --
    /// the clamp is a backstop, and it is also what keeps a bogus `to` from
    /// reserving a buffer the size of the range rather than the file.
    pub fn read_range(&mut self, from: u64, to: u64) -> io::Result<Vec<u8>> {
        let end = to.min(self.file.metadata()?.len());
        if end <= from {
            return Ok(Vec::new());
        }
        let len = end - from;
        self.file.seek(SeekFrom::Start(from))?;
        // `len` is bounded by the file size by here, so on a 32-bit target
        // this cannot truncate the way the requested range could. The
        // fallback is 0 rather than `usize::MAX` because `with_capacity`
        // allocates eagerly: an under-sized capacity costs a reallocation,
        // an oversized one is an instant abort.
        let mut buf = Vec::with_capacity(usize::try_from(len).unwrap_or(0));
        Read::by_ref(&mut self.file)
            .take(len)
            .read_to_end(&mut buf)?;
        Ok(buf)
    }
}

/// `<data_dir>` is already `0700`; session directories under it match rather
/// than relying on the parent (docs/01-scope.md).
fn create_dir_owner_only(dir: &Path) -> Result<()> {
    fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(dir, fs::Permissions::from_mode(0o700))
            .with_context(|| format!("setting owner-only permissions on {}", dir.display()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "anchord-log-unit-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    /// A failed append stops persistence *and* publishes where it stopped.
    /// Without the second half a client is handed a replay range that ends
    /// short of `next_offset` and live chunks that resume past a hole it was
    /// never told about.
    ///
    /// Lives here rather than in `tests/` because inducing a real
    /// write failure means handing `OutputLog` a read-only handle, which
    /// needs the private field.
    #[test]
    fn a_failed_append_caps_the_log_and_keeps_offsets_advancing() {
        let dir = scratch("io-error");
        let mut log = OutputLog::open(&dir, LogLimits::default()).expect("open");
        log.append(b"the bytes that made it");
        assert_eq!(log.readable_end(), 22);

        // Swap in a read-only handle to the same file: writes now fail the
        // way a full disk or a revoked mount would, without needing either.
        log.file = Arc::new(File::open(log.path()).expect("reopen read-only"));

        let appended = log.append(b"and the ones that did not");
        assert_eq!(appended.start, 22, "the offset is still handed out");
        assert_eq!(
            log.next_offset(),
            47,
            "offsets keep advancing -- the session is still running"
        );
        assert_eq!(log.readable_end(), 22, "nothing more reached the disk");
        assert_eq!(
            log.log_capped_at(),
            Some(22),
            "the log must say where persistence stopped, not report an open-ended range"
        );
        assert!(
            matches!(
                appended.events.as_slice(),
                [LogEvent::IoError { at: 22, .. }]
            ),
            "expected one IoError at the truncation point, got {:?}",
            appended.events
        );

        // The invariant still holds, which is the whole point of setting the
        // cap: file_length == min(next_offset, log_capped_at).
        let on_disk = fs::metadata(log.path()).unwrap().len();
        assert_eq!(on_disk, log.next_offset().min(log.log_capped_at().unwrap()));

        // And it is sticky: a later append neither retries nor re-reports.
        let again = log.append(b"still nothing");
        assert!(
            again.events.is_empty(),
            "IoError is reported once, not per chunk"
        );
        assert_eq!(log.readable_end(), 22);
        assert_eq!(
            log.log_capped_at(),
            Some(22),
            "the cap must not move to the new offset"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    /// `LogSyncer` flushes registered logs off the caller's thread and stops
    /// tracking a log whose `OutputLog` is gone.
    #[test]
    fn the_syncer_flushes_and_prunes() {
        let dir = scratch("syncer");
        let syncer = LogSyncer::new(Duration::from_millis(20));

        let mut log = OutputLog::open(&dir, LogLimits::default()).expect("open");
        syncer.register(&log.sync_handle());
        log.append(b"flush me");

        std::thread::sleep(Duration::from_millis(80));
        assert_eq!(fs::read(log.path()).unwrap(), b"flush me");

        // Dropping the log drops the last strong `Arc<File>`, so the syncer's
        // `Weak` stops upgrading and the entry is pruned. Nothing to assert
        // beyond "this does not panic or keep the file open forever"; the
        // syncer's final pass on drop is what would surface a wedged thread.
        drop(log);
        std::thread::sleep(Duration::from_millis(40));
        drop(syncer);

        let _ = fs::remove_dir_all(&dir);
    }
}
