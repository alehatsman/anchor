//! The session directory: create, get, list, purge, and the two listener
//! threads that turn the PTY's one-shot signals into session state.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex};

use parking_lot::Mutex;
use tokio::sync::watch;
use tracing::warn;

use crate::log::{LogEvent, LogLimits, LogSyncer, OutputLog};
use crate::model::{self, lost_reason, SessionId, SpawnSpec, State};
use crate::pty::{self};
use crate::store::Store;

use super::fanout::Fanout;
use super::persist::OutputPersister;
use super::{ControlLease, Runtime, Session};

/// How often the cached output offset is written back while output flows.
/// The value is a cache — the log file is authoritative and startup
/// reconciles it — so this trades precision for not touching SQLite per
/// chunk.
const OUTPUT_BYTES_PERSIST_INTERVAL_MS: i64 = 1_000;

/// The one effective PTY size a session may have, per docs/02-model.md's
/// create sequence.
///
/// Re-exported from `pty.rs`, which owns it and clamps to the same range as a
/// backstop. This layer rejects what that layer would clamp; both reading the
/// one constant is what keeps those two answers consistent (#18).
pub use crate::pty::SIZE_RANGE;

/// Why a create was refused. Each maps onto a protocol error code.
#[derive(Debug, thiserror::Error)]
pub enum CreateError {
    #[error("cwd is not a directory: {0}")]
    InvalidCwd(PathBuf),
    #[error("command not found: {0}")]
    ExecutableNotFound(String),
    #[error("invalid label: {0}")]
    InvalidLabel(#[from] model::labels::InvalidLabel),
    #[error("cols and rows must be between 1 and 1000, got {cols}x{rows}")]
    InvalidSize { cols: u16, rows: u16 },
    #[error("session storage: {0}")]
    Log(#[from] anyhow::Error),
    #[error("spawning the child: {0}")]
    Spawn(#[source] anyhow::Error),
    #[error("no such session")]
    NotFound,
    #[error("session is still running; terminate it first")]
    StillRunning,
}

/// Holds every live session, plus everything a new one needs to exist.
#[derive(Debug)]
pub struct SessionManager {
    root: PathBuf,
    limits: LogLimits,
    syncer: LogSyncer,
    sessions: Mutex<Vec<Arc<Session>>>,
    store: Option<Arc<StdMutex<Store>>>,
    persister: Option<OutputPersister>,
}

impl SessionManager {
    /// A manager rooted at `root`, with default log limits and no store.
    pub fn new(root: PathBuf) -> Self {
        Self::with_limits(root, LogLimits::default())
    }

    /// As [`new`](Self::new), with explicit log limits.
    pub fn with_limits(root: PathBuf, limits: LogLimits) -> Self {
        Self {
            root,
            limits,
            syncer: LogSyncer::new(limits.sync_interval),
            sessions: Mutex::new(Vec::new()),
            store: None,
            persister: None,
        }
    }

    /// Persists session rows to `store`. Without one the manager is
    /// in-memory, which is what most fixtures want.
    pub fn with_store(mut self, store: Arc<StdMutex<Store>>) -> Self {
        self.persister = Some(OutputPersister::start(Arc::clone(&store)));
        self.store = Some(store);
        self
    }

    /// Where session directories live.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Spawns a session.
    ///
    /// Order matters, per docs/02-model.md: validate, open the log, insert
    /// the row, *then* spawn — the row and the log exist before the child can
    /// emit a byte.
    pub fn create(
        &self,
        spec: &SpawnSpec,
        labels: BTreeMap<String, String>,
    ) -> Result<Arc<Session>, CreateError> {
        if !spec.cwd.is_dir() {
            return Err(CreateError::InvalidCwd(spec.cwd.clone()));
        }
        if !resolve_executable(&spec.command, &spec.cwd, spec.login_shell) {
            return Err(CreateError::ExecutableNotFound(spec.command.clone()));
        }
        // Checked before the log directory or the row exist, so a rejected
        // spec leaves nothing behind. The PTY layer clamps the size as a
        // backstop, but a caller that asked for 0 columns made a mistake and
        // should be told, not silently given 1.
        if !SIZE_RANGE.contains(&spec.cols) || !SIZE_RANGE.contains(&spec.rows) {
            return Err(CreateError::InvalidSize {
                cols: spec.cols,
                rows: spec.rows,
            });
        }
        model::labels::validate(&labels)?;

        let id = model::new_session_id();
        let created_at = crate::now_ms();

        let log = OutputLog::open(&self.root.join(&id), self.limits)?;
        let sync = log.sync_handle();
        self.syncer.register(&sync);
        let fanout = Arc::new(Mutex::new(Fanout::new(log)));

        // The row exists before the child can produce a byte. Best effort: a
        // failed write here does not stop a working session, it only means a
        // restart will not know this one existed.
        if let Some(store) = &self.store {
            let row = model::Session {
                id: id.clone(),
                labels: labels.clone(),
                spec: spec.clone(),
                state: State::Running,
                // Not known yet — the spawn below has not run.
                pid: None,
                exit_code: None,
                lost_reason: None,
                cols: spec.cols,
                rows: spec.rows,
                output_bytes: 0,
                log_capped_at: None,
                created_at,
                exited_at: None,
            };
            let mut store = lock(store);
            if let Err(e) = store.insert(&row) {
                warn!(session = %id, error = %e, "persisting the new session row failed");
            }
        }

        // Built before the Session exists, because the reader thread's
        // closure has to own its own handles to what it updates.
        let publish_fanout = Arc::clone(&fanout);
        let persister = self.persister.clone();
        let mut last_persist_ms = 0i64;
        let id_for_closure = id.clone();

        let env: Vec<(String, String)> = spec
            .env
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let pty_spec = pty::SpawnSpec {
            program: &spec.command,
            args: &spec.args,
            cwd: &spec.cwd,
            env: &env,
            login_shell: spec.login_shell,
            cols: spec.cols,
            rows: spec.rows,
        };

        let spawned = pty::spawn(&pty_spec, move |bytes| {
            // Runs on the PTY reader thread. Persist, advance the offset and
            // fan out under one short lock; everything after it is
            // non-blocking.
            let (events, next_offset, capped_at) = {
                let mut fanout = publish_fanout.lock();
                let events = fanout.publish(bytes);
                (events, fanout.log.next_offset(), fanout.log.log_capped_at())
            };
            trace_log_events(&id_for_closure, &events);

            if let Some(persister) = &persister {
                // Throttled, and the queue drops rather than waits — the
                // reader thread never blocks on SQLite.
                let now = crate::now_ms();
                if now - last_persist_ms >= OUTPUT_BYTES_PERSIST_INTERVAL_MS {
                    last_persist_ms = now;
                    persister.note_output_bytes(&id_for_closure, next_offset, capped_at);
                }
            }
        })
        .map_err(|e| self.fail_row(&id, CreateError::Spawn(e)))?;

        let (exited_tx, exited) = watch::channel(false);
        let (eof_tx, eof) = watch::channel(false);

        let session = Arc::new(Session {
            id: id.clone(),
            spec: spec.clone(),
            labels: Mutex::new(labels),
            pty: spawned.session,
            pid: spawned.pid,
            fanout,
            runtime: Mutex::new(Runtime {
                state: State::Running,
                exit_code: None,
                lost_reason: None,
                exited_at: None,
                cols: spec.cols,
                rows: spec.rows,
            }),
            resize: Mutex::new(super::ResizeState {
                // The session has just been created at its spec size, so the
                // window starts open: the first resize applies immediately.
                last_applied: std::time::Instant::now() - std::time::Duration::from_secs(1),
                pending: None,
                flushing: false,
            }),
            persist: self.persister.clone(),
            control: Mutex::new(ControlLease::default()),
            // Capacity is small on purpose: lease changes are rare, and a
            // receiver that lagged past them resyncs from the next frame.
            lease_changes: tokio::sync::broadcast::channel(16).0,
            size_changes: tokio::sync::broadcast::channel(16).0,
            created_at,
            exited,
            exited_tx,
            eof,
            eof_tx,
            sync,
        });

        // A thread that cannot be spawned is a session that cannot report
        // its own exit, so the create fails rather than handing back a
        // half-wired session. Panicking here would take down the caller's
        // connection without an answer — which is exactly how this was
        // found, under thread pressure from a parallel test suite.
        //
        // By now the child is running and `pty::spawn` has stood its guard
        // down, and this session is not in `sessions`, so nobody could ever
        // `terminate` it: stop it here before failing, or it runs on as an
        // unreachable orphan. The eof listener goes first so that, whichever
        // spawn fails, no listener is left that would later `mark_exited`
        // over the `spawn_failed` row `fail_row` writes.
        let wiring_failed = |e: std::io::Error| {
            let _ = session.terminate();
            self.fail_row(&id, CreateError::Spawn(e.into()))
        };
        spawn_eof_listener(Arc::clone(&session), spawned.eof_rx).map_err(wiring_failed)?;
        spawn_exit_listener(Arc::clone(&session), spawned.exit_rx, self.store.clone())
            .map_err(wiring_failed)?;

        self.sessions.lock().push(Arc::clone(&session));
        Ok(session)
    }

    /// Sets and removes labels on a live session, then persists the result.
    ///
    /// The whole set is validated after the edit is applied, not before: the
    /// limits are properties of the resulting set, and a client that removes
    /// one label while adding another must not be judged on the intermediate.
    pub fn label(
        &self,
        id: &str,
        set: BTreeMap<String, String>,
        remove: &[String],
    ) -> Result<BTreeMap<String, String>, CreateError> {
        let session = self.get(id).ok_or(CreateError::NotFound)?;

        // One critical section across read, edit, write and persist (#12).
        //
        // This op is per-key on the wire -- `set` and `remove` name keys, not
        // a whole map -- but the only way to apply it is to rewrite the map.
        // Reading under one lock and writing under another made two clients
        // touching *disjoint* keys clobber each other: both read the old map,
        // both wrote their own edit of it, and the loser lost every key it
        // had, not just the one it was writing. Namespacing by key prefix,
        // which is how clients are told to share a daemon
        // (docs/02-model.md), did not help and could not.
        //
        // The store write stays inside the guard so the two cannot be
        // ordered differently: serialized in memory and racing to the store
        // leaves the store holding whichever map arrived last, and a restart
        // then resurrects the lost key's absence.
        //
        // Nesting labels -> store is one-way by rule, and the rule is the
        // other side's to keep: **nothing may hold the store lock while it
        // touches a `Session`**. This comment used to claim the cycle could
        // not exist because `snapshot` clones labels and releases before the
        // store is touched. That was true of `snapshot` and false of its
        // caller: `op_list` held the store guard across the whole `match`
        // arm that called it, which is store -> labels, and the daemon
        // deadlocked (#19). An audit of the callee cannot see the nesting a
        // caller creates, so the invariant now lives on `server::State`'s
        // accessors, where a read that only needs rows cannot keep the guard.
        let mut guard = session.labels.lock();

        let mut labels = guard.clone();
        for key in remove {
            labels.remove(key);
        }
        labels.extend(set);
        // Before any mutation: `?` here must leave the session's labels
        // exactly as they were, which is why the edit is built on a clone.
        model::labels::validate(&labels)?;

        *guard = labels.clone();
        if let Some(store) = &self.store {
            let mut store = lock(store);
            if let Err(e) = store.replace_labels(id, &labels) {
                warn!(session = %id, error = %e, "persisting labels failed");
            }
        }
        drop(guard);
        Ok(labels)
    }

    /// Live sessions whose labels match every pair in `filter`.
    ///
    /// The in-memory twin of [`Store::list_filtered`](crate::store::Store::list_filtered):
    /// same AND-across-keys rule, against sessions this manager still holds.
    pub fn list_filtered(&self, filter: &BTreeMap<String, String>) -> Vec<Arc<Session>> {
        self.sessions
            .lock()
            .iter()
            .filter(|s| {
                let labels = s.labels.lock();
                filter.iter().all(|(k, v)| labels.get(k) == Some(v))
            })
            .map(Arc::clone)
            .collect()
    }

    /// Marks a row whose child never started, and returns the error that
    /// caused it unchanged.
    ///
    /// Every failure between inserting the row and having a live session
    /// goes through here: the row is already visible to `get` and `list`,
    /// and leaving it `running` would advertise a process that does not
    /// exist and never did.
    fn fail_row(&self, id: &str, error: CreateError) -> CreateError {
        if let Some(store) = &self.store {
            if let Err(e) = lock(store).mark_spawn_failed(id, crate::now_ms()) {
                warn!(session = %id, error = %e, "recording a failed spawn failed");
            }
        }
        error
    }

    /// One session by id, live or exited, until it is purged.
    pub fn get(&self, id: &str) -> Option<Arc<Session>> {
        self.sessions
            .lock()
            .iter()
            .find(|s| s.id == id)
            .map(Arc::clone)
    }

    /// Every session the manager still holds.
    pub fn list(&self) -> Vec<Arc<Session>> {
        self.sessions.lock().clone()
    }

    /// Drops a session from the directory. A terminated session stays listed
    /// as `exited` until this is called.
    pub fn purge(&self, id: &str) -> Option<Arc<Session>> {
        let mut sessions = self.sessions.lock();
        let idx = sessions.iter().position(|s| s.id == id)?;
        Some(sessions.remove(idx))
    }

    /// Forgets a session entirely: its row, its labels and its output log.
    ///
    /// Refuses while the session is still running — a caller terminates
    /// first. Deleting the log of a live PTY would leave the reader thread
    /// appending to a file nobody can read, and the session still streaming
    /// to subscribers from a log that no longer exists.
    ///
    /// Order matters: the directory goes first, then the row. A crash in
    /// between leaves a row whose log is gone, which startup reconciliation
    /// already handles (it reads zero bytes and caps there). The reverse
    /// would leave an orphan directory nothing points at.
    pub fn forget(&self, id: &str) -> Result<bool, CreateError> {
        if let Some(session) = self.get(id) {
            if !session.state().is_terminal() {
                return Err(CreateError::StillRunning);
            }
            self.purge(id);
        }

        let dir = self.root.join(id);
        match std::fs::remove_dir_all(&dir) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                warn!(session = %id, error = %e, "removing the session directory failed");
            }
        }

        match &self.store {
            Some(store) => lock(store)
                .delete(id)
                .map_err(|e| CreateError::Log(e.context("deleting the session row"))),
            // No store: the session existed only in memory, and purging it
            // from the directory above is the whole of forgetting it.
            None => Ok(true),
        }
    }
}

fn lock<T>(m: &StdMutex<T>) -> std::sync::MutexGuard<'_, T> {
    match m.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn trace_log_events(id: &SessionId, events: &[LogEvent]) {
    for event in events {
        match event {
            LogEvent::Warned { output_bytes } => {
                warn!(session = %id, output_bytes, "output log is getting large")
            }
            LogEvent::Capped { at } => {
                warn!(session = %id, at, "output log capped; it stops growing here")
            }
            LogEvent::IoError { at, error } => {
                warn!(session = %id, at, error, "writing the output log failed")
            }
        }
    }
}

/// Consumes `exit_rx` on its own thread — block on a channel, never poll.
/// This is the only thing that moves a session to `exited`, and it fires off
/// the reaper's `wait()`, never off reader EOF.
fn spawn_exit_listener(
    session: Arc<Session>,
    exit_rx: std::sync::mpsc::Receiver<pty::PtyExit>,
    store: Option<Arc<StdMutex<Store>>>,
) -> std::io::Result<()> {
    std::thread::Builder::new()
        .name("session-exit".into())
        .spawn(move || {
            // A closed channel with no message means the PTY's control thread
            // died without publishing an exit — a bug there, not a reason to
            // panic here.
            let Ok(exit) = exit_rx.recv() else { return };

            let (exit_code, reason) = match exit.status {
                // The `-1` arm is effectively unreachable: it needs a u32
                // above i32::MAX. portable-pty reports a signalled child as
                // exit code 1 (`status.code().unwrap_or(1)`), so a session
                // killed by SIGHUP/SIGTERM/SIGKILL is recorded as `exit_code:
                // 1`, indistinguishable from `exit 1`.
                Some(status) => (Some(i32::try_from(status.exit_code()).unwrap_or(-1)), None),
                // No status means the child was never observed exiting: a kill
                // timeout, or a `wait()` that errored. Neither leaves an exit
                // code, and both are recorded as `kill_timeout`.
                None => (None, Some(lost_reason::KILL_TIMEOUT)),
            };

            let exited_at = crate::now_ms();
            {
                let mut runtime = session.runtime.lock();
                // A kill timeout means the child was signalled and never
                // observed exiting, so the daemon cannot say how it ended:
                // that is `lost`, not `exited`. See docs/02-model.md.
                runtime.state = if reason == Some(lost_reason::KILL_TIMEOUT) {
                    State::Lost
                } else {
                    State::Exited
                };
                runtime.exit_code = exit_code;
                runtime.lost_reason = reason;
                runtime.exited_at = Some(exited_at);
            }
            session.sync_log();

            if let Some(store) = &store {
                let snapshot = session.snapshot();
                let store = lock(store);
                if let Err(e) = store.mark_exited(&snapshot) {
                    warn!(session = %session.id, error = %e, "persisting the exit failed");
                }
            }

            // Never fails while this Sender is alive, and it is: `session`
            // owns it and this thread holds a strong reference.
            let _ = session.exited_tx.send(true);
        })
        .map(|_| ())
}

/// Consumes `eof_rx` on its own thread. Not a state signal — it exists so a
/// caller can tell "the reader has genuinely caught up" from "the reader has
/// not been scheduled yet" when finalizing a stream.
fn spawn_eof_listener(
    session: Arc<Session>,
    eof_rx: std::sync::mpsc::Receiver<()>,
) -> std::io::Result<()> {
    std::thread::Builder::new()
        .name("session-eof".into())
        .spawn(move || {
            if eof_rx.recv().is_err() {
                return;
            }
            let _ = session.eof_tx.send(true);

            // EOF on the master means no byte will ever follow, so the
            // subscribers can be released. Without this their senders live
            // as long as the session is listed, and `Subscription::recv`
            // never returns `None` — every consumer has to infer the end
            // from somewhere else, which is exactly the kind of implicit
            // contract that gets a trailing chunk dropped.
            session.close_subscribers();
        })
        .map(|_| ())
}

/// Whether `command` can actually be spawned, checked before a PTY is opened
/// so a typo is a clean error rather than a session that dies instantly.
///
/// A path (absolute or relative) is checked directly, relative to the
/// session's own cwd rather than the daemon's. A bare name is looked up on
/// `PATH` — and under a login shell, on the `PATH` that shell builds, which
/// is the whole reason `login_shell` exists.
fn resolve_executable(command: &str, cwd: &Path, login_shell: bool) -> bool {
    if command.contains('/') {
        return is_executable_file(&cwd.join(command));
    }
    if let Some(paths) = std::env::var_os("PATH") {
        for dir in std::env::split_paths(&paths) {
            if is_executable_file(&dir.join(command)) {
                return true;
            }
        }
    }
    login_shell && pty::resolves_in_login_shell(command, cwd)
}

fn is_executable_file(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(path)
        .map(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
}
