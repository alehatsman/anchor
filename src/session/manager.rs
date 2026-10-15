//! The session directory: create, get, list, purge, and the two listener
//! threads that turn the PTY's one-shot signals into session state.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
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

/// Why a create was refused. Each maps onto a protocol error code.
#[derive(Debug, thiserror::Error)]
pub enum CreateError {
    #[error("cwd is not a directory: {0}")]
    InvalidCwd(PathBuf),
    #[error("command not found: {0}")]
    ExecutableNotFound(String),
    #[error("too many sessions: {max} already running")]
    TooManySessions { max: usize },
    #[error("opening the output log: {0}")]
    Log(#[from] anyhow::Error),
    #[error("spawning the child: {0}")]
    Spawn(#[source] anyhow::Error),
}

/// Holds every live session, plus everything a new one needs to exist.
#[derive(Debug)]
pub struct SessionManager {
    root: PathBuf,
    limits: LogLimits,
    syncer: LogSyncer,
    sessions: Mutex<Vec<Arc<Session>>>,
    max_sessions: Option<usize>,
    /// Counts creates in flight so two concurrent ones cannot both pass the
    /// cap check before either inserts.
    reserved: AtomicUsize,
    store: Option<Arc<StdMutex<Store>>>,
    persister: Option<OutputPersister>,
}

/// Decrements the in-flight count on every exit path from `create`.
struct ReservationGuard<'a>(&'a AtomicUsize);

impl Drop for ReservationGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
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
            max_sessions: None,
            reserved: AtomicUsize::new(0),
            store: None,
            persister: None,
        }
    }

    /// Caps how many sessions may exist at once.
    pub fn with_max_sessions(mut self, max_sessions: usize) -> Self {
        self.max_sessions = Some(max_sessions);
        self
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

    fn reserve_slot(&self) -> Result<ReservationGuard<'_>, CreateError> {
        let reserved = self.reserved.fetch_add(1, Ordering::SeqCst) + 1;
        let guard = ReservationGuard(&self.reserved);
        if let Some(max) = self.max_sessions {
            if self.sessions.lock().len() + reserved > max {
                return Err(CreateError::TooManySessions { max });
            }
        }
        Ok(guard)
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
        let _reservation = self.reserve_slot()?;

        if !spec.cwd.is_dir() {
            return Err(CreateError::InvalidCwd(spec.cwd.clone()));
        }
        if !resolve_executable(&spec.command, &spec.cwd, spec.login_shell) {
            return Err(CreateError::ExecutableNotFound(spec.command.clone()));
        }

        let id = model::new_session_id();
        let created_at = crate::now_ms();

        let log = OutputLog::open(&self.root.join(&id), self.limits, None)?;
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
        let last_persist_ms = Arc::new(AtomicI64::new(0));
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
                let last = last_persist_ms.load(Ordering::Relaxed);
                if now - last >= OUTPUT_BYTES_PERSIST_INTERVAL_MS
                    && last_persist_ms
                        .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
                        .is_ok()
                {
                    persister.note_output_bytes(&id_for_closure, next_offset, capped_at);
                }
            }
        })
        .map_err(CreateError::Spawn)?;

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
            control: Mutex::new(ControlLease::default()),
            created_at,
            exited,
            exited_tx,
            eof,
            eof_tx,
            sync,
        });

        spawn_exit_listener(Arc::clone(&session), spawned.exit_rx, self.store.clone());
        spawn_eof_listener(Arc::clone(&session), spawned.eof_rx);

        self.sessions.lock().push(Arc::clone(&session));
        Ok(session)
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
) {
    std::thread::Builder::new()
        .name("session-exit".into())
        .spawn(move || {
            // A closed channel with no message means the PTY's control thread
            // died without publishing an exit — a bug there, not a reason to
            // panic here.
            let Ok(exit) = exit_rx.recv() else { return };

            let (exit_code, reason) = match exit.status {
                // -1 is the conventional "could not be represented" sentinel
                // for a status that is not a plain exit code, e.g. a signal
                // death.
                Some(status) => (Some(i32::try_from(status.exit_code()).unwrap_or(-1)), None),
                None => (
                    None,
                    Some(match exit.lost_reason {
                        Some(pty::LostReason::KillTimeout) => lost_reason::KILL_TIMEOUT,
                        // A wait() that errored leaves no exit code either.
                        Some(pty::LostReason::WaitError) | None => lost_reason::KILL_TIMEOUT,
                    }),
                ),
            };

            let exited_at = crate::now_ms();
            {
                let mut runtime = session.runtime.lock();
                runtime.state = State::Exited;
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
        .expect("spawning the session-exit thread");
}

/// Consumes `eof_rx` on its own thread. Not a state signal — it exists so a
/// caller can tell "the reader has genuinely caught up" from "the reader has
/// not been scheduled yet" when finalizing a stream.
fn spawn_eof_listener(session: Arc<Session>, eof_rx: std::sync::mpsc::Receiver<()>) {
    std::thread::Builder::new()
        .name("session-eof".into())
        .spawn(move || {
            if eof_rx.recv().is_ok() {
                let _ = session.eof_tx.send(true);
            }
        })
        .expect("spawning the session-eof thread");
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
    login_shell && resolves_in_login_shell(command, cwd)
}

/// Asks the user's login shell whether it can find `command`. The daemon's
/// own `PATH` is not the one the session will get.
fn resolves_in_login_shell(command: &str, cwd: &Path) -> bool {
    // `command -v --` keeps a name that looks like an option from being
    // parsed as one; the name itself is a separate argv entry either way.
    std::process::Command::new(pty::login_shell_path())
        .args([
            "-l",
            "-c",
            r#"command -v -- "$1" >/dev/null 2>&1"#,
            "sh",
            command,
        ])
        .current_dir(cwd)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn is_executable_file(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(path)
        .map(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
}
