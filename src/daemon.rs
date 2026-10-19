//! The daemon's startup sequence, shared by the `anchord` binary and
//! `anchor daemon` so there is exactly one copy of it.
//!
//! Order matters, per docs/02-model.md:
//!
//! ```text
//! resolve data dir -> arm SIGTERM/SIGINT -> bind unix socket
//! -> open SQLite (WAL), migrate -> mark live sessions LOST
//! -> reconcile output_bytes -> serve
//! ```
//!
//! Both of the first two orderings are load-bearing, and the comments at
//! each step say why: the bind precedes every write because only the process
//! holding the socket may decide what a live row means, and the handlers
//! precede the bind because a bound socket is connectable while the database
//! is still opening.

use std::sync::{Arc, Mutex};

use anyhow::Result;

use crate::paths::Paths;
use crate::server::State;
use crate::session::SessionManager;
use crate::store::Store;

/// Runs the daemon in the foreground until the listener fails.
pub async fn run(paths: &Paths) -> Result<()> {
    paths.ensure_dirs()?;
    tracing::info!(
        data_dir = %paths.data_dir().display(),
        socket = %paths.socket_path().display(),
        "anchord starting"
    );

    // Armed before the bind, not at the `select!` that awaits it: the socket
    // is connectable as soon as it is bound, and a signal that lands while
    // the database is still opening must be handled, not fatal.
    let stop = arm_stop_signal();

    // Bind before touching the database -- not just before touching a row.
    // `Store::open` migrates: DDL, a `meta` write, a `journal_mode` change. A
    // stray `anchord` from a newer build (an upgrade where the supervisor
    // races the old daemon) would otherwise apply its migrations to the
    // database the running daemon is serving, then die at the bind, leaving
    // the old daemon on a schema it does not know. The socket is the only
    // cross-process lock there is; `busy_timeout` merely serialises writers.
    //
    // Startup recovery is the sharper edge: it rewrites every live session
    // to LOST, which is right after a crash and catastrophic otherwise -- a
    // second launch would tell the running daemon's clients their sessions
    // are gone while the PTYs are still running. Whoever holds the socket is
    // the only process that may decide what a live row means.
    //
    // If `Store::open` then fails, the listener drops and leaves a stale
    // socket file; the next start already removes that (`server::bind`).
    let listener = crate::server::bind(paths.socket_path())?;

    let store = Store::open(paths)?;
    tracing::info!(
        version = crate::db::schema_version(store.conn())?,
        "schema ready"
    );

    // A restart means no PTY survived: anything still claiming to be live is
    // lost. The log files are intact and remain readable.
    let lost = store.mark_live_sessions_lost()?;
    if lost > 0 {
        tracing::warn!(sessions = lost, "marked LOST after daemon restart");
    }

    let corrected = store.reconcile_output_bytes(paths)?;
    if corrected > 0 {
        tracing::warn!(
            sessions = corrected,
            "output offsets reconciled against log files"
        );
    }

    let store = Arc::new(Mutex::new(store));
    let manager =
        Arc::new(SessionManager::new(paths.sessions_dir()).with_store(Arc::clone(&store)));

    tracing::info!(socket = %paths.socket_path().display(), "listening");
    let serving = crate::server::serve(listener, State::new(manager, store));

    // A service manager stops a service with SIGTERM, so it is an
    // instruction rather than an accident (docs/07-lifecycle.md#stopping).
    // Returning instead of dying of the signal is what lets the runtime
    // unwind, and unwinding is what drops the `LogSyncer` and gives every
    // live log its final fsync.
    tokio::select! {
        result = serving => result,
        signal = stop => {
            tracing::info!(signal, "stopping");
            Ok(())
        }
    }
}

/// Installs the SIGTERM/SIGINT handlers *now* and returns a future that
/// resolves on the first of them, naming the one that arrived.
///
/// Installing is synchronous, which is the point: a unix socket is connectable
/// the moment it is bound, so a client can signal the daemon while it is still
/// opening the database. Armed lazily at the `select!` below, that window
/// had the default disposition and the signal killed the process outright.
/// A signal that arrives after this returns is queued on the stream and is
/// seen as soon as the future is polled, however late.
///
/// SIGINT too: a daemon run in the foreground is stopped with Ctrl-C, and a
/// developer deserves the same final fsync launchd gets.
#[cfg(unix)]
fn arm_stop_signal() -> impl std::future::Future<Output = &'static str> {
    use tokio::signal::unix::{signal, SignalKind};

    // A failure to install a handler is not a reason to refuse to serve: the
    // daemon simply keeps the default disposition for that signal, which is
    // exactly where it was before this existed.
    let term = signal(SignalKind::terminate())
        .map_err(|e| tracing::warn!(error = %e, "no SIGTERM handler; shutdown will not be clean"))
        .ok();
    let int = signal(SignalKind::interrupt())
        .map_err(|e| tracing::warn!(error = %e, "no SIGINT handler"))
        .ok();

    async move {
        match (term, int) {
            (Some(mut term), Some(mut int)) => tokio::select! {
                _ = term.recv() => "SIGTERM",
                _ = int.recv() => "SIGINT",
            },
            // SIGTERM is the one the service manager sends, so losing SIGINT
            // is a degraded shutdown rather than none.
            (Some(mut term), None) => {
                term.recv().await;
                "SIGTERM"
            }
            (None, Some(mut int)) => {
                int.recv().await;
                "SIGINT"
            }
            (None, None) => std::future::pending().await,
        }
    }
}

/// The Windows counterpart: console control events rather than signals,
/// registered *now* for the same reason as the Unix arm -- the pipe is
/// connectable the moment it is bound.
///
/// Ctrl-C and Ctrl-Break for a daemon in the foreground; close, logoff and
/// shutdown for the console or session going away under it, which gives the
/// handler a few seconds before the system ends the process. That is the
/// only stop Windows announces: Task Scheduler's "End" and `taskkill /F` are
/// `TerminateProcess`, which nothing can catch, so those exits get no final
/// fsync -- the periodic one is what bounds the loss.
#[cfg(windows)]
fn arm_stop_signal() -> impl std::future::Future<Output = &'static str> {
    use tokio::signal::windows;

    // A failure to install one is not a reason to refuse to serve, as on
    // Unix; that event simply keeps its default.
    let mut c = windows::ctrl_c()
        .map_err(|e| tracing::warn!(error = %e, "no ctrl-c handler; shutdown will not be clean"))
        .ok();
    let mut brk = windows::ctrl_break().ok();
    let mut close = windows::ctrl_close().ok();
    let mut logoff = windows::ctrl_logoff().ok();
    let mut shutdown = windows::ctrl_shutdown().ok();

    async move {
        // A listener that was never installed yields `None`, which disables
        // its branch rather than ending the wait.
        tokio::select! {
            Some(()) = async { c.as_mut()?.recv().await } => "CTRL-C",
            Some(()) = async { brk.as_mut()?.recv().await } => "CTRL-BREAK",
            Some(()) = async { close.as_mut()?.recv().await } => "CTRL-CLOSE",
            Some(()) = async { logoff.as_mut()?.recv().await } => "CTRL-LOGOFF",
            Some(()) = async { shutdown.as_mut()?.recv().await } => "CTRL-SHUTDOWN",
            else => std::future::pending().await,
        }
    }
}

/// Tracing from the first commit. `ANCHOR_LOG` overrides `RUST_LOG`.
pub fn init_tracing() {
    use tracing_subscriber::EnvFilter;

    let filter = std::env::var("ANCHOR_LOG")
        .ok()
        .filter(|v| !v.is_empty())
        .map(EnvFilter::new)
        .unwrap_or_else(|| {
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"))
        });

    // A second init is not an error: `anchor daemon` and a test harness can
    // both reach this.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .try_init();
}
