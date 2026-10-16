//! The daemon's startup sequence, shared by the `anchord` binary and
//! `anchor daemon` so there is exactly one copy of it.
//!
//! Order matters, per docs/02-model.md:
//!
//! ```text
//! resolve data dir -> open SQLite (WAL), migrate -> bind unix socket
//! -> mark live sessions LOST -> reconcile output_bytes -> serve
//! ```
//!
//! The bind comes before the recovery writes, not after. See the comment on
//! it: recovery is destructive, and only the process that holds the socket
//! has the right to perform it.

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

    let store = Store::open(paths)?;
    tracing::info!(
        version = crate::db::schema_version(store.conn())?,
        "schema ready"
    );

    // Bind before touching a single row. Startup recovery rewrites every
    // live session to LOST, which is right after a crash and catastrophic
    // otherwise: an accidental second launch — a stray `anchord` in a
    // shell, a supervisor racing itself — would tell the running daemon's
    // clients their sessions are gone while the PTYs are still running.
    //
    // Claiming the socket first makes that impossible: whoever holds it is
    // the only process that may decide what a live row means.
    let listener = crate::server::bind(paths.socket_path())?;

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
        signal = stop_signal() => {
            tracing::info!(signal, "stopping");
            Ok(())
        }
    }
}

/// Resolves on the first SIGTERM or SIGINT, naming the one that arrived.
///
/// SIGINT too: a daemon run in the foreground is stopped with Ctrl-C, and a
/// developer deserves the same final fsync launchd gets.
#[cfg(unix)]
async fn stop_signal() -> &'static str {
    use tokio::signal::unix::{signal, SignalKind};

    // A failure to install a handler is not a reason to refuse to serve: the
    // daemon simply keeps the default disposition for that signal, which is
    // exactly where it was before this existed.
    let mut term = match signal(SignalKind::terminate()) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(error = %e, "no SIGTERM handler; shutdown will not be clean");
            std::future::pending().await
        }
    };
    let mut int = match signal(SignalKind::interrupt()) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(error = %e, "no SIGINT handler");
            // SIGTERM still works, and it is the one the service manager
            // sends, so this is a degraded shutdown rather than none.
            term.recv().await;
            return "SIGTERM";
        }
    };

    tokio::select! {
        _ = term.recv() => "SIGTERM",
        _ = int.recv() => "SIGINT",
    }
}

#[cfg(not(unix))]
async fn stop_signal() -> &'static str {
    match tokio::signal::ctrl_c().await {
        Ok(()) => "CTRL-C",
        Err(e) => {
            tracing::warn!(error = %e, "no ctrl-c handler; shutdown will not be clean");
            std::future::pending().await
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
