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
    crate::server::serve(listener, State::new(manager, store)).await
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
