//! `anchord` — the daemon.
//!
//! Startup sequence, per docs/02-model.md:
//!
//! ```text
//! resolve data dir -> open SQLite (WAL), migrate -> mark live sessions LOST
//! -> reconcile output_bytes -> bind unix socket -> serve
//! ```

use anyhow::Result;
use tracing_subscriber::EnvFilter;

use anchor::paths::Paths;
use anchor::store::Store;

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let paths = Paths::resolve()?;
    paths.ensure_dirs()?;
    tracing::info!(
        data_dir = %paths.data_dir().display(),
        socket = %paths.socket_path().display(),
        "anchord starting"
    );

    let store = Store::open(&paths)?;
    tracing::info!(
        version = anchor::db::schema_version(store.conn())?,
        "schema ready"
    );

    // A restart means no PTY survived: anything still claiming to be live is
    // lost. The log files are intact and remain readable.
    let lost = store.mark_live_sessions_lost()?;
    if lost > 0 {
        tracing::warn!(sessions = lost, "marked LOST after daemon restart");
    }

    let corrected = store.reconcile_output_bytes(&paths)?;
    if corrected > 0 {
        tracing::warn!(
            sessions = corrected,
            "output_bytes reconciled against log files"
        );
    }

    let listener = anchor::server::bind(paths.socket_path())?;
    tracing::info!(socket = %paths.socket_path().display(), "listening");
    anchor::server::serve(listener).await
}

/// Tracing from the first commit. `ANCHOR_LOG` overrides `RUST_LOG`.
fn init_tracing() {
    let filter = std::env::var("ANCHOR_LOG")
        .ok()
        .filter(|v| !v.is_empty())
        .map(EnvFilter::new)
        .unwrap_or_else(|| {
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"))
        });

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();
}
