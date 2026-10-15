//! anchor — a local daemon that owns long-running PTY processes.
//!
//! M0 is the skeleton: data directory resolution, SQLite schema with
//! forward-only migrations, the session store, and a unix socket that answers
//! `ping`. No PTY layer yet — that is M1.

pub mod db;
pub mod log;
pub mod model;
pub mod paths;
pub mod proto;
pub mod pty;
pub mod server;
pub mod session;
pub mod store;
pub mod stream;

/// Milliseconds since the unix epoch.
///
/// Every timestamp anchor stores (`created_at`, `exited_at`) uses this unit.
pub fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
