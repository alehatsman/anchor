//! anchor — a local daemon that owns long-running PTY processes.
//!
//! A session is a PTY plus an append-only output log, addressed by byte
//! offset, outliving every client that attaches to it. Clients speak a
//! newline-delimited JSON protocol over a unix socket, switching to binary
//! framing once attached (docs/04-protocol.md).
//!
//! The daemon is meant to be started by the user's own service manager and
//! never by a client, which is what lets a client be restarted — or shipped
//! — without taking its sessions down (docs/07-lifecycle.md). That is the
//! property the whole thing exists for; it does **not** extend to anchord
//! crashing, which takes its PTYs with it.

// Unix-only until docs/09-windows.md §5 step 4 lands the named-pipe
// transport and the console input path. Gated out rather than stubbed, so a
// Windows build cannot boot a daemon that reports healthy and hosts nothing.
#[cfg(unix)]
pub mod client;
#[cfg(unix)]
pub mod daemon;
pub mod db;
pub mod log;
pub mod model;
pub mod paths;
pub mod proto;
pub mod pty;
#[cfg(unix)]
pub mod server;
pub mod service;
pub mod session;
pub mod store;
#[cfg(unix)]
pub mod stream;
#[cfg(unix)]
pub mod tty;

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
