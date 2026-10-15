//! The session model. See docs/02-model.md.

use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// A ULID, lexicographically sortable by creation time.
pub type SessionId = String;

pub fn new_session_id() -> SessionId {
    ulid::Ulid::generate().to_string()
}

/// `running -> closing -> exited`, plus `lost`.
///
/// `exited` fires off the reaper's `wait()`, never off reader EOF — a
/// descendant can hold the PTY master open long after the child is gone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum State {
    Running,
    Closing,
    Exited,
    Lost,
}

impl State {
    pub fn as_str(self) -> &'static str {
        match self {
            State::Running => "running",
            State::Closing => "closing",
            State::Exited => "exited",
            State::Lost => "lost",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "running" => Some(State::Running),
            "closing" => Some(State::Closing),
            "exited" => Some(State::Exited),
            "lost" => Some(State::Lost),
            _ => None,
        }
    }

    /// True while the daemon is expected to hold a live PTY for this session.
    /// These are the rows a restart turns into `lost`.
    pub fn is_live(self) -> bool {
        matches!(self, State::Running | State::Closing)
    }
}

/// Why a session became `lost`. See docs/02-model.md.
pub mod lost_reason {
    pub const DAEMON_RESTART: &str = "daemon_restart";
    pub const SPAWN_FAILED: &str = "spawn_failed";
    pub const KILL_TIMEOUT: &str = "kill_timeout";
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpawnSpec {
    pub command: String,
    pub args: Vec<String>,
    pub cwd: PathBuf,
    /// Overrides on top of the daemon's own environment.
    pub env: BTreeMap<String, String>,
    pub cols: u16,
    pub rows: u16,
    pub login_shell: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Session {
    pub id: SessionId,
    /// Caller-owned key/value. anchor never interprets a label.
    pub labels: BTreeMap<String, String>,
    pub spec: SpawnSpec,
    pub state: State,
    pub pid: Option<u32>,
    pub exit_code: Option<i32>,
    pub lost_reason: Option<String>,
    /// The effective PTY size, which may differ from `spec` after a resize.
    pub cols: u16,
    pub rows: u16,
    /// Cached length of `output.vt`. The file is authoritative; this is
    /// reconciled against the real size at startup.
    pub output_bytes: u64,
    /// Milliseconds since the unix epoch.
    pub created_at: i64,
    pub exited_at: Option<i64>,
}

impl Session {
    /// A new `running` session from a spawn spec, before the child exists.
    pub fn new(id: SessionId, spec: SpawnSpec, labels: BTreeMap<String, String>) -> Self {
        let (cols, rows) = (spec.cols, spec.rows);
        Self {
            id,
            labels,
            spec,
            state: State::Running,
            pid: None,
            exit_code: None,
            lost_reason: None,
            cols,
            rows,
            output_bytes: 0,
            created_at: crate::now_ms(),
            exited_at: None,
        }
    }
}
