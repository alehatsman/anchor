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
    /// Writing the output log failed. The only reason that can be set while
    /// the child is still running — the session continues, unpersisted.
    pub const IO_ERROR: &str = "io_error";
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
    /// Cached length of the byte stream. Never rewinds. Equal to the file
    /// length unless the log was capped or a write failed, in which case the
    /// stream kept advancing past what is on disk.
    pub output_bytes: u64,
    /// Offset at which persistence stopped — a size cap or an I/O error.
    /// `None` while the whole stream is readable from `output.vt`.
    pub log_capped_at: Option<u64>,
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
            log_capped_at: None,
            created_at: crate::now_ms(),
            exited_at: None,
        }
    }
}

/// Limits on labels, from docs/02-model.md.
pub mod labels {
    use std::collections::BTreeMap;

    /// At most this many labels on one session.
    pub const MAX_PER_SESSION: usize = 32;
    /// Longest label key.
    pub const MAX_KEY_LEN: usize = 64;
    /// Longest label value, in bytes.
    pub const MAX_VALUE_BYTES: usize = 256;

    /// Why a label was rejected. anchor never interprets a label — these are
    /// the only things it checks.
    #[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
    pub enum InvalidLabel {
        #[error("too many labels: {count} (max {MAX_PER_SESSION})")]
        TooMany { count: usize },
        #[error("label key {key:?} is empty or longer than {MAX_KEY_LEN} characters")]
        KeyLength { key: String },
        #[error("label key {key:?} must match [a-z0-9_.-]")]
        KeyCharset { key: String },
        #[error(
            "label {key:?} has a value of {len} bytes (max {MAX_VALUE_BYTES})"
        )]
        ValueLength { key: String, len: usize },
    }

    /// Checks a whole label set. Called before a session is created and
    /// before labels are replaced, so an invalid set never reaches the store.
    pub fn validate(labels: &BTreeMap<String, String>) -> Result<(), InvalidLabel> {
        if labels.len() > MAX_PER_SESSION {
            return Err(InvalidLabel::TooMany { count: labels.len() });
        }
        for (key, value) in labels {
            validate_key(key)?;
            if value.len() > MAX_VALUE_BYTES {
                return Err(InvalidLabel::ValueLength {
                    key: key.clone(),
                    len: value.len(),
                });
            }
        }
        Ok(())
    }

    /// `[a-z0-9_.-]{1,64}`, spelled out rather than pulled in as a regex
    /// dependency for one pattern.
    pub fn validate_key(key: &str) -> Result<(), InvalidLabel> {
        if key.is_empty() || key.len() > MAX_KEY_LEN {
            return Err(InvalidLabel::KeyLength { key: key.to_string() });
        }
        if !key
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'_' | b'.' | b'-'))
        {
            return Err(InvalidLabel::KeyCharset { key: key.to_string() });
        }
        Ok(())
    }
}
