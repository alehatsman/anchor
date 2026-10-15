//! The control framing: newline-delimited JSON, request/response, correlated
//! by `id`. See docs/04-protocol.md.
//!
//! After a successful `attach` the connection stops being newline-delimited
//! JSON and carries binary frames instead — see [`frame`].

use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::model::{Session, SpawnSpec};

/// The fields every request carries. Op-specific fields are parsed per op,
/// so an unknown op can still be answered by `id`.
#[derive(Debug, Clone, Deserialize)]
pub struct Request {
    pub id: u64,
    pub op: String,
}

/// A spawn spec as it arrives on the wire. Everything but `command` has a
/// default, so a caller spawning a shell sends two fields.
#[derive(Debug, Clone, Deserialize)]
pub struct SpecWire {
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    /// Defaults to the daemon's cwd — explicit is better, but a missing cwd
    /// should not be an error.
    #[serde(default)]
    pub cwd: Option<PathBuf>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default = "default_cols")]
    pub cols: u16,
    #[serde(default = "default_rows")]
    pub rows: u16,
    #[serde(default)]
    pub login_shell: bool,
}

fn default_cols() -> u16 {
    80
}

fn default_rows() -> u16 {
    24
}

impl SpecWire {
    /// Resolves into the model's spec. `cwd` falls back to the daemon's own
    /// working directory.
    pub fn resolve(self) -> std::io::Result<SpawnSpec> {
        let cwd = match self.cwd {
            Some(cwd) => cwd,
            None => std::env::current_dir()?,
        };
        Ok(SpawnSpec {
            command: self.command,
            args: self.args,
            cwd,
            env: self.env,
            cols: self.cols,
            rows: self.rows,
            login_shell: self.login_shell,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    NotFound,
    InvalidSpec,
    InvalidLabel,
    SpawnFailed,
    AlreadyExited,
    LeaseHeld,
    TooLarge,
    UnknownOp,
    /// A line that is not a valid request object at all.
    BadRequest,
}

impl ErrorCode {
    pub fn as_str(self) -> &'static str {
        match self {
            ErrorCode::NotFound => "not_found",
            ErrorCode::InvalidSpec => "invalid_spec",
            ErrorCode::InvalidLabel => "invalid_label",
            ErrorCode::SpawnFailed => "spawn_failed",
            ErrorCode::AlreadyExited => "already_exited",
            ErrorCode::LeaseHeld => "lease_held",
            ErrorCode::TooLarge => "too_large",
            ErrorCode::UnknownOp => "unknown_op",
            ErrorCode::BadRequest => "bad_request",
        }
    }
}

/// `{"id":N,"ok":true, ...payload}`. `payload` must be a JSON object.
///
/// `id` and `ok` are the envelope and are written last, so a payload field
/// of either name cannot overwrite them — a response whose `id` is a session
/// id instead of the request's is unanswerable, and that is exactly the bug
/// a payload named `id` used to cause.
pub fn ok(id: u64, payload: Value) -> Value {
    let mut out = json!({});
    if let (Some(out_map), Value::Object(fields)) = (out.as_object_mut(), payload) {
        out_map.extend(fields);
        out_map.insert("id".to_string(), json!(id));
        out_map.insert("ok".to_string(), json!(true));
    }
    out
}

/// `{"id":N,"ok":false,"error":{"code":...,"message":...}}`.
///
/// An id of 0 means the request was unparseable and had no usable id.
pub fn error(id: u64, code: ErrorCode, message: impl Into<String>) -> Value {
    json!({
        "id": id,
        "ok": false,
        "error": { "code": code.as_str(), "message": message.into() },
    })
}

/// One session as it appears in a response.
///
/// `env` is deliberately **not** included: a spec's environment can carry
/// tokens a client passed in at create time, and echoing it back widens who
/// can read them. docs/03-pty.md calls for exactly this redaction.
pub fn session_json(session: &Session) -> Value {
    json!({
        "id": session.id,
        "labels": session.labels,
        "state": session.state.as_str(),
        "pid": session.pid,
        "exit_code": session.exit_code,
        "lost_reason": session.lost_reason,
        "cols": session.cols,
        "rows": session.rows,
        "output_bytes": session.output_bytes,
        "log_capped_at": session.log_capped_at,
        "created_at": session.created_at,
        "exited_at": session.exited_at,
        "command": session.spec.command,
        "args": session.spec.args,
        "cwd": session.spec.cwd,
        "login_shell": session.spec.login_shell,
        "env_count": session.spec.env.len(),
    })
}

/// The binary framing an attached connection carries.
///
/// ```text
/// ┌────────┬──────────┬──────────────┐
/// │ u8 tag │ u64 off  │ payload      │
/// └────────┴──────────┴──────────────┘
/// ```
///
/// The offset is big-endian: a frame header is read byte by byte off a
/// socket, and network order is the convention for that. Payload length is
/// framed by a u32 that follows the offset, so a reader never has to guess
/// where a frame ends.
pub mod frame {
    /// Output from the PTY. `off` is the offset of the first byte.
    pub const OUTPUT: u8 = 0x01;
    /// Input to the PTY, client to daemon. Lease holder only.
    pub const INPUT: u8 = 0x02;
    /// The session exited. Payload is the exit code as i32 little-endian,
    /// or empty if it is unknown.
    pub const EXIT: u8 = 0x03;
    /// Lease state. Payload is 1 if this client holds it, 0 if revoked.
    pub const LEASE: u8 = 0x04;
    /// Resize, client to daemon. Payload is cols then rows, both `u16`
    /// big-endian. Lease holder only.
    ///
    /// A frame rather than a control op because the lease is
    /// per-connection: a controller holds it on *this* connection, and a
    /// control op arrives on a different one that can prove nothing about
    /// it. Resize has to travel where the lease is already known.
    pub const RESIZE: u8 = 0x05;

    /// Header size: tag, offset, payload length.
    pub const HEADER_LEN: usize = 1 + 8 + 4;

    /// Encodes one frame.
    pub fn encode(tag: u8, offset: u64, payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(HEADER_LEN + payload.len());
        out.push(tag);
        out.extend_from_slice(&offset.to_be_bytes());
        out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        out.extend_from_slice(payload);
        out
    }

    /// Decodes a header. Returns `(tag, offset, payload_len)`.
    pub fn decode_header(buf: &[u8; HEADER_LEN]) -> (u8, u64, usize) {
        let tag = buf[0];
        let offset = u64::from_be_bytes(buf[1..9].try_into().expect("8 bytes"));
        let len = u32::from_be_bytes(buf[9..13].try_into().expect("4 bytes")) as usize;
        (tag, offset, len)
    }

    /// Encodes a resize payload: cols then rows, big-endian.
    pub fn encode_size(cols: u16, rows: u16) -> Vec<u8> {
        let mut out = Vec::with_capacity(4);
        out.extend_from_slice(&cols.to_be_bytes());
        out.extend_from_slice(&rows.to_be_bytes());
        out
    }

    /// Decodes a resize payload. `None` if it is not four bytes.
    pub fn decode_size(payload: &[u8]) -> Option<(u16, u16)> {
        let cols = u16::from_be_bytes(payload.get(0..2)?.try_into().ok()?);
        let rows = u16::from_be_bytes(payload.get(2..4)?.try_into().ok()?);
        Some((cols, rows))
    }
}
