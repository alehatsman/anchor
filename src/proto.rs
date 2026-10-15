//! The control framing: newline-delimited JSON, request/response, correlated
//! by `id`. See docs/04-protocol.md.
//!
//! M0 answers `ping` only. Every other op is a well-formed error response —
//! a bad request never drops the connection.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// The fields every request carries. Op-specific fields are parsed by the
/// handler for that op, so an unknown op can still be answered by `id`.
#[derive(Debug, Clone, Deserialize)]
pub struct Request {
    pub id: u64,
    pub op: String,
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
pub fn ok(id: u64, payload: Value) -> Value {
    let mut out = json!({ "id": id, "ok": true });
    if let (Some(out_map), Value::Object(fields)) = (out.as_object_mut(), payload) {
        out_map.extend(fields);
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
