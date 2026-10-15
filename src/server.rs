//! The unix socket server.
//!
//! One thread per connection, newline-delimited JSON in and out. The socket
//! is 0600: filesystem permissions are the security boundary, so there is no
//! auth and no TCP.

use std::io::{BufRead, BufReader, Read as _, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;

use anyhow::{bail, Context, Result};
use serde_json::{json, Value};

use crate::proto::{self, ErrorCode, Request};

/// Longest control line accepted. Beyond this the request is refused with
/// `too_large` rather than buffered.
const MAX_LINE_BYTES: usize = 1024 * 1024;

/// Bind the socket, refusing to displace a daemon that is already running.
///
/// A socket file left behind by a dead daemon is stale: nothing accepts on it,
/// so it is removed and rebound.
pub fn bind(socket_path: &Path) -> Result<UnixListener> {
    if socket_path.exists() {
        if UnixStream::connect(socket_path).is_ok() {
            bail!("anchord is already running on {}", socket_path.display());
        }
        tracing::warn!(socket = %socket_path.display(), "removing stale socket");
        std::fs::remove_file(socket_path)
            .with_context(|| format!("removing stale socket {}", socket_path.display()))?;
    }

    let listener = UnixListener::bind(socket_path)
        .with_context(|| format!("binding {}", socket_path.display()))?;
    std::fs::set_permissions(socket_path, std::fs::Permissions::from_mode(0o600))
        .with_context(|| format!("setting 0600 on {}", socket_path.display()))?;
    Ok(listener)
}

/// Accept connections until the listener fails. One thread per client.
pub fn serve(listener: UnixListener) -> Result<()> {
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                std::thread::spawn(move || {
                    if let Err(e) = handle_connection(stream) {
                        tracing::warn!(error = %e, "connection ended with an error");
                    }
                });
            }
            Err(e) => tracing::warn!(error = %e, "accept failed"),
        }
    }
    Ok(())
}

fn handle_connection(stream: UnixStream) -> Result<()> {
    let mut out = stream.try_clone().context("cloning stream for writes")?;
    let mut reader = BufReader::new(stream);
    let mut line = String::new();

    loop {
        line.clear();
        let read = read_line_bounded(&mut reader, &mut line)?;
        match read {
            Read::Eof => return Ok(()),
            Read::TooLarge => {
                respond(
                    &mut out,
                    proto::error(0, ErrorCode::TooLarge, "request line too large"),
                )?;
                // The rest of that line is unread garbage; the client has to
                // reconnect rather than desynchronise the framing.
                return Ok(());
            }
            Read::Line => {}
        }

        if line.trim().is_empty() {
            continue;
        }
        respond(&mut out, dispatch(&line))?;
    }
}

/// Answer one control request. Never panics, never drops the connection.
fn dispatch(line: &str) -> Value {
    let request: Request = match serde_json::from_str(line) {
        Ok(r) => r,
        Err(e) => {
            // Salvage the id if the line was an object with one, so the
            // client can still correlate the failure.
            let id = serde_json::from_str::<Value>(line)
                .ok()
                .and_then(|v| v.get("id").and_then(Value::as_u64))
                .unwrap_or(0);
            return proto::error(id, ErrorCode::BadRequest, format!("malformed request: {e}"));
        }
    };

    match request.op.as_str() {
        "ping" => proto::ok(request.id, json!({ "pong": true })),
        other => proto::error(
            request.id,
            ErrorCode::UnknownOp,
            format!("unknown op {other:?}"),
        ),
    }
}

fn respond(out: &mut UnixStream, value: Value) -> Result<()> {
    let mut buf = serde_json::to_vec(&value)?;
    buf.push(b'\n');
    out.write_all(&buf).context("writing response")?;
    out.flush().context("flushing response")?;
    Ok(())
}

enum Read {
    Line,
    Eof,
    TooLarge,
}

/// `BufRead::read_line` with a hard ceiling, so one client cannot make the
/// daemon buffer without bound.
fn read_line_bounded(reader: &mut impl BufRead, out: &mut String) -> Result<Read> {
    let mut buf = Vec::new();
    let n = reader
        .by_ref()
        .take(MAX_LINE_BYTES as u64 + 1)
        .read_until(b'\n', &mut buf)
        .context("reading request")?;
    if n == 0 {
        return Ok(Read::Eof);
    }
    if buf.len() > MAX_LINE_BYTES {
        return Ok(Read::TooLarge);
    }
    match String::from_utf8(buf) {
        Ok(s) => {
            out.push_str(&s);
            Ok(Read::Line)
        }
        Err(_) => {
            out.push_str("<invalid utf-8>");
            Ok(Read::Line)
        }
    }
}
