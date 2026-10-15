//! The unix socket server.
//!
//! Newline-delimited JSON in and out, one task per connection. The socket is
//! 0600: filesystem permissions are the security boundary, so there is no
//! auth and no TCP.
//!
//! [`bind`] is deliberately synchronous and returns a `std` listener, so a
//! caller can claim the socket — and fail loudly if another daemon already
//! holds it — before any runtime exists. [`serve`] adopts it.

use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixListener as StdUnixListener;
use std::path::Path;

use anyhow::{bail, Context, Result};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};

use crate::proto::{self, ErrorCode, Request};

/// Longest control line accepted. Beyond this the request is refused with
/// `too_large` rather than buffered.
const MAX_LINE_BYTES: u64 = 1024 * 1024;

/// Binds the socket, refusing to displace a daemon that is already running.
///
/// A socket file left behind by a dead daemon is stale: nothing accepts on
/// it, so it is removed and rebound.
pub fn bind(socket_path: &Path) -> Result<StdUnixListener> {
    if socket_path.exists() {
        if std::os::unix::net::UnixStream::connect(socket_path).is_ok() {
            bail!("anchord is already running on {}", socket_path.display());
        }
        tracing::warn!(socket = %socket_path.display(), "removing stale socket");
        std::fs::remove_file(socket_path)
            .with_context(|| format!("removing stale socket {}", socket_path.display()))?;
    }

    let listener = StdUnixListener::bind(socket_path)
        .with_context(|| format!("binding {}", socket_path.display()))?;
    std::fs::set_permissions(socket_path, std::fs::Permissions::from_mode(0o600))
        .with_context(|| format!("setting 0600 on {}", socket_path.display()))?;
    Ok(listener)
}

/// Accepts connections until the listener fails. One task per client.
///
/// Takes the `std` listener from [`bind`] and adopts it onto the runtime this
/// is polled on.
pub async fn serve(listener: StdUnixListener) -> Result<()> {
    listener
        .set_nonblocking(true)
        .context("setting the listener non-blocking")?;
    let listener = UnixListener::from_std(listener).context("adopting the listener")?;

    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(stream).await {
                        tracing::warn!(error = %e, "connection ended with an error");
                    }
                });
            }
            Err(e) => tracing::warn!(error = %e, "accept failed"),
        }
    }
}

async fn handle_connection(stream: UnixStream) -> Result<()> {
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();

    loop {
        line.clear();
        // Bounded, so one client cannot make the daemon buffer without
        // limit. `take` counts bytes, not lines, and is reset per request.
        let read = (&mut reader)
            .take(MAX_LINE_BYTES + 1)
            .read_line(&mut line)
            .await
            .context("reading request")?;

        if read == 0 {
            return Ok(()); // EOF
        }
        if read as u64 > MAX_LINE_BYTES {
            respond(
                &mut write_half,
                proto::error(0, ErrorCode::TooLarge, "request line too large"),
            )
            .await?;
            // The rest of that line is unread; the client reconnects rather
            // than desynchronising the framing.
            return Ok(());
        }
        if line.trim().is_empty() {
            continue;
        }

        respond(&mut write_half, dispatch(&line)).await?;
    }
}

/// Answers one control request. Never panics, never drops the connection.
fn dispatch(line: &str) -> Value {
    let request: Request = match serde_json::from_str(line) {
        Ok(r) => r,
        Err(e) => {
            // Salvage the id if the line was an object carrying one, so the
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

async fn respond(out: &mut (impl AsyncWriteExt + Unpin), value: Value) -> Result<()> {
    let mut buf = serde_json::to_vec(&value)?;
    buf.push(b'\n');
    out.write_all(&buf).await.context("writing response")?;
    out.flush().await.context("flushing response")?;
    Ok(())
}
