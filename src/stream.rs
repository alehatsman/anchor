//! The streaming half of a connection: everything after a successful
//! `attach`.
//!
//! The connection stops being newline-delimited JSON and carries binary
//! frames (see [`crate::proto::frame`]). Three things happen at once:
//!
//! - **history, then live.** The catch-up rounds run first, off the fan-out
//!   mutex, and the subscriber is registered only in the round that finds
//!   the remaining gap small enough — so a subscriber's queue is never asked
//!   to hold history and live output at the same time.
//! - **input, if this connection holds the lease.** Checked per frame, so a
//!   connection preempted mid-stream stops being able to type immediately.
//! - **exit, once.** Fired off the session's exited signal, with a bounded
//!   wait for trailing output so the last chunk is not left behind.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};

use crate::proto::frame;
use crate::session::{ReplayStep, Session};

/// How long to wait for the reader thread to drain trailing output after the
/// child exits, before sending `exit`.
///
/// The reaper's `wait()` can return before the reader's last `read()` does,
/// so firing `exit` off the exit signal alone can cut the final chunk from a
/// live viewer. It is still on disk and a reconnect replays it — this just
/// avoids making the viewer reconnect to see the last line.
const EXIT_DRAIN_GRACE: Duration = Duration::from_millis(250);

/// Default for how long one frame write may take before the connection is
/// closed.
///
/// The queue budget protects a *registered* subscriber, but it cannot help a
/// client that stops reading during catch-up: there is no slot to drop yet,
/// so the write simply blocks and the connection parks a task forever. The
/// PTY is never affected — catch-up runs off the fan-out mutex — but a
/// client that is not reading is not a client. Closing it is the same remedy
/// backpressure applies, for the same reason: reconnect and replay.
const WRITE_TIMEOUT_DEFAULT: Duration = Duration::from_secs(30);

/// `WRITE_TIMEOUT_DEFAULT`, or `ANCHOR_WRITE_TIMEOUT_MS` when set.
///
/// An operator knob rather than a hidden constant: the right value depends
/// on how slow the slowest legitimate client is, and a fixture needs a
/// smaller one to prove the behaviour without waiting half a minute.
fn write_timeout() -> Duration {
    std::env::var("ANCHOR_WRITE_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(WRITE_TIMEOUT_DEFAULT)
}

/// Serves one attached connection until it ends.
///
/// `holder` identifies this connection for the control lease. One connection
/// is one client: docs/04-protocol.md leaves lease identity to an open
/// issue, and per-connection is the honest reading of "exactly one writer".
pub async fn serve_attached(
    mut out: OwnedWriteHalf,
    input: OwnedReadHalf,
    // Bytes the client pipelined after its attach line, already read off the
    // socket by the control loop's buffered reader.
    buffered: Vec<u8>,
    session: Arc<Session>,
    from: u64,
    holder: String,
    control_epoch: Option<u64>,
) -> Result<()> {
    // Tell the client where it stands on the lease before any output, so it
    // never has to guess whether its keystrokes will be accepted.
    let holds_control = control_epoch.is_some();
    write_frame(&mut out, frame::LEASE, 0, &[u8::from(holds_control)]).await?;

    // Shared, because either half can change it: the input half when the
    // client claims the lease, and the output half's view of whether this
    // connection still holds it after someone else does.
    let epoch = Arc::new(parking_lot::Mutex::new(control_epoch));

    // The lease is per-connection, so it must not outlive this one. A guard
    // rather than a line at the end: this returns on a client disconnect, a
    // write error and a normal exit alike, and a lease stuck on a dead
    // connection would lock every other client out of typing.
    let _lease = LeaseGuard {
        session: Arc::clone(&session),
        holder: holder.clone(),
        epoch: Arc::clone(&epoch),
    };

    // Input runs independently: a client that only observes never sends a
    // frame, and a controller must not have its keystrokes queued behind
    // the output it is reading.
    let mut input_task = tokio::spawn(pump_input(
        std::io::Cursor::new(buffered).chain(input),
        Arc::clone(&session),
        holder.clone(),
        Arc::clone(&epoch),
    ));

    // Either half ending ends the connection. Waiting only on the output
    // half would miss a client that disconnects while the session is quiet:
    // there is no write to fail, so nothing would notice until the next
    // chunk — and the lease would stay held on a dead connection until then.
    let result = tokio::select! {
        result = pump_output(&mut out, &session, from, &holder, &epoch) => result,
        _ = &mut input_task => Ok(()),
    };
    input_task.abort();
    result
}

/// Catch-up rounds, then the live subscription, then `exit`.
async fn pump_output(
    out: &mut OwnedWriteHalf,
    session: &Arc<Session>,
    from: u64,
    holder: &str,
    epoch: &Arc<parking_lot::Mutex<Option<u64>>>,
) -> Result<()> {
    let mut lease_changes = session.watch_lease();
    let replay = session.attach(from).context("beginning the attach")?;

    // Catch up in bounded rounds. Each round is written to the client before
    // the next is asked for — that is what makes the gap a measurement of
    // whether the client is keeping up, and therefore what makes this
    // converge.
    let mut step = next_round(replay).await?;
    let attach = loop {
        match step {
            ReplayStep::History {
                offset,
                bytes,
                replay,
            } => {
                write_frame(out, frame::OUTPUT, offset, &bytes).await?;
                step = next_round_from(replay, bytes).await?;
            }
            ReplayStep::Live(attach) => break attach,
        }
    };

    if !attach.replay.is_empty() {
        write_frame(out, frame::OUTPUT, attach.replay_from, &attach.replay).await?;
    }

    let mut subscription = attach.subscription;
    let mut exited = false;
    loop {
        tokio::select! {
            chunk = subscription.recv() => match chunk {
                Some(chunk) => {
                    write_frame(out, frame::OUTPUT, chunk.offset, &chunk.bytes).await?;
                }
                // The subscription ended. Either the session is over, or
                // this subscriber fell behind and was dropped — and those
                // are not the same thing to tell a client.
                None => break,
            },
            change = lease_changes.recv() => {
                // The lease moved. Tell this connection where it now
                // stands, so a preempted client stops typing into a void
                // and a bystander learns control is taken.
                if let Ok(new_holder) = change {
                    let mine = new_holder == holder;
                    if !mine {
                        *epoch.lock() = None;
                    }
                    write_frame(out, frame::LEASE, 0, &[u8::from(mine)]).await?;
                }
            },
            () = session.wait_exited() => {
                // Bounded wait for the reader thread to catch up, then mop
                // up whatever became ready in the meantime.
                let _ = tokio::time::timeout(EXIT_DRAIN_GRACE, session.wait_eof()).await;
                while let Some(chunk) = subscription.try_recv() {
                    write_frame(out, frame::OUTPUT, chunk.offset, &chunk.bytes).await?;
                }
                exited = true;
                break;
            }
        }
    }

    // A subscriber dropped for falling behind must not be told the session
    // exited: it is still running, and an exit frame would send the client
    // off to render a dead session instead of reconnecting from its last
    // offset. Closing the connection is the whole message.
    if !exited && !session.state().is_terminal() {
        tracing::debug!(
            session = %session.id,
            "subscriber dropped for falling behind; closing without an exit frame"
        );
        return Ok(());
    }

    let snapshot = session.snapshot();
    let payload = match snapshot.exit_code {
        Some(code) => code.to_le_bytes().to_vec(),
        // Empty when the exit code is unknown — a kill timeout, or a wait
        // that errored.
        None => Vec::new(),
    };
    write_frame(out, frame::EXIT, snapshot.output_bytes, &payload).await?;
    out.flush().await.ok();
    Ok(())
}

/// One catch-up round. The read is file I/O, so it runs off the runtime's
/// worker rather than blocking it.
async fn next_round(replay: crate::session::Replay) -> Result<ReplayStep> {
    tokio::task::spawn_blocking(move || replay.next_round())
        .await
        .context("catch-up round panicked")?
        .context("reading replay history")
}

/// The next round, handing this round's own bytes back — which is what makes
/// "write before asking for more" something the compiler checks.
async fn next_round_from(
    replay: crate::session::HistoryReplay,
    bytes: Vec<u8>,
) -> Result<ReplayStep> {
    tokio::task::spawn_blocking(move || replay.written(bytes))
        .await
        .context("catch-up round panicked")?
        .context("reading replay history")
}

/// Reads input frames and writes them to the PTY, while this connection
/// holds the lease.
async fn pump_input(
    mut input: impl tokio::io::AsyncRead + Unpin,
    session: Arc<Session>,
    holder: String,
    epoch: Arc<parking_lot::Mutex<Option<u64>>>,
) {
    let mut header = [0u8; frame::HEADER_LEN];
    loop {
        if input.read_exact(&mut header).await.is_err() {
            return; // client went away
        }
        let (tag, _offset, len) = frame::decode_header(&header);
        // One PTY read is 64 KiB, and a keystroke is a handful of bytes; a
        // client claiming more than a chunk is not typing.
        if len > 64 * 1024 {
            tracing::warn!(len, "oversized input frame; dropping the connection");
            return;
        }
        let mut payload = vec![0u8; len];
        if input.read_exact(&mut payload).await.is_err() {
            return;
        }
        // A lease frame from the client is a claim: take control, even
        // from a holder that has it. Attach never preempts — this is the
        // explicit act that does, which is why it is a separate request
        // rather than a flag on attach.
        if tag == frame::LEASE {
            *epoch.lock() = Some(session.claim_control(&holder));
            continue;
        }
        if tag != frame::INPUT {
            continue; // nothing else flows this way
        }

        // An observer reads its socket purely so a disconnect is noticed;
        // nothing it sends is ever applied.
        let Some(current) = *epoch.lock() else {
            continue;
        };

        // Checked per frame: a connection preempted mid-stream stops being
        // able to type the moment the lease moves.
        match session.write_if_controller(&holder, current, &payload) {
            Ok(()) => {}
            Err(None) => {
                tracing::debug!(session = %session.id, "input from a non-controller; ignored");
            }
            Err(Some(e)) => {
                tracing::debug!(session = %session.id, error = %e, "input rejected");
                return;
            }
        }
    }
}

async fn write_frame(out: &mut OwnedWriteHalf, tag: u8, offset: u64, payload: &[u8]) -> Result<()> {
    let frame = frame::encode(tag, offset, payload);
    // Bounded: see WRITE_TIMEOUT. A client that is not draining its socket
    // must not hold a task indefinitely.
    let write = async {
        out.write_all(&frame).await.context("writing a frame")?;
        out.flush().await.context("flushing a frame")
    };
    match tokio::time::timeout(write_timeout(), write).await {
        Ok(result) => result,
        Err(_) => anyhow::bail!("the client stopped reading; closing the connection"),
    }
}

/// Releases this connection's control lease when the connection ends.
///
/// `release_control` is a no-op unless this holder still owns this epoch, so
/// a connection that was preempted while streaming cannot clear the lease
/// out from under whoever took it.
struct LeaseGuard {
    session: Arc<Session>,
    holder: String,
    epoch: Arc<parking_lot::Mutex<Option<u64>>>,
}

impl Drop for LeaseGuard {
    fn drop(&mut self) {
        if let Some(epoch) = *self.epoch.lock() {
            self.session.release_control(&self.holder, epoch);
        }
    }
}
