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

use crate::server::{ConnRead, ConnWrite};
use anyhow::{Context, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

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

/// How long a stream waits for the reaper to record the exit after the PTY
/// master reports EOF. EOF means the child's side of the pty is closed, so
/// its `wait()` is already imminent; this only covers the scheduling gap
/// between the two threads.
const EXIT_REAP_GRACE: Duration = Duration::from_secs(5);

/// How long the output half may keep going after the input half ends on a
/// session that has already exited, so an exit frame already being written
/// is not cancelled underneath it. A live session's detach does not wait:
/// see the call site.
const INPUT_END_GRACE: Duration = Duration::from_millis(250);

/// The largest inbound frame payload the daemon will allocate for.
///
/// An upstream `len` is an unverified claim (docs/04-protocol.md), so this is
/// a security bound, not a tuning knob: without it `len = 0xFFFFFFFF` buys a
/// 4 GiB allocation for free. One PTY read is `READ_BUFFER_SIZE` and a
/// keystroke is a handful of bytes, so a client claiming more than a single
/// chunk is not typing. Derived rather than restated, so changing the
/// reader's buffer cannot leave this cap meaning something else (#18).
const MAX_INPUT_FRAME_BYTES: usize = crate::pty::READ_BUFFER_SIZE;

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
/// `lease` carries the holder id that identifies this connection for the
/// control lease, and releases it when this returns -- or when the caller
/// drops it first, because the connection died before getting here. One
/// connection is one client, which is the honest reading of "exactly one writer" when
/// nothing on the wire carries a caller-supplied identity. A client that
/// needs a reconnect to count as the same holder layers that above anchor —
/// see docs/06-teleport-seam.md.
pub async fn serve_attached(
    mut out: ConnWrite,
    input: ConnRead,
    // Bytes the client pipelined after its attach line, already read off the
    // socket by the control loop's buffered reader.
    buffered: Vec<u8>,
    session: Arc<Session>,
    from: u64,
    handshake: Handshake,
) -> Result<()> {
    let Handshake {
        lease,
        lease_changes,
        size_changes,
    } = handshake;
    let timeout = write_timeout();
    let holder = lease.holder.clone();
    let epoch = Arc::clone(&lease.epoch);

    // Tell the client where it stands on the lease before any output, so it
    // never has to guess whether its keystrokes will be accepted. Re-read
    // rather than echoing what attach decided: a claim may have landed since,
    // and its broadcast is only a second telling.
    let holds_control = still_mine(&session, &holder, &epoch);
    write_frame(
        &mut out,
        timeout,
        frame::LEASE,
        0,
        &[u8::from(holds_control)],
    )
    .await?;

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
    let mut output = std::pin::pin!(pump_output(
        &mut out,
        &session,
        from,
        timeout,
        &lease,
        lease_changes,
        size_changes
    ));
    let result = tokio::select! {
        result = output.as_mut() => result,
        _ = &mut input_task => {
            // The input half ended. If the session is over, the output
            // half may be mid-exit-frame: `select!` drops the losing future
            // at its next await point, and dropping it there hands the
            // client a closed stream instead of the exit it is owed. Give
            // it a bounded chance to finish.
            //
            // Only when an exit is actually owed, though. On a live session
            // this arm means the client detached, and the right response is
            // to end now — the lease is released when the connection ends,
            // and making the next attacher wait out a grace for a client
            // that has already gone is the wrong trade.
            if session.state().is_terminal() {
                match tokio::time::timeout(INPUT_END_GRACE, output).await {
                    Ok(result) => result,
                    Err(_) => Ok(()),
                }
            } else {
                Ok(())
            }
        }
    };
    input_task.abort();
    result
}

/// Catch-up rounds, then the live subscription, then `exit`.
async fn pump_output(
    out: &mut ConnWrite,
    session: &Arc<Session>,
    from: u64,
    timeout: Duration,
    lease: &LeaseGuard,
    mut lease_changes: tokio::sync::broadcast::Receiver<String>,
    mut size_changes: tokio::sync::broadcast::Receiver<(u16, u16)>,
) -> Result<()> {
    let (holder, epoch) = (lease.holder.as_str(), &lease.epoch);
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
                write_frame(out, timeout, frame::OUTPUT, offset, &bytes).await?;
                step = next_round_from(replay, bytes).await?;
            }
            ReplayStep::Live(attach) => break attach,
        }
    };

    if !attach.replay.is_empty() {
        write_frame(
            out,
            timeout,
            frame::OUTPUT,
            attach.replay_from,
            &attach.replay,
        )
        .await?;
    }

    let mut subscription = attach.subscription;
    let mut exited = false;
    loop {
        tokio::select! {
            chunk = subscription.recv() => match chunk {
                Some(chunk) => {
                    write_frame(out, timeout, frame::OUTPUT, chunk.offset, &chunk.bytes).await?;
                }
                // The subscription ended. Either the session is over, or
                // this subscriber fell behind and was dropped — and those
                // are not the same thing to tell a client.
                None => {
                    if subscription.overflowed() {
                        break;
                    }
                    // EOF: no byte can follow, so this connection is owed an
                    // exit frame. The exit itself is recorded by the reaper
                    // thread, which is not ordered against the EOF thread
                    // that just closed this subscription — so wait for it
                    // rather than reading a `state` that may still say
                    // `running` and finalizing with an unknown exit code.
                    let _ =
                        tokio::time::timeout(EXIT_REAP_GRACE, session.wait_exited()).await;
                    exited = true;
                    break;
                }
            },
            size = size_changes.recv() => {
                // The pty's size changed, possibly at some other client's
                // request. Everyone attached is told, the originator
                // included: what goes out is the size actually applied, so
                // a client that asked for more than the clamp allows
                // learns what it really got.
                match size {
                    Ok((cols, rows)) => {
                        write_frame(
                            out,
                            timeout,
                            frame::RESIZE,
                            0,
                            &frame::encode_size(cols, rows),
                        )
                        .await?;
                    }
                    // Lagged past one or more changes. Only the latest
                    // matters -- a size is a level, not an event -- so
                    // read the current one rather than replaying history.
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        let (cols, rows) = session.size();
                        write_frame(
                            out,
                            timeout,
                            frame::RESIZE,
                            0,
                            &frame::encode_size(cols, rows),
                        )
                        .await?;
                    }
                    // The session is tearing down; the other arms notice.
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {}
                }
            },
            change = lease_changes.recv() => {
                // The lease moved. Tell this connection where it now
                // stands, so a preempted client stops typing into a void
                // and a bystander learns control is taken.
                //
                // The authoritative answer is the lease itself, never the
                // message: broadcasts queue behind a slow write, so by the
                // time this one is read it may describe a holder that has
                // since been replaced -- including by this very connection.
                // Trusting the name wiped a freshly claimed epoch and left
                // a client told it holds control while every keystroke was
                // discarded. Lagged is the same case with the message lost.
                match change {
                    Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        let mine = still_mine(session, holder, epoch);
                        write_frame(out, timeout, frame::LEASE, 0, &[u8::from(mine)]).await?;
                    }
                    // The session is tearing down; the other arms notice.
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {}
                }
            },
            () = session.wait_exited() => {
                // Bounded wait for the reader thread to catch up, then mop
                // up whatever became ready in the meantime.
                let _ = tokio::time::timeout(EXIT_DRAIN_GRACE, session.wait_eof()).await;
                while let Some(chunk) = subscription.try_recv() {
                    write_frame(out, timeout, frame::OUTPUT, chunk.offset, &chunk.bytes).await?;
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
    if !exited {
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
    write_frame(out, timeout, frame::EXIT, snapshot.output_bytes, &payload).await?;
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
        if len > MAX_INPUT_FRAME_BYTES {
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
        //
        // The payload is checked, not assumed: docs/04-protocol.md defines
        // `1` as the claim, and a daemon that treats every lease frame as
        // one would hand control to a client that sent `0` meaning the
        // opposite — or to a future frame this build does not understand.
        if tag == frame::LEASE {
            if payload.first() == Some(&1) {
                // The epoch mutex is held across the claim, so the output
                // half cannot read the claim's own broadcast and re-check
                // the lease before the new epoch is stored -- it would see
                // the old epoch, call this connection the loser, and tell it
                // so. Lock order is epoch, then the lease's own mutex, the
                // same as `still_mine` and the guard's drop. (Not written as
                // `*epoch.lock() = Some(claim())`: the right-hand side runs
                // first, which would take the lock only after the claim.)
                let mut held = epoch.lock();
                *held = Some(session.claim_control(&holder));
            }
            continue;
        }
        // Resize travels here, where the lease is known. Checked per
        // frame like input, so a preempted connection stops being able to
        // reshape the terminal at the same instant it stops typing.
        if tag == frame::RESIZE {
            let Some(current) = *epoch.lock() else {
                continue;
            };
            let Some((cols, rows)) = frame::decode_size(&payload) else {
                tracing::debug!(session = %session.id, "malformed resize frame; ignored");
                continue;
            };
            match session.resize_if_controller(&holder, current, cols, rows) {
                Ok(()) => {}
                Err(None) => {
                    tracing::debug!(session = %session.id, "resize from a non-controller; ignored");
                }
                Err(Some(e)) => {
                    tracing::debug!(session = %session.id, error = %e, "resize rejected");
                }
            }
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
        //
        // Off the runtime's worker: the write holds the lease mutex and, when
        // the child has stopped reading, retries with a sleep for up to
        // WRITE_ENQUEUE_TIMEOUT. On a worker, a handful of stuck children
        // park every worker and stall accept and every other session. Frames
        // stay ordered because each is awaited before the next is read. This
        // moves the wait off the hot path; it does not remove the mutex hold,
        // so a claim on this session still waits behind a stuck write.
        let written = {
            let session = Arc::clone(&session);
            let holder = holder.clone();
            tokio::task::spawn_blocking(move || {
                session.write_if_controller(&holder, current, &payload)
            })
            .await
        };
        match written {
            Ok(Ok(())) => {}
            Ok(Err(None)) => {
                tracing::debug!(session = %session.id, "input from a non-controller; ignored");
            }
            // The write failed, which on a session that just exited means
            // the pty is gone. That is the *output* half's news to deliver,
            // as an exit frame — returning here ends the connection through
            // the outer select instead, cancelling that write mid-flight and
            // closing the stream silently. Keep reading: this half exists to
            // notice the client disconnecting, and the client has not.
            Ok(Err(Some(e))) => {
                tracing::debug!(session = %session.id, error = %e, "input rejected");
            }
            Err(e) => {
                tracing::warn!(session = %session.id, error = %e, "input write panicked");
                return;
            }
        }
    }
}

async fn write_frame(
    out: &mut ConnWrite,
    timeout: Duration,
    tag: u8,
    offset: u64,
    payload: &[u8],
) -> Result<()> {
    let frame = frame::encode(tag, offset, payload);
    // Bounded: see `write_timeout`. A client that is not draining its socket
    // must not hold a task indefinitely.
    let write = async {
        out.write_all(&frame).await.context("writing a frame")?;
        out.flush().await.context("flushing a frame")
    };
    match tokio::time::timeout(timeout, write).await {
        Ok(result) => result,
        Err(_) => anyhow::bail!("the client stopped reading; closing the connection"),
    }
}

/// Whether this connection still holds the lease, read from the lease itself.
/// Clears the epoch when it does not, so the input half stops typing at once
/// and the guard does not release a lease that now belongs to someone else.
fn still_mine(session: &Session, holder: &str, epoch: &parking_lot::Mutex<Option<u64>>) -> bool {
    let mut held = epoch.lock();
    let mine = held.is_some_and(|e| session.is_controller(holder, e));
    if !mine {
        *held = None;
    }
    mine
}

/// This connection's claim on the control lease, from the moment it is taken.
///
/// Built in the same step as `take_control` (see [`Handshake`]), so every way
/// out -- a failed attach reply, a failed first frame, a client disconnect, a
/// write error, a normal exit -- releases the lease by dropping it. A lease stuck on
/// a dead connection locks every other client out of typing, and an observer
/// downgrade that nobody can undo is how that shows up.
///
/// `release_control` is a no-op unless this holder still owns this epoch, so
/// a connection that was preempted while streaming cannot clear the lease
/// out from under whoever took it.
struct LeaseGuard {
    session: Arc<Session>,
    holder: String,
    // Shared, because either half can change it: the input half when the
    // client claims the lease, and the output half's view of whether this
    // connection still holds it after someone else does.
    epoch: Arc<parking_lot::Mutex<Option<u64>>>,
}

/// What an `attach` decides before the connection turns binary: the lease,
/// and the two watchers that hear about changes to it and to the size.
///
/// `begin` is the one place that orders them. Both watchers are subscribed
/// *before* the lease decision and before the caller reads the size for the
/// attach reply: a claim or resize landing between a decision and a later
/// subscribe is never heard, and this connection goes on believing it may
/// type -- the one thing the lease exists to prevent.
pub struct Handshake {
    lease: LeaseGuard,
    lease_changes: tokio::sync::broadcast::Receiver<String>,
    size_changes: tokio::sync::broadcast::Receiver<(u16, u16)>,
}

impl Handshake {
    /// Takes the lease if `control` is asked for and it is free. Attach never
    /// preempts: asking while someone else holds it is an observer attach.
    ///
    /// The caller holds the result across the attach reply, which can fail
    /// to send: dropping it releases the lease, so a connection that dies
    /// here does not strand it.
    pub fn begin(session: &Arc<Session>, holder: &str, control: bool) -> Self {
        let lease_changes = session.watch_lease();
        let size_changes = session.watch_size();
        let epoch = if control {
            session.take_control(holder)
        } else {
            None
        };
        Self {
            lease: LeaseGuard {
                session: Arc::clone(session),
                holder: holder.to_string(),
                epoch: Arc::new(parking_lot::Mutex::new(epoch)),
            },
            lease_changes,
            size_changes,
        }
    }

    /// Whether the attach decision granted control.
    pub fn granted(&self) -> bool {
        self.lease.epoch.lock().is_some()
    }
}

impl Drop for LeaseGuard {
    fn drop(&mut self) {
        if let Some(epoch) = *self.epoch.lock() {
            self.session.release_control(&self.holder, epoch);
        }
    }
}
