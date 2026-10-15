//! The PTY primitive: spawn, read, write, resize, exit detection, termination.
//!
//! Ported from teleport's `daemon/src/pty.rs`, which is the reference
//! implementation — every rule here was paid for with a real bug there. See
//! docs/03-pty.md, and teleport's `docs/03-pty-layer.md` behind it.
//!
//! Four dedicated `std::thread`s per session — reader, writer, control
//! (`resize`/`terminate`), reaper (`child.wait()`). Never a blocking-pool
//! task: a permanently blocking loop would hold its slot forever.
//!
//! **Scope boundary:** this file owns the raw primitive only. It knows
//! nothing about `output.vt`, byte offsets, or subscribers — that is the
//! session layer, built on what this exposes. The reader thread calls a
//! caller-supplied `on_output` closure synchronously, once per chunk, and
//! **that closure must never block**, for the same reason the reader loop
//! must not. This file cannot enforce it; it is a contract on the caller.
//!
//! **Unix only.** Windows is deferred behind teleport's unresolved `#W1`
//! (a gracefully-exiting ConPTY child was never observed as exited), and its
//! ConPTY startup handshake is not ported. When Windows lands, every
//! `#[cfg(windows)]` it needs belongs in this module and nowhere else.

#![cfg(unix)]

use std::io::{Read, Result as IoResult, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use portable_pty::{
    native_pty_system, Child, ChildKiller, CommandBuilder, ExitStatus, MasterPty, PtySize,
};
use tracing::warn;

/// Bytes per chunk read from the PTY master.
const READ_BUFFER_SIZE: usize = 64 * 1024;

/// Capacity of the write-request channel. Writes are caller-paced
/// (keystrokes, pasted input), so a bounded blocking channel is fine here —
/// it is on its own thread, so a child that never reads its PTY cannot wedge
/// `resize`/`terminate` behind a stuck write.
const WRITE_CHANNEL_CAPACITY: usize = 32;

/// Capacity of the control channel. Bounded rather than unbounded so the
/// sender is `Sync` and a caller can hold a `PtySession` behind a shared
/// reference instead of a mutex. Control traffic is rare and short.
const CONTROL_CHANNEL_CAPACITY: usize = 8;

/// Bounded wait for the graceful signal before the hard kill.
const GRACEFUL_WAIT: Duration = Duration::from_secs(5);
/// Bounded wait for the hard kill before giving up.
const KILL_WAIT: Duration = Duration::from_secs(2);

/// Resize dimensions are clamped to this range. Rejecting an out-of-range
/// value is a protocol error one layer up; clamping here is the backstop that
/// keeps a bad number out of `TIOCSWINSZ`.
const SIZE_RANGE: std::ops::RangeInclusive<u16> = 1..=1000;

const STATE_RUNNING: u8 = 0;
const STATE_CLOSING: u8 = 1;
const STATE_EXITED: u8 = 2;

/// Lifecycle policy on top of `portable-pty`'s platform abstraction. If a
/// caller needs to know the platform, the abstraction has leaked and the fix
/// belongs here, not at the call site.
pub trait TerminalSession {
    /// Enqueues `bytes` onto the write channel; the writer thread sends them
    /// on. Normally returns immediately, but if the child never reads its PTY
    /// and the channel is already full, this blocks the caller until it
    /// drains or the session exits — unlike `terminate`, that bound is not
    /// enforced here.
    ///
    /// Takes `&self`: every method here only sends on a channel or touches an
    /// atomic. A caller wrapping this in a mutex just to call `write` would
    /// recreate the wedge it avoids — a blocked write would hold the lock and
    /// `resize`/`terminate` would queue behind it.
    fn write(&self, bytes: &[u8]) -> Result<()>;

    /// Applies a new PTY size, clamped to `SIZE_RANGE`.
    fn resize(&self, cols: u16, rows: u16) -> Result<()>;

    /// Blocks for up to `GRACEFUL_WAIT + KILL_WAIT` (~7s): that bounded wait
    /// is what termination means here, not something the caller schedules
    /// separately. Idempotent — a second call while closing or exited is a
    /// no-op.
    fn terminate(&self) -> Result<()>;
}

/// Why a session ended up without a clean exit status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LostReason {
    /// The hard-kill step did not produce an observed exit within `KILL_WAIT`.
    KillTimeout,
    /// `child.wait()` returned an OS error rather than a status.
    WaitError,
}

/// Published exactly once: by the reaper when `child.wait()` returns, or by
/// `terminate`'s policy if it has to give up.
///
/// `running -> exited` fires off this and nothing else. Reader EOF is a
/// separate signal: a descendant holding the master open can delay EOF long
/// after the child is gone, and conflating the two is a known bug.
#[derive(Debug, Clone)]
pub struct PtyExit {
    /// `None` only when `lost_reason` is set.
    pub status: Option<ExitStatus>,
    /// Set only when `status` could not be obtained.
    pub lost_reason: Option<LostReason>,
}

/// What to spawn. Argv array, never a concatenated shell string — a shell is
/// only ever `program` because the caller chose it.
#[derive(Debug)]
pub struct SpawnSpec<'a> {
    /// The executable to spawn.
    pub program: &'a str,
    /// `program`'s argv, not including `program` itself.
    pub args: &'a [String],
    /// Working directory to spawn in.
    pub cwd: &'a Path,
    /// Overrides layered onto the daemon's own environment, which the child
    /// inherits by default. Pass explicit overrides only; never pre-flatten
    /// the daemon's whole environment in here.
    pub env: &'a [(String, String)],
    /// Exec `program` through the user's login shell, so it inherits the
    /// environment their dotfiles build rather than the bare one launchd or
    /// systemd hands a background service.
    pub login_shell: bool,
    /// Initial PTY size.
    pub cols: u16,
    /// Initial PTY size.
    pub rows: u16,
}

/// The handle plus the two independent one-shot signals a caller needs: the
/// child's exit, and the master's EOF. Never wait for one to infer the other.
#[derive(Debug)]
pub struct SpawnedSession {
    /// The live handle — `write`/`resize`/`terminate`.
    pub session: PtySession,
    /// Fires exactly once, with the child's exit.
    pub exit_rx: Receiver<PtyExit>,
    /// Fires exactly once, when the reader's `read()` reaches EOF.
    pub eof_rx: Receiver<()>,
    /// The child's OS pid.
    pub pid: Option<u32>,
}

/// The user's login shell. One resolution, one fallback, one place.
pub fn login_shell_path() -> String {
    std::env::var("SHELL").unwrap_or_else(|_| "/bin/sh".to_string())
}

/// Builds the `(shell, argv)` pair that runs `spec.program` under a login
/// shell, or `None` when the spec did not ask for one.
///
/// The shell string is the fixed literal `exec "$0" "$@"`: no request data is
/// ever interpolated into shell source. `sh -c script name args...` binds
/// `name` to `$0` and `args...` to `$@` as separate argv entries, which
/// `exec` passes through untouched.
///
/// `-l`, not `-i`: a login shell sources the profile files that build the
/// environment; an interactive one also loads prompt, completion and plugins
/// — startup cost and stray output with no business in an agent's PTY.
fn login_shell_wrapper(spec: &SpawnSpec<'_>) -> Option<(String, Vec<String>)> {
    if !spec.login_shell {
        return None;
    }
    let mut argv = vec![
        "-l".to_string(),
        "-c".to_string(),
        r#"exec "$0" "$@""#.to_string(),
        spec.program.to_string(),
    ];
    argv.extend_from_slice(spec.args);
    Some((login_shell_path(), argv))
}

/// Spawns `spec` behind a fresh PTY and starts the four session threads.
///
/// `on_output` runs synchronously on the reader thread for every chunk read.
/// It must never block.
pub fn spawn(
    spec: &SpawnSpec<'_>,
    on_output: impl FnMut(&[u8]) + Send + 'static,
) -> Result<SpawnedSession> {
    let system = native_pty_system();
    let pair = system
        .openpty(PtySize {
            rows: spec.rows,
            cols: spec.cols,
            pixel_width: 0,
            pixel_height: 0,
        })
        .context("openpty")?;

    let mut cmd = match login_shell_wrapper(spec) {
        Some((shell, argv)) => {
            let mut cmd = CommandBuilder::new(shell);
            cmd.args(argv);
            cmd
        }
        None => {
            let mut cmd = CommandBuilder::new(spec.program);
            cmd.args(spec.args);
            cmd
        }
    };
    cmd.cwd(spec.cwd);

    // The child already inherits the daemon's environment, TERM included —
    // but a daemon started by launchd or systemd has no controlling terminal
    // and so no TERM at all, and a child that inherits nothing there is
    // treated as a dumb terminal by colour detection and renders monochrome
    // for every viewer. Fill this in only when it is unset: a daemon started
    // from a real terminal (inside tmux, say) must keep its own TERM.
    if cmd.get_env("TERM").is_none() {
        cmd.env("TERM", "xterm-256color");
        cmd.env("COLORTERM", "truecolor");
    }
    // An explicit override always wins, including over the default above.
    for (k, v) in spec.env {
        cmd.env(k, v);
    }

    let child = pair.slave.spawn_command(cmd).context("spawn_command")?;

    // The master only sees EOF once every slave-side handle is gone, and ours
    // is one of them.
    drop(pair.slave);

    let pid = child.process_id();
    let killer = child.clone_killer();

    let reader = pair.master.try_clone_reader().context("clone pty reader")?;
    let writer = pair.master.take_writer().context("take pty writer")?;

    let (write_tx, write_rx) = mpsc::sync_channel::<Vec<u8>>(WRITE_CHANNEL_CAPACITY);
    let (control_tx, control_rx) = mpsc::sync_channel::<ControlEvent>(CONTROL_CHANNEL_CAPACITY);
    let (exit_tx, exit_rx) = mpsc::sync_channel::<PtyExit>(1);
    let (eof_tx, eof_rx) = mpsc::sync_channel::<()>(1);
    let state = Arc::new(AtomicU8::new(STATE_RUNNING));

    std::thread::Builder::new()
        .name("pty-reader".into())
        .spawn(move || reader_thread_main(reader, on_output, &eof_tx))
        .context("spawning reader thread")?;

    std::thread::Builder::new()
        .name("pty-writer".into())
        .spawn(move || writer_thread_main(writer, &write_rx))
        .context("spawning writer thread")?;

    std::thread::Builder::new()
        .name("pty-reaper".into())
        .spawn({
            let control_tx = control_tx.clone();
            move || reaper_thread_main(child, &control_tx)
        })
        .context("spawning reaper thread")?;

    std::thread::Builder::new()
        .name("pty-control".into())
        .spawn({
            let state = Arc::clone(&state);
            move || control_thread_main(pair.master, pid, killer, &control_rx, &exit_tx, &state)
        })
        .context("spawning control thread")?;

    Ok(SpawnedSession {
        session: PtySession {
            write_tx,
            control_tx,
            state,
        },
        exit_rx,
        eof_rx,
        pid,
    })
}

/// The caller-facing handle. Not `Clone`: one owner drives the lifecycle, and
/// sharing it is the session layer's job.
#[derive(Debug)]
pub struct PtySession {
    write_tx: SyncSender<Vec<u8>>,
    control_tx: SyncSender<ControlEvent>,
    state: Arc<AtomicU8>,
}

impl TerminalSession for PtySession {
    fn write(&self, bytes: &[u8]) -> Result<()> {
        if self.state.load(Ordering::SeqCst) != STATE_RUNNING {
            bail!("session is closing or has exited; input rejected");
        }
        self.write_tx
            .send(bytes.to_vec())
            .context("pty writer thread is gone")
    }

    fn resize(&self, cols: u16, rows: u16) -> Result<()> {
        let cols = cols.clamp(*SIZE_RANGE.start(), *SIZE_RANGE.end());
        let rows = rows.clamp(*SIZE_RANGE.start(), *SIZE_RANGE.end());
        self.control_tx
            .send(ControlEvent::Resize { cols, rows })
            .context("pty control thread is gone")
    }

    fn terminate(&self) -> Result<()> {
        if self
            .state
            .compare_exchange(
                STATE_RUNNING,
                STATE_CLOSING,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_err()
        {
            return Ok(()); // already closing or exited — idempotent
        }

        let (reply_tx, reply_rx) = mpsc::sync_channel::<PtyExit>(1);
        if self
            .control_tx
            .send(ControlEvent::Terminate { reply_tx })
            .is_err()
        {
            // The control thread is already gone: it processed a spontaneous
            // child exit and dropped its receiver in the window since the
            // compare_exchange above. The session is exited either way.
            return Ok(());
        }

        // Blocks for up to GRACEFUL_WAIT + KILL_WAIT — that bounded wait is
        // what terminate means. A broken reply channel is the same race as
        // above, caught one step later, and is equally a success.
        let _ = reply_rx.recv();
        Ok(())
    }
}

enum ControlEvent {
    Resize { cols: u16, rows: u16 },
    Terminate { reply_tx: SyncSender<PtyExit> },
    ChildExited(IoResult<ExitStatus>),
}

fn reader_thread_main(
    mut reader: Box<dyn Read + Send>,
    mut on_output: impl FnMut(&[u8]) + Send,
    eof_tx: &SyncSender<()>,
) {
    // Heap, not a 64 KiB stack array: the buffer lives for the thread's whole
    // run and there is nothing to gain from the stack.
    let mut buf = vec![0u8; READ_BUFFER_SIZE];
    loop {
        match reader.read(&mut buf) {
            Ok(0) | Err(_) => break,
            Ok(n) => on_output(&buf[..n]),
        }
    }
    // Best-effort: if nothing is waiting on EOF anymore there is nothing to do.
    let _ = eof_tx.try_send(());
}

fn writer_thread_main(mut writer: Box<dyn Write + Send>, write_rx: &Receiver<Vec<u8>>) {
    for chunk in write_rx {
        // `write_all` can block indefinitely if the child never reads. That
        // is contained to this thread by design — it never touches resize or
        // terminate. A permanently stuck write leaks this thread until the
        // session is dropped; accepted, not solved here.
        if writer.write_all(&chunk).is_err() {
            break;
        }
    }
}

fn reaper_thread_main(
    mut child: Box<dyn Child + Send + Sync>,
    control_tx: &SyncSender<ControlEvent>,
) {
    let result = child.wait();
    // Ignored if the control thread already finished — e.g. it gave up on a
    // hard-kill timeout. Its result stands.
    let _ = control_tx.send(ControlEvent::ChildExited(result));
}

fn control_thread_main(
    master: Box<dyn MasterPty + Send>,
    pid: Option<u32>,
    _killer: Box<dyn ChildKiller + Send + Sync>,
    control_rx: &Receiver<ControlEvent>,
    exit_tx: &SyncSender<PtyExit>,
    state: &Arc<AtomicU8>,
) {
    loop {
        let Ok(event) = control_rx.recv() else {
            return; // both the session and the reaper are gone
        };

        match event {
            ControlEvent::Resize { cols, rows } => {
                if state.load(Ordering::SeqCst) == STATE_RUNNING {
                    // Best-effort: if the PTY is already gone there is
                    // nothing to resize.
                    let _ = master.resize(PtySize {
                        rows,
                        cols,
                        pixel_width: 0,
                        pixel_height: 0,
                    });
                }
            }

            ControlEvent::ChildExited(result) => {
                state.store(STATE_EXITED, Ordering::SeqCst);
                let _ = exit_tx.try_send(pty_exit_from_wait(result));
                return; // `master` drops with this frame
            }

            ControlEvent::Terminate { reply_tx } => {
                // Step 2: graceful signal. Not dropping the master — the
                // reader and writer hold their own dups of the same open file
                // description, so dropping it closes nothing and raises no
                // signal.
                graceful_signal_group(pid);

                let exit = match wait_for_child_exited(control_rx, Instant::now() + GRACEFUL_WAIT) {
                    Some(result) => pty_exit_from_wait(result),
                    None => {
                        // Step 4: hard kill. Not `killer.kill()` —
                        // portable-pty 0.9's cloned killer sends SIGHUP on
                        // Unix, which a child ignoring HUP survives. This
                        // step has to send SIGKILL itself.
                        if let Err(e) = hard_kill_group(pid) {
                            warn!(?pid, error = %e, "hard kill failed");
                        }
                        match wait_for_child_exited(control_rx, Instant::now() + KILL_WAIT) {
                            Some(result) => pty_exit_from_wait(result),
                            // Step 5: give up. Exited, with no exit code.
                            None => PtyExit {
                                status: None,
                                lost_reason: Some(LostReason::KillTimeout),
                            },
                        }
                    }
                };

                state.store(STATE_EXITED, Ordering::SeqCst);
                // Best-effort notification; `exit_tx` is the record.
                let _ = exit_tx.try_send(exit.clone());
                // The caller's receiver may already be gone if its own wait
                // returned first.
                let _ = reply_tx.send(exit);
                return; // `master` drops with this frame
            }
        }
    }
}

/// Step 2: SIGHUP to the session's process group, then SIGTERM to the child.
/// The child leads the group — portable-pty spawns it under `setsid` — so the
/// group is the same target the hard kill uses.
fn graceful_signal_group(pid: Option<u32>) {
    // A negative `pid_t` means "this process's whole group" to kill(2).
    // Never send one just because a u32 wrapped through the cast; no real pid
    // reaches that range, and skipping is the safe failure mode.
    let Some(pid) = pid.and_then(|p| libc::pid_t::try_from(p).ok()) else {
        return;
    };
    // SAFETY: killpg/kill with a pid we own — this session's child — and
    // signals that cannot affect memory safety.
    unsafe {
        libc::killpg(pid, libc::SIGHUP);
        libc::kill(pid, libc::SIGTERM);
    }
}

/// Step 4: SIGKILL to the whole process group.
fn hard_kill_group(pid: Option<u32>) -> IoResult<()> {
    let Some(pid) = pid.and_then(|p| libc::pid_t::try_from(p).ok()) else {
        return Err(std::io::Error::other("no valid pid to kill"));
    };
    // SAFETY: as above.
    let rc = unsafe { libc::killpg(pid, libc::SIGKILL) };
    if rc == 0 {
        return Ok(());
    }
    let err = std::io::Error::last_os_error();
    // ESRCH: the group died between the graceful wait timing out and now.
    // Nothing left to kill is not a failure.
    if err.raw_os_error() == Some(libc::ESRCH) {
        Ok(())
    } else {
        Err(err)
    }
}

/// Services — and mostly ignores — other control events while waiting
/// specifically for the child's exit, up to `deadline`. `None` on timeout.
fn wait_for_child_exited(
    control_rx: &Receiver<ControlEvent>,
    deadline: Instant,
) -> Option<IoResult<ExitStatus>> {
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return None;
        }
        match control_rx.recv_timeout(remaining) {
            Ok(ControlEvent::ChildExited(result)) => return Some(result),
            Ok(ControlEvent::Resize { .. }) => {} // ignored while closing
            Ok(ControlEvent::Terminate { reply_tx }) => {
                // A second terminate while closing should not reach here —
                // the caller-side compare_exchange makes it a no-op first.
                // If it somehow does, dropping the reply channel makes that
                // caller's wait return promptly instead of hanging.
                drop(reply_tx);
            }
            // Timed out, or the reaper is gone without reporting. Either way
            // no exit arrived in time.
            Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => return None,
        }
    }
}

fn pty_exit_from_wait(result: IoResult<ExitStatus>) -> PtyExit {
    match result {
        Ok(status) => PtyExit {
            status: Some(status),
            lost_reason: None,
        },
        Err(_) => PtyExit {
            status: None,
            lost_reason: Some(LostReason::WaitError),
        },
    }
}
