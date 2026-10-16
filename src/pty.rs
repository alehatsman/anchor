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
//! **Unix only, by omission rather than by blocker.** This deferral used to
//! cite teleport's `#W1` (a gracefully-exiting ConPTY child was never
//! observed as exited) as unresolved. It was fixed on 2026-09-05 --
//! teleport#9 -- and this comment went stale for six weeks while reading as
//! a live obstacle.
//!
//! The root cause was never reaping. conhost's ConPTY startup handshake
//! writes a cursor-position query (`ESC[6n`) to the pty and blocks its
//! IO thread waiting for a reply; the child had already exited and conhost
//! was still mid-handshake, so nothing tore down. Answering that one query
//! returns `wait()` in single-digit milliseconds. teleport's
//! `ConptyDsrProbe` does exactly that and then stops answering, so a later
//! `ESC[6n` from the application still gets a real cursor position --
//! the stopping is the subtle half, since a probe that kept replying would
//! corrupt every app that asks.
//!
//! So what remains for Windows is a port, not a research problem: bring the
//! probe and its test across, and decide the access-control story, since
//! `server.rs`'s 0600 boundary has no named-pipe equivalent. When Windows
//! lands, every `#[cfg(windows)]` it needs belongs in this module and
//! nowhere else.

#![cfg(unix)]

use std::io::{Read, Result as IoResult, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
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

/// Longest a `write` will wait for room on the write channel.
///
/// The channel only fills when the child has stopped reading its PTY, and a
/// caller holds the control lease across this call — so an unbounded wait
/// there wedges every other lease operation behind a child that may never
/// read again. A keystroke that cannot be delivered in this long is not
/// going to be; saying so beats blocking.
const WRITE_ENQUEUE_TIMEOUT: Duration = Duration::from_millis(250);

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
    /// on. Normally returns immediately. If the child has stopped reading
    /// its PTY and the channel is full, this waits up to
    /// `WRITE_ENQUEUE_TIMEOUT` and then fails rather than blocking: callers
    /// hold the control lease across this call, so an unbounded wait would
    /// wedge every other lease operation behind an unresponsive child.
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
///
/// An **empty** `SHELL` is treated as unset. It is set that way by some
/// service managers and container images, and taking it literally spawns
/// the empty program -- which fails at exec with a message about `""`
/// rather than about a shell, long after the useful place to notice.
pub fn login_shell_path() -> String {
    std::env::var("SHELL")
        .ok()
        .filter(|shell| !shell.trim().is_empty())
        .unwrap_or_else(|| "/bin/sh".to_string())
}

/// The shell families that need a different `-c` invocation
/// (docs/03-pty.md, anchor#10).
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum ShellFamily {
    /// `sh`, `bash`, `zsh`, `ksh`, `dash`, and anything unrecognised.
    Posix,
    /// `fish`: takes `-l`, binds `$argv`, never word-splits on expansion.
    Fish,
    /// `csh`, `tcsh`: rejects `-l` beside `-c`, has no `$0`/`$@`, and
    /// word-splits `$argv` unless it is quoted with `:q`.
    Csh,
}

/// Classifies a shell by the file name of its path.
///
/// The file name, not the whole path: `/opt/homebrew/bin/fish` and
/// `/usr/local/bin/fish` are the same shell. A version suffix is not
/// stripped — `fish3` is not a name any distribution ships, and guessing at
/// suffixes would misclassify a shell genuinely called something else.
///
/// Unrecognised means `Posix`. An unknown shell is far likelier to be
/// POSIX-compatible than not, and if it is not, the spawn fails loudly with
/// the shell's own usage message rather than silently running under the
/// wrong environment.
fn shell_family(shell: &str) -> ShellFamily {
    let name = std::path::Path::new(shell)
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    match name.as_str() {
        "fish" => ShellFamily::Fish,
        "csh" | "tcsh" => ShellFamily::Csh,
        _ => ShellFamily::Posix,
    }
}

/// Builds the `(shell, argv)` pair that runs `spec.program` under a login
/// shell, or `None` when the spec did not ask for one.
///
/// **No request data is ever interpolated into shell source.** Every form
/// here passes a fixed script literal and hands the program and its
/// arguments to the shell as separate argv entries; the script expands them
/// back without splitting or globbing. That property is the reason the
/// wrapper exists in this shape, and a new family does not get added without
/// it.
///
/// `-l`, not `-i`: a login shell sources the profile files that build the
/// environment; an interactive one also loads prompt, completion and plugins
/// — startup cost and stray output with no business in an agent's PTY.
///
/// csh and tcsh get no `-l`: they reject it beside `-c` outright, and no
/// flag combines the two. Those users get `.cshrc` and not `.login`, which
/// is a real shortfall against what `login_shell` promises and is documented
/// as one. Falling back to `/bin/sh` instead would source their config not
/// at all, while still claiming to be their login shell.
fn login_shell_wrapper(spec: &SpawnSpec<'_>) -> Option<(String, Vec<String>)> {
    if !spec.login_shell {
        return None;
    }
    let shell = login_shell_path();
    if shell_family(&shell) == ShellFamily::Csh {
        // Say it where someone hits it, not only in the spec. Without this
        // the shortfall surfaces as `command not found` for a program that
        // is on PATH in the user's real login shell -- a true error that
        // names the wrong layer, and the one thing `login_shell` exists to
        // prevent.
        tracing::warn!(
            shell = %shell,
            "login_shell under csh/tcsh sources .cshrc but not .login: those \
             shells reject -l beside -c. A PATH set in .login will be missing."
        );
    }
    let argv = wrapper_argv(&shell, spec.program, spec.args);
    Some((shell, argv))
}

/// The argv for a given shell, split out of `login_shell_wrapper` so it is
/// testable without mutating `$SHELL` — an env var is process-global, and a
/// test that writes it races every other test in the binary.
fn wrapper_argv(shell: &str, program: &str, args: &[String]) -> Vec<String> {
    // `$argv:q` in the csh form, never a bare `$argv`: csh word-splits and
    // globs an unquoted expansion, which turns one argument `a b` into two
    // and a literal `*` into the directory listing.
    let mut argv = match shell_family(shell) {
        ShellFamily::Posix => vec![
            "-l".to_string(),
            "-c".to_string(),
            r#"exec "$0" "$@""#.to_string(),
        ],
        ShellFamily::Fish => vec!["-l".to_string(), "-c".to_string(), "exec $argv".to_string()],
        ShellFamily::Csh => vec!["-c".to_string(), "exec $argv:q".to_string()],
    };
    argv.push(program.to_string());
    argv.extend_from_slice(args);
    argv
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

    // From here until every thread is running, a failure would leave the
    // child alive with nothing watching it. See `OrphanGuard`.
    let orphan_guard = OrphanGuard { pid, armed: true };

    let reader = pair.master.try_clone_reader().context("clone pty reader")?;
    let writer = pair.master.take_writer().context("take pty writer")?;

    let (write_tx, write_rx) = mpsc::sync_channel::<Vec<u8>>(WRITE_CHANNEL_CAPACITY);
    let (control_tx, control_rx) = mpsc::sync_channel::<ControlEvent>(CONTROL_CHANNEL_CAPACITY);
    let (exit_tx, exit_rx) = mpsc::sync_channel::<PtyExit>(1);
    let (eof_tx, eof_rx) = mpsc::sync_channel::<()>(1);
    let state = Arc::new(AtomicU8::new(STATE_RUNNING));
    // Set by the reaper the instant `wait` returns. See `reaped` in
    // `control_thread_main`: once the child is reaped its pid belongs to
    // the kernel again, and signalling it is signalling a stranger.
    let reaped = Arc::new(AtomicBool::new(false));

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
            let reaped = Arc::clone(&reaped);
            move || reaper_thread_main(child, &control_tx, &reaped)
        })
        .context("spawning reaper thread")?;

    std::thread::Builder::new()
        .name("pty-control".into())
        .spawn({
            let state = Arc::clone(&state);
            move || {
                control_thread_main(
                    pair.master,
                    pid,
                    killer,
                    &control_rx,
                    &exit_tx,
                    &state,
                    &reaped,
                )
            }
        })
        .context("spawning control thread")?;

    // Every thread is up: the session can now be terminated through its own
    // control path, so the guard stands down.
    orphan_guard.disarm();

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
        // `SyncSender` has no bounded send, so the bound is explicit:
        // retry until the deadline, then give up. The retry interval is
        // short relative to the timeout, so a channel that drains is
        // noticed promptly rather than after a fixed sleep.
        let deadline = Instant::now() + WRITE_ENQUEUE_TIMEOUT;
        let mut pending = bytes.to_vec();
        loop {
            match self.write_tx.try_send(pending) {
                Ok(()) => return Ok(()),
                Err(mpsc::TrySendError::Full(returned)) => {
                    if Instant::now() >= deadline {
                        bail!("the child is not reading its input; try again");
                    }
                    pending = returned;
                    std::thread::sleep(Duration::from_millis(2));
                }
                Err(mpsc::TrySendError::Disconnected(_)) => {
                    bail!("pty writer thread is gone")
                }
            }
        }
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
    // EOF is signalled from a drop guard, not from the end of the loop,
    // so it still fires if `on_output` panics. `on_output` is a
    // caller-supplied closure that runs on this thread: a panic in it
    // unwinds straight past a plain send, and the session would then wait
    // on an EOF that can never arrive -- wedged with no error anywhere,
    // which is the worst shape a failure can take.
    struct EofOnDrop<'a>(&'a SyncSender<()>);
    impl Drop for EofOnDrop<'_> {
        fn drop(&mut self) {
            // Best-effort: if nothing is waiting on EOF anymore there is
            // nothing to do.
            let _ = self.0.try_send(());
        }
    }
    let _eof = EofOnDrop(eof_tx);

    // Heap, not a 64 KiB stack array: the buffer lives for the thread's whole
    // run and there is nothing to gain from the stack.
    let mut buf = vec![0u8; READ_BUFFER_SIZE];
    loop {
        match reader.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => on_output(&buf[..n]),
            // A signal delivered to this thread interrupts the read; it is
            // not the end of anything. Treating it as EOF would retire the
            // reader of a session that is still running, and the session
            // would go silent for good -- still listed, still accepting
            // input, never producing another byte. Retry.
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            // Every other error is terminal, including the `EIO` that a
            // pty master returns once its child is gone. That one *is* the
            // normal end of a session, which is why this arm is not an
            // error path.
            Err(_) => break,
        }
    }
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
    reaped: &AtomicBool,
) {
    let result = child.wait();
    // Published before the event is sent, and deliberately so: the channel
    // send can be delayed by scheduling, and for that whole span the pid is
    // already back in the kernel's pool. A `Terminate` arriving in the gap
    // would signal whatever now owns that number.
    reaped.store(true, Ordering::SeqCst);
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
    reaped: &AtomicBool,
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
                // Step 1: do not signal a pid that is no longer ours. Once
                // `wait` has reaped the child the number is free for the
                // kernel to hand to anything, so a kill aimed at a session
                // that already ended lands on an unrelated process group.
                // The `ChildExited` event is already sent or imminent --
                // waiting for it is both correct and the whole job here.
                let pid = pid_to_signal(pid, reaped.load(Ordering::SeqCst));

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

/// The pid a `Terminate` may signal, or `None` if signalling it would hit
/// a stranger.
///
/// Once `wait` has reaped the child, its pid goes back to the kernel's
/// pool and can be handed to any new process. A terminate racing that is
/// not a lost signal -- it is a `killpg` aimed at an unrelated process
/// group, which is a far worse outcome than doing nothing.
///
/// This does not close the window completely and cannot: the pid is free
/// from the moment `wait` returns, and the flag is set immediately after.
/// What it removes is the wide case -- a session that exited some time ago
/// and was terminated afterwards -- leaving only the few instructions
/// between the two.
fn pid_to_signal(pid: Option<u32>, reaped: bool) -> Option<u32> {
    if reaped {
        return None;
    }
    pid
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

/// Kills a freshly spawned child if `spawn` cannot finish wiring it up.
///
/// Spawning the process and starting the threads that own it are not one
/// atomic act, and everything between them can fail — cloning the master,
/// or any of the four threads under memory or thread pressure. The child is
/// already running by then, and nothing else holds a reference to it: no
/// reaper to collect it, no control thread to signal it, no handle to find
/// it by. It would outlive the daemon as an orphan holding a PTY.
struct OrphanGuard {
    pid: Option<u32>,
    armed: bool,
}

impl OrphanGuard {
    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for OrphanGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        // The whole group, as the termination policy does: the child leads
        // it, so anything it already started goes with it.
        match hard_kill_group(self.pid) {
            Ok(()) => warn!(pid = ?self.pid, "killed a child whose session could not be started"),
            Err(e) => warn!(pid = ?self.pid, error = %e, "killing a half-spawned child failed"),
        }
    }
}

#[cfg(test)]
mod reader_tests {
    use super::*;
    use std::io::ErrorKind;
    use std::sync::mpsc::sync_channel;

    /// Returns each scripted result once, in order, then EOF.
    struct Scripted(std::vec::IntoIter<IoResult<Vec<u8>>>);

    impl Read for Scripted {
        fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
            match self.0.next() {
                None => Ok(0),
                Some(Err(e)) => Err(e),
                Some(Ok(bytes)) => {
                    buf[..bytes.len()].copy_from_slice(&bytes);
                    Ok(bytes.len())
                }
            }
        }
    }

    fn run(script: Vec<IoResult<Vec<u8>>>) -> Vec<u8> {
        let (eof_tx, _eof_rx) = sync_channel(1);
        let mut seen = Vec::new();
        reader_thread_main(
            Box::new(Scripted(script.into_iter())),
            |bytes| seen.extend_from_slice(bytes),
            &eof_tx,
        );
        seen
    }

    /// An interrupted read is not EOF.
    ///
    /// This used to `break` on any error at all. A signal delivered to the
    /// reader thread would retire it while the session was still running,
    /// and that session would go silent permanently -- still listed, still
    /// accepting input, never emitting another byte. Nothing would report
    /// it, because from every other angle the session is healthy.
    #[test]
    fn an_interrupted_read_is_retried_not_treated_as_the_end() {
        let out = run(vec![
            Ok(b"before".to_vec()),
            Err(std::io::Error::from(ErrorKind::Interrupted)),
            Ok(b"after".to_vec()),
        ]);
        assert_eq!(
            out,
            b"beforeafter",
            "output after an interrupted read was lost"
        );
    }

    /// Terminating a session whose child has already been reaped must not
    /// signal anything.
    ///
    /// Deliberately a test of the decision rather than of the race: the
    /// race needs a pid to be reused between two instructions, which
    /// cannot be arranged. What it does guard is the check being deleted
    /// as redundant -- it looks redundant, because the `ChildExited` event
    /// is usually already queued, and "usually" is the whole problem.
    #[test]
    fn a_reaped_child_is_never_signalled() {
        assert_eq!(pid_to_signal(Some(4242), false), Some(4242));
        assert_eq!(
            pid_to_signal(Some(4242), true),
            None,
            "a reaped pid belongs to the kernel again; killpg would hit a stranger"
        );
        assert_eq!(pid_to_signal(None, false), None);
    }

    /// A panic in the caller's closure must still signal EOF.
    ///
    /// `on_output` is caller-supplied and runs on the reader thread, so a
    /// panic in it unwinds past the end of the loop. When the EOF send
    /// lived there, the session was left waiting on a signal that could
    /// never arrive: wedged, with nothing logged and nothing failed --
    /// the worst shape a failure can take, because there is no symptom to
    /// chase.
    #[test]
    fn a_panicking_output_callback_still_signals_eof() {
        let (eof_tx, eof_rx) = sync_channel(1);
        let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            reader_thread_main(
                Box::new(Scripted(vec![Ok(b"boom".to_vec())].into_iter())),
                |_| panic!("the caller's closure panicked"),
                &eof_tx,
            );
        }));
        assert!(panicked.is_err(), "the panic was swallowed, not unwound");
        assert!(
            eof_rx.try_recv().is_ok(),
            "EOF was never signalled, so the session waits on it forever"
        );
    }

    /// The other half of the same decision: a real error still ends the
    /// reader. A pty master returns `EIO` once its child is gone, and that
    /// is the ordinary end of a session -- retrying it would spin forever.
    #[test]
    fn a_real_error_still_ends_the_reader() {
        let out = run(vec![
            Ok(b"before".to_vec()),
            Err(std::io::Error::from_raw_os_error(libc::EIO)),
            Ok(b"never".to_vec()),
        ]);
        assert_eq!(out, b"before", "the reader read past a terminal error");
    }
}

/// The login-shell wrapper (docs/03-pty.md, anchor#10).
///
/// The forms are asserted by *running* them wherever the shell is installed,
/// not by matching the argv strings. A string assertion restates the code it
/// is checking and so passes for any form, correct or not; only an exec
/// proves that `a b` stayed one argument and `*` stayed a star.
#[cfg(test)]
mod login_shell_tests {
    use super::*;

    /// Arguments chosen to break every way a shell can mishandle them: an
    /// embedded space (word splitting), a glob (filename expansion), a
    /// dollar (variable expansion) and a quote (quote removal).
    const TRICKY: [&str; 4] = ["a b", "*", "c$d", "e\"f"];

    /// Runs `program` under `shell` using the wrapper's own argv, and
    /// returns what the program saw as its arguments.
    fn argv_as_seen_by_the_child(shell: &str) -> Option<Vec<String>> {
        if !std::path::Path::new(shell).exists() {
            return None;
        }
        let args: Vec<String> = TRICKY.iter().map(|s| (*s).to_string()).collect();
        // `printf` with a per-argument format repeats for each operand, so
        // the output has one bracketed line per argv entry — which is the
        // split that matters.
        let argv = wrapper_argv(shell, "/usr/bin/printf", &{
            let mut v = vec!["[%s]\n".to_string()];
            v.extend(args);
            v
        });
        let out = std::process::Command::new(shell)
            .args(&argv)
            .output()
            .unwrap_or_else(|e| panic!("running {shell}: {e}"));
        assert!(
            out.status.success(),
            "{shell} rejected the wrapper: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        Some(
            String::from_utf8_lossy(&out.stdout)
                .lines()
                .map(|l| l.trim_start_matches('[').trim_end_matches(']').to_string())
                .collect(),
        )
    }

    /// Every shell installed here must deliver the arguments unsplit,
    /// unglobbed and unexpanded. Shells that are absent are skipped and
    /// named, so a green run never silently means "tested nothing".
    #[test]
    fn installed_shells_pass_arguments_through_untouched() {
        let mut tested = 0;
        for shell in [
            "/bin/sh", "/bin/bash", "/bin/zsh", "/bin/csh", "/bin/tcsh",
            "/usr/bin/fish", "/usr/local/bin/fish", "/opt/homebrew/bin/fish",
        ] {
            let Some(seen) = argv_as_seen_by_the_child(shell) else {
                continue;
            };
            tested += 1;
            assert_eq!(seen, TRICKY, "{shell} mangled its arguments");
        }
        assert!(tested > 0, "no shell to test against; the suite proved nothing");
        eprintln!("login-shell wrapper exercised against {tested} shell(s)");
    }

    /// The csh family is the reason this classification exists: `-l` beside
    /// `-c` is rejected outright, so the csh form must not carry it.
    #[test]
    fn the_csh_form_omits_the_login_flag() {
        for shell in ["/bin/csh", "/bin/tcsh", "/usr/local/bin/tcsh"] {
            assert_eq!(shell_family(shell), ShellFamily::Csh, "{shell}");
            let argv = wrapper_argv(shell, "prog", &[]);
            assert!(
                !argv.contains(&"-l".to_string()),
                "csh rejects -l beside -c: {argv:?}"
            );
        }
    }

    /// fish takes `-l`, so dropping it there would cost the profile the
    /// feature exists to source.
    #[test]
    fn the_fish_form_keeps_the_login_flag() {
        for shell in ["/opt/homebrew/bin/fish", "/usr/bin/fish"] {
            assert_eq!(shell_family(shell), ShellFamily::Fish, "{shell}");
            assert!(
                wrapper_argv(shell, "prog", &[]).contains(&"-l".to_string()),
                "fish supports -l and the profile is the point"
            );
        }
    }

    /// An unrecognised shell gets the POSIX form rather than an error: it is
    /// far likelier to be POSIX-compatible than not, and if it is not, the
    /// spawn fails loudly with the shell's own usage message.
    #[test]
    fn an_unknown_shell_gets_the_posix_form() {
        for shell in ["/bin/sh", "/bin/bash", "/usr/bin/ksh", "/opt/weird/nushell"] {
            assert_eq!(shell_family(shell), ShellFamily::Posix, "{shell}");
        }
        assert_eq!(
            wrapper_argv("/opt/weird/nushell", "prog", &["x".into()]),
            vec!["-l", "-c", r#"exec "$0" "$@""#, "prog", "x"]
        );
    }

    /// Classification is by file name, so where a shell is installed does
    /// not change how it is invoked.
    #[test]
    fn the_install_location_does_not_change_the_family() {
        for shell in ["/usr/bin/fish", "/opt/homebrew/bin/fish", "fish"] {
            assert_eq!(shell_family(shell), ShellFamily::Fish, "{shell}");
        }
    }

    /// A spec that did not ask for a login shell gets no wrapper at all.
    #[test]
    fn no_wrapper_without_the_flag() {
        let spec = SpawnSpec {
            program: "/bin/echo",
            args: &[],
            cwd: std::path::Path::new("/"),
            env: &[],
            cols: 80,
            rows: 24,
            login_shell: false,
        };
        assert!(login_shell_wrapper(&spec).is_none());
    }
}
