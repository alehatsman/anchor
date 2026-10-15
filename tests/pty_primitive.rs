//! PTY integration fixtures -- docs/10-testing.md#1-pty-integration-fixtures-daemontestspty_rs.
//!
//! Unix-focused: this is what could actually be run and verified in this
//! environment. Windows needs the same fixtures re-run on real hardware
//! (docs/10-testing.md#platform-matrix); the two exit-code fixtures are
//! expected to fail there until
//! [W1](../../docs/15-open-questions.md#w1--conpty-children-are-never-observed-as-exited-on-windows)
//! resolves -- not attempted here, tracked instead.

#![cfg(unix)]

use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::time::{Duration, Instant};

use anchor::pty::{self, SpawnSpec, TerminalSession};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

fn temp_dir() -> PathBuf {
    std::env::temp_dir()
}

/// Spawns `/bin/sh -c script` and returns the session handle plus its output
/// channel. `on_output` (docs/pty.rs's reader-thread callback) just forwards
/// chunks into an unbounded `mpsc` channel -- non-blocking `send`, satisfying
/// the "never block the reader" contract.
fn spawn_sh(script: &str, cols: u16, rows: u16) -> (pty::SpawnedSession, Receiver<Vec<u8>>) {
    let (out_tx, out_rx) = mpsc::channel::<Vec<u8>>();
    let cwd = temp_dir();
    let spec = SpawnSpec {
        program: "/bin/sh",
        args: &["-c".to_string(), script.to_string()],
        cwd: &cwd,
        env: &[],
        cols,
        rows,
        login_shell: false,
    };
    let spawned = pty::spawn(&spec, move |chunk| {
        let _ = out_tx.send(chunk.to_vec());
    })
    .expect("spawn /bin/sh");
    (spawned, out_rx)
}

/// Spawns interactive `/bin/sh` (no `-c`) so the test can drive it with
/// multiple `write()`s, same output wiring as `spawn_sh`.
fn spawn_interactive_sh(cols: u16, rows: u16) -> (pty::SpawnedSession, Receiver<Vec<u8>>) {
    let (out_tx, out_rx) = mpsc::channel::<Vec<u8>>();
    let cwd = temp_dir();
    let spec = SpawnSpec {
        program: "/bin/sh",
        args: &[],
        cwd: &cwd,
        env: &[],
        cols,
        rows,
        login_shell: false,
    };
    let spawned = pty::spawn(&spec, move |chunk| {
        let _ = out_tx.send(chunk.to_vec());
    })
    .expect("spawn /bin/sh");
    (spawned, out_rx)
}

/// Accumulates chunks from `rx` until `pred(&acc)` is true, or panics once
/// `rx` has gone `stall` without delivering anything.
///
/// **The budget is per-silence, not total** (anchor#11). These tests drive a
/// real `/bin/sh`, and a total-elapsed deadline measures how fast this
/// machine happens to be rather than whether the PTY layer works — under
/// load the producer is simply still producing when the clock runs out, and
/// the suite reports a bug that is not there. A reader that has genuinely
/// stopped still fails within `stall`, which is the condition these tests
/// are about.
fn recv_until(
    rx: &Receiver<Vec<u8>>,
    stall: Duration,
    mut pred: impl FnMut(&[u8]) -> bool,
) -> Vec<u8> {
    let mut acc = Vec::new();
    loop {
        match rx.recv_timeout(stall) {
            Ok(chunk) => {
                acc.extend_from_slice(&chunk);
                if pred(&acc) {
                    return acc;
                }
            }
            Err(RecvTimeoutError::Timeout) => panic!(
                "no output for {stall:?}; the predicate never matched. {}",
                tail(&acc)
            ),
            Err(RecvTimeoutError::Disconnected) => panic!(
                "output channel closed before the predicate matched. {}",
                tail(&acc)
            ),
        }
    }
}

/// The end of what was collected, for a failure message.
///
/// Bounded deliberately: these tests move megabytes, and a panic that dumps
/// all of it buries the one line that says what went wrong under a wall of
/// output no one scrolls back through.
fn tail(acc: &[u8]) -> String {
    const TAIL: usize = 256;
    let start = acc.len().saturating_sub(TAIL);
    format!(
        "Got {} bytes, ending {:?}",
        acc.len(),
        String::from_utf8_lossy(&acc[start..])
    )
}

fn contains(acc: &[u8], needle: &str) -> bool {
    acc.windows(needle.len()).any(|w| w == needle.as_bytes())
}

#[test]
fn echo_roundtrip() {
    let (spawned, out_rx) = spawn_interactive_sh(24, 80);
    spawned.session.write(b"echo hello\n").unwrap();
    recv_until(&out_rx, DEFAULT_TIMEOUT, |acc| contains(acc, "hello"));
}

#[test]
fn large_write_arrives_intact_and_in_order() {
    // Raw mode: a byte-exact payload can (and here, does) contain bytes that
    // cooked-mode termios treats as control characters (0x04 = EOF, 0x03 =
    // SIGINT, ...), not literal data -- that's line-discipline policy, not
    // pty.rs's job to work around (see the module doc and
    // docs/03-pty.md#spawn). Raw mode also drops ONLCR, so no
    // \n -> \r\n surprises on the way back either. Same lesson the spike
    // already learned the hard way for S3
    // (docs/15-open-questions.md#s3--a-blocking-write-wedges-terminate).
    let (spawned, out_rx) = spawn_sh("stty raw -echo; printf READY; cat", 24, 80);

    // `stty` runs, then the sentinel, then `cat` execs -- all sequential in
    // the same shell script. Seeing "READY" come back proves raw mode is
    // already active before we write; without this sync point the payload
    // below can race the write against `stty` and land while the pty is
    // still in cooked mode, so the driver mangles it (echo, erase, EOF char
    // 0x04, ...) instead of passing it through untouched.
    recv_until(&out_rx, DEFAULT_TIMEOUT, |acc| contains(acc, "READY"));

    let payload: Vec<u8> = (0..1024 * 1024)
        .map(|i| u8::try_from(i % 256).expect("i % 256 is always in 0..256"))
        .collect();
    spawned.session.write(&payload).unwrap();

    let got = recv_until(&out_rx, DEFAULT_TIMEOUT, |acc| acc.len() >= payload.len());
    assert_eq!(
        &got[..payload.len()],
        &payload[..],
        "cat's echo must match byte-for-byte, in order"
    );
}

#[test]
fn large_burst_read_drops_nothing() {
    const N: usize = 8 * 1024 * 1024; // 8 MiB -- doc says 100 MiB from `yes`;
                                      // scaled down so the fixture stays fast without changing what it proves
                                      // (throughput without loss/stall). Content is `yes`'s repeating "y\n".
                                      // Raw mode so ONLCR doesn't inflate every "y\n" into "y\r\n" -- see the
                                      // note on large_write_arrives_intact_and_in_order above.
    let (_spawned, out_rx) = spawn_sh(&format!("stty raw -echo; yes | head -c {N}"), 24, 80);

    // DEFAULT_TIMEOUT, not the longer budget this used to need: the wait is
    // per-silence now, and a burst this size is many thousands of chunks. It
    // never goes 10s without one unless the reader has actually stopped.
    let got = recv_until(&out_rx, DEFAULT_TIMEOUT, |acc| acc.len() >= N);
    assert_eq!(got.len(), N, "must not drop or duplicate bytes");
    assert!(
        got.iter().all(|&b| b == b'y' || b == b'\n'),
        "content must be exactly yes's output, undamaged"
    );
}

#[test]
fn resize_is_observed_by_the_child() {
    let (spawned, out_rx) = spawn_interactive_sh(24, 80);
    spawned.session.resize(120, 40).unwrap();
    // resize() hands off to the control thread and returns immediately
    // (docs/03-pty.md#resize: "short and non-blocking") -- it carries
    // no ack, so give it a moment to land before asking the child to query
    // its size, rather than racing the control thread against the writer
    // thread on two independent channels.
    std::thread::sleep(Duration::from_millis(200));
    spawned.session.write(b"stty size\n").unwrap();
    let acc = recv_until(&out_rx, DEFAULT_TIMEOUT, |acc| contains(acc, "40 120"));
    assert!(
        contains(&acc, "40 120"),
        "stty size should report the resized rows/cols"
    );
}

#[test]
fn clean_exit_zero_is_recorded_via_wait_not_eof() {
    let (spawned, _out_rx) = spawn_sh("exit 0", 24, 80);
    let exit = spawned
        .exit_rx
        .recv_timeout(DEFAULT_TIMEOUT)
        .expect("exit_rx should fire");
    let status = exit
        .status
        .expect("clean exit must carry a status, not a lost_reason");
    assert_eq!(status.exit_code(), 0);
    assert!(status.success());
    assert!(exit.lost_reason.is_none());
}

#[test]
fn nonzero_exit_is_recorded_accurately() {
    let (spawned, _out_rx) = spawn_sh("exit 7", 24, 80);
    let exit = spawned
        .exit_rx
        .recv_timeout(DEFAULT_TIMEOUT)
        .expect("exit_rx should fire");
    let status = exit.status.expect("exit must carry a status");
    assert_eq!(status.exit_code(), 7);
    assert!(!status.success());
}

// The next two fixtures are the same question -- "does a detached grandchild
// keep the pty's master open past the direct child's exit?" -- with two
// different, *correct* answers, because Linux and macOS genuinely differ here.
// Split by `target_os` for that reason, not because macOS is flaky. This
// resolves [S5](../../docs/15-open-questions.md#s5--a-detached-grandchild-cannot-hold-a-pty-past-its-parent-on-macos),
// which was previously a `#[cfg(target_os = "linux")]` gate with an unverified
// hypothesis attached.
//
// Both kernels tear down a session leader's controlling terminal when it
// exits. Only one of them exempts ptys:
//
//   Linux -- drivers/tty/tty_jobctrl.c, `disassociate_ctty()`:
//       if (on_exit && tty->driver->type != TTY_DRIVER_TYPE_PTY)
//               tty_vhangup_session(tty);
//       else { ... kill_pgrp(tty_pgrp, SIGHUP, on_exit); ... }
//     PTYs take the `else` branch: the foreground process group just gets
//     SIGHUP. A grandchild that ignores SIGHUP survives it still holding its
//     inherited slave fd, so the master stays open and EOF lags exit by
//     however long that grandchild lives.
//
//   macOS/XNU -- bsd/kern/kern_exit.c, `proc_exit()`, under the comment
//   "Controlling process. Signal foreground pgrp, drain controlling terminal
//   and revoke access to controlling terminal.":
//       pgsignal(tpgrp, SIGHUP, 1);
//       ...
//       VNOP_REVOKE(ttyvp, REVOKEALL, &context);
//     No pty exemption, and `REVOKEALL` is not a signal -- it invalidates
//     every descriptor pointing at that tty, in every process, whatever their
//     signal disposition. The master sees EOF the instant the session leader
//     exits.
//
// Verified on real hardware (Darwin 24.6.0, arm64) rather than inferred, via
// `spike/src/bin/s10_ctty_revoke.rs`: after the direct child's `exit 0` the
// detached grandchild is still *alive* -- `kill(pid, 0)` succeeds for seconds
// afterwards and it runs to completion -- but every write to its inherited
// pty fd fails with `EIO` from the very first attempt, and `open("/dev/tty")`
// fails with `ENXIO`. A fresh `open()` of the same `/dev/ttysNNN` path still
// succeeds, so the device is fine and it is specifically the *descriptors*
// that were revoked; the master is already permanently at EOF by then, so
// nothing written through that reopened fd can ever be read. That answers
// S5's open question in the negative: there is no userspace recipe on macOS
// for "outlive the direct parent while still holding the pty". The same
// spike's `noctty` control -- identical setup minus `setsid` + `TIOCSCTTY` --
// behaves exactly like Linux, which pins the mechanism to controlling-terminal
// teardown specifically rather than to fd accounting or to SIGHUP.
//
// This costs the product nothing: XNU drains the tty before revoking it, so
// no output is lost on the way out. Measured at 208894/208894 bytes of a
// 20k-line burst, both with a prompt reader and with one deliberately stalled
// a full second before it started reading.
#[test]
#[cfg(target_os = "linux")]
fn eof_and_exit_are_independent_signals() {
    // The direct child exits almost immediately; a backgrounded grandchild
    // (ignoring SIGHUP) keeps the pty's slave side open for a few seconds
    // after. wait() must not be blocked on that, and EOF must not arrive
    // early just because the child already exited
    // (docs/15-open-questions.md#s2--eof-is-not-exit, S2's verified recipe).
    //
    // SIGHUP ignored *before* the fork races the parent's `exit 0` (S2's own
    // finding) is what the grandchild actually needs: fork inherits signal
    // disposition, and an ignored signal survives `exec` too (POSIX), so
    // plain `trap '' HUP` on the parent covers a backgrounded `sleep` with no
    // exec of its own -- no interpreter, no external `setsid` binary needed.
    // Verified locally (`pty::fork` + `trap '' HUP; sleep 2 & echo $!` on
    // Linux): direct child exits at ~1ms, EOF at ~2000ms, matching the
    // grandchild's `sleep`. This is the Linux-only half of what was, until
    // S5, a cross-platform fixture.
    //
    // `echo`ing the grandchild's pid (`$!`, the backgrounding shell's own
    // idiom -- same as `terminate_kills_the_grandchild_process_tree` below)
    // and probing it with `kill(pid, 0)` turns "it actually started and is
    // running" into a fact about process state instead of a latency guess.
    //
    // Deliberately not probing `kill(pid, 0)` for "dead" *after* EOF fires:
    // once reparented off its exited direct parent, this pid is reaped by
    // whatever ancestor becomes its subreaper (usually init), not by us, so
    // it may sit as a zombie for a while -- still `kill`-able, not actually
    // running. That would make a "must be dead now" assertion its own flake.
    let (spawned, out_rx) = spawn_sh(
        "trap '' HUP; sleep 2 & pid=$!; echo GRANDCHILD_PID:$pid; exit 0",
        24,
        80,
    );

    let t0 = Instant::now();
    let exit = spawned
        .exit_rx
        .recv_timeout(DEFAULT_TIMEOUT)
        .expect("exit_rx should fire promptly");
    let exit_latency = t0.elapsed();
    assert!(
        exit_latency < Duration::from_millis(500),
        "wait() should not wait on the grandchild: took {exit_latency:?}"
    );
    assert_eq!(exit.status.unwrap().exit_code(), 0);

    let acc = recv_until(&out_rx, DEFAULT_TIMEOUT, |acc| {
        contains(acc, "GRANDCHILD_PID:")
    });
    let text = String::from_utf8_lossy(&acc);
    let pid: i32 = text
        .split("GRANDCHILD_PID:")
        .nth(1)
        .and_then(|rest| rest.split_whitespace().next())
        .and_then(|s| s.parse().ok())
        .expect("should have parsed the detached grandchild's pid from output");

    // SAFETY: kill(pid, 0) is a pure liveness probe, sends no signal.
    let rc = unsafe { libc::kill(pid, 0) };
    assert_eq!(rc, 0, "grandchild should still be alive right after exit_rx fired -- it should be mid-`sleep 2`, not gone already");

    // EOF should arrive later, once the grandchild's sleep ends and it exits too.
    match spawned.eof_rx.recv_timeout(Duration::from_secs(5)) {
        Ok(()) => {
            let eof_latency = t0.elapsed();
            assert!(
                eof_latency >= Duration::from_millis(1500),
                "EOF arrived suspiciously early: {eof_latency:?}"
            );
        }
        Err(e) => panic!("EOF never arrived within 5s of the grandchild's sleep ending ({e})"),
    }
}

/// macOS's half of the split documented above: `exit_rx` and `eof_rx` are
/// still two independent channels, but a `REVOKEALL` on the controlling tty
/// means EOF *coincides* with the session leader's exit instead of lagging it.
///
/// The grandchild-liveness assertion is the load-bearing one. It is what
/// separates "the kernel revoked a descriptor out from under a healthy
/// process" (the actual behaviour) from "the grandchild died and its fd
/// closed the ordinary way" -- the two hypotheses S5 could not distinguish
/// without hardware -- and it is what would fail the day either kernel
/// changes its mind.
#[test]
#[cfg(target_os = "macos")]
fn eof_follows_the_session_leaders_exit_even_with_a_live_grandchild() {
    let (spawned, out_rx) = spawn_sh(
        "trap '' HUP; sleep 5 & pid=$!; echo GRANDCHILD_PID:$pid; exit 0",
        24,
        80,
    );

    let t0 = Instant::now();
    let exit = spawned
        .exit_rx
        .recv_timeout(DEFAULT_TIMEOUT)
        .expect("exit_rx should fire promptly");
    let exit_latency = t0.elapsed();
    assert!(
        exit_latency < Duration::from_millis(500),
        "wait() should not wait on the grandchild: took {exit_latency:?}"
    );
    assert_eq!(exit.status.unwrap().exit_code(), 0);

    // The pid still reaches us despite the revoke: XNU drains the tty before
    // revoking it, so the direct child's output is never truncated.
    let acc = recv_until(&out_rx, DEFAULT_TIMEOUT, |acc| {
        contains(acc, "GRANDCHILD_PID:")
    });
    let text = String::from_utf8_lossy(&acc);
    let pid: i32 = text
        .split("GRANDCHILD_PID:")
        .nth(1)
        .and_then(|rest| rest.split_whitespace().next())
        .and_then(|s| s.parse().ok())
        .expect("should have parsed the detached grandchild's pid from output");

    // EOF promptly, *not* at the end of the grandchild's `sleep 5`: the revoke
    // is what ends the stream here, not the last slave fd being closed.
    spawned
        .eof_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("EOF should follow the session leader's exit, well inside the grandchild's sleep");
    let eof_latency = t0.elapsed();
    assert!(
        eof_latency < Duration::from_secs(2),
        "EOF should coincide with the session leader's exit on macOS, not lag it: {eof_latency:?}"
    );

    // SAFETY: kill(pid, 0) is a pure liveness probe, sends no signal.
    let rc = unsafe { libc::kill(pid, 0) };
    assert_eq!(
        rc, 0,
        "the grandchild must still be alive after EOF -- macOS revoked its pty \
         descriptor, it did not kill the process (docs/15-open-questions.md#s5)"
    );
}

#[test]
fn terminate_reaches_exited_within_the_bounded_policy() {
    let (spawned, _out_rx) = spawn_sh("sleep 30", 24, 80);

    let t0 = Instant::now();
    spawned
        .session
        .terminate()
        .expect("terminate should not error");
    let terminate_latency = t0.elapsed();

    // SIGTERM should kill a plain `sleep` near-instantly -- well under the
    // full 5s+2s bounded policy (docs/03-pty.md#concrete-policy).
    assert!(
        terminate_latency < Duration::from_secs(2),
        "terminate() took {terminate_latency:?}, expected a fast SIGTERM kill"
    );

    let exit = spawned
        .exit_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("exit_rx should already have fired by the time terminate() returns");
    assert!(
        !exit.status.is_some_and(|s| s.success()),
        "a signal-killed sleep should not report success"
    );
}

/// Issue #49: a child that ignores both graceful signals must still die by
/// step 4's hard kill (docs/03-pty.md#concrete-policy), not ride out
/// `KILL_WAIT` and leak as a live process behind an `exited` row.
#[test]
fn terminate_hard_kills_a_child_that_ignores_hup_and_term() {
    let (spawned, out_rx) = spawn_sh(
        "trap '' HUP TERM; echo TRAPPED; while :; do sleep 1; done",
        24,
        80,
    );
    recv_until(&out_rx, DEFAULT_TIMEOUT, |acc| contains(acc, "TRAPPED"));
    let pid = spawned
        .pid
        .and_then(|p| libc::pid_t::try_from(p).ok())
        .expect("spawned session should report its pid");

    spawned
        .session
        .terminate()
        .expect("terminate should not error");

    let exit = spawned
        .exit_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("exit_rx should already have fired by the time terminate() returns");
    assert!(
        exit.lost_reason.is_none(),
        "hard kill should produce an observed exit, got lost_reason {:?}",
        exit.lost_reason
    );
    assert!(
        !exit.status.is_some_and(|s| s.success()),
        "a SIGKILLed shell should not report success"
    );

    // SAFETY: kill(pid, 0) is a pure liveness probe, sends no signal.
    let rc = unsafe { libc::kill(pid, 0) };
    assert!(
        rc == -1 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH),
        "session pid {pid} is still alive after terminate"
    );
}

#[test]
fn terminate_under_output_load_does_not_deadlock() {
    let (spawned, out_rx) = spawn_sh("yes", 24, 80);

    // Let it produce a real burst before terminating, so terminate() is
    // racing an active reader, not an idle one.
    recv_until(&out_rx, DEFAULT_TIMEOUT, |acc| acc.len() >= 256 * 1024);

    let t0 = Instant::now();
    spawned
        .session
        .terminate()
        .expect("terminate should not error");
    assert!(
        t0.elapsed() < Duration::from_secs(8),
        "terminate() must not deadlock behind the writer/reader threads"
    );

    spawned
        .exit_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("exit_rx should fire once terminate() returns");

    // The reader thread should still be able to drain and reach EOF -- no
    // lost tail, no stuck reader.
    match spawned.eof_rx.recv_timeout(Duration::from_secs(5)) {
        Ok(()) => {}
        Err(e) => panic!("reader never reached EOF after terminate ({e})"),
    }
}

#[test]
fn terminate_kills_the_grandchild_process_tree() {
    let (spawned, out_rx) = spawn_sh("sleep 30 & echo GRANDCHILD_PID:$!; wait", 24, 80);

    let acc = recv_until(&out_rx, DEFAULT_TIMEOUT, |acc| {
        contains(acc, "GRANDCHILD_PID:")
    });
    let text = String::from_utf8_lossy(&acc);
    let pid: i32 = text
        .split("GRANDCHILD_PID:")
        .nth(1)
        .and_then(|rest| rest.split_whitespace().next())
        .and_then(|s| s.parse().ok())
        .expect("should have parsed the grandchild pid from output");

    // SAFETY: kill(pid, 0) is a pure liveness probe, sends no signal.
    let rc = unsafe { libc::kill(pid, 0) };
    assert_eq!(rc, 0, "grandchild should be alive before terminate");

    spawned
        .session
        .terminate()
        .expect("terminate should not error");

    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        // SAFETY: same liveness probe.
        if unsafe { libc::kill(pid, 0) } == -1
            && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
        {
            return; // gone -- the whole tree was killed via killpg, not just the shell
        }
        assert!(
            Instant::now() <= deadline,
            "grandchild pid {pid} was still alive {:?} after terminate",
            deadline.elapsed()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}
