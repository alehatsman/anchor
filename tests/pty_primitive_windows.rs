//! The Windows twin of `pty_primitive.rs`, lifted from teleport's
//! `daemon/tests/pty_primitive_windows.rs` along with the ConPTY code it
//! defends (docs/09-windows.md §1).
//!
//! Every fixture spawns a real ConPTY child through the production
//! `pty::spawn` and drives it for real: `cmd.exe` and `powershell.exe`, no
//! `/bin/sh`, no `stty`, no `libc::kill`. Run on a real Windows 11 box
//! (build 26200) before it was committed, not guessed at from the Unix file.
//!
//! **Two things changed on the way over, both in anchor's direction.**
//! teleport's waits are total-elapsed budgets (a 30s burst precondition, a
//! 60s exit for a 1.4 MB loop); here every wait on a producer resets
//! whenever output moves and fails only on an unbroken stall
//! (docs/08-testing.md, "Wait on the condition, not on the clock"). And the
//! clean-exit fixture also checks the exit is published *once*: the
//! property anchor is opinionated about (docs/01-scope.md).
//!
//! **One Unix fixture is deliberately not attempted, not faked:**
//! `large_write_arrives_intact_and_in_order`. ConPTY has no raw input mode;
//! every byte written to the master crosses conhost's VT input parser
//! whatever console mode the child sets, so a payload containing `ESC` every
//! 256 bytes is consumed as control sequences by construction.
//! `large_text_burst_read_drops_nothing` and `echo_roundtrip` cover what the
//! platform does allow: ordinary text through the same reader and writer, at
//! real volume.
//!
//! **Serialized**, on a lock rather than `--test-threads=1`. Each fixture is
//! a real conhost; GitHub's `windows-latest` has two vCPUs, and teleport
//! measured its burst precondition starving there when eight ran at once.

#![cfg(windows)]

use std::path::PathBuf;
use std::process::Command as StdCommand;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::time::{Duration, Instant};

use anchor::pty::{self, SpawnSpec, TerminalSession};

/// How long a fixture may see *no* output before it gives up. A stall
/// bound, not a budget: any progress resets it (docs/08-testing.md).
const STALL: Duration = Duration::from_secs(10);

/// GRACEFUL_WAIT (5s) + KILL_WAIT (2s) is the documented terminate policy
/// (docs/03-pty.md#termination); the extra second is scheduling slop around
/// the boundary. A policy bound, so it does not expire with hardware.
const TERMINATE_POLICY: Duration = Duration::from_secs(5 + 2 + 1);

/// One real conhost at a time. A poisoned lock is taken anyway: one fixture
/// failing must not turn into every later one failing confusingly.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn serialize() -> std::sync::MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn temp_dir() -> PathBuf {
    std::env::temp_dir()
}

fn spawn(
    program: &str,
    args: &[String],
    cols: u16,
    rows: u16,
) -> (pty::SpawnedSession, Receiver<Vec<u8>>) {
    spawn_in(program, args, &temp_dir(), cols, rows)
}

fn spawn_in(
    program: &str,
    args: &[String],
    cwd: &std::path::Path,
    cols: u16,
    rows: u16,
) -> (pty::SpawnedSession, Receiver<Vec<u8>>) {
    let (out_tx, out_rx) = mpsc::channel::<Vec<u8>>();
    let spec = SpawnSpec {
        program,
        args,
        cwd,
        env: &[],
        cols,
        rows,
        login_shell: false,
    };
    let spawned = pty::spawn(&spec, move |chunk| {
        // The test's receiver may already be gone; nothing to do then.
        let _ = out_tx.send(chunk.to_vec());
    })
    .unwrap_or_else(|e| panic!("spawning {program}: {e:#}"));
    (spawned, out_rx)
}

/// `cmd.exe /c script`: a one-shot script, not an interactive shell.
fn spawn_cmd_script(script: &str) -> (pty::SpawnedSession, Receiver<Vec<u8>>) {
    spawn("cmd.exe", &["/c".to_string(), script.to_string()], 80, 24)
}

/// Interactive `cmd.exe`, driven by `write()`.
fn spawn_interactive_cmd(cols: u16, rows: u16) -> (pty::SpawnedSession, Receiver<Vec<u8>>) {
    spawn("cmd.exe", &[], cols, rows)
}

/// Accumulates output until `pred` holds, failing only when output stops
/// arriving for `STALL`. Same contract as `pty_primitive.rs`'s helper.
fn recv_until(rx: &Receiver<Vec<u8>>, mut pred: impl FnMut(&[u8]) -> bool) -> Vec<u8> {
    let mut acc = Vec::new();
    loop {
        match rx.recv_timeout(STALL) {
            Ok(chunk) => {
                acc.extend_from_slice(&chunk);
                if pred(&acc) {
                    return acc;
                }
            }
            Err(RecvTimeoutError::Timeout) => panic!(
                "no output for {STALL:?}; the predicate never matched. {}",
                tail(&acc)
            ),
            Err(RecvTimeoutError::Disconnected) => panic!(
                "output channel closed before the predicate matched. {}",
                tail(&acc)
            ),
        }
    }
}

/// Collects output until the child's exit is published, failing only if
/// output stalls for `STALL` with no exit. The exit, not EOF, ends it: the
/// two are independent signals and this waits on the one it names.
fn collect_until_exit(
    spawned: &pty::SpawnedSession,
    rx: &Receiver<Vec<u8>>,
) -> (pty::PtyExit, Vec<u8>) {
    let mut acc = Vec::new();
    let mut last_progress = Instant::now();
    loop {
        if let Ok(exit) = spawned.exit_rx.try_recv() {
            acc.extend(rx.try_iter().flatten());
            return (exit, acc);
        }
        match rx.recv_timeout(Duration::from_millis(50)) {
            Ok(chunk) => {
                acc.extend_from_slice(&chunk);
                last_progress = Instant::now();
            }
            Err(RecvTimeoutError::Timeout) => assert!(
                last_progress.elapsed() < STALL,
                "no output and no exit for {STALL:?}. {}",
                tail(&acc)
            ),
            // The reader is done; the exit may still be in flight.
            Err(RecvTimeoutError::Disconnected) => {
                let exit = spawned
                    .exit_rx
                    .recv_timeout(STALL)
                    .expect("the reader reached EOF and no exit was ever published");
                return (exit, acc);
            }
        }
    }
}

/// The end of what was collected, for a failure message. Bounded: these
/// fixtures move megabytes.
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

/// Removes the VT sequences conhost wraps a session's own output in -- CSI
/// (`ESC [` ... final byte) and OSC (`ESC ]` ... BEL) -- and nothing else.
/// Not a VT parser: just enough to see through conhost's session-init burst
/// and its process-exit teardown, which is all these fixtures meet.
fn strip_vt_sequences(input: &[u8]) -> Vec<u8> {
    const ESC: u8 = 0x1B;
    let mut out = Vec::with_capacity(input.len());
    let mut i = 0;
    while let Some(&b) = input.get(i) {
        let Some(&next) = input.get(i + 1) else {
            out.push(b);
            i += 1;
            continue;
        };
        if b != ESC {
            out.push(b);
            i += 1;
            continue;
        }
        match next {
            b'[' => {
                let mut j = i + 2;
                while input.get(j).is_some_and(|c| (0x30..=0x3F).contains(c)) {
                    j += 1;
                }
                while input.get(j).is_some_and(|c| (0x20..=0x2F).contains(c)) {
                    j += 1;
                }
                i = (j + 1).min(input.len());
            }
            b']' => {
                let mut j = i + 2;
                while input.get(j).is_some_and(|c| *c != 0x07) {
                    j += 1;
                }
                i = (j + 1).min(input.len());
            }
            _ => i += 2,
        }
    }
    out
}

/// The first run of digits after `label`, so `mode con`'s `Lines:` and
/// `Columns:` read without depending on its column alignment.
fn number_after(text: &str, label: &str) -> Option<u32> {
    let start = text.find(label)? + label.len();
    let rest = text.get(start..)?;
    let digits_start = rest.find(|c: char| c.is_ascii_digit())?;
    let digits_end = rest
        .get(digits_start..)?
        .find(|c: char| !c.is_ascii_digit())
        .map_or(rest.len(), |i| digits_start + i);
    rest.get(digits_start..digits_end)?.parse().ok()
}

/// Process ids whose command line contains `marker`, by an out-of-band OS
/// query rather than anything read back through the pty.
///
/// The marker travels in the query process's *environment*, not its script:
/// a query whose own command line contains the marker matches itself, every
/// time (found the hard way in teleport). `Win32_Process.CommandLine` is
/// argv, so the environment sidesteps it. `wmic` is gone from this build;
/// `Get-CimInstance` is its replacement and ships on every Windows 10/11.
fn pids_matching(marker: &str) -> Vec<u32> {
    let out = StdCommand::new("powershell.exe")
        .args([
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            "Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -like ('*' + $env:ANCHOR_MARKER + '*') } | Select-Object -ExpandProperty ProcessId",
        ])
        .env("ANCHOR_MARKER", marker)
        .output()
        .expect("running the out-of-band powershell probe");
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(|l| l.trim().parse().ok())
        .collect()
}

#[test]
fn echo_roundtrip() {
    let _serial = serialize();
    let (spawned, out_rx) = spawn_interactive_cmd(80, 24);
    spawned.session.write(b"echo hello\r\n").unwrap();
    recv_until(&out_rx, |acc| contains(acc, "hello"));
}

/// The child starts in the directory the spec names, not the daemon's.
/// `portable-pty` on Windows falls back to `USERPROFILE` when the cwd is not
/// a directory, so this also pins that a real one is honoured.
#[test]
fn the_child_runs_in_the_requested_directory() {
    let _serial = serialize();
    let dir = tempfile::tempdir().unwrap();
    let canonical = dir.path().canonicalize().unwrap();
    let (spawned, out_rx) = spawn_in(
        "cmd.exe",
        &["/c".to_string(), "cd".to_string()],
        dir.path(),
        200,
        24,
    );
    let (_exit, out) = collect_until_exit(&spawned, &out_rx);
    let text = String::from_utf8_lossy(&strip_vt_sequences(&out)).to_lowercase();
    // `canonicalize` adds the `\\?\` verbatim prefix that `cd` never prints.
    let want = canonical
        .to_string_lossy()
        .trim_start_matches(r"\\?\")
        .to_lowercase();
    assert!(
        text.contains(&want),
        "the child did not start in {want}: {text:?}"
    );
}

#[test]
fn large_text_burst_read_drops_nothing() {
    const LINES: usize = 200_000;
    // No `yes`, and no raw mode (see the module doc): a `for /L` loop is
    // cmd.exe's own repeat primitive, and CRLF is what Windows text output
    // is. ~1.4 MB is dozens of 64 KiB reads.
    //
    // No sentinels, deliberately (teleport found both ways they fail):
    // bracketing the loop with `echo READY`/`echo DONE` made cmd ~15x slower
    // per iteration, and whichever write is last before exit races conhost's
    // own teardown sequence. Stripping VT sequences instead leaves exactly
    // the loop's output with nothing to skip past.
    let _serial = serialize();
    let (spawned, out_rx) = spawn_cmd_script(&format!("for /L %i in (1,1,{LINES}) do @echo y"));

    let (exit, raw) = collect_until_exit(&spawned, &out_rx);
    assert_eq!(
        exit.status.map(|s| s.exit_code()),
        Some(0),
        "the loop failed"
    );
    spawned
        .eof_rx
        .recv_timeout(STALL)
        .expect("the reader should reach EOF once cmd.exe exits");
    let mut raw = raw;
    raw.extend(out_rx.try_iter().flatten());

    let stripped = strip_vt_sequences(&raw);
    let expected: Vec<u8> = b"y\r\n".repeat(LINES);
    assert!(
        stripped == expected,
        "once conhost's own sequences are stripped, the output must be exactly the \
         loop's: {} bytes expected, {} got. {}",
        expected.len(),
        stripped.len(),
        tail(&stripped)
    );
}

#[test]
fn resize_is_observed_by_the_child() {
    let _serial = serialize();
    let (spawned, out_rx) = spawn_interactive_cmd(80, 24);
    spawned.session.resize(120, 40).unwrap();
    // `resize` hands off to the control thread with no ack, so ask until the
    // child sees it rather than sleeping a guess. Bounded by rounds, which
    // is a convergence bound and does not expire with hardware.
    for _ in 0..50 {
        spawned.session.write(b"mode con\r\n").unwrap();
        let acc = recv_until(&out_rx, |acc| contains(acc, "Code page:"));
        let text = String::from_utf8_lossy(&acc);
        if number_after(&text, "Lines:") == Some(40) && number_after(&text, "Columns:") == Some(120)
        {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("the child never saw the resize to 120x40");
}

/// The exit is observed off `wait()`, and published exactly once.
///
/// This is the case `#W1` found unobserved: without `ConptyDsrProbe` the
/// child exits and conhost, still mid-handshake, never tears down.
#[test]
fn clean_exit_zero_is_recorded_via_wait_not_eof() {
    let _serial = serialize();
    let (spawned, _out_rx) = spawn_cmd_script("exit 0");
    let exit = spawned
        .exit_rx
        .recv_timeout(STALL)
        .expect("no exit was published: this is exactly what #W1 was");
    let status = exit.status.expect("a clean exit carries a status");
    assert_eq!(status.exit_code(), 0);
    assert!(status.success());
    assert!(exit.lost_reason.is_none());
    // Once, not once-or-more: the control thread is gone after publishing,
    // so the channel disconnects rather than delivering a second exit.
    assert!(
        matches!(
            spawned.exit_rx.recv_timeout(STALL),
            Err(RecvTimeoutError::Disconnected)
        ),
        "a second exit was published, or the publisher never let go"
    );
}

#[test]
fn nonzero_exit_is_recorded_accurately() {
    let _serial = serialize();
    let (spawned, _out_rx) = spawn_cmd_script("exit 7");
    let exit = spawned
        .exit_rx
        .recv_timeout(STALL)
        .expect("exit_rx should fire");
    let status = exit.status.expect("exit must carry a status");
    assert_eq!(status.exit_code(), 7);
    assert!(!status.success());
    assert_eq!(pty::exit_code(&status), 7);
}

/// An exit code above `i32::MAX` survives into the session's record.
///
/// An `NTSTATUS` is the common case, not a corner: it is what a child torn
/// down with its console reports. The Unix conversion would have recorded
/// it as `-1`, an exit code that is not the one the child exited with.
#[test]
fn an_exit_code_past_i32_max_is_recorded_exactly() {
    let _serial = serialize();
    // 0xC000013A, STATUS_CONTROL_C_EXIT, as cmd spells an exit code.
    let (spawned, _out_rx) = spawn_cmd_script("exit -1073741510");
    let exit = spawned
        .exit_rx
        .recv_timeout(STALL)
        .expect("exit_rx should fire");
    let status = exit.status.expect("exit must carry a status");
    assert_eq!(status.exit_code(), 0xC000_013A);
    assert_eq!(
        pty::exit_code(&status) as u32,
        0xC000_013A,
        "the recorded code must round-trip to the status the child exited with"
    );
}

#[test]
fn terminate_reaches_exited_within_the_bounded_policy() {
    let _serial = serialize();
    // A direct child blocked well past any bound here, on `ping`'s own
    // countdown rather than a background job (that is the grandchild case).
    let (spawned, _out_rx) = spawn_cmd_script("ping -n 31 127.0.0.1 >nul");

    let t0 = Instant::now();
    spawned
        .session
        .terminate()
        .expect("terminate should not error");
    let latency = t0.elapsed();
    // The documented policy, not the usual fast path. Alone this completes
    // in milliseconds; teleport once caught it falling through to the hard
    // kill under concurrent teardowns, which the policy allows.
    assert!(
        latency < TERMINATE_POLICY,
        "terminate() took {latency:?}, past the bounded policy"
    );

    let exit = spawned
        .exit_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("the exit is published before terminate() returns");
    assert!(
        !exit.status.as_ref().is_some_and(|s| s.success()),
        "a forcibly closed ping must not report success"
    );
    eprintln!(
        "terminate took {latency:?}; exit {:?}",
        exit.status.map(|s| s.exit_code())
    );
}

/// The graceful step is what ends the child: closing the pseudoconsole, not
/// the hard kill five seconds later.
///
/// `terminate_reaches_exited_within_the_bounded_policy` cannot tell them
/// apart, and this was found by checking: with the master's drop removed
/// from the graceful step, every other fixture here stayed green, because
/// the hard kill ends the child at 5s, the policy allows 7s, and the
/// control thread drops the master on its way out anyway. The exit code is
/// what differs. The hard kill reports `1`; a child whose console was
/// closed under it reports an `NTSTATUS` -- `0xC0000142` for `ping` under
/// `cmd /c` on build 26200, in ~2ms.
#[test]
fn terminate_closes_the_pseudoconsole_rather_than_hard_killing() {
    let _serial = serialize();
    let (spawned, _out_rx) = spawn_cmd_script("ping -n 31 127.0.0.1 >nul");
    spawned.session.terminate().unwrap();
    let exit = spawned
        .exit_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("the exit is published before terminate() returns");
    let code = exit
        .status
        .expect("a closed console still leaves a status")
        .exit_code();
    assert_ne!(
        code, 1,
        "the child exited with the hard kill's code: the graceful step closed nothing, \
         and terminate waited out GRACEFUL_WAIT to get here"
    );
}

#[test]
fn terminate_under_output_load_does_not_deadlock() {
    let _serial = serialize();
    // Finite only nominally: 50 million `echo y` outlasts anything here.
    let (spawned, out_rx) = spawn_cmd_script("for /L %i in (1,1,50000000) do @echo y");

    // A real burst in flight first, so terminate races an active reader.
    recv_until(&out_rx, |acc| acc.len() >= 256 * 1024);

    let t0 = Instant::now();
    spawned
        .session
        .terminate()
        .expect("terminate should not error");
    assert!(
        t0.elapsed() < TERMINATE_POLICY,
        "terminate() must not deadlock behind the reader or writer"
    );
    spawned
        .exit_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("the exit is published before terminate() returns");
    // The reader still drains to EOF: no stuck reader, no lost tail.
    spawned
        .eof_rx
        .recv_timeout(STALL)
        .expect("the reader never reached EOF after terminate");
}

/// Closing the pseudoconsole takes the attached tree with it.
///
/// The mechanism differs from Unix: no process group and no signal, just
/// ClosePseudoConsole. `start /B` runs the grandchild with no console of its
/// own, still attached to this session's -- so closing it must take the
/// grandchild down too, which is checked by an OS process query rather than
/// by anything the pty says.
#[test]
fn terminate_kills_the_grandchild_process_tree() {
    let _serial = serialize();
    let marker = format!("ANCHOR_W2_MARKER_{}", anchor::model::new_session_id());
    let (spawned, _out_rx) = spawn_interactive_cmd(80, 24);
    spawned
        .session
        .write(
            format!(
                "start /B powershell -NoProfile -Command \"Start-Sleep -Seconds 60 # {marker}\"\r\n"
            )
            .as_bytes(),
        )
        .unwrap();

    let deadline = Instant::now() + STALL;
    let pids = loop {
        let pids = pids_matching(&marker);
        if !pids.is_empty() {
            break pids;
        }
        assert!(
            Instant::now() < deadline,
            "the grandchild never appeared, so this would prove nothing"
        );
        std::thread::sleep(Duration::from_millis(100));
    };

    spawned
        .session
        .terminate()
        .expect("terminate should not error");

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let alive = pids_matching(&marker);
        if alive.is_empty() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "grandchild pid(s) {alive:?} (originally {pids:?}) outlived terminate by 5s"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Input after `terminate` is refused rather than queued for a child that
/// is gone: the CLOSING state rejects input on both platforms.
#[test]
fn input_after_terminate_is_rejected() {
    let _serial = serialize();
    let (spawned, _out_rx) = spawn_interactive_cmd(80, 24);
    spawned.session.terminate().unwrap();
    assert!(
        spawned.session.write(b"echo late\r\n").is_err(),
        "a terminated session accepted input"
    );
}
