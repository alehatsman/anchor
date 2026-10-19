//! SIGTERM is an instruction, not an accident (docs/07-lifecycle.md#stopping).
//!
//! launchd and systemd both stop a service by signalling it, so the last M0
//! exit criterion — "SIGTERM shuts down cleanly" (issue #2) — is the one M5's
//! unit files lean on. Black-box on purpose: the only thing a service manager
//! can see is how the process left, so that is what these assert.
//!
//! Short paths on purpose: a unix socket path is capped near 104 bytes, so
//! this cannot use the usual deep temp directory.

#![cfg(unix)]

use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use anchor::client::Client;
use serde_json::json;

/// A short-lived data dir under /tmp, kept short enough for a socket path.
struct DataDir(PathBuf);

impl DataDir {
    fn new(tag: &str) -> Self {
        let path = PathBuf::from(format!("/tmp/anchor-sd-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(path.join("run")).expect("creating the data dir");
        Self(path)
    }

    fn socket(&self) -> PathBuf {
        self.0.join("run/anchor.sock")
    }
}

impl Drop for DataDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Kills the daemon on drop, so a failing assertion never leaks a process.
struct Daemon(Child);

impl Daemon {
    fn start(dir: &DataDir) -> Self {
        let child = Command::new(env!("CARGO_BIN_EXE_anchord"))
            .env("XDG_DATA_HOME", &dir.0)
            .env("XDG_RUNTIME_DIR", dir.0.join("run"))
            .env("ANCHOR_LOG", "warn")
            .spawn()
            .expect("spawning anchord");
        Self(child)
    }

    fn signal(&self, signum: libc::c_int) {
        // Safe: the pid is this process's own child and has not been reaped,
        // so it cannot have been recycled.
        let sent = unsafe { libc::kill(self.0.id() as libc::pid_t, signum) };
        assert_eq!(sent, 0, "kill({signum}) failed");
    }

    /// Waits up to `bound` for the daemon to leave, or panics. Polls rather
    /// than blocking in `wait()`: a daemon that ignores the signal must fail
    /// the test, not hang it.
    fn wait_within(&mut self, bound: Duration) -> std::process::ExitStatus {
        let deadline = Instant::now() + bound;
        loop {
            match self.0.try_wait().expect("try_wait") {
                Some(status) => return status,
                None => assert!(
                    Instant::now() < deadline,
                    "anchord was still running {bound:?} after the signal"
                ),
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Waits for anchord to bind, on the condition rather than on a clock.
///
/// A fixed ten-second budget asserts the box was fast, not that the daemon
/// came up: a cold anchord on a loaded runner takes longer than that, and a
/// dead one would be reported only after the whole budget ran out. So check
/// for death directly, and keep the deadline as a backstop against hanging
/// the suite (docs/08-testing.md; same shape as tests/cli_attach.rs).
fn connect(daemon: &mut Daemon, socket: &Path) -> Client {
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        if let Some(status) = daemon.0.try_wait().expect("polling anchord") {
            panic!("anchord exited before binding its socket: {status}");
        }
        if let Ok(client) = Client::connect(socket) {
            return client;
        }
        assert!(
            Instant::now() < deadline,
            "anchord is still running but never bound its socket"
        );
        std::thread::sleep(Duration::from_millis(25));
    }
}

/// The difference a service manager can actually observe: a daemon that
/// handles the signal exits 0, and one that does not is reported as killed
/// by signal 15. Nothing else about the two paths is visible from outside,
/// which is why this is the assertion and not an `fsync` count.
#[test]
fn sigterm_exits_zero_rather_than_dying_of_the_signal() {
    let dir = DataDir::new("term");
    let mut daemon = Daemon::start(&dir);
    let _client = connect(&mut daemon, &dir.socket());

    daemon.signal(libc::SIGTERM);
    let status = daemon.wait_within(Duration::from_secs(10));

    assert_eq!(
        status.signal(),
        None,
        "anchord died of a signal instead of handling it: {status:?}"
    );
    assert_eq!(status.code(), Some(0), "unclean exit: {status:?}");
}

/// Ctrl-C on a foreground daemon takes the same path — a developer gets the
/// final fsync launchd gets.
#[test]
fn sigint_takes_the_same_path() {
    let dir = DataDir::new("int");
    let mut daemon = Daemon::start(&dir);
    let _client = connect(&mut daemon, &dir.socket());

    daemon.signal(libc::SIGINT);
    let status = daemon.wait_within(Duration::from_secs(10));

    assert_eq!(status.signal(), None, "anchord died of SIGINT: {status:?}");
    assert_eq!(status.code(), Some(0), "unclean exit: {status:?}");
}

/// A live session does not hold the shutdown open. The PTY dies with the
/// daemon by design (docs/07-lifecycle.md), so there is nothing to wait for
/// and waiting would only delay the SIGKILL the service manager sends next.
///
/// The log is checked for completeness afterwards. That is **not** proof of
/// the final fsync — the page cache serves the same bytes either way — only
/// that shutdown does not truncate or lose a log it was in the middle of.
#[test]
fn a_running_session_does_not_delay_shutdown() {
    let dir = DataDir::new("live");
    let mut daemon = Daemon::start(&dir);
    let mut client = connect(&mut daemon, &dir.socket());

    let created = client
        .call(
            "create",
            json!({
                "spec": {
                    "command": "/bin/sh",
                    // Ignores both graceful signals, and the ignore survives
                    // exec into `sleep`. A daemon that waited on this session
                    // the way `terminate` does would cost the whole 5 s + 2 s
                    // policy; a plain `sleep 300` dies on the first SIGHUP and
                    // would make even a polite shutdown look instant. It
                    // expires by itself because nothing here can reap it.
                    "args": ["-c", "trap '' HUP TERM; stty raw -echo; echo still-here; \
                                    i=0; while [ $i -lt 120 ]; do sleep 1; i=$((i+1)); done"],
                    "cwd": "/tmp",
                },
            }),
        )
        .expect("create");
    let id = created["session"]
        .as_str()
        .expect("a session id")
        .to_string();

    let log = dir.0.join("anchor/sessions").join(&id).join("output.vt");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if std::fs::read(&log).is_ok_and(|b| b.windows(10).any(|w| w == b"still-here")) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the session never produced output"
        );
        std::thread::sleep(Duration::from_millis(50));
    }

    // Well under what waiting on that session would take, and well under the
    // ten seconds an idle daemon is allowed above.
    daemon.signal(libc::SIGTERM);
    let status = daemon.wait_within(Duration::from_secs(3));
    assert_eq!(status.code(), Some(0), "unclean exit: {status:?}");

    // The PTY master closing sends the shell a SIGHUP it ignores: put it
    // down rather than leave it for its 120 s expiry.
    if let Some(pid) = created["pid"].as_i64() {
        // SAFETY: a plain kill(2); a recycled pid is vanishingly unlikely
        // within the test's lifetime and SIGKILL of a stale one is harmless
        // to this process.
        unsafe { libc::kill(pid as libc::pid_t, libc::SIGKILL) };
    }

    let bytes = std::fs::read(&log).expect("the log file after shutdown");
    assert!(
        bytes.windows(10).any(|w| w == b"still-here"),
        "shutdown lost a log it was writing: {:?}",
        String::from_utf8_lossy(&bytes)
    );
}
