//! Black-box restart recovery — the M1 exit criterion that needed M3's ops
//! to be testable end to end (issue #5).
//!
//! Spawns the real `anchord` against a throwaway data dir, SIGKILLs it, and
//! drives the restarted daemon over the socket: a session that was running
//! comes back `lost`, and its log is intact and readable.
//!
//! Short paths on purpose: a unix socket path is capped near 104 bytes, so
//! this cannot use the usual deep temp directory.

#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use anchor::client::Client;
use serde_json::json;

/// A short-lived data dir under /tmp, kept short enough for a socket path.
struct DataDir(PathBuf);

impl DataDir {
    fn new(tag: &str) -> Self {
        let path = PathBuf::from(format!("/tmp/anchor-rr-{}-{tag}", std::process::id()));
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

    /// SIGKILL: the daemon gets no chance to tidy up, which is the whole
    /// point — this is a crash, not a shutdown.
    fn kill(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        self.kill();
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

fn log_bytes(client: &mut Client, id: &str) -> Vec<u8> {
    let response = client
        .call("log", json!({ "session": id, "from": 0, "max": 65536 }))
        .expect("log");
    response["bytes"]
        .as_array()
        .expect("a byte array")
        .iter()
        .map(|b| b.as_u64().expect("a byte") as u8)
        .collect()
}

#[test]
fn a_sigkilled_daemon_recovers_sessions_as_lost_with_readable_logs() {
    let dir = DataDir::new("lost");
    let mut daemon = Daemon::start(&dir);
    let mut client = connect(&mut daemon, &dir.socket());

    let created = client
        .call(
            "create",
            json!({
                "spec": {
                    "command": "/bin/sh",
                    "args": ["-c", "stty raw -echo; echo survive-me; sleep 300"],
                    "cwd": "/tmp",
                },
                "labels": { "wm.item": "mr:218" },
            }),
        )
        .expect("create");
    let id = created["session"]
        .as_str()
        .expect("a session id")
        .to_string();

    // Wait for the child's output to reach the log.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let bytes = log_bytes(&mut client, &id);
        if bytes.windows(10).any(|w| w == b"survive-me") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the session never produced output"
        );
        std::thread::sleep(Duration::from_millis(50));
    }

    daemon.kill();
    drop(client);

    let mut daemon = Daemon::start(&dir);
    let mut client = connect(&mut daemon, &dir.socket());

    let session = client
        .call("get", json!({ "session": &id }))
        .expect("get after restart")["session"]
        .clone();

    // No PTY survives a daemon death, so a row still claiming to be live is
    // lost — and it says why.
    assert_eq!(session["state"], "lost");
    assert_eq!(session["lost_reason"], "daemon_restart");
    assert!(session["exited_at"].as_i64().is_some());
    // Labels survive: a client's own state outlives the process.
    assert_eq!(session["labels"]["wm.item"], "mr:218");

    // The log is intact and still readable through the socket.
    let bytes = log_bytes(&mut client, &id);
    assert!(
        bytes.windows(10).any(|w| w == b"survive-me"),
        "the log must survive a crash: {:?}",
        String::from_utf8_lossy(&bytes)
    );

    // And the recovered offset matches the file exactly.
    let on_disk = std::fs::metadata(dir.0.join("anchor/sessions").join(&id).join("output.vt"))
        .expect("the log file")
        .len();
    assert_eq!(
        session["output_bytes"].as_u64(),
        Some(on_disk),
        "output_bytes must match the file after recovery reconciles them"
    );

    // A lost session is still listed, and still findable by its labels.
    let found = client
        .call("list", json!({ "filter": { "wm.item": "mr:218" } }))
        .expect("list");
    assert_eq!(found["sessions"].as_array().expect("sessions").len(), 1);

    daemon.kill();
}

#[test]
fn a_second_daemon_refuses_to_displace_a_live_one() {
    let dir = DataDir::new("single");
    let mut daemon = Daemon::start(&dir);
    let _client = connect(&mut daemon, &dir.socket());

    let second = Command::new(env!("CARGO_BIN_EXE_anchord"))
        .env("XDG_DATA_HOME", &dir.0)
        .env("XDG_RUNTIME_DIR", dir.0.join("run"))
        .env("ANCHOR_LOG", "error")
        .output()
        .expect("running a second anchord");
    assert!(
        !second.status.success(),
        "a second daemon must refuse the socket, not take it over"
    );
    let stderr = String::from_utf8_lossy(&second.stderr);
    assert!(
        stderr.contains("already running"),
        "unexpected error: {stderr}"
    );

    daemon.kill();
}

#[test]
fn a_second_daemon_does_not_touch_the_live_one_s_sessions() {
    let dir = DataDir::new("no-steal");
    let mut daemon = Daemon::start(&dir);
    let mut client = connect(&mut daemon, &dir.socket());

    let created = client
        .call(
            "create",
            json!({
                "spec": {
                    "command": "/bin/sh",
                    "args": ["-c", "stty raw -echo; echo alive; sleep 300"],
                    "cwd": "/tmp",
                },
            }),
        )
        .expect("create");
    let id = created["session"]
        .as_str()
        .expect("a session id")
        .to_string();

    // A second daemon must refuse the socket *and change nothing*. Startup
    // recovery rewrites every live row to LOST, so doing that before the
    // bind check means an accidental second launch — a stray `anchord` in a
    // shell, a supervisor racing itself — tells the running daemon's
    // clients their sessions are gone while the PTYs are still running.
    let second = Command::new(env!("CARGO_BIN_EXE_anchord"))
        .env("XDG_DATA_HOME", &dir.0)
        .env("XDG_RUNTIME_DIR", dir.0.join("run"))
        .env("ANCHOR_LOG", "error")
        .output()
        .expect("running a second anchord");
    assert!(!second.status.success(), "the second daemon must refuse");

    // Asserted against the row, not through the live daemon: `get` answers
    // from its in-memory session and would mask a corrupted row until the
    // next restart read it back.
    let row = std::process::Command::new("sqlite3")
        .arg(dir.0.join("anchor/anchor.db"))
        .arg(format!(
            "SELECT state, COALESCE(lost_reason,'') FROM sessions WHERE id='{id}';"
        ))
        .output()
        .expect("reading the row with sqlite3");
    let row = String::from_utf8_lossy(&row.stdout).trim().to_string();
    assert_eq!(
        row, "running|",
        "a refused second daemon must not rewrite the live daemon's rows"
    );

    daemon.kill();
}

/// A daemon that cannot take the socket must not have opened the database.
///
/// `Store::open` migrates -- DDL, a `meta` write, a journal-mode change -- so
/// opening before the bind lets a stray newer `anchord` rewrite the schema
/// under the daemon that is serving it, then die. The socket is the only
/// cross-process lock; here the test holds it, and a fresh data dir makes
/// "never opened" observable: `anchor.db` must not exist.
#[test]
fn a_daemon_that_loses_the_bind_never_opens_the_database() {
    let dir = DataDir::new("bind-first");
    let _held = std::os::unix::net::UnixListener::bind(dir.socket()).expect("holding the socket");

    let refused = Command::new(bin("anchord"))
        .env("XDG_DATA_HOME", &dir.0)
        .env("XDG_RUNTIME_DIR", dir.0.join("run"))
        .env("ANCHOR_LOG", "error")
        .output()
        .expect("running anchord");
    assert!(!refused.status.success(), "the bind must have been refused");
    assert!(
        String::from_utf8_lossy(&refused.stderr).contains("already running"),
        "expected the bind refusal, got: {}",
        String::from_utf8_lossy(&refused.stderr)
    );
    assert!(
        !dir.0.join("anchor/anchor.db").exists(),
        "a daemon that lost the bind created or migrated the database"
    );
}

/// A signal that lands while the daemon is still opening its database must
/// be handled, not fatal.
///
/// The socket is connectable the moment it is bound, and the bind now comes
/// before `Store::open`, so a supervisor (or a test) can SIGTERM the daemon in
/// that window. Made deterministic by holding a write transaction on the
/// database: anchord binds, connects succeed, and `Store::open` then blocks on
/// the lock -- exactly where the default disposition used to kill it.
#[test]
fn a_signal_during_database_open_is_handled_not_fatal() {
    use std::os::unix::process::ExitStatusExt;

    let dir = DataDir::new("sig-open");
    let db = dir.0.join("anchor/anchor.db");
    std::fs::create_dir_all(db.parent().unwrap()).unwrap();
    let blocker = rusqlite::Connection::open(&db).expect("creating the database");
    blocker
        .pragma_update(None, "journal_mode", "WAL")
        .expect("wal");
    blocker.execute_batch("BEGIN IMMEDIATE;").expect("lock");

    let mut daemon = Daemon::start(&dir);
    let _client = connect(&dir.socket());
    // Bound and connectable; still stuck behind the lock.
    assert!(
        daemon.0.try_wait().expect("polling anchord").is_none(),
        "anchord exited before the signal"
    );

    // SAFETY: plain kill(2) on our own child.
    unsafe { libc::kill(daemon.0.id() as libc::pid_t, libc::SIGTERM) };
    blocker.execute_batch("ROLLBACK;").expect("unlock");

    let deadline = Instant::now() + Duration::from_secs(10);
    let status = loop {
        if let Some(status) = daemon.0.try_wait().expect("polling anchord") {
            break status;
        }
        assert!(Instant::now() < deadline, "anchord never exited");
        std::thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(
        status.signal(),
        None,
        "anchord died of the signal instead of handling it: {status:?}"
    );
    assert_eq!(status.code(), Some(0), "unclean exit: {status:?}");
}
