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

fn bin(name: &str) -> PathBuf {
    // target/debug/deps/<test binary> -> target/debug/<name>
    let mut path = std::env::current_exe().expect("test binary path");
    path.pop();
    if path.ends_with("deps") {
        path.pop();
    }
    path.join(name)
}

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
        let child = Command::new(bin("anchord"))
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

/// Connects once the socket is accepting, or panics.
fn connect(socket: &Path) -> Client {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(client) = Client::connect(socket) {
            return client;
        }
        assert!(
            Instant::now() < deadline,
            "anchord never accepted connections"
        );
        std::thread::sleep(Duration::from_millis(50));
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
    let mut client = connect(&dir.socket());

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
    let mut client = connect(&dir.socket());

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
    let _client = connect(&dir.socket());

    let second = Command::new(bin("anchord"))
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
