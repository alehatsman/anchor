//! `anchor attach` against a real daemon.
//!
//! Drives the CLI binary rather than the client library, because the bugs
//! these cover live in the handover between them: which half of a split
//! socket owns the buffered bytes, and which fd has to be shut down for a
//! blocking read to end.

#![cfg(unix)]

use std::io::Write;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use anchor::client::Client;
use serde_json::json;

fn bin(name: &str) -> PathBuf {
    let mut path = std::env::current_exe().expect("test binary path");
    path.pop();
    if path.ends_with("deps") {
        path.pop();
    }
    path.join(name)
}

/// Short path: a unix socket path is capped near 104 bytes.
struct Daemon {
    child: Child,
    dir: PathBuf,
}

impl Daemon {
    fn start(tag: &str) -> Self {
        let dir = PathBuf::from(format!("/tmp/anchor-cli-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("run")).expect("creating the data dir");
        let child = Command::new(bin("anchord"))
            .env("XDG_DATA_HOME", &dir)
            .env("XDG_RUNTIME_DIR", dir.join("run"))
            .env("ANCHOR_LOG", "warn")
            .spawn()
            .expect("spawning anchord");
        Self { child, dir }
    }

    fn socket(&self) -> PathBuf {
        self.dir.join("run/anchor.sock")
    }

    fn client(&self) -> Client {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Ok(client) = Client::connect(&self.socket()) {
                return client;
            }
            assert!(Instant::now() < deadline, "anchord never came up");
            std::thread::sleep(Duration::from_millis(25));
        }
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Runs `anchor attach`, sends `stdin_bytes` after `settle`, and returns
/// its stdout once it exits — panicking if it outlives `timeout`, which is
/// the failure this is for.
///
/// The delay matters: `~.` typed instantly detaches before any replay
/// arrives, which is correct behaviour and tests nothing. Waiting first is
/// what a person does, and what makes the replay assertion meaningful.
fn run_attach(
    socket: &PathBuf,
    id: &str,
    stdin_bytes: &[u8],
    settle: Duration,
    timeout: Duration,
) -> String {
    let mut child = Command::new(bin("anchor"))
        .arg("--socket")
        .arg(socket)
        .arg("attach")
        .arg(id)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawning anchor attach");

    std::thread::sleep(settle);
    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(stdin_bytes)
        .expect("writing to anchor");

    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait().expect("try_wait") {
            Some(_) => break,
            None => {
                if Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!(
                        "`anchor attach` did not exit within {timeout:?} after `~.` — \
                         a detach must not wait on a session that happens to be quiet"
                    );
                }
                std::thread::sleep(Duration::from_millis(25));
            }
        }
    }

    let output = child.wait_with_output().expect("output");
    String::from_utf8_lossy(&output.stdout).into_owned()
}

#[test]
fn tilde_dot_detaches_from_an_idle_session() {
    let daemon = Daemon::start("detach");
    let mut client = daemon.client();

    // Deliberately silent after its banner: the session produces nothing
    // more, so a detach that waits for output waits forever.
    let created = client
        .call(
            "create",
            json!({
                "spec": {
                    "command": "/bin/sh",
                    "args": ["-c", "stty raw -echo; echo banner; sleep 300"],
                    "cwd": "/tmp",
                },
            }),
        )
        .expect("create");
    let id = created["session"]
        .as_str()
        .expect("a session id")
        .to_string();

    // Wait for the banner so the attach has history to replay.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let log = client
            .call("log", json!({ "session": &id, "from": 0, "max": 4096 }))
            .expect("log");
        let bytes: Vec<u8> = log["bytes"]
            .as_array()
            .expect("bytes")
            .iter()
            .filter_map(serde_json::Value::as_u64)
            .map(|b| b as u8)
            .collect();
        if bytes.windows(6).any(|w| w == b"banner") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the session never produced output"
        );
        std::thread::sleep(Duration::from_millis(50));
    }

    let stdout = run_attach(
        &daemon.socket(),
        &id,
        b"~.",
        Duration::from_millis(500),
        Duration::from_secs(10),
    );

    // The replay still arrived: detaching promptly must not come at the
    // cost of dropping what the client was owed.
    assert!(
        stdout.contains("banner"),
        "replayed output was lost at the attach handover: {stdout:?}"
    );
}

#[test]
fn an_attach_replays_history_without_losing_the_first_frames() {
    let daemon = Daemon::start("replay");
    let mut client = daemon.client();

    // A burst, so the first frames land in the same read as the attach
    // response and a fresh reader would drop them.
    let created = client
        .call(
            "create",
            json!({
                "spec": {
                    "command": "/bin/sh",
                    "args": ["-c", "stty raw -echo; i=0; while [ $i -lt 200 ]; do echo line-$i; i=$((i+1)); done; sleep 300"],
                    "cwd": "/tmp",
                },
            }),
        )
        .expect("create");
    let id = created["session"]
        .as_str()
        .expect("a session id")
        .to_string();

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let session =
            client.call("get", json!({ "session": &id })).expect("get")["session"].clone();
        if session["output_bytes"].as_u64().unwrap_or(0) > 1000 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the session never produced output"
        );
        std::thread::sleep(Duration::from_millis(50));
    }

    let stdout = run_attach(
        &daemon.socket(),
        &id,
        b"~.",
        Duration::from_millis(800),
        Duration::from_secs(10),
    );

    // Every line, in order and without a gap. A dropped frame at the
    // handover shows up as a missing line in the middle, not at the edges.
    for i in 0..200 {
        assert!(
            stdout.contains(&format!("line-{i}")),
            "line-{i} missing from the replay — frames lost at the handover"
        );
    }
}
