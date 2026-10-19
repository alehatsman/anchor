//! `anchor purge` against a real daemon.
//!
//! Drives the CLI binary rather than the client library, because the bug this
//! covers was the absence of a subcommand (#14). `anchord` has always served
//! `purge` correctly; a library-level test would have passed throughout the
//! whole period the verb could not be reached from a shell.
//!
//! So every assertion here goes through `anchor`'s argv, and the three cases
//! are the three answers the daemon can give: forgotten, still running, never
//! existed.

#![cfg(unix)]

use std::path::PathBuf;
use std::process::{Child, Command, Output};
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
        let dir = PathBuf::from(format!("/tmp/anchor-purge-{}-{tag}", std::process::id()));
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

    /// Runs the CLI against this daemon and returns the whole result. The
    /// exit status is part of what is being asserted, so a failing run is
    /// returned rather than unwrapped.
    fn cli(&self, args: &[&str]) -> Output {
        Command::new(bin("anchor"))
            .arg("--socket")
            .arg(self.socket())
            .args(args)
            .output()
            .expect("running anchor")
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Spawns a session that runs until it is terminated.
fn create_sleeper(client: &mut Client) -> String {
    let response = client
        .call(
            "create",
            json!({
                "spec": {
                    "command": "/bin/sh",
                    "args": ["-c", "sleep 30"],
                    "cwd": std::env::temp_dir(),
                },
            }),
        )
        .expect("create");
    assert_eq!(response["state"], "running");
    response["session"]
        .as_str()
        .expect("a session id")
        .to_string()
}

/// Waits for a session to reach a terminal state. On the condition, not on a
/// clock: `terminate` returning says the signal was sent, not that `wait()`
/// has been observed, and a fixed sleep here would be asserting that the
/// reaper beat the sleep.
fn await_exited(client: &mut Client, id: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let state = client.call("get", json!({ "session": id })).expect("get")["session"]["state"]
            .as_str()
            .unwrap_or("")
            .to_string();
        if state == "exited" || state == "lost" {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the session never left {state:?}"
        );
        std::thread::sleep(Duration::from_millis(25));
    }
}

#[test]
fn purge_forgets_an_exited_session() {
    let daemon = Daemon::start("forgets");
    let mut client = daemon.client();

    let id = create_sleeper(&mut client);
    client
        .call("terminate", json!({ "session": &id }))
        .expect("terminate");
    await_exited(&mut client, &id);

    // The row is still listed after terminate -- that is the state #14 had
    // no first-party way to clear, so assert it is reachable before purging.
    let before = client.call("get", json!({ "session": &id })).expect("get");
    assert_eq!(before["session"]["state"], "exited");

    let purged = daemon.cli(&["purge", &id]);
    assert!(
        purged.status.success(),
        "anchor purge failed: {}",
        String::from_utf8_lossy(&purged.stderr)
    );

    // The real assertion: the daemon no longer knows the id. Without this,
    // a `purge` subcommand that parsed its argument and sent nothing would
    // pass on exit status alone.
    let after = client
        .call_raw("get", json!({ "session": &id }))
        .expect("get");
    assert_eq!(
        after["error"]["code"], "not_found",
        "the session survived the purge: {after}"
    );
}

#[test]
fn purge_refuses_a_running_session() {
    let daemon = Daemon::start("refuses");
    let mut client = daemon.client();

    let id = create_sleeper(&mut client);

    let refused = daemon.cli(&["purge", &id]);
    assert!(
        !refused.status.success(),
        "purging a running session should fail"
    );
    // The daemon's code is passed through rather than reworded, which is the
    // whole reason the subcommand carries no logic of its own.
    let stderr = String::from_utf8_lossy(&refused.stderr);
    assert!(
        stderr.contains("already_exited"),
        "the daemon's error code did not reach the shell: {stderr:?}"
    );

    // And the refusal left the session alone.
    let still = client.call("get", json!({ "session": &id })).expect("get");
    assert_eq!(still["session"]["state"], "running");
}

#[test]
fn purge_reports_an_unknown_id() {
    let daemon = Daemon::start("unknown");
    let _client = daemon.client();

    let missing = daemon.cli(&["purge", "01ZZZZZZZZZZZZZZZZZZZZZZZZ"]);
    assert!(
        !missing.status.success(),
        "purging an unknown id should fail"
    );
    let stderr = String::from_utf8_lossy(&missing.stderr);
    assert!(
        stderr.contains("not_found"),
        "a typo should be reported as a typo: {stderr:?}"
    );
}
