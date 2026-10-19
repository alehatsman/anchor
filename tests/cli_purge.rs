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

    /// Waits for anchord to bind, on the condition rather than on a clock --
    /// see the same helper in `cli_attach.rs` for why a fixed budget here is
    /// an assertion about the box rather than about the daemon.
    fn client(&mut self) -> Client {
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            if let Some(status) = self.child.try_wait().expect("polling anchord") {
                panic!("anchord exited before binding its socket: {status}");
            }
            if let Ok(client) = Client::connect(&self.socket()) {
                return client;
            }
            assert!(
                Instant::now() < deadline,
                "anchord is still running but never bound its socket"
            );
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
    let mut daemon = Daemon::start("forgets");
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
    let mut daemon = Daemon::start("refuses");
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
    let mut daemon = Daemon::start("unknown");
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

// ---------------------------------------------------------------------------
// The rest of the CLI's surface: what a closed pipe, a typo and `--help` do.
// They live here because this file already carries the fixture that runs
// `anchor` against a daemon on a private socket.
// ---------------------------------------------------------------------------

/// A pipe closed by the reader ends the command quietly, like `cat`.
///
/// Rust ignores `SIGPIPE`, so `anchor logs <id> | head -c 5` surfaced as
/// `Error: Broken pipe (os error 32)` and exit 1 -- and `anchor ls | head`
/// as a panic and exit 101. Both are the same cause. The output has to be
/// bigger than the pipe buffer, or the writer finishes before the reader
/// leaves and nothing is broken.
#[test]
fn a_closed_pipe_ends_logs_quietly() {
    use std::io::Read;
    use std::os::unix::process::ExitStatusExt;

    let mut daemon = Daemon::start("pipe");
    let mut client = daemon.client();
    let created = client
        .call(
            "create",
            json!({
                "spec": {
                    "command": "/bin/sh",
                    "args": ["-c", "head -c 400000 /dev/zero | tr '\\0' x; sleep 30"],
                    "cwd": std::env::temp_dir(),
                },
            }),
        )
        .expect("create");
    let id = created["session"].as_str().expect("an id").to_string();

    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let session =
            client.call("get", json!({ "session": &id })).expect("get")["session"].clone();
        if session["output_bytes"].as_u64().unwrap_or(0) > 300_000 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the session never produced output"
        );
        std::thread::sleep(Duration::from_millis(50));
    }

    let mut logs = Command::new(bin("anchor"))
        .arg("--socket")
        .arg(daemon.socket())
        .args(["logs", &id])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawning anchor logs");

    // What `head -c 5` does: take a little, then leave.
    let mut stdout = logs.stdout.take().expect("stdout");
    let mut first = [0u8; 5];
    stdout.read_exact(&mut first).expect("the first bytes");
    drop(stdout);

    let deadline = Instant::now() + Duration::from_secs(60);
    let status = loop {
        if let Some(status) = logs.try_wait().expect("try_wait") {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = logs.kill();
            panic!("anchor logs never noticed its reader was gone");
        }
        std::thread::sleep(Duration::from_millis(25));
    };
    let mut stderr = String::new();
    logs.stderr
        .take()
        .expect("stderr")
        .read_to_string(&mut stderr)
        .expect("reading stderr");

    assert_eq!(
        status.signal(),
        Some(libc::SIGPIPE),
        "a closed pipe should end the process like `cat`, got {status:?} with stderr {stderr:?}"
    );
    assert!(
        stderr.is_empty(),
        "a closed pipe is not worth a message: {stderr:?}"
    );
}

/// An error about a session names the session, keeps the code first, and
/// says where to look.
///
/// The daemon's `not_found` says "no such session" without saying which, so
/// with several ids in flight the user is left guessing. Every verb that
/// takes an id, because each makes its own call.
#[test]
fn a_not_found_error_names_the_session_and_what_to_try() {
    let mut daemon = Daemon::start("names-the-id");
    let _client = daemon.client();
    let id = "01ZZZZZZZZZZZZZZZZZZZZZZZZ";

    for args in [
        vec!["get", id],
        vec!["logs", id],
        vec!["kill", id],
        vec!["purge", id],
        vec!["label", id, "a=b"],
        vec!["attach", id],
    ] {
        let out = daemon.cli(&args);
        assert_eq!(out.status.code(), Some(1), "{args:?}");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("Error: not_found:"),
            "the code must stay the leading token, a script greps for it ({args:?}): {stderr:?}"
        );
        assert!(stderr.contains(id), "which session? ({args:?}): {stderr:?}");
        assert!(
            stderr.contains("anchor ls"),
            "nothing says what to try next ({args:?}): {stderr:?}"
        );
    }
}

/// A malformed `--size` says it was `--size`.
#[test]
fn a_bad_size_names_the_flag() {
    let mut daemon = Daemon::start("bad-size");
    let _client = daemon.client();

    for size in ["80xabc", "abcx24"] {
        let out = daemon.cli(&["create", "--size", size, "--", "true"]);
        assert!(!out.status.success(), "{size}");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("--size"),
            "`{size}` gave an error that never mentions the flag: {stderr:?}"
        );
    }
}

/// `--help` says what is true, and says something about every argument a
/// user has to supply.
///
/// `--help` runs without a daemon, so none is started.
#[test]
fn the_help_text_describes_what_the_commands_do() {
    let help = |args: &[&str]| -> String {
        let out = Command::new(bin("anchor"))
            .args(args)
            .arg("--help")
            .output()
            .expect("running anchor --help");
        assert!(out.status.success(), "{args:?} --help failed");
        String::from_utf8_lossy(&out.stdout).into_owned()
    };

    // `ls --json` prints `response["sessions"]`, not the response.
    let ls = help(&["ls"]);
    assert!(ls.contains("JSON array"), "{ls}");
    assert!(!ls.contains("raw JSON response"), "{ls}");

    // The global `--socket` is shown under `daemon`, where it is bound.
    let daemon = help(&["daemon"]);
    assert!(daemon.contains("bind"), "{daemon}");

    for verb in ["get", "attach", "logs", "kill", "purge", "label"] {
        let text = help(&[verb]);
        assert!(
            text.contains("Session id"),
            "`anchor {verb} <ID>` does not say what an id is: {text}"
        );
    }
    assert!(help(&["create"]).contains("Working directory"));
    assert!(help(&["logs"]).contains("byte offset"));
    assert!(help(&["attach"]).contains("byte offset"));
}

/// `anchord` takes no arguments, so it must refuse them.
///
/// It used to start a daemon whatever it was given: `anchord --help` and
/// `anchord --socket X` both quietly ran one. The daemon here is pointed at
/// a private directory even though a correct `anchord` never gets that far,
/// because a broken one would start -- and must not start against the real
/// socket.
#[test]
fn anchord_refuses_arguments_it_does_not_have() {
    use std::io::Read;

    let run = |tag: &str, args: &[&str]| -> (std::process::ExitStatus, String, String) {
        let dir = PathBuf::from(format!("/tmp/anchor-purge-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("run")).expect("creating the data dir");
        let mut child = Command::new(bin("anchord"))
            .args(args)
            .env("XDG_DATA_HOME", &dir)
            .env("XDG_RUNTIME_DIR", dir.join("run"))
            .env("ANCHOR_LOG", "warn")
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("spawning anchord");
        let deadline = Instant::now() + Duration::from_secs(60);
        let status = loop {
            if let Some(status) = child.try_wait().expect("try_wait") {
                break status;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                let _ = std::fs::remove_dir_all(&dir);
                panic!("`anchord {args:?}` started a daemon instead of finishing");
            }
            std::thread::sleep(Duration::from_millis(25));
        };
        let (mut out, mut err) = (String::new(), String::new());
        child
            .stdout
            .take()
            .unwrap()
            .read_to_string(&mut out)
            .unwrap();
        child
            .stderr
            .take()
            .unwrap()
            .read_to_string(&mut err)
            .unwrap();
        let _ = std::fs::remove_dir_all(&dir);
        (status, out, err)
    };

    let (status, _, stderr) = run("refuses-bogus", &["--socket", "/tmp/x.sock"]);
    assert_eq!(status.code(), Some(2), "{stderr:?}");
    assert!(stderr.contains("--socket"), "{stderr:?}");

    let (status, stdout, _) = run("refuses-version", &["--version"]);
    assert!(status.success());
    assert!(stdout.contains("anchord"), "{stdout:?}");
}
