//! `anchor attach` against a real daemon.
//!
//! Drives the CLI binary rather than the client library, because the bugs
//! these cover live in the handover between them: which half of a split
//! socket owns the buffered bytes, and which fd has to be shut down for a
//! blocking read to end.

#![cfg(unix)]

use std::io::{Read, Write};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
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

    /// Waits for anchord to bind, on the condition rather than on a clock.
    ///
    /// A fixed budget here asserts the box was fast, not that the daemon came
    /// up: a cold anchord on a loaded runner takes well over ten seconds to
    /// start, and this fixture went red four ways under deliberate CPU
    /// contention with nothing broken (docs/08-testing.md).
    ///
    /// The failure actually worth catching is a daemon that *died*, and that
    /// is a different observation rather than a slower one — so check it
    /// directly instead of inferring it from elapsed time. The remaining
    /// deadline is a backstop against hanging the suite, not the assertion.
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
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Runs `anchor attach`, waits until `await_marker` has actually appeared
/// on its stdout, then sends `stdin_bytes` and returns everything it
/// printed once it exits — panicking if it outlives `timeout`, which is
/// the failure this is for.
///
/// The wait matters, and it must be on the output rather than on a clock:
/// `~.` typed before any replay arrives detaches correctly and tests
/// nothing, so a test that sleeps a fixed interval first is really
/// asserting that the replay beat the sleep. Under a loaded suite it does
/// not always, and the test fails claiming lost output when nothing was
/// lost.
///
/// Waiting for the marker keeps both directions honest. If the handover
/// really does drop what the client was owed, the marker never arrives,
/// the wait burns its deadline, and the caller's assertion fails on the
/// empty stdout — which is the bug this test exists to catch.
fn run_attach(
    socket: &PathBuf,
    id: &str,
    stdin_bytes: &[u8],
    await_marker: &str,
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

    // Drained on its own thread: the child keeps running, so reading to
    // EOF here would deadlock against the `~.` this has not sent yet.
    let seen = Arc::new(Mutex::new(Vec::<u8>::new()));
    let mut stdout = child.stdout.take().expect("stdout");
    let collector = {
        let seen = Arc::clone(&seen);
        std::thread::spawn(move || {
            let mut buf = [0u8; 4096];
            while let Ok(n) = stdout.read(&mut buf) {
                if n == 0 {
                    break;
                }
                seen.lock()
                    .expect("stdout buffer")
                    .extend_from_slice(&buf[..n]);
            }
        })
    };

    // Up to `timeout` for the replay to land. Exceeding it is not failed
    // here: the caller's assertion on the returned stdout is the error
    // message worth reading.
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let has_marker = {
            let seen = seen.lock().expect("stdout buffer");
            seen.windows(await_marker.len())
                .any(|w| w == await_marker.as_bytes())
        };
        if has_marker {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }

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

    collector.join().expect("stdout collector");
    let seen = seen.lock().expect("stdout buffer");
    String::from_utf8_lossy(&seen).into_owned()
}

#[test]
fn tilde_dot_detaches_from_an_idle_session() {
    let mut daemon = Daemon::start("detach");
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
        "banner",
        Duration::from_secs(10),
    );

    // The replay still arrived: detaching promptly must not come at the
    // cost of dropping what the client was owed.
    assert!(
        stdout.contains("banner"),
        "replayed output was lost at the attach handover: {stdout:?}"
    );
}

/// A paste longer than one read reaches the session whole, with nothing
/// typed after it.
///
/// The attach loop polls stdin's descriptor and then reads up to 1 KiB.
/// It used to read through `std::io::stdin()`, which is an 8 KiB
/// `BufReader`: one 3000-byte paste filled that buffer in a single read,
/// the loop forwarded the first 1024 bytes, and the rest sat in userspace
/// where `poll` on the fd cannot see it -- delivered only when the next
/// keystroke happened to make the fd readable again. A child waiting for
/// the whole paste waited for a key nobody was going to press.
#[test]
fn a_paste_longer_than_one_read_arrives_whole() {
    const PASTE: usize = 3000;
    let mut daemon = Daemon::start("paste");
    let mut client = daemon.client();
    let created = client
        .call(
            "create",
            json!({
                "spec": {
                    "command": "/bin/sh",
                    "args": ["-c", format!(
                        "stty raw -echo; echo ready; head -c {PASTE} >/dev/null; echo got-all; sleep 300"
                    )],
                    "cwd": "/tmp",
                },
            }),
        )
        .expect("create");
    let id = created["session"]
        .as_str()
        .expect("a session id")
        .to_string();

    let mut child = Command::new(bin("anchor"))
        .arg("--socket")
        .arg(daemon.socket())
        .arg("attach")
        .arg(&id)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawning anchor attach");
    let seen = Arc::new(Mutex::new(Vec::<u8>::new()));
    let mut stdout = child.stdout.take().expect("stdout");
    {
        let seen = Arc::clone(&seen);
        std::thread::spawn(move || {
            let mut buf = [0u8; 4096];
            while let Ok(n) = stdout.read(&mut buf) {
                if n == 0 {
                    break;
                }
                seen.lock().expect("stdout").extend_from_slice(&buf[..n]);
            }
        });
    }
    let saw = |needle: &str| {
        let seen = seen.lock().expect("stdout");
        seen.windows(needle.len()).any(|w| w == needle.as_bytes())
    };
    let deadline = Instant::now() + Duration::from_secs(30);
    while !saw("ready") {
        assert!(Instant::now() < deadline, "the session never became ready");
        std::thread::sleep(Duration::from_millis(10));
    }

    // One write, so it lands in the CLI's stdin as one chunk. No `~` in it.
    let stdin = child.stdin.as_mut().expect("stdin");
    stdin.write_all(&[b'p'; PASTE]).expect("pasting");
    stdin.flush().expect("flushing the paste");

    // Nothing else is typed: the whole paste has to get there on its own.
    let deadline = Instant::now() + Duration::from_secs(30);
    while !saw("got-all") {
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!(
                "the child never received all {PASTE} pasted bytes: the tail of the paste \
                 is stuck in the client, waiting for a keystroke"
            );
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(b"\r~.")
        .expect("detaching");
    let _ = child.wait();
}

#[test]
fn an_attach_replays_history_without_losing_the_first_frames() {
    let mut daemon = Daemon::start("replay");
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
        // The last line: detaching only once it has arrived is what makes
        // "every line, in order" a statement about the handover rather
        // than about how fast the replay happened to be.
        "line-199",
        Duration::from_secs(10),
    );

    // Every line, in order and without a gap.
    //
    // This is an end-to-end smoke test, not the evidence for the handover
    // bug it was written after: whether the first frames land in the same
    // read as the attach response is a timing accident, so passing here
    // does not mean the buffered bytes were kept. Reintroducing that bug
    // leaves this test green. The discriminating test is
    // `client::tests::the_attach_handover_keeps_bytes_that_arrived_with_the_response`,
    // which drives both sides of a socket pair and fails the moment the
    // buffer is dropped.
    for i in 0..200 {
        assert!(
            stdout.contains(&format!("line-{i}")),
            "line-{i} missing from the replay — frames lost at the handover"
        );
    }
}

/// `anchor attach` must come back on its own when the session ends.
///
/// The controller's stdin loop is a blocking read, and nothing wakes a
/// blocking read on stdin — so before this, a controller whose session had
/// exited sat there looking attached until the user happened to press a
/// key. The session was gone and the output thread had already finished.
///
/// Deliberately sends **no** stdin at all: a test that typed something
/// would unblock the read by hand and pass either way.
#[test]
fn attach_returns_when_the_session_exits() {
    let mut daemon = Daemon::start("exit-returns");
    let mut client = daemon.client();

    // Exits on its own shortly after the attach has settled.
    let created = client
        .call(
            "create",
            json!({
                "spec": {
                    "command": "/bin/sh",
                    "args": ["-c", "stty raw -echo; echo bye; sleep 1; exit 0"],
                    "cwd": "/tmp",
                },
            }),
        )
        .expect("create");
    let id = created["session"]
        .as_str()
        .expect("a session id")
        .to_string();

    let mut child = Command::new(bin("anchor"))
        .arg("--socket")
        .arg(daemon.socket())
        .arg("attach")
        .arg(&id)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawning anchor attach");

    // Keep stdin open and silent for the whole wait: closing it would end
    // the read via EOF, which is a different exit path than the one under
    // test.
    let _stdin = child.stdin.take().expect("stdin");

    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        match child.try_wait().expect("try_wait") {
            Some(status) => {
                assert!(status.success(), "attach exited with {status}");
                break;
            }
            None => {
                if Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!(
                        "`anchor attach` did not return after its session exited — \
                         a controller must not sit on a dead session waiting for a keystroke"
                    );
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    }
}

/// A terminal left in raw mode outlives the process that did it: no echo,
/// no line editing, and a blind `reset` the only way back. `Drop` restores
/// it, but `Drop` is exactly what does not run when a process is
/// signalled -- and a client attached to a long-running session is the
/// thing someone kills from another window.
///
/// Every signal that ends the process by default, not just `SIGTERM`: raw
/// mode stops the *keyboard* raising `SIGINT` and `SIGQUIT`, but `kill -INT`
/// and `pkill` are not the keyboard.
///
/// Driven through a real pty because that is the only way the behaviour
/// exists at all: under the pipes every other test here uses, stdin is not
/// a terminal, `RawMode::enter` returns `None`, and the whole path is
/// skipped. A test that proved this over pipes would be proving nothing.
#[test]
fn a_signalled_attach_leaves_the_terminal_usable() {
    let mut daemon = Daemon::start("sigterm-raw");
    let mut client = daemon.client();
    let created = client
        .call(
            "create",
            json!({
                "spec": {
                    "command": "/bin/sh",
                    "args": ["-c", "stty raw -echo; echo banner; cat"],
                    "cwd": "/tmp",
                },
            }),
        )
        .expect("create");
    let id = created["session"]
        .as_str()
        .expect("a session id")
        .to_string();

    for (name, signal) in [
        ("SIGTERM", libc::SIGTERM),
        ("SIGHUP", libc::SIGHUP),
        ("SIGINT", libc::SIGINT),
        ("SIGQUIT", libc::SIGQUIT),
    ] {
        let (master, slave) = open_pty();
        let echoes = |label: &str| -> bool {
            // SAFETY: tcgetattr on the master of a pty this test owns; both
            // ends share one termios.
            let mut t: libc::termios = unsafe { std::mem::zeroed() };
            assert_eq!(
                unsafe { libc::tcgetattr(master, &mut t) },
                0,
                "reading terminal attributes ({label})"
            );
            t.c_lflag & libc::ECHO != 0
        };
        assert!(echoes("before"), "a fresh pty echoes");

        // SAFETY: `slave` is a live fd; each `Stdio` takes its own dup.
        let (stdin, stdout) = unsafe {
            use std::os::fd::FromRawFd;
            (
                Stdio::from(std::os::fd::OwnedFd::from_raw_fd(libc::dup(slave))),
                Stdio::from(std::os::fd::OwnedFd::from_raw_fd(libc::dup(slave))),
            )
        };
        let mut attach = Command::new(bin("anchor"))
            .args(["--socket", daemon.socket().to_str().unwrap(), "attach", &id])
            .stdin(stdin)
            .stdout(stdout)
            .stderr(Stdio::null())
            .spawn()
            .expect("spawning anchor attach");

        // Wait for raw mode to actually be on, rather than for a span of
        // time: signalling before it engages would restore nothing and pass.
        let deadline = Instant::now() + Duration::from_secs(10);
        while echoes("waiting") {
            assert!(
                Instant::now() < deadline,
                "attach never entered raw mode, so {name} proves nothing"
            );
            std::thread::sleep(Duration::from_millis(20));
        }

        // SAFETY: signalling a child this test spawned.
        assert_eq!(unsafe { libc::kill(attach.id() as i32, signal) }, 0);
        let status = attach.wait().expect("waiting for attach");

        assert!(
            echoes("after"),
            "attach killed by {name} left the terminal with no echo"
        );
        // And it still looks killed to whoever was waiting on it: restoring
        // the terminal must not turn a fatal signal into a clean exit.
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(
            status.signal(),
            Some(signal),
            "attach did not die of {name}: {status:?}"
        );

        // SAFETY: fds this test opened.
        unsafe {
            libc::close(master);
            libc::close(slave);
        }
    }
}

/// An observer must be able to leave with ^C.
///
/// It never reads stdin and `~.` is scanned only on the controller path, so
/// if raw mode is on, `ISIG` is off and ^C is an unread byte: on a quiet
/// session the only way out is closing the window. The pty is the child's
/// *controlling* terminal here, because that is what makes the line
/// discipline turn the ^C byte into a SIGINT for it at all.
#[test]
fn an_observer_can_be_interrupted_from_the_keyboard() {
    let mut daemon = Daemon::start("observe-ctrl-c");
    let mut client = daemon.client();
    let created = client
        .call(
            "create",
            json!({
                "spec": {
                    "command": "/bin/sh",
                    "args": ["-c", "stty raw -echo; echo banner; cat"],
                    "cwd": "/tmp",
                },
            }),
        )
        .expect("create");
    let id = created["session"]
        .as_str()
        .expect("a session id")
        .to_string();

    let (master, slave) = open_pty();
    // SAFETY: `slave` is a live fd; each `Stdio` takes its own dup.
    let (stdin, stdout) = unsafe {
        use std::os::fd::FromRawFd;
        (
            Stdio::from(std::os::fd::OwnedFd::from_raw_fd(libc::dup(slave))),
            Stdio::from(std::os::fd::OwnedFd::from_raw_fd(libc::dup(slave))),
        )
    };
    let mut command = Command::new(bin("anchor"));
    command
        .args([
            "--socket",
            daemon.socket().to_str().unwrap(),
            "attach",
            "--observe",
            &id,
        ])
        .stdin(stdin)
        .stdout(stdout)
        .stderr(Stdio::null());
    // SAFETY: only async-signal-safe calls between fork and exec: a new
    // session, then adopting the pty as its controlling terminal.
    unsafe {
        use std::os::unix::process::CommandExt;
        command.pre_exec(|| {
            if libc::setsid() < 0 || libc::ioctl(0, libc::TIOCSCTTY as _, 0) < 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut attach = command.spawn().expect("spawning anchor attach");

    // The replay arriving on the pty means the observer is attached and
    // blocked on a quiet session, which is the state ^C has to break.
    let mut seen = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    while !seen.windows(6).any(|w| w == b"banner") {
        assert!(
            Instant::now() < deadline,
            "the observer never printed the replay"
        );
        let mut pollfd = libc::pollfd {
            fd: master,
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: one initialised pollfd, count matches.
        if unsafe { libc::poll(&mut pollfd, 1, 100) } > 0 {
            let mut buf = [0u8; 256];
            // SAFETY: reading into a buffer this test owns.
            let n = unsafe { libc::read(master, buf.as_mut_ptr().cast(), buf.len()) };
            if n > 0 {
                seen.extend_from_slice(&buf[..n as usize]);
            }
        }
    }

    // SAFETY: writing one byte to a pty master this test owns.
    assert_eq!(
        unsafe { libc::write(master, b"\x03".as_ptr().cast(), 1) },
        1
    );

    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        // Keep draining the master: a session leader's exit waits for its
        // terminal's output to be read.
        let mut pollfd = libc::pollfd {
            fd: master,
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: one initialised pollfd, count matches.
        if unsafe { libc::poll(&mut pollfd, 1, 25) } > 0 {
            let mut buf = [0u8; 256];
            // SAFETY: reading into a buffer this test owns.
            unsafe { libc::read(master, buf.as_mut_ptr().cast(), buf.len()) };
        }
        match attach.try_wait().expect("try_wait") {
            Some(_) => break,
            None => {
                if Instant::now() >= deadline {
                    let _ = attach.kill();
                    panic!(
                        "an observer ignored ^C -- it holds the terminal raw, so ISIG \
                         is off and the only way out is closing the window"
                    );
                }
                std::thread::sleep(Duration::from_millis(25));
            }
        }
    }

    // SAFETY: fds this test opened.
    unsafe {
        libc::close(master);
        libc::close(slave);
    }
}

/// Starts `anchor attach` on a quiet session with piped stdio and waits for
/// the replay, so the attach is live when the caller pulls the daemon out
/// from under it.
fn attach_to_a_live_session(
    daemon: &mut Daemon,
    extra: &[&str],
) -> (Child, Arc<Mutex<Vec<u8>>>, std::process::ChildStdin) {
    let mut client = daemon.client();
    let created = client
        .call(
            "create",
            json!({
                "spec": {
                    "command": "/bin/sh",
                    "args": ["-c", "stty raw -echo; echo banner; cat"],
                    "cwd": "/tmp",
                },
            }),
        )
        .expect("create");
    let id = created["session"]
        .as_str()
        .expect("a session id")
        .to_string();

    let mut child = Command::new(bin("anchor"))
        .arg("--socket")
        .arg(daemon.socket())
        .arg("attach")
        .args(extra)
        .arg(&id)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawning anchor attach");
    let stdin = child.stdin.take().expect("stdin");

    let seen = Arc::new(Mutex::new(Vec::<u8>::new()));
    let mut stdout = child.stdout.take().expect("stdout");
    {
        let seen = Arc::clone(&seen);
        std::thread::spawn(move || {
            let mut buf = [0u8; 4096];
            while let Ok(n) = stdout.read(&mut buf) {
                if n == 0 {
                    break;
                }
                seen.lock().unwrap().extend_from_slice(&buf[..n]);
            }
        });
    }
    let deadline = Instant::now() + Duration::from_secs(60);
    while !seen.lock().unwrap().windows(6).any(|w| w == b"banner") {
        assert!(Instant::now() < deadline, "the attach never replayed");
        std::thread::sleep(Duration::from_millis(10));
    }
    (child, seen, stdin)
}

/// A daemon that dies mid-attach is a failure, and says so.
///
/// `pump_output` used to discard its read error, so a daemon crash, a
/// detach and a desynchronised stream all ended the attach with no message
/// and exit 0: the terminal quietly returned to the shell, and a script
/// could not tell it from a clean detach.
///
/// Both roles, because they end differently: the controller learns of it
/// from the `ended` flag, the observer from joining the output thread.
#[test]
fn a_daemon_that_dies_mid_attach_is_an_error() {
    for (role, extra) in [("controller", vec![]), ("observer", vec!["--observe"])] {
        let mut daemon = Daemon::start(&format!("dies-{role}"));
        let (child, _seen, _stdin) = attach_to_a_live_session(&mut daemon, &extra);

        // Killed, not stopped: no goodbye frame, the way a crash looks.
        daemon.child.kill().expect("killing the daemon");
        let _ = daemon.child.wait();

        let deadline = Instant::now() + Duration::from_secs(30);
        let mut child = child;
        let status = loop {
            if let Some(status) = child.try_wait().expect("try_wait") {
                break status;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                panic!("the {role} outlived the daemon");
            }
            std::thread::sleep(Duration::from_millis(25));
        };
        let mut stderr = String::new();
        child
            .stderr
            .take()
            .expect("stderr")
            .read_to_string(&mut stderr)
            .expect("reading stderr");

        assert!(
            !status.success(),
            "the {role} reported a clean exit after its daemon died"
        );
        assert!(
            stderr.contains("connection to anchord ended"),
            "the {role} said nothing useful about the daemon dying: {stderr:?}"
        );
    }
}

/// `attach` must keep `SIGPIPE` ignored, even though every other command
/// restores the default.
///
/// A stdout that has gone away has to surface as an ordinary error so the
/// raw-mode guard unwinds and restores the terminal. Under the default
/// disposition the process would be killed mid-write, with `Drop` unrun. The
/// read end is closed *before* the attach starts, so the replay's first
/// write is already into a broken pipe.
#[test]
fn attach_survives_a_closed_stdout_as_an_error() {
    let mut daemon = Daemon::start("sigpipe-attach");
    let mut client = daemon.client();
    let created = client
        .call(
            "create",
            json!({
                "spec": {
                    "command": "/bin/sh",
                    "args": ["-c", "stty raw -echo; echo banner; cat"],
                    "cwd": "/tmp",
                },
            }),
        )
        .expect("create");
    let id = created["session"]
        .as_str()
        .expect("a session id")
        .to_string();

    let mut fds = [0 as libc::c_int; 2];
    // SAFETY: pipe fills two fds we own.
    assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
    // SAFETY: closing the read end we just made; wrapping the write end,
    // which the `Stdio` then owns.
    let stdout = unsafe {
        use std::os::fd::FromRawFd;
        libc::close(fds[0]);
        Stdio::from(std::os::fd::OwnedFd::from_raw_fd(fds[1]))
    };

    let mut attach = Command::new(bin("anchor"))
        .args(["--socket", daemon.socket().to_str().unwrap(), "attach", &id])
        .stdin(Stdio::piped())
        .stdout(stdout)
        .stderr(Stdio::null())
        .spawn()
        .expect("spawning anchor attach");
    let _stdin = attach.stdin.take();

    let deadline = Instant::now() + Duration::from_secs(60);
    let status = loop {
        if let Some(status) = attach.try_wait().expect("try_wait") {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = attach.kill();
            panic!("attach never noticed its stdout was gone");
        }
        std::thread::sleep(Duration::from_millis(25));
    };
    use std::os::unix::process::ExitStatusExt;
    assert_eq!(
        status.signal(),
        None,
        "attach was killed by SIGPIPE instead of failing: {status:?}"
    );
    assert_eq!(status.code(), Some(1), "expected an ordinary error exit");
}

fn open_pty() -> (i32, i32) {
    let (mut master, mut slave) = (0, 0);
    // SAFETY: openpty fills two fds we own; null for the optional name,
    // termios and winsize arguments.
    let rc = unsafe {
        libc::openpty(
            &mut master,
            &mut slave,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    };
    assert_eq!(rc, 0, "openpty failed");
    (master, slave)
}

/// A relative `--cwd` is resolved against the client's directory, not the
/// daemon's.
///
/// The spec goes over the wire and the daemon spawns from it, so a
/// relative path sent as typed is resolved wherever anchord happens to be
/// running -- `/` under launchd. The session then starts in a directory
/// nobody named, and nothing reports it: the row says `.`, which looks
/// right from either end.
#[test]
fn a_relative_cwd_is_resolved_before_it_is_sent() {
    let mut daemon = Daemon::start("relative-cwd");
    let mut client = daemon.client();

    let status = Command::new(bin("anchor"))
        .args([
            "--socket",
            daemon.socket().to_str().unwrap(),
            "create",
            "--cwd",
            ".",
            "--",
            "/bin/sh",
            "-c",
            "exit 0",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("running anchor create");
    assert!(status.success(), "create failed: {status:?}");

    let listed = client.call("list", json!({})).expect("list");
    let cwd = listed["sessions"][0]["cwd"]
        .as_str()
        .expect("a cwd on the row");
    assert!(
        std::path::Path::new(cwd).is_absolute(),
        "the daemon was handed a relative cwd to resolve: {cwd:?}"
    );
    assert_eq!(
        cwd,
        std::env::current_dir().unwrap().to_str().unwrap(),
        "`.` did not resolve to the client's own directory"
    );
}

/// Sets a pty's size, the way a terminal emulator does when its window
/// moves.
fn set_window_size(fd: i32, cols: u16, rows: u16) {
    let size = libc::winsize {
        ws_row: rows,
        ws_col: cols,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    // SAFETY: TIOCSWINSZ with a winsize this test owns, on a pty it opened.
    assert_eq!(
        unsafe { libc::ioctl(fd, libc::TIOCSWINSZ, &size) },
        0,
        "setting the window size"
    );
}

/// Polls `get` until the session reports `(cols, rows)`. Returns what it
/// last saw, so the caller's assertion is the one that reports the
/// failure.
fn await_size(client: &mut Client, id: &str, want: (u16, u16), timeout: Duration) -> (u16, u16) {
    let deadline = Instant::now() + timeout;
    loop {
        let got = client.call("get", json!({ "session": id })).expect("get");
        let session = &got["session"];
        let seen = (
            session["cols"].as_u64().expect("cols") as u16,
            session["rows"].as_u64().expect("rows") as u16,
        );
        if seen == want || Instant::now() >= deadline {
            return seen;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

/// Resizing the window after attaching reaches the PTY.
///
/// `anchor attach` used to size the session exactly once, before the
/// replay, and then install nothing: resize the terminal afterwards and
/// the child kept rendering at the size it had when you attached --
/// wrapped lines, a status bar in the wrong row, a full-screen TUI drawing
/// outside the visible area, and no way back but detaching and attaching
/// again (anchor#15).
///
/// Driven through a real pty for the same reason as the raw-mode test:
/// over pipes `window_size` returns `None` and the whole path is skipped.
/// The `SIGWINCH` is sent by hand because this pty has no controlling
/// session for the kernel to deliver one to -- which is the one thing a
/// real terminal does differently, and not the thing under test.
#[test]
fn a_resize_after_attaching_reaches_the_session() {
    let mut daemon = Daemon::start("sigwinch");
    let mut client = daemon.client();
    let created = client
        .call(
            "create",
            json!({
                "spec": {
                    "command": "/bin/sh",
                    "args": ["-c", "stty raw -echo; echo banner; cat"],
                    "cwd": "/tmp",
                    // Deliberately neither of the sizes below, so the
                    // attach-time resize is visible too.
                    "cols": 60,
                    "rows": 20,
                },
            }),
        )
        .expect("create");
    let id = created["session"]
        .as_str()
        .expect("a session id")
        .to_string();

    let (master, slave) = open_pty();
    // openpty leaves the size at 0x0, which `window_size` reports as "not
    // a size" -- so without this the client would send nothing and the
    // test would be measuring the wrong silence.
    set_window_size(master, 80, 24);

    // SAFETY: `slave` is a live fd; each `Stdio` takes its own dup.
    let (stdin, stdout) = unsafe {
        use std::os::fd::FromRawFd;
        (
            Stdio::from(std::os::fd::OwnedFd::from_raw_fd(libc::dup(slave))),
            Stdio::from(std::os::fd::OwnedFd::from_raw_fd(libc::dup(slave))),
        )
    };
    let mut attach = Command::new(bin("anchor"))
        .args(["--socket", daemon.socket().to_str().unwrap(), "attach", &id])
        .stdin(stdin)
        .stdout(stdout)
        .stderr(Stdio::null())
        .spawn()
        .expect("spawning anchor attach");

    // The attach-time resize is also the signal that the client is up and
    // holds the lease: without it, the SIGWINCH below would race the
    // attach and fail for the wrong reason.
    assert_eq!(
        await_size(&mut client, &id, (80, 24), Duration::from_secs(10)),
        (80, 24),
        "attach never sized the session to its terminal"
    );

    set_window_size(master, 120, 50);
    // SAFETY: signalling a child this test spawned.
    assert_eq!(
        unsafe { libc::kill(attach.id() as i32, libc::SIGWINCH) },
        0,
        "signalling the attach"
    );

    assert_eq!(
        await_size(&mut client, &id, (120, 50), Duration::from_secs(10)),
        (120, 50),
        "the window moved and the session never heard about it"
    );

    let _ = attach.kill();
    let _ = attach.wait();
    // SAFETY: fds this test opened.
    unsafe {
        libc::close(master);
        libc::close(slave);
    }
}
