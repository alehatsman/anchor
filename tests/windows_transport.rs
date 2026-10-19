//! The Windows transport, end to end: a named pipe with an owner-only DACL,
//! claimed or refused, and the CLI driving a real ConPTY session through it
//! (docs/09-windows.md §3-§4, docs/04-protocol.md#windows-a-named-pipe).
//!
//! Every assertion about access control is asked of the kernel -- the SDDL
//! the OS holds for the pipe or the directory -- never of the string the
//! descriptor was built from. A test that restates what the code meant to
//! set passes for anything (docs/08-testing.md).
//!
//! Each test claims a pipe name of its own (`\\.\pipe\anchor-test-<id>`),
//! because the real one is per user and machine-global: tests sharing it
//! would refuse each other, and one would refuse the user's own anchord.

#![cfg(windows)]

use std::io::{BufReader, Read, Write};
use std::os::windows::io::AsHandle;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anchor::client::{Client, Stream};
use anchor::winsec::{self, SecurityDescriptor, UserSid};
use serde_json::{json, Value};

/// How long a fixture may see no progress before it gives up. A stall
/// bound for waits on a child or a daemon, not a budget for the test.
const STALL: Duration = Duration::from_secs(10);

fn unique_pipe() -> PathBuf {
    PathBuf::from(format!(
        r"\\.\pipe\anchor-test-{}",
        anchor::model::new_session_id()
    ))
}

fn my_sid() -> String {
    UserSid::current().unwrap().to_sid_string().unwrap()
}

struct Harness {
    dir: tempfile::TempDir,
    pipe: PathBuf,
}

/// An in-process daemon on a pipe of its own, the way the Unix protocol
/// tests run one on a socket in a temp dir.
fn harness() -> Harness {
    let dir = tempfile::tempdir().unwrap();
    let pipe = unique_pipe();
    let paths = anchor::paths::Paths::new(dir.path().join("data"), pipe.clone());
    paths.ensure_dirs().unwrap();
    let store = Arc::new(Mutex::new(anchor::store::Store::open(&paths).unwrap()));
    let manager = Arc::new(
        anchor::session::SessionManager::new(paths.sessions_dir()).with_store(Arc::clone(&store)),
    );
    let listener = anchor::server::bind(&pipe).unwrap();
    let state = anchor::server::State::new(manager, store);
    tokio::spawn(async move {
        let _ = anchor::server::serve(listener, state).await;
    });
    Harness { dir, pipe }
}

/// Polls `get` until the session leaves `running`, failing only on a stall.
fn wait_until_ended(client: &mut Client, id: &str) -> Value {
    let deadline = Instant::now() + STALL;
    loop {
        let got = client.call("get", json!({ "session": id })).unwrap();
        if got["session"]["state"] != "running" {
            return got["session"].clone();
        }
        assert!(Instant::now() < deadline, "the session never ended: {got}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// The exit code a session records is the one its child exited with,
/// carried over the pipe and the wire protocol unchanged -- and its output
/// is readable back by offset.
#[tokio::test(flavor = "multi_thread")]
async fn a_session_round_trips_over_the_pipe() {
    let h = harness();
    let mut client = Client::connect(&h.pipe).expect("connecting over the pipe");
    assert_eq!(client.call("ping", json!({})).unwrap()["pong"], true);

    let created = client
        .call(
            "create",
            json!({ "spec": {
                "command": "cmd.exe",
                "args": ["/c", "echo over-the-pipe & exit 5"],
                "cwd": std::env::temp_dir(),
            }}),
        )
        .unwrap();
    let id = created["session"].as_str().unwrap().to_string();
    let ended = wait_until_ended(&mut client, &id);
    assert_eq!(ended["state"], "exited");
    assert_eq!(ended["exit_code"], 5, "{ended}");

    let log = client
        .call("log", json!({ "session": id, "from": 0 }))
        .unwrap();
    let bytes: Vec<u8> = log["bytes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|b| b.as_u64().unwrap() as u8)
        .collect();
    assert!(
        String::from_utf8_lossy(&bytes).contains("over-the-pipe"),
        "the transcript lost the output"
    );
}

/// docs/09-windows.md §7 Q1: a DACL granting the creating user's SID and
/// nothing else, with that user as owner -- the owner being what a client
/// verifies. Read back from the kernel through a client's own handle.
#[tokio::test(flavor = "multi_thread")]
async fn the_pipe_grants_its_owner_and_nobody_else() {
    let h = harness();
    let stream = Stream::connect(&h.pipe).unwrap();
    let sddl = winsec::handle_sddl(stream.as_handle()).unwrap();
    let sid = my_sid();
    assert_eq!(
        sddl,
        format!("O:{sid}D:P(A;;FA;;;{sid})"),
        "the pipe grants more than its owner, or is owned by someone else"
    );
}

/// The data directory, the database and a session's transcript are this
/// user's alone (docs/09-windows.md §4.6): one ACE each, for this SID, and
/// the directory's DACL protected from whatever its parent grants.
#[tokio::test(flavor = "multi_thread")]
async fn the_data_dir_is_owner_only_on_windows() {
    let h = harness();
    let mut client = Client::connect(&h.pipe).unwrap();
    let created = client
        .call(
            "create",
            json!({ "spec": {
                "command": "cmd.exe",
                "args": ["/c", "echo secret"],
                "cwd": std::env::temp_dir(),
            }}),
        )
        .unwrap();
    let id = created["session"].as_str().unwrap().to_string();
    wait_until_ended(&mut client, &id);

    let sid = my_sid();
    let data = h.dir.path().join("data");
    let dir_sddl = winsec::path_sddl(&data).unwrap();
    assert!(
        dir_sddl.starts_with("D:P"),
        "the data dir inherits its parent's DACL: {dir_sddl}"
    );
    for path in [
        data.clone(),
        data.join("anchor.db"),
        data.join("sessions"),
        data.join("sessions").join(&id),
        data.join("sessions").join(&id).join("output.vt"),
    ] {
        let sddl = winsec::path_sddl(&path).unwrap();
        let aces: Vec<&str> = sddl.split('(').skip(1).collect();
        assert_eq!(
            aces.len(),
            1,
            "{} has more than one ACE: {sddl}",
            path.display()
        );
        assert!(
            aces[0].ends_with(&format!(";{sid})")) && aces[0].starts_with("A;"),
            "{} grants someone other than its owner: {sddl}",
            path.display()
        );
    }
}

/// A second daemon on a live daemon's pipe is refused, and the first keeps
/// serving: the Windows mirror of the Unix socket rule.
#[tokio::test(flavor = "multi_thread")]
async fn a_second_daemon_is_refused_while_the_first_serves() {
    let h = harness();
    let err = anchor::server::bind(&h.pipe).expect_err("a second bind must fail");
    assert!(
        err.to_string().contains("already running"),
        "unexpected refusal: {err:#}"
    );
    let mut client = Client::connect(&h.pipe).unwrap();
    assert_eq!(client.call("ping", json!({})).unwrap()["pong"], true);
}

/// docs/09-windows.md §4.2, the reason not to port probe-and-replace: a
/// pipe name someone else holds is a squatter. anchord refuses to start,
/// says so, and leaves the squatter's pipe exactly as it was.
///
/// The squatter here grants SYSTEM alone, so this user cannot even open it
/// -- the shape another user's pipe has from this side.
#[tokio::test(flavor = "multi_thread")]
async fn a_squatted_pipe_name_is_refused_not_replaced() {
    let name = unique_pipe();
    let squatter = create_pipe(&name, "D:P(A;;GA;;;SY)");

    let err = anchor::server::bind(&name).expect_err("a squatted name must not be claimed");
    let message = format!("{err:#}");
    assert!(
        message.contains("not this user's anchord") && message.contains("not replacing it"),
        "the refusal does not say what happened: {message}"
    );
    // Still the squatter's, unreplaced: a second attempt is refused the
    // same way, and a client is refused too rather than reaching anchord.
    assert!(anchor::server::bind(&name).is_err());
    let refused = Client::connect(&name)
        .err()
        .expect("a client must not get through");
    assert!(
        format!("{refused:#}").contains("Access is denied"),
        "{refused:#}"
    );
    drop(squatter);
}

/// The squatter `FILE_FLAG_FIRST_PIPE_INSTANCE` exists for: one that grants
/// everyone everything. Without the flag, `CreateNamedPipeW` on its name
/// does not fail -- it quietly adds an instance to *its* pipe, and clients
/// then reach either process, at the kernel's choosing. With it, the create
/// fails and nothing is joined.
#[tokio::test(flavor = "multi_thread")]
async fn a_permissive_squatter_is_not_joined() {
    let name = unique_pipe();
    let squatter = create_pipe(&name, "D:P(A;;GA;;;WD)");
    assert!(
        anchor::server::bind(&name).is_err(),
        "bind joined a pipe someone else created instead of refusing the name"
    );
    drop(squatter);
}

/// A path that is not under `\\.\pipe\` is refused by both ends rather than
/// opened as a file.
#[test]
fn a_filesystem_path_is_not_an_endpoint() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("anchor.sock");
    std::fs::write(&file, b"").unwrap();
    let bind = anchor::server::bind(&file).expect_err("bind accepted a file path");
    assert!(bind.to_string().contains("not a named pipe"), "{bind:#}");
    let connect = Client::connect(&file)
        .err()
        .expect("connect accepted a file path");
    assert!(
        format!("{connect:#}").contains("not a named pipe"),
        "{connect:#}"
    );
}

/// A raw pipe with the given SDDL, standing in for someone else's.
fn create_pipe(name: &Path, sddl: &str) -> std::os::windows::io::OwnedHandle {
    use std::os::windows::ffi::OsStrExt;
    use std::os::windows::io::FromRawHandle;
    use windows_sys::Win32::Foundation::INVALID_HANDLE_VALUE;
    use windows_sys::Win32::Storage::FileSystem::{FILE_FLAG_OVERLAPPED, PIPE_ACCESS_DUPLEX};
    use windows_sys::Win32::System::Pipes::{
        CreateNamedPipeW, PIPE_TYPE_BYTE, PIPE_UNLIMITED_INSTANCES, PIPE_WAIT,
    };
    let sd = SecurityDescriptor::from_sddl(sddl).unwrap();
    let attributes = sd.attributes();
    let wide: Vec<u16> = name.as_os_str().encode_wide().chain(Some(0)).collect();
    // SAFETY: a NUL-terminated name and attributes that outlive the call.
    let handle = unsafe {
        CreateNamedPipeW(
            wide.as_ptr(),
            PIPE_ACCESS_DUPLEX | FILE_FLAG_OVERLAPPED,
            PIPE_TYPE_BYTE | PIPE_WAIT,
            PIPE_UNLIMITED_INSTANCES,
            4096,
            4096,
            0,
            &attributes,
        )
    };
    assert_ne!(handle, INVALID_HANDLE_VALUE, "creating the stand-in pipe");
    // SAFETY: just created, owned by nothing else.
    unsafe { std::os::windows::io::OwnedHandle::from_raw_handle(handle) }
}

/// An `anchor daemon` child on a pipe of its own, killed on drop.
struct Daemon {
    child: Child,
    pipe: PathBuf,
    _data: tempfile::TempDir,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn anchor() -> Command {
    Command::new(env!("CARGO_BIN_EXE_anchor"))
}

fn start_daemon() -> Daemon {
    let data = tempfile::tempdir().unwrap();
    let pipe = unique_pipe();
    let child = anchor()
        .arg("--socket")
        .arg(&pipe)
        .arg("daemon")
        .env("LOCALAPPDATA", data.path())
        .env("ANCHOR_LOG", "warn")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("starting anchor daemon");
    let deadline = Instant::now() + STALL;
    while Client::connect(&pipe).is_err() {
        assert!(
            Instant::now() < deadline,
            "the daemon never started listening"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    Daemon {
        child,
        pipe,
        _data: data,
    }
}

/// A CLI process whose stdout and stderr are collected on threads, so a
/// full pipe never blocks it and output can be waited on as it arrives.
struct Cli {
    child: Child,
    stdout: mpsc::Receiver<Vec<u8>>,
    seen: Vec<u8>,
    stderr: std::thread::JoinHandle<String>,
}

fn spawn_cli(daemon: &Daemon, args: &[&str]) -> Cli {
    let mut child = anchor()
        .arg("--socket")
        .arg(&daemon.pipe)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("starting the CLI");
    let mut out = child.stdout.take().unwrap();
    let (tx, stdout) = mpsc::channel();
    std::thread::spawn(move || {
        let mut buf = [0u8; 4096];
        while let Ok(n) = out.read(&mut buf) {
            if n == 0 || tx.send(buf[..n].to_vec()).is_err() {
                return;
            }
        }
    });
    let err = child.stderr.take().unwrap();
    let stderr = std::thread::spawn(move || {
        let mut text = String::new();
        let _ = BufReader::new(err).read_to_string(&mut text);
        text
    });
    Cli {
        child,
        stdout,
        seen: Vec::new(),
        stderr,
    }
}

impl Cli {
    fn type_in(&mut self, bytes: &[u8]) {
        let stdin = self.child.stdin.as_mut().unwrap();
        stdin.write_all(bytes).unwrap();
        stdin.flush().unwrap();
    }

    /// Waits for `needle` on stdout, failing only if output stalls.
    fn wait_for(&mut self, needle: &str) {
        while !String::from_utf8_lossy(&self.seen).contains(needle) {
            match self.stdout.recv_timeout(STALL) {
                Ok(chunk) => self.seen.extend_from_slice(&chunk),
                Err(e) => panic!(
                    "{needle:?} never appeared ({e}); got {:?}",
                    String::from_utf8_lossy(&self.seen)
                ),
            }
        }
    }

    /// Waits for the process to end, failing only past `STALL`.
    fn finish(mut self) -> (std::process::ExitStatus, String) {
        let deadline = Instant::now() + STALL;
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                return (status, self.stderr.join().unwrap());
            }
            assert!(Instant::now() < deadline, "the CLI did not exit");
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

fn cli_ok(daemon: &Daemon, args: &[&str]) -> String {
    let out = anchor()
        .arg("--socket")
        .arg(&daemon.pipe)
        .args(args)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "anchor {args:?} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// M6's exit criterion through the real binaries: spawn a shell, type into
/// it, detach with `~.` from a session that has gone quiet, find it still
/// running, reattach and see the scrollback, and observe its end exactly
/// once with the code it actually exited with.
///
/// stdin is a pipe here, not a console, so raw mode is skipped as it is for
/// any scripted attach; the console path is exercised by hand (recorded in
/// docs/09-windows.md). The detach is the part that matters: it is the
/// stop-a-blocked-read path, and a session that has gone quiet is the case
/// where nothing else would end that read.
#[test]
fn detach_reattach_and_one_exit_through_the_cli() {
    let daemon = start_daemon();
    let id = cli_ok(&daemon, &["create", "--", "cmd.exe"]);

    let mut attached = spawn_cli(&daemon, &["attach", &id]);
    attached.type_in(b"echo marker-one\r");
    attached.wait_for("marker-one");
    // Quiet now: cmd has printed its prompt and waits. The detach must not.
    std::thread::sleep(Duration::from_millis(500));
    let t0 = Instant::now();
    attached.type_in(b"~.");
    let (status, stderr) = attached.finish();
    assert!(status.success(), "detach was not clean: {stderr}");
    assert!(
        t0.elapsed() < Duration::from_secs(2),
        "detaching from a quiet session took {:?}: the blocked read was not stopped",
        t0.elapsed()
    );

    let got: Value = serde_json::from_str(&cli_ok(&daemon, &["get", &id])).unwrap();
    assert_eq!(got["state"], "running", "a detach ended the session");

    let mut observer = spawn_cli(&daemon, &["attach", "--observe", &id]);
    observer.wait_for("marker-one"); // the scrollback, replayed from 0
    assert_eq!(cli_ok(&daemon, &["kill", &id]), "exited");
    let (status, stderr) = observer.finish();
    assert!(
        status.success(),
        "the observer did not end cleanly: {stderr}"
    );
    let exits: Vec<&str> = stderr
        .lines()
        .filter(|l| l.contains("session exited"))
        .collect();
    assert_eq!(
        exits.len(),
        1,
        "the exit was not reported exactly once: {stderr}"
    );

    let got: Value = serde_json::from_str(&cli_ok(&daemon, &["get", &id])).unwrap();
    assert_eq!(got["state"], "exited");
    let code = got["exit_code"]
        .as_i64()
        .expect("an exit code was recorded");
    assert!(
        exits[0].contains(&format!("({code})")),
        "the CLI reported a different code than the daemon recorded: {} vs {code}",
        exits[0]
    );
    // A console closed under cmd ends it with an NTSTATUS, recorded as the
    // signed value Windows tools print; never collapsed to -1.
    assert_ne!(code, -1, "an NTSTATUS was collapsed to -1");
}
