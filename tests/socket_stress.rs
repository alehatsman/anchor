//! The backpressure and reconnect rules, exercised *through* the socket
//! rather than in-process.
//!
//! The ported fixtures cover the fan-out itself. These cover the protocol
//! code on top of it: a storm of attachers, a reader that stops reading, and
//! clients that vanish mid-replay. The invariant under all of them is the
//! one docs/03-pty.md opens with — the PTY reader never waits for a
//! subscriber, and one bad client never harms another.

use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anchor::proto::frame;
use serde_json::{json, Value};

/// A client that stops reading is closed after this; the production default
/// is 30s, which would make these fixtures glacial.
///
/// Every wait in this file is derived from it rather than written out as
/// its own literal. Two independent numbers that have to stay in a ratio
/// drift apart the first time someone tunes one of them, and the failure
/// is a flake, not an error.
const WRITE_TIMEOUT: Duration = Duration::from_millis(2000);

fn harness() -> (tempfile::TempDir, PathBuf) {
    // SAFETY: set before any other thread in this binary reads it.
    unsafe {
        std::env::set_var(
            "ANCHOR_WRITE_TIMEOUT_MS",
            WRITE_TIMEOUT.as_millis().to_string(),
        )
    };
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("anchor.sock");
    let paths = anchor::paths::Paths::new(dir.path().join("data"), socket.clone());
    paths.ensure_dirs().unwrap();
    let store = Arc::new(Mutex::new(anchor::store::Store::open(&paths).unwrap()));
    let manager = Arc::new(
        anchor::session::SessionManager::new(paths.sessions_dir()).with_store(Arc::clone(&store)),
    );
    let listener = anchor::server::bind(&socket).unwrap();
    let state = anchor::server::State::new(manager, store);
    tokio::spawn(async move {
        let _ = anchor::server::serve(listener, state).await;
    });
    (dir, socket)
}

struct Conn {
    reader: BufReader<UnixStream>,
    writer: UnixStream,
    next_id: u64,
}

impl Conn {
    fn connect(socket: &Path) -> Self {
        let stream = UnixStream::connect(socket).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(20)))
            .unwrap();
        Self {
            reader: BufReader::new(stream.try_clone().unwrap()),
            writer: stream,
            next_id: 0,
        }
    }

    fn call(&mut self, op: &str, mut body: Value) -> Value {
        self.next_id += 1;
        body["id"] = json!(self.next_id);
        body["op"] = json!(op);
        let mut line = serde_json::to_vec(&body).unwrap();
        line.push(b'\n');
        self.writer.write_all(&line).unwrap();
        self.writer.flush().unwrap();
        let mut response = String::new();
        self.reader.read_line(&mut response).unwrap();
        serde_json::from_str(&response).expect("a JSON response")
    }

    fn frame(&mut self) -> std::io::Result<(u8, u64, Vec<u8>)> {
        let mut header = [0u8; frame::HEADER_LEN];
        self.reader.read_exact(&mut header)?;
        let (tag, offset, len) = frame::decode_header(&header);
        let mut payload = vec![0u8; len];
        self.reader.read_exact(&mut payload)?;
        Ok((tag, offset, payload))
    }
}

/// A session that produces output continuously, so attachers are always
/// racing a live producer rather than a quiet one.
fn busy_session(conn: &mut Conn) -> String {
    let response = conn.call(
        "create",
        json!({
            "spec": {
                "command": "/bin/sh",
                "args": ["-c", "stty raw -echo; echo started; while :; do yes | head -c 65536; sleep 0.02; done"],
                "cwd": std::env::temp_dir(),
            }
        }),
    );
    assert_eq!(response["ok"], true, "create failed: {response}");
    response["session"].as_str().unwrap().to_string()
}

/// Offsets must be contiguous and gapless across every frame a client sees:
/// that is the entire promise an offset makes.
fn assert_contiguous(frames: &[(u64, usize)], from: u64, who: &str) {
    let mut expected = from;
    for (offset, len) in frames {
        assert_eq!(
            *offset, expected,
            "{who}: a gap or overlap at offset {offset} (expected {expected})"
        );
        expected += *len as u64;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_storm_of_attachers_each_gets_a_gapless_stream() {
    let (_dir, socket) = harness();
    let mut control = Conn::connect(&socket);
    let id = busy_session(&mut control);

    // Everyone attaches at once against a session that never stops
    // producing — the shape of a UI reconnecting every client after a
    // daemon update.
    const STORM: usize = 16;
    let handles: Vec<_> = (0..STORM)
        .map(|n| {
            let socket = socket.clone();
            let id = id.clone();
            std::thread::spawn(move || {
                let mut conn = Conn::connect(&socket);
                let ready = conn.call("attach", json!({ "session": id, "from": 0 }));
                assert_eq!(ready["ok"], true, "attacher {n}: {ready}");

                let mut frames = Vec::new();
                let mut bytes = 0usize;
                let deadline = Instant::now() + Duration::from_secs(20);
                // Read a decent stretch, then leave.
                while bytes < 200_000 && Instant::now() < deadline {
                    match conn.frame() {
                        Ok((frame::OUTPUT, offset, payload)) => {
                            bytes += payload.len();
                            frames.push((offset, payload.len()));
                        }
                        Ok((frame::LEASE, _, _)) => {}
                        Ok((frame::EXIT, _, _)) | Err(_) => break,
                        Ok(_) => {}
                    }
                }
                assert_contiguous(&frames, 0, &format!("attacher {n}"));
                assert!(bytes > 0, "attacher {n} received nothing");
            })
        })
        .collect();

    for handle in handles {
        handle.join().expect("an attacher panicked");
    }

    // The session is untouched by all of that, and the daemon still answers.
    let after = control.call("get", json!({ "session": &id }));
    assert_eq!(after["session"]["state"], "running");
    control.call("terminate", json!({ "session": &id }));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_client_that_stops_reading_never_stalls_the_session() {
    let (_dir, socket) = harness();
    let mut control = Conn::connect(&socket);
    let id = busy_session(&mut control);

    // Attach and then simply never read. The socket buffer fills, the
    // subscriber's 8 MiB budget fills, and the daemon drops this one rather
    // than letting it slow the PTY reader.
    let mut sloth = Conn::connect(&socket);
    let ready = sloth.call("attach", json!({ "session": &id, "from": 0 }));
    assert_eq!(ready["ok"], true);

    // Meanwhile a healthy client attaches and must be entirely unaffected.
    let mut healthy = Conn::connect(&socket);
    healthy.call("attach", json!({ "session": &id, "from": 0 }));

    let mut bytes = 0usize;
    let mut frames = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(20);
    while bytes < 300_000 {
        assert!(
            Instant::now() < deadline,
            "a healthy client starved behind a slow one: only {bytes} bytes"
        );
        match healthy.frame() {
            Ok((frame::OUTPUT, offset, payload)) => {
                bytes += payload.len();
                frames.push((offset, payload.len()));
            }
            Ok(_) => {}
            Err(e) => panic!("the healthy client was disconnected: {e}"),
        }
    }
    assert_contiguous(&frames, 0, "healthy client");

    // The session kept running throughout, and the control plane still
    // answers while one client is wedged.
    let after = control.call("get", json!({ "session": &id }));
    assert_eq!(after["session"]["state"], "running");
    assert!(after["session"]["output_bytes"].as_u64().unwrap() > 0);

    // And the slow one is dropped, as docs/03-pty.md says it should be:
    // "if a subscriber's queue overflows, the daemon closes that
    // connection. That is normal. Reconnect and replay."
    //
    // Critically it must be *closed*, not told the session exited: it is
    // still running, and an exit frame would send the client off to render
    // a dead session instead of reconnecting.
    // Stay silent past the write bound. Reading even once here would let
    // the daemon's blocked write complete and reset the clock, which is
    // precisely what a real stalled client does not do.
    tokio::time::sleep(2 * WRITE_TIMEOUT).await;

    // Generous against a loaded machine: the bound itself is what is being
    // tested, and overshooting it only costs time on a failure.
    let deadline = Instant::now() + 12 * WRITE_TIMEOUT;
    loop {
        assert!(
            Instant::now() < deadline,
            "a client that never reads must eventually be closed, not parked forever"
        );
        match sloth.frame() {
            Ok((frame::EXIT, _, _)) => {
                panic!("a dropped subscriber was told the session exited; it is still running")
            }
            Ok(_) => {}
            Err(_) => break, // connection closed, which is the contract
        }
    }

    // The session really is still alive after that.
    let after = control.call("get", json!({ "session": &id }));
    assert_eq!(after["session"]["state"], "running");

    control.call("terminate", json!({ "session": &id }));
}

#[tokio::test(flavor = "multi_thread")]
async fn clients_vanishing_mid_stream_do_not_disturb_the_rest() {
    let (_dir, socket) = harness();
    let mut control = Conn::connect(&socket);
    let id = busy_session(&mut control);

    let mut survivor = Conn::connect(&socket);
    survivor.call("attach", json!({ "session": &id, "from": 0 }));

    // Twenty clients attach and are dropped mid-replay, without ever
    // finishing catch-up — the abandoned-tab case.
    for _ in 0..20 {
        let mut doomed = Conn::connect(&socket);
        doomed.call("attach", json!({ "session": &id, "from": 0 }));
        let _ = doomed.frame(); // the lease frame, maybe one output frame
        drop(doomed);
    }

    // The survivor's stream is still gapless and still flowing.
    let mut bytes = 0usize;
    let mut frames = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(20);
    while bytes < 150_000 {
        assert!(
            Instant::now() < deadline,
            "the survivor stalled at {bytes} bytes"
        );
        match survivor.frame() {
            Ok((frame::OUTPUT, offset, payload)) => {
                bytes += payload.len();
                frames.push((offset, payload.len()));
            }
            Ok(_) => {}
            Err(e) => panic!("the survivor was disconnected: {e}"),
        }
    }
    assert_contiguous(&frames, 0, "survivor");

    control.call("terminate", json!({ "session": &id }));
}

#[tokio::test(flavor = "multi_thread")]
async fn reconnecting_at_the_last_offset_loses_nothing() {
    let (_dir, socket) = harness();
    let mut control = Conn::connect(&socket);
    let id = busy_session(&mut control);

    // Read a stretch, drop, reconnect at exactly where we stopped, and the
    // two halves must join with no gap and no duplicate. This is the
    // recover-by-offset discipline the protocol is built on.
    let mut first = Conn::connect(&socket);
    first.call("attach", json!({ "session": &id, "from": 0 }));
    let mut consumed = 0u64;
    let mut frames = Vec::new();
    while consumed < 100_000 {
        if let Ok((frame::OUTPUT, offset, payload)) = first.frame() {
            frames.push((offset, payload.len()));
            consumed = offset + payload.len() as u64;
        }
    }
    assert_contiguous(&frames, 0, "first connection");
    drop(first);

    let mut second = Conn::connect(&socket);
    let ready = second.call("attach", json!({ "session": &id, "from": consumed }));
    assert_eq!(ready["ok"], true, "reconnect refused: {ready}");

    let mut frames = Vec::new();
    let mut bytes = 0usize;
    while bytes < 50_000 {
        if let Ok((frame::OUTPUT, offset, payload)) = second.frame() {
            bytes += payload.len();
            frames.push((offset, payload.len()));
        }
    }
    // The resumed stream starts exactly where the first one stopped.
    assert_contiguous(&frames, consumed, "second connection");

    control.call("terminate", json!({ "session": &id }));
}
