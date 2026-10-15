//! M3: `attach` and the binary stream framing.
//!
//! The exit criterion this covers: detach, reconnect and see scrollback and
//! continue typing; two observers plus one controller behaving correctly.

use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anchor::proto::frame;
use serde_json::{json, Value};

fn harness() -> (tempfile::TempDir, std::path::PathBuf) {
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

/// A connection in control mode: newline JSON until `attach`, binary after.
struct Conn {
    reader: BufReader<UnixStream>,
    writer: UnixStream,
    next_id: u64,
}

impl Conn {
    fn connect(socket: &Path) -> Self {
        let stream = UnixStream::connect(socket).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
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

    /// Reads one binary frame.
    fn frame(&mut self) -> (u8, u64, Vec<u8>) {
        let mut header = [0u8; frame::HEADER_LEN];
        self.reader.read_exact(&mut header).expect("a frame header");
        let (tag, offset, len) = frame::decode_header(&header);
        let mut payload = vec![0u8; len];
        self.reader
            .read_exact(&mut payload)
            .expect("a frame payload");
        (tag, offset, payload)
    }

    /// Reads one binary frame, or `None` if the stream ended cleanly on a
    /// frame boundary. `frame` panics there instead, which is right for the
    /// tests that require a frame -- this one exists for the test that has
    /// to tell "ended" from "sent an exit".
    fn try_frame(&mut self) -> Option<(u8, u64, Vec<u8>)> {
        let mut header = [0u8; frame::HEADER_LEN];
        match self.reader.read_exact(&mut header) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return None,
            Err(e) => panic!("reading a frame header: {e}"),
        }
        let (tag, offset, len) = frame::decode_header(&header);
        let mut payload = vec![0u8; len];
        self.reader
            .read_exact(&mut payload)
            .expect("a frame payload");
        Some((tag, offset, payload))
    }

    /// Collects output frames until `needle` has been seen, returning
    /// everything received. Panics on timeout rather than hanging.
    fn read_until(&mut self, needle: &str) -> Vec<u8> {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut acc = Vec::new();
        loop {
            let (tag, _offset, payload) = self.frame();
            if tag == frame::OUTPUT {
                acc.extend_from_slice(&payload);
                if acc.windows(needle.len()).any(|w| w == needle.as_bytes()) {
                    return acc;
                }
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for {needle:?}; got {:?}",
                String::from_utf8_lossy(&acc)
            );
        }
    }

    fn send_input(&mut self, bytes: &[u8]) {
        self.writer
            .write_all(&frame::encode(frame::INPUT, 0, bytes))
            .unwrap();
        self.writer.flush().unwrap();
    }
}

fn create_shell(conn: &mut Conn) -> String {
    let response = conn.call(
        "create",
        json!({
            "spec": {
                "command": "/bin/sh",
                "args": ["-c", "stty raw -echo; echo ready; cat"],
                "cwd": std::env::temp_dir(),
            }
        }),
    );
    assert_eq!(response["ok"], true, "create failed: {response}");
    response["session"].as_str().unwrap().to_string()
}

#[tokio::test(flavor = "multi_thread")]
async fn attach_replays_history_then_streams_live() {
    let (_dir, socket) = harness();
    let mut control = Conn::connect(&socket);
    let id = create_shell(&mut control);

    // A second connection attaches from 0 and must see the banner that was
    // produced before it ever connected.
    let mut viewer = Conn::connect(&socket);
    let ready = viewer.call("attach", json!({ "session": &id, "from": 0 }));
    assert_eq!(ready["ok"], true, "attach failed: {ready}");
    assert_eq!(ready["offset"], 0);
    assert_eq!(ready["control"], false, "control was not requested");

    let (tag, _, payload) = viewer.frame();
    assert_eq!(tag, frame::LEASE, "the lease frame comes first");
    assert_eq!(payload, vec![0], "this connection does not hold the lease");

    viewer.read_until("ready");

    control.call("terminate", json!({ "session": &id }));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_controller_types_and_observers_only_watch() {
    let (_dir, socket) = harness();
    let mut setup = Conn::connect(&socket);
    let id = create_shell(&mut setup);

    // One controller, two observers — the exit criterion's shape.
    let mut controller = Conn::connect(&socket);
    let granted = controller.call(
        "attach",
        json!({ "session": &id, "from": 0, "control": true }),
    );
    assert_eq!(granted["control"], true, "the lease was free");
    assert_eq!(controller.frame().2, vec![1], "controller holds the lease");

    let mut observers: Vec<Conn> = (0..2)
        .map(|_| {
            let mut o = Conn::connect(&socket);
            let r = o.call(
                "attach",
                json!({ "session": &id, "from": 0, "control": true }),
            );
            // Attach never preempts: asking for control while someone else
            // holds it still attaches, as an observer.
            assert_eq!(r["ok"], true);
            assert_eq!(r["control"], false, "attach must not preempt");
            assert_eq!(o.frame().2, vec![0]);
            o
        })
        .collect();

    controller.read_until("ready");
    for o in &mut observers {
        o.read_until("ready");
    }

    // The controller types; `cat` echoes it back to every attached client.
    controller.send_input(b"hello-from-controller\n");
    controller.read_until("hello-from-controller");
    for o in &mut observers {
        o.read_until("hello-from-controller");
    }

    // An observer's input is ignored, not applied. Prove it by sending it,
    // then sending a known marker from the controller: the marker arrives
    // and the observer's bytes never do.
    observers[0].send_input(b"ignored-input\n");
    controller.send_input(b"marker-after\n");
    let seen = controller.read_until("marker-after");
    assert!(
        !seen.windows(13).any(|w| w == b"ignored-input"),
        "an observer must not be able to write to the PTY"
    );

    setup.call("terminate", json!({ "session": &id }));
}

#[tokio::test(flavor = "multi_thread")]
async fn detach_and_reconnect_from_an_offset_has_no_gap() {
    let (_dir, socket) = harness();
    let mut control = Conn::connect(&socket);
    let id = create_shell(&mut control);

    let mut first = Conn::connect(&socket);
    first.call(
        "attach",
        json!({ "session": &id, "from": 0, "control": true }),
    );
    assert_eq!(first.frame().2, vec![1]);
    let seen = first.read_until("ready");
    let resume_at = seen.len() as u64;

    first.send_input(b"before-detach\n");
    let seen = first.read_until("before-detach");
    let resume_at = resume_at.max(seen.len() as u64);

    // Drop the connection entirely — the session must not notice.
    drop(first);

    // Reconnect at the offset we had consumed: no gap, no duplicate.
    let mut second = Conn::connect(&socket);
    let ready = second.call(
        "attach",
        json!({ "session": &id, "from": resume_at, "control": true }),
    );
    assert_eq!(ready["ok"], true, "reconnect failed: {ready}");
    // The lease was taken by a connection that is gone; a new one may take
    // it, since the holder is per-connection.
    assert_eq!(second.frame().2, vec![1], "the lease is grantable again");

    second.send_input(b"after-reconnect\n");
    let after = second.read_until("after-reconnect");
    assert!(
        !after.windows(13).any(|w| w == b"before-detach"),
        "resuming from an offset must not replay what was already consumed"
    );

    control.call("terminate", json!({ "session": &id }));
}

#[tokio::test(flavor = "multi_thread")]
async fn exit_is_framed_with_the_code() {
    let (_dir, socket) = harness();
    let mut control = Conn::connect(&socket);
    let created = control.call(
        "create",
        json!({
            "spec": {
                "command": "/bin/sh",
                "args": ["-c", "stty raw -echo; echo bye; exit 7"],
                "cwd": std::env::temp_dir(),
            }
        }),
    );
    let id = created["session"].as_str().unwrap().to_string();

    let mut viewer = Conn::connect(&socket);
    viewer.call("attach", json!({ "session": &id, "from": 0 }));
    assert_eq!(viewer.frame().0, frame::LEASE);

    // Read frames until the exit frame arrives.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let (tag, _offset, payload) = viewer.frame();
        if tag == frame::EXIT {
            assert_eq!(payload.len(), 4, "an exit code is an i32");
            let code = i32::from_le_bytes(payload.try_into().unwrap());
            assert_eq!(code, 7);
            break;
        }
        assert!(Instant::now() < deadline, "no exit frame arrived");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn attaching_past_the_head_is_refused() {
    let (_dir, socket) = harness();
    let mut control = Conn::connect(&socket);
    let id = create_shell(&mut control);

    let mut client = Conn::connect(&socket);
    let refused = client.call("attach", json!({ "session": &id, "from": 999_999 }));
    assert_eq!(refused["ok"], false);
    assert_eq!(refused["error"]["code"], "invalid_spec");

    // The connection survives a refused attach and is still a control
    // connection.
    assert_eq!(client.call("ping", json!({}))["ok"], true);

    let missing = client.call("attach", json!({ "session": "01JNOPE0000000000000000000" }));
    assert_eq!(missing["error"]["code"], "not_found");

    control.call("terminate", json!({ "session": &id }));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_lease_frame_claims_control_and_the_loser_is_told() {
    let (_dir, socket) = harness();
    let mut setup = Conn::connect(&socket);
    let id = create_shell(&mut setup);

    let mut first = Conn::connect(&socket);
    first.call(
        "attach",
        json!({ "session": &id, "from": 0, "control": true }),
    );
    assert_eq!(first.frame().2, vec![1], "the lease was free");

    // A second connection attaches as an observer — attach never preempts.
    let mut second = Conn::connect(&socket);
    second.call(
        "attach",
        json!({ "session": &id, "from": 0, "control": true }),
    );
    assert_eq!(second.frame().2, vec![0], "attach must not preempt");

    first.read_until("ready");
    second.read_until("ready");

    // It claims control explicitly, which does preempt.
    second
        .writer
        .write_all(&frame::encode(frame::LEASE, 0, &[1]))
        .unwrap();
    second.writer.flush().unwrap();

    // The loser is told, rather than left typing into a void.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let (tag, _, payload) = first.frame();
        if tag == frame::LEASE {
            assert_eq!(payload, vec![0], "the previous holder lost the lease");
            break;
        }
        assert!(Instant::now() < deadline, "no revocation arrived");
    }

    // And the claimer is told it now holds it.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let (tag, _, payload) = second.frame();
        if tag == frame::LEASE {
            assert_eq!(payload, vec![1], "the claimer holds the lease");
            break;
        }
        assert!(Instant::now() < deadline, "no grant arrived");
    }

    // Input now follows the lease: the claimer's reaches the PTY.
    second.send_input(b"after-claim\n");
    second.read_until("after-claim");

    // The preempted connection's input is ignored from here on.
    first.send_input(b"should-be-ignored\n");
    second.send_input(b"marker-two\n");
    let seen = second.read_until("marker-two");
    assert!(
        !seen.windows(17).any(|w| w == b"should-be-ignored"),
        "a preempted connection must not be able to write"
    );

    setup.call("terminate", json!({ "session": &id }));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_lease_frame_that_does_not_claim_changes_nothing() {
    let (_dir, socket) = harness();
    let mut setup = Conn::connect(&socket);
    let id = create_shell(&mut setup);

    let mut holder = Conn::connect(&socket);
    holder.call(
        "attach",
        json!({ "session": &id, "from": 0, "control": true }),
    );
    assert_eq!(holder.frame().2, vec![1]);

    let mut observer = Conn::connect(&socket);
    observer.call("attach", json!({ "session": &id, "from": 0 }));
    assert_eq!(observer.frame().2, vec![0]);
    holder.read_until("ready");
    observer.read_until("ready");

    // Payload 0 is not a claim. Treating any lease frame as one would hand
    // control to a client that asked for the opposite.
    observer
        .writer
        .write_all(&frame::encode(frame::LEASE, 0, &[0]))
        .unwrap();
    observer.writer.flush().unwrap();

    // The holder still holds it: its input still reaches the PTY, and the
    // observer's still does not.
    holder.send_input(b"still-mine\n");
    holder.read_until("still-mine");

    observer.send_input(b"not-yours\n");
    holder.send_input(b"marker\n");
    let seen = holder.read_until("marker");
    assert!(
        !seen.windows(9).any(|w| w == b"not-yours"),
        "a lease frame with payload 0 must not grant control"
    );

    setup.call("terminate", json!({ "session": &id }));
}

#[tokio::test(flavor = "multi_thread")]
async fn resize_follows_the_lease() {
    let (_dir, socket) = harness();
    let mut setup = Conn::connect(&socket);
    let id = create_shell(&mut setup);

    // Nobody holds the lease yet: the control op is how a client sizes a
    // session to its terminal before attaching to it.
    let before = setup.call("resize", json!({ "session": &id, "cols": 100, "rows": 30 }));
    assert_eq!(
        before["ok"], true,
        "a free lease permits the control op: {before}"
    );

    let mut controller = Conn::connect(&socket);
    controller.call(
        "attach",
        json!({ "session": &id, "from": 0, "control": true }),
    );
    assert_eq!(controller.frame().2, vec![1]);
    controller.read_until("ready");

    // Now someone is typing. A bystander's control op must not reshape the
    // terminal under them.
    let refused = setup.call("resize", json!({ "session": &id, "cols": 40, "rows": 10 }));
    assert_eq!(refused["ok"], false);
    assert_eq!(refused["error"]["code"], "lease_held");

    // The holder resizes on its own connection, where the lease is known.
    controller
        .writer
        .write_all(&frame::encode(
            frame::RESIZE,
            0,
            &frame::encode_size(132, 50),
        ))
        .unwrap();
    controller.writer.flush().unwrap();

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let got = setup.call("get", json!({ "session": &id }))["session"].clone();
        if got["cols"] == 132 && got["rows"] == 50 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the holder's resize frame never applied; size is {}x{}",
            got["cols"],
            got["rows"]
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // And an observer's resize frame is ignored, like its input.
    let mut observer = Conn::connect(&socket);
    observer.call("attach", json!({ "session": &id, "from": 0 }));
    assert_eq!(observer.frame().2, vec![0]);
    observer
        .writer
        .write_all(&frame::encode(frame::RESIZE, 0, &frame::encode_size(20, 5)))
        .unwrap();
    observer.writer.flush().unwrap();

    tokio::time::sleep(Duration::from_millis(300)).await;
    let got = setup.call("get", json!({ "session": &id }))["session"].clone();
    assert_eq!(got["cols"], 132, "an observer must not resize");
    assert_eq!(got["rows"], 50);

    setup.call("terminate", json!({ "session": &id }));
}

/// A session that exits while its controller is still typing must still be
/// told it exited -- the stream must never just end.
///
/// Regression for three separate races that each closed an attached
/// connection with no `exit` frame, all of which only showed up on a session
/// that ends immediately:
///
/// 1. the EOF thread's `close_subscribers` running before the reaper had
///    written `state`, so the stream could not tell "the session is over"
///    from "this subscriber fell behind" and assumed the latter;
/// 2. a failed pty write ending the input half, which ended the whole
///    connection through the outer `select!` and cancelled the exit frame
///    being written;
/// 3. the input half winning that `select!` for any other reason, with the
///    same result.
///
/// Repeated because each one is a race: a single round passed often enough
/// to look green.
#[tokio::test(flavor = "multi_thread")]
async fn a_session_that_exits_under_its_controller_still_sends_exit() {
    let (_dir, socket) = harness();
    let mut control = Conn::connect(&socket);

    for round in 0..15 {
        let created = control.call(
            "create",
            json!({
                "spec": {
                    "command": "/bin/sh",
                    "args": ["-c", "stty raw -echo; echo ready; exit 5"],
                    "cwd": std::env::temp_dir(),
                }
            }),
        );
        assert_eq!(created["ok"], true, "round {round}: create failed: {created}");
        let id = created["session"].as_str().unwrap().to_string();

        let mut viewer = Conn::connect(&socket);
        let attached = viewer.call(
            "attach",
            json!({ "session": &id, "from": 0, "control": true }),
        );
        assert_eq!(attached["ok"], true, "round {round}: attach failed");

        // Type into it. The pty is already gone or about to be, so this is
        // the write that used to take the connection down with it.
        viewer.send_input(b"hello\n");

        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match viewer.try_frame() {
                Some((tag, _offset, payload)) => {
                    if tag == frame::EXIT {
                        assert_eq!(
                            payload.len(),
                            4,
                            "round {round}: an exit code is an i32"
                        );
                        break;
                    }
                }
                None => panic!(
                    "round {round}: the stream ended without an exit frame"
                ),
            }
            assert!(
                Instant::now() < deadline,
                "round {round}: no exit frame arrived"
            );
        }
    }
}

/// An input frame claiming more than one PTY chunk ends that connection.
///
/// The length is a client's unverified claim about how much follows, and
/// the daemon allocates on it. Without a cap, `len = 0xFFFFFFFF` is a 4 GiB
/// allocation for free, from any client that can reach the socket. The
/// limit is a chunk because that is the largest thing a legitimate client
/// has to say: one PTY read is 64 KiB and a keystroke is a few bytes.
///
/// Both ends of the boundary are checked. 64 KiB exactly must still work --
/// a paste is a real thing a client does -- so an off-by-one that turned
/// `>` into `>=` would break ordinary use while still passing a test that
/// only looked at the oversized case.
#[tokio::test(flavor = "multi_thread")]
async fn an_input_frame_larger_than_a_chunk_ends_the_connection() {
    let (_dir, socket) = harness();
    let mut setup = Conn::connect(&socket);
    let id = create_shell(&mut setup);

    let mut controller = Conn::connect(&socket);
    controller.call(
        "attach",
        json!({ "session": &id, "from": 0, "control": true }),
    );
    assert_eq!(controller.frame().2, vec![1]);
    controller.read_until("ready");

    // Exactly a chunk is legal, and the connection carries on after it.
    controller.send_input(&vec![b'.'; 64 * 1024]);
    controller.send_input(b"\x04"); // EOT: ends `cat`'s current line
    controller.send_input(b"still-here\n");
    controller.read_until("still-here");

    // One byte more is not, and the daemon drops the connection on the
    // header alone -- it never waits for a payload it has already refused,
    // which is the whole point of checking before allocating.
    let mut header = frame::encode(frame::INPUT, 0, &[]);
    let len = (64 * 1024u32 + 1).to_be_bytes();
    let at = header.len() - 4;
    header[at..].copy_from_slice(&len);
    controller.writer.write_all(&header).unwrap();
    controller.writer.flush().unwrap();

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match controller.try_frame() {
            None => break,
            Some(_) => assert!(
                Instant::now() < deadline,
                "the connection outlived an oversized input frame"
            ),
        }
    }

    // And the session itself is untouched: one client's bad frame must not
    // take down a PTY every other client is watching. The lease is free
    // again, because that connection ended.
    let mut next = Conn::connect(&socket);
    let reply = next.call(
        "attach",
        json!({ "session": &id, "from": 0, "control": true }),
    );
    assert_eq!(reply["control"], true, "the lease was not released: {reply}");
    assert_eq!(next.frame().2, vec![1]);
    next.send_input(b"alive\n");
    next.read_until("alive");

    setup.call("terminate", json!({ "session": &id }));
}

/// A resize frame is clamped like a resize op.
///
/// The control op range-checks and refuses; the frame path does not go
/// through it at all. The pty clamps the ioctl as a backstop, but
/// `runtime` and the store were written from the *request* -- so an
/// attached client could leave a row claiming a width the pty had
/// refused, and a restart would restore the session to a size it never
/// had.
#[tokio::test(flavor = "multi_thread")]
async fn a_resize_frame_is_clamped_like_the_control_op() {
    let (_dir, socket) = harness();
    let mut setup = Conn::connect(&socket);
    let id = create_shell(&mut setup);

    let mut controller = Conn::connect(&socket);
    controller.call(
        "attach",
        json!({ "session": &id, "from": 0, "control": true }),
    );
    assert_eq!(controller.frame().2, vec![1]);
    controller.read_until("ready");

    // Far past the 1..=1000 the control op enforces.
    controller
        .writer
        .write_all(&frame::encode(
            frame::RESIZE,
            0,
            &frame::encode_size(60000, 60000),
        ))
        .unwrap();
    controller.writer.flush().unwrap();

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let got = setup.call("get", json!({ "session": &id }))["session"].clone();
        if got["cols"] == 1000 && got["rows"] == 1000 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the stored size never settled at the clamp; it is {}x{}",
            got["cols"],
            got["rows"]
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    setup.call("terminate", json!({ "session": &id }));
}
