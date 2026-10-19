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

    /// The offset and payload of the next OUTPUT frame, skipping any other tag.
    fn first_output(&mut self) -> (u64, Vec<u8>) {
        loop {
            let (tag, offset, payload) = self.frame();
            if tag == frame::OUTPUT {
                return (offset, payload);
            }
        }
    }

    /// Claims the lease. Frames on one connection are handled in order, so
    /// whatever was sent before this has been handled -- or ignored -- by the
    /// time the claim takes effect. That is the only ordering barrier a
    /// negative assertion has: two connections are two daemon tasks and
    /// nothing orders them against each other.
    fn claim(&mut self) {
        self.writer
            .write_all(&frame::encode(frame::LEASE, 0, &[1]))
            .unwrap();
        self.writer.flush().unwrap();
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

    // An observer's input is ignored, not applied. Prove it on ONE
    // connection, where order is guaranteed: the observer sends the bytes,
    // claims the lease, and types a marker. The claim is handled after the
    // bytes, so the marker is applied and the bytes -- were they ever
    // applied -- would already be in the output ahead of it.
    observers[0].send_input(b"ignored-input\n");
    observers[0].claim();
    observers[0].send_input(b"marker-after\n");
    let seen = observers[0].read_until("marker-after");
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
    // `read_until` starts a fresh accumulator per call, so the bytes consumed
    // are the SUM of the two stretches. Taking the larger of the two lands
    // the offset inside the old output, where a gap cannot be seen.
    let banner_len = first.read_until("ready").len() as u64;

    first.send_input(b"before-detach\n");
    let echoed_len = first.read_until("before-detach").len() as u64;
    let resume_at = banner_len + echoed_len;

    // Drop the connection entirely — the session must not notice.
    drop(first);

    // Reconnect at the offset we had consumed: no gap, no duplicate.
    //
    // Retried, because the lease is released when the daemon *notices* the
    // old connection is gone -- its read returns EOF and its task runs --
    // and dropping a socket does not make that happen before the next line
    // of this test. Asserting control on the first try asserted that the
    // daemon had been scheduled in between, which is true on an idle
    // machine and false under a loaded suite. A real client retries here
    // too; attach never preempts, so there is nothing else it can do.
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut second = loop {
        let mut conn = Conn::connect(&socket);
        let ready = conn.call(
            "attach",
            json!({ "session": &id, "from": resume_at, "control": true }),
        );
        assert_eq!(ready["ok"], true, "reconnect failed: {ready}");
        if ready["control"] == true {
            break conn;
        }
        assert!(
            Instant::now() < deadline,
            "the lease never became grantable after its holder vanished"
        );
        drop(conn);
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    assert_eq!(second.frame().2, vec![1], "the lease is grantable again");

    second.send_input(b"after-reconnect\n");
    // No gap, no duplicate, stated as the daemon states it: the first byte
    // after the resume point carries exactly the resume offset.
    let (offset, after) = second.first_output();
    assert_eq!(
        offset, resume_at,
        "the stream must continue exactly where the client stopped"
    );
    assert!(
        after.starts_with(b"after-reconnect"),
        "the first byte after the resume point is the new echo, not a replay: {:?}",
        String::from_utf8_lossy(&after)
    );

    // The same, for an offset that is genuinely in the past: replay starts at
    // the byte asked for, not one after it. (Resuming at the tail, above,
    // never reaches the replay arm of `Session::attach`.)
    let mut mid = Conn::connect(&socket);
    let ready = mid.call("attach", json!({ "session": &id, "from": banner_len }));
    assert_eq!(ready["ok"], true, "mid-stream attach failed: {ready}");
    assert_eq!(mid.frame().0, frame::LEASE);
    let (offset, payload) = mid.first_output();
    assert_eq!(
        offset, banner_len,
        "replay must start at the requested byte"
    );
    assert!(
        payload.starts_with(b"before-detach"),
        "replay from {banner_len} must begin with the first byte after the banner, got {:?}",
        String::from_utf8_lossy(&payload)
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
    // Ordered on the preempted connection itself: its stale input, then a
    // claim, then a marker.
    first.send_input(b"should-be-ignored\n");
    first.claim();
    first.send_input(b"marker-two\n");
    let seen = first.read_until("marker-two");
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

    // The observer's own claim, after its own input, is the barrier.
    observer.send_input(b"not-yours\n");
    observer.claim();
    observer.send_input(b"marker\n");
    let seen = observer.read_until("marker");
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

    // Let the 100 ms coalescing window close. This is not a wait for a
    // result -- nothing below depends on it for correctness -- it is what
    // makes this test able to FAIL: a resize that arrived inside the window
    // is only queued, and an observer resize wrongly honoured would still
    // look "not applied" when we look.
    tokio::time::sleep(Duration::from_millis(150)).await;
    observer
        .writer
        .write_all(&frame::encode(frame::RESIZE, 0, &frame::encode_size(20, 5)))
        .unwrap();
    observer.writer.flush().unwrap();

    // The claim is handled after the resize frame on the same connection;
    // its grant is the proof that the resize was read and refused.
    observer.claim();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let (tag, _, payload) = observer.frame();
        if tag == frame::LEASE && payload == vec![1] {
            break;
        }
        assert!(Instant::now() < deadline, "the claim was never granted");
    }
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
        assert_eq!(
            created["ok"], true,
            "round {round}: create failed: {created}"
        );
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
                        assert_eq!(payload.len(), 4, "round {round}: an exit code is an i32");
                        break;
                    }
                }
                None => panic!("round {round}: the stream ended without an exit frame"),
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
    controller.send_input(b"\x04"); // 0x04 is not EOF here: the session is in raw mode, so it is just a byte
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
    assert_eq!(
        reply["control"], true,
        "the lease was not released: {reply}"
    );
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

/// Every attached client is told when the size changes, including the one
/// that asked.
///
/// Output is bytes and geometry is not in them, so a client resized by
/// some *other* client has nothing it can observe that says so -- it just
/// keeps rendering at a width the pty no longer has. Same reasoning as the
/// lease: anchor is opinionated about there being one effective size, so
/// it owes every attached client the current one.
#[tokio::test(flavor = "multi_thread")]
async fn a_resize_is_announced_to_everyone_attached() {
    let (_dir, socket) = harness();
    let mut setup = Conn::connect(&socket);
    let id = create_shell(&mut setup);

    let mut controller = Conn::connect(&socket);
    let reply = controller.call(
        "attach",
        json!({ "session": &id, "from": 0, "control": true }),
    );
    // The reply carries the size, so a client starts from the truth rather
    // than waiting for a change that may never come.
    assert_eq!(
        reply["cols"], 80,
        "attach reply is missing the size: {reply}"
    );
    assert_eq!(reply["rows"], 24);
    assert_eq!(controller.frame().2, vec![1]);
    controller.read_until("ready");

    let mut observer = Conn::connect(&socket);
    let seen = observer.call("attach", json!({ "session": &id, "from": 0 }));
    assert_eq!(seen["cols"], 80);
    assert_eq!(observer.frame().2, vec![0]);

    // The controller resizes; the observer must hear about it.
    controller
        .writer
        .write_all(&frame::encode(
            frame::RESIZE,
            0,
            &frame::encode_size(133, 51),
        ))
        .unwrap();
    controller.writer.flush().unwrap();

    assert_eq!(
        next_resize(&mut observer),
        (133, 51),
        "a bystander was never told the terminal changed size"
    );
    // And the originator too: it must not assume what it asked for is what
    // it got.
    assert_eq!(next_resize(&mut controller), (133, 51));

    setup.call("terminate", json!({ "session": &id }));
}

/// What is announced is the size that was *applied*, not the size that was
/// asked for.
///
/// A client that asks past the clamp and is told its own request back
/// would render at a width the pty refused -- the echo would be worse than
/// silence, because it looks authoritative.
#[tokio::test(flavor = "multi_thread")]
async fn the_announced_size_is_the_one_that_was_applied() {
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

    controller
        .writer
        .write_all(&frame::encode(
            frame::RESIZE,
            0,
            &frame::encode_size(5000, 5000),
        ))
        .unwrap();
    controller.writer.flush().unwrap();

    assert_eq!(
        next_resize(&mut controller),
        (1000, 1000),
        "the client was told its own request back instead of the clamp"
    );

    setup.call("terminate", json!({ "session": &id }));
}

/// Reads frames until a resize arrives, returning its size. Output frames
/// in between are ordinary -- the session is still running.
fn next_resize(conn: &mut Conn) -> (u16, u16) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let (tag, _offset, payload) = conn.frame();
        if tag == frame::RESIZE {
            assert_eq!(payload.len(), 4, "a resize payload is two u16s");
            return (
                u16::from_be_bytes([payload[0], payload[1]]),
                u16::from_be_bytes([payload[2], payload[3]]),
            );
        }
        assert!(
            Instant::now() < deadline,
            "no resize frame arrived; last tag was {tag:#04x}"
        );
    }
}

/// `read_until` for a long stream: it keeps only the tail it needs
/// to match, where `read_until` rescans everything received on every frame.
fn drain_until(conn: &mut Conn, needle: &[u8]) {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut tail: Vec<u8> = Vec::new();
    loop {
        let (tag, _, payload) = conn.frame();
        if tag == frame::OUTPUT {
            tail.extend_from_slice(&payload);
            if tail.windows(needle.len()).any(|w| w == needle) {
                return;
            }
            let keep = tail.len().saturating_sub(needle.len());
            tail.drain(..keep);
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {needle:?}"
        );
    }
}

/// A session whose child reads nothing from its stdin, ever.
fn create_deaf(conn: &mut Conn) -> String {
    let response = conn.call(
        "create",
        json!({
            "spec": {
                "command": "/bin/sh",
                "args": ["-c", "stty raw -echo; exec sleep 600"],
                "cwd": std::env::temp_dir(),
            }
        }),
    );
    assert_eq!(response["ok"], true, "create failed: {response}");
    response["session"].as_str().unwrap().to_string()
}

/// A connection that dies between the daemon taking the lease for it and the
/// daemon finishing the handshake must not leave that lease held by a holder
/// that no longer exists. Before the guard was built at the decision, the
/// attach reply failing to send returned past it, and every later
/// `control: true` attach was silently downgraded to observer for the rest
/// of the session.
#[tokio::test(flavor = "multi_thread")]
async fn a_connection_that_dies_mid_handshake_does_not_strand_the_lease() {
    let (_dir, socket) = harness();
    let mut setup = Conn::connect(&socket);
    let id = create_shell(&mut setup);

    // Send the attach and close without reading a byte. The daemon still
    // reads the line (data sent before a close stays readable), takes the
    // lease, and then finds nobody to answer.
    for _ in 0..5 {
        let mut line = serde_json::to_vec(&json!({
            "id": 1, "op": "attach", "session": &id, "from": 0, "control": true,
        }))
        .unwrap();
        line.push(b'\n');
        let mut dead = UnixStream::connect(&socket).unwrap();
        dead.write_all(&line).unwrap();
        drop(dead);
    }

    // The lease must come free. Each probe is a real attach, so a probe that
    // loses to a handler still unwinding is just retried.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let mut probe = Conn::connect(&socket);
        let r = probe.call(
            "attach",
            json!({ "session": &id, "from": 0, "control": true }),
        );
        assert_eq!(r["ok"], true, "attach failed: {r}");
        if r["control"] == true {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the lease is still held by a connection that is gone"
        );
        drop(probe);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    setup.call("terminate", json!({ "session": &id }));
}

/// A claim is not undone by a broadcast that was already in flight. The
/// output half used to decide "mine" by name alone, so a stale announcement
/// that someone else had taken the lease -- read after this connection had
/// claimed it back -- wiped the new epoch: the client was told it held
/// control while every keystroke was discarded.
///
/// The stale announcement is made deterministic by parking A's output half
/// in a write nobody is reading: B's claim queues behind it, and so does the
/// claim A then makes.
#[tokio::test(flavor = "multi_thread")]
async fn a_claim_survives_a_stale_broadcast_about_someone_else() {
    let (_dir, socket) = harness();
    let mut setup = Conn::connect(&socket);
    // Waits for a line, then emits far more than a socket buffer holds and
    // far less than a subscriber's queue budget, then echoes input.
    let created = setup.call(
        "create",
        json!({
            "spec": {
                "command": "/bin/sh",
                "args": ["-c", "stty raw -echo; echo ready; read x; yes x | head -c 300000; echo burst-over; exec cat"],
                "cwd": std::env::temp_dir(),
            }
        }),
    );
    assert_eq!(created["ok"], true, "create failed: {created}");
    let id = created["session"].as_str().unwrap().to_string();

    let mut a = Conn::connect(&socket);
    a.call(
        "attach",
        json!({ "session": &id, "from": 0, "control": true }),
    );
    assert_eq!(a.frame().2, vec![1], "A starts as the controller");
    a.read_until("ready");

    let mut b = Conn::connect(&socket);
    b.call("attach", json!({ "session": &id, "from": 0 }));
    assert_eq!(b.frame().2, vec![0]);

    // A starts the burst and then stops reading, so its output half parks in
    // a write. B drains the burst, which is how the test knows it is over.
    a.send_input(b"go\n");
    drain_until(&mut b, b"burst-over");

    let claim = frame::encode(frame::LEASE, 0, &[1]);
    b.writer.write_all(&claim).unwrap();
    // B's own grant is the proof that its claim has been applied and
    // announced -- and A's output half, parked, has not read it.
    loop {
        let (tag, _, payload) = b.frame();
        if tag == frame::LEASE && payload == vec![1] {
            break;
        }
    }

    // A claims it back. B being told it lost is the proof A's claim landed.
    a.writer.write_all(&claim).unwrap();
    loop {
        let (tag, _, payload) = b.frame();
        if tag == frame::LEASE && payload == vec![0] {
            break;
        }
    }

    // A reads again: the stale "B has it" comes first, then "A has it".
    // Wait until it has been told it holds control, then type.
    loop {
        let (tag, _, payload) = a.frame();
        if tag == frame::LEASE && payload == vec![1] {
            break;
        }
    }
    a.send_input(b"typed-after-claim\n");
    drain_until(&mut a, b"typed-after-claim");

    setup.call("terminate", json!({ "session": &id }));
}

/// A child that has stopped reading its stdin must not take the daemon's
/// runtime workers with it. Input used to be written from the async task:
/// once the write channel filled, each frame parked a worker in a retry
/// sleep, so as many stuck controllers as there are workers stalled `accept`
/// and every other session.
///
/// Two workers and two stuck sessions make that exact: if either flood is
/// still on a worker, an unrelated control op cannot be answered.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_child_that_stops_reading_does_not_stall_the_daemon() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let (_dir, socket) = harness();
    let mut setup = Conn::connect(&socket);
    let ids = [create_deaf(&mut setup), create_deaf(&mut setup)];

    let stop = Arc::new(AtomicBool::new(false));
    let mut jammed = Vec::new();
    let mut floods = Vec::new();
    for id in &ids {
        let mut conn = Conn::connect(&socket);
        let r = conn.call(
            "attach",
            json!({ "session": id, "from": 0, "control": true }),
        );
        assert_eq!(r["control"], true);
        let writer = conn.writer.try_clone().unwrap();
        writer
            .set_write_timeout(Some(Duration::from_millis(100)))
            .unwrap();
        let stuck = Arc::new(AtomicBool::new(false));
        jammed.push(Arc::clone(&stuck));
        let stop = Arc::clone(&stop);
        floods.push((
            conn,
            std::thread::spawn(move || {
                let mut writer = writer;
                let frame = frame::encode(frame::INPUT, 0, &[b'x'; 1024]);
                while !stop.load(Ordering::SeqCst) {
                    // Whole frames, resumed after a timeout: a torn frame
                    // would desynchronise the stream and end the connection.
                    let mut sent = 0;
                    while sent < frame.len() && !stop.load(Ordering::SeqCst) {
                        match writer.write(&frame[sent..]) {
                            Ok(n) => sent += n,
                            // A write that times out means the daemon has
                            // stopped taking input off this socket: the jam
                            // is established. Keep pushing -- it must have
                            // input waiting for as long as the test looks.
                            Err(e)
                                if matches!(
                                    e.kind(),
                                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                                ) =>
                            {
                                stuck.store(true, Ordering::SeqCst);
                            }
                            Err(_) => return,
                        }
                    }
                }
            }),
        ));
    }

    let deadline = Instant::now() + Duration::from_secs(30);
    while !jammed.iter().all(|j| j.load(Ordering::SeqCst)) {
        assert!(Instant::now() < deadline, "the floods never jammed");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Unrelated work, answered while both stuck sessions are still stuck.
    let started = Instant::now();
    let listed = setup.call("list", json!({}));
    assert_eq!(listed["ok"], true);
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "a control op took {:?} with two stuck children",
        started.elapsed()
    );

    stop.store(true, Ordering::SeqCst);
    for (conn, flood) in floods {
        let _ = conn.writer.shutdown(std::net::Shutdown::Both);
        flood.join().unwrap();
    }
    for id in &ids {
        setup.call("terminate", json!({ "session": id }));
    }
}
