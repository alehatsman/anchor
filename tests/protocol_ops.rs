//! M3: the control ops, driven over a real socket against a real daemon
//! state — create a session, find it by label, read its log, terminate it.

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{json, Value};

struct Harness {
    _dir: tempfile::TempDir,
    socket: std::path::PathBuf,
}

impl Harness {
    fn session_dir(&self, id: &str) -> std::path::PathBuf {
        self._dir.path().join("data/sessions").join(id)
    }
}

fn harness() -> Harness {
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
    Harness { _dir: dir, socket }
}

struct Client {
    reader: BufReader<UnixStream>,
    writer: UnixStream,
    next_id: u64,
}

impl Client {
    fn connect(socket: &Path) -> Self {
        let stream = UnixStream::connect(socket).unwrap();
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
        let value: Value = serde_json::from_str(&response).expect("a JSON response line");
        assert_eq!(value["id"], self.next_id, "responses are correlated by id");
        value
    }

    /// Calls and asserts success, returning the response.
    fn ok(&mut self, op: &str, body: Value) -> Value {
        let value = self.call(op, body);
        assert_eq!(value["ok"], true, "{op} failed: {value}");
        value
    }
}

/// Spawns a shell that writes `text` and stays alive, so the session is
/// still running when the test inspects it.
/// Runs `script` directly. `create_echo` wraps its argument in `printf`,
/// so a script passed there becomes text the shell prints rather than code
/// it runs — which quietly turns a test of stubborn children into a test of
/// nothing.
fn create_script(client: &mut Client, script: &str) -> String {
    let response = client.ok(
        "create",
        json!({
            "spec": {
                "command": "/bin/sh",
                "args": ["-c", script],
                "cwd": std::env::temp_dir(),
            },
        }),
    );
    assert_eq!(response["state"], "running");
    response["session"]
        .as_str()
        .expect("a session id")
        .to_string()
}

fn create_echo(client: &mut Client, text: &str, labels: Value) -> String {
    let response = client.ok(
        "create",
        json!({
            "spec": {
                "command": "/bin/sh",
                "args": ["-c", format!("stty raw -echo; printf '{text}'; sleep 30")],
                "cwd": std::env::temp_dir(),
            },
            "labels": labels,
        }),
    );
    assert_eq!(response["state"], "running");
    assert_eq!(response["output_offset"], 0);
    assert!(
        response["pid"].as_u64().is_some(),
        "a running session has a pid"
    );
    response["session"]
        .as_str()
        .expect("a session id")
        .to_string()
}

/// Polls `log` until the session has produced `needle`.
fn wait_for_output(client: &mut Client, id: &str, needle: &str) -> Vec<u8> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let response = client.ok("log", json!({ "session": id, "from": 0, "max": 65536 }));
        let bytes: Vec<u8> = response["bytes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|b| b.as_u64().unwrap() as u8)
            .collect();
        if bytes.windows(needle.len()).any(|w| w == needle.as_bytes()) {
            return bytes;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {needle:?}; got {:?}",
            String::from_utf8_lossy(&bytes)
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn create_get_log_and_terminate() {
    let h = harness();
    let mut client = Client::connect(&h.socket);

    let id = create_echo(&mut client, "hello anchor", json!({ "wm.item": "mr:218" }));

    let bytes = wait_for_output(&mut client, &id, "hello anchor");
    assert!(!bytes.is_empty());

    let got = client.ok("get", json!({ "session": &id }))["session"].clone();
    assert_eq!(got["id"], id.as_str());
    assert_eq!(got["state"], "running");
    assert_eq!(got["labels"]["wm.item"], "mr:218");
    assert_eq!(got["command"], "/bin/sh");
    assert!(got["output_bytes"].as_u64().unwrap() >= bytes.len() as u64);

    // A spec's env may carry secrets a client passed in; the response says
    // how many there are, never what they are.
    assert!(got.get("env").is_none(), "env must not be echoed back");
    assert_eq!(got["env_count"], 0);

    let terminated = client.ok("terminate", json!({ "session": &id }));
    assert!(terminated["ok"].as_bool().unwrap());

    // Terminating twice is not an error.
    let again = client.call("terminate", json!({ "session": &id }));
    assert_eq!(again["ok"], true, "terminate is idempotent: {again}");

    let after = client.ok("get", json!({ "session": &id }))["session"].clone();
    assert_eq!(after["state"], "exited");
}

#[tokio::test(flavor = "multi_thread")]
async fn list_filters_by_label() {
    let h = harness();
    let mut client = Client::connect(&h.socket);

    let a = create_echo(
        &mut client,
        "a",
        json!({ "wm.item": "mr:1", "wm.agent": "impl-a" }),
    );
    let b = create_echo(
        &mut client,
        "b",
        json!({ "wm.item": "mr:1", "wm.agent": "impl-b" }),
    );
    let c = create_echo(&mut client, "c", json!({ "wm.item": "mr:2" }));

    let all = client.ok("list", json!({}))["sessions"]
        .as_array()
        .unwrap()
        .len();
    assert_eq!(all, 3);

    let by_item = client.ok("list", json!({ "filter": { "wm.item": "mr:1" } }));
    let ids: Vec<&str> = by_item["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids.len(), 2);
    assert!(ids.contains(&a.as_str()) && ids.contains(&b.as_str()));

    // Every pair must match.
    let both = client.ok(
        "list",
        json!({ "filter": { "wm.item": "mr:1", "wm.agent": "impl-b" } }),
    );
    let ids: Vec<&str> = both["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec![b.as_str()]);

    for id in [&a, &b, &c] {
        client.ok("terminate", json!({ "session": id }));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn labels_are_editable_over_the_wire() {
    let h = harness();
    let mut client = Client::connect(&h.socket);
    let id = create_echo(
        &mut client,
        "x",
        json!({ "wm.item": "mr:1", "wm.stale": "yes" }),
    );

    let updated = client.ok(
        "label",
        json!({ "session": &id, "set": { "wm.agent": "impl-b" }, "remove": ["wm.stale"] }),
    );
    assert_eq!(updated["labels"]["wm.agent"], "impl-b");
    assert!(updated["labels"].get("wm.stale").is_none());

    // The query sees the edit.
    let found = client.ok("list", json!({ "filter": { "wm.agent": "impl-b" } }));
    assert_eq!(found["sessions"].as_array().unwrap().len(), 1);

    // An invalid label is refused with its own code.
    let bad = client.call(
        "label",
        json!({ "session": &id, "set": { "WM.Item": "x" } }),
    );
    assert_eq!(bad["ok"], false);
    assert_eq!(bad["error"]["code"], "invalid_label");

    // Labels outlive the process they describe.
    client.ok("terminate", json!({ "session": &id }));
    let after_exit = client.ok(
        "label",
        json!({ "session": &id, "set": { "wm.outcome": "done" } }),
    );
    assert_eq!(after_exit["labels"]["wm.outcome"], "done");
}

#[tokio::test(flavor = "multi_thread")]
async fn resize_applies_and_is_bounded() {
    let h = harness();
    let mut client = Client::connect(&h.socket);
    let id = create_echo(&mut client, "x", json!({}));

    let resized = client.ok("resize", json!({ "session": &id, "cols": 120, "rows": 40 }));
    assert_eq!(resized["cols"], 120);
    let got = client.ok("get", json!({ "session": &id }))["session"].clone();
    assert_eq!(got["cols"], 120);
    assert_eq!(got["rows"], 40);

    for (cols, rows) in [(0, 24), (80, 0), (1001, 24)] {
        let bad = client.call(
            "resize",
            json!({ "session": &id, "cols": cols, "rows": rows }),
        );
        assert_eq!(bad["ok"], false, "{cols}x{rows} must be refused");
        assert_eq!(bad["error"]["code"], "invalid_spec");
    }

    client.ok("terminate", json!({ "session": &id }));
}

#[tokio::test(flavor = "multi_thread")]
async fn errors_are_specific_and_never_drop_the_connection() {
    let h = harness();
    let mut client = Client::connect(&h.socket);

    let missing = client.call("get", json!({ "session": "01JNOPE0000000000000000000" }));
    assert_eq!(missing["error"]["code"], "not_found");

    let bad_command = client.call(
        "create",
        json!({ "spec": { "command": "definitely-not-on-path-xyz" } }),
    );
    assert_eq!(bad_command["error"]["code"], "invalid_spec");

    let bad_cwd = client.call(
        "create",
        json!({ "spec": { "command": "/bin/sh", "cwd": "/nope/not/a/dir" } }),
    );
    assert_eq!(bad_cwd["error"]["code"], "invalid_spec");

    let bad_label = client.call(
        "create",
        json!({ "spec": { "command": "/bin/sh" }, "labels": { "Bad Key": "v" } }),
    );
    assert_eq!(bad_label["error"]["code"], "invalid_label");

    let missing_field = client.call("resize", json!({ "session": "x" }));
    assert_eq!(missing_field["error"]["code"], "invalid_spec");

    // A huge `max` is clamped, so what comes back is the session error --
    // not a complaint about the size. See
    // `an_oversized_log_range_is_clamped_not_refused`: this used to answer
    // `too_large` and that broke paging through a long log.
    let huge = client.call("log", json!({ "session": "x", "max": 999_999_999u64 }));
    assert_eq!(huge["error"]["code"], "not_found");

    // Still usable after all of that.
    assert_eq!(client.ok("ping", json!({}))["pong"], true);
}

#[tokio::test(flavor = "multi_thread")]
async fn log_serves_byte_ranges_from_an_offset() {
    let h = harness();
    let mut client = Client::connect(&h.socket);
    let id = create_echo(&mut client, "0123456789", json!({}));

    let all = wait_for_output(&mut client, &id, "0123456789");
    let start = all
        .windows(10)
        .position(|w| w == b"0123456789")
        .expect("the payload is in the log");

    let tail = client.ok(
        "log",
        json!({ "session": &id, "from": start + 5, "max": 5 }),
    );
    let bytes: Vec<u8> = tail["bytes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|b| b.as_u64().unwrap() as u8)
        .collect();
    assert_eq!(bytes, b"56789");
    assert_eq!(tail["from"], start as u64 + 5);
    assert_eq!(tail["to"], start as u64 + 10);
    assert_eq!(tail["log_capped_at"], Value::Null);

    client.ok("terminate", json!({ "session": &id }));
}

#[tokio::test(flavor = "multi_thread")]
async fn purge_forgets_a_session_entirely() {
    let h = harness();
    let mut client = Client::connect(&h.socket);
    let id = create_echo(&mut client, "purge-me", json!({ "wm.item": "mr:9" }));
    wait_for_output(&mut client, &id, "purge-me");

    // A running session is not purgeable: deleting the log of a live PTY
    // would leave the reader appending to a file nobody can read.
    let refused = client.call("purge", json!({ "session": &id }));
    assert_eq!(refused["ok"], false);
    assert_eq!(refused["error"]["code"], "already_exited");

    client.ok("terminate", json!({ "session": &id }));
    let purged = client.ok("purge", json!({ "session": &id }));
    assert_eq!(purged["purged"], true);

    // Gone from get, from list, and from the filesystem.
    let gone = client.call("get", json!({ "session": &id }));
    assert_eq!(gone["error"]["code"], "not_found");
    assert!(
        client.ok("list", json!({ "filter": { "wm.item": "mr:9" } }))["sessions"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let gone_log = client.call("log", json!({ "session": &id, "from": 0 }));
    assert_eq!(gone_log["error"]["code"], "not_found");

    // Purging what was never there is a 404, so a typo is reported rather
    // than silently succeeding.
    let missing = client.call("purge", json!({ "session": "01JNOPE0000000000000000000" }));
    assert_eq!(missing["error"]["code"], "not_found");
}

/// A slow terminate must not stall unrelated connections.
///
/// The bounded wait in the termination policy is up to ~7s, and a child
/// that ignores SIGHUP and SIGTERM takes all of it. Enough of those at once
/// will occupy every runtime worker unless the blocking work is moved off
/// them — so this saturates deliberately rather than firing one and hoping
/// the scheduler picks a different thread.
#[tokio::test(flavor = "multi_thread")]
async fn a_slow_terminate_does_not_stall_other_connections() {
    let h = harness();
    let mut control = Client::connect(&h.socket);

    let workers = std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(4);
    let stubborn = workers * 2;

    let mut ids = Vec::new();
    for _ in 0..stubborn {
        // Ignores both graceful signals, so terminate must run its whole
        // wait and then hard-kill.
        let id = create_script(
            &mut control,
            "trap '' HUP TERM; stty raw -echo; echo stubborn; while :; do sleep 1; done",
        );
        ids.push(id);
    }
    for id in &ids {
        wait_for_output(&mut control, id, "stubborn");
    }

    // Fire every terminate at once, each on its own connection.
    let mut terminators = Vec::new();
    for id in &ids {
        let socket = h.socket.clone();
        let id = id.clone();
        terminators.push(std::thread::spawn(move || {
            let mut conn = Client::connect(&socket);
            conn.call("terminate", json!({ "session": id }));
        }));
    }

    // While those are blocked in their graceful waits, an unrelated
    // connection must still be answered promptly.
    let start = Instant::now();
    let mut bystander = Client::connect(&h.socket);
    assert_eq!(bystander.ok("ping", json!({}))["pong"], true);
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(2),
        "a ping waited {elapsed:?} behind {stubborn} terminates; slow work is \
         holding runtime workers"
    );

    for t in terminators {
        t.join().expect("a terminate panicked");
    }
}

/// A failure that is the daemon's own must not be reported as a bad
/// request.
///
/// `invalid_spec` is a statement about what the client sent: it means do
/// not retry, the same bytes will fail the same way. Answering it when the
/// log file could not be read sends whoever is debugging to stare at a
/// request that was fine, and tells a client not to retry something that
/// might well succeed a moment later. `internal` is the only code that
/// says "not your fault".
#[tokio::test(flavor = "multi_thread")]
async fn a_daemon_side_failure_is_internal_not_invalid_spec() {
    let setup = harness();
    let mut client = Client::connect(&setup.socket);
    let id = create_echo(&mut client, "ready", json!({}));
    wait_for_output(&mut client, &id, "ready");

    // Make the log unreadable underneath a session that is otherwise
    // perfectly healthy, so the request is unimpeachable and only the
    // daemon's own storage is broken.
    let log = setup.session_dir(&id).join("output.vt");
    let mut perms = std::fs::metadata(&log).unwrap().permissions();
    std::os::unix::fs::PermissionsExt::set_mode(&mut perms, 0o000);
    std::fs::set_permissions(&log, perms).unwrap();

    let reply = client.call("log", json!({ "session": &id, "from": 0, "max": 4096 }));
    assert_eq!(reply["ok"], false, "the unreadable log was served: {reply}");
    assert_eq!(
        reply["error"]["code"], "internal",
        "a storage failure was blamed on the request: {reply}"
    );

    // And the connection survives it, like every other error.
    assert_eq!(client.call("ping", json!({}))["ok"], true);

    let mut perms = std::fs::metadata(&log).unwrap().permissions();
    std::os::unix::fs::PermissionsExt::set_mode(&mut perms, 0o600);
    std::fs::set_permissions(&log, perms).unwrap();
    client.call("terminate", json!({ "session": &id }));
}

/// The codes that *are* about the request keep saying so. Changing where
/// `internal` applies must not quietly reclassify the ordinary refusals a
/// client branches on.
#[tokio::test(flavor = "multi_thread")]
async fn the_request_level_codes_are_unchanged() {
    let setup = harness();
    let mut client = Client::connect(&setup.socket);

    let missing = client.call("get", json!({ "session": "01NOPE" }));
    assert_eq!(missing["error"]["code"], "not_found");

    let bad_size = client.call(
        "create",
        json!({ "spec": { "command": "/bin/sh", "args": [], "cwd": "/tmp",
                          "cols": 0, "rows": 0 } }),
    );
    assert_eq!(bad_size["error"]["code"], "invalid_spec");

    let bad_cmd = client.call(
        "create",
        json!({ "spec": { "command": "/nope/not/a/command", "args": [], "cwd": "/tmp" } }),
    );
    assert_eq!(bad_cmd["error"]["code"], "invalid_spec");

    let bad_label = client.call(
        "label",
        json!({ "session": "01NOPE", "set": { "": "empty key" } }),
    );
    assert!(
        ["invalid_label", "not_found"].contains(&bad_label["error"]["code"].as_str().unwrap()),
        "unexpected: {bad_label}"
    );
}

/// Asking for more than one response can carry is paging, not an error.
///
/// docs/04-protocol.md has always said a `log` response is "capped at
/// 4 MiB whatever `max` asks for". Refusing the request instead made that
/// false where it counts: a client walking a long log asks for the range
/// it wants and pages through what comes back, so refusing the *first*
/// call means the loop never runs and a log past 4 MiB cannot be read by
/// range at all. Found from the client side by teleport, whose catch-up
/// loop this silently disabled.
#[tokio::test(flavor = "multi_thread")]
async fn an_oversized_log_range_is_clamped_not_refused() {
    let setup = harness();
    let mut client = Client::connect(&setup.socket);
    let id = create_echo(&mut client, "ready", json!({}));
    let seen = wait_for_output(&mut client, &id, "ready");

    let reply = client.call(
        "log",
        json!({ "session": &id, "from": 0, "max": 64 * 1024 * 1024u64 }),
    );
    assert_eq!(
        reply["ok"], true,
        "a range wider than one response was refused: {reply}"
    );
    let bytes = reply["bytes"].as_array().expect("a byte array");
    assert!(
        !bytes.is_empty() && bytes.len() <= 4 * 1024 * 1024,
        "expected a clamped range, got {} bytes",
        bytes.len()
    );
    assert!(
        bytes.len() >= seen.len(),
        "the clamp lost output that was already there"
    );

    client.call("terminate", json!({ "session": &id }));
}

/// Terminate, then purge, with nothing in between.
///
/// `terminate` returns once the child is reaped, but the session's state
/// is written by the thread draining the pty's exit channel -- a different
/// thread, with nothing ordering the two. A reply sent before that landed
/// said `"state": "closing"` for a successful terminate, and refused the
/// `purge` a client sends next, because `forget` requires a terminal
/// state. The obvious two-call sequence raced against itself.
///
/// Honest about what this proves: it is a regression guard, not a
/// reproduction. With the wait removed it still passes, because the exit
/// is already in the channel by the time `terminate` returns and the
/// writing thread almost always gets scheduled first. The ordering gap is
/// real by construction and the fix closes it by construction; this test
/// catches the gross regression -- a reply that reports `closing`, or a
/// purge refused outright -- not the narrow interleaving.
///
/// Found from the client side by teleport, whose purge does exactly this
/// with no wait between the two calls.
#[tokio::test(flavor = "multi_thread")]
async fn terminate_then_purge_never_races() {
    let setup = harness();
    let mut client = Client::connect(&setup.socket);

    for round in 0..15 {
        let id = create_echo(&mut client, "ready", json!({}));
        wait_for_output(&mut client, &id, "ready");

        let ended = client.call("terminate", json!({ "session": &id }));
        assert_eq!(ended["ok"], true, "round {round}: {ended}");
        assert!(
            ["exited", "lost"].contains(&ended["state"].as_str().unwrap_or("")),
            "round {round}: terminate reported a non-terminal state: {ended}"
        );

        let purged = client.call("purge", json!({ "session": &id }));
        assert_eq!(
            purged["ok"], true,
            "round {round}: purge lost the race with terminate: {purged}"
        );
    }
}

/// The `log` reply is serialised from a typed struct now, to stop a 4 MiB
/// read costing `size_of::<Value>()` per byte. The wire must not notice.
///
/// Compared byte for byte against what the old `json!` construction emits
/// for the same values, including bytes that are not ASCII.
#[tokio::test(flavor = "multi_thread")]
async fn the_log_reply_is_byte_identical_to_the_value_tree_it_replaced() {
    let h = harness();
    let mut client = Client::connect(&h.socket);
    let id = create_echo(&mut client, r"a\377\000b", json!({}));
    wait_for_output(&mut client, &id, "b");

    let mut stream = UnixStream::connect(&h.socket).unwrap();
    let request = format!(r#"{{"id":9,"op":"log","session":"{id}","from":0,"max":65536}}"#);
    stream.write_all(request.as_bytes()).unwrap();
    stream.write_all(b"\n").unwrap();
    let mut raw = Vec::new();
    BufReader::new(stream).read_until(b'\n', &mut raw).unwrap();

    let got: Value = serde_json::from_slice(&raw).unwrap();
    assert_eq!(got["ok"], true, "{got}");
    assert!(
        got["bytes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|b| b.as_u64().unwrap() > 127),
        "the fixture must carry a non-ASCII byte: {got}"
    );

    let old = anchor::proto::ok(
        9,
        json!({
            "from": got["from"],
            "to": got["to"],
            "next_offset": got["next_offset"],
            "log_capped_at": got["log_capped_at"],
            "bytes": got["bytes"],
        }),
    );
    let mut expected = serde_json::to_vec(&old).unwrap();
    expected.push(b'\n');
    assert_eq!(
        String::from_utf8_lossy(&raw),
        String::from_utf8_lossy(&expected)
    );
    assert_eq!(raw, expected);
}

/// `decode_size` is documented `None` unless the payload is four bytes; it
/// accepted four *or more*.
#[test]
fn decode_size_wants_exactly_four_bytes() {
    use anchor::proto::frame::decode_size;
    assert_eq!(decode_size(&[0, 80, 0, 24]), Some((80, 24)));
    assert_eq!(decode_size(&[0, 80, 0]), None);
    assert_eq!(decode_size(&[0, 80, 0, 24, 9]), None);
}

/// The resize range error states the range the check actually uses.
#[tokio::test(flavor = "multi_thread")]
async fn the_resize_range_message_comes_from_the_range() {
    let h = harness();
    let mut client = Client::connect(&h.socket);
    let reply = client.call("resize", json!({ "session": "x", "cols": 0, "rows": 24 }));
    let range = anchor::pty::SIZE_RANGE;
    let want = format!("between {} and {}", range.start(), range.end());
    assert!(
        reply["error"]["message"].as_str().unwrap().contains(&want),
        "{reply}"
    );
}
