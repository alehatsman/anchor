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

    let huge = client.call("log", json!({ "session": "x", "max": 999_999_999u64 }));
    assert_eq!(huge["error"]["code"], "too_large");

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
