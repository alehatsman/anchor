//! #19: `list` and `label` must not take the store lock and a session's
//! locks in opposite orders.
//!
//! `SessionManager::label` holds a session's labels and then takes the
//! store. `op_list` used to hold the store guard across `Session::snapshot`,
//! which takes the same session's locks — store -> labels against labels ->
//! store, i.e. an ABBA that wedged the whole daemon: every worker ended up
//! on one of the two mutexes, nothing returned to `accept`, and the listen
//! backlog filled until new connections were refused outright.
//!
//! This is a race, so it is driven as a bounded storm rather than a single
//! ordered pair: several clients calling `list` while several others call
//! `label` on the same session. On the buggy build it hangs within a few
//! hundred ops; the watchdog turns that hang into a failure rather than a
//! test run that never ends.

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{json, Value};

/// Clients per side. A handful on each side is enough to interleave; more
/// only makes an already-reliable repro louder.
const CLIENTS_PER_SIDE: usize = 3;

/// Ops per client. The historical failure showed up well inside this.
const OPS_PER_CLIENT: usize = 300;

/// How long the storm may go without a SINGLE operation completing before it
/// is called a deadlock. A budget for the whole run would measure how fast
/// ~1,800 round trips are on this machine; a real deadlock does not slow
/// down, it stops, and the failure this guards is the whole daemon stopping.
const STALL: Duration = Duration::from_secs(20);

/// The daemon runs on a runtime of its own, not the test's. In the ABBA
/// state every worker is parked in `Mutex::lock`, and dropping a multi-thread
/// runtime joins its workers -- so a panic raised from inside the test's own
/// runtime would unwind into a drop that never returns, and the failure
/// message this test exists to deliver would never be seen. Here the
/// runtime is shut down in the background, which does not wait.
struct Harness {
    _dir: tempfile::TempDir,
    socket: PathBuf,
    runtime: Option<tokio::runtime::Runtime>,
}

impl Drop for Harness {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
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
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let listener = {
        let _enter = runtime.enter();
        anchor::server::bind(&socket).unwrap()
    };
    let state = anchor::server::State::new(manager, store);
    runtime.spawn(async move {
        let _ = anchor::server::serve(listener, state).await;
    });
    Harness {
        _dir: dir,
        socket,
        runtime: Some(runtime),
    }
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
        serde_json::from_str(&response).expect("a JSON response line")
    }

    fn ok(&mut self, op: &str, body: Value) -> Value {
        let value = self.call(op, body);
        assert_eq!(value["ok"], true, "{op} failed: {value}");
        value
    }
}

#[test]
fn list_and_label_do_not_deadlock_the_daemon() {
    let h = harness();
    let mut client = Client::connect(&h.socket);

    // A live session: the fold only reaches `Session::snapshot`, and so the
    // session's locks, for a row the manager still holds. A purely stored
    // row would exercise nothing.
    let created = client.ok(
        "create",
        json!({
            "spec": {
                "command": "/bin/sh",
                "args": ["-c", "sleep 300"],
                "cwd": std::env::temp_dir(),
            },
            "labels": { "wm.item": "mr:218" },
        }),
    );
    let id = created["session"]
        .as_str()
        .expect("a session id")
        .to_string();

    let (done_tx, done_rx) = mpsc::channel();
    let mut workers = Vec::new();
    let ops = Arc::new(AtomicU64::new(0));

    for n in 0..CLIENTS_PER_SIDE {
        // The lister: holds the store to read rows, then folds live state.
        let socket = h.socket.clone();
        let done = done_tx.clone();
        let list_ops = Arc::clone(&ops);
        workers.push(std::thread::spawn(move || {
            let mut client = Client::connect(&socket);
            for _ in 0..OPS_PER_CLIENT {
                client.ok("list", json!({}));
                list_ops.fetch_add(1, Ordering::Relaxed);
            }
            let _ = done.send(format!("list-{n}"));
        }));

        // The labeller: holds the session's labels, then takes the store.
        let socket = h.socket.clone();
        let done = done_tx.clone();
        let session = id.clone();
        let label_ops = Arc::clone(&ops);
        workers.push(std::thread::spawn(move || {
            let mut client = Client::connect(&socket);
            let key = format!("wm.n{n}");
            for i in 0..OPS_PER_CLIENT {
                let mut set = serde_json::Map::new();
                set.insert(key.clone(), json!(i.to_string()));
                client.ok(
                    "label",
                    json!({ "session": &session, "set": Value::Object(set) }),
                );
                label_ops.fetch_add(1, Ordering::Relaxed);
            }
            let _ = done.send(format!("label-{n}"));
        }));
    }
    drop(done_tx);

    // Watchdog. Joining the threads directly would hang forever on the
    // failure this test exists for, which reads as an infra problem rather
    // than as the bug.
    let expected = CLIENTS_PER_SIDE * 2;
    let mut finished = 0;
    let mut seen = 0;
    let mut stall_deadline = Instant::now() + STALL;
    while finished < expected {
        match done_rx.recv_timeout(Duration::from_millis(250)) {
            Ok(_) => finished += 1,
            Err(_) => {}
        }
        let now_seen = ops.load(Ordering::Relaxed);
        if now_seen > seen {
            seen = now_seen;
            stall_deadline = Instant::now() + STALL;
        }
        assert!(
            Instant::now() < stall_deadline,
            "daemon stopped answering after {seen} ops, {finished}/{expected} clients finished: \
             `list` and `label` are taking the store and a session's locks in \
             opposite orders (#19)"
        );
    }
    for worker in workers {
        worker.join().expect("client thread");
    }

    // Still serving: a client that connects after the storm is answered,
    // which is the property the backlog filling up destroyed.
    let mut after = Client::connect(&h.socket);
    after.ok("get", json!({ "session": &id }));
    after.ok("terminate", json!({ "session": &id }));
}
