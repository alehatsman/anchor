//! M0 exit criterion: the daemon binds, a client connects, control framing
//! answers. `ping` is the only op in M0; everything else is a clean error.

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;

use serde_json::Value;
use std::sync::{Arc, Mutex};

/// Serve on a socket in a temp dir for the duration of the test.
fn serve(dir: &tempfile::TempDir) -> std::path::PathBuf {
    let socket = dir.path().join("anchor.sock");
    let listener = anchor::server::bind(&socket).unwrap();
    let state = state(dir_paths(&socket));
    tokio::spawn(async move {
        let _ = anchor::server::serve(listener, state).await;
    });
    socket
}

/// A data dir beside the socket, so each test gets its own store and
/// sessions root.
fn dir_paths(socket: &std::path::Path) -> anchor::paths::Paths {
    let data = socket.parent().unwrap().join("data");
    let paths = anchor::paths::Paths::new(data, socket.to_path_buf());
    paths.ensure_dirs().unwrap();
    paths
}

fn state(paths: anchor::paths::Paths) -> anchor::server::State {
    let store = Arc::new(Mutex::new(anchor::store::Store::open(&paths).unwrap()));
    let manager = Arc::new(
        anchor::session::SessionManager::new(paths.sessions_dir()).with_store(Arc::clone(&store)),
    );
    anchor::server::State::new(manager, store)
}

struct Client {
    reader: BufReader<UnixStream>,
    writer: UnixStream,
}

impl Client {
    fn connect(socket: &Path) -> Self {
        let stream = UnixStream::connect(socket).unwrap();
        Self {
            reader: BufReader::new(stream.try_clone().unwrap()),
            writer: stream,
        }
    }

    /// Sends bytes that are deliberately not valid UTF-8, which `send_raw`
    /// cannot express because its argument is a `&str`.
    fn send_bytes(&mut self, bytes: &[u8]) -> Value {
        self.writer.write_all(bytes).unwrap();
        self.writer.write_all(b"\n").unwrap();
        self.writer.flush().unwrap();
        let mut response = String::new();
        self.reader.read_line(&mut response).unwrap();
        serde_json::from_str(&response).expect("response is a JSON line")
    }

    fn send_raw(&mut self, line: &str) -> Value {
        self.writer.write_all(line.as_bytes()).unwrap();
        self.writer.write_all(b"\n").unwrap();
        self.writer.flush().unwrap();
        let mut response = String::new();
        self.reader.read_line(&mut response).unwrap();
        serde_json::from_str(&response).expect("response is a JSON line")
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn ping_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    let socket = serve(&dir);
    let mut client = Client::connect(&socket);

    let reply = client.send_raw(r#"{"id":1,"op":"ping"}"#);
    assert_eq!(reply["id"], 1);
    assert_eq!(reply["ok"], true);
    assert_eq!(reply["pong"], true);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_connection_stays_open_across_requests() {
    let dir = tempfile::tempdir().unwrap();
    let socket = serve(&dir);
    let mut client = Client::connect(&socket);

    for id in 1..=3u64 {
        let reply = client.send_raw(&format!(r#"{{"id":{id},"op":"ping"}}"#));
        assert_eq!(reply["id"], id);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unknown_op_is_an_error_not_a_disconnect() {
    let dir = tempfile::tempdir().unwrap();
    let socket = serve(&dir);
    let mut client = Client::connect(&socket);

    // A name the daemon genuinely does not know. `create` was the stand-in
    // for this before M3 implemented it.
    let reply = client.send_raw(r#"{"id":7,"op":"teleport-me"}"#);
    assert_eq!(reply["id"], 7);
    assert_eq!(reply["ok"], false);
    assert_eq!(reply["error"]["code"], "unknown_op");

    // Still usable.
    assert_eq!(client.send_raw(r#"{"id":8,"op":"ping"}"#)["ok"], true);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_malformed_line_keeps_its_id_when_it_has_one() {
    let dir = tempfile::tempdir().unwrap();
    let socket = serve(&dir);
    let mut client = Client::connect(&socket);

    let reply = client.send_raw(r#"{"id":9}"#);
    assert_eq!(reply["id"], 9);
    assert_eq!(reply["error"]["code"], "bad_request");

    let garbage = client.send_raw("not json at all");
    assert_eq!(garbage["id"], 0);
    assert_eq!(garbage["error"]["code"], "bad_request");

    assert_eq!(client.send_raw(r#"{"id":10,"op":"ping"}"#)["ok"], true);
}

#[tokio::test(flavor = "multi_thread")]
async fn many_clients_are_served_concurrently() {
    let dir = tempfile::tempdir().unwrap();
    let socket = serve(&dir);

    let handles: Vec<_> = (0..16u64)
        .map(|n| {
            let socket = socket.clone();
            std::thread::spawn(move || {
                let mut client = Client::connect(&socket);
                let reply = client.send_raw(&format!(r#"{{"id":{n},"op":"ping"}}"#));
                assert_eq!(reply["id"], n);
                assert_eq!(reply["ok"], true);
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_socket_is_private_to_this_user() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let socket = serve(&dir);
    let mode = std::fs::metadata(&socket).unwrap().permissions().mode();
    assert_eq!(mode & 0o777, 0o600);
}

#[tokio::test(flavor = "multi_thread")]
async fn binding_refuses_to_displace_a_live_daemon() {
    let dir = tempfile::tempdir().unwrap();
    let socket = serve(&dir);
    let err = anchor::server::bind(&socket).unwrap_err().to_string();
    assert!(err.contains("already running"), "unexpected error: {err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stale_socket_file_is_replaced() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("anchor.sock");

    // A socket file with nothing accepting on it: what a crash leaves behind.
    // Dropping a listener does not unlink its path, which is exactly the mess.
    drop(anchor::server::bind(&socket).unwrap());
    assert!(socket.exists());
    assert!(UnixStream::connect(&socket).is_err());

    let listener = anchor::server::bind(&socket).unwrap();
    let state = state(dir_paths(&socket));
    tokio::spawn(async move {
        let _ = anchor::server::serve(listener, state).await;
    });
    let mut client = Client::connect(&socket);
    assert_eq!(client.send_raw(r#"{"id":1,"op":"ping"}"#)["ok"], true);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_socket_that_cannot_be_probed_is_never_removed() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("anchor.sock");

    // Not a socket at all: a plain file where one is expected. Connecting
    // fails, but not with "nobody is listening" — and anything other than
    // that says nothing about whether a daemon is alive. Deleting it on a
    // guess is how a running daemon loses its socket and its clients.
    std::fs::write(&socket, b"not a socket").unwrap();

    let err = anchor::server::bind(&socket).unwrap_err().to_string();
    assert!(err.contains("cannot determine"), "unexpected error: {err}");
    assert!(socket.exists(), "a socket we could not probe must survive");
    assert_eq!(
        std::fs::read(&socket).unwrap(),
        b"not a socket",
        "and must be left untouched"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_data_directory_is_owner_only() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    // Pre-created wide open, as an older build or a stray mkdir would.
    std::fs::create_dir_all(&data).unwrap();
    std::fs::set_permissions(&data, std::fs::Permissions::from_mode(0o755)).unwrap();

    let paths = anchor::paths::Paths::new(&data, dir.path().join("anchor.sock"));
    paths.ensure_dirs().unwrap();

    // Session logs are verbatim terminal transcripts; the directory holding
    // them must not be readable by anyone else on the machine.
    for path in [data.clone(), paths.sessions_dir()] {
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o700, "{} is {mode:o}, not 0700", path.display());
    }
}

/// A line that is not even text is still a line that is not a request
/// object, and docs/04-protocol.md says those are answered `bad_request`.
///
/// It used to be read straight into a `String`, so invalid UTF-8 failed
/// the read itself and the connection was dropped with no reply -- which a
/// client cannot tell from the daemon dying. The whole line is consumed
/// either way, so the framing survives and the connection stays usable.
#[tokio::test(flavor = "multi_thread")]
async fn a_line_that_is_not_utf8_is_answered_not_dropped() {
    let dir = tempfile::tempdir().unwrap();
    let socket = serve(&dir);
    let mut client = Client::connect(&socket);

    // A lone continuation byte: valid in no UTF-8 sequence.
    let reply = client.send_bytes(&[0x7b, 0x22, 0xff, 0xfe, 0x22, 0x7d]);
    assert_eq!(reply["id"], 0);
    assert_eq!(reply["error"]["code"], "bad_request");

    // The connection is still usable, which is the half that makes
    // answering worth more than closing.
    assert_eq!(client.send_raw(r#"{"id":2,"op":"ping"}"#)["ok"], true);
}
