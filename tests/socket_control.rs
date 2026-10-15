//! M0 exit criterion: the daemon binds, a client connects, control framing
//! answers. `ping` is the only op in M0; everything else is a clean error.

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;

use serde_json::Value;

/// Serve on a socket in a temp dir for the duration of the test.
fn serve(dir: &tempfile::TempDir) -> std::path::PathBuf {
    let socket = dir.path().join("anchor.sock");
    let listener = anchor::server::bind(&socket).unwrap();
    std::thread::spawn(move || {
        let _ = anchor::server::serve(listener);
    });
    socket
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

    fn send_raw(&mut self, line: &str) -> Value {
        self.writer.write_all(line.as_bytes()).unwrap();
        self.writer.write_all(b"\n").unwrap();
        self.writer.flush().unwrap();
        let mut response = String::new();
        self.reader.read_line(&mut response).unwrap();
        serde_json::from_str(&response).expect("response is a JSON line")
    }
}

#[test]
fn ping_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    let socket = serve(&dir);
    let mut client = Client::connect(&socket);

    let reply = client.send_raw(r#"{"id":1,"op":"ping"}"#);
    assert_eq!(reply["id"], 1);
    assert_eq!(reply["ok"], true);
    assert_eq!(reply["pong"], true);
}

#[test]
fn the_connection_stays_open_across_requests() {
    let dir = tempfile::tempdir().unwrap();
    let socket = serve(&dir);
    let mut client = Client::connect(&socket);

    for id in 1..=3u64 {
        let reply = client.send_raw(&format!(r#"{{"id":{id},"op":"ping"}}"#));
        assert_eq!(reply["id"], id);
    }
}

#[test]
fn an_unknown_op_is_an_error_not_a_disconnect() {
    let dir = tempfile::tempdir().unwrap();
    let socket = serve(&dir);
    let mut client = Client::connect(&socket);

    let reply = client.send_raw(r#"{"id":7,"op":"create"}"#);
    assert_eq!(reply["id"], 7);
    assert_eq!(reply["ok"], false);
    assert_eq!(reply["error"]["code"], "unknown_op");

    // Still usable.
    assert_eq!(client.send_raw(r#"{"id":8,"op":"ping"}"#)["ok"], true);
}

#[test]
fn a_malformed_line_keeps_its_id_when_it_has_one() {
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

#[test]
fn many_clients_are_served_concurrently() {
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

#[test]
fn the_socket_is_private_to_this_user() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let socket = serve(&dir);
    let mode = std::fs::metadata(&socket).unwrap().permissions().mode();
    assert_eq!(mode & 0o777, 0o600);
}

#[test]
fn binding_refuses_to_displace_a_live_daemon() {
    let dir = tempfile::tempdir().unwrap();
    let socket = serve(&dir);
    let err = anchor::server::bind(&socket).unwrap_err().to_string();
    assert!(err.contains("already running"), "unexpected error: {err}");
}

#[test]
fn a_stale_socket_file_is_replaced() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("anchor.sock");

    // A socket file with nothing accepting on it: what a crash leaves behind.
    // Dropping a listener does not unlink its path, which is exactly the mess.
    drop(anchor::server::bind(&socket).unwrap());
    assert!(socket.exists());
    assert!(UnixStream::connect(&socket).is_err());

    let listener = anchor::server::bind(&socket).unwrap();
    std::thread::spawn(move || {
        let _ = anchor::server::serve(listener);
    });
    let mut client = Client::connect(&socket);
    assert_eq!(client.send_raw(r#"{"id":1,"op":"ping"}"#)["ok"], true);
}
