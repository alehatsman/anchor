//! A slow `create` must not stall other connections.
//!
//! `create` resolves a bare command through the user's login shell when
//! `login_shell` is set, and runs that shell to completion. Done on a runtime
//! worker, a slow shell startup (a heavy `.zshrc`) froze every connection
//! sharing the worker. The runtime here has ONE worker, so any blocking
//! `create` holds the whole daemon and a `ping` on another connection waits
//! behind it.
//!
//! One `#[test]` in the file because it sets `SHELL` for the whole process;
//! see `pty_login_shell.rs` for the same tradeoff.

#![cfg(unix)]

use std::io::{BufRead, BufReader, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{json, Value};

const SHELL_STARTUP: Duration = Duration::from_secs(3);

fn call(stream: &mut UnixStream, request: Value) -> Value {
    let mut line = serde_json::to_vec(&request).unwrap();
    line.push(b'\n');
    stream.write_all(&line).unwrap();
    let mut response = String::new();
    BufReader::new(stream.try_clone().unwrap())
        .read_line(&mut response)
        .unwrap();
    serde_json::from_str(&response).unwrap()
}

#[tokio::test]
async fn a_slow_create_does_not_stall_other_connections() {
    let dir = tempfile::tempdir().unwrap();

    // A login shell that takes `SHELL_STARTUP` to answer anything.
    let shell = dir.path().join("slow-shell");
    std::fs::write(
        &shell,
        format!("#!/bin/sh\nsleep {}\nexit 0\n", SHELL_STARTUP.as_secs()),
    )
    .unwrap();
    std::fs::set_permissions(&shell, std::fs::Permissions::from_mode(0o755)).unwrap();
    // SAFETY: this file's only test; nothing else in the process reads it.
    unsafe { std::env::set_var("SHELL", &shell) };

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

    let cwd = dir.path().to_path_buf();
    let creating_socket = socket.clone();
    let creating = tokio::task::spawn_blocking(move || {
        let mut stream = UnixStream::connect(creating_socket).unwrap();
        let started = Instant::now();
        // Not on PATH, so the only way to resolve it is the slow shell.
        call(
            &mut stream,
            json!({
                "id": 1, "op": "create",
                "spec": { "command": "no-such-command-anchor", "cwd": cwd, "login_shell": true },
            }),
        );
        started.elapsed()
    });

    let pinging = tokio::task::spawn_blocking(move || {
        // Let the create reach the daemon and start the shell.
        std::thread::sleep(Duration::from_millis(500));
        let mut stream = UnixStream::connect(socket).unwrap();
        let started = Instant::now();
        let reply = call(&mut stream, json!({ "id": 1, "op": "ping" }));
        assert_eq!(reply["pong"], true, "{reply}");
        started.elapsed()
    });

    let ping_took = pinging.await.unwrap();
    let create_took = creating.await.unwrap();

    assert!(
        create_took >= SHELL_STARTUP,
        "the fixture is inert: create took {create_took:?}, so the slow shell never ran"
    );
    assert!(
        ping_took < Duration::from_millis(1500),
        "a ping waited {ping_took:?} behind a create that took {create_took:?}: \
         create is blocking the runtime worker"
    );
}
