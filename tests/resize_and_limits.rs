//! The three rules docs/02-model.md and docs/03-pty.md state about size:
//! validated at create, coalesced on the way to the PTY, and persisted once
//! applied.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anchor::model::SpawnSpec;
use anchor::paths::Paths;
use anchor::session::{CreateError, SessionManager};
use anchor::store::Store;

fn paths(dir: &tempfile::TempDir) -> Paths {
    let paths = Paths::new(dir.path().join("data"), dir.path().join("anchor.sock"));
    paths.ensure_dirs().unwrap();
    paths
}

/// A child that stays alive well past the test; only the program differs by
/// platform.
fn spec(cols: u16, rows: u16) -> SpawnSpec {
    #[cfg(unix)]
    let (command, args) = ("/bin/sh", ["-c", "sleep 30"]);
    #[cfg(windows)]
    let (command, args) = ("cmd.exe", ["/c", "ping -n 31 127.0.0.1 >nul"]);
    SpawnSpec {
        command: command.to_string(),
        args: args.iter().map(|a| (*a).to_string()).collect(),
        cwd: std::env::temp_dir(),
        env: BTreeMap::new(),
        cols,
        rows,
        login_shell: false,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn create_refuses_a_size_outside_the_range() {
    let dir = tempfile::tempdir().unwrap();
    let paths = paths(&dir);
    let manager = SessionManager::new(paths.sessions_dir());

    // The PTY layer clamps as a backstop, but a caller that asked for zero
    // columns made a mistake and is told so rather than handed a 1x1 PTY.
    for (cols, rows) in [(0, 24), (80, 0), (1001, 24), (80, 1001)] {
        let err = manager
            .create(&spec(cols, rows), BTreeMap::new())
            .expect_err(&format!("{cols}x{rows} must be refused"));
        assert!(
            matches!(err, CreateError::InvalidSize { .. }),
            "{cols}x{rows} gave {err:?}"
        );
    }
    assert!(manager.list().is_empty(), "nothing may be created");

    // The edges of the range are valid.
    for (cols, rows) in [(1, 1), (1000, 1000)] {
        let session = manager
            .create(&spec(cols, rows), BTreeMap::new())
            .expect("an in-range size is accepted");
        assert_eq!(session.size(), (cols, rows));
        session.terminate().unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn resizes_are_coalesced_and_the_last_one_wins() {
    let dir = tempfile::tempdir().unwrap();
    let paths = paths(&dir);
    let manager = SessionManager::new(paths.sessions_dir());
    let session = manager.create(&spec(80, 24), BTreeMap::new()).unwrap();

    // The first resize is outside the window and applies immediately.
    session.resize(100, 30).unwrap();
    assert_eq!(session.size(), (100, 30));

    // A burst inside the window: a drag emitting a resize per frame. Only
    // the first of these is deferred, and the last one asked for must win.
    //
    // No bound on how long the burst takes: a stall in the scheduler would
    // fail it with nothing broken, and "never blocks" is already shown by the
    // last size arriving below.
    for width in 101..=140 {
        session.resize(width, 30).unwrap();
    }
    // Still the size applied before the burst: the rest are pending. This
    // one is inherently coupled to the 100 ms window -- if the machine stalls
    // for longer than that between the first resize and here, the flush has
    // legitimately run. It is not widened because there is no condition to
    // wait on for "has not happened yet".
    assert_eq!(
        session.size(),
        (100, 30),
        "a resize inside the window must not reach the PTY yet"
    );

    // Once the window closes the latest pending size is applied — not
    // dropped, which would leave the terminal at the wrong width.
    //
    // Polled rather than slept at: a flat wait asserts that the coalescing
    // window closed faster than the sleep, which on a loaded machine is a
    // claim about the machine. The deadline is generous and the assertion
    // after it is the one that reports the real failure.
    let deadline = Instant::now() + Duration::from_secs(5);
    while session.size() != (140, 30) && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(session.size(), (140, 30), "the last resize must win");

    session.terminate().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn an_applied_size_is_persisted() {
    let dir = tempfile::tempdir().unwrap();
    let paths = paths(&dir);
    let store = Arc::new(Mutex::new(Store::open(&paths).unwrap()));
    let manager = SessionManager::new(paths.sessions_dir()).with_store(Arc::clone(&store));

    let session = manager.create(&spec(80, 24), BTreeMap::new()).unwrap();
    let id = session.id.clone();
    assert_eq!(
        store.lock().unwrap().get(&id).unwrap().unwrap().cols,
        80,
        "the row starts at the spec size"
    );

    session.resize(132, 50).unwrap();
    assert_eq!(session.size(), (132, 50));

    // Persistence is queued off the caller's thread, so give it a moment.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let row = store.lock().unwrap().get(&id).unwrap().unwrap();
        if (row.cols, row.rows) == (132, 50) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "an applied size must reach the row, or a restart reports the size \
             the session was created at: got {}x{}",
            row.cols,
            row.rows
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    // The spec's original size is kept separately — it records what was
    // asked for at create, which a resize does not change.
    let row = store.lock().unwrap().get(&id).unwrap().unwrap();
    assert_eq!((row.spec.cols, row.spec.rows), (80, 24));

    session.terminate().unwrap();
}
