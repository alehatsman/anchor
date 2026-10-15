//! M0 exit criterion: a session row round-trips.

use std::collections::BTreeMap;
use std::path::PathBuf;

use anchor::model::{lost_reason, Session, SpawnSpec, State};
use anchor::paths::Paths;
use anchor::store::Store;

fn paths(dir: &tempfile::TempDir) -> Paths {
    let data = dir.path().join("data");
    let socket = dir.path().join("anchor.sock");
    let paths = Paths::new(data, socket);
    paths.ensure_dirs().unwrap();
    paths
}

fn sample_session() -> Session {
    let spec = SpawnSpec {
        command: "/bin/zsh".into(),
        args: vec!["-c".into(), "echo hi".into()],
        cwd: PathBuf::from("/tmp"),
        env: BTreeMap::from([("TERM".to_string(), "xterm-256color".to_string())]),
        cols: 120,
        rows: 40,
        login_shell: true,
    };
    let labels = BTreeMap::from([
        ("wm.item".to_string(), "gitlab-main:mr:218".to_string()),
        ("wm.agent".to_string(), "impl-a".to_string()),
    ]);
    Session::new(anchor::model::new_session_id(), spec, labels)
}

#[test]
fn session_row_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    let paths = paths(&dir);
    let mut store = Store::open(&paths).unwrap();

    let session = sample_session();
    store.insert(&session).unwrap();

    let loaded = store.get(&session.id).unwrap().expect("session is present");
    assert_eq!(loaded, session);

    let listed = store.list().unwrap();
    assert_eq!(listed, vec![session]);
}

#[test]
fn missing_session_is_none() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&paths(&dir)).unwrap();
    assert!(store.get("01J0000000000000000000000").unwrap().is_none());
}

#[test]
fn schema_migration_is_idempotent_across_opens() {
    let dir = tempfile::tempdir().unwrap();
    let paths = paths(&dir);

    let mut store = Store::open(&paths).unwrap();
    let session = sample_session();
    store.insert(&session).unwrap();
    assert_eq!(anchor::db::schema_version(store.conn()).unwrap(), 2);
    drop(store);

    let reopened = Store::open(&paths).unwrap();
    assert_eq!(anchor::db::schema_version(reopened.conn()).unwrap(), 2);
    assert_eq!(reopened.get(&session.id).unwrap(), Some(session));
}

#[test]
fn restart_marks_live_sessions_lost() {
    let dir = tempfile::tempdir().unwrap();
    let paths = paths(&dir);
    let mut store = Store::open(&paths).unwrap();

    let running = sample_session();
    store.insert(&running).unwrap();

    let mut exited = sample_session();
    exited.state = State::Exited;
    exited.exit_code = Some(0);
    exited.exited_at = Some(1_700_000_000_000);
    store.insert(&exited).unwrap();

    assert_eq!(store.mark_live_sessions_lost().unwrap(), 1);

    let after = store.get(&running.id).unwrap().unwrap();
    assert_eq!(after.state, State::Lost);
    assert_eq!(
        after.lost_reason.as_deref(),
        Some(lost_reason::DAEMON_RESTART)
    );
    assert!(after.exited_at.is_some());

    // An already-exited session is untouched, exit code and all.
    let untouched = store.get(&exited.id).unwrap().unwrap();
    assert_eq!(untouched, exited);
}

#[test]
fn a_shorter_log_file_caps_the_stream_instead_of_rewinding_it() {
    let dir = tempfile::tempdir().unwrap();
    let paths = paths(&dir);
    let mut store = Store::open(&paths).unwrap();

    // The stream counted 999 bytes; only 12 are on disk. That is a capped
    // log — clients may already hold offsets past 12, so the offset must not
    // rewind to the file length.
    let mut session = sample_session();
    session.output_bytes = 999;
    store.insert(&session).unwrap();

    std::fs::create_dir_all(paths.session_dir(&session.id)).unwrap();
    std::fs::write(paths.output_log(&session.id), b"hello anchor").unwrap();

    assert_eq!(store.reconcile_output_bytes(&paths).unwrap(), 1);
    let after = store.get(&session.id).unwrap().unwrap();
    assert_eq!(after.output_bytes, 999);
    assert_eq!(after.log_capped_at, Some(12));

    // Nothing changed: a second pass is a no-op.
    assert_eq!(store.reconcile_output_bytes(&paths).unwrap(), 0);
}

#[test]
fn a_longer_log_file_corrects_a_stale_cached_offset() {
    let dir = tempfile::tempdir().unwrap();
    let paths = paths(&dir);
    let mut store = Store::open(&paths).unwrap();

    // A crash between the write and the row update: the file holds more than
    // the column remembers, and the file is authoritative for content.
    let mut session = sample_session();
    session.output_bytes = 4;
    store.insert(&session).unwrap();

    std::fs::create_dir_all(paths.session_dir(&session.id)).unwrap();
    std::fs::write(paths.output_log(&session.id), b"hello anchor").unwrap();

    assert_eq!(store.reconcile_output_bytes(&paths).unwrap(), 1);
    let after = store.get(&session.id).unwrap().unwrap();
    assert_eq!(after.output_bytes, 12);
    assert_eq!(after.log_capped_at, None);
}

#[test]
fn a_missing_log_file_caps_at_zero_without_losing_the_offset() {
    let dir = tempfile::tempdir().unwrap();
    let paths = paths(&dir);
    let mut store = Store::open(&paths).unwrap();

    let mut session = sample_session();
    session.output_bytes = 42;
    store.insert(&session).unwrap();

    assert_eq!(store.reconcile_output_bytes(&paths).unwrap(), 1);
    let after = store.get(&session.id).unwrap().unwrap();
    assert_eq!(after.output_bytes, 42);
    assert_eq!(after.log_capped_at, Some(0));
}
