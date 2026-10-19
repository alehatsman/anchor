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

/// A recorded cap and a stream ahead of the file are both kept as they are:
/// the file stopped growing at the cap, so the stream length is the row's to
/// carry, and recovery must not rewind it to the file length.
#[test]
fn a_recorded_cap_and_a_stream_ahead_of_it_survive_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let paths = paths(&dir);
    let mut store = Store::open(&paths).unwrap();

    let mut session = sample_session();
    session.output_bytes = 200;
    session.log_capped_at = Some(12);
    store.insert(&session).unwrap();

    std::fs::create_dir_all(paths.session_dir(&session.id)).unwrap();
    std::fs::write(paths.output_log(&session.id), b"hello anchor").unwrap();

    assert_eq!(store.reconcile_output_bytes(&paths).unwrap(), 0);
    let after = store.get(&session.id).unwrap().unwrap();
    assert_eq!(after.output_bytes, 200, "the stream length was rewound");
    assert_eq!(after.log_capped_at, Some(12));
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

/// The recorded offset can only go forward.
///
/// `set_output_bytes` is written from the reader thread on a throttle, so
/// one can be overtaken by the final count `mark_exited` writes. The
/// offset is a promise to every client holding one: rewinding it, even
/// briefly, tells a client that bytes it has already read do not exist.
#[test]
fn a_stale_output_count_can_never_rewind_the_offset() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(&paths(&dir)).unwrap();
    let session = sample_session();
    store.insert(&session).unwrap();

    store.set_output_bytes(&session.id, 5_000, None).unwrap();
    // A throttled write from before that one, arriving late.
    store.set_output_bytes(&session.id, 1_200, None).unwrap();

    let row = store.get(&session.id).unwrap().unwrap();
    assert_eq!(
        row.output_bytes, 5_000,
        "a late write rewound the offset below what clients already hold"
    );

    // Forward still works, or the guard would have frozen the count.
    store.set_output_bytes(&session.id, 9_000, None).unwrap();
    assert_eq!(store.get(&session.id).unwrap().unwrap().output_bytes, 9_000);
}

/// A session that has ended does not acquire a new geometry.
///
/// Size is persisted off the thread that applied it, so a resize in
/// flight can land after the session exited. A dead session with a fresh
/// width is a row describing something that never happened.
#[test]
fn a_resize_in_flight_does_not_land_on_an_exited_session() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(&paths(&dir)).unwrap();
    let mut session = sample_session();
    store.insert(&session).unwrap();

    store.set_size(&session.id, 120, 40).unwrap();
    session.state = anchor::model::State::Exited;
    session.exit_code = Some(0);
    store.mark_exited(&session).unwrap();

    store.set_size(&session.id, 999, 999).unwrap();
    let row = store.get(&session.id).unwrap().unwrap();
    assert_eq!(
        (row.cols, row.rows),
        (120, 40),
        "an exited session was resized after the fact"
    );
}
