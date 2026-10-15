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
    assert_eq!(anchor::db::schema_version(store.conn()).unwrap(), 1);
    drop(store);

    let reopened = Store::open(&paths).unwrap();
    assert_eq!(anchor::db::schema_version(reopened.conn()).unwrap(), 1);
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
fn output_bytes_are_reconciled_against_the_log_file() {
    let dir = tempfile::tempdir().unwrap();
    let paths = paths(&dir);
    let mut store = Store::open(&paths).unwrap();

    let mut session = sample_session();
    session.output_bytes = 999; // a stale cached length
    store.insert(&session).unwrap();

    std::fs::create_dir_all(paths.session_dir(&session.id)).unwrap();
    std::fs::write(paths.output_log(&session.id), b"hello anchor").unwrap();

    assert_eq!(store.reconcile_output_bytes(&paths).unwrap(), 1);
    assert_eq!(store.get(&session.id).unwrap().unwrap().output_bytes, 12);

    // The file is authoritative and nothing changed: a second pass is a no-op.
    assert_eq!(store.reconcile_output_bytes(&paths).unwrap(), 0);
}

#[test]
fn a_missing_log_file_means_zero_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let paths = paths(&dir);
    let mut store = Store::open(&paths).unwrap();

    let mut session = sample_session();
    session.output_bytes = 42;
    store.insert(&session).unwrap();

    assert_eq!(store.reconcile_output_bytes(&paths).unwrap(), 1);
    assert_eq!(store.get(&session.id).unwrap().unwrap().output_bytes, 0);
}
