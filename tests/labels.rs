//! M2: labels and the label-filtered query.
//!
//! The capability that moved the boundary — a client answers "which session
//! is for X?" with the labels it set, instead of keeping a side-table.

use std::collections::BTreeMap;

use anchor::model::labels::{self, InvalidLabel};
use anchor::model::{Session, SpawnSpec};
use anchor::paths::Paths;
use anchor::store::Store;

fn paths(dir: &tempfile::TempDir) -> Paths {
    let paths = Paths::new(dir.path().join("data"), dir.path().join("anchor.sock"));
    paths.ensure_dirs().unwrap();
    paths
}

fn session_with(labels: BTreeMap<String, String>) -> Session {
    let spec = SpawnSpec {
        command: "/bin/sh".to_string(),
        args: vec![],
        cwd: std::path::PathBuf::from("/tmp"),
        env: BTreeMap::new(),
        cols: 80,
        rows: 24,
        login_shell: false,
    };
    Session::new(anchor::model::new_session_id(), spec, labels)
}

fn labels_of(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

/// The roadmap's M2 exit criterion: 500 sessions, filter by a label, correct
/// results.
#[test]
fn five_hundred_sessions_filter_correctly() {
    let dir = tempfile::tempdir().unwrap();
    let paths = paths(&dir);
    let mut store = Store::open(&paths).unwrap();

    let mut expected_mr218 = Vec::new();
    for i in 0..500 {
        let item = format!("gitlab-main:mr:{}", 200 + i % 25);
        let agent = if i % 2 == 0 { "impl-a" } else { "impl-b" };
        let session = session_with(labels_of(&[
            ("wm.item", item.as_str()),
            ("wm.agent", agent),
        ]));
        if item == "gitlab-main:mr:218" {
            expected_mr218.push(session.id.clone());
        }
        store.insert(&session).unwrap();
    }

    let found = store
        .list_filtered(&labels_of(&[("wm.item", "gitlab-main:mr:218")]))
        .unwrap();
    assert_eq!(found.len(), expected_mr218.len());
    assert_eq!(found.len(), 20, "25 items across 500 sessions");
    for session in &found {
        assert_eq!(
            session.labels.get("wm.item").map(String::as_str),
            Some("gitlab-main:mr:218")
        );
    }

    // Newest first, same as an unfiltered list.
    let mut sorted = found.iter().map(|s| s.id.clone()).collect::<Vec<_>>();
    sorted.sort();
    sorted.reverse();
    assert_eq!(
        found.iter().map(|s| s.id.clone()).collect::<Vec<_>>(),
        sorted
    );
}

#[test]
fn filters_narrow_across_keys() {
    let dir = tempfile::tempdir().unwrap();
    let paths = paths(&dir);
    let mut store = Store::open(&paths).unwrap();

    for (item, agent) in [("mr:1", "impl-a"), ("mr:1", "impl-b"), ("mr:2", "impl-a")] {
        store
            .insert(&session_with(labels_of(&[
                ("wm.item", item),
                ("wm.agent", agent),
            ])))
            .unwrap();
    }

    // Every pair must match: AND, not OR.
    let both = store
        .list_filtered(&labels_of(&[("wm.item", "mr:1"), ("wm.agent", "impl-a")]))
        .unwrap();
    assert_eq!(both.len(), 1);

    let one_key = store
        .list_filtered(&labels_of(&[("wm.item", "mr:1")]))
        .unwrap();
    assert_eq!(one_key.len(), 2);

    // An empty filter is every session, not none.
    assert_eq!(store.list_filtered(&BTreeMap::new()).unwrap().len(), 3);

    // A key that exists with a different value matches nothing.
    assert!(store
        .list_filtered(&labels_of(&[("wm.item", "mr:3")]))
        .unwrap()
        .is_empty());
    // A key nobody has matches nothing either.
    assert!(store
        .list_filtered(&labels_of(&[("nope", "x")]))
        .unwrap()
        .is_empty());
}

#[test]
fn labels_are_mutable_after_create() {
    let dir = tempfile::tempdir().unwrap();
    let paths = paths(&dir);
    let mut store = Store::open(&paths).unwrap();

    let session = session_with(labels_of(&[("wm.item", "mr:1"), ("wm.stale", "yes")]));
    store.insert(&session).unwrap();

    let updated = labels_of(&[("wm.item", "mr:1"), ("wm.agent", "impl-b")]);
    store.replace_labels(&session.id, &updated).unwrap();

    let read_back = store.get(&session.id).unwrap().unwrap();
    assert_eq!(read_back.labels, updated);
    // The removed label no longer matches, and the new one does.
    assert!(store
        .list_filtered(&labels_of(&[("wm.stale", "yes")]))
        .unwrap()
        .is_empty());
    assert_eq!(
        store
            .list_filtered(&labels_of(&[("wm.agent", "impl-b")]))
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn label_limits_are_enforced() {
    // Keys: [a-z0-9_.-]{1,64}
    assert!(labels::validate_key("wm.item").is_ok());
    assert!(labels::validate_key("a-b_c.0").is_ok());
    assert!(labels::validate_key(&"k".repeat(64)).is_ok());

    assert!(matches!(
        labels::validate_key(""),
        Err(InvalidLabel::KeyLength { .. })
    ));
    assert!(matches!(
        labels::validate_key(&"k".repeat(65)),
        Err(InvalidLabel::KeyLength { .. })
    ));
    for bad in ["WM.item", "wm item", "wm/item", "wm:item", "wm+item"] {
        assert!(
            matches!(
                labels::validate_key(bad),
                Err(InvalidLabel::KeyCharset { .. })
            ),
            "{bad:?} must be rejected"
        );
    }

    // Values: at most 256 bytes — bytes, not characters.
    let ok = labels_of(&[("k", &"v".repeat(256))]);
    assert!(labels::validate(&ok).is_ok());
    let too_long = labels_of(&[("k", &"v".repeat(257))]);
    assert!(matches!(
        labels::validate(&too_long),
        Err(InvalidLabel::ValueLength { len: 257, .. })
    ));
    // 128 two-byte characters is 256 bytes: at the limit, not over it.
    let multibyte = labels_of(&[("k", &"é".repeat(128))]);
    assert!(labels::validate(&multibyte).is_ok());
    let over = labels_of(&[("k", &"é".repeat(129))]);
    assert!(matches!(
        labels::validate(&over),
        Err(InvalidLabel::ValueLength { .. })
    ));

    // At most 32 labels per session.
    let max: BTreeMap<String, String> = (0..32)
        .map(|i| (format!("k{i}"), "v".to_string()))
        .collect();
    assert!(labels::validate(&max).is_ok());
    let over: BTreeMap<String, String> = (0..33)
        .map(|i| (format!("k{i}"), "v".to_string()))
        .collect();
    assert!(matches!(
        labels::validate(&over),
        Err(InvalidLabel::TooMany { count: 33 })
    ));
}

#[test]
fn anchor_does_not_interpret_a_label() {
    // No reserved prefix: namespacing is the caller's problem.
    let labels = labels_of(&[
        ("wm.item", "gitlab-main:mr:218"),
        ("teleport.session", "x"),
        ("anchor.internal", "not-reserved"),
    ]);
    assert!(labels::validate(&labels).is_ok());
}

/// The manager-level half: validation happens before anything is created,
/// and labels can be edited on a live session.
mod live {
    use super::{labels_of, paths};
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    use anchor::model::SpawnSpec;
    use anchor::session::{CreateError, SessionManager};
    use anchor::store::Store;

    /// A child that stays alive well past the test. The only platform
    /// difference here is which program does the waiting.
    fn spec() -> SpawnSpec {
        #[cfg(unix)]
        let (command, args) = ("/bin/sh", ["-c", "sleep 30"]);
        #[cfg(windows)]
        let (command, args) = ("cmd.exe", ["/c", "ping -n 31 127.0.0.1 >nul"]);
        SpawnSpec {
            command: command.to_string(),
            args: args.iter().map(|a| (*a).to_string()).collect(),
            cwd: std::env::temp_dir(),
            env: BTreeMap::new(),
            cols: 80,
            rows: 24,
            login_shell: false,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn create_rejects_an_invalid_label_before_anything_exists() {
        let dir = tempfile::tempdir().unwrap();
        let paths = paths(&dir);
        let manager = SessionManager::new(paths.sessions_dir());

        let err = manager
            .create(&spec(), labels_of(&[("WM.Item", "x")]))
            .expect_err("an uppercase key must be rejected");
        assert!(matches!(err, CreateError::InvalidLabel(_)), "got {err:?}");

        // Nothing was created: no session, and no session directory.
        assert!(manager.list().is_empty());
        let dirs = std::fs::read_dir(paths.sessions_dir())
            .map(|d| d.count())
            .unwrap_or(0);
        assert_eq!(
            dirs, 0,
            "a rejected label must leave no log directory behind"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn labels_can_be_set_and_removed_on_a_live_session() {
        let dir = tempfile::tempdir().unwrap();
        let paths = paths(&dir);
        let store = Arc::new(Mutex::new(Store::open(&paths).unwrap()));
        let manager = SessionManager::new(paths.sessions_dir()).with_store(Arc::clone(&store));

        let session = manager
            .create(
                &spec(),
                labels_of(&[("wm.item", "mr:1"), ("wm.stale", "yes")]),
            )
            .expect("create");
        let id = session.id.clone();

        let updated = manager
            .label(
                &id,
                labels_of(&[("wm.agent", "impl-b")]),
                &["wm.stale".to_string()],
            )
            .expect("label");
        assert_eq!(
            updated,
            labels_of(&[("wm.item", "mr:1"), ("wm.agent", "impl-b")])
        );

        // Visible to the live query...
        assert_eq!(
            manager
                .list_filtered(&labels_of(&[("wm.agent", "impl-b")]))
                .len(),
            1
        );
        assert!(manager
            .list_filtered(&labels_of(&[("wm.stale", "yes")]))
            .is_empty());

        // ...and persisted, so a restart can still answer the same question.
        let persisted = store.lock().unwrap().get(&id).unwrap().unwrap();
        assert_eq!(persisted.labels, updated);

        session.terminate().expect("terminate");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_edit_is_judged_on_the_result_not_the_intermediate() {
        let dir = tempfile::tempdir().unwrap();
        let paths = paths(&dir);
        let manager = SessionManager::new(paths.sessions_dir());

        let full: BTreeMap<String, String> = (0..32)
            .map(|i| (format!("k{i}"), "v".to_string()))
            .collect();
        let session = manager.create(&spec(), full).expect("create at the limit");

        // Swapping one label for another stays at 32 and must be allowed,
        // even though the set passes through 33 if you apply adds first.
        let result = manager.label(
            &session.id.clone(),
            labels_of(&[("k99", "v")]),
            &["k0".to_string()],
        );
        let labels = result.expect("a swap at the limit is valid");
        assert_eq!(labels.len(), 32);
        assert!(labels.contains_key("k99") && !labels.contains_key("k0"));

        // Actually exceeding it still fails.
        let err = manager
            .label(&session.id.clone(), labels_of(&[("k100", "v")]), &[])
            .expect_err("33 labels must be rejected");
        assert!(matches!(err, CreateError::InvalidLabel(_)), "got {err:?}");

        session.terminate().expect("terminate");
    }

    /// Two clients sharing one anchord are told to stay out of each other's
    /// way by namespacing their keys (docs/02-model.md) -- teleport writes
    /// `teleport.*`, another client writes its own prefix. That only works if
    /// a per-key op is per-key in effect.
    ///
    /// It was not: `label` read the map under one lock and wrote it under
    /// another, so concurrent writers each overwrote the other's edit and the
    /// loser lost *every* key it held, not just the one in flight (#12).
    ///
    /// Asserting on the store as well as on memory is the point, not belt and
    /// braces: holding a lock across only the in-memory edit serializes the
    /// writers and still lets them reach the store in the opposite order,
    /// leaving the persisted map one key short. That variant passes a
    /// memory-only assertion and fails this one.
    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_writers_on_distinct_keys_keep_every_key() {
        const WRITERS: usize = 24;
        // Rounds, because the store half of this is probabilistic and one
        // round is not a test. Measured against the memory-only variant
        // described above: 1 round caught it 2 times in 8, 12 rounds 4 in 6,
        // 60 rounds 8 in 8. The original two-lock form fails on the first
        // round every time -- only the store ordering needs the repetition.
        // A fresh session per round keeps the rounds independent.
        //
        // This is a volume budget, not a timeout (docs/08-testing.md): it
        // buys detection probability, so shrinking it trades that away
        // without failing anything.
        const ROUNDS: usize = 60;

        let dir = tempfile::tempdir().unwrap();
        let paths = paths(&dir);
        let store = Arc::new(Mutex::new(Store::open(&paths).unwrap()));
        let manager =
            Arc::new(SessionManager::new(paths.sessions_dir()).with_store(Arc::clone(&store)));

        for round in 0..ROUNDS {
            let session = manager
                .create(&spec(), labels_of(&[("base", "kept")]))
                .expect("create");
            let id = session.id.clone();

            // Released together, so the reads overlap rather than queue behind
            // each other -- the interleaving is the premise of the test.
            let start = Arc::new(std::sync::Barrier::new(WRITERS));
            let handles: Vec<_> = (0..WRITERS)
                .map(|i| {
                    let manager = Arc::clone(&manager);
                    let start = Arc::clone(&start);
                    let id = id.clone();
                    std::thread::spawn(move || {
                        start.wait();
                        manager
                            .label(&id, labels_of(&[(&format!("k{i}"), "v")]), &[])
                            .expect("label");
                    })
                })
                .collect();
            for handle in handles {
                handle.join().expect("writer thread");
            }

            let live = manager
                .get(&id)
                .expect("session is still live")
                .labels
                .lock()
                .clone();
            let persisted = lock_store(&store).get(&id).unwrap().unwrap().labels;

            for i in 0..WRITERS {
                let key = format!("k{i}");
                assert_eq!(
                    live.get(&key).map(String::as_str),
                    Some("v"),
                    "round {round}: {key} was lost in memory: a writer's whole \
                 map replaced it"
                );
                assert_eq!(
                    persisted.get(&key).map(String::as_str),
                    Some("v"),
                    "round {round}: {key} was lost in the store: writers reached \
                 it out of order"
                );
            }
            assert_eq!(live.get("base").map(String::as_str), Some("kept"));
            assert_eq!(persisted.get("base").map(String::as_str), Some("kept"));

            session.terminate().expect("terminate");
        }
    }

    fn lock_store(store: &Mutex<Store>) -> std::sync::MutexGuard<'_, Store> {
        match store.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

/// A create whose child never starts must not leave a row claiming to be
/// running — docs/02-model.md's create sequence: "spawn ← failure: row →
/// EXITED, lost_reason=spawn_failed".
///
/// Driven at the store, deliberately. A bad command does not reach this
/// path on Unix: `posix_spawn` forks first and the exec failure happens in
/// the *child*, which writes its error to the PTY and exits — so the parent
/// sees a successful spawn and an ordinary non-zero exit, which the reaper
/// already records. What actually reaches the failure path is resource
/// exhaustion — `openpty`, or a thread that cannot be spawned — and
/// contriving that in a test would be far less honest than naming it.
#[test]
fn a_failed_spawn_leaves_no_phantom_running_row() {
    let dir = tempfile::tempdir().unwrap();
    let paths = paths(&dir);
    let mut store = Store::open(&paths).unwrap();

    // The row exists before the spawn, by design: the child must never be
    // able to emit a byte that nothing is recorded for.
    let session = session_with(BTreeMap::new());
    store.insert(&session).unwrap();
    assert_eq!(
        store.get(&session.id).unwrap().unwrap().state,
        anchor::model::State::Running
    );

    store
        .mark_spawn_failed(&session.id, 1_700_000_000_000)
        .unwrap();

    let row = store.get(&session.id).unwrap().unwrap();
    assert_eq!(
        row.state,
        anchor::model::State::Exited,
        "a session whose child never started must not read as running"
    );
    assert_eq!(
        row.lost_reason.as_deref(),
        Some(anchor::model::lost_reason::SPAWN_FAILED)
    );
    assert_eq!(row.exited_at, Some(1_700_000_000_000));

    // Not `lost`: lost means it ran and the daemon lost track of it. This
    // one never ran, and a restart must not promote it to a session that
    // once existed.
    assert_ne!(row.state, anchor::model::State::Lost);
}
