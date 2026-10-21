//! **An exited session holds nothing.** The resource half of
//! docs/02-model.md's lifecycle, and the gate docs/08-testing.md asks for.
//!
//! This is the fixture anchor#37 did not have. A session that had exited kept
//! its `PtySession`, so `write_tx` lived until an explicit `purge`, so the
//! pty writer thread never saw its channel close and never retired — one
//! orphaned thread holding one `dup()` of a dead child's master, per session,
//! for the life of the daemon. The output log's write handle leaked on the
//! same mechanism, so the real cost was two descriptors each. anchord reached
//! launchd's 256-fd default after ~105 sessions and every spawn after that
//! failed `EMFILE`, with nothing in the suite able to notice: before this
//! file, no test asserted anything about a descriptor anywhere.
//!
//! **Nothing here purges, and that is the whole point.** Purging drops the
//! record, which dropped the handles, which is exactly what masked the leak
//! for three days. The claim is that an exited session holds nothing *while
//! still listed*, so the sessions stay listed and the manager stays alive for
//! the duration of the assertion.
//!
//! Baseline-relative, never absolute: the number of descriptors a process
//! holds at rest is an implementation detail and pinning it writes a test
//! that fails on unrelated work. The leak was a slope, so this measures the
//! slope.

#![cfg(unix)]

use std::collections::BTreeMap;
use std::time::Duration;

use anchor::model::SpawnSpec;
use anchor::session::SessionManager;

/// Enough that a per-session leak clears the noise floor by a wide margin,
/// few enough to stay quick. At the old rate this leaked 16.
const SESSIONS: usize = 8;

/// Descriptors open to this process. `/dev/fd` is the one spelling that works
/// on both Linux (a symlink to `/proc/self/fd`) and macOS, and reading it the
/// same way at both ends keeps the directory's own handle out of the delta.
fn open_fds() -> usize {
    std::fs::read_dir("/dev/fd")
        .expect("/dev/fd is readable")
        .count()
}

#[tokio::test]
async fn an_exited_session_releases_its_pty_and_log_without_being_purged() {
    let dir = tempfile::tempdir().expect("a tempdir");
    let manager = SessionManager::new(dir.path().to_path_buf());
    let cwd = std::env::temp_dir();

    // After one full create/exit cycle, so the baseline includes anything
    // allocated once and kept for the process's life -- the syncer thread's
    // registrations, lazily-opened store handles. What a first iteration
    // would otherwise charge to the slope is charged to the baseline here.
    let warmup = manager
        .create(
            &SpawnSpec {
                command: "/bin/sh".to_string(),
                args: vec!["-c".into(), "exit 0".into()],
                cwd: cwd.clone(),
                env: BTreeMap::new(),
                cols: 80,
                rows: 24,
                login_shell: false,
            },
            BTreeMap::new(),
        )
        .expect("create the warm-up session");
    warmup.wait_exited().await;
    warmup.wait_eof().await;
    settle().await;

    let baseline = open_fds();

    let mut sessions = Vec::new();
    for _ in 0..SESSIONS {
        sessions.push(
            manager
                .create(
                    &SpawnSpec {
                        command: "/bin/sh".to_string(),
                        args: vec!["-c".into(), "exit 0".into()],
                        cwd: cwd.clone(),
                        env: BTreeMap::new(),
                        cols: 80,
                        rows: 24,
                        login_shell: false,
                    },
                    BTreeMap::new(),
                )
                .expect("create a session"),
        );
    }

    // Both signals, because the two handles are released on different edges:
    // the PTY at the exit the reaper observes, the log at EOF on the master.
    // Waiting on only one would leave the other's release racing the count.
    for session in &sessions {
        session.wait_exited().await;
        session.wait_eof().await;
    }
    settle().await;

    // The premise, stated loudly: a test that counted descriptors while the
    // sessions were still running, or after they had been purged, would pass
    // for the wrong reason.
    assert_eq!(
        manager.list().len(),
        SESSIONS + 1,
        "the sessions must still be listed -- purging is what used to mask this"
    );

    let after = open_fds();
    assert!(
        after <= baseline,
        "{SESSIONS} exited sessions are still holding {} file descriptors \
         between them ({baseline} before, {after} after). An exited session \
         holds nothing: docs/02-model.md, docs/03-pty.md step 7.",
        after.saturating_sub(baseline),
    );
}

/// Release runs on the exit and EOF listener threads, so it trails the
/// `watch` the waiters above observe by a scheduling hop. Polling for a
/// *drop* needs a settle rather than a condition to wait on -- there is no
/// edge that says "and now the descriptor is closed".
async fn settle() {
    tokio::time::sleep(Duration::from_millis(250)).await;
}
