//! M3's gate: replay and live output meet exactly once, with no gap and no
//! duplicate -- ROADMAP.md and the
//! attach-boundary half of ROADMAP.md
//!
//! `cfg(unix)`-gated because the fixtures drive a real `/bin/sh`; the
//! platform-independent offset accounting is in `output_log.rs`.

#![cfg(unix)]

mod support;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anchor::log::LogLimits;
use anchor::model::SpawnSpec;
use anchor::session::{AttachError, Session, SessionManager};

const RECV_TIMEOUT: Duration = Duration::from_secs(10);

fn sessions_root(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "anchor-replay-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock before 1970")
            .as_nanos()
    ))
}

/// A session that emits exactly `bytes` bytes of `yes` output as fast as it
/// can, then blocks forever reading its own stdin. `stty raw -echo` keeps
/// the tty line discipline from injecting anything of its own, so the log is
/// the child's output and nothing else.
///
/// Every fixture here reaches this output through `attach`, never a bare
/// `subscribe()`: the child is already producing by the time `create`
/// returns, and only `attach` accounts for the bytes that landed before the
/// caller registered.
///
/// The trailing `cat` reads nothing real -- nothing ever writes to this
/// pty's input side -- it exists only to hold the slave fd open past the
/// last byte of output, so these fixtures' exact-byte-count assertions never
/// race the child's own exit: the session ends when the test calls
/// `terminate()`, on the test's schedule, not the child's. Every caller
/// terminates the session when it's done.
fn spawn_emitting(manager: &SessionManager, bytes: usize) -> std::sync::Arc<Session> {
    let cwd = std::env::temp_dir();
    let args = vec![
        "-c".to_string(),
        format!("stty raw -echo; yes | head -c {bytes}; cat"),
    ];
    manager
        .create(
            &SpawnSpec {
                command: "/bin/sh".to_string(),
                args: args.clone(),
                cwd: cwd.clone(),
                env: BTreeMap::new(),
                cols: 80,
                rows: 24,
                login_shell: false,
            },
            BTreeMap::new(),
        )
        .expect("create session")
}

/// A session that emits 64 KiB bursts, forever, at roughly 1 MB/s -- the
/// sustained rate the M2 gate is stated in. Used where a fixture needs live
/// output to still be flowing *after* it has inspected the log; a bounded
/// emitter finishes long before the assertions run.
fn spawn_emitting_forever(manager: &SessionManager) -> std::sync::Arc<Session> {
    let cwd = std::env::temp_dir();
    let args = vec![
        "-c".to_string(),
        "stty raw -echo; while :; do yes | head -c 65536; sleep 0.05; done".to_string(),
    ];
    manager
        .create(
            &SpawnSpec {
                command: "/bin/sh".to_string(),
                args: args.clone(),
                cwd: cwd.clone(),
                env: BTreeMap::new(),
                cols: 80,
                rows: 24,
                login_shell: false,
            },
            BTreeMap::new(),
        )
        .expect("create session")
}

/// **The M3 gate.** Attach at 0, take live chunks, disconnect exactly
/// between two of them, let output keep flowing with zero subscribers,
/// reconnect at the recorded offset, and check byte-for-byte that
/// replay + live equals the log -- no gap, no duplicate.
///
/// Was `target_os = "linux"`-gated: it failed on `macos-latest` with "first
/// subscriber disconnected early" during the initial live-drain loop, which
/// was
/// teleport #N5
/// -- the 256-chunk half of the queue bound holding only ~3.5 KiB on macOS,
/// where a pty read averages 14 bytes. Un-gated with the fix that made the
/// bound a byte budget.
///
/// It was also un-gated once *before* that fix, on 20/20 local passes, and
/// failed first try on the runner. Worth keeping in mind if this ever goes
/// red again: this file's suite takes 20.2 s on `macos-latest` and 7.6 s on
/// an M-series Mac, and a subscriber that never stalls never trips a queue
/// bound, so a green local run says very little here. Whatever the next
/// theory is, get it green on the runner before believing it.
#[tokio::test]
async fn disconnect_between_chunks_and_reconnect_has_no_gap_or_duplicate() {
    const TOTAL: usize = 2 * 1024 * 1024;
    let manager = SessionManager::new(sessions_root("gate"));
    let session = spawn_emitting(&manager, TOTAL);

    // First connection: replay whatever already exists, then go live.
    let first = session.attach(0).expect("attach at 0");
    assert_eq!(
        first.replay_from, 0,
        "attaching at 0 replays from the start"
    );
    let (mut acc, mut first, _) = support::catch_up(first, Duration::ZERO).await;
    let mut end = first.replay_to();
    assert_eq!(end, acc.len() as u64);

    while acc.len() < 64 * 1024 {
        let chunk = tokio::time::timeout(RECV_TIMEOUT, first.subscription.recv())
            .await
            .expect("first subscriber timed out")
            .expect("first subscriber disconnected early");
        assert_eq!(
            chunk.offset, end,
            "live output must continue exactly where replay stopped"
        );
        acc.extend_from_slice(&chunk.bytes);
        end += chunk.bytes.len() as u64;
    }

    // Disconnect *between* chunks, and let the session run on with nobody
    // attached -- the case ROADMAP.md says must survive.
    drop(first.subscription);

    // Wait for the condition the assertion below actually depends on --
    // that output accumulated while nobody was attached -- rather than for
    // a fixed span and a hope. A sleep here encodes a guess about how fast
    // the writer is, and the guess is wrong first on a loaded machine,
    // which is exactly when a flake is hardest to read.
    let deadline = Instant::now() + Duration::from_secs(10);
    while session.next_offset() <= end {
        assert!(
            Instant::now() < deadline,
            "the session produced nothing while detached; it was supposed to run on"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let second = session
        .attach(end)
        .expect("re-attach at the recorded offset");
    assert_eq!(
        second.replay_from, end,
        "replay resumes at exactly the offset we held"
    );
    let (replayed, mut second, _) = support::catch_up(second, Duration::ZERO).await;
    assert!(
        second.replay_to() > end,
        "output kept accumulating while nobody was attached"
    );
    acc.extend_from_slice(&replayed);
    end = second.replay_to();
    assert_eq!(
        end,
        acc.len() as u64,
        "replay must not gap or overlap what we already had"
    );

    while acc.len() < TOTAL {
        let chunk = tokio::time::timeout(RECV_TIMEOUT, second.subscription.recv())
            .await
            .expect("second subscriber timed out")
            .expect("second subscriber disconnected early");
        assert_eq!(
            chunk.offset, end,
            "live output must continue exactly where replay stopped"
        );
        acc.extend_from_slice(&chunk.bytes);
        end += chunk.bytes.len() as u64;
    }

    let on_disk = std::fs::read(session.log_path()).expect("read output.vt");
    assert!(on_disk.len() >= acc.len());
    assert_eq!(
        acc,
        on_disk[..acc.len()],
        "replay + live must equal the log byte for byte"
    );
    assert_eq!(
        acc,
        vec![b"y\n".to_vec(); TOTAL / 2].concat(),
        "and the log must be the child's actual output"
    );

    session.terminate().expect("terminate");
}

/// The same boundary, hammered: re-attach over and over against a writer
/// that never stops, taking one chunk each time. Every attach point lands
/// somewhere different relative to the reader thread's append, and none of
/// them may gap or duplicate (ROADMAP.md "attach while the reader is
/// mid-write -- hammer with a concurrent writer").
#[tokio::test]
async fn repeated_attach_against_a_concurrent_writer_never_gaps() {
    const TOTAL: usize = 4 * 1024 * 1024;
    let manager = SessionManager::new(sessions_root("fuzz"));
    let session = spawn_emitting(&manager, TOTAL);

    let mut acc: Vec<u8> = Vec::new();
    let mut end: u64 = 0;

    for round in 0..200 {
        let replay = session
            .attach(end)
            .unwrap_or_else(|e| panic!("attach round {round}: {e}"));
        assert_eq!(
            replay.replay_from, end,
            "round {round}: replay must start where we left off"
        );
        let (replayed, mut attach, _) = support::catch_up(replay, Duration::ZERO).await;
        acc.extend_from_slice(&replayed);
        end = attach.replay_to();
        assert_eq!(
            end,
            acc.len() as u64,
            "round {round}: replay gapped or duplicated"
        );

        // Take one live chunk before dropping, so the subscriber path is
        // exercised at the boundary too -- not just the file read.
        if acc.len() < TOTAL {
            if let Ok(Some(chunk)) =
                tokio::time::timeout(RECV_TIMEOUT, attach.subscription.recv()).await
            {
                assert_eq!(
                    chunk.offset, end,
                    "round {round}: first live chunk must start at the boundary"
                );
                acc.extend_from_slice(&chunk.bytes);
                end += chunk.bytes.len() as u64;
            }
        }
    }

    let on_disk = std::fs::read(session.log_path()).expect("read output.vt");
    assert!(!acc.is_empty(), "the fuzz loop never observed any output");
    assert_eq!(
        acc,
        on_disk[..acc.len()],
        "the accumulated stream must equal the log byte for byte"
    );

    session.terminate().expect("terminate");
}

/// A client holding an offset the daemon never handed out -- a purged log, or
/// a stale client after a `lost` session. Error, not a panic, and it reports
/// where the daemon actually is (docs/04-protocol.md).
#[tokio::test]
async fn attaching_past_next_offset_is_offset_ahead() {
    const BURST: u64 = 1024;

    let manager = SessionManager::new(sessions_root("ahead"));
    let session = spawn_emitting(&manager, usize::try_from(BURST).expect("BURST is 1024"));

    // Let the whole burst land before attaching to anything. `spawn_emitting`
    // writes exactly `BURST` bytes and then blocks in `cat` forever, so once
    // the head reaches it the producer is done for good -- which is what the
    // at-head attach below actually requires, and what this fixture was
    // missing (#38). With the burst still in flight, bytes that land between
    // `attach` and the subscriber's registration come back in
    // `Attach::replay`, which `support::catch_up` folds into the same
    // accumulator it returns; `replayed.is_empty()` then fails on timing
    // alone, with nothing wrong in the product. Same precondition
    // `session_catchup.rs`'s D1 gate and `reconnect_storm.rs` already wait
    // on, for the same reason.
    support::await_output(&session, BURST, "the burst precondition").await;

    let next_offset = session.next_offset();
    match session.attach(next_offset + 4096) {
        Err(AttachError::OffsetAhead {
            requested,
            next_offset: reported,
        }) => {
            assert_eq!(requested, next_offset + 4096);
            assert!(
                reported >= next_offset,
                "the reported offset must be the daemon's own"
            );
        }
        Err(other) => panic!("expected OffsetAhead, got {other}"),
        Ok(_) => panic!("attaching past next_offset must not succeed"),
    }

    // Attaching *at* next_offset is legal and simply replays nothing.
    let replay = session
        .attach(session.next_offset())
        .expect("attach at the head");
    assert_eq!(
        replay.replay_from, replay.next_offset,
        "attaching at the head replays nothing"
    );
    let (replayed, attach, _) = support::catch_up(replay, Duration::ZERO).await;
    assert!(
        replayed.is_empty(),
        "and the catch-up loop goes live on its first round"
    );
    assert_eq!(attach.replay_from, attach.replay_to());

    session.terminate().expect("terminate");
}

/// Past a cap the bytes are gone. Replay stops at `log_capped_at`, a client
/// asking for what is beyond it gets nothing and is told where the stream
/// resumes, and live output keeps flowing throughout
/// (docs/02-model.md#size-cap).
#[tokio::test]
async fn replay_across_a_cap_stops_at_the_cap_and_live_output_continues() {
    const CAP: u64 = 64 * 1024;

    let limits = LogLimits {
        max_bytes: CAP,
        warn_bytes: CAP / 2,
        ..LogLimits::default()
    };
    let manager = SessionManager::with_limits(sessions_root("cap"), limits);
    // Must still be producing when the assertions below run -- the point of
    // the cap is that live streaming outlives persistence.
    let session = spawn_emitting_forever(&manager);

    // Wait for the cap to actually be hit, then for output to run well past it.
    support::await_output(&session, CAP * 4, "the past-the-cap precondition").await;

    assert_eq!(
        session.log_capped_at(),
        Some(CAP),
        "the cap lands on log_max_bytes exactly"
    );
    let on_disk = std::fs::read(session.log_path()).expect("read output.vt");
    assert_eq!(
        on_disk.len() as u64,
        CAP,
        "the file must stop growing at the cap"
    );

    // A replay that starts before the cap serves what exists and stops there.
    let from_start = session.attach(0).expect("attach at 0");
    assert_eq!(from_start.replay_from, 0);
    assert_eq!(
        from_start.log_capped_at,
        Some(CAP),
        "ready must carry the cap so a client can render the gap"
    );
    let (replayed, from_start, _) = support::catch_up(from_start, Duration::ZERO).await;
    assert_eq!(
        from_start.replay_to(),
        CAP,
        "replay must clamp to log_capped_at"
    );
    assert_eq!(replayed.len() as u64, CAP);

    // A client asking for bytes past the cap gets no replay, and is told
    // where the live stream resumes rather than being served the wrong bytes.
    let past_cap = session.attach(CAP + 1024).expect("attach past the cap");
    assert_eq!(
        past_cap.replay_from, past_cap.next_offset,
        "replay_from must be next_offset past a cap"
    );
    let (replayed, mut past_cap, _) = support::catch_up(past_cap, Duration::ZERO).await;
    assert!(
        replayed.is_empty(),
        "the bytes past a cap are gone, not served from the wrong position"
    );
    assert_eq!(
        past_cap.replay_from, past_cap.next_offset,
        "the boundary moved on while we caught up; replay_from must move with it, not point into the hole"
    );

    // Live streaming is unaffected by the cap.
    let mut end = past_cap.next_offset;
    for _ in 0..4 {
        let chunk = tokio::time::timeout(RECV_TIMEOUT, past_cap.subscription.recv())
            .await
            .expect("live output stopped after the cap")
            .expect("subscriber disconnected after the cap");
        assert_eq!(
            chunk.offset, end,
            "offsets keep advancing contiguously past the cap"
        );
        end += chunk.bytes.len() as u64;
    }
    assert!(end > CAP);
    assert_eq!(
        std::fs::metadata(session.log_path()).unwrap().len(),
        CAP,
        "the file must still not have grown"
    );

    session.terminate().expect("terminate");
}
