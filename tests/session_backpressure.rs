//! Session ownership and backpressure --
//! ROADMAP.md the M2 subset
//! of ROADMAP.md (the rest of that section
//! needs `log.rs`/M3 and is not in scope here).

#![cfg(unix)]

mod support;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anchor::model::SpawnSpec;
use anchor::session::SessionManager;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

/// The longest a healthy subscriber may go without a chunk while the
/// session is actively producing. Generous against a loaded machine on
/// purpose: a real backpressure failure parks the reader for the whole
/// write timeout, which is far longer than this, so the bound does not
/// need to be tight to catch it.
const STALL_BOUND: Duration = Duration::from_secs(5);

fn temp_dir() -> PathBuf {
    std::env::temp_dir()
}

/// A throwaway `<data_dir>/sessions` root. Each session writes its
/// `output.vt` under here (docs/02-model.md#storage); these tests only
/// care that the log exists somewhere private to them.
fn sessions_root(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "anchor-sessions-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock before 1970")
            .as_nanos()
    ))
}

fn spec(args: &[String], cols: u16, rows: u16, cwd: &std::path::Path) -> SpawnSpec {
    SpawnSpec {
        command: "/bin/sh".to_string(),
        args: args.to_vec(),
        cwd: cwd.to_path_buf(),
        env: BTreeMap::new(),
        cols,
        rows,
        login_shell: false,
    }
}

/// A session with zero subscribers is not a special case that needs to be
/// avoided -- it survives indefinitely (ROADMAP.md). Create one,
/// drive it through write/resize/terminate with nobody ever subscribing.
#[tokio::test]
async fn zero_subscribers_session_survives() {
    let manager = SessionManager::new(sessions_root("zero-subs"));
    let cwd = temp_dir();
    let args = vec![];
    let session = manager
        .create(&spec(&args, 80, 24, &cwd), BTreeMap::new())
        .expect("create session");

    session
        .write(b"echo hi\n")
        .expect("write with no subscriber");
    session.resize(40, 120).expect("resize with no subscriber");
    session.terminate().expect("terminate with no subscriber");
}

/// S3 one layer up (docs/03-pty.md#termination): a
/// `Session` must not serialize `write`/`resize`/`terminate` behind one lock.
/// A child that never reads its pty eventually blocks the pty writer thread;
/// `terminate()` must still return within its bounded policy (~7s) instead of
/// queueing behind that stuck writer.
///
/// **Reaching that premise is the whole difficulty (anchor#9).** The first
/// version of this test pushed 4 KiB chunks of `x` with no newline, and a tty
/// in canonical mode *discards* on line overflow rather than queueing: the
/// writer never blocked, 436 MB went into a child that read nothing, and
/// `terminate()` raced nothing for as long as the test existed. Chunks of
/// complete lines land in the canonical queue instead, which is ~1 KiB, so it
/// fills immediately and the next write to the master blocks for real.
///
/// The stall is then *waited for*, never slept past, and "stalled" means
/// **wedged, not merely slow**. `Session::write` fails once the 32-slot write
/// channel has been full for `WRITE_ENQUEUE_TIMEOUT`, and a tty that discards
/// is slow enough to produce that on its own — the blob form above fails
/// writes too, while the pty writer thread keeps making progress. Only an
/// unbroken streak of rejections says the writer thread is stuck inside one
/// `write()` and will never take another chunk. A single success resets the
/// streak, so the premise cannot be faked by a busy machine.
///
/// If the streak never arrives the test fails loudly instead of passing
/// vacuously, which is the whole point of the issue.
#[tokio::test]
async fn terminate_is_not_wedged_by_a_stuck_write() {
    let manager = SessionManager::new(sessions_root("stuck-write"));
    let cwd = temp_dir();
    // Execs `sleep`, which never reads its stdin, so nothing drains the tty.
    let args = vec!["-c".to_string(), "sleep 30".to_string()];
    let session = manager
        .create(&spec(&args, 80, 24, &cwd), BTreeMap::new())
        .expect("create session");

    // Runs on its own thread, not this test's async task, so a write that
    // blocks doesn't block the assertion below from ever running.
    let stalled = Arc::new(AtomicBool::new(false));
    let writer_session = Arc::clone(&session);
    let writer_stalled = Arc::clone(&stalled);
    std::thread::spawn(move || {
        // Complete lines, not a 4 KiB blob: see the note above.
        let chunk: Vec<u8> = std::iter::repeat_n(b"xxxxxxxxxxxxxxx\n", 64)
            .flatten()
            .copied()
            .collect();
        // Eight consecutive rejections is >=2s with not one chunk accepted:
        // a discarding tty interleaves successes and never gets here.
        const WEDGED_STREAK: u32 = 8;
        let mut consecutive = 0;
        loop {
            if writer_session.write(&chunk).is_ok() {
                consecutive = 0;
                continue;
            }
            consecutive += 1;
            if consecutive == WEDGED_STREAK {
                writer_stalled.store(true, Ordering::SeqCst);
                return;
            }
        }
    });

    let deadline = Instant::now() + DEFAULT_TIMEOUT;
    while !stalled.load(Ordering::SeqCst) {
        assert!(
            Instant::now() < deadline,
            "writes never stayed rejected for {DEFAULT_TIMEOUT:?}, so the pty \
             writer thread never wedged and this test would prove nothing"
        );
        std::thread::sleep(Duration::from_millis(10));
    }

    let t0 = Instant::now();
    session.terminate().expect("terminate should not error");
    assert!(
        t0.elapsed() < Duration::from_secs(9),
        "terminate() must not be wedged behind a stuck write"
    );
}

/// **Changed in M4** (docs/04-protocol.md):
/// `terminate()` alone must leave the session resolvable and listed as
/// `exited` -- only an explicit `?purge=true` (`SessionManager::purge`)
/// removes it. The M2-era version of this test asserted the opposite
/// (`terminate()` self-removing); that broke the "stays in the list as
/// exited" contract M4's API needs, so the behavior and this test both
/// changed together.
#[tokio::test]
async fn terminate_leaves_the_session_listed_until_purged() {
    let manager = SessionManager::new(sessions_root("terminate-leaves"));
    let cwd = temp_dir();
    let args = vec![];
    let session = manager
        .create(&spec(&args, 80, 24, &cwd), BTreeMap::new())
        .expect("create session");
    let id = session.id.clone();

    session.terminate().expect("terminate should not error");
    assert!(
        manager.get(&id).is_some(),
        "a terminated session must stay listed until purged"
    );

    // `terminate()` returning only guarantees the state left `running`; the
    // final `-> exited` transition is made by the exit listener thread
    // reacting to the same `wait()` result, asynchronously
    // (docs/03-pty.md#termination), so give it a moment.
    let deadline = Instant::now() + DEFAULT_TIMEOUT;
    loop {
        if session.state() == anchor::model::State::Exited {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "session never reached exited after terminate()"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert!(
        manager.purge(&id).is_some(),
        "purge must return the session it removed"
    );
    assert!(
        manager.get(&id).is_none(),
        "purged session must be gone from the manager"
    );
}

/// A subscriber that reads gets exactly what the session wrote, in order,
/// with monotonically increasing, non-overlapping offsets between
/// consecutive chunks.
///
/// Uses an interactive shell and subscribes *before* writing the command
/// that produces the output under test -- with a `-c "printf ..."` spec the
/// child can run and exit before `subscribe()` gets scheduled, and a
/// subscriber that registers after the fact sees nothing (no replay exists
/// yet; that's M3/M4, not this module's job). Even so, the shell's own
/// spawn-time chatter can land before `subscribe()`'s lock is acquired, so
/// this checks contiguity chunk-to-chunk, not that the first one starts at
/// offset 0 -- what happened before you subscribed is exactly the boundary
/// M2 doesn't promise anything about.
#[tokio::test]
async fn subscriber_receives_output_in_order() {
    let manager = SessionManager::new(sessions_root("in-order"));
    let cwd = temp_dir();
    let args = vec![];
    let session = manager
        .create(&spec(&args, 80, 24, &cwd), BTreeMap::new())
        .expect("create session");

    let mut sub = session.subscribe();
    session.write(b"printf 'hello world'\n").expect("write");

    // Cooked-mode echo means `acc` also carries the echoed input line, so
    // check for content with `contains` rather than an exact match.
    let mut acc = Vec::new();
    let mut next_expected_offset: Option<u64> = None;
    let deadline = Instant::now() + DEFAULT_TIMEOUT;
    while !contains(&acc, "hello world") {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "timed out; got {:?}",
            String::from_utf8_lossy(&acc)
        );
        let chunk = tokio::time::timeout(remaining, sub.recv())
            .await
            .expect("recv timed out")
            .expect("subscriber disconnected before all output arrived");
        if let Some(expected) = next_expected_offset {
            assert_eq!(
                chunk.offset, expected,
                "offsets must be contiguous, no gap or overlap"
            );
        }
        next_expected_offset = Some(chunk.offset + chunk.bytes.len() as u64);
        acc.extend_from_slice(&chunk.bytes);
    }
}

fn contains(acc: &[u8], needle: &str) -> bool {
    acc.windows(needle.len()).any(|w| w == needle.as_bytes())
}

/// The M2 gate itself: a subscriber that never reads must not slow the PTY
/// drain. A second, actively-draining subscriber on the same session proves
/// the reader thread kept moving; the never-reading one must eventually be
/// disconnected (bounded queue, docs/03-pty.md#backpressure) instead
/// of accumulating unboundedly.
///
/// Was gated off macOS, where this fixture's *fast* subscriber was
/// disconnected too, 3 times in 20 runs on real hardware. That was
/// teleport #N5:
/// the queue bound was `min(256 chunks, 8 MiB)`, and a macOS pty read
/// averages **14 bytes** (max 1024, the tty output buffer), so 256 chunks
/// held ~3.5 KiB -- roughly 2300x tighter than the byte half was designed to
/// allow, and enough to drop any subscriber that stalled even briefly. Linux
/// ptys coalesce far more, so the byte half governed there and the bound
/// behaved as documented.
///
/// Un-gated once the bound became a single byte budget that charges each
/// chunk a fixed overhead alongside its payload, which is what the count half
/// was really protecting. `fanout.rs`'s
/// `tiny_chunks_get_the_budget_not_a_slot_count` covers the same regression
/// deterministically and without a real pty.
#[tokio::test]
async fn slow_subscriber_is_disconnected_and_never_blocks_the_reader() {
    const N: usize = 10 * 1024 * 1024; // 10 MiB, comfortably past the 8 MiB bound.
    let manager = SessionManager::new(sessions_root("slow-sub"));
    let cwd = temp_dir();
    let args = vec![
        "-c".to_string(),
        format!("stty raw -echo; yes | head -c {N}"),
    ];
    let session = manager
        .create(&spec(&args, 80, 24, &cwd), BTreeMap::new())
        .expect("create session");

    let slow = session.subscribe(); // never read from this one.

    // `attach(0)`, not `subscribe()`. The child is already producing by the
    // time `create` returns, so a plain subscriber silently misses whatever
    // landed before it registered -- tens of KiB under load -- and can never
    // account for all N bytes, which reads as a stall that looks exactly like
    // the backpressure failure this test is trying to detect. `attach` counts
    // the replay too and closes that race
    // (docs/04-protocol.md).
    let replay = session.attach(0).expect("attach at 0");
    let (received_bytes, mut fast, _) = support::catch_up(replay, Duration::ZERO).await;
    let mut received = received_bytes.len();

    // Measured as the longest *gap* between chunks, not as total elapsed
    // time.
    //
    // The invariant is that the never-reading subscriber does not block the
    // reader thread. When that breaks, the fast subscriber goes quiet while
    // the reader sits in a write to the slow one -- a stall. It is not that
    // everything runs proportionally slower, which is what CPU contention
    // looks like.
    //
    // Total elapsed cannot tell those apart. A full-suite run under load
    // drained this in 22s with nothing wrong, and the old assertion
    // reported it as "reader looks backpressured" -- a load symptom wearing
    // the costume of the bug. A long gap means the same thing whether the
    // machine is idle or thrashing, which is what makes it the right
    // instrument here.
    let mut longest_gap = Duration::ZERO;
    let mut last_chunk_at = Instant::now();
    while received < N {
        let chunk = tokio::time::timeout(DEFAULT_TIMEOUT, fast.subscription.recv())
            .await
            .unwrap_or_else(|_| {
                panic!("fast subscriber stalled after {received}/{N} bytes -- reader was blocked by the slow one")
            })
            .expect("fast subscriber disconnected before receiving all output");
        longest_gap = longest_gap.max(last_chunk_at.elapsed());
        last_chunk_at = Instant::now();
        received += chunk.bytes.len();
    }
    assert!(
        longest_gap < STALL_BOUND,
        "fast subscriber went quiet for {longest_gap:?} mid-stream; \
         the reader was parked on the subscriber that never reads"
    );

    // The never-reading subscriber must have been dropped once it exceeded
    // the bound, not silently buffered -- that's what keeps memory flat.
    let disconnected = tokio::time::timeout(DEFAULT_TIMEOUT, async {
        let mut slow = slow;
        while slow.recv().await.is_some() {}
    })
    .await;
    assert!(
        disconnected.is_ok(),
        "slow subscriber was never disconnected -- unbounded buffering"
    );
}

/// A resize that arrives while the session is closing is refused, not
/// recorded: the PTY's control thread drops it, so the size the session
/// reports, persists and announces would otherwise be one the terminal
/// never had. The child ignores HUP and TERM, which holds the session in
/// `closing` for the whole graceful wait instead of racing it.
#[test]
fn resize_while_closing_is_refused_and_not_recorded() {
    let manager = SessionManager::new(sessions_root("resize-closing"));
    let cwd = temp_dir();
    let args = vec![
        "-c".to_string(),
        "trap '' HUP TERM; echo TRAPPED; while :; do sleep 1; done".to_string(),
    ];
    let session = manager
        .create(&spec(&args, 80, 24, &cwd), BTreeMap::new())
        .expect("create");

    // Output exists only once the trap is installed; terminating earlier
    // would let HUP kill the shell and close the window we are testing.
    wait_until(DEFAULT_TIMEOUT, "the trap to be installed", || {
        session.next_offset() > 0
    });

    let mut size_changes = session.watch_size();
    let terminator = {
        let session = Arc::clone(&session);
        std::thread::spawn(move || session.terminate())
    };

    // `write` is refused as soon as the PTY leaves `running`, which makes
    // it the observable edge of the closing window.
    wait_until(
        DEFAULT_TIMEOUT,
        "the session to stop accepting input",
        || session.write(b"x").is_err(),
    );

    assert!(
        session.resize(100, 40).is_err(),
        "a resize during closing must be refused"
    );
    assert_eq!(session.size(), (80, 24), "the refused size was recorded");
    assert!(
        size_changes.try_recv().is_err(),
        "a refused size was announced to subscribers"
    );

    terminator
        .join()
        .expect("terminator panicked")
        .expect("terminate");
}

/// Polls `cond` until it holds, failing with `what` after `timeout`. A
/// condition, not a sleep: the backstop is generous because it only ever
/// bounds a failure.
fn wait_until(timeout: Duration, what: &str, mut cond: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while !cond() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(5));
    }
}
