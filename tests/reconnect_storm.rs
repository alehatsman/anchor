//! N4 -- reconnect-storm load test
//! (listed under M1 in ROADMAP.md).
//!
//! Catch-up ([04](../../docs/04-protocol.md))
//! reads history off the same `Mutex<Fanout>` the PTY reader thread locks on every
//! `publish()`. N4's own text is explicit about what this file is and isn't:
//! bounding *one* client's catch-up (stalled-round / total-round ceilings) was
//! already closed by D1; what was never measured is many clients catching up
//! *at once* -- the shape of a reconnect storm after a network blip drops every
//! attached client on a multi-session host in the same second.
//!
//! This is a measurement, not a correctness gate with a number picked in
//! advance: N4 says "if it stays acceptable, record the number and close
//! this." So the hard assertions here are the invariants that must hold
//! regardless of how contended the runner gets (the control subscriber is
//! never disconnected, no gap/duplicate, no single `recv()` stalls
//! unboundedly), and the actual throughput/latency numbers are printed for a
//! human to read out of a real CI run's log -- same methodology
//! teleport #W3
//! used to find its own contention, not a threshold guessed at against a
//! runner this repo cannot reproduce locally
//! (teleport #N5's
//! own lesson).
//!
//! `cfg(unix)`, not `target_os = "linux"`. It shipped Linux-only "for the same
//! reason `session_catchup.rs` and the N5 fixtures are" -- but that reason had
//! already been overturned four commits earlier: #29/#32 root-caused those
//! gates to the queue bound's *count* half (wrong on Linux too, merely
//! invisible there) and un-gated all three to `cfg(unix)`. Rather than
//! re-guess, measured on real macOS hardware (Darwin 24.6.0, arm64): 20/20
//! green, 6.77-6.84s wall clock, storm/baseline ratio 0.966-1.094 -- no
//! measurable degradation, and nowhere near `STORM_STALL`.
//!
//! It ports because of how this fixture is built, which is worth stating so
//! the gate is not reintroduced by reflex: the hard assertions are invariants
//! (no disconnect, no gap, no unbounded stall), never a throughput threshold,
//! so there is no tuned number to re-tune. And N5's tiny-read behaviour never
//! reaches the measured window -- the trickle is one 1000-byte `printf` per
//! 10ms, which arrives as one 1000-byte chunk on macOS too (106 chunks /
//! 106,000 bytes, exactly 1000 B each). `yes`'s tiny lines only shape the
//! backlog phase, which completes before anyone attaches and which
//! `session_catchup.rs` already proves on macOS with this same 12 MiB
//! constant.

#![cfg(unix)]

mod support;

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anchor::model::SpawnSpec;
use anchor::session::{Session, SessionManager};

const RECV_TIMEOUT: Duration = Duration::from_secs(10);
/// How long the storm as a whole may go without ANY client catching up
/// another byte. Not a budget for the catch-up: 40 clients each replaying
/// 12 MiB takes as long as the machine takes, and a total deadline would
/// measure that rather than whether anything is stuck. A client that is
/// definitively losing ground is expected to hit `MAX_STALLED_ROUNDS`
/// (session/replay.rs) and register within a handful of rounds; this is the
/// loud, bounded backstop if the clamp logic itself regresses and the storm
/// simply stops moving.
const STORM_STALL: Duration = Duration::from_secs(10);
/// How long to measure the control subscriber's throughput before, and then
/// during, the storm. Long enough to average out scheduling noise, short
/// enough that CI doesn't pay minutes for a measurement fixture.
const MEASURE_WINDOW: Duration = Duration::from_secs(2);
/// Simultaneous reconnecting clients. Large enough to actually contend the
/// fan-out mutex against a real reader thread (N5 needed real concurrency to
/// show up too, not just "more than one"), small enough to stay a
/// reconnect-storm-after-a-blip scenario rather than a stress test of a
/// different question entirely.
const STORM_SIZE: usize = 40;
/// The static backlog every `attach(0)` (control and storm alike) has to
/// replay. Same value `session_catchup.rs`'s D1 gate uses, reused
/// deliberately rather than picked fresh: it is already proven, on this
/// exact CI, to be more than the 8 MiB queue bound (so no subscriber could
/// hold it live) and to take several real 1 MiB catch-up rounds -- exactly
/// the per-client work this fixture wants `STORM_SIZE` clients doing
/// concurrently.
const BACKLOG: u64 = 12 * 1024 * 1024;

/// A session with a static `BACKLOG`-byte backlog already on disk, followed
/// by a slow, bounded trickle -- exactly `session_catchup.rs`'s own
/// backlog-then-trickle shape, reused rather than reinvented.
///
/// **This fixture's own first attempt used a raw unbounded `yes` (then a
/// paced burst-then-sleep loop) running for the fixture's whole duration,
/// and both overflowed the control subscriber's own 8 MiB queue within a
/// fraction of a second on real `ubuntu-latest` CI -- before the storm ever
/// started, and before any contention was even in play.** The actual bug
/// was ordering, not rate: unlike `session_catchup.rs`'s own fixture, this
/// one was attaching the control subscriber the moment *any* output existed
/// rather than after the intended backlog had fully landed, so its
/// `attach(0)` raced an actively-bursting producer no pacing choice could
/// out-guess. `spawn_hot_session` now only ever produces `BACKLOG` bytes
/// once, and the caller (`control_subscriber_survives_a_concurrent_reconnect_storm`)
/// waits for all of it to land *before* attaching anyone -- after that, the
/// only ongoing production is the trickle below, slow enough that the
/// control subscriber can never overflow regardless of storm contention.
/// The backlog itself is what gives `STORM_SIZE` concurrent `attach(0)`
/// clients real multi-round catch-up work once they do arrive.
fn spawn_hot_session(manager: &SessionManager) -> Arc<Session> {
    let cwd = std::env::temp_dir();
    let args = vec![
        "-c".to_string(),
        format!(
            "stty raw -echo; yes | head -c {BACKLOG}; \
             while :; do printf '%01000d' 1; sleep 0.01; done"
        ),
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

/// Drains `subscription` for `window`, appending every byte to `acc` and
/// advancing `*next_offset` -- asserting each chunk starts exactly where the
/// last one left off, so a gap or duplicate introduced by contention on the
/// fan-out mutex fails loudly here rather than surviving as a length-only
/// coincidence. Also asserts no single `recv()` ever stalls past
/// `RECV_TIMEOUT` -- the actual invariant N4 cares about (the reader thread
/// must never be blocked indefinitely by the storm) -- and never returns
/// `None` (never disconnected). Returns bytes/chunks received in this window
/// only, for the throughput printout.
async fn drain_for(
    subscription: &mut anchor::session::Subscription,
    acc: &mut Vec<u8>,
    next_offset: &mut u64,
    window: Duration,
) -> (u64, u64) {
    let mut bytes = 0u64;
    let mut chunks = 0u64;
    let deadline = Instant::now() + window;
    while Instant::now() < deadline {
        let chunk = tokio::time::timeout(RECV_TIMEOUT, subscription.recv())
            .await
            .expect("control subscriber stalled past RECV_TIMEOUT during measurement")
            .expect("control subscriber was disconnected during measurement");
        assert_eq!(
            chunk.offset, *next_offset,
            "control subscriber must see contiguous offsets even under storm contention"
        );
        *next_offset += chunk.bytes.len() as u64;
        bytes += chunk.bytes.len() as u64;
        chunks += 1;
        acc.extend_from_slice(&chunk.bytes);
    }
    (bytes, chunks)
}

/// The N4 measurement: one long-lived "control" subscriber (attached before
/// the storm, the way a client that was already watching a session would be)
/// is drained continuously while `STORM_SIZE` fresh clients simultaneously
/// `attach(0)` and catch up as fast as they can against the same hot session.
/// The control subscriber must survive the storm untouched -- no
/// disconnect, no stall, no gap in what it received -- and its measured
/// throughput before vs. during the storm is printed for a human to read out
/// of the real CI run this needs (`cargo test -- --nocapture`).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn control_subscriber_survives_a_concurrent_reconnect_storm() {
    let dir = tempfile::tempdir().expect("a tempdir");
    let manager = SessionManager::new(dir.path().to_path_buf());
    let session = spawn_hot_session(&manager);

    // Wait for the *entire* backlog to land before anyone attaches -- not
    // just "some output exists." This is the precondition D1's own fixture
    // insists on, and skipping it was this fixture's own first bug: an
    // `attach(0)` that races an actively-bursting producer can overflow a
    // subscriber's queue with no contention involved at all (see
    // `spawn_hot_session`'s doc comment).
    support::await_output(&session, BACKLOG, "the backlog precondition").await;

    // The control subscriber: attaches once, up front, like a client that
    // was already watching when the network blip that triggers the storm
    // happens to everyone else.
    let replay = session.attach(0).expect("control attach");
    let (mut control_acc, control_attach, _rounds) =
        support::catch_up(replay, Duration::ZERO).await;
    // `catch_up` already folds `attach.replay` (the final post-registration
    // stretch) into its returned accumulator -- see its own doc comment.
    // Appending it again here was this fixture's own bug, caught by the
    // very byte-for-byte check below firing on real ubuntu-latest CI: it
    // silently double-counted that stretch's bytes, making `control_acc`
    // longer than what the log actually holds at this point and failing
    // the `on_disk.len() >= control_acc.len()` assertion for a reason that
    // had nothing to do with the storm.
    let mut control_next_offset = control_attach.next_offset;
    let mut control_sub = control_attach.subscription;

    // Baseline: how fast does this subscriber drain with nobody else
    // touching the fan-out mutex?
    let (baseline_bytes, baseline_chunks) = drain_for(
        &mut control_sub,
        &mut control_acc,
        &mut control_next_offset,
        MEASURE_WINDOW,
    )
    .await;

    // The storm: STORM_SIZE clients, each a fresh attach(0) against the same
    // hot session, all racing the fan-out mutex concurrently. Real
    // concurrency (`flavor = "multi_thread"`), not cooperative interleaving
    // on one thread -- N5 needed the same to reproduce anything real.
    // Individual storm clients are allowed to fall behind and get clamped
    // (`caught_up == false`) if 40-way contention on the fan-out mutex ever
    // makes one lose ground -- that is an acceptable outcome, not what this
    // fixture is checking; only the control subscriber's own survival and
    // continuity are asserted.
    let progress = Arc::new(AtomicU64::new(0));
    let storm_tasks: Vec<_> = (0..STORM_SIZE)
        .map(|_| {
            let session = Arc::clone(&session);
            let progress = Arc::clone(&progress);
            // Blocking work, so on the blocking pool: as plain tasks, a client
            // that got stuck would hold a worker, and enough of them would
            // starve the very loop below that is there to notice.
            tokio::task::spawn_blocking(move || {
                let replay = session.attach(0).expect("storm client attach");
                let (_acc, _attach, rounds) = support::catch_up_allow_clamp(replay, &progress);
                rounds
            })
        })
        .collect();

    // Measure the control subscriber's throughput *during* the storm --
    // concurrently with the storm tasks above, not after they finish, since
    // "after" would just measure the quiet aftermath.
    let (storm_bytes, storm_chunks) = drain_for(
        &mut control_sub,
        &mut control_acc,
        &mut control_next_offset,
        MEASURE_WINDOW,
    )
    .await;

    // Wait for every client, failing only if the storm as a whole stops
    // moving: the deadline resets whenever any client catches up more bytes.
    let mut seen = 0;
    let mut stall_deadline = Instant::now() + STORM_STALL;
    while !storm_tasks.iter().all(|t| t.is_finished()) {
        let now_seen = progress.load(Ordering::Relaxed);
        if now_seen > seen {
            seen = now_seen;
            stall_deadline = Instant::now() + STORM_STALL;
        }
        assert!(
            Instant::now() < stall_deadline,
            "no storm client made progress for {STORM_STALL:?} ({seen} bytes caught up in all)"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    for task in storm_tasks {
        task.await.expect("storm client task panicked");
    }

    // The invariant: the control subscriber was never disconnected and never
    // stalled past RECV_TIMEOUT on any single recv() (both already asserted
    // inside `drain_for`, which would have panicked otherwise). Printed
    // rather than threshold-asserted -- see the module doc for why.
    let baseline_rate = baseline_bytes as f64 / MEASURE_WINDOW.as_secs_f64();
    let storm_rate = storm_bytes as f64 / MEASURE_WINDOW.as_secs_f64();
    println!(
        "N4: baseline {baseline_bytes} bytes / {baseline_chunks} chunks over {:?} ({baseline_rate:.0} B/s); \
         during a {STORM_SIZE}-client storm: {storm_bytes} bytes / {storm_chunks} chunks over {:?} ({storm_rate:.0} B/s); \
         ratio {:.3}",
        MEASURE_WINDOW,
        MEASURE_WINDOW,
        storm_rate / baseline_rate.max(1.0)
    );

    // No gap, no duplicate: everything the control subscriber ever received
    // (initial replay, baseline window, storm window) must equal a
    // byte-for-byte prefix of the on-disk log, even after sitting through
    // `STORM_SIZE` concurrent clients hammering the same fan-out mutex.
    let on_disk = std::fs::read(session.log_path()).expect("read output.vt");
    assert!(
        on_disk.len() >= control_acc.len(),
        "the log must have at least as many bytes as the control subscriber ever saw"
    );
    assert_eq!(
        control_acc,
        on_disk[..control_acc.len()],
        "replay + live must equal the log byte for byte -- no gap, no duplicate, storm or not"
    );

    session.terminate().expect("terminate");
}
