//! **The D1 gate.** Attaching far behind a session that is still producing
//! must complete -- no `1013`, no gap, no duplicate --
//! docs/04-protocol.md and
//! ROADMAP.md
//!
//! The failure this guards against is a livelock, not a crash: register the
//! subscriber first and it spends its whole queue buffering live output for
//! the duration of a replay it has not finished writing, overflows, and is
//! disconnected *before it ever goes live*. It then reconnects further behind
//! and fails again. An idle session never shows it.
//!
//! The convergence arithmetic is covered deterministically by the unit tests
//! in `session.rs`; this fixture is the end-to-end one, so it pays real time
//! to a real child.
//!
//! **This fixture used to depend on a wall-clock rate, and that cost two
//! wrong turns.** The control subscriber was killed by a trickle out-running
//! the queue bound during the catch-up window, which made the fixture correct
//! only inside a rate band: too slow and its own guard fired because nothing
//! was being reproduced; too fast and the *attaching* subscriber was
//! disconnected mid-catch-up for a reason D1 is not about. It was tuned
//! twice -- once by slowing the rounds, once by batching the trickle's writes
//! to take `fork` cost out of the rate -- and both times it passed locally and
//! failed on `macos-latest`, a runner nobody can reproduce. Chasing a third
//! calibration was not going to work.
//!
//! It is now driven by **bytes, not rate**. The child blocks on a read from
//! its pty until the test writes to it, then emits a `BURST` larger than the
//! whole queue bound. The control subscriber is registered across all of it
//! and never drains, so it is disconnected by construction on any hardware,
//! at any speed. Nothing here is calibrated against how fast anything runs.
//!
//! The trade that buys: the burst is drained to completion *before* the
//! catch-up walk starts, so the control dies just before the window rather
//! than inside it. That is deliberate. Letting the burst land during the walk
//! reintroduces a rate dependency in the other direction -- a producer that
//! out-runs the client for four consecutive rounds trips
//! `MAX_STALLED_ROUNDS`, `should_register` clamps and reports a hole, and
//! `support::catch_up`'s `caught_up` assertion fires. What the control proves
//! is that the register-first ordering is fatal; *when* it dies was never the
//! claim, and it is no longer load-bearing.
//!
//! The session is still producing throughout the walk -- that is the trickle,
//! which now only has to be non-zero rather than land inside a band.
//!
//! Un-gated from `target_os = "linux"` to `cfg(unix)` once the queue bound
//! became a byte budget
//! (teleport #N5).
//! The old count half is what this fixture used to reach: `tick\n` is 5 bytes,
//! so 256 chunks tripped at ~5 KiB -- on Linux too, not just macOS. Fixing the
//! bound removed that, which is why the redesign above shipped in the same
//! change rather than after it.

#![cfg(unix)]

mod support;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anchor::model::SpawnSpec;
use anchor::session::{SessionManager, MAX_QUEUE_BYTES};

const RECV_TIMEOUT: Duration = Duration::from_secs(30);
/// Backlog to attach behind. Sized in catch-up rounds rather than against the
/// queue bound: it is what makes this attach take many bounded rounds instead
/// of one, which is the shape D1 is about.
const BACKLOG: u64 = 12 * 1024 * 1024;
/// Live output the child emits on demand, once the control subscriber is
/// registered. Half again the queue bound, so that subscriber overflows on
/// any platform whatever its pty read granularity happens to be -- the whole
/// point of driving this with bytes instead of a rate.
const BURST: u64 = 3 * MAX_QUEUE_BYTES as u64 / 2;
/// The byte the test writes to release the burst. Newline because the child
/// waits on `read`, and the pty is in raw mode with echo off so it neither
/// needs translating nor lands back in the output log.
const TRIGGER: &[u8] = b"\n";
/// What one catch-up round costs this "client" -- a slow link, so the walk is
/// a real sequence of bounded rounds rather than a memcpy. Its value is no
/// longer load-bearing: nothing is racing it.
const ROUND_LATENCY: Duration = Duration::from_millis(50);
/// Writes the trickle emits between forks of `sleep`, so its rate is set by
/// the batch rather than by what `fork` costs on this OS. Only has to be
/// non-zero now.
const TICKS_PER_SLEEP: u32 = 4;

fn sessions_root(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "anchor-catchup-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock before 1970")
            .as_nanos()
    ))
}

/// A session in exactly the shape D1 is about: a large backlog already on
/// disk, and a child that is *still emitting* while the client catches up.
///
/// Three phases, in order. The backlog, so there is something to walk. Then a
/// blocking `read` on the pty, which is the whole trick -- it lets the test
/// decide, rather than guess, when live output starts, so the control
/// subscriber below is provably registered for all of it. Then `BURST`, and
/// finally the trickle that keeps the session producing for the rest of the
/// walk.
///
/// `read` rather than `head -n 1`: it is a shell builtin that consumes one
/// byte at a time from a non-seekable fd, so it cannot swallow output the
/// test has not accounted for.
fn spawn_backlog_then_trickle(
    manager: &SessionManager,
) -> std::sync::Arc<anchor::session::Session> {
    let cwd = std::env::temp_dir();
    let args = vec![
        "-c".to_string(),
        format!(
            "stty raw -echo; yes | head -c {BACKLOG}; \
             read _trigger; \
             yes | head -c {BURST}; \
             while :; do i=0; \
             while [ $i -lt {TICKS_PER_SLEEP} ]; do printf 'tick\\n'; i=$((i+1)); done; \
             sleep 0.01; done"
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

#[tokio::test]
async fn attaching_far_behind_a_producing_session_reaches_live() {
    let manager = SessionManager::new(sessions_root("d1"));
    let session = spawn_backlog_then_trickle(&manager);

    // Wait for the backlog to actually exist -- attaching before it does
    // would test nothing.
    let deadline = Instant::now() + RECV_TIMEOUT;
    while session.next_offset() < BACKLOG {
        assert!(
            Instant::now() < deadline,
            "the child never produced the backlog"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // A subscriber registered *before* its replay is written -- the ordering
    // D1 rejects. Nothing reads it, ever, which is exactly what a client on a
    // slow link does to its own queue while it writes a replay out.
    let registered_first = session.subscribe();

    // Release the burst, and let it land in full before the walk starts.
    // `registered_first` is registered across every byte of it and drains
    // none, and `BURST` is half again the entire queue bound, so it overflows
    // on any platform at any speed. This is the one thing the fixture used to
    // leave to a wall-clock race; see the module doc.
    session.write(TRIGGER).expect("write the burst trigger");
    let deadline = Instant::now() + RECV_TIMEOUT;
    while session.next_offset() < BACKLOG + BURST {
        assert!(
            Instant::now() < deadline,
            "the child never emitted the burst"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let replay = session.attach(0).expect("attach at 0");
    assert_eq!(replay.replay_from, 0);
    let ready_next_offset = replay.next_offset;
    assert!(ready_next_offset >= BACKLOG + BURST);

    // Catch up the way a client on a slow link does: write each round out
    // before asking for the next, with `ROUND_LATENCY` standing in for the
    // network. The child keeps trickling throughout, so this is a catch-up
    // against a session that is still producing, which is the D1 shape.
    let (mut acc, attach, rounds) = support::catch_up(replay, ROUND_LATENCY).await;

    assert!(
        rounds > 8,
        "a {BACKLOG}-byte backlog plus a {BURST}-byte burst must take several \
         bounded rounds, got {rounds}"
    );
    let mut end = attach.replay_to();
    assert_eq!(
        end,
        acc.len() as u64,
        "replay rounds plus the final stretch must equal every byte served"
    );
    assert!(
        end >= ready_next_offset,
        "catch-up must reach at least the boundary `ready` announced"
    );

    // The whole point: this subscriber is still connected, and its first
    // chunk continues the replay exactly.
    let mut subscription = attach.subscription;
    for i in 0..8 {
        let chunk = tokio::time::timeout(RECV_TIMEOUT, subscription.recv())
            .await
            .unwrap_or_else(|_| panic!("live chunk {i} never arrived"))
            .unwrap_or_else(|| {
                panic!("subscriber was disconnected after catch-up -- D1 has regressed")
            });
        assert_eq!(
            chunk.offset, end,
            "live output must continue exactly where replay stopped"
        );
        acc.extend_from_slice(&chunk.bytes);
        end += chunk.bytes.len() as u64;
    }

    // The control: the up-front subscriber blew its queue on live output it
    // had to buffer while owing its client a replay -- the same session this
    // attach walked through unharmed. Draining to `None` is the disconnect.
    let old_ordering_died = tokio::time::timeout(RECV_TIMEOUT, async {
        let mut sub = registered_first;
        while sub.recv().await.is_some() {}
    })
    .await;
    assert!(
        old_ordering_died.is_ok(),
        "the pre-registered subscriber survived a {BURST}-byte burst against a \
         {MAX_QUEUE_BYTES}-byte queue bound -- the bound is not being enforced"
    );

    let on_disk = std::fs::read(session.log_path()).expect("read output.vt");
    assert!(on_disk.len() as u64 >= end);
    assert_eq!(
        acc,
        on_disk[..acc.len()],
        "replay + live must equal the log byte for byte -- no gap, no duplicate"
    );

    session.terminate().expect("terminate");
}
