//! Shared test-only helpers for the ported session fixtures.
//!
//! Only `catch_up` is taken from teleport's support module — everything else
//! there boots its HTTP daemon, which anchor does not have.
#![allow(dead_code)]

use std::time::Duration;

use anchor::session::{Attach, Replay, ReplayStep};

/// Generous upper bound on rounds any fixture using [`catch_up`] should ever
/// need. The "never converges" failure mode has its own deterministic
/// coverage in session.rs's unit tests; this exists only so a real
/// regression here hangs a test loudly and boundedly instead of forever.
const MAX_TEST_ROUNDS: u32 = 1024;

/// Drives a `Replay` to the live boundary the way M4's WS loop will: write
/// each catch-up round out before asking for the next
/// (docs/04-protocol.md). `round_delay`
/// paces each round -- `Duration::ZERO` for "as fast as possible" (most
/// fixtures, where a `Vec` client always outruns the producer), nonzero to
/// simulate a slow client's network round-trip
/// (`session_catchup.rs`'s D1 gate, which needs the producer to gain ground
/// on a genuinely slow consumer, not just an unrealistically instant one).
///
/// Returns every replayed byte (catch-up rounds and the final stretch
/// together), the live handover, and how many bounded rounds it took.
/// Rounds are asserted contiguous with each other; the join onto
/// `Attach::replay_from` is the caller's to check, because a cap can
/// legitimately move it forward. `attach.caught_up` is asserted here too --
/// every caller of this helper drives a client that outruns the producer, so
/// a catch-up that gives up and clamps means the fixture stopped testing
/// what it says it tests, not a real assertion about the product.
pub(crate) async fn catch_up(replay: Replay, round_delay: Duration) -> (Vec<u8>, Attach, u32) {
    let mut acc = Vec::new();
    let mut next = replay.replay_from;
    let mut rounds = 0u32;
    let mut step = replay.next_round().expect("first catch-up round");
    loop {
        match step {
            ReplayStep::History {
                offset,
                bytes,
                replay,
            } => {
                assert_eq!(offset, next, "catch-up rounds must be contiguous");
                next = offset + bytes.len() as u64;
                acc.extend_from_slice(&bytes);
                rounds += 1;
                assert!(
                    rounds <= MAX_TEST_ROUNDS,
                    "catch-up did not converge within {MAX_TEST_ROUNDS} rounds"
                );
                tokio::time::sleep(round_delay).await; // the "network"
                step = replay.written(bytes).expect("catch-up round");
            }
            ReplayStep::Live(attach) => {
                assert!(
                    attach.caught_up,
                    "a client that always outruns the producer must converge"
                );
                acc.extend_from_slice(&attach.replay);
                return (acc, attach, rounds);
            }
        }
    }
}

/// Like [`catch_up`], but for a caller that expects some of its concurrent
/// clients to lose the race against the producer -- N4's reconnect-storm
/// fixture, where `STORM_SIZE` clients all attach at once against an
/// unbounded `yes` and some are expected to hit the same
/// stalled/total-round clamp D1's own fixture in `session_catchup.rs`
/// exercises deliberately. Always paced at `Duration::ZERO`: a storm client
/// is modeling "reconnect as fast as possible," not a slow network.
///
/// `progress` is bumped by every round's byte count, so a caller running many
/// of these at once can tell "slow" from "stuck" without a total-time budget.
pub(crate) fn catch_up_allow_clamp(
    replay: Replay,
    progress: &std::sync::atomic::AtomicU64,
) -> (Vec<u8>, Attach, u32) {
    let mut acc = Vec::new();
    let mut next = replay.replay_from;
    let mut rounds = 0u32;
    let mut step = replay.next_round().expect("first catch-up round");
    loop {
        match step {
            ReplayStep::History {
                offset,
                bytes,
                replay,
            } => {
                assert_eq!(offset, next, "catch-up rounds must be contiguous");
                next = offset + bytes.len() as u64;
                acc.extend_from_slice(&bytes);
                progress.fetch_add(bytes.len() as u64, std::sync::atomic::Ordering::Relaxed);
                rounds += 1;
                assert!(
                    rounds <= MAX_TEST_ROUNDS,
                    "catch-up did not converge within {MAX_TEST_ROUNDS} rounds"
                );
                step = replay.written(bytes).expect("catch-up round");
            }
            ReplayStep::Live(attach) => {
                acc.extend_from_slice(&attach.replay);
                return (acc, attach, rounds);
            }
        }
    }
}

/// How long a producer may make *no* progress before a fixture gives up.
///
/// Deliberately not a budget for the whole target: how long a real
/// `/bin/sh` takes to push N megabytes through a pty depends on what else
/// the machine is doing, and under a full-suite run it is competing with
/// every other test binary doing the same thing.
pub(crate) const PRODUCER_STALL: Duration = Duration::from_secs(10);

/// Waits for a session to reach `target` bytes, failing only if it *stops
/// making progress*.
///
/// For preconditions, never for assertions. A fixture that needs real
/// output on disk before it can test anything is waiting on a shell, and a
/// fixed deadline there converts CPU contention into a failure of whatever
/// invariant the test guards. That is worse than an ordinary flake: the
/// failure wears the costume of the thing you care about, so the next
/// person to see it dismisses the real breakage too.
///
/// A genuinely dead producer still fails, and no slower than before: the
/// deadline resets only when the offset actually moves.
pub(crate) async fn await_output(session: &anchor::session::Session, target: u64, what: &str) {
    let mut last = 0;
    let mut stall_deadline = std::time::Instant::now() + PRODUCER_STALL;
    loop {
        let offset = session.next_offset();
        if offset >= target {
            return;
        }
        if offset > last {
            last = offset;
            stall_deadline = std::time::Instant::now() + PRODUCER_STALL;
        }
        assert!(
            std::time::Instant::now() < stall_deadline,
            "{what}: stopped producing at {offset} of {target} bytes"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
