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
pub(crate) fn catch_up_allow_clamp(replay: Replay) -> (Vec<u8>, Attach, u32) {
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
                step = replay.written(bytes).expect("catch-up round");
            }
            ReplayStep::Live(attach) => {
                acc.extend_from_slice(&attach.replay);
                return (acc, attach, rounds);
            }
        }
    }
}
