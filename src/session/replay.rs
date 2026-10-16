//! The catch-up half of an attach -- read history off `fanout.rs`'s mutex in
//! bounded rounds, registering a live subscriber only once the remaining gap
//! is small enough to fit its queue. See the parent module doc
//! (`session/mod.rs`, the D1 callout) and
//! docs/04-protocol.md for the design
//! this implements; `mod.rs`'s `Session::attach` is the only place that
//! constructs a [`Replay`].

use std::sync::Arc;

use parking_lot::Mutex;

use crate::log::LogReader;

use super::fanout::{Fanout, Subscription};

/// The most history a subscriber may still owe its client at the moment it is
/// registered. One eighth of the queue bound, so seven eighths stay free for
/// live output while the client writes that last stretch out -- which is the
/// headroom D1 says the design was missing
/// (docs/04-protocol.md).
///
/// This derivation is only honest while the queue bound is *actually* a byte
/// budget. It was not, until teleport #N5: a
/// second, count-based half of the bound governed on macOS at ~3.5 KiB, so
/// this handed subscribers live with a debt 300x larger than the queue they
/// were about to be given.
pub const LIVE_GAP_BYTES: u64 = super::fanout::MAX_QUEUE_BYTES as u64 / 8;

/// Bytes one catch-up round reads off the fan-out mutex. Doubles as the
/// pacing unit the client paints at, instead of one write of the whole log
/// (teleport docs/15-open-questions.md).
pub const REPLAY_ROUND_BYTES: u64 = LIVE_GAP_BYTES;

/// Consecutive rounds the gap may fail to shrink before the daemon stops
/// trying to catch this client up. A client that loses ground four rounds
/// running is not going to survive the live stream either; clamping and
/// reporting the hole beats an unbounded reconnect loop.
const MAX_STALLED_ROUNDS: u32 = 4;

/// Absolute ceiling on catch-up rounds, independent of `MAX_STALLED_ROUNDS`.
/// A client whose throughput barely exceeds the producer's shrinks the gap
/// by a few bytes every round -- resetting `stalled_rounds` to 0 each time
/// -- and can defer registration indefinitely, re-acquiring the fan-out
/// mutex every round with no bound in sight. Sized at four full backlogs of
/// the default log cap so a legitimate one-shot catch-up of the largest
/// on-disk log never trips it; a client that still hasn't converged after
/// this many rounds gets the same clamp-and-report-the-hole treatment a
/// stalled one does.
const MAX_CATCHUP_ROUNDS: u32 = 4 * (crate::log::DEFAULT_LOG_MAX_BYTES / REPLAY_ROUND_BYTES) as u32;

/// Whether this round should register the subscriber and go live. A pure
/// function of the three catch-up floors so each can be exercised without
/// driving a `Replay` through real rounds: the gap already fits (the common
/// case), the client has stopped gaining ground at all
/// (`MAX_STALLED_ROUNDS`), or it hasn't converged in any number of rounds
/// (`MAX_CATCHUP_ROUNDS`) even though it kept gaining a little each time.
fn should_register(gap: u64, stalled_rounds: u32, total_rounds: u32) -> bool {
    gap <= LIVE_GAP_BYTES
        || stalled_rounds >= MAX_STALLED_ROUNDS
        || total_rounds >= MAX_CATCHUP_ROUNDS
}

/// The catch-up half of an attach: a read handle plus a cursor, not yet
/// registered with the fan-out.
///
/// The three public fields are exactly what `ready` needs
/// (docs/04-protocol.md) and they are available before
/// any history has been written, which is why `ready` can still be the first
/// frame. Drive it with [`next_round`](Self::next_round) until it hands back
/// an [`Attach`].
#[derive(Debug)]
pub struct Replay {
    /// Where replay actually starts -- `ready.replay_from`. The requested
    /// offset, except for a client attaching past a cap: there it is
    /// `next_offset` and nothing is replayed
    /// (docs/02-model.md#the-size-cap).
    pub replay_from: u64,
    /// The boundary captured when the client attached -- `ready.next_offset`.
    /// **Not** necessarily the offset the subscriber is finally registered
    /// at; a client that consumes up to here holds the session's full history
    /// as of the moment it attached, which is all `ready` ever promised
    /// (docs/04-protocol.md).
    pub next_offset: u64,
    /// The cap as of the attach, for `ready`. A cap that lands *during*
    /// catch-up shows up on [`Attach::log_capped_at`] instead; either way the
    /// client sees the hole it creates as a jump in the offset prefix
    /// (docs/04-protocol.md).
    pub log_capped_at: Option<u64>,

    pub(super) fanout: Arc<Mutex<Fanout>>,
    pub(super) reader: LogReader,
    /// Next byte still owed to the client.
    pub(super) cursor: u64,
    /// The gap as of the previous round, for convergence detection. Starts at
    /// `u64::MAX` so the first round can never count as a stall.
    pub(super) previous_gap: u64,
    pub(super) stalled_rounds: u32,
    /// Rounds run so far, counted whether or not the gap shrank. A client
    /// that gains a few bytes of ground every round resets `stalled_rounds`
    /// forever without ever registering -- see `MAX_CATCHUP_ROUNDS`.
    pub(super) total_rounds: u32,
}

/// One step of a catch-up loop. The `History` variant carries the rest of the
/// [`Replay`] rather than borrowing it, so a caller cannot pump a replay that
/// has already gone live and register a second subscriber by accident.
#[derive(Debug)]
pub enum ReplayStep {
    /// A bounded stretch of history. Write it to the client, then call
    /// [`HistoryReplay::written`], handing `bytes` back, to get the next step.
    History {
        /// Offset of `bytes`'s first byte.
        offset: u64,
        /// This round's stretch of history.
        bytes: Vec<u8>,
        /// Call [`HistoryReplay::written`] on this, with `bytes` back, to
        /// get the next step.
        replay: HistoryReplay,
    },
    /// The gap closed: the subscriber is registered and the handover is set
    /// up. Write [`Attach::replay`] first, then stream the subscription.
    Live(Attach),
}

/// A `Replay` that has just served one round of history. `written` is the
/// only way back to a [`ReplayStep`] from here, and it takes that round's
/// `bytes` as an argument -- not just `self` -- so a caller cannot ask for
/// the next round without first having them in hand. That is what turns
/// "write this round before requesting the next one"
/// (docs/04-protocol.md) from a
/// comment into something the compiler checks: there is no path from a
/// `History` step to the next one that does not pass through `bytes`.
#[derive(Debug)]
pub struct HistoryReplay {
    round_len: usize,
    replay: Replay,
}

impl HistoryReplay {
    /// Advances the catch-up loop. `bytes` must be the round's own bytes --
    /// checked by length, not just present for the type checker's sake -- so
    /// passing back the wrong thing (or a placeholder) fails loudly here
    /// rather than quietly reintroducing the pre-fetch race D1 closed.
    pub fn written(self, bytes: Vec<u8>) -> Result<ReplayStep, AttachError> {
        assert_eq!(
            bytes.len(),
            self.round_len,
            "HistoryReplay::written must be called with this round's own bytes -- \
             docs/04-protocol.md"
        );
        self.replay.next_round()
    }
}

/// A subscriber registered at the replay boundary, with the last stretch of
/// history it still owes. Write `replay` (starting at `replay_from`), then
/// every chunk from `subscription`; the two meet exactly once -- no gap, no
/// duplicate.
#[derive(Debug)]
pub struct Attach {
    /// Where the final stretch of replay starts.
    pub replay_from: u64,
    /// The final stretch of history, read after registration and bounded by
    /// `LIVE_GAP_BYTES`. Empty for a client that was already at the boundary,
    /// or one attaching past a cap.
    pub replay: Vec<u8>,
    /// `N` -- the boundary. Every chunk from `subscription` starts here or
    /// later, guaranteed by the single lock the registering round takes.
    pub next_offset: u64,
    /// The cap as of registration, if the log was capped.
    pub log_capped_at: Option<u64>,
    /// False when the catch-up loop gave up: the client kept losing ground,
    /// so `replay_from` was moved forward and the bytes behind it were
    /// dropped. M4 has nothing extra to send -- the hole is visible as a jump
    /// in the offset prefix, which clients must already handle for the
    /// log-cap case (docs/04-protocol.md).
    pub caught_up: bool,
    /// The live handle: every chunk from here starts at or after
    /// `next_offset`.
    pub subscription: Subscription,
}

impl Attach {
    /// One past the last replayed byte.
    pub fn replay_to(&self) -> u64 {
        self.replay_from + self.replay.len() as u64
    }
}

impl Replay {
    /// Advances the catch-up loop by one round.
    ///
    /// Either hands back a bounded stretch of history for the caller to write
    /// -- read *off* the fan-out mutex, so a slow client never holds up the
    /// PTY -- or, once the remaining gap fits comfortably inside a
    /// subscriber's queue, registers and returns the live handover.
    ///
    /// The caller must write each `History` round to its client **before**
    /// asking for the next one. That is what makes the gap a measurement of
    /// whether the client is outrunning the producer, and therefore what
    /// makes the loop converge (docs/04-protocol.md).
    pub fn next_round(mut self) -> Result<ReplayStep, AttachError> {
        // Everything under the lock is arithmetic plus, on the last round, a
        // registration: no file I/O, same short hold the reader thread takes.
        let (next_offset, end, log_capped_at, subscription) = {
            let mut fanout = self.fanout.lock();
            let next_offset = fanout.log.next_offset();
            let end = next_offset.min(fanout.log.readable_end());
            let log_capped_at = fanout.log.log_capped_at();

            let gap = end.saturating_sub(self.cursor);
            if gap >= self.previous_gap {
                self.stalled_rounds += 1;
            } else {
                self.stalled_rounds = 0;
            }
            self.previous_gap = gap;
            self.total_rounds += 1;

            let register = should_register(gap, self.stalled_rounds, self.total_rounds);
            let subscription = register.then(|| fanout.register(&self.fanout));
            (next_offset, end, log_capped_at, subscription)
        };

        let Some(subscription) = subscription else {
            let to = (self.cursor + REPLAY_ROUND_BYTES).min(end);
            let offset = self.cursor;
            let bytes = self
                .reader
                .read_range(offset, to)
                .map_err(|source| AttachError::Read { offset, source })?;
            // Advance by what was actually read, not by what was asked for: a
            // short read must not leave a hole the client is never told about.
            self.cursor += bytes.len() as u64;
            let round_len = bytes.len();
            return Ok(ReplayStep::History {
                offset,
                bytes,
                replay: HistoryReplay {
                    round_len,
                    replay: self,
                },
            });
        };

        let mut replay_from = self.cursor;
        let mut caught_up = true;
        if replay_from >= end {
            // Nothing left, or everything left is behind a cap. Either way the
            // stream resumes at the boundary rather than at a file position
            // that no longer means what the client thinks
            // (docs/02-model.md#the-size-cap).
            replay_from = next_offset;
        } else if end - replay_from > LIVE_GAP_BYTES {
            // Stalled: this client cannot be caught up. Serve it the freshest
            // `LIVE_GAP_BYTES` and leave a reported hole behind them, rather
            // than a queue it is certain to overflow.
            replay_from = end - LIVE_GAP_BYTES;
            caught_up = false;
        }

        // Read off the lock. The subscriber is already registered, so these
        // bytes and the first queued chunk are contiguous by construction:
        // this range ends at or before `end <= next_offset`, and every chunk
        // that subscription will ever see starts at or after `next_offset`.
        let replay =
            self.reader
                .read_range(replay_from, end)
                .map_err(|source| AttachError::Read {
                    offset: replay_from,
                    source,
                })?;

        Ok(ReplayStep::Live(Attach {
            replay_from,
            replay,
            next_offset,
            log_capped_at,
            caught_up,
            subscription,
        }))
    }
}

/// Why `Session::attach` couldn't produce an [`Attach`].
#[derive(Debug, thiserror::Error)]
pub enum AttachError {
    /// The client holds an offset the daemon never handed out -- a purged
    /// log, or a stale client after a `lost` session. M4 renders this as the
    /// `offset_ahead` error frame (docs/04-protocol.md).
    #[error("requested offset {requested} is ahead of next_offset {next_offset}")]
    OffsetAhead {
        /// The offset the client asked to resume from.
        requested: u64,
        /// The offset the daemon actually has data from.
        next_offset: u64,
    },
    /// `Session::attach`'s own `fanout.log.reader()` call failed before any
    /// round ever ran.
    #[error("opening the log for replay: {0}")]
    Open(std::io::Error),
    /// A `read_range` call failed partway through catch-up -- history-round
    /// or the final post-registration stretch alike. Distinct from `Open` so
    /// an incident doesn't read a mid-walk read failure as though the
    /// initial open never succeeded (issue #1 finding 4).
    ///
    /// `offset` is where this round was about to read from -- nothing at or
    /// past it was served. The `Replay` itself is gone (a transient failure
    /// still drops the cursor, the stall counters and the open `LogReader`
    /// along with the error, same as any other `?`), but `offset` is
    /// everything `Session::attach` needs to pick the walk back up: a fresh
    /// `attach(offset)` resumes exactly where this one left off, replaying
    /// nothing twice, losing nothing before it (issue #1 finding 6). A
    /// caller that already tracks how much it has written could derive this
    /// itself from prior rounds, but only for a round that isn't the first;
    /// carrying it here means that isn't a special case.
    #[error("reading a replay range at offset {offset}: {source}")]
    Read {
        /// Where this round was about to read from.
        offset: u64,
        /// The underlying I/O failure.
        source: std::io::Error,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::{LogLimits, OutputLog};
    use std::path::PathBuf;

    fn scratch_fanout(dir: &std::path::Path) -> Arc<Mutex<Fanout>> {
        let log = OutputLog::open(dir, LogLimits::default(), None).expect("open log");
        Arc::new(Mutex::new(Fanout::new(log)))
    }

    fn scratch_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "anchor-session-unit-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    /// Builds a `Replay` straight onto a scratch `Fanout`, skipping the PTY
    /// that `Session::attach` would need. The two D1 fixtures below are about
    /// the catch-up loop's arithmetic, and that is entirely decided by the
    /// log and the subscriber list -- driving it with a real child would add
    /// timing to a question that has none.
    fn scratch_replay(fanout: &Arc<Mutex<Fanout>>) -> Replay {
        let guard = fanout.lock();
        let next_offset = guard.log.next_offset();
        let reader = guard.log.reader().expect("reader");
        drop(guard);
        Replay {
            replay_from: 0,
            next_offset,
            log_capped_at: None,
            fanout: Arc::clone(fanout),
            reader,
            cursor: 0,
            previous_gap: u64::MAX,
            stalled_rounds: 0,
            total_rounds: 0,
        }
    }

    fn publish_mib(fanout: &Arc<Mutex<Fanout>>, mib: usize) {
        let block = vec![b'x'; 64 * 1024];
        for _ in 0..(mib * 16) {
            fanout.lock().publish(&block);
        }
    }

    /// **D1, structurally.** No subscriber exists while history is still
    /// being served, so the 8 MiB queue bound cannot be spent on replay --
    /// which is the whole failure
    /// (docs/04-protocol.md). Needs
    /// private `Fanout::subscribers`, so it lives here.
    #[tokio::test]
    async fn no_subscriber_exists_until_the_remaining_gap_fits() {
        let dir = scratch_dir("catchup");
        let fanout = scratch_fanout(&dir);
        publish_mib(&fanout, 3);

        let replay = scratch_replay(&fanout);
        let next_offset = replay.next_offset;

        let assert_no_subscriber = |rounds: u64| {
            assert!(
                fanout.lock().subscribers.is_empty(),
                "round {rounds}: a subscriber must not be registered while history is still owed"
            );
        };

        let mut rounds = 0u64;
        assert_no_subscriber(rounds);
        let mut step = replay.next_round().expect("first catch-up round");
        let attach = loop {
            match step {
                ReplayStep::History {
                    offset,
                    bytes,
                    replay: rest,
                } => {
                    assert_eq!(
                        offset,
                        rounds * REPLAY_ROUND_BYTES,
                        "rounds must be contiguous"
                    );
                    assert_eq!(bytes.len() as u64, REPLAY_ROUND_BYTES, "a round is bounded");
                    rounds += 1;
                    assert_no_subscriber(rounds);
                    step = rest.written(bytes).expect("catch-up round");
                }
                ReplayStep::Live(attach) => break attach,
            }
        };

        assert_eq!(
            rounds, 2,
            "3 MiB is two 1 MiB rounds, then a 1 MiB gap that fits"
        );
        assert_eq!(
            fanout.lock().subscribers.len(),
            1,
            "the last round registers"
        );
        assert!(attach.caught_up);
        assert_eq!(attach.replay_from, 2 * REPLAY_ROUND_BYTES);
        assert_eq!(attach.replay.len() as u64, LIVE_GAP_BYTES);
        assert_eq!(
            attach.replay_to(),
            next_offset,
            "replay must meet the live boundary exactly"
        );

        drop(attach);
        assert!(
            fanout.lock().subscribers.is_empty(),
            "dropping the attach unregisters"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The other half of D1: convergence is not guaranteed, so the loop must
    /// have a floor. A producer the client never outruns makes the gap grow
    /// every round; after `MAX_STALLED_ROUNDS` the daemon stops trying,
    /// clamps replay to the freshest `LIVE_GAP_BYTES`, and reports the hole
    /// rather than spinning or handing over a queue that is certain to
    /// overflow.
    #[tokio::test]
    async fn a_catch_up_that_never_converges_gives_up_and_reports_the_hole() {
        let dir = scratch_dir("stall");
        let fanout = scratch_fanout(&dir);
        publish_mib(&fanout, 3);

        let replay = scratch_replay(&fanout);
        let mut rounds = 0u64;
        assert!(rounds < 16, "the catch-up loop did not terminate");
        let mut step = replay.next_round().expect("first catch-up round");
        let attach = loop {
            match step {
                ReplayStep::History {
                    bytes,
                    replay: rest,
                    ..
                } => {
                    rounds += 1;
                    assert_eq!(bytes.len() as u64, REPLAY_ROUND_BYTES);
                    // The producer gains 2 MiB for every 1 MiB served: this
                    // client is losing ground, every round, on purpose.
                    publish_mib(&fanout, 2);
                    assert!(rounds < 16, "the catch-up loop did not terminate");
                    step = rest.written(bytes).expect("catch-up round");
                }
                ReplayStep::Live(attach) => break attach,
            }
        };

        assert_eq!(
            rounds,
            u64::from(MAX_STALLED_ROUNDS),
            "one round to set the baseline, then four stalls"
        );
        assert!(!attach.caught_up, "a clamped replay must say so");
        let served = rounds * REPLAY_ROUND_BYTES;
        assert!(
            attach.replay_from > served,
            "the clamp must leave a hole, not silently rewind"
        );
        assert_eq!(
            attach.replay.len() as u64,
            LIVE_GAP_BYTES,
            "the client gets the freshest MiB"
        );
        assert_eq!(
            attach.replay_to(),
            attach.next_offset,
            "and it still meets the live boundary exactly -- the hole is behind it, not in front"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Issue #1 finding 5: `HistoryReplay::written` is the only way to reach
    /// the next `ReplayStep`, and it takes this round's bytes as an
    /// argument rather than being reachable off bare `self` -- so a caller
    /// that tries to advance without holding the right bytes (a pre-fetch,
    /// or a placeholder) is caught here instead of silently reintroducing
    /// the queue-overflow race D1 closed.
    #[test]
    #[should_panic(expected = "HistoryReplay::written must be called with this round's own bytes")]
    fn written_rejects_bytes_that_are_not_this_round_s_own() {
        let dir = scratch_dir("written-guard");
        let fanout = scratch_fanout(&dir);
        publish_mib(&fanout, 3);

        let replay = scratch_replay(&fanout);
        let ReplayStep::History { replay, .. } = replay.next_round().expect("first catch-up round")
        else {
            panic!("3 MiB backlog must not go live on the first round");
        };

        let _ = replay.written(vec![0u8; 1]); // not this round's REPLAY_ROUND_BYTES-sized stretch
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `MAX_STALLED_ROUNDS` only catches a client that stops gaining ground
    /// *entirely*; one that gains a few bytes every round resets
    /// `stalled_rounds` to 0 forever and would re-acquire the fan-out mutex
    /// indefinitely without a second floor.
    /// Driving that through real rounds would mean actually shrinking a
    /// multi-gigabyte gap one byte at a time, so this exercises the pure
    /// decision `next_round` defers to instead -- deterministic and fast,
    /// same as the arithmetic it's testing.
    #[test]
    fn total_rounds_forces_registration_even_when_the_gap_keeps_shrinking() {
        let gap = LIVE_GAP_BYTES + 1; // never small enough to register on its own
        assert!(
            !should_register(gap, 0, MAX_CATCHUP_ROUNDS - 1),
            "must not register early just because rounds are piling up"
        );
        assert!(
            should_register(gap, 0, MAX_CATCHUP_ROUNDS),
            "a client that never stalls must still be bounded by total rounds"
        );
    }

    /// Issue #1 finding 4: `Open` (the initial `fanout.log.reader()` call)
    /// and `Read` (any `read_range` during catch-up) used to be one `Io`
    /// variant with a message hardcoded to the open case, so a read failure
    /// several rounds into a large backlog read as though the daemon had
    /// never managed to open the log at all. Guards against the two
    /// collapsing back into one variant, or trading messages.
    #[test]
    fn open_and_read_failures_report_distinct_messages() {
        let boom = || std::io::Error::other("boom");
        let open = AttachError::Open(boom()).to_string();
        let read = AttachError::Read {
            offset: 4096,
            source: boom(),
        }
        .to_string();
        assert!(
            open.starts_with("opening the log for replay"),
            "got: {open}"
        );
        assert!(read.starts_with("reading a replay range"), "got: {read}");
        assert_ne!(open, read);
    }

    /// Issue #1 finding 6: a `read_range` failure mid-catch-up used to drop
    /// the whole `Replay` -- cursor, stall counters and open `LogReader`
    /// alike -- along with the error, leaving no way to resume through the
    /// session API except the caller reconstructing the offset from its own
    /// bookkeeping (impossible on the very first round, which has no prior
    /// successful round to derive it from). `AttachError::Read` now carries
    /// that offset itself: `Session::attach(offset)` picks the walk back up
    /// exactly there, so recovery doesn't depend on the caller having kept
    /// count. Both `next_round` call sites (the per-round history read and
    /// the final post-registration read) feed their own resume point in --
    /// `self.cursor` and `replay_from` respectively -- this pins the
    /// message shape so a future edit can't drop the offset again.
    #[test]
    fn read_failure_carries_the_offset_to_resume_at() {
        let err = AttachError::Read {
            offset: 2 * 1024 * 1024,
            source: std::io::Error::other("boom"),
        };
        assert_eq!(
            err.to_string(),
            "reading a replay range at offset 2097152: boom",
            "the resume offset must be in the message, not just the source error"
        );
    }
}
