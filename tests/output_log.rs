//! `log.rs` on its own: append accounting, the size cap, clamped range
//! reads, and restart recovery.
//!
//! The subset of docs/10-testing.md#2-sessionoffset-unit-tests that needs no
//! PTY -- deliberately not `cfg(unix)`-gated, so the offset model is checked
//! on every platform in the CI matrix even while the fixtures that spawn
//! `/bin/sh` (`session_replay.rs`) are Unix-only.

use std::path::PathBuf;

use anchor::log::{LogEvent, LogLimits, OutputLog, StoredState, LOG_FILE_NAME};

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "anchord-log-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock before 1970")
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn limits(max_bytes: u64, warn_bytes: u64) -> LogLimits {
    LogLimits {
        max_bytes,
        warn_bytes,
        ..LogLimits::default()
    }
}

/// `file_length == min(next_offset, log_capped_at)`, the identity that is the
/// entire replay index (docs/02-model.md#output-log). Asserted after
/// every operation, not just at the end.
fn assert_invariant(log: &OutputLog, dir: &std::path::Path) {
    let on_disk = std::fs::metadata(dir.join(LOG_FILE_NAME))
        .expect("stat log")
        .len();
    let expected = log
        .log_capped_at()
        .map_or(log.next_offset(), |cap| log.next_offset().min(cap));
    assert_eq!(
        on_disk,
        expected,
        "file_length must equal min(next_offset {}, log_capped_at {:?})",
        log.next_offset(),
        log.log_capped_at()
    );
    assert_eq!(
        on_disk,
        log.readable_end(),
        "readable_end must be the file length"
    );
}

/// An uncapped log persists everything and hands back the offset each chunk
/// landed at; a range read returns exactly the bytes the live stream carried
/// for that range.
#[test]
fn append_offsets_and_range_reads_agree() {
    let dir = scratch("roundtrip");
    let mut log = OutputLog::open(&dir, LogLimits::default(), None).expect("open");

    let chunks: [&[u8]; 3] = [b"hello ", b"terminal ", b"world"];
    let mut starts = Vec::new();
    for chunk in chunks {
        let appended = log.append(chunk);
        starts.push(appended.start);
        assert!(
            appended.events.is_empty(),
            "no cap, no warning, no error expected"
        );
        assert_invariant(&log, &dir);
    }
    assert_eq!(starts, vec![0, 6, 15]);
    assert_eq!(log.next_offset(), 20);
    assert_eq!(log.log_capped_at(), None);

    let all: Vec<u8> = chunks.concat();
    let mut reader = log.reader().expect("reader");

    // Whole log, one chunk, and a range straddling two chunk boundaries --
    // replay is a byte range, and must not care where chunks fell.
    assert_eq!(reader.read_range(0, 20).unwrap(), all);
    assert_eq!(reader.read_range(6, 15).unwrap(), b"terminal ".to_vec());
    assert_eq!(reader.read_range(3, 17).unwrap(), all[3..17].to_vec());
    assert_eq!(reader.read_range(20, 20).unwrap(), Vec::<u8>::new());
    assert_eq!(
        reader.read_range(9, 4).unwrap(),
        Vec::<u8>::new(),
        "an inverted range is empty, not a panic"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// The cap: the file stops growing at `log_max_bytes`, `log_capped_at` is set
/// to exactly that offset, and `next_offset` keeps advancing so live
/// streaming can continue (docs/02-model.md#size-cap).
#[test]
fn hitting_the_cap_stops_the_file_but_not_the_offset() {
    let dir = scratch("cap");
    let mut log = OutputLog::open(&dir, limits(50, 30), None).expect("open");

    let mut events = Vec::new();
    for _ in 0..10 {
        events.extend(log.append(&[b'y'; 20]).events);
        assert_invariant(&log, &dir);
    }

    assert_eq!(
        log.log_capped_at(),
        Some(50),
        "cap lands on log_max_bytes exactly"
    );
    assert_eq!(
        log.readable_end(),
        50,
        "the file stopped growing at the cap"
    );
    assert_eq!(
        log.next_offset(),
        200,
        "offsets keep advancing past the cap"
    );

    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, LogEvent::Capped { at: 50 }))
            .count(),
        1,
        "Capped is reported once, at the cap offset: {events:?}"
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, LogEvent::Warned { .. }))
            .count(),
        1,
        "log_warn_bytes fires once, not per chunk: {events:?}"
    );

    // The last persisted chunk was truncated to fill the budget exactly, so
    // the tail on disk is still real output, not padding.
    let mut reader = log.reader().expect("reader");
    assert_eq!(reader.read_range(40, 50).unwrap(), vec![b'y'; 10]);
    assert_eq!(
        reader.read_range(40, 200).unwrap(),
        vec![b'y'; 10],
        "a read past the cap returns only what is on disk"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// The M3 gate's restart clause. A capped log's file stopped growing while
/// the column kept counting; taking the file length unconditionally on
/// recovery would rewind `next_offset` below offsets live clients already
/// hold (docs/02-model.md#restart-recovery).
#[test]
fn reopening_a_capped_log_does_not_rewind_next_offset() {
    let dir = scratch("no-rewind");
    let stored = {
        let mut log = OutputLog::open(&dir, limits(50, 1024), None).expect("open");
        for _ in 0..10 {
            log.append(&[b'y'; 20]);
        }
        assert_eq!((log.next_offset(), log.readable_end()), (200, 50));
        StoredState {
            output_bytes: log.next_offset(),
            log_capped_at: log.log_capped_at(),
        }
    };

    let reopened = OutputLog::open(&dir, limits(50, 1024), Some(stored)).expect("reopen");
    assert_eq!(
        reopened.next_offset(),
        200,
        "next_offset must not move backwards across a restart"
    );
    assert_eq!(reopened.log_capped_at(), Some(50));
    assert_invariant(&reopened, &dir);

    let _ = std::fs::remove_dir_all(&dir);
}

/// The other half of `max()`: for a normal log the column lags a crash by
/// seconds and the file, which cannot lie about bytes it holds, wins.
#[test]
fn recovery_takes_the_larger_of_the_file_and_the_column() {
    let dir = scratch("max-rule");
    {
        let mut log = OutputLog::open(&dir, LogLimits::default(), None).expect("open");
        log.append(&[b'x'; 4096]);
    }

    let stale = StoredState {
        output_bytes: 1000,
        log_capped_at: None,
    };
    let recovered = OutputLog::open(&dir, LogLimits::default(), Some(stale)).expect("reopen");
    assert_eq!(
        recovered.next_offset(),
        4096,
        "the file wins when the column lags"
    );
    assert_eq!(
        recovered.log_capped_at(),
        None,
        "a full file is not a capped one"
    );
    assert_invariant(&recovered, &dir);

    let _ = std::fs::remove_dir_all(&dir);
}

/// Recovering with a column ahead of the file but no recorded cap: the bytes
/// between them are gone whatever the reason, so the log says so rather than
/// leaving a null cap that claims they are readable.
#[test]
fn a_column_ahead_of_the_file_is_reported_as_capped() {
    let dir = scratch("implied-cap");
    {
        let mut log = OutputLog::open(&dir, LogLimits::default(), None).expect("open");
        log.append(b"only these bytes survived");
    }

    let stored = StoredState {
        output_bytes: 9_000,
        log_capped_at: None,
    };
    let recovered = OutputLog::open(&dir, LogLimits::default(), Some(stored)).expect("reopen");
    assert_eq!(recovered.next_offset(), 9_000);
    assert_eq!(
        recovered.log_capped_at(),
        Some(25),
        "persistence demonstrably stopped at the file length"
    );
    assert_invariant(&recovered, &dir);

    let _ = std::fs::remove_dir_all(&dir);
}

/// Appending to a log that reopened already capped is a no-op on disk and a
/// normal advance for the offset -- a restart must not restart persistence
/// at a position whose offsets are already spent.
#[test]
fn a_reopened_capped_log_does_not_resume_appending() {
    let dir = scratch("capped-append");
    {
        let mut log = OutputLog::open(&dir, limits(40, 1024), None).expect("open");
        for _ in 0..4 {
            log.append(&[b'y'; 20]);
        }
    }

    let stored = StoredState {
        output_bytes: 80,
        log_capped_at: Some(40),
    };
    let mut log = OutputLog::open(&dir, limits(40, 1024), Some(stored)).expect("reopen");
    let appended = log.append(b"more output");
    assert_eq!(appended.start, 80);
    assert_eq!(log.next_offset(), 91);
    assert_eq!(
        log.readable_end(),
        40,
        "the file must not grow again after a cap"
    );
    assert_invariant(&log, &dir);

    let _ = std::fs::remove_dir_all(&dir);
}
