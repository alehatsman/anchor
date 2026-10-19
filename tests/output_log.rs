//! `log.rs` on its own: append accounting, the size cap, clamped range
//! reads, and restart recovery.
//!
//! The part of the offset model that needs no PTY -- deliberately not
//! `cfg(unix)`-gated, so it stays checkable on any platform anchord is ever
//! built for, while the fixtures that spawn `/bin/sh` (`session_replay.rs`)
//! are Unix-only. There is no CI matrix today; this is a property of the
//! file, not a claim that something runs it elsewhere.

use std::path::PathBuf;

use anchor::log::{LogEvent, LogLimits, OutputLog, LOG_FILE_NAME};

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
/// entire replay index (docs/02-model.md#storage). Asserted after
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
    let mut log = OutputLog::open(&dir, LogLimits::default()).expect("open");

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
/// streaming can continue (docs/02-model.md#the-size-cap).
#[test]
fn hitting_the_cap_stops_the_file_but_not_the_offset() {
    let dir = scratch("cap");
    let mut log = OutputLog::open(&dir, limits(50, 30)).expect("open");

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

/// A file that already holds `max_bytes` opens capped: whatever stopped it
/// growing, appending more must not push it past the budget. (The rest of
/// restart recovery -- a stream ahead of its file -- is
/// `Store::reconcile_output_bytes`'s, tested in `store_roundtrip.rs`.)
#[test]
fn a_full_file_opens_capped_and_does_not_grow() {
    let dir = scratch("full-open");
    {
        let mut log = OutputLog::open(&dir, limits(40, 1024)).expect("open");
        log.append(&[b'y'; 40]);
    }

    let mut log = OutputLog::open(&dir, limits(40, 1024)).expect("reopen");
    assert_eq!(log.next_offset(), 40);
    assert_eq!(log.log_capped_at(), Some(40));
    let appended = log.append(b"more output");
    assert_eq!(appended.start, 40);
    assert_eq!(log.next_offset(), 51);
    assert_eq!(log.readable_end(), 40, "the file must not grow after a cap");
    assert_invariant(&log, &dir);

    let _ = std::fs::remove_dir_all(&dir);
}

/// The log is a verbatim terminal transcript; its protection must not rest on
/// the parent directory alone.
#[cfg(unix)]
#[test]
fn the_log_file_is_owner_only() {
    use std::os::unix::fs::PermissionsExt;

    let dir = scratch("mode");
    let _log = OutputLog::open(&dir, LogLimits::default()).expect("open");
    let mode = std::fs::metadata(dir.join(LOG_FILE_NAME))
        .expect("stat")
        .permissions()
        .mode();
    assert_eq!(mode & 0o777, 0o600, "output.vt mode was {:o}", mode & 0o777);

    let _ = std::fs::remove_dir_all(&dir);
}

/// A write that lands k bytes and then fails -- a full disk -- must cap the
/// log at the file's *real* length. Capping at the pre-write length leaves
/// the file k bytes longer than the log believes, so `file_length ==
/// min(next_offset, log_capped_at)` is false on exactly the failure the cap
/// exists for. Induced with `RLIMIT_FSIZE`: the kernel writes up to the limit
/// and then fails the rest, which is a short write followed by an error.
///
/// Every other test in this binary writes well under the limit, so it is safe
/// to set process-wide.
#[cfg(unix)]
#[test]
fn a_short_write_caps_at_the_real_file_length() {
    const LIMIT: u64 = 1024 * 1024;

    let dir = scratch("short-write");
    let mut log = OutputLog::open(&dir, LogLimits::default()).expect("open");
    log.append(b"before the disk fills");

    // The kernel signals SIGXFSZ past the limit; ignored, the write fails
    // with EFBIG instead of killing the test.
    // SAFETY: plain libc calls with valid arguments.
    unsafe {
        libc::signal(libc::SIGXFSZ, libc::SIG_IGN);
        let limit = libc::rlimit {
            rlim_cur: LIMIT as libc::rlim_t,
            rlim_max: libc::RLIM_INFINITY,
        };
        assert_eq!(libc::setrlimit(libc::RLIMIT_FSIZE, &limit), 0);
    }

    let appended = log.append(&vec![b'z'; 2 * LIMIT as usize]);

    let on_disk = std::fs::metadata(dir.join(LOG_FILE_NAME)).unwrap().len();
    assert_eq!(on_disk, LIMIT, "the sabotage must produce a short write");
    assert!(
        matches!(appended.events.as_slice(), [LogEvent::IoError { .. }]),
        "expected one IoError, got {:?}",
        appended.events
    );
    assert_eq!(
        log.log_capped_at(),
        Some(on_disk),
        "the cap must be where the file really ends"
    );
    assert_eq!(log.readable_end(), on_disk);
    assert_invariant(&log, &dir);

    let _ = std::fs::remove_dir_all(&dir);
}
