//! Regression test for the TERM/COLORTERM default `pty::spawn` fills in when
//! the daemon's own environment has no TERM to inherit
//! (docs/03-pty.md#spawn-details-that-bite). A background service (launchd/systemd) has
//! no controlling terminal and so no TERM at all -- without a default, the
//! child is treated as a "dumb" terminal by color-detection libraries and
//! silently renders in monochrome. Same PTY bytes to every client, so every
//! viewer sees it the same broken way; this is not a browser/client
//! rendering gap (found live: a work-Mac the daemon installed as a service
//! produced a colorless Claude Code session, identically on desktop Chrome
//! and iPhone Safari, while a daemon started by hand from an interactive
//! shell -- TERM inherited from that shell -- rendered the same tool in
//! full color).
//!
//! All three cases live in one #[test] fn, run strictly sequentially,
//! because they mutate the process-wide TERM/COLORTERM env vars -- cargo
//! runs the #[test] fns *within* one file's binary in parallel by default,
//! and this file has exactly one, so there's nothing else in this process
//! for that mutation to race against, and a serialization dependency is not
//! worth pulling in for it.

#![cfg(unix)]

use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver};
use std::time::Duration;

use anchor::pty::{self, SpawnSpec};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

/// Safe here only because this file's single #[test] fn is the sole thing
/// running in this test binary's process -- see the module doc.
fn set_env(key: &str, value: &str) {
    // SAFETY: no other test in this binary runs concurrently with this one.
    unsafe { std::env::set_var(key, value) };
}

/// Safe here only because this file's single #[test] fn is the sole thing
/// running in this test binary's process -- see the module doc.
fn remove_env(key: &str) {
    // SAFETY: no other test in this binary runs concurrently with this one.
    unsafe { std::env::remove_var(key) };
}

fn temp_dir() -> PathBuf {
    std::env::temp_dir()
}

/// Spawns `/bin/sh -c script` with the given explicit env overrides
/// (`spec.env` -- distinct from the ambient process env this test mutates)
/// and returns its output channel, same wiring as `pty_primitive.rs`'s own
/// `spawn_sh`.
fn spawn_sh(script: &str, env: &[(String, String)]) -> (pty::SpawnedSession, Receiver<Vec<u8>>) {
    let (out_tx, out_rx) = mpsc::channel::<Vec<u8>>();
    let cwd = temp_dir();
    let spec = SpawnSpec {
        program: "/bin/sh",
        args: &["-c".to_string(), script.to_string()],
        cwd: &cwd,
        env,
        cols: 80,
        rows: 24,
        login_shell: false,
    };
    let spawned = pty::spawn(&spec, move |chunk| {
        let _ = out_tx.send(chunk.to_vec());
    })
    .expect("spawn /bin/sh");
    (spawned, out_rx)
}

/// Collects output until `pred` holds, failing only if the child goes
/// `stall` without producing anything. A total-elapsed deadline would
/// measure how fast this machine runs a cold `/bin/sh` (a login shell reads
/// the user's profile first), not whether the output ever arrives.
fn recv_until(
    rx: &Receiver<Vec<u8>>,
    stall: Duration,
    mut pred: impl FnMut(&[u8]) -> bool,
) -> Vec<u8> {
    let mut acc = Vec::new();
    loop {
        match rx.recv_timeout(stall) {
            Ok(chunk) => {
                acc.extend_from_slice(&chunk);
                if pred(&acc) {
                    return acc;
                }
            }
            Err(e) => panic!(
                "no output for {stall:?} before the predicate matched ({e}); got {} bytes: {:?}",
                acc.len(),
                String::from_utf8_lossy(&acc)
            ),
        }
    }
}

fn contains(acc: &[u8], needle: &str) -> bool {
    acc.windows(needle.len()).any(|w| w == needle.as_bytes())
}

/// Reports `$TERM`/`$COLORTERM` as the child shell sees them (unset
/// interpolates to empty, not the literal string "TERM=" -- `sh` always
/// expands `$X` to nothing for an unset `X`), with a sentinel so
/// `recv_until` has something to wait for.
const REPORT_SCRIPT: &str = "printf 'TERM=[%s] COLORTERM=[%s]' \"$TERM\" \"$COLORTERM\"; echo DONE";

#[test]
fn term_default_fills_in_only_when_the_daemon_itself_has_none() {
    let orig_term = std::env::var("TERM").ok();
    let orig_colorterm = std::env::var("COLORTERM").ok();

    // Case 1: daemon's own environment has no TERM at all (the launchd/
    // systemd case) -- pty::spawn must fill in a real default for both.
    remove_env("TERM");
    remove_env("COLORTERM");
    let (_spawned, out_rx) = spawn_sh(REPORT_SCRIPT, &[]);
    let got = recv_until(&out_rx, DEFAULT_TIMEOUT, |acc| contains(acc, "DONE"));
    let got = String::from_utf8_lossy(&got);
    assert!(
        got.contains("TERM=[xterm-256color]"),
        "no ambient TERM must default to xterm-256color, got: {got}"
    );
    assert!(
        got.contains("COLORTERM=[truecolor]"),
        "no ambient TERM must also default COLORTERM to truecolor, got: {got}"
    );

    // Case 2: daemon's own environment already has a real TERM (the
    // interactive-shell/tmux case) -- must be inherited untouched, and
    // COLORTERM must NOT be invented alongside it.
    set_env("TERM", "screen-256color");
    remove_env("COLORTERM");
    let (_spawned, out_rx) = spawn_sh(REPORT_SCRIPT, &[]);
    let got = recv_until(&out_rx, DEFAULT_TIMEOUT, |acc| contains(acc, "DONE"));
    let got = String::from_utf8_lossy(&got);
    assert!(
        got.contains("TERM=[screen-256color]"),
        "an already-set ambient TERM must be inherited as-is, got: {got}"
    );
    assert!(
        got.contains("COLORTERM=[]"),
        "COLORTERM must not be invented when the ambient TERM was already set, got: {got}"
    );

    // Case 3: an explicit per-session override (SpawnSpec::env, e.g. from a
    // future API field) must still win even with no ambient TERM -- the
    // default fill happens before spec.env is layered on, never after.
    remove_env("TERM");
    let (_spawned, out_rx) = spawn_sh(REPORT_SCRIPT, &[("TERM".to_string(), "vt100".to_string())]);
    let got = recv_until(&out_rx, DEFAULT_TIMEOUT, |acc| contains(acc, "DONE"));
    let got = String::from_utf8_lossy(&got);
    assert!(
        got.contains("TERM=[vt100]"),
        "an explicit spec.env override must win over the built-in default, got: {got}"
    );

    match orig_term {
        Some(v) => set_env("TERM", &v),
        None => remove_env("TERM"),
    }
    match orig_colorterm {
        Some(v) => set_env("COLORTERM", &v),
        None => remove_env("COLORTERM"),
    }
}
