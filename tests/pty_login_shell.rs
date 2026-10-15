//! Regression test for `SpawnSpec::login_shell` -- the flag that runs a
//! preset's command through the user's login shell instead of exec'ing it
//! directly (docs/03-pty.md#spawn). anchord's premise is running as
//! a background service, and launchd/systemd hand it none of the environment
//! a login shell builds: `PATH` beyond the system default, `EDITOR`, `LANG`,
//! language-manager shims. The `shell` preset never felt this (`$SHELL -l`
//! rebuilds its own environment on the way in); `claude`/`codex` exec a
//! named binary with no shell in between and got only what the service
//! manager handed the daemon.
//!
//! Two things are asserted, and the second is the one that matters for
//! docs/06-security.md: the wrapper passes the program and its arguments as
//! *separate argv entries* bound to `$0`/`"$@"`, never as a concatenated
//! shell string. An argument containing a space must not split, and one
//! containing a glob must not expand.
//!
//! All cases live in one #[test] fn, run strictly sequentially, because they
//! mutate the process-wide `SHELL` var -- cargo runs the #[test] fns *within*
//! one file's binary in parallel, and this file has exactly one, so there is
//! nothing else in this process for that mutation to race against. Same
//! tradeoff `pty_term_default.rs` and `presets.rs` already accepted rather
//! than pulling in a serialization dependency.

#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver};
use std::time::{Duration, Instant};

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

fn scratch_dir() -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "anchor-login-shell-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock before the unix epoch")
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).expect("create scratch dir");
    dir
}

/// A stand-in `$SHELL` that reports the exact argv it was handed and exits.
/// Recording argv is the only way to assert the wrapper's shape without
/// depending on whatever the real login shell's profile happens to contain
/// on the machine running the test.
fn write_recorder_shell(dir: &std::path::Path) -> PathBuf {
    let path = dir.join("recorder-shell");
    std::fs::write(
        &path,
        "#!/bin/sh\nprintf 'ARGV:'\nfor a in \"$@\"; do printf '[%s]' \"$a\"; done\necho DONE\n",
    )
    .expect("write recorder shell");
    let mut perms = std::fs::metadata(&path)
        .expect("stat recorder shell")
        .permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&path, perms).expect("chmod recorder shell");
    path
}

fn spawn(
    program: &str,
    args: &[String],
    cwd: &std::path::Path,
    login_shell: bool,
) -> (pty::SpawnedSession, Receiver<Vec<u8>>) {
    let (out_tx, out_rx) = mpsc::channel::<Vec<u8>>();
    let spec = SpawnSpec {
        program,
        args,
        cwd,
        env: &[],
        cols: 80,
        rows: 24,
        login_shell,
    };
    let spawned = pty::spawn(&spec, move |chunk| {
        let _ = out_tx.send(chunk.to_vec());
    })
    .expect("spawn");
    (spawned, out_rx)
}

fn recv_until(
    rx: &Receiver<Vec<u8>>,
    timeout: Duration,
    mut pred: impl FnMut(&[u8]) -> bool,
) -> String {
    let deadline = Instant::now() + timeout;
    let mut acc = Vec::new();
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "timed out waiting for predicate; got {} bytes: {:?}",
            acc.len(),
            String::from_utf8_lossy(&acc)
        );
        match rx.recv_timeout(remaining) {
            Ok(chunk) => {
                acc.extend_from_slice(&chunk);
                if pred(&acc) {
                    return String::from_utf8_lossy(&acc).into_owned();
                }
            }
            Err(e) => panic!(
                "output channel closed before predicate matched ({e}); got {} bytes: {:?}",
                acc.len(),
                String::from_utf8_lossy(&acc)
            ),
        }
    }
}

fn contains(acc: &[u8], needle: &str) -> bool {
    acc.windows(needle.len()).any(|w| w == needle.as_bytes())
}

#[test]
fn login_shell_wraps_the_command_without_ever_building_a_shell_string() {
    let orig_shell = std::env::var("SHELL").ok();
    let dir = scratch_dir();
    let recorder = write_recorder_shell(&dir);

    // An argument with a space and one that is a bare glob: both are exactly
    // what a concatenated shell string would mangle, and the scratch dir has
    // a file in it for `*` to expand to if the quoting were wrong.
    let args = vec!["a b".to_string(), "*".to_string()];

    // Case 1: the wrapper's literal shape. `sh -c script name args...` binds
    // `name` to `$0` and `args...` to `$@`, so `exec "$0" "$@"` is the whole
    // of the shell source -- no request data in it.
    set_env("SHELL", recorder.to_str().expect("utf-8 recorder path"));
    let (_spawned, rx) = spawn("/bin/echo", &args, &dir, true);
    let got = recv_until(&rx, DEFAULT_TIMEOUT, |acc| contains(acc, "DONE"));
    assert!(
        got.contains(r#"ARGV:[-l][-c][exec "$0" "$@"][/bin/echo][a b][*]"#),
        "login_shell must exec $SHELL -l -c 'exec \"$0\" \"$@\"' <program> <args...>, got: {got}"
    );

    // Case 2: end to end through a real shell -- the arguments must arrive
    // at the program unsplit and unexpanded.
    set_env("SHELL", "/bin/sh");
    let (_spawned, rx) = spawn("/bin/echo", &args, &dir, true);
    let got = recv_until(&rx, DEFAULT_TIMEOUT, |acc| contains(acc, "*"));
    assert!(
        got.contains("a b *"),
        "arguments must survive the wrapper unsplit and unglobbed, got: {got}"
    );
    assert!(
        !got.contains("recorder-shell"),
        "a bare `*` argument must not have globbed against cwd, got: {got}"
    );

    // Case 3: off by default -- no shell is involved at all, even with a
    // $SHELL set. This is what every existing preset and every raw-`command`
    // session keeps doing.
    set_env("SHELL", recorder.to_str().expect("utf-8 recorder path"));
    let (_spawned, rx) = spawn("/bin/echo", &args, &dir, false);
    let got = recv_until(&rx, DEFAULT_TIMEOUT, |acc| contains(acc, "*"));
    assert!(
        got.contains("a b *"),
        "without login_shell the program is exec'd directly, got: {got}"
    );
    assert!(
        !got.contains("ARGV:"),
        "without login_shell $SHELL must never be invoked, got: {got}"
    );

    // Case 4: an *empty* `SHELL` is unset, not a shell. Some service
    // managers and container images export it that way, and taking it
    // literally execs the empty program -- a failure that names `""`
    // instead of naming the shell, at the least useful moment.
    set_env("SHELL", "");
    assert_eq!(
        anchor::pty::login_shell_path(),
        "/bin/sh",
        "an empty $SHELL must fall back, not become the program"
    );
    let (_spawned, rx) = spawn("/bin/echo", &args, &dir, true);
    let got = recv_until(&rx, DEFAULT_TIMEOUT, |acc| contains(acc, "*"));
    assert!(
        got.contains("a b *"),
        "an empty $SHELL must still run the program, got: {got}"
    );

    // Whitespace is the same thing wearing a hat.
    set_env("SHELL", "   ");
    assert_eq!(
        anchor::pty::login_shell_path(),
        "/bin/sh",
        "a blank $SHELL must fall back too"
    );

    match orig_shell {
        Some(v) => set_env("SHELL", &v),
        None => remove_env("SHELL"),
    }
    let _ = std::fs::remove_dir_all(&dir);
}
