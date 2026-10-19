//! `anchor service install|uninstall|status` — registering `anchord` with
//! the user's own service manager (docs/07-lifecycle.md).
//!
//! The point of this module is the invariant in that document: **no client
//! ever spawns anchord**. A daemon started as a child of a client has the
//! client's lifetime, which is exactly the property anchor exists to break.
//! Something else has to start it, and on a single-user machine that is the
//! OS's per-user service manager.
//!
//! Login-scoped and root-free on both platforms: a launchd *user agent* on
//! macOS, a systemd *user unit* on Linux. Boot-scoped supervision
//! (`LaunchDaemons`, systemd lingering) needs elevation and starts sessions
//! for a user who is not logged in — a different decision than this one, and
//! deliberately not taken here.
//!
//! Neither unit pins a socket path. `anchord` resolves the same path a
//! client resolves, and a unit that baked one in would diverge from that
//! resolution the moment either side changed.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

/// What `install`/`uninstall` did, so the caller can report it without this
/// module printing.
#[derive(Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The registration was written (or rewritten) at this path.
    Installed(PathBuf),
    /// The registration at this path was removed.
    Uninstalled(PathBuf),
    /// There was nothing to remove. Not an error.
    NotInstalled,
}

/// The user's home directory, resolved the same way `paths.rs` resolves
/// `$HOME` — an empty `HOME` is treated as unset rather than as `/`.
fn home_dir() -> Result<PathBuf> {
    std::env::var("HOME")
        .ok()
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
        .context("HOME is not set; cannot find the user's service directory")
}

/// Warns when the binary being registered lives in a build directory.
///
/// `current_exe()` is whatever is running right now. For a `cargo build`
/// that is `target/debug/anchord`, which the next build or a `cargo clean`
/// can remove out from under an installed unit — with no error until the
/// next login, when the service manager fails to start a binary that is no
/// longer there.
fn warn_if_build_output(exe: &Path) {
    if is_build_output(exe) {
        eprintln!(
            "warning: {} looks like a `cargo build` output, not a stable install location — \
             a later build or `cargo clean` removes it and the service then fails to start \
             with no warning. Install anchord somewhere stable first if this is meant to \
             persist.",
            exe.display()
        );
    }
}

/// Whether a path points inside a cargo build directory.
///
/// Matches `target/debug` or `target/release` at any depth rather than at a
/// fixed one — a workspace, a `CARGO_TARGET_DIR`, and a plain crate all put
/// it somewhere different, and a check that only fires for one layout warns
/// nobody.
fn is_build_output(exe: &Path) -> bool {
    let parts: Vec<_> = exe
        .components()
        .map(std::path::Component::as_os_str)
        .collect();
    parts
        .windows(2)
        .any(|w| matches!(w, [a, b] if *a == "target" && (*b == "debug" || *b == "release")))
}

/// The binary to register: `anchord` next to whatever is running.
///
/// Resolved from `current_exe()` rather than searched for on `PATH`, so the
/// registration names the build the user actually invoked. Absolute, because
/// a relative path would resolve against the service manager's working
/// directory rather than the user's.
fn anchord_path() -> Result<PathBuf> {
    let exe = std::env::current_exe().context("resolving this binary's own path")?;
    let exe = exe.canonicalize().unwrap_or(exe);
    // `anchor` and `anchord` ship side by side; when `anchor` is what ran,
    // its sibling is the daemon. When `anchord` itself ran, this is a no-op.
    let candidate = exe.with_file_name(if cfg!(windows) {
        "anchord.exe"
    } else {
        "anchord"
    });
    if candidate.exists() {
        return Ok(candidate);
    }
    anyhow::bail!(
        "no anchord binary next to {} — install anchord alongside anchor, or run \
         `anchor service install` from the build that contains it",
        exe.display()
    )
}

#[cfg(target_os = "macos")]
mod platform {
    use std::fs;
    use std::path::PathBuf;
    use std::process::Command;

    use anyhow::{Context, Result};

    use super::{anchord_path, warn_if_build_output, Outcome};

    /// Reverse-DNS, as launchd expects, and the file name must match the
    /// label or `launchctl` complains on load.
    pub(super) const LABEL: &str = "dev.anchor.anchord";

    fn agents_dir() -> Result<PathBuf> {
        Ok(super::home_dir()?.join("Library/LaunchAgents"))
    }

    pub(super) fn plist_path() -> Result<PathBuf> {
        Ok(agents_dir()?.join(format!("{LABEL}.plist")))
    }

    /// Escapes a value for XML character data. A path can legally contain
    /// `&` or `<`, and an unescaped one makes the plist unparseable — which
    /// launchd reports as a load failure long after `install` said it
    /// succeeded.
    fn xml_escape(value: &str) -> String {
        value
            .replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
    }

    /// `launchctl` is best-effort: writing the plist is what persists, and
    /// loading it is what makes it take effect now. A machine where the
    /// bootstrap fails (already loaded, no GUI session) still has a correct
    /// registration for next login, so this reports rather than fails.
    fn bootstrap(path: &std::path::Path) -> bool {
        let uid = unsafe { libc::getuid() };
        Command::new("launchctl")
            .args(["bootstrap", &format!("gui/{uid}")])
            .arg(path)
            .output()
            .is_ok_and(|o| o.status.success())
    }

    fn bootout(path: &std::path::Path) {
        let uid = unsafe { libc::getuid() };
        drop(
            Command::new("launchctl")
                .args(["bootout", &format!("gui/{uid}")])
                .arg(path)
                .output(),
        );
    }

    pub(super) fn install() -> Result<Outcome> {
        let exe = anchord_path()?;
        warn_if_build_output(&exe);

        let dir = agents_dir()?;
        fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
        let path = plist_path()?;

        // KeepAlive restarts anchord if it exits. That restores the
        // *service*, never the sessions it was holding -- a dead anchord's
        // ptys are gone (docs/07-lifecycle.md).
        //
        // RunAtLoad starts it at login. There is deliberately no socket
        // argument: anchord resolves the same path a client resolves.
        let plist = render(&exe);

        // An unchanged registration is left strictly alone, because the
        // alternative is not harmless: `bootout` stops the running anchord,
        // and a dead anchord's ptys go with it (docs/07-lifecycle.md). An
        // install that is idempotent on paper but kills every live session
        // in practice is the opposite of what this command promises, and
        // sessions outliving their clients is the property anchor exists
        // for. Compare first, act only on a real difference.
        if fs::read_to_string(&path).is_ok_and(|current| current == plist) {
            return Ok(Outcome::Installed(path));
        }

        // The registration really is changing, so the restart is the point
        // rather than a side effect -- but it still costs the live sessions,
        // and the user is told so rather than finding out.
        if path.exists() {
            bootout(&path);
            eprintln!(
                "note: the registration changed, so anchord was restarted. Any sessions it \
                 was holding are gone (docs/07-lifecycle.md)."
            );
        }
        fs::write(&path, plist).with_context(|| format!("writing {}", path.display()))?;

        if !bootstrap(&path) {
            eprintln!(
                "note: wrote {} but `launchctl bootstrap` did not take — it will start at \
                 next login. Run `anchor service status` to check.",
                path.display()
            );
        }
        Ok(Outcome::Installed(path))
    }

    /// The agent's plist. Split out of `install` so the escaping is
    /// testable without touching the user's `LaunchAgents` directory.
    pub(super) fn render(exe: &std::path::Path) -> String {
        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{label}</string>
    <key>ProgramArguments</key>
    <array>
        <string>{exe}</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>ProcessType</key>
    <string>Interactive</string>
</dict>
</plist>
"#,
            label = LABEL,
            exe = xml_escape(&exe.to_string_lossy()),
        )
    }

    pub(super) fn uninstall() -> Result<Outcome> {
        let path = plist_path()?;
        if !path.exists() {
            return Ok(Outcome::NotInstalled);
        }
        bootout(&path);
        fs::remove_file(&path).with_context(|| format!("removing {}", path.display()))?;
        Ok(Outcome::Uninstalled(path))
    }

    pub(super) fn status() -> Result<String> {
        let path = plist_path()?;
        if !path.exists() {
            return Ok("not installed".into());
        }
        let uid = unsafe { libc::getuid() };
        let listed = Command::new("launchctl")
            .args(["print", &format!("gui/{uid}/{LABEL}")])
            .output();
        Ok(format!(
            "installed at {}\n{}",
            path.display(),
            launchd_state(listed)
        ))
    }

    /// What `launchctl print` can honestly support. Its exit status says the
    /// agent is registered, which a crash-looping agent also is; it does not
    /// say anchord is running, so neither does this.
    pub(super) fn launchd_state(listed: std::io::Result<std::process::Output>) -> String {
        match listed {
            Err(e) => format!("cannot tell: running launchctl failed: {e}"),
            Ok(o) if o.status.success() => {
                "registered with launchd (not checked: whether anchord is running)".into()
            }
            Ok(_) => "not registered — starts at next login, or run `anchor service install` again"
                .into(),
        }
    }
}

#[cfg(target_os = "linux")]
mod platform {
    use std::fs;
    use std::path::PathBuf;
    use std::process::Command;

    use anyhow::{Context, Result};

    use super::{anchord_path, warn_if_build_output, Outcome};

    pub(super) const UNIT: &str = "anchord.service";

    fn unit_dir() -> Result<PathBuf> {
        Ok(super::home_dir()?.join(".config/systemd/user"))
    }

    pub(super) fn unit_path() -> Result<PathBuf> {
        Ok(unit_dir()?.join(UNIT))
    }

    /// Quotes a value for a unit `Key=value` line. `systemd.service(5)`
    /// documents shell-style quoting here; without it a path containing a
    /// space splits `ExecStart` into several words and the unit fails to
    /// start, after `install` has already reported success. `%` is doubled
    /// because systemd expands `%` specifiers (`%h`, `%n`, ...) in
    /// `ExecStart`, which would mangle a path that contains one.
    fn quote(value: &str) -> String {
        format!(
            "\"{}\"",
            value
                .replace('\\', "\\\\")
                .replace('"', "\\\"")
                .replace('%', "%%")
        )
    }

    fn systemctl(args: &[&str]) -> bool {
        Command::new("systemctl")
            .arg("--user")
            .args(args)
            .output()
            .is_ok_and(|o| o.status.success())
    }

    pub(super) fn install() -> Result<Outcome> {
        let exe = anchord_path()?;
        warn_if_build_output(&exe);

        let dir = unit_dir()?;
        fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
        let path = unit_path()?;

        // Restart=always revives the *service*, never the sessions a dead
        // anchord was holding (docs/07-lifecycle.md).
        //
        // default.target, not a boot target: this is login-scoped. Enabling
        // lingering would start anchord for a user who is not logged in,
        // which is a decision for the user to make, not for `install`.
        //
        // No socket argument: anchord resolves the same path a client does.
        let unit = render(&exe);

        // Unchanged registration, nothing to do -- the same rule as macOS,
        // for a weaker reason: there is no `bootout` here to kill sessions
        // with, but running `enable --now` to achieve nothing still reports
        // work that did not happen.
        if fs::read_to_string(&path).is_ok_and(|current| current == unit) {
            return Ok(Outcome::Installed(path));
        }
        let existed = path.exists();

        fs::write(&path, unit).with_context(|| format!("writing {}", path.display()))?;

        // `daemon-reload` makes systemd re-read the unit file; it does not
        // restart anything, and `enable --now` on an already-active unit is
        // a no-op. So a *changed* `ExecStart` is on disk and enabled, but
        // the old anchord keeps running until it next stops -- normally at
        // logout. That is the opposite of macOS, where replacing the
        // registration restarts immediately.
        //
        // The divergence stands deliberately: converging it means restarting
        // anchord here, and that would kill every live session on Linux to
        // fix a cosmetic inconsistency. Say what happens instead.
        systemctl(&["daemon-reload"]);
        if !systemctl(&["enable", "--now", UNIT]) {
            eprintln!(
                "note: wrote {} but `systemctl --user enable --now` did not take — it will \
                 start at next login. Run `anchor service status` to check.",
                path.display()
            );
        } else if existed {
            eprintln!(
                "note: the registration changed. A running anchord keeps the old one until \
                 it next starts; restart it yourself when its sessions can afford it."
            );
        }
        Ok(Outcome::Installed(path))
    }

    /// The unit file. Split out of `install` so the quoting is testable
    /// without touching the user's systemd directory.
    pub(super) fn render(exe: &std::path::Path) -> String {
        format!(
            "[Unit]\n\
             Description=anchor session daemon\n\
             Documentation=https://github.com/alehatsman/anchor\n\
             \n\
             [Service]\n\
             Type=simple\n\
             ExecStart={exe}\n\
             Restart=always\n\
             RestartSec=1\n\
             \n\
             [Install]\n\
             WantedBy=default.target\n",
            exe = quote(&exe.to_string_lossy()),
        )
    }

    pub(super) fn uninstall() -> Result<Outcome> {
        let path = unit_path()?;
        if !path.exists() {
            return Ok(Outcome::NotInstalled);
        }
        systemctl(&["disable", "--now", UNIT]);
        fs::remove_file(&path).with_context(|| format!("removing {}", path.display()))?;
        systemctl(&["daemon-reload"]);
        Ok(Outcome::Uninstalled(path))
    }

    pub(super) fn status() -> Result<String> {
        let path = unit_path()?;
        if !path.exists() {
            return Ok("not installed".into());
        }
        let active = Command::new("systemctl")
            .args(["--user", "is-active", UNIT])
            .output();
        Ok(format!(
            "installed at {}\n{}",
            path.display(),
            systemd_state(active)
        ))
    }

    /// `is-active` prints the unit state (`inactive`, `failed`, ...) and a
    /// missing user bus prints its complaint to stderr; say whichever it was
    /// instead of collapsing every failure to "not running".
    pub(super) fn systemd_state(active: std::io::Result<std::process::Output>) -> String {
        match active {
            Err(e) => format!("cannot tell: running systemctl failed: {e}"),
            Ok(o) if o.status.success() => "running".into(),
            Ok(o) => {
                let said = String::from_utf8_lossy(if o.stdout.is_empty() {
                    o.stderr.as_slice()
                } else {
                    o.stdout.as_slice()
                });
                format!(
                    "not running ({}) — starts at next login, or run `anchor service install` again",
                    said.trim()
                )
            }
        }
    }
}

/// Platforms with no per-user service manager this maps onto. Saying so is
/// the point: an `install` that succeeded and registered nothing would be
/// discovered only when the sessions it was supposed to supervise were gone.
#[cfg(not(any(target_os = "macos", target_os = "linux")))]
mod platform {
    use anyhow::{bail, Result};

    use super::Outcome;

    fn unsupported<T>() -> Result<T> {
        bail!(
            "`anchor service` supports macOS (launchd user agent) and Linux (systemd user \
             unit). On this platform, start anchord from whatever supervises your user's \
             processes — see docs/07-lifecycle.md for what that has to guarantee."
        )
    }

    pub(super) fn install() -> Result<Outcome> {
        unsupported()
    }
    pub(super) fn uninstall() -> Result<Outcome> {
        unsupported()
    }
    pub(super) fn status() -> Result<String> {
        unsupported()
    }
}

/// Registers anchord with the user's service manager. Idempotent.
pub fn install() -> Result<Outcome> {
    platform::install()
}

/// Removes the registration. Not an error when nothing is installed.
pub fn uninstall() -> Result<Outcome> {
    platform::uninstall()
}

/// What is registered, and whether it is running.
pub fn status() -> Result<String> {
    platform::status()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A `%` in the path is a specifier to systemd, not a literal; it has to
    /// be doubled in `ExecStart`. Unverified against a real systemd here: the
    /// rule is `systemd.service(5)`'s, and this checks only that we apply it.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_percent_in_the_path_is_escaped_for_systemd() {
        let rendered = platform::render(&PathBuf::from("/opt/100%/anchord"));
        assert!(
            rendered.contains("ExecStart=\"/opt/100%%/anchord\""),
            "a bare % is expanded as a specifier: {rendered}"
        );
    }

    /// `status` must not claim more than `launchctl print` told it: a failure
    /// to run it is "cannot tell", and a zero exit is "registered", never
    /// "running".
    #[cfg(target_os = "macos")]
    #[test]
    fn launchd_status_says_only_what_it_knows() {
        use std::os::unix::process::ExitStatusExt;
        use std::process::{ExitStatus, Output};

        let out = |code| Output {
            status: ExitStatus::from_raw(code),
            stdout: Vec::new(),
            stderr: Vec::new(),
        };

        let cannot =
            platform::launchd_state(Err(std::io::Error::from(std::io::ErrorKind::NotFound)));
        assert!(cannot.starts_with("cannot tell"), "{cannot}");

        let registered = platform::launchd_state(Ok(out(0)));
        assert!(registered.starts_with("registered"), "{registered}");

        let missing = platform::launchd_state(Ok(out(1 << 8)));
        assert!(missing.starts_with("not registered"), "{missing}");
    }

    /// Same contract on Linux: a missing `systemctl` is not "not running",
    /// and the unit's own words (`failed`, a bus error) are passed on.
    #[cfg(target_os = "linux")]
    #[test]
    fn systemd_status_says_only_what_it_knows() {
        use std::os::unix::process::ExitStatusExt;
        use std::process::{ExitStatus, Output};

        let cannot =
            platform::systemd_state(Err(std::io::Error::from(std::io::ErrorKind::NotFound)));
        assert!(cannot.starts_with("cannot tell"), "{cannot}");

        let failed = platform::systemd_state(Ok(Output {
            status: ExitStatus::from_raw(3 << 8),
            stdout: b"failed\n".to_vec(),
            stderr: Vec::new(),
        }));
        assert!(failed.contains("(failed)"), "{failed}");

        let running = platform::systemd_state(Ok(Output {
            status: ExitStatus::from_raw(0),
            stdout: b"active\n".to_vec(),
            stderr: Vec::new(),
        }));
        assert_eq!(running, "running");
    }

    /// A path with a space must survive the unit format's quoting. Getting
    /// this wrong is silent: `install` reports success and the service
    /// fails to start later, because the path split into several words.
    #[test]
    fn a_path_with_a_space_stays_one_word() {
        let exe = PathBuf::from("/Users/a b/bin/anchord");
        let rendered = platform::render(&exe);
        #[cfg(target_os = "linux")]
        assert!(
            rendered.contains("ExecStart=\"/Users/a b/bin/anchord\""),
            "unquoted ExecStart splits on the space: {rendered}"
        );
        #[cfg(target_os = "macos")]
        assert!(
            rendered.contains("<string>/Users/a b/bin/anchord</string>"),
            "the path belongs in one <string>: {rendered}"
        );
    }

    /// XML metacharacters in a path must be escaped, or the plist does not
    /// parse and launchd rejects it well after `install` said it worked.
    #[cfg(target_os = "macos")]
    #[test]
    fn xml_metacharacters_in_a_path_are_escaped() {
        let exe = PathBuf::from("/Users/a&b/<x>/anchord");
        let rendered = platform::render(&exe);
        assert!(
            rendered.contains("/Users/a&amp;b/&lt;x&gt;/anchord"),
            "unescaped path breaks the plist: {rendered}"
        );
        assert!(
            !rendered.contains("a&b"),
            "a bare & is not valid XML: {rendered}"
        );
    }

    /// The registration must not pin a socket path: anchord has to resolve
    /// the same one a client resolves (docs/07-lifecycle.md).
    #[test]
    fn no_socket_path_is_baked_in() {
        let rendered = platform::render(&PathBuf::from("/usr/local/bin/anchord"));
        assert!(
            !rendered.contains("--socket") && !rendered.contains(".sock"),
            "the unit pins a socket path: {rendered}"
        );
    }

    /// Restart-on-exit is the point of being supervised at all.
    #[test]
    fn the_service_is_restarted_if_it_exits() {
        let rendered = platform::render(&PathBuf::from("/usr/local/bin/anchord"));
        #[cfg(target_os = "linux")]
        assert!(rendered.contains("Restart=always"), "{rendered}");
        #[cfg(target_os = "macos")]
        assert!(rendered.contains("<key>KeepAlive</key>"), "{rendered}");
    }

    /// Login-scoped, never boot-scoped: a boot-scoped registration needs
    /// elevation and runs for a user who is not logged in.
    #[cfg(target_os = "linux")]
    #[test]
    fn the_unit_is_login_scoped() {
        let rendered = platform::render(&PathBuf::from("/usr/local/bin/anchord"));
        assert!(rendered.contains("WantedBy=default.target"), "{rendered}");
        assert!(
            !rendered.contains("multi-user.target"),
            "multi-user.target is boot-scoped: {rendered}"
        );
    }

    /// A build-output path is recognised wherever `target/debug` appears,
    /// not only at a fixed depth — the warning is worthless if it only fires
    /// for one project layout.
    ///
    /// Calls `is_build_output` rather than restating its rule: a test that
    /// re-implements the predicate it checks passes for any predicate,
    /// including a broken one.
    #[test]
    fn build_output_paths_are_recognised() {
        for path in [
            "/home/u/p/target/debug/anchord",
            "/home/u/p/target/release/anchord",
            "/a/b/c/d/target/debug/anchord",
            "/tmp/cargo-target/target/debug/anchord",
        ] {
            assert!(
                is_build_output(&PathBuf::from(path)),
                "{path} should be seen as a build output"
            );
        }
    }

    /// An installed path is not a build output just because the word appears
    /// somewhere in it.
    #[test]
    fn installed_paths_are_not_build_outputs() {
        for path in [
            "/usr/local/bin/anchord",
            "/home/u/.cargo/bin/anchord",
            "/opt/target-practice/bin/anchord",
            "/home/u/debug/anchord",
        ] {
            assert!(
                !is_build_output(&PathBuf::from(path)),
                "{path} is an install location, not a build output"
            );
        }
    }

    /// `install` decides whether to touch anything by comparing the
    /// registration it would write against the one already on disk, and on
    /// macOS the cost of getting that comparison wrong is every live
    /// session: a difference it reports falsely means `bootout`, and
    /// `bootout` stops the daemon.
    ///
    /// So the rendering has to be a pure function of the path. Anything
    /// that varied per call — a timestamp, a set iterated in hash order —
    /// would make an unchanged reinstall read as a changed one, silently
    /// restoring the exact behaviour this guards against while every other
    /// test stayed green.
    #[test]
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    fn a_registration_renders_identically_for_the_same_binary() {
        let exe = PathBuf::from("/usr/local/bin/anchord");
        let other = PathBuf::from("/opt/homebrew/bin/anchord");
        let kind = if cfg!(target_os = "macos") {
            "plist"
        } else {
            "unit"
        };

        assert_eq!(
            platform::render(&exe),
            platform::render(&exe),
            "the {kind} must render byte-identically twice, or an unchanged \
             reinstall reads as a changed one and restarts the daemon"
        );
        assert_ne!(
            platform::render(&exe),
            platform::render(&other),
            "a different binary must render differently, or a changed \
             reinstall reads as unchanged and never takes effect"
        );
    }
}
