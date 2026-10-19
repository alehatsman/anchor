//! `anchor service install|uninstall|status` — registering `anchord` with
//! the user's own service manager (docs/07-lifecycle.md).
//!
//! The point of this module is the invariant in that document: **no client
//! ever spawns anchord**. A daemon started as a child of a client has the
//! client's lifetime, which is exactly the property anchor exists to break.
//! Something else has to start it, and on a single-user machine that is the
//! OS's per-user service manager.
//!
//! Login-scoped and root-free on every platform: a launchd *user agent* on
//! macOS, a systemd *user unit* on Linux, a Task Scheduler task triggered by
//! this user's logon on Windows. Boot-scoped supervision (`LaunchDaemons`,
//! systemd lingering, a boot trigger or a stored password) needs elevation
//! and starts sessions for a user who is not logged in — a different
//! decision than this one, and deliberately not taken here.
//!
//! No registration pins a socket path or a pipe name. `anchord` resolves the
//! same endpoint a client resolves, and a unit that baked one in would
//! diverge from that resolution the moment either side changed.

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
#[cfg_attr(not(any(target_os = "macos", target_os = "linux")), allow(dead_code))]
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
#[cfg_attr(
    not(any(target_os = "macos", target_os = "linux", windows)),
    allow(dead_code)
)]
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
#[cfg_attr(
    not(any(target_os = "macos", target_os = "linux", windows)),
    allow(dead_code)
)]
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
#[cfg_attr(
    not(any(target_os = "macos", target_os = "linux", windows)),
    allow(dead_code)
)]
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

/// Windows: a Task Scheduler task for the current user (docs/09-windows.md
/// §4.7, docs/07-lifecycle.md).
///
/// Every behaviour below was checked on Windows 11 build 26200, as a
/// standard (non-elevated) user, before it was written down:
///
/// - **`schtasks /SC ONLOGON` is `Access is denied`** for a standard user --
///   the failure teleport hit on the same build -- because that trigger fires
///   for *any* user's logon. A `LogonTrigger` scoped to this user's SID,
///   imported as XML, registers without elevation. That scoping is the whole
///   difference, and it is also the login-scoped rule this module keeps on
///   every platform.
/// - **No console window**: the action is `conhost.exe --headless anchord`.
///   A console app started by the scheduler otherwise opens a window, and
///   closing it would end anchord and every session in it.
/// - **Revival is a repetition, not `RestartOnFailure`.** A killed anchord
///   was never restarted by `RestartOnFailure` (the task just went back to
///   Ready). A one-minute repetition with `IgnoreNew` is a no-op while
///   anchord runs and starts it again within a minute once it does not --
///   observed: killed, answering again 75s later. It is systemd's
///   `Restart=always`, at a coarser grain.
/// - **Re-registering does not touch a running anchord**: `/Create /F` over
///   a running task left the same process serving. So, as on Linux, a
///   changed registration applies at the next start.
/// - **`/End` does not stop anchord**: it ends the headless conhost and the
///   daemon keeps serving as an orphan. `uninstall` therefore stops anchord
///   itself, found as the server of this user's own, owner-verified pipe.
///
/// What was *not* observed, and cannot be from inside one logon session:
/// the logon trigger firing at an actual logon. Registration, the action,
/// the repetition and the principal all were.
#[cfg(windows)]
mod platform {
    use std::path::{Path, PathBuf};
    use std::process::Command;

    use anyhow::{bail, Context, Result};

    use super::{anchord_path, warn_if_build_output, Outcome};

    /// The task's name, in the root folder: a folder of our own would need
    /// rights a standard user may not have.
    pub(super) const TASK: &str = "anchord";

    /// The registration as last written, kept beside the data so `install`
    /// can compare against it, the way Linux compares the unit file.
    /// Task Scheduler's own export is normalised and would never compare
    /// equal to what was imported.
    fn registration_path() -> Result<PathBuf> {
        Ok(crate::paths::Paths::resolve()?
            .data_dir()
            .join("anchord.task.xml"))
    }

    fn schtasks(args: &[&str]) -> std::io::Result<std::process::Output> {
        Command::new("schtasks.exe").args(args).output()
    }

    fn registered() -> bool {
        schtasks(&["/Query", "/TN", TASK]).is_ok_and(|o| o.status.success())
    }

    fn xml_escape(value: &str) -> String {
        value
            .replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
            .replace('"', "&quot;")
    }

    fn conhost() -> PathBuf {
        let root = std::env::var_os("SystemRoot")
            .filter(|v| !v.is_empty())
            .map_or_else(|| PathBuf::from(r"C:\Windows"), PathBuf::from);
        root.join("System32").join("conhost.exe")
    }

    pub(super) fn install() -> Result<Outcome> {
        let exe = anchord_path()?;
        warn_if_build_output(&exe);
        let user = crate::winsec::UserSid::current()?.to_sid_string()?;
        let xml = render(&exe, &conhost(), &user);

        let path = registration_path()?;
        // The same rule as the other two platforms: an unchanged
        // registration is left alone, and says nothing happened.
        if registered() && std::fs::read_to_string(&path).is_ok_and(|current| current == xml) {
            return Ok(Outcome::Installed(path));
        }
        let paths = crate::paths::Paths::resolve()?;
        paths.ensure_dirs()?;
        std::fs::write(&path, &xml).with_context(|| format!("writing {}", path.display()))?;

        // schtasks reads the file as UTF-16, as the declaration says.
        let wide = path.with_extension("utf16.xml");
        let mut bytes = vec![0xFF, 0xFE];
        bytes.extend(xml.encode_utf16().flat_map(u16::to_le_bytes));
        std::fs::write(&wide, bytes).with_context(|| format!("writing {}", wide.display()))?;
        let created = schtasks(&[
            "/Create",
            "/TN",
            TASK,
            "/XML",
            &wide.to_string_lossy(),
            "/F",
        ]);
        let _ = std::fs::remove_file(&wide);
        let created = created.context("running schtasks")?;
        if !created.status.success() {
            let _ = std::fs::remove_file(&path);
            bail!(
                "schtasks refused the registration: {}",
                String::from_utf8_lossy(&created.stderr).trim()
            );
        }

        // Start it now rather than at next logon -- systemd's `enable
        // --now`. With `IgnoreNew` this is a no-op if one is already
        // running, so a changed registration does not restart anchord: it
        // applies at the next start, as on Linux.
        let _ = schtasks(&["/Run", "/TN", TASK]);
        Ok(Outcome::Installed(path))
    }

    /// The task definition. A pure function of its inputs, so an unchanged
    /// reinstall compares equal and does nothing.
    ///
    /// The user is a SID, not `DOMAIN\name`: a name can be renamed, and the
    /// environment variables it would come from can say anything.
    pub(super) fn render(exe: &Path, conhost: &Path, user: &str) -> String {
        let exe = xml_escape(&exe.to_string_lossy());
        let conhost = xml_escape(&conhost.to_string_lossy());
        let user = xml_escape(user);
        // The repetition is the watchdog (see the module doc); the time
        // trigger carries it from the moment of install, since a logon
        // trigger's repetition only starts at the next logon. Under an
        // interactive token neither runs while the user is logged off.
        format!(
            r#"<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.2" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo>
    <Description>anchor session daemon. Managed by `anchor service`; see docs/07-lifecycle.md.</Description>
  </RegistrationInfo>
  <Triggers>
    <LogonTrigger>
      <Enabled>true</Enabled>
      <UserId>{user}</UserId>
      <Repetition>
        <Interval>PT1M</Interval>
        <StopAtDurationEnd>false</StopAtDurationEnd>
      </Repetition>
    </LogonTrigger>
    <TimeTrigger>
      <Enabled>true</Enabled>
      <StartBoundary>2026-01-01T00:00:00</StartBoundary>
      <Repetition>
        <Interval>PT1M</Interval>
        <StopAtDurationEnd>false</StopAtDurationEnd>
      </Repetition>
    </TimeTrigger>
  </Triggers>
  <Principals>
    <Principal id="Author">
      <UserId>{user}</UserId>
      <LogonType>InteractiveToken</LogonType>
      <RunLevel>LeastPrivilege</RunLevel>
    </Principal>
  </Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <ExecutionTimeLimit>PT0S</ExecutionTimeLimit>
    <Enabled>true</Enabled>
  </Settings>
  <Actions Context="Author">
    <Exec>
      <Command>{conhost}</Command>
      <Arguments>--headless "{exe}"</Arguments>
    </Exec>
  </Actions>
</Task>
"#
        )
    }

    pub(super) fn uninstall() -> Result<Outcome> {
        let path = registration_path()?;
        if !registered() {
            let _ = std::fs::remove_file(&path);
            return Ok(Outcome::NotInstalled);
        }
        let deleted = schtasks(&["/Delete", "/TN", TASK, "/F"]).context("running schtasks")?;
        if !deleted.status.success() {
            bail!(
                "schtasks could not remove the task: {}",
                String::from_utf8_lossy(&deleted.stderr).trim()
            );
        }
        let _ = std::fs::remove_file(&path);
        // `/End` would not stop it (see the module doc), so stop it here, as
        // `disable --now` and `bootout` do on the other platforms. Its
        // sessions end with it -- the boundary docs/07-lifecycle.md states.
        if let Some(pid) = running_daemon() {
            match terminate(pid) {
                Ok(()) => eprintln!("stopped anchord (pid {pid}); its sessions ended with it"),
                Err(e) => eprintln!("warning: could not stop anchord (pid {pid}): {e}"),
            }
        }
        Ok(Outcome::Uninstalled(path))
    }

    pub(super) fn status() -> Result<String> {
        let path = registration_path()?;
        if !registered() {
            return Ok("not installed".into());
        }
        // Asked of the pipe rather than of the task: the task's own state
        // says "Ready" for an anchord whose conhost is gone but which is
        // still serving, and "Running" says nothing about whether it answers.
        Ok(format!(
            "installed as scheduled task {TASK} ({})\n{}",
            path.display(),
            match running_daemon() {
                Some(pid) => format!("running (pid {pid})"),
                None => "not running — the task starts it within a minute while you are \
                         logged in, or run `anchor service install` again"
                    .into(),
            }
        ))
    }

    /// The pid serving this user's pipe, if anything is -- through the
    /// client's own connect, so a pipe this user does not own is never
    /// mistaken for anchord.
    fn running_daemon() -> Option<u32> {
        use std::os::windows::io::AsRawHandle;
        let pipe = crate::paths::Paths::resolve()
            .ok()?
            .socket_path()
            .to_path_buf();
        let stream = crate::client::Stream::connect(&pipe).ok()?;
        let mut pid = 0u32;
        // SAFETY: a connected client handle and an out-pointer we own.
        let ok = unsafe {
            windows_sys::Win32::System::Pipes::GetNamedPipeServerProcessId(
                stream.as_raw_handle(),
                &mut pid,
            )
        };
        (ok != 0).then_some(pid)
    }

    fn terminate(pid: u32) -> std::io::Result<()> {
        use windows_sys::Win32::Foundation::CloseHandle;
        use windows_sys::Win32::System::Threading::{
            OpenProcess, TerminateProcess, PROCESS_TERMINATE,
        };
        // SAFETY: plain values in, a handle or null out.
        let handle = unsafe { OpenProcess(PROCESS_TERMINATE, 0, pid) };
        if handle.is_null() {
            return Err(std::io::Error::last_os_error());
        }
        // SAFETY: the handle opened above, closed once.
        let ok = unsafe { TerminateProcess(handle, 0) };
        let err = std::io::Error::last_os_error();
        // SAFETY: as above.
        unsafe { CloseHandle(handle) };
        if ok == 0 {
            return Err(err);
        }
        Ok(())
    }
}

/// Platforms with no per-user service manager this maps onto. Saying so is
/// the point: an `install` that succeeded and registered nothing would be
/// discovered only when the sessions it was supposed to supervise were gone.
#[cfg(not(any(target_os = "macos", target_os = "linux", windows)))]
mod platform {
    use anyhow::{bail, Result};

    use super::Outcome;

    fn unsupported<T>() -> Result<T> {
        bail!(
            "`anchor service` supports macOS (launchd user agent), Linux (systemd user \
             unit) and Windows (a scheduled task). On this platform, start anchord from \
             whatever supervises your user's \
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

    /// The registration for `exe`, through whichever renderer this platform
    /// has. Windows' also takes the conhost it runs under and the user, both
    /// fixed here so the rendering under test is a pure function of `exe`.
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    fn rendered(exe: &std::path::Path) -> String {
        platform::render(exe)
    }

    #[cfg(windows)]
    fn rendered(exe: &std::path::Path) -> String {
        platform::render(
            exe,
            std::path::Path::new("C:/Windows/System32/conhost.exe"),
            "S-1-5-21-1-2-3-1001",
        )
    }

    /// A path with a space must survive the unit format's quoting. Getting
    /// this wrong is silent: `install` reports success and the service
    /// fails to start later, because the path split into several words.
    #[test]
    #[cfg(any(target_os = "macos", target_os = "linux", windows))]
    fn a_path_with_a_space_stays_one_word() {
        let exe = PathBuf::from("/Users/a b/bin/anchord");
        let rendered = rendered(&exe);
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
        // conhost re-splits its arguments, so the path is quoted inside them.
        #[cfg(windows)]
        assert!(
            rendered.contains(r#"<Arguments>--headless "/Users/a b/bin/anchord"</Arguments>"#),
            "unquoted, conhost splits the path on the space: {rendered}"
        );
    }

    /// XML metacharacters in a path must be escaped, or the plist (or the
    /// task) does not parse and is rejected well after `install` said it
    /// worked.
    #[cfg(any(target_os = "macos", windows))]
    #[test]
    fn xml_metacharacters_in_a_path_are_escaped() {
        let exe = PathBuf::from("/Users/a&b/<x>/anchord");
        let rendered = rendered(&exe);
        assert!(
            rendered.contains("/Users/a&amp;b/&lt;x&gt;/anchord"),
            "unescaped path breaks the XML: {rendered}"
        );
        assert!(
            !rendered.contains("a&b"),
            "a bare & is not valid XML: {rendered}"
        );
    }

    /// The registration must not pin a socket path: anchord has to resolve
    /// the same one a client resolves (docs/07-lifecycle.md). On Windows,
    /// the same for the pipe.
    #[test]
    #[cfg(any(target_os = "macos", target_os = "linux", windows))]
    fn no_socket_path_is_baked_in() {
        let rendered = rendered(&PathBuf::from("/usr/local/bin/anchord"));
        assert!(
            !rendered.contains("--socket")
                && !rendered.contains(".sock")
                && !rendered.contains("pipe"),
            "the registration pins an endpoint: {rendered}"
        );
    }

    /// Restart-on-exit is the point of being supervised at all. On Windows
    /// it is a one-minute repetition that `IgnoreNew` turns into a no-op
    /// while anchord runs -- `RestartOnFailure` was observed not to revive a
    /// killed daemon (see the module doc).
    #[test]
    #[cfg(any(target_os = "macos", target_os = "linux", windows))]
    fn the_service_is_restarted_if_it_exits() {
        let rendered = rendered(&PathBuf::from("/usr/local/bin/anchord"));
        #[cfg(target_os = "linux")]
        assert!(rendered.contains("Restart=always"), "{rendered}");
        #[cfg(target_os = "macos")]
        assert!(rendered.contains("<key>KeepAlive</key>"), "{rendered}");
        #[cfg(windows)]
        {
            assert!(
                rendered.contains("<MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>"),
                "without IgnoreNew every repetition starts a second anchord: {rendered}"
            );
            assert_eq!(
                rendered.matches("<Interval>PT1M</Interval>").count(),
                2,
                "both triggers must repeat, or nothing revives a dead anchord: {rendered}"
            );
            assert!(
                rendered.contains("<ExecutionTimeLimit>PT0S</ExecutionTimeLimit>"),
                "the default limit ends a task after 72 hours: {rendered}"
            );
        }
    }

    /// Login-scoped, never boot-scoped: a boot-scoped registration needs
    /// elevation and runs for a user who is not logged in.
    #[cfg(target_os = "linux")]
    #[test]
    fn the_unit_is_login_scoped() {
        let rendered = rendered(&PathBuf::from("/usr/local/bin/anchord"));
        assert!(rendered.contains("WantedBy=default.target"), "{rendered}");
        assert!(
            !rendered.contains("multi-user.target"),
            "multi-user.target is boot-scoped: {rendered}"
        );
    }

    /// The same rule on Windows: this user's logon, this user's interactive
    /// token, no stored password and no elevation. An unscoped logon trigger
    /// is also exactly what a standard user is refused (`Access is denied`).
    #[cfg(windows)]
    #[test]
    fn the_task_is_login_scoped() {
        let rendered = rendered(&PathBuf::from("/usr/local/bin/anchord"));
        assert!(
            rendered.contains("<LogonTrigger>")
                && rendered.contains("<UserId>S-1-5-21-1-2-3-1001</UserId>"),
            "the logon trigger must be scoped to this user: {rendered}"
        );
        assert!(
            rendered.contains("<LogonType>InteractiveToken</LogonType>"),
            "{rendered}"
        );
        assert!(
            rendered.contains("<RunLevel>LeastPrivilege</RunLevel>"),
            "{rendered}"
        );
        for boot_scoped in ["BootTrigger", "S4U", "Password", "HighestAvailable"] {
            assert!(
                !rendered.contains(boot_scoped),
                "{boot_scoped} is not login-scoped or needs elevation: {rendered}"
            );
        }
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
    #[cfg(any(target_os = "macos", target_os = "linux", windows))]
    fn a_registration_renders_identically_for_the_same_binary() {
        let exe = PathBuf::from("/usr/local/bin/anchord");
        let other = PathBuf::from("/opt/homebrew/bin/anchord");
        let kind = if cfg!(target_os = "macos") {
            "plist"
        } else {
            "unit or task"
        };

        assert_eq!(
            rendered(&exe),
            rendered(&exe),
            "the {kind} must render byte-identically twice, or an unchanged \
             reinstall reads as a changed one and restarts the daemon"
        );
        assert_ne!(
            rendered(&exe),
            rendered(&other),
            "a different binary must render differently, or a changed \
             reinstall reads as unchanged and never takes effect"
        );
    }
}
