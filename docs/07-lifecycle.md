# 07 — anchord's lifecycle

The reason anchor exists is that a client can be restarted without taking
its sessions down. That property is not a consequence of the protocol; it is
a consequence of **who owns anchord's process**. This document fixes that.

## The rule

**No client ever spawns anchord.**

A daemon started as a child of a client dies with it, or is orphaned by it —
either way its lifetime is the client's lifetime, and the whole point is
lost. A client that cannot reach the socket reports that it cannot reach the
socket. It does not help.

Teleport already works this way: it resolves a socket path and connects, and
nothing in it starts anchord
([06-teleport-seam.md](06-teleport-seam.md)). This document is what makes
that an invariant rather than an accident.

So something else has to start it, and on a single-user machine the only
sensible candidate is the OS's own per-user service manager.

## What anchor ships

```text
anchor service install     register anchord with the user's service manager
anchor service uninstall   remove it
anchor service status      what is registered; on macOS, only whether it is loaded
                           in launchd (not whether anchord is running); on
                           Windows, whether anchord answers on this user's pipe
```

- **macOS** — a launchd **user agent** in `~/Library/LaunchAgents`.
- **Linux** — a systemd **user unit** in `~/.config/systemd/user`.
- **Windows** — a **Task Scheduler task** named `anchord`, triggered by this
  user's logon, running `conhost.exe --headless anchord.exe` under the
  user's interactive token at least privilege. The XML it was registered
  from is kept as `%LOCALAPPDATA%\anchor\anchord.task.xml`, which is what
  `install` compares against. How each choice was arrived at, and checked,
  is [09-windows.md](09-windows.md) §4.7.

All three are **login-scoped and need no root** (on Windows, no
elevation: a logon trigger scoped to this user's SID is what a standard
user may register; an unscoped one is `Access is denied`). A boot-scoped unit (launchd's
`LaunchDaemons`, or systemd lingering) starts sessions for a user who is not
logged in, needs elevation to install, and is a different product decision
than this one. `install` therefore never enables lingering and never writes
outside the user's own home.

This does not contradict "not a process manager"
([01-scope.md](01-scope.md)). anchor is not supervising anything here; it is
asking to *be* supervised, and writing the one file that asks.

## Restart, and what it does not save

The unit restarts anchord if it exits. On Windows "restarts" means within a
minute: Task Scheduler's own `RestartOnFailure` was observed not to revive
a killed anchord, so the task repeats every minute under `IgnoreNew`, which
is a no-op while anchord runs. That is worth having — the next
session works without anyone noticing — but be exact about what it does not
do:

**If anchord dies, its PTYs lose their master and are hung up.** A child
that ignores SIGHUP keeps running as an orphan anchor no longer tracks, and
its row is `lost`. The master fd has no handoff. A restarted
anchord is a clean daemon whose previous sessions are marked `lost`
([02-model.md](02-model.md)). Restart buys continuity of *the service*, not
of *the sessions*.

This is the same boundary teleport documented and deliberately declined to
pay for (a broker process per PTY), and anchor inherits the choice. anchor
makes **updates** survivable by being small and changing rarely. It does not
make **crashes** survivable. Nothing in the README, the unit file, or the
release notes may imply otherwise.

## Why that is still the whole point

Updating a client is routine; a client crash is rare. Before this, shipping a
new teleport meant killing every running agent, so teleport's updater refused
to run while sessions were live. With anchord supervised separately, the
client restarts and reconnects by offset
([04-protocol.md](04-protocol.md)) and the sessions never notice.

## Stopping

launchd and systemd both stop a service by sending **SIGTERM** and escalating to
SIGKILL if it does not go. anchord therefore has to treat SIGTERM as an
instruction, not an accident: it stops accepting, lets the runtime unwind,
and **exits 0**. SIGINT is handled identically, because a daemon run in the
foreground during development is stopped with Ctrl-C.

What that buys is one thing, and it is worth being exact about it: unwinding
drops the `LogSyncer`, whose `Drop` does a final `fsync` pass over every
live log. Dying of the signal skips that and leaves the tail of each log
unsynced until the kernel writes it back. Neither case loses data to a
process death — the page cache outlives the process — but only the clean
path survives the machine losing power a moment later.

**Windows has no SIGTERM.** anchord treats the console control events the
same way — Ctrl-C and Ctrl-Break in the foreground, and close, logoff and
shutdown when the console or the session goes away — and unwinds to the
same final fsync. What Windows does not announce cannot be handled:
`TerminateProcess` (Task Manager, `taskkill /F`, `anchor service
uninstall`) ends the process outright, as SIGKILL does.

What it deliberately does **not** do:

- **It does not unlink the socket.** The next daemon already probes a socket
  file it finds and replaces it when nothing answers (the staleness probe in
  `bind()`, `src/server.rs`); unlinking on the way out adds a second path to
  the same outcome and a way to remove a file that is no longer ours.
- **It does not mark sessions `lost`.** Startup does that, has to do it
  anyway for the crash case, and is the only place that decides what a live
  row means (see the bind-before-recovery note in `daemon.rs`).
- **It does not terminate sessions gracefully.** PTYs lose their master with
  the daemon — the master fd closes, and the kernel hangs up each child. A
  child that ignores SIGHUP survives that as an untracked orphan, and its
  row is `lost`. That boundary is
  the section above, and a shutdown path is not where it gets renegotiated.

## Edge cases the implementation must handle

- **Install is idempotent, and that has to mean *inert*.** Running it twice
  is not an error, does not stack two registrations, and — the part that
  costs something when it is got wrong — does not touch the running daemon.
  `install` renders the registration, compares it against the one on disk,
  and returns without acting when the two are identical. Replacing a
  registration on macOS means `launchctl bootout`, which stops anchord, and
  every session it was holding loses its master with it (the boundary above). An install
  that is idempotent on paper and kills live sessions in practice would
  break the one property anchor exists for.
- **A registration that really did change is applied, and said out loud.**
  On macOS the daemon is restarted and `install` prints that it was, because
  the restart costs the sessions. On Linux the changed unit is written and
  reloaded, but systemd will not restart an already-running service, so the
  old anchord keeps going until it next stops. The platforms differ here
  deliberately: converging them means restarting anchord on Linux too, which
  would kill sessions to fix a cosmetic inconsistency. Windows behaves like
  Linux: re-registering a task (`schtasks /Create /F`) was observed to leave
  the running anchord serving, so a changed registration applies at its
  next start.
- **Uninstall stops anchord on every platform.** On Windows that has to be
  done by hand: `schtasks /End` stops the headless conhost and leaves
  anchord serving as an orphan (observed), so `uninstall` finds anchord as
  the server of this user's own owner-verified pipe and terminates it.
- **The binary path is recorded absolutely**, resolved from the running
  executable. A relative path resolves against the service manager's working
  directory, not the user's.
- **A `cargo build` output path is warned about.** `target/debug/anchord` can
  be removed by the next build, which breaks the unit with no error until
  the next login.
- **Paths containing spaces** must survive the unit file's quoting rules.
  All three formats have them and they are not the same rules; on Windows
  the path is also re-split by conhost, so it is quoted inside the arguments.
- **Uninstall removes only what install wrote**, and is not an error when
  nothing is installed.
- **An unsupported platform says so**, rather than succeeding and doing
  nothing.
- **The socket path is not baked in.** The unit starts `anchord` with no
  socket argument, so it resolves the same path a client resolves
  ([04-protocol.md](04-protocol.md)). A unit that pinned a path would
  silently diverge from the client's resolution the moment either changed.
