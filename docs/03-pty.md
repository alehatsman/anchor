# 03 — PTY layer

**Read [teleport/docs/03-pty-layer.md](https://github.com/alehatsman/teleport/blob/main/docs/03-pty-layer.md)
before writing a line of this file.** It is the highest-risk file in that
product, every rule in it was paid for with a real bug, and anchor's job is to
preserve those rules — not to rediscover them.

This doc states the invariants an implementation must hold and points at the
reference for each. Where teleport and this doc disagree, teleport is right
and this doc is stale; file an issue.

## The rule

> **The PTY reader must never wait for a subscriber.**

Not for a slow client, not for a disconnecting one, not for a socket buffer.
Anything else is a deadlock waiting for a bad network.

## Thread model — four dedicated OS threads per session

```text
                  ┌── reader   read() → append log → advance offset → fanout
                  ├── writer   write()            (can block indefinitely)
  PTY ────────────┤
                  ├── control  resize(), terminate()
                  └── reaper   child.wait()       (blocks until exit)
```

`std::thread`, **not** `tokio::task::spawn_blocking`. A permanently blocking
loop occupies a blocking-pool slot forever; enough sessions and unrelated
blocking work starves.

Four, not two. `read()`, `write()` and `wait()` block independently:
- a write to an unread PTY blocks indefinitely and, if it shares a thread
  with control, wedges `terminate` behind it (teleport `#S3`)
- nothing in a two-thread model calls `wait()`, so the exit code has no path
  into the session row (teleport `#S1`)

`resize` and `terminate` share the control thread — both bounded, and
`terminate`'s own bounded wait belongs there.

Threads reach async code only through channels. Reader → subscribers uses
`try_send`, **never** `blocking_send`.

## Reader loop ordering — not negotiable

```text
1. read()
2. append to output.vt
3. advance next_offset
4. fan out to subscribers (try_send; on failure mark slow)
```

**Persist, then advance, then fan out.** A subscriber must never see an offset
for bytes not yet on disk, or a reconnect seeking to that offset reads past
EOF.

Steps 3–4 happen under one short mutex that **also guards subscriber
registration**. That mutex is what closes the attach race.

Buffer 64 KiB. No `fsync` per chunk — fsync on close and on a timer.

## Backpressure

Every subscriber has one bounded budget: **8 MiB**, charged per queued chunk
as payload + fixed per-chunk overhead. Overflow **disconnects that
subscriber**; it never blocks the reader.

One bound in one unit, deliberately. Teleport shipped two (a chunk count and
a byte budget) and the count half governed on macOS, where PTY reads average
14 bytes — a design promising 8 MiB delivered ~3.5 KiB (teleport `#N5`).
Charging per-chunk overhead against the byte budget protects the same thing
without a second number to get wrong.

A disconnected subscriber reconnects and replays from its last offset. This
converts a slow consumer into an ordinary reconnection. **Test it
deliberately** — and check the test reaches its own premise.

**A tty discards rather than blocks when the written data is not whole
lines.** In canonical mode an over-long line with no terminator is thrown
away at the line-length limit, so a child that never reads its pty absorbs
unbounded input and the writer never blocks. anchor#9: a test built on that
shape pushed 436 MB into a `sleep 30` and proved nothing for its whole life.
Complete lines queue instead; the canonical queue is ~1 KiB, fills at once,
and the next write to the master blocks for real. A write rejected by the
session layer is also not proof on its own — the channel fills from slowness
too. Only an unbroken streak of rejections means the writer thread is
wedged.

### Catch up before registering, not after

A subscriber is not registered until the history it owes its client fits
inside the bound with room to spare. Catching up happens first, in bounded
rounds, off the mutex.

Registering first and replaying afterwards spends the same 8 MiB twice and
makes every attach to a busy session fail.

## Resize and the control lease

There is exactly **one** effective PTY size. "Every client sends its own
dimensions" is wrong — a 390px phone must not fight a 160-column desktop.

**One control lease per session.** Only the lease holder may send input or
resize. Everyone else observes. Transfer is explicit.

- coalesce: at most one resize per 100ms per session, keep the latest
- clamp both dimensions to `1..=1000`, reject otherwise as a protocol error
- persist applied cols/rows, throttled to ~1/s

## Termination

State machine:

```text
RUNNING → CLOSING → EXITED
            ├ reject new input
            ├ keep draining output (reader thread stays alive)
            ├ graceful signal
            ├ bounded wait
            └ hard kill
```

Policy:

1. state = `closing`; input writes error to the controller
2. graceful: `killpg(pgid, SIGHUP)`, then `SIGTERM` to the child
3. wait up to **5s**, reader still draining
4. still alive → `killpg(pgid, SIGKILL)`
5. wait **2s** more → give up, `EXITED`, `exit_code=null`,
   `lost_reason="kill_timeout"`
6. on reader EOF: flush + fsync log, record `exited_at`, `exit_code`, emit
   `exit` to subscribers, close them

**`portable-pty` 0.9's `kill()` is not a hard kill on Unix.** The
`ChildKiller` handle sends only `SIGHUP`. A child ignoring HUP and TERM
survives both the graceful step and a naive kill step. Step 4 must send
`SIGKILL` itself. Teleport issue #49; regression test
`terminate_hard_kills_a_child_that_ignores_hup_and_term`.

**Do not drop the master as a termination mechanism.** The reader holds a
cloned fd and the writer holds another — both `dup()`s of the same open file
description. Dropping the `MasterPty` closes nothing and raises no signal
(teleport `#S4`).

## Spawn details that bite

- **argv array, never a concatenated shell string.**
- **`login_shell`** runs the program under `$SHELL` so it inherits the user's
  environment. A daemon started by launchd/systemd inherits none of it —
  `PATH` beyond the system default, `EDITOR`, `LANG`, version manager shims.
  Without this, `claude` is not even on `PATH`. `-l`, not `-i`: a login shell
  sources the profile that builds the environment; an interactive one also
  loads prompt, completion and plugins, which is startup cost and stray
  output with no business in a PTY. Ignored on Windows.

  **The invariant: no request data is ever interpolated into shell source.**
  Every form below passes a *fixed* script literal and hands the program and
  its arguments to the shell as separate argv entries, which the script
  expands back without splitting or globbing. A form that cannot do that does
  not get added to this table.

  The POSIX form is not universal, so `$SHELL`'s file name selects one of
  three (anchor#10):

  | `$SHELL` | invocation | sources |
  | --- | --- | --- |
  | anything else (default) | `-l -c 'exec "$0" "$@"' <program> <args…>` | login profile |
  | `fish` | `-l -c 'exec $argv' <program> <args…>` | login profile |
  | `csh`, `tcsh` | `-c 'exec $argv:q' <program> <args…>` | `.cshrc` only |

  The default is the POSIX form, for `sh`/`bash`/`zsh`/`ksh`/`dash` and for
  any shell not recognised — an unknown shell is far more likely to be
  POSIX-compatible than not, and guessing wrong fails loudly at spawn rather
  than silently.

  fish binds remaining arguments to `$argv` and does not word-split on
  expansion, so `exec $argv` needs no quoting. csh does word-split; `:q`
  is what preserves one word per argument, and plain `$argv` is a bug
  — it splits `a b` into two arguments and globs a literal `*`.

  **csh and tcsh cannot have `-l` and `-c` together** — they reject `-l`
  outright in that position, and there is no flag that combines them. Those
  users get `.cshrc` but **not** `.login`. That is a real difference from
  what `login_shell: true` promises, and it is accepted rather than papered
  over: falling back to `/bin/sh` would source the user's config not at all
  while still claiming to be their login shell, which is worse because it
  is invisible.

  A spawn under csh **logs a warning naming the shortfall**. Without it the
  failure reaches the user as `command not found` for a program that is on
  `PATH` in their real login shell — a true error pointing at the wrong
  layer, which is the one thing `login_shell` exists to prevent.
- **`TERM` default.** A daemon with no controlling terminal has no `TERM`, and
  children render monochrome because color detection treats absent `TERM` as
  dumb. Set `TERM=xterm-256color` + `COLORTERM=truecolor` **only when unset**;
  never clobber a real one, and an explicit `env` override always wins.
- **Drop the slave handle** in the parent after spawn on Unix, so the master
  sees EOF when the child exits.
- **Do not copy the daemon's full environment into stored metadata.** The
  child inherits it; only explicit overrides are recorded, and they are
  redacted in API responses.

## Platform

Unix first. Windows is a known-harder target and teleport has an unresolved
blocker there (`#W1`: a gracefully-exiting ConPTY child was never observed as
exited). **v1 is Unix-only.** Keep every `#[cfg(windows)]` inside the PTY
module so the port stays possible; a platform conditional anywhere else is a
bug.
