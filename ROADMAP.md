# Roadmap

Each milestone retires one risk and is independently abandonable. No milestone
starts before the previous one's exit criteria are met.

Ordering principle: **the risky subsystem first, the ergonomics last.** The
PTY layer is where this project lives or dies; everything else is plumbing
around it.

---

## M0 — Skeleton ✅

Scope: single crate; `anchord` binds the socket and answers `ping`; SQLite
schema + forward-only migrations; data dir resolution; `tracing` from the
first commit.

Exit: daemon starts, a client connects, a session row round-trips. No PTY yet.

---

## M1 — PTY core, ported with its tests ✅

The whole bet. Port teleport's PTY layer and, critically, **its test suite**.

Scope: `pty.rs`, `session/*`, `log.rs` per [docs/03-pty.md](docs/03-pty.md) —
four-thread model, reader ordering, 8 MiB bound, catch-up-before-register,
control lease, termination policy including the `SIGKILL` step.

Port from `teleport/daemon/tests/`: `pty_primitive`, `output_log`,
`session_replay`, `session_catchup`, `session_backpressure`,
`reconnect_storm`, `restart_recovery`, `pty_login_shell`, `pty_term_default`.

Exit:
- every ported test passes
- a 10 MiB burst of output past the 8 MiB bound
  (`tests/session_backpressure.rs`) to a deliberately slow subscriber
  disconnects **that subscriber** and the PTY reader never stalls
- a child that ignores SIGHUP and SIGTERM is still reaped
- kill `anchord`, restart: sessions are `LOST`, logs are intact and readable

Risk retired: the only one that matters. If the ported layer does not hold
under its own tests, stop.

---

## M2 — Labels and query ✅

The reason the boundary moved.

Scope: `labels` table + index; `create` accepts labels; `label` mutates them;
`list` filters by exact match on any key; limits enforced (§02).

Exit: 500 sessions, filter by a label, correct results. A client can answer
"which session is for X?" with no side-table.

---

## M3 — Socket protocol and CLI ✅

Scope: full control ops and binary stream framing per
[docs/04-protocol.md](docs/04-protocol.md); `anchor` CLI with raw-mode
passthrough attach and `~.` detach.

Exit: spawn a shell via CLI, detach, reconnect from a second terminal, see
scrollback and continue typing. Two observers plus one controller behave
correctly.

---

## M4 — Teleport on anchor, behind a flag ✅

Scope: an anchor client in teleport; its session layer switchable; default
**off**.

Exit: **teleport's own test suite passes against the anchor path.** That is
the acceptance criterion for the whole extraction. Evidence lives in teleport,
not here: `teleport/daemon/tests/anchor_http.rs` (its HTTP surface against an
external `anchord`) and `daemon/tests/anchor_client.rs`. Teleport's default is
unchanged and it ships normally throughout.

---

## M5 — Prove the payoff ✅

> Done: an agent survives teleport restarting under it (demonstrated by
> `an_agent_survives_teleport_restarting_under_it`), anchord restarts without
> teleport noticing beyond a reconnect, the updater restarts under load when
> the daemon advertises `sessions_survive_restart`, `anchor service
> install` gives anchord a lifecycle of its own, and an unreachable anchord
> is reported rather than read as an empty session list.
>
> **Both exit criteria were demonstrated against a live daemon** at teleport
> `4912d9c`, 2026-10-16: a session created by the old teleportd survived the
> upgrade with its transcript intact, and `anchord` was rebuilt and bounced
> under a running teleportd that never noticed. `use_anchor=true` is deployed
> on `work_mac` — an opt-in flag, **not** the default. See teleport `docs/20-retiring-the-in-process-pty.md`.
>
> **Closed 2026-10-19.** anchor now has two real consumers driving it —
> teleport, and [tally](https://github.com/alehatsman/tally), which is a
> control surface for anchor sessions and nothing else. The boundary this
> milestone existed to prove is load-bearing in practice, not just
> demonstrated once.
>
> What was still listed as open here was never anchor's code, and only one
> item of it survives: **deleting teleport's in-process PTY layer.** That is
> gated on Windows and nothing else now, so it belongs to M6 rather than
> trailing M5 indefinitely. teleport's Windows build break (its #107) was
> fixed by gating its `anchor_*` modules on `unix` — the gate holds until
> anchord can host a session on Windows, which is exactly M6's exit.

Scope: packaging so `anchord` is a separate launchd/systemd unit with its
own lifecycle. The original scope also said "default teleport to anchor";
**that was not done.** teleport's `use_anchor` is still `false`
(`teleport/daemon/src/config.rs:140`, as of teleport `d7f34e9`), and
teleport `docs/20-retiring-the-in-process-pty.md:12` says "Nothing here is a
decision to flip the default." The flip is listed explicitly under "Later,
unscheduled" so it is not forgotten.

Exit, met: **update teleport while agents are running, and they survive.**
That is the thing anchor exists for. Demonstrated against a live daemon and
written down. The in-process deletion that followed it is M6's, because
until Windows has an anchord it is a deletion teleport cannot afford.

---

## M6 — Windows, so teleport can rely on anchor everywhere ✅

> **Done 2026-10-20, merged as #33.** All five steps landed in order, one
> commit each, every one run on a real Windows 11 Pro box (build 26200) as
> a standard user before the next began:
>
> 1. the `windows` CI job — red before the port (39 compile errors) when
>    its steps were run on that box; on GitHub it went red twice on the PR
>    for things that box could not show (an 8.3 short path in the runner's
>    `TEMP`, and SDDL spelling the runner's built-in Administrator as `LA`),
>    then green
> 2. the ConPTY layer and `tests/pty_primitive_windows.rs`, 12 fixtures;
>    removing `ConptyDsrProbe` reproduces `#W1`, and a fixture the donor
>    lacked tells the graceful step from the hard kill
> 3. a ledger the fixtures write as their last statement, checked by an
>    ungated test — red on an early return and on a gated-out module
> 4. the named pipe (owner-only DACL, fail-if-taken, identification-only
>    clients, owner check, `CancelIoEx` detach), the console input path,
>    and the owner-only data directory — each revert-checked
> 5. autostart as a per-user Task Scheduler task. teleport's `Access is
>    denied` reproduced and explained: an unscoped logon trigger needs
>    elevation, a user-scoped one does not. The logon trigger has not been
>    seen to fire; registration, the headless action and the one-minute
>    watchdog have
>
> **Exit criterion, as demonstrated on that box:** a `cmd.exe` or
> `powershell.exe` session created, typed into, detached with `~.` while
> quiet, found still running, reattached with its scrollback, and its exit
> observed exactly once with the code the child exited with — an `NTSTATUS`
> above `i32::MAX` included. Windows suite green; the Linux suite unchanged
> and green (WSL); all three CI platforms green on GitHub. What remains is
> teleport's side: lifting its `unix` gate and the in-process deletion.
> Detail and evidence: [docs/09-windows.md](docs/09-windows.md).

Scheduled 2026-10-19, reversing the deferral of 2026-10-16. The costing was
[docs/09-windows.md](docs/09-windows.md); both of its §7 questions were
answered the day it was scheduled — anchord takes an **owner-only DACL** on
the named pipe rather than a token, and Windows is in scope.

Why it came back: teleport is to rely on anchor fully, on every platform it
ships. While anchord is Unix-only, teleport's in-process PTY layer can never
be deleted and Windows never gets session survival. The deferral was not
neutral — it was a permanent cap on the thing anchor exists to do.

Scope, and the order *is* the specification. Each step is gated on the one
above it ([docs/09-windows.md](docs/09-windows.md) §5):

1. a Windows runner in CI that **executes a deliberately failing test**.
   Nothing else starts until a red is observable there.
2. the PTY layer behind `#[cfg(windows)]`, lifted from teleport along with
   `pty_primitive_windows.rs`. Serialized, or the suite oversubscribes its
   own runner.
3. an **ungated** assertion that fails when the Windows fixtures do not
   execute. A count gate is insufficient: the count is never zero.
4. transport (named pipe + DACL), CLI console input, ACL permissions.
5. autostart, **or** a written statement that Windows has no autostart
   story. Blocked upstream on a real `Access is denied`, so it is last and
   may legitimately end in the statement.

Step 3 before step 4 is deliberate: the PTY layer carries the invariant that
an exit is observed exactly once, and it gets defended before breadth is
piled on top of it.

Exit: **anchord hosts a session on Windows with the same guarantees it makes
on Unix** — above all that an exit is observed exactly once and reported
accurately. Then, and only then, teleport's in-process PTY layer becomes
deletable rather than Unix-only-deletable (#16).

Risk retired, and it was a different shape from M1's. The hard part was
free going in: ConPTY exit detection was already solved and proven on real
Windows in teleport. The risk was breadth — the transport had **no prior art
in either repo**, and anchor was taking on an access-control model it had so
far been able to decline to have. A Windows build where exits are unreliable
is worse than no Windows build, which is why step 1 was a runner and step 3 a
tripwire rather than a count.

Both halves of that breadth are now built and under test. The named pipe and
its owner-only DACL live in `src/server.rs`, `src/client.rs` and
`src/winsec.rs`; `tests/windows_transport.rs` covers them by asking the
kernel for the SDDL it holds rather than the string the descriptor was built
from, so the access-control claim is checked against the OS and not against
itself.

---

## Later, unscheduled

- **flip teleport's default to anchor** (`use_anchor: true`). Promised by
  M5's original scope and not done; nothing gates it except a decision in
  teleport, and until it happens the in-process PTY layer is the default
  everywhere
- non-PTY sessions (only with a real consumer — see scope doc)
- crash survival via per-PTY broker processes (explicitly deferred)

---

## Sequencing

```
M0  skeleton    ← no risk
M1  pty + tests ← the only real risk
M2  labels      ← the new capability
M3  protocol    ← the surface
M4  teleport    ← proof against a real consumer
M5  payoff      ← demonstrated behind use_anchor=true; the default flip is not done
M6  windows     ← done; the handover now waits on teleport's unix gate
```
