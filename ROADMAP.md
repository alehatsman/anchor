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
- a 5s burst of output to a deliberately slow subscriber disconnects **that
  subscriber** and the PTY reader never stalls
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
the acceptance criterion for the whole extraction. Teleport's default is
unchanged and it ships normally throughout.

---

## M5 — Flip the default, prove the payoff — exit criteria met, 2026-10-16

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
> on `work_mac`. See teleport `docs/20-retiring-the-in-process-pty.md`.
>
> Open, and neither is anchor's code: flipping teleport's *compiled*
> `use_anchor` default (still `false` at `daemon/src/config.rs:140`), and
> deleting teleport's in-process PTY layer. The delete acquired two new
> gates once it stopped being reversible — a Windows transport arm for
> anchor (#16, deferred) and anchord present on the three hosts that do not
> have it yet.

Scope: default teleport to anchor; packaging so `anchord` is a separate
launchd/systemd unit with its own lifecycle.

Exit: **update teleport while agents are running, and they survive.** That is
the thing anchor exists for. Demonstrate it, write it down, then delete
teleport's in-process PTY layer.

---

## M6 — Windows, so teleport can rely on anchor everywhere

Scheduled 2026-10-19, reversing the deferral of 2026-10-16. The costing is
[docs/09-windows.md](docs/09-windows.md); both of its §7 questions are now
answered — anchord acquires an **owner-only DACL** on the named pipe rather
than a token, and Windows is in scope.

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

Risk retired, and it is a different shape from M1's. The hard part is
already free: ConPTY exit detection is solved and proven on real Windows in
teleport. What is unproven is breadth — the transport has **no prior art in
either repo**, and anchor acquires an access-control model it has so far
been able to decline to have. A Windows build where exits are unreliable is
worse than no Windows build, which is why step 1 is a runner and step 3 is
a tripwire rather than a count.

---

## Later, unscheduled

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
M5  flip        ← the payoff, demonstrated
M6  windows     ← the last platform teleport cannot yet hand over
```
