# Roadmap

Each milestone retires one risk and is independently abandonable. No milestone
starts before the previous one's exit criteria are met.

Ordering principle: **the risky subsystem first, the ergonomics last.** The
PTY layer is where this project lives or dies; everything else is plumbing
around it.

---

## M0 — Skeleton

Scope: single crate; `anchord` binds the socket and answers `ping`; SQLite
schema + forward-only migrations; data dir resolution; `tracing` from the
first commit.

Exit: daemon starts, a client connects, a session row round-trips. No PTY yet.

---

## M1 — PTY core, ported with its tests

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

## M2 — Labels and query

The reason the boundary moved.

Scope: `labels` table + index; `create` accepts labels; `label` mutates them;
`list` filters by exact match on any key; limits enforced (§02).

Exit: 500 sessions, filter by a label, correct results. A client can answer
"which session is for X?" with no side-table.

---

## M3 — Socket protocol and CLI

Scope: full control ops and binary stream framing per
[docs/04-protocol.md](docs/04-protocol.md); `anchor` CLI with raw-mode
passthrough attach and `~.` detach.

Exit: spawn a shell via CLI, detach, reconnect from a second terminal, see
scrollback and continue typing. Two observers plus one controller behave
correctly.

---

## M4 — Teleport on anchor, behind a flag

Scope: an anchor client in teleport; its session layer switchable; default
**off**.

Exit: **teleport's own test suite passes against the anchor path.** That is
the acceptance criterion for the whole extraction. Teleport's default is
unchanged and it ships normally throughout.

---

## M5 — Flip the default, prove the payoff

Scope: default teleport to anchor; packaging so `anchord` is a separate
launchd/systemd unit with its own lifecycle.

Exit: **update teleport while agents are running, and they survive.** That is
the thing anchor exists for. Demonstrate it, write it down, then delete
teleport's in-process PTY layer.

---

## Later, unscheduled

- Windows (blocked on teleport `#W1`)
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
```
