# 01 — Scope

The hardest thing about this project is keeping it small. This doc exists to
make refusal cheap.

## The closed list

anchor provides a **supervised, attachable, replayable PTY session**:

1. spawn spec — argv, cwd, env, PTY size, login-shell wrapping
2. labels — arbitrary key/value, owned by the caller, queryable
3. lifecycle — `running → closing → exited`, plus `lost`
4. output log — append-only, addressable by byte offset
5. subscribers — many, bounded, backpressured
6. control lease — exactly one writer of input and size
7. reaping — exit code, independent of output EOF

If a proposed feature is not on that list, the default answer is no.

## How to reject a feature

Ask: **can a client do this with what anchor already exposes?**

| request | answer | because |
|---|---|---|
| "restart the process if it dies" | no | client watches lifecycle and spawns again. That policy differs per client; baking one in is wrong for the others. |
| "run sessions on another machine" | no | transport and auth are a client concern. teleport already does this. |
| "group sessions into projects" | no | that is what labels are for. |
| "run a session without a PTY" | **maybe** | see open question below. Not in v1. |
| "tail the log with a filter" | no | client reads bytes from an offset and filters. |
| "schedule a session for later" | no | cron exists. |
| "limit memory/CPU" | no | that is a cgroup, and therefore systemd. |
| "web UI" | no | that is teleport. |
| "windows, panes, layouts" | no | that is tmux. |

The honest test: **if the answer involves policy, it belongs to a client.**
anchor provides mechanism.

## What anchor is allowed to be opinionated about

Only the things that cannot be correct in a client:

- **One effective PTY size.** A pseudoconsole has exactly one. Two clients at
  different widths must be arbitrated here, not negotiated between them.
- **Backpressure.** A slow subscriber must never slow the PTY reader. Only
  anchor is positioned to enforce this.
- **Byte offsets are authoritative.** A subscriber must never see an offset
  for bytes not yet on disk.
- **Exit codes.** Reaping has to happen exactly once, by the process holding
  the child handle.

## Non-goals, stated so nobody re-litigates

- not a process manager (systemd)
- not a container runtime (docker, containerd)
- not a terminal multiplexer (tmux, zellij)
- not a remote access product (teleport, ssh)
- not a scheduler (cron, systemd timers)
- not multi-user — one user, one daemon, loopback only

## Open questions

**Non-PTY sessions.** A client may eventually want a plain stdout/stderr
process with the same lifecycle, labels, and replay log, but no terminal.
That is a small generalization and a large scope risk — it invites "then why
not restart policies". Deferred past v1. Revisit only with a concrete
consumer.

**Crash survival.** If `anchord` dies, its PTYs die with it; the master fd
has no handoff. Teleport documented the fix (a broker process per PTY) and
deliberately did not pay for it. anchor inherits that decision. The reason
anchor exists is to make *updates* survivable by being small and stable —
not to make crashes survivable. Do not conflate the two.
