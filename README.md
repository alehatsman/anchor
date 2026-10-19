# anchor

A local daemon that owns long-running PTY processes so the things watching them
can come and go.

```text
   clients (come and go)              anchord (stays)
   ┌──────────────┐
   │ teleport     │──┐
   ├──────────────┤  │   unix socket   ┌──────────────────────┐
   │ workmanager  │──┼────────────────▶│ PTY: claude          │
   ├──────────────┤  │                 │ PTY: codex           │
   │ your tool    │──┘                 │ PTY: shell           │
   └──────────────┘                    │ append-only logs     │
                                       │ sessions.db          │
                                       └──────────────────────┘
```

Start an agent, close your terminal, update the UI you were watching it
through, come back an hour later from a different tool — the process never
noticed. That is the entire pitch.

## Status

**M0–M5 are done; the payoff is demonstrated. M6 — Windows — is scheduled**,
so that teleport can rely on anchor on every platform it ships rather than
keeping an in-process PTY layer alive for one of them.

`anchord` owns real PTY
sessions: four threads each, an append-only output log addressable by byte
offset, many bounded subscribers with backpressure, a control lease, and
termination that reaps a child ignoring SIGHUP and SIGTERM. Ported from
teleport along with its test suite, which is the point — those fixtures are
the behavioural spec.

Labels are queryable: filter sessions by exact match on any key, which is
the capability that moved this boundary out of teleport in the first place.
The socket speaks the full control protocol, and `anchor` drives it:

```sh
anchor create --label wm.item=mr:218 -- claude
anchor ls --label wm.item=mr:218
anchor attach <id>          # raw passthrough, ~. to detach
anchor logs <id> --from 0
anchor kill <id>            # terminate it
anchor purge <id>           # forget it: row, labels and output
```

Close the terminal, reconnect from another one, and the process never
noticed. See
[ROADMAP.md](ROADMAP.md), [docs/](docs/) and the open issues.

```sh
cargo test                # ported fixtures, protocol, CLI, restart recovery
                          # CI runs this on Linux and macOS both: they differ
                          # in ways the suite has already caught
cargo run --bin anchord   # or: anchor daemon

anchor service install    # register anchord with launchd/systemd, so it
                          # outlives every client (docs/07-lifecycle.md)
anchor service status
```

**No client should ever spawn `anchord`.** A daemon started as a child of a
client has the client's lifetime, which is the whole property anchor exists
to break. `anchor service install` registers it with the user's own service
manager instead — a launchd user agent on macOS, a systemd user unit on
Linux, both login-scoped and needing no root.

It is being extracted from [teleport](https://github.com/alehatsman/teleport),
which already runs this subsystem in production as part of a larger product.
Teleport is the reference implementation and the source of every hard-won
detail in these docs — read it alongside them.

## What it is

A **supervised, attachable, replayable PTY session**, and nothing else:

- **spawn spec** — argv, cwd, env, PTY size, login-shell wrapping
- **labels** — arbitrary key/value, owned by the caller
- **lifecycle** — `running → closing → exited`, plus `lost`
- **output log** — append-only, addressable by byte offset
- **subscribers** — many, bounded, with backpressure
- **control lease** — exactly one writer of input and size
- **reaping** — exit codes, independent of output EOF

Seven bullets. Everything not on that list belongs to a client.

## What it is not

**Not a process manager.** No restart policies, no dependency ordering, no
health checks, no resource limits. That is systemd, and systemd already
exists.

**Not Docker.** No images, no filesystem isolation, no networking, no
registry, no build. If a feature request starts with "like Docker, but…",
the answer is no.

**Not a terminal multiplexer.** No windows, no panes, no layouts, no copy
mode, no config language. tmux already exists and is better at it.

**Not remote.** Binds loopback. Reaching it across a network is a client's
job — teleport does exactly that, with its own auth and transport.

The closest honest analogy is **containerd**: a runtime extracted from a
product once the boundary was known, deliberately smaller than the thing it
came from.

## Why it exists

Two reasons, and the second is the one that forced it.

**1. More than one tool wants this.** teleport (web/mobile UI, remote access)
and [workmanager](https://github.com/alehatsman/workmanager) (a work-tracker
TUI that spawns agents on merge requests and issues) both need
spawn/attach/replay. Only one of them needs passkeys, device identity and an
embedded web app — which is 2,800 lines of teleport that the other would
otherwise pull through its dependency graph.

**2. teleport cannot ship a release while agents are running.** From its own
architecture doc:

> The update row is the one that bites in practice. A crash is rare; shipping
> a new version is routine, and it has the same blast radius. The updater
> therefore refuses to restart a daemon with running sessions.

The layer that changes weekly — UI, auth, remote access, packaging — is fused
to the layer that must never restart. Splitting them lets a small, boring,
rarely-changed daemon hold the PTYs while everything above it ships freely.

That second reason holds even if no other client is ever built.

**This now works, and it is demonstrated rather than claimed.** teleport's
`an_agent_survives_teleport_restarting_under_it` starts an agent, drops the
teleport that spawned it, starts a new one against the same `anchord`, and
requires the agent to still be running with its transcript intact. Its
updater restarts under load only when the daemon advertises
`sessions_survive_restart` on `/health`.

### The boundary, stated plainly

**If `anchord` dies, its PTYs die with it.** The master fd has no handoff.
anchor makes *updates* survivable by being small and changing rarely; it does
not make *crashes* survivable. teleport documented that fix — a broker
process per PTY — and deliberately declined to pay for it, and anchor
inherits the choice.

A restarted `anchord` is a clean daemon whose previous sessions are `lost`.
Nothing here should ever be read as more than that.

## Design

| doc | what |
|---|---|
| [docs/01-scope.md](docs/01-scope.md) | the closed list, and how to reject a feature |
| [docs/02-model.md](docs/02-model.md) | sessions, labels, lifecycle, storage |
| [docs/03-pty.md](docs/03-pty.md) | thread model, backpressure, termination |
| [docs/04-protocol.md](docs/04-protocol.md) | the unix socket API |
| [docs/05-extraction.md](docs/05-extraction.md) | how this gets carved out of teleport |
| [docs/06-teleport-seam.md](docs/06-teleport-seam.md) | where the two actually meet |
| [docs/07-lifecycle.md](docs/07-lifecycle.md) | who starts anchord, and what restart does not save |
| [docs/08-testing.md](docs/08-testing.md) | why a green suite is not evidence, and what to check instead |
| [docs/09-windows.md](docs/09-windows.md) | what a Windows port would cost, and why it is deferred |
| [ROADMAP.md](ROADMAP.md) | milestones, each retiring one risk |

## License

MIT
