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

**M0 — skeleton.** `anchord` resolves its data dir, migrates its SQLite
schema, recovers sessions left behind by a restart, and answers `ping` on a
unix socket. No PTY layer yet; that is M1, and it is the milestone this
project lives or dies on. See [ROADMAP.md](ROADMAP.md), [docs/](docs/) and
the open issues.

```sh
cargo test          # store round-trip, migrations, restart recovery, socket
cargo run --bin anchord
```

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

## Design

| doc | what |
|---|---|
| [docs/01-scope.md](docs/01-scope.md) | the closed list, and how to reject a feature |
| [docs/02-model.md](docs/02-model.md) | sessions, labels, lifecycle, storage |
| [docs/03-pty.md](docs/03-pty.md) | thread model, backpressure, termination |
| [docs/04-protocol.md](docs/04-protocol.md) | the unix socket API |
| [docs/05-extraction.md](docs/05-extraction.md) | how this gets carved out of teleport |
| [ROADMAP.md](ROADMAP.md) | milestones, each retiring one risk |

## License

MIT
