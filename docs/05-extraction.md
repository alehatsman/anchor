# 05 — Extraction from teleport

anchor is not a greenfield build. Teleport runs this subsystem today; the job
is to carve it out, widen it (labels), and put a socket in front of it.

**Treat teleport as the reference implementation.** When this spec and
teleport's behaviour differ, assume teleport is right — its rules came from
production bugs, these docs came from reading.

## What to take

From `teleport/daemon/src`:

```text
pty.rs                 804   take wholesale; the rules in 03-pty.md live here
session/manager.rs     769   take; widen with labels
session/replay.rs      630   take; this is catch-up-before-register
session/mod.rs         566   take
session/fanout.rs      340   take; the 8 MiB bound lives here
session/osc.rs         238   take if cheap — OSC title parsing is a nicety
session/types.rs       230   take; add labels
session/control.rs     157   take
log.rs                 585   take wholesale
persistence.rs       1,716   take the session half; leave the auth tables
```

## What to leave

```text
auth.rs                953   teleport product — passkeys, principals
auth_routes.rs         779   teleport product
auth_store.rs          708   teleport product
web_assets.rs          283   teleport product
embedded_web.rs         46   teleport product
device.rs               83   teleport product
api.rs               1,414   HTTP surface — anchor speaks a unix socket
ws.rs                  665   WebSocket framing — anchor has its own (04)
presets.rs             343   a UX concept; belongs to clients
```

Note `presets.rs`. "Named spawn configurations the user picks from a
launcher" is a client concern. anchor takes a `SpawnSpec`; teleport keeps its
presets and resolves them before calling.

## What to add

1. **Labels** — the whole reason the boundary moves. `session/types.rs:121-129`
   and `api.rs:597` show the gap: no arbitrary metadata, so every orchestrator
   keeps a side-table.
2. **Label-filtered list.**
3. **The socket protocol** ([04-protocol.md](04-protocol.md)) replacing HTTP + WS.
4. **The CLI.**

## Sequencing — do not break teleport

The safe order, and it matters:

1. **anchor ships standalone and is proven on its own** (ROADMAP M0–M3).
   Teleport is untouched throughout. No risk to a working product.
2. **teleport gains an anchor client behind a flag**, defaulting off. Both
   paths exist; the in-process one stays authoritative.
3. **Flip the default** once teleport's own test suite passes against anchor.
   Its tests are the acceptance criteria — `daemon/tests/` covers reconnect
   storms, session catch-up, backpressure, restart recovery, output logging,
   OSC titles and resume.
4. **Delete teleport's in-process PTY layer** only after a release has run on
   the anchor path.

Step 3 is the real gate. `teleport/daemon/tests/` is the most valuable asset
in this extraction — a behavioural spec for the subsystem, already written.
**Port those tests to anchor early** (ROADMAP M1) and they become anchor's
acceptance suite too.

## What this buys teleport

It can ship releases while agents run. Today it cannot:

> The updater therefore refuses to restart a daemon with running sessions.
> — teleport `docs/01-architecture.md`

After extraction, `anchord` is small and changes rarely; teleport's UI, auth
and packaging change weekly and restart freely.

## What it does not buy

**Crash survival.** If `anchord` dies, its PTYs die. The master fd has no
handoff. Teleport documented the fix (a broker process per PTY) and chose not
to pay for it; anchor inherits that choice. See
[01-scope.md](01-scope.md#open-questions).

Smaller and more stable is a real mitigation. It is not the same thing as
surviving a crash, and the README must not imply otherwise.
