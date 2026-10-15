# 06 — The teleport seam

What M4 actually requires, from reading teleport rather than assuming. Read
with [05-extraction.md](05-extraction.md), which covers what moves; this
covers **where the switch goes and what is missing on the anchor side**.

Written before any teleport code changes. If it disagrees with teleport,
teleport is right — file an issue and fix this.

## The seam is not where the roadmap implies

ROADMAP's M4 says "an anchor client in teleport; its session layer
switchable". That reads as one swap behind `AppState.sessions`. It is not,
and the reason is worth stating before anyone starts.

Today `AppState.sessions` is a concrete `SessionManager`
([api.rs:48](https://github.com/alehatsman/teleport/blob/main/daemon/src/api.rs)),
and two layers reach through it to an `Arc<Session>` with **synchronous,
in-process methods**:

| caller | what it does with a `Session` |
|---|---|
| `api.rs` | `create`, `get`, `list`, `purge`, `root` |
| `ws.rs` | `attach` → a `Replay` it pumps, `subscribe_events`, `next_offset`, `attach_control` / `claim_control` / `release_control` / `is_controller` / `write_if_controller` / `begin_control_grace`, `resize` |

`session.attach(from)` hands back a `Replay` that `ws.rs` drives round by
round, holding the fan-out mutex each time. Over a unix socket there is no
`Replay` to hand back — catch-up happens **inside anchord**, and the client
receives frames. So the swap cannot sit under `Session`: the type's shape is
the in-process design.

**The seam belongs one layer up**, at the two places that own a whole
interaction:

1. `api.rs`'s handlers — each becomes one control request.
2. `ws.rs`'s attach loop — becomes a proxy: teleport's WebSocket on one side,
   anchor's `attach` stream on the other, translating frames.

That is a bigger change than "switch the manager", and it is the honest
scope of M4.

## What anchor does not have, and must not grow

Four things teleport's API serves today have no anchor equivalent, by
decision ([01-scope.md](01-scope.md) — they are client concerns):

| teleport field | source today | where it goes |
|---|---|---|
| `title` | `session/osc.rs` scanning the PTY stream | teleport, over the attach stream |
| `claude_resume_id` | same scanner | teleport, same place |
| `last_bell_ms` | reader loop watching for `0x07` | teleport, same place |
| `idle_since_ms` | a periodic sweep over live sessions | teleport, from its own stream timing |

All four are **derivable from the bytes anchor already streams**. Teleport
attaches anyway; scanning that stream costs it one pass it is already making
for the browser. None of them belongs in anchor: each is a policy about what
a UI wants to show, and a second client would want different ones.

This is the part of M4 that is new teleport code rather than moved teleport
code. Plan for it.

## The lease gap, which blocks part of M4

anchor's lease is deliberately smaller than teleport's:

| behaviour | teleport | anchor |
|---|---|---|
| holder identity | `client_id` + epoch, from the WS handshake | the connection itself |
| attach never preempts | yes | yes |
| explicit claim preempts | yes | yes |
| check-and-write atomically | yes | yes |
| **disconnect grace** | `control_grace_ms`, a window where the same `client_id` reclaims | **absent** |
| **`control_revoked` events** | broadcast to every other connection | **absent** |

The grace window exists because a browser's WebSocket drops on a phone
backgrounding or a network blip, and the user's own reconnect must not have
to fight for its own lease. Per-connection identity cannot express that:
the reconnect *is* a different connection.

Two of these must be settled before `ws.rs` can be a faithful proxy:

- **Lease identity on the wire.** Either `attach` grows a caller-supplied
  holder id (so a reconnect is the same client), or teleport keeps its own
  lease layer above anchor and anchor's lease governs only "which teleport".
- **Lease-change notification.** `control_revoked` tells a bystander who
  controls now. anchor sends a `lease` frame to the connection that lost it,
  but nothing to the others.

docs/04-protocol.md already defers lease handover to an open issue. M4 is
the consumer that forces the answer.

## Sequencing that does not endanger teleport

Unchanged from [05-extraction.md](05-extraction.md), restated against what is
above:

1. **anchor ships standalone and proven** — M0–M3, done. Teleport untouched.
2. **A client, then the seam.** Teleport gains an anchor client and the two
   seam points above, behind a flag defaulting **off**. Both paths exist;
   the in-process one stays authoritative.
3. **Flip the default** once teleport's own suite passes against the anchor
   path. Those tests are the acceptance criteria, not a smoke check.
4. **Delete teleport's in-process PTY layer** only after a release has run
   on the anchor path.

Teleport ships normally throughout. Nothing in step 2 changes its default
behaviour, which is what makes M4 safe to do incrementally.

## Open questions this raises

1. **Does teleport depend on the `anchor` crate, or write its own client?**
   The crate already exposes a blocking `client::Client`; teleport is
   async. Either anchor ships an async client, or teleport writes one over
   the documented protocol. A dependency couples their release cycles —
   which is the thing the split was meant to end.
2. **Lease identity**, above.
3. **Lease-change notification**, above.
