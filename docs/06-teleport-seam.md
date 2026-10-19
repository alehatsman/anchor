# 06 — The teleport seam

What M4 actually requires, from reading teleport rather than assuming. Read
with [05-extraction.md](05-extraction.md), which covers what moves; this
covers **where the switch goes and what is missing on the anchor side**.

Status, 2026-10-19: written before any teleport code changed; M4 and M5
have since shipped, so read the analysis below as the reasoning that led to
them, not a forecast. If it disagrees with teleport, teleport is right —
file an issue and fix this.

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

## The lease gap

M4 and M5 both shipped without this being settled, so it did not block them
— teleport kept its own lease layer above anchor's. One of the two gaps has
since closed.

anchor's lease is still deliberately smaller than teleport's:

| behaviour | teleport | anchor |
|---|---|---|
| holder identity | `client_id` + epoch, from the WS handshake | the connection itself |
| attach never preempts | yes | yes |
| explicit claim preempts | yes | yes |
| check-and-write atomically | yes | yes |
| **disconnect grace** | `control_grace_ms`, a window where the same `client_id` reclaims | **absent** |
| `control_revoked` events | broadcast to every other connection | broadcast to every attached connection |

**Lease-change notification is no longer a gap.** Every attached connection
is told when the lease moves, not only the two involved
([04-protocol.md](04-protocol.md)): `Session::watch_lease` hands each stream
a `broadcast::Receiver` and `src/stream.rs` subscribes before announcing the
attach's own lease decision, so a bystander learns control is taken rather
than discovering it when its keystrokes stop working.

**Disconnect grace remains absent, and is the one that needs an answer.**
The grace window exists because a browser's WebSocket drops on a phone
backgrounding or a network blip, and the user's own reconnect must not have
to fight for its own lease. Per-connection identity cannot express that:
the reconnect *is* a different connection.

So one thing is left to settle before `ws.rs` could be a thin proxy rather
than a lease layer in its own right:

- **Lease identity on the wire.** Either `attach` grows a caller-supplied
  holder id (so a reconnect is the same client), or teleport keeps its own
  lease layer above anchor permanently and anchor's lease governs only
  "which teleport". The second is what shipped; it is a live arrangement
  rather than a decision anyone made.

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

1. **Lease identity**, above. Still live: teleport keeps its own lease
   layer on top of anchor's.

Decided and shipped, no longer open: teleport writes its own client over the
documented protocol (`daemon/src/anchor_client.rs`) and has no `anchor`
dependency, so the two release cycles stay uncoupled; and lease-change
notification is no longer a gap (see the lease table above).
