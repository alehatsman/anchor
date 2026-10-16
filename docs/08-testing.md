# 08 — testing

anchor's bet is a ported test suite (ROADMAP.md M1): the tests are the
product's claim to being a faithful port, so a test that passes without
checking anything costs more here than a missing test. This document is the
set of rules that were paid for with real defects, each one found by a green
fixture that was lying.

## A green test is a claim; check it can fail

Every fix here was **revert-checked**: deliberately reintroduce the defect
and confirm the test goes red, with a message that says what broke. A fix
that was never seen to fail is a fix you hope works.

Two tests in this suite passed with their bug reintroduced before anyone
checked. Both looked fine on review.

### A test that restates its own predicate passes for anything

`service.rs`'s build-output warning was asserted by a test that
re-implemented the same path check it was testing. Any predicate, correct or
not, satisfied it. The fix is to call the real function
(`is_build_output`), never a copy of its logic.

The same goes for asserting on strings a form was built from. The
`login_shell` wrapper is checked by **executing** each installed shell and
comparing the argv the child actually received (anchor#10), because an
assertion that the argv *equals* what the code produced is a tautology.

### A test must be able to reach its own premise

`terminate_is_not_wedged_by_a_stuck_write` was green from the day it was
written and never once tested what it named (anchor#9). It pushed 4 KiB
blobs of `x` into a child that never reads, expecting the writer to block.

**A tty in canonical mode discards an over-long line with no terminator**
rather than queueing it, so the writer never blocked: 436 MB absorbed by a
`sleep 30`, and `terminate()` racing nothing. Complete lines queue instead;
the canonical queue is ~1 KiB, so it fills at once and the next write to the
master blocks for real. See docs/03-pty.md#backpressure.

When a test depends on a condition it has to *create*, waiting for that
condition is part of the test, and failing to reach it must be loud:

```
writes never stayed rejected for 10s, so the pty writer thread never
wedged and this test would prove nothing
```

## Wait on the condition, not on the clock

Preconditions wait on the *condition*; assertions measure the thing they
*name* (anchor#11). A fixed sleep is a guess about a machine, and a
total-elapsed budget wrapped around a data transfer measures how fast this
box happens to be.

**The predicate, and it is the only portable part:** any progress resets the
budget, and only an unbroken stall fails. It applies to a producer that must
fill a backlog, a consumer that must drain one, and a writer that must wedge.

**Where the counter lives is a local question with a local answer.** In
anchor the session layer *returns an error* when the write channel stays
full, so the writer thread is the only thing that can observe a streak and
the counter belongs inside it — "the writer returned" and "the writer
wedged" are the same event. In a codebase whose channel blocks instead, the
writer can report nothing and the streak must be sampled from outside, which
then needs the worker to publish that it gave up — without that flag, a
worker that quit looks exactly like a worker that wedged, and the fixed test
is vacuous again. A rule that fixed the counter's location would break one
of the two.

Note also that a single rejected write is **not** a wedge. The channel fills
from slowness too. Only the unbroken streak distinguishes them.

## Three kinds of fixed number; one of them expires

| kind | example | expires? |
| --- | --- | --- |
| **condition wait** | poll until the `exit` frame arrives, bounded | no — a slow box just arrives later |
| **policy bound** | `terminate()` under 8s against a documented 7s policy | no — it measures a stated contract |
| **volume budget** | "this 10 MiB transfer finishes in 30s" | **yes** — sized against today's hardware |

Only the third is dangerous, and it is dangerous even with headroom:
headroom is a property of this year's machine, not of the test. A volume
budget with margin is not a safer volume budget, only an unexpired one.

**Audit the shared precondition helpers too.** A volume budget hidden inside
a helper is invisible to a search over test bodies, because every caller
looks like a function call. anchor's helpers are clean by construction:
`support::await_output` resets its deadline whenever the offset moves, and
`support::catch_up` is bounded by *rounds* rather than time — a round count
is a convergence bound and does not expire with hardware.

## Flakes do not show up on green runs

Five clean full-suite runs found none of four real flakes. Deliberate CPU
contention makes them deterministic:

```sh
PIDS=(); for i in $(seq 10); do (while :; do :; done) & PIDS+=($!); done
cargo test; kill "${PIDS[@]}"
```

**Collect `$!` explicitly; never `jobs -p`.** A non-interactive `zsh -c` has
no job table, so `jobs -p` returns nothing, the `kill` kills nothing, and
every run leaks ten spinners at PPID 1 that burn ~12% CPU each until the
machine reboots. This happened: 60 of them, for over two hours, silently
inflating every measurement taken in that window. **Verify a cleanup you
performed:**

```sh
ps -eo pid,ppid,command | grep "[w]hile :; do :; done" | wc -l
```

Fixing one instance of a harness defect does not close the class. After each
fix, search the suite for the *shape* — the `#11` total-elapsed pattern was
still live in `pty_primitive`'s `recv_until` after `#11` was closed.

## Publish the pair, not the green number

A single passing duration hides whether a fixture sits at 60% of its budget
or 115%. Record loaded and quiet side by side, say what "loaded" meant, and
keep the factor:

| suite | loaded | quiet | factor |
| --- | --- | --- | --- |
| `session_catchup` | 139.2s | 21.1s | 6.6x |
| `reconnect_storm` | 59.3s | 10.3s | 5.8x |
| `session_backpressure` | 52.3s | 8.8s | 5.9x |
| `pty_primitive` | 38.9s | 7.5s | 5.2x |
| `protocol_ops` | 6.3s | 5.6s | 1.1x |
| `socket_stress` | 5.8s | 5.0s | 1.2x |

*(2026-10-16; "loaded" meant ~60 leaked spinners, which is also how the
`jobs -p` trap above was discovered.)*

**Inflation factor is a cheap audit for volume dependence.** Contention
inflates throughput-dependent fixtures and leaves the rest alone — note
`protocol_ops` at 1.1x against `session_catchup` at 6.6x. Sorting a suite by
factor finds the tests with a throughput dependency faster than reading
them, including budgets that have *not* drifted yet. It tells you a fixture
depends on throughput; it never tells you its budget is wrong. Second
opinion, not replacement.

These same four suites are the four heaviest in teleport's suite as well,
arrived at independently. The pressure points belong to testing a PTY
multiplexer, not to either codebase — which is the reason this file exists
rather than living in one commit message.

## A comment is a claim with a date on it

Two of anchor's Windows deferrals cited teleport blockers that had been
fixed six weeks earlier — `#W1` in `src/pty.rs` and in `docs/03-pty.md`,
both reading as live obstacles long after `teleport#9` closed them. The
cost was not cosmetic: that citation was the strongest argument against a
scope decision, right up until someone checked the date.

Stale citations fail differently from stale code. Code that goes wrong
breaks a build or reds a test; a comment that goes wrong keeps compiling
and keeps persuading. Nothing in the toolchain checks whether
"unresolved", "unverified" or "not yet ported" is still true, so the claim
survives exactly as long as nobody looks.

Three habits, all cheap:

- **Date the claim, not just the note.** "Unverified on real Windows
  (2026-09-04)" invites a check. "Unverified on real Windows" invites
  belief.
- **Treat a cross-repo citation as the repeater's.** It stops being the
  other repo's problem the moment it is restated here as fact. Both of
  today's stale notes were accurate when written, in a repo that had since
  fixed them and not told anyone.
- **Prefer a reference that resolves to nothing over one that resolves to
  the wrong thing.** `S4` looked like GitHub issue #4; issue #4 exists, is
  closed, and is in the same subsystem on an adjacent date. A dangling
  reference makes a reader keep looking. A plausible one makes them stop.

The general form is the same failure this file is about. A test that cannot
fail by not running, and a comment that cannot fail by being out of date,
are both claims nothing checks — and both read as evidence.
