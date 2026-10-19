# 09 — Windows

Status: **deferred, 2026-10-16.** Windows is not being built. The owner
answered §7's Q2: not now.

Nothing here is implemented and nothing is scheduled. This document is kept
because the costing is the expensive part and it is done — not because the
work is queued. **anchord is Unix-only, deliberately, as of this date.**

§7's Q1 (does anchor acquire an access-control model?) is therefore
unanswered rather than decided. It would have to be answered before any of
this starts.

What would reopen it: a client that needs sessions on Windows and cannot
use a Unix host. teleport is the obvious candidate — its `use_anchor` path
cannot ship on Windows while this is deferred, which is a consequence worth
knowing before anyone deletes its in-process PTY layer (§8).

## Goal

anchord runs on Windows and hosts sessions there with the same guarantees it
makes on Unix — most importantly that a session's exit is observed exactly
once and reported accurately.

That last clause is the whole reason this is a spec rather than a port.
[01-scope.md](01-scope.md) lists exit codes among the four things anchor is
allowed to be opinionated about, because reaping has to happen exactly once,
by the process holding the child handle. A Windows build where exits are
unreliable would make a core invariant false on a platform nobody is
watching. That is worse than having no Windows build.

## Non-goals

- **Boot-scoped supervision.** Login-scoped only, matching
  [07-lifecycle.md](07-lifecycle.md).
- **Remote access.** Unchanged: still a client's job
  ([01-scope.md](01-scope.md)).
- **Parity of `login_shell`.** Windows has no login-shell convention. The
  flag is ignored rather than an error, so one `presets.toml` stays portable
  across a user's machines — teleport settled this at `pty.rs:191-215` and
  anchor should copy the decision, not re-litigate it.
- **A GUI, a service account, or an installer.** Out of scope.

## 1. What is already solved

The ConPTY work is done and proven, in teleport, against real Windows. It is
the part that could not have been estimated; everything else in this document
is breadth rather than depth. Inventory and provenance in #13.

The two repos' PTY layers are structurally near-identical — same
`try_clone_reader`/`take_writer`, same `mpsc::sync_channel::<Vec<u8>>`, same
four-thread layout — so this is lifting matching code into matching
structure:

| piece | teleport | anchor insertion point |
|---|---|---|
| `ConptyDsrProbe` | `daemon/src/pty.rs:444-520` | `src/pty.rs:350-353`, one `#[cfg(windows)]` line |
| Windows terminate arm | `daemon/src/pty.rs:683-690` | see §3 — **not** a straight copy |
| `drop(pair.slave)` gate | `daemon/src/pty.rs:284` | `src/pty.rs:341` |
| `login_shell_path` (COMSPEC) | `daemon/src/pty.rs:191` | `src/pty.rs`, alongside the csh arm |
| `pty_primitive_windows.rs` | `daemon/tests/` | `tests/` |

**The negative knowledge transfers too, and is the expensive part to
rediscover.** ConPTY has no raw input mode: every byte crosses conhost's VT
parser regardless of the child's console mode, so a byte-exact `0..256`
payload is unreproducible *by construction*. The Unix fixture asserting it
has no Windows equivalent and never will. The suite must also be serialized
or it oversubscribes its own runner.

## 2. The decision that gates everything

anchor's security model is one sentence, at `src/server.rs:4`:

> 0600: filesystem permissions are the security boundary, so there is no auth
> and no TCP.

It is enforced in `bind()` at `src/server.rs:139-145` — a `umask(0o077)`
around the bind, then an explicit `set_permissions(0o600)`. The data directory is `0700`
(`src/paths.rs:102`) because it holds verbatim terminal transcripts,
credentials included (`src/paths.rs:77`).

**A named pipe has no 0600.** Windows access control is a DACL on the pipe
object, and anchor would be acquiring an access-control model for the first
time — something it has so far been able to *decline* to have. That is a
scope change, not a port, and §7 asks it as a question rather than answering
it here.

Everything else in this spec is contingent on that answer.

## 3. Interfaces

### 3.1 The transport seam

There is **no prior art in either repo**. teleport's daemon is TCP
(`TcpListener::bind` in `bind_with_fallback`, `daemon/src/main.rs:370`),
which is portable for free, and its anchor client
is `UnixStream` (`daemon/src/anchor_client.rs:29`) — that *is* teleport#107.
Searched both repos including `spike/`: no `NamedPipe`, no `\\.\pipe`. This
is net-new design, and teleport needs the client half of it too.

Today the concrete type is spread across the client surface, not hidden
behind one seam (`src/client.rs:52,53,130,146,162,202`). The seam has to be
introduced before a second arm is possible:

- `src/server.rs` — `bind()` returns a `StdUnixListener` today and callers
  depend on that type.
- `src/client.rs` — `Client`, `FrameStream` and `Interrupt` each hold a
  `UnixStream` directly.

**Proposal:** a platform type alias pair rather than a trait.
`tokio::net::windows::named_pipe` and `UnixListener`/`UnixStream` do not
share a trait, but every use site here needs only read, write, clone, and
(see §4.1) cancel. A trait buys abstraction nothing calls for and costs
dynamic dispatch on the hot attach path. Two concrete types behind one alias,
with the platform differences confined to `client.rs`/`server.rs`, matches
how `pty.rs` already confines ConPTY.

The wire protocol itself is transport-agnostic: newline-delimited JSON, then
binary frames after `attach` ([04-protocol.md](04-protocol.md)). Nothing in
the framing assumes a socket. Mixed-endianness and all, it moves unchanged.

### 3.2 Pipe naming and discovery

Unix resolves `$XDG_RUNTIME_DIR/anchor.sock`, falling back to
`$XDG_DATA_HOME/anchor/anchor.sock`, treating a set-but-empty variable as
absent ([04-protocol.md](04-protocol.md)).

Windows needs its own rule, and it must be **per-user**, because the pipe
namespace is machine-global. A single `\\.\pipe\anchor` is wrong on a
multi-user box: two users' anchords would collide, and whichever bound first
would serve the other's requests.

Naming and the `--socket` override's Windows meaning are part of the §7
decision, since both depend on the access-control answer.

## 4. Edge cases

### 4.1 `Interrupt::stop()` has no named-pipe equivalent

`src/client.rs:208` ends a blocked read from another thread with
`UnixStream::shutdown(Shutdown::Both)`. This is what lets a client detach
from a quiet session instead of hanging on output that may never come.

Named pipes have no `shutdown`. The Windows path needs `CancelIoEx` against
the pipe handle, or overlapped I/O with an event. **This is the one place
where the Unix design does not merely need porting but needs a different
mechanism**, and it is on the detach path, which is user-visible and
currently works.

### 4.2 Stale-socket replacement is dangerous on Windows

On Unix, anchord probes an existing socket and replaces it when nothing
answers (`bind()` at `src/server.rs:90`, the probe at `:105-117`) — correct,
because a stale socket file is not a conflict.

**Do not port this behaviour.** On Windows the equivalent is pipe squatting:
a malicious local process can create `\\.\pipe\<name>` before anchord starts
and serve clients in its place, receiving spawn requests and session output.
The server must create with `FILE_FLAG_FIRST_PIPE_INSTANCE` and **fail** if
the name is taken, rather than probing and replacing.

The client has the mirror problem: connecting to a pipe someone else owns.
It should verify the server's identity before trusting it.

### 4.3 Impersonation

A named-pipe server can impersonate a connecting client. anchord has no
reason to and must not be able to: clients should connect at
`SECURITY_IDENTIFICATION` rather than the default, so a compromised or
squatted server cannot act as the user who connected to it.

### 4.4 The terminate arm conflicts with anchor's existing comment

anchor's step-2 comment (`src/pty.rs:615`) states:

> Not dropping the master — the reader and writer hold their own dups of the
> same open file description, so dropping it closes nothing and raises no
> signal.

True on Unix and **exactly backwards on Windows**, where dropping the master
*is* the mechanism (ClosePseudoConsole, verified 2026-09-05, teleport#9/S4).
The comment must be split by platform, not extended.

Mechanically, anchor moves `master` into `control_thread_main`
(`src/pty.rs:573`); teleport wraps it `let mut master = Some(master)` so
`.take()` works, with a note on why that needs `#[allow]` rather than
`#[expect]` (whether `unused_mut` fires is itself platform-dependent).

anchor has diverged *in its favour* here: it factored platform logic into
`pid_to_signal`, `graceful_signal_group` and `hard_kill_group`, where
teleport inlines `cfg` blocks in the control loop. The Windows arm should be
Windows implementations of those three, keeping the loop platform-free.

### 4.5 The CLI blocks on `libc::poll`

`src/bin/anchor.rs:461` polls `STDIN_FILENO` directly. Windows needs a
console-input path (`ReadConsoleInput` or a reader thread). Independent of
the transport work, and required before `anchor attach` works at all.

### 4.6 Directory permissions

`0700` via `PermissionsExt` appears at `src/paths.rs:102`, `src/log.rs:454`
(already `#[cfg(unix)]`-gated) and `src/session/manager.rs:607`. The claim at
`src/paths.rs:77` — that transcripts are not readable by other users — must
hold on Windows via an ACL, or the claim has to be narrowed in the doc. It
may not silently become false.

### 4.7 Autostart is unsolved, including upstream

`src/service.rs` is macOS + Linux only (`src/service.rs:107` and `:257`).
teleport has a
40-line Task Scheduler scaffold (`desktop/src-tauri/src/autostart/windows.rs`)
that **does not work yet**: attempted 2026-09-06 on real Windows 11 build
26200, every task-creation attempt returned `Access is denied`
(teleport `docs/11-mvp-plan.md:1042`). Assume no donor.

## 5. Validation

The suite must not be able to pass by not running. teleport#108 is the worked
example: `anchor_http` reported `ok. 17 passed` in 0.01s having executed
nothing, because gated tests early-return and a returning test is a passing
test. See [08-testing.md](08-testing.md).

So, in order, and each step gated on the previous:

1. **A Windows runner exists in CI** and executes a deliberately failing test.
   Nothing else starts until a red is observable.
2. **Port the PTY layer** (§1) behind `#[cfg(windows)]`, with
   `pty_primitive_windows.rs`. The suite must be serialized (§1).
3. **Assert the port is covered**: an ungated assertion that fails when the
   Windows fixtures do not execute, mirroring teleport's `5db5fbd`. A count
   gate is insufficient — the count is never zero.
4. **Transport** (§3), **CLI input** (§4.5), **permissions** (§4.6).
5. **Autostart** (§4.7), or state that Windows has no autostart story.

Step 3 before step 4 is deliberate: the PTY layer is the part carrying the
invariant, and it should be defended before breadth is added on top of it.

teleport's deletion spec holds `ConptyDsrProbe` and `pty_primitive_windows.rs`
as transfers rather than deletions, and will not release them until the
behaviour is **under test in anchor**. That gate is this document's step 3.

## 6. What this does not buy

Adopting teleport's Windows PTY code does not get anchor to Windows. It
removes the one risk that could not be estimated. The remainder — a transport
with no donor, an access-control model anchor has never had, a console-input
path, and an autostart story that is unsolved upstream — is broad rather than
deep, and none of it is started.

## 7. Open questions — owner's call

**Q1. Does anchor acquire an access-control model?**

Today `src/server.rs:4` can say "there is no auth" and mean it, because the
filesystem says it instead. On Windows that sentence needs a DACL behind it.
Three options:

- **(a) Explicit owner-only DACL.** The pipe is created with a DACL granting
  the creating user's SID and nothing else. Closest in spirit to `0600`, and
  keeps "the OS is the boundary, not anchor" true. Costs a Windows security
  descriptor in `server.rs` and the §4.2/§4.3 hardening.
- **(b) Default pipe security.** Cheapest, and wrong: the default grants more
  than the owner, which would make the `paths.rs:77` transcript claim false
  on Windows.
- **(c) A token, like teleport's.** Portable and uniform, but it is anchor
  acquiring authentication — a far larger scope change than (a), and it would
  make the Unix path's "no auth" claim obsolete too.

**Recommendation: (a).** It preserves the existing position — the OS enforces
access, anchor does not authenticate — rather than replacing it. (c) is the
only option that changes what anchor *is*.

**Q2. Is Windows in scope at all, given §6?**

The honest framing: the hard part is free, the broad part is not started, and
the autostart story is blocked upstream on a real `Access is denied`. A
defensible answer is "anchord is unix-only, stated deliberately" — which is
also an answer teleport needs, since its own `use_anchor` path cannot ship on
Windows without this work.

This document does not recommend one. It exists so the question is asked with
the costs visible rather than discovered during a port.

## 8. Consequence for teleport

Deferring this is not neutral downstream, and the dependency runs the other
way from how it looks.

teleport's in-process PTY layer is its only **possible** Windows session
path while anchord is Unix-only. Possible, not working: `teleportd` does not
currently compile on Windows at all.

Checked rather than assumed, 2026-10-16. teleport's CI builds
`windows-latest` (the `daemon` job's matrix,
`.github/workflows/ci.yml:56-62`) and the `daemon
(windows-latest)` job is red with `error[E0432]: unresolved import
tokio::net::UnixStream` at `daemon/src/anchor_client.rs:29`. That is
teleport#107, and it is a build break rather than a missing feature.

The sequence matters: teleport's Windows PTY work is **proven** — W1 and S4
were both verified on real Windows 11 build 26200, and
`pty_primitive_windows.rs` ran there. The anchor client is what broke the
build, by declaring four ungated `anchor_*` modules in
`daemon/src/lib.rs:28-33`. anchor's integration took teleport's Windows build
down, and this deferral leaves it down.

**This also reverses the argument against the obvious fix.** `#[cfg(unix)]`
on the anchor modules was rejected as self-defeating, correctly, *while the
deletion was going ahead* — gating them is only right if the in-process layer
stays as the Windows path, and the deletion would have removed it. With
Windows deferred here, the in-process layer stays. The gate is now simply the
fix, and it needs nothing from anchor.

So the options are narrower than they look:

- **teleport keeps its in-process layer as the Windows path**, cfg-gates the
  anchor modules, and the deletion becomes a Unix-only deletion rather than a
  removal. This restores a build that used to work and depends on nothing
  here.
- **teleport states that teleportd is Unix-only too**, and the deletion
  proceeds as specified — in which case the Windows CI job should be dropped
  rather than left red, since a permanently failing job is a gate nobody can
  read.

Both are teleport's call, not anchor's. What anchor owes them is the fact,
not the choice: anchord will not host a session on Windows, and no date is
attached to that changing.

One practical consequence. teleport's deletion spec holds
`ConptyDsrProbe` and `pty_primitive_windows.rs` as transfers rather than
deletions, released only once the behaviour is under test in anchor (§5,
step 3). That gate is now indefinite. If teleport deletes its in-process
layer and anchor never ports the probe, that code should be preserved
deliberately — it is the only proven ConPTY exit-handshake work in either
repo, and §1's negative knowledge is not recoverable by reading the code
that remains.
