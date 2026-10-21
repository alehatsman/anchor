# 09 — Windows

Status: **done, merged to main 2026-10-20 (#33); every gate run on a real
Windows 11 Pro box (build 26200), as a standard non-elevated user, and the
Windows CI job green on GitHub.** Scheduled as M6 on 2026-10-19, reversing
the deferral of 2026-10-16. Everything below that says "verified" means
verified on that box, by the commands or tests named beside it; the CI job
runs the same steps, and its first runs caught two things the box could not
(§5, gate 1).

| §5 gate | state | how it was checked |
|---|---|---|
| 1. a Windows runner that can go red | done | its steps run locally: `cargo clippy` failed with 39 errors before any port; on GitHub, red twice on the PR, then green |
| 2. the PTY layer | done | `tests/pty_primitive_windows.rs`, 12 fixtures, real ConPTY; revert-checked |
| 3. the fixtures cannot pass by not running | done | a ledger plus an ungated checker; red on an early return and on a gated-out module |
| 4. transport, console input, permissions | done | `tests/windows_transport.rs`, `client.rs` unit tests, `icacls`; revert-checked |
| 5. autostart | done, one part unobserved | `anchor service install` as a standard user; the logon trigger itself has not been seen to fire |

What "done" bought, in one line: `anchord` runs on Windows, `anchor`
creates a `cmd.exe` or `powershell.exe` session over the pipe, detaches
with `~.` from it while it is quiet (5–7 ms), reattaches and sees the
scrollback, and observes its exit exactly once with the code the child
exited with — including an `NTSTATUS` above `i32::MAX`. Windows suite 90
passed, 0 failed, 1 ignored (the ledger check, run after it: 12 of 12);
Linux (WSL Ubuntu 24.04) green at 185 passed, 0 failed, 1 ignored.

The reopen condition §8 named was met by decision rather than by
discovery: **teleport is to rely on anchor fully, on every platform it
ships.** That made anchord's Unix-only status the thing blocking teleport's
in-process PTY layer from ever being deleted, rather than the thing
excusing it.

**Both of §7's questions are answered, and the answers held:**

- **Q1 — does anchor acquire an access-control model? Yes: option (a), an
  explicit owner-only DACL.** The pipe is created with a DACL granting the
  creating user's SID and nothing else. This preserves anchor's existing
  position rather than replacing it — the OS enforces access, anchor does
  not authenticate — which is why it was preferred over a token. `0600` and
  an owner-only DACL are the same sentence in two kernels' vocabularies.
  Read back from the kernel: `O:<sid>D:P(A;;FA;;;<sid>)`.
- **Q2 — is Windows in scope? Yes**, as M6, sequenced per §5 and gated step
  by step — which is how it landed, one commit per gate.

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
| `ConptyDsrProbe` | `daemon/src/pty.rs:444-520` | `spawn` in `src/pty.rs`, one `#[cfg(windows)]` line |
| Windows terminate arm | `daemon/src/pty.rs:683-690` | see §3 — **not** a straight copy |
| `drop(pair.slave)` gate | `daemon/src/pty.rs:284` | the `drop(pair.slave)` in `spawn`, `src/pty.rs` — kept on both platforms, see §4.4 |
| `login_shell_path` (COMSPEC) | `daemon/src/pty.rs:191` | `src/pty.rs`, alongside the csh arm |
| `pty_primitive_windows.rs` | `daemon/tests/` | `tests/` |

### Why `ConptyDsrProbe` exists

Worth recording, because the fix does not look like the symptom. A
gracefully-exiting ConPTY child was once never observed as exited — teleport's
`#W1`, fixed 2026-09-05 in teleport#9.

The root cause was never reaping. conhost's ConPTY startup handshake writes a
cursor-position query (`ESC[6n`) to the pty and blocks its IO thread waiting
for a reply. The child had already exited and conhost was still mid-handshake,
so nothing tore down. Answering that one query returns `wait()` in single-digit
milliseconds.

`ConptyDsrProbe` answers it and then **stops** answering, which is the subtle
half: a probe that kept replying would corrupt every application that asks for
a cursor position of its own.

So Windows is a port, not a research problem.

**The negative knowledge transfers too, and is the expensive part to
rediscover.** ConPTY has no raw input mode: every byte crosses conhost's VT
parser regardless of the child's console mode, so a byte-exact `0..256`
payload is unreproducible *by construction*. The Unix fixture asserting it
has no Windows equivalent and never will. The suite must also be serialized
or it oversubscribes its own runner.

## 2. The decision that gates everything

anchor's security model is one sentence, in `src/server.rs`'s module doc:

> 0600: filesystem permissions are the security boundary, so there is no auth
> and no TCP.

It is enforced in `bind()` (`src/server.rs`) — a `umask(0o077)`
around the bind, then an explicit `set_permissions(0o600)`. The data directory is `0700`
(`create_owner_only`, `src/paths.rs`) because it holds verbatim terminal
transcripts, credentials included (`Paths::ensure_dirs`).

**A named pipe has no 0600.** Windows access control is a DACL on the pipe
object, so anchor had to acquire an access-control model for the first time —
something it had so far been able to *decline* to have. That was a scope
change rather than a port, which is why it gated everything else here.

**Decided 2026-10-19, and shipped: option (a), an explicit owner-only DACL**
— the pipe is created granting the creating user's SID and nothing else
(`src/winsec.rs`, `src/server.rs`). The model it acquires is the one it
already had: the OS enforces access and anchor still does not authenticate.
`0600` and an owner-only DACL are the same sentence in two kernels'
vocabularies, which is why this was preferred over a token check. §7 carries
the question, the options weighed, and why this one won.

## 3. Interfaces

### 3.1 The transport seam

There is **no prior art in either repo**. teleport's daemon is TCP
(`TcpListener::bind` in `bind_with_fallback`, `daemon/src/main.rs:370`),
which is portable for free, and its anchor client
is `UnixStream` (`daemon/src/anchor_client.rs:29`) — that *is* teleport#107.
Searched both repos including `spike/`: no `NamedPipe`, no `\\.\pipe`. This
is net-new design, and teleport needs the client half of it too.

Today the concrete type is spread across the client surface, not hidden
behind one seam (`src/client.rs`). The seam has to be
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

**Landed as proposed.** `server.rs` aliases `Listener`, `ConnRead` and
`ConnWrite`; `client.rs` aliases `Stream`. The Windows `PipeStream` offers
the `UnixStream` methods `client.rs` calls — `connect`, `try_clone`,
`shutdown`, `Read`/`Write` on `&Self` — under the same names, which is what
let `Client`, `FrameStream` and `Interrupt` stay free of conditionals;
`stream.rs` changed only the names of its two halves. "Cancel" turned out
to be `shutdown` itself (§4.1).

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

**Decided 2026-10-19: `\\.\pipe\anchor-<SID>`**, the string form of the user
SID in the process token. The client-facing contract — name, how to open
it, how to verify it — is [04-protocol.md](04-protocol.md#windows-a-named-pipe),
written for a client author (teleport's is the first) rather than for this
codebase.

Why the SID and not the user name: a name is not unique across domains or
after a rename, and it is the one thing a squatter can predict *and* pick
freely. The SID is neither. Predictability is not the defence, though —
§4.2's fail-if-taken and the client's owner check are. The name only has to
be distinct per user, and the SID is the one identifier Windows itself
treats as the user.

No environment variable moves it. Unix needs `XDG_RUNTIME_DIR` because a
socket is a file and has to live somewhere writable; a pipe has no
directory. `--socket` keeps its meaning on Windows as "the endpoint", takes
a full `\\.\pipe\…` path, and refuses anything else — a filesystem path
there would be a mistake, not a choice. Both the name and that `--socket`
meaning were settled with the rest of §7's Q1 and are implemented:
`Paths::resolve` builds the name (`src/paths.rs`) and `bind` rejects
anything without the `\\.\pipe\` prefix (`src/server.rs`).

**Server identity is the pipe's owner.** anchord writes its own user SID as
the owner of the pipe's security descriptor; the client reads the owner off
the handle it opened and requires it to equal its own SID. Chosen over
asking for the server's process token (`GetNamedPipeServerProcessId` →
`OpenProcessToken`), because an elevated process's token is not normally
open to a non-elevated one, and that check would then refuse a legitimate
daemon. That is reasoning, **not checked here**: it needs an elevated
anchord, and none was run. The owner check does not depend on it either
way, since anchord writes the owner explicitly rather than taking an
elevated token's default (the Administrators group). Creating a pipe owned
by another user needs a privilege (`SeRestorePrivilege`) that already owns
the machine.

## 4. Edge cases

### 4.1 `Interrupt::stop()` has no named-pipe equivalent

`Interrupt` (`src/client.rs`) ends a blocked read from another thread with
`UnixStream::shutdown(Shutdown::Both)`. This is what lets a client detach
from a quiet session instead of hanging on output that may never come.

Named pipes have no `shutdown`. The Windows path needs `CancelIoEx` against
the pipe handle, or overlapped I/O with an event. **This is the one place
where the Unix design does not merely need porting but needs a different
mechanism**, and it is on the detach path, which is user-visible and
currently works.

**Resolved: both, and the overlapped half turned out to be load-bearing on
its own.** A pipe handle opened for synchronous I/O serialises every
operation on it, so the attach path's read, blocked on a quiet session,
would hold the keystroke writes on the other thread behind it — the CLI
could not even send `~.`. `PipeStream` (`src/client.rs`) is overlapped: each
read and write waits on its own completion event *and* a stop event shared
by every clone, and `shutdown` — the same method name `UnixStream` has, so
`Interrupt::stop` did not change — sets the stop event; the blocked read
then cancels its own I/O (`CancelIoEx`) and waits for the cancel to land
before its buffer is released. Pinned by
`a_stop_ends_a_read_blocked_on_a_quiet_pipe` and
`a_write_is_not_held_behind_a_blocked_read`; with `FILE_FLAG_OVERLAPPED`
removed both go red (the write times out behind the read; stop never
returns).

### 4.2 Stale-socket replacement is dangerous on Windows

On Unix, anchord probes an existing socket and replaces it when nothing
answers (`MAX_SOCKET_PATH` and the staleness probe in `bind()`,
`src/server.rs`) — correct,
because a stale socket file is not a conflict.

**Do not port this behaviour.** On Windows the equivalent is pipe squatting:
a malicious local process can create `\\.\pipe\<name>` before anchord starts
and serve clients in its place, receiving spawn requests and session output.
The server must create with `FILE_FLAG_FIRST_PIPE_INSTANCE` and **fail** if
the name is taken, rather than probing and replacing.

The client has the mirror problem: connecting to a pipe someone else owns.
It should verify the server's identity before trusting it.

**Resolved.** `bind` creates the first instance with
`FILE_FLAG_FIRST_PIPE_INSTANCE` and fails on a taken name; it then probes
with the client's own owner check, only to *say* whether the holder is
this user's anchord ("already running") or not ("not this user's anchord
… not replacing it"). The accept loop creates the next instance before
handing the connected one off, so the name is never free between
connections. The client reads the pipe's owner off the handle it opened
and refuses anything not owned by itself, before sending a byte.

Checking the flag was worth it, because the case it exists for is not the
obvious one. A squatter that denies this user is refused with or without
the flag — creating another instance needs rights it does not grant. A
squatter that grants **everyone** is the dangerous one: without the flag,
`CreateNamedPipeW` on its name succeeds by *joining its pipe*, and clients
reach either process at the kernel's choosing. With the flag removed,
`a_permissive_squatter_is_not_joined` went red — and so did
`a_second_daemon_is_refused_while_the_first_serves`, because a second
anchord silently joined the first one's pipe instead of being refused.

What the suite cannot do is create a pipe owned by another user: that
needs a privilege a test run does not hold. The client's comparison is
therefore driven by telling it to expect SYSTEM against a pipe this user
owns (`a_pipe_owned_by_someone_else_is_refused`), and the matching side
by reading back the real pipe's owner.

### 4.3 Impersonation

A named-pipe server can impersonate a connecting client. anchord has no
reason to and must not be able to: clients should connect at
`SECURITY_IDENTIFICATION` rather than the default, so a compromised or
squatted server cannot act as the user who connected to it.

**Resolved, and asked of the kernel.**
`the_server_can_identify_the_client_but_not_impersonate_it` impersonates
from the server end of a real pipe and reads the thread token's level back:
`SecurityIdentification`. With the QoS flags removed from the client's
open, the same test reads a higher level and fails. anchord itself never
calls `ImpersonateNamedPipeClient`.

### 4.4 The terminate arm conflicts with anchor's existing comment

anchor's step-2 comment (in `control_thread_main`, `src/pty.rs`) states:

> Not dropping the master — the reader and writer hold their own dups of the
> same open file description, so dropping it closes nothing and raises no
> signal.

True on Unix and **exactly backwards on Windows**, where dropping the master
*is* the mechanism (ClosePseudoConsole, verified 2026-09-05, teleport#9/S4).
The comment must be split by platform, not extended.

Mechanically, anchor moves `master` into `control_thread_main`
(`src/pty.rs`); teleport wraps it `let mut master = Some(master)` so
`.take()` works, with a note on why that needs `#[allow]` rather than
`#[expect]` (whether `unused_mut` fires is itself platform-dependent).

anchor has diverged *in its favour* here: it factored platform logic into
`pid_to_signal`, `graceful_signal_group` and `hard_kill_group`, where
teleport inlines `cfg` blocks in the control loop. The Windows arm should be
Windows implementations of those three, keeping the loop platform-free.

**Resolved that way.** The loop holds `Option<master>` on both platforms and
passes it to `graceful_signal_group`, whose Unix version leaves it alone
and whose Windows version drops it; the step-2 comment lives on the two
functions rather than being shared. No `#[allow]` was needed after all:
`&mut master` is taken on both platforms, so `unused_mut` fires on neither.
The hard kill is `TerminateProcess` by pid, which is safe for the reason
the Windows `pid_to_signal` documents — the reaper's handle pins the pid
until `reaped` is published.

**Revert-checking found a hole in the donor's coverage.** With the master's
drop removed from the graceful step, every one of teleport's fixtures
stayed green: the hard kill ends the child at 5 s, the policy allows 7 s,
and the control thread drops the master on its way out anyway. The paths
differ in what the child reports — an `NTSTATUS` (`0xC0000142` for `ping`
under `cmd /c`) in about 2 ms against the hard kill's `1` at 5 s — so
`terminate_closes_the_pseudoconsole_rather_than_hard_killing` asserts on
that, and goes red without the drop.

### 4.5 The CLI blocks on `libc::poll`

`stdin_readable` (`src/bin/anchor.rs`) polls `STDIN_FILENO` directly. Windows needs a
console-input path (`ReadConsoleInput` or a reader thread). Independent of
the transport work, and required before `anchor attach` works at all.

**Resolved with a reader thread** (`src/tty/windows.rs`), behind one
`Keyboard` interface the attach loop calls on both platforms. Not a wait on
the console handle: it is signalled by focus and mouse records that
`ReadFile` then discards while it blocks, so wait-then-read can block
anyway. Raw mode is three console-mode bits plus
`ENABLE_VIRTUAL_TERMINAL_INPUT` (so arrows arrive as the VT sequences
ConPTY expects) and UTF-8 code pages, restored on drop and from a console
control handler on close or logoff. Output gets
`ENABLE_VIRTUAL_TERMINAL_PROCESSING` for observers too — a Windows console
interprets escape sequences only when asked — and goes out as raw bytes,
because `std::io::stdout()` on a console refuses bytes that are not valid
UTF-8. There is no `SIGWINCH`; the size is compared on each loop wakeup,
the same latency the Unix flag already had.

**Moving the Unix half found a Unix bug.** It read through
`std::io::stdin()`, an 8 KiB `BufReader`: a paste over 1 KiB filled that
buffer in one read, the loop forwarded 1 KiB, and the rest sat where `poll`
on the fd cannot see it until the next keystroke. The Unix keyboard now
reads the fd; `a_paste_longer_than_one_read_arrives_whole` is green, and
red (stuck for its full 30 s) with the old read put back.

Verified by hand in a real console as well as by test: the scripted
attach in `detach_reattach_and_one_exit_through_the_cli` has stdin on a
pipe, where raw mode is correctly skipped.

### 4.6 Directory permissions

`0700` via `PermissionsExt` appears in `create_owner_only` (`src/paths.rs`)
and `create_dir_owner_only` (`src/log.rs`, already `#[cfg(unix)]`-gated).
The claim on `Paths::ensure_dirs` — that transcripts are not readable by other users — must
hold on Windows via an ACL, or the claim has to be narrowed in the doc. It
may not silently become false.

**Held, via an ACL.** `%LOCALAPPDATA%\anchor` gets a protected DACL with one
inheritable ACE for this user (`D:P(A;OICI;FA;;;<sid>)`, `winsec.rs`), and
the database and every transcript inherit it: `icacls` shows exactly
`<user>:(OI)(CI)(F)` on the directory and `<user>:(I)(F)` on `anchor.db` and
`output.vt`. Without it the directory inherits SYSTEM and Administrators
from its parent — `the_data_dir_is_owner_only_on_windows` goes red saying
so. The claim's honest limit is the same as on Unix: an administrator can
take ownership and read anything, as root can.

### 4.7 Autostart is unsolved, including upstream

`src/service.rs` is macOS + Linux only (its two `platform` modules).
teleport has a
40-line Task Scheduler scaffold (`desktop/src-tauri/src/autostart/windows.rs`)
that **does not work yet**: attempted 2026-09-06 on real Windows 11 build
26200, every task-creation attempt returned `Access is denied`
(teleport `docs/11-mvp-plan.md:1042`). Assume no donor.

**Solved here, and the cause of the upstream failure found.** On the same
build, as a standard user, `schtasks /Create /SC ONLOGON` reproduces
`Access is denied` exactly — that trigger fires for *any* user's logon,
which needs elevation. A `LogonTrigger` scoped to this user's SID, imported
as XML (or through `Register-ScheduledTask -User`), registers without it.
`anchor service install` (`src/service.rs`) is that, plus what checking
turned up:

- the action is `conhost.exe --headless anchord.exe`: no window, so nothing
  a user can close to end every session (anchord's parent observed as a
  windowless conhost);
- `RestartOnFailure` does **not** revive a killed anchord — the task just
  went back to Ready — so revival is a one-minute repetition under
  `IgnoreNew`: killed, answering again 10 s later at the next minute;
- `/Create /F` over a running task leaves the running anchord alone, so a
  changed registration applies at the next start, as on Linux, and an
  unchanged one is not re-imported at all;
- `/End` stops the headless conhost but **not** anchord, which keeps
  serving as an orphan; `uninstall` therefore stops anchord itself, found
  as the server of this user's own owner-verified pipe.

The part not observed: the logon trigger firing at an actual logon, which
cannot be done from inside the only logon session. Registration, the
action, the principal and the watchdog all were.

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

### As it landed, 2026-10-20

Each gate is its own commit on `feat/windows-m6`, in this order, and each
was run on the Windows box before the next began. The branch merged as #33.

1. **The runner.** `.github/workflows/ci.yml`'s `windows` job. Before any
   port its steps failed on this box at `cargo clippy` — 39 compile errors —
   which is a red, but one from the build rather than from an executed
   test. That is the one way this gate is weaker than written: the lib did
   not compile on Windows, so no test could run to fail until step 2.
   On GitHub the job then went red on executed tests, twice, for runner
   differences the box could not show, before going green. First,
   `the_child_runs_in_the_requested_directory`: the runner's `TEMP` is
   spelled with an 8.3 short name (`runner~1`), so the child's `cd` printed
   a different string for the same directory; the fixture now compares
   canonical paths (`6397968`). Second, two permission tests on a correct
   descriptor: the runner is the built-in Administrator (RID 500), which
   SDDL writes as the alias `LA` rather than the SID string; the tests now
   ask how SDDL spells this user (`c8398cc`). The same PR run also caught a
   Linux reset race on an oversized line, present on main before it
   (`09b3900`).
2. **The PTY layer.** Twelve fixtures, serialized on a lock. Every wait is
   a stall bound rather than teleport's 30 s / 60 s volume budgets
   ([08-testing.md](08-testing.md)). With `ConptyDsrProbe` removed, `exit 0`
   and `exit 7` are never published — teleport's `#W1`, reproduced here.
   Everything past the PTY layer *refused* on Windows at this step rather
   than half-working, per §7: both binaries exited with an error naming
   step 4, and the data directory refused to be created without its ACL.
3. **The tripwire.** Each ConPTY fixture's last statement writes its name
   to a ledger (`ANCHOR_CONPTY_LEDGER`) — the last statement, not a drop
   guard, so an early return does not count. CI then runs
   `every_conpty_fixture_ran_to_completion`, which lists the binary's own
   fixtures and fails unless each is in the ledger, or when there are none.
   Revert-checked both ways teleport#108 can happen: an early-returning
   fixture reports `13 passed` while the checker names it; the module gated
   out reports `1 passed` while the checker says no fixture is compiled in.
   An ungated `a_windows_ci_run_keeps_the_fixture_ledger` makes the ledger
   mandatory under Windows CI, which is 5db5fbd's shape exactly.
4. **Transport, input, permissions.** §4.1–§4.6 above, each with what
   proved it.
5. **Autostart.** §4.7 above.

## 6. What this does not buy

Adopting teleport's Windows PTY code did not get anchor to Windows; it
removed the one risk that could not be estimated. The breadth that remained
— a transport with no donor, an access-control model anchor had never had,
a console-input path, and an autostart story unsolved upstream — is now
built. What it still does not buy:

- **Crash survival**, on Windows as on Unix ([07-lifecycle.md](07-lifecycle.md)):
  anchord dying closes every pseudoconsole it holds.
- **Logs from a supervised anchord.** Under the headless conhost its
  stderr goes nowhere; there is no journald or `StandardErrorPath`
  equivalent wired up. A foreground `anchor daemon` logs as usual.
- **A final fsync on every stop.** Console close, logoff and shutdown are
  handled and unwind cleanly; `TerminateProcess` — Task Manager, `taskkill
  /F`, `uninstall` — cannot be caught, so the periodic fsync bounds the
  loss, as SIGKILL does on Unix.
- **An observed logon trigger** (§4.7).

## 7. The two questions, answered 2026-10-19

Both were the owner's call and both have been made. The reasoning is kept
rather than replaced by the answer, because the options rejected here are
the ones a future reader will be tempted to re-propose.

**Q1. Does anchor acquire an access-control model? → Yes, (a).**

Today `src/server.rs`'s module doc can say "there is no auth" and mean it, because the
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

**Decided: (a).** It preserves the existing position — the OS enforces
access, anchor does not authenticate — rather than replacing it. (c) is the
only option that changes what anchor *is*, and it would make the Unix path's
"no auth" claim obsolete as a side effect of a Windows port, which is too
much to pay here.

Consequence to carry into §3.2 and §4.2: the DACL is what the pipe name's
per-user rule leans on, so the two are designed together rather than in
sequence.

**Q2. Is Windows in scope at all, given §6? → Yes, as M6.**

The framing has not changed: the hard part is free, the broad part is not
started, and the autostart story is blocked upstream on a real `Access is
denied`. What changed is the requirement. teleport is to rely on anchor
fully, so "anchord is unix-only, stated deliberately" stops being a
defensible answer and becomes a permanent cap on teleport — its in-process
PTY layer could never be deleted, and Windows would never get session
survival (§8).

So the cost is accepted rather than argued down. §5's gating is the part
that must not be skipped: it exists so that a port which stalls halfway
leaves a daemon that *refuses* to run on Windows rather than one that boots,
reports healthy and hosts nothing.

**Autostart (§4.7) is explicitly allowed to end in "no autostart story on
Windows."** It is the one step gated on an upstream failure nobody here can
fix, and it is sequenced last so it cannot block the rest.

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

**M6 resolves this, but not quickly, so the two horizons must not be
conflated.** The decision of 2026-10-19 is that anchord grows a Windows arm
and teleport relies on it everywhere. That answers the *end state*. It does
not unbreak teleport's build this week: M6 is gated work with an unsolved
transport in the middle of it, and teleport's `windows-latest` job is red
today.

So the two repos need different answers on different clocks:

- **Now, teleport's call and needing nothing from anchor:** `#[cfg(unix)]`
  on the four `anchor_*` modules restores a build that worked until
  `780ff74`. The gate was once rejected as self-defeating because it is only
  correct while the in-process layer remains the Windows path — and under
  M6 that is exactly what it remains, until anchord can host a session on
  Windows. The objection expired; the fix did not.
- **At the end of M6:** the gate comes off, teleport's Windows path becomes
  anchord like every other platform, and only then does the in-process
  deletion become a removal rather than a Unix-only deletion.

What anchor owes teleport is the date it does not have: **anchord will not
host a session on Windows until M6 lands, and M6 has gates rather than an
estimate.** Nothing in teleport should wait on it that could instead be
gated on `unix`.

One practical consequence, now resolved in M6's favour. teleport's deletion
spec holds `ConptyDsrProbe` and `pty_primitive_windows.rs` as transfers
rather than deletions, released only once the behaviour is under test in
anchor (§5, step 3). That gate is no longer indefinite — §5 step 3 *is* M6's
third gate, and it is the step that releases them. Until it fires they must
be preserved deliberately — it is the only proven ConPTY exit-handshake work
in either repo, and §1's negative knowledge is not recoverable by reading the
code that remains.

### Where this stands, 2026-10-20

**The anchor side of "at the end of M6" exists, on main** (#33). anchord
hosts sessions on Windows, so the reason for teleport's `unix` gate on its
`anchor_*` modules has expired; lifting it is teleport's call.

What teleport's Windows client needs is one page:
[04-protocol.md](04-protocol.md#windows-a-named-pipe). The wire protocol is
byte-for-byte the Unix one; only finding and trusting the endpoint
differs. tokio's `ClientOptions` already opens at
`SECURITY_IDENTIFICATION`; the owner check is the part a client has to add.

**§5 step 3 has fired.** `ConptyDsrProbe` and `pty_primitive_windows.rs`
are under test here, with a tripwire against their silently not running,
so teleport's transfer-not-delete hold on them can be released. Both keep
their teleport provenance in their doc comments, and the negative knowledge in §1 is restated in the fixture file's module doc
rather than left to be rediscovered.
