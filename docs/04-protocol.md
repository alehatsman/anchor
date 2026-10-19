# 04 — Protocol

Unix socket, `$XDG_RUNTIME_DIR/anchor.sock`, fallback
`$XDG_DATA_HOME/anchor/anchor.sock`. Loopback equivalent; no TCP, no TLS, no
auth. Filesystem permissions (0600) are the security boundary — see
[01-scope.md](01-scope.md): remote access is a client's job. On Windows, a
named pipe whose DACL is the same boundary: see
[below](#windows-a-named-pipe).

**A variable that is set but empty does not count as set.** Both lookups
treat `""` as absent and fall through (`non_empty_env`, `src/paths.rs`).
A client that skips this resolves `$XDG_RUNTIME_DIR/anchor.sock` to the
*relative* path `anchor.sock` when the variable is empty, then reports
anchord unreachable while naming no cause — the daemon is fine and the
client is knocking on a path in its own working directory. Empty-not-absent
is the common shape in practice: a service manager clearing a variable it
does not set, or a scratch environment built by hand.

Clients resolve this themselves rather than asking anchord, because the
resolution is how you *find* anchord. Any client that gets it wrong is
wrong before there is anyone to ask.

## Windows: a named pipe

On Windows the transport is a named pipe, not a socket. **The wire protocol
is identical**: the same newline-delimited JSON, the same switch to binary
frames after `attach`, byte for byte. Only how a client finds anchord and
decides to trust it differs, and that is the whole of this section. Why each
rule exists is [09-windows.md](09-windows.md) §3–§4.

**The name.** `\\.\pipe\anchor-<SID>`, where `<SID>` is the string form
(`S-1-5-21-…`) of the user SID in the client's own process token
(`TokenUser`, as `ConvertSidToStringSidW` writes it). Per user, because the
pipe namespace is machine-global. No environment variable changes it; the
CLI's `--socket` takes a full pipe path (`\\.\pipe\…`) instead, and refuses
anything else.

**Connecting.** Open the name with `CreateFileW`, `GENERIC_READ |
GENERIC_WRITE`, `OPEN_EXISTING`, and `SECURITY_SQOS_PRESENT |
SECURITY_IDENTIFICATION` in the flags — so a server cannot impersonate the
client at more than identification level. tokio's `ClientOptions` sets
exactly these by default; do not override `security_qos_flags`.

- `ERROR_FILE_NOT_FOUND`: no anchord is running for this user.
- `ERROR_PIPE_BUSY`: every instance is mid-handoff. Retry briefly; anchord
  always has a listening instance between connections.
- `ERROR_ACCESS_DENIED`: the pipe exists and is not yours to open. anchord
  grants nobody but its owner, so this is someone else's pipe at your name.
  Treat it as an error, not as "not running".

**Verifying, before the first byte is sent.** Read the pipe's owner from the
handle you opened (`GetSecurityInfo(handle, SE_KERNEL_OBJECT,
OWNER_SECURITY_INFORMATION, …)`) and require it to be **equal to your own
user SID** (`EqualSid`). anchord sets the owner explicitly when it creates
the pipe, and only a process running as you — or one holding a privilege
that already owns the machine — can create a pipe owned by you. A mismatch
means another user created the name first: close the handle and refuse.
Sending a request first would hand that process whatever the request
carries.

**What anchord guarantees on its side**, so a client need not check it: the
pipe's DACL grants your SID and nothing else (no Administrators, no SYSTEM,
no Everyone); the first instance is created with
`FILE_FLAG_FIRST_PIPE_INSTANCE`, so anchord **fails to start** if the name
is already taken rather than probing and replacing it; remote clients are
rejected (`PIPE_REJECT_REMOTE_CLIENTS`); it is byte mode; and anchord never
impersonates a client.

**Ending a blocked read.** There is no `shutdown`. A client that must end a
read from another thread — a detach from a quiet session — cancels it
(`CancelIoEx`, or dropping the tokio handle) rather than waiting for output
that may never come.

## Framing

Two framings on one connection:

- **control** — newline-delimited JSON, request/response, correlated by `id`
- **stream** — after `attach`, the connection switches to binary frames

## Control

Request:

```json
{"id":1,"op":"create","spec":{...},"labels":{"wm.item":"..."}}
{"id":2,"op":"list","filter":{"wm.item":"gitlab-main:mr:218"}}
{"id":3,"op":"get","session":"01J..."}
{"id":4,"op":"label","session":"01J...","set":{"k":"v"},"remove":["x"]}
{"id":5,"op":"resize","session":"01J...","cols":120,"rows":40}
{"id":6,"op":"terminate","session":"01J..."}
{"id":7,"op":"log","session":"01J...","from":0,"max":65536}
{"id":8,"op":"attach","session":"01J...","from":12345,"control":true}
{"id":9,"op":"purge","session":"01J..."}
{"id":10,"op":"ping"}
```

`ping` replies `{"pong":true}` and touches no session; it is a liveness
probe, and a live op like the rest.

`terminate` ends a session; `purge` forgets it — its row, its labels and
its `output.vt`. They are separate because an exited session is still worth
listing and still worth replaying, often long after it ended. A client that
wants the record gone asks for that explicitly.

`purge` refuses a running session with `already_exited`: deleting the log
of a live PTY would leave the reader thread appending to a file nobody can
read. Terminate first. Purging an id that was never known is `not_found`,
so a typo is reported rather than silently succeeding.

Response:

```json
{"id":1,"ok":true,"session":"01J...","state":"running","pid":4242,"output_offset":0}
{"id":2,"ok":true,"sessions":[{...}]}
{"id":6,"ok":false,"error":{"code":"not_found","message":"no such session"}}
```

`id` is the correlation id and nothing else. A session id travels as
`session`, never as `id`, so a response is always answerable — `create`
returning its new session under `id` would collide with the very field the
client matches the response by.

A session object omits `env`: a spec's environment can carry what a client
passed in at create time, and echoing it back widens who can read it. The
count goes out as `env_count`.

`log` returns its range as a **byte array**, not a string. Raw PTY bytes are
not UTF-8 in general — control sequences, and partial multi-byte characters
at a chunk boundary — and a lossy conversion would corrupt the stream. One
`log` response is capped at 4 MiB whatever `max` asks for; streaming is what
`attach` is for.

**Capped means clamped, not refused.** A larger `max` is served as 4 MiB,
and the client pages on from where the bytes ran out. Refusing it would
break the loop that reads a long log by range — the first call fails, so
no call ever succeeds, and a log past 4 MiB becomes readable only by
attaching. Asking for more than one response can carry is the normal shape
of paging, not a malformed request.

Error codes: `not_found`, `invalid_spec`, `invalid_label`, `spawn_failed`,
`already_exited`, `lease_held`, `too_large`, `unknown_op`, `bad_request`,
`internal`.

**`internal` is the daemon's fault, and that distinction is the point of
having it.** Every other code says something about the request: it names a
session that does not exist, it asks for a size out of range, it arrives
while someone else holds the lease. A client reading one of those knows not
to retry, because sending the same bytes again will fail the same way.
`internal` means the request was fine and the daemon could not serve it —
the database would not read, the log file would not open — so retrying is
reasonable and reporting it to the user as a malformed request is not.

Reporting a disk failure as `invalid_spec` is worse than reporting nothing
at all: it sends whoever is debugging it to look at the request.

An unknown `op` (anything not in the list above) returns `unknown_op`; it
never drops the connection.

`bad_request` is for a line that is not a request object at all — malformed
JSON, no `op`, or bytes that are not valid UTF-8. The last one is answered
rather than dropped for the same reason as the others: a closed connection
is indistinguishable from the daemon dying, and the offending line has been
consumed either way, so the framing survives and the connection stays
usable. `invalid_spec` means the request parsed and its contents
were wrong. If the line carried a usable `id` the response echoes it;
otherwise the response carries `id: 0`. A control line longer than 1 MiB is
answered `too_large`, and that connection is closed rather than left
desynchronised mid-line: the rest of that line was never read, so anything
following it would be parsed as the next request and every request after
would be off by one.

A client sending one may see its own write fail before it finishes — the
daemon answers at the cap, ends its side, and drains at most 16 MiB or 2 s
of what is still arriving before it closes. (The drain is there because
closing a unix socket with unread input makes Linux reset the connection,
which can discard the reply before the client reads it.) The reply is
already on the wire. **Read it before acting on the write
error**, or the one case the daemon took care to report arrives looking
exactly like the daemon dying.

## Attach

`attach` takes a byte `from` offset and a `control` flag.

- the daemon replies `{"id":8,"ok":true,"offset":N,"control":true|false}`
- it then **catches the client up** from `from` to the live head before
  registering it as a subscriber (see [03-pty.md](03-pty.md))
- the connection then carries binary frames

Frame:

```text
┌────────┬──────────┬──────────┬──────────────┐
│ u8 tag │ u64 off  │ u32 len  │ payload      │
└────────┴──────────┴──────────┴──────────────┘

client → daemon:

tag 0x02  input    lease holder only
tag 0x04  lease    payload 1 = claim control
tag 0x05  resize   payload = cols,rows as u16 BE each; lease holder only

daemon → client:

tag 0x01  output   off = offset of first byte
tag 0x03  exit     payload = exit code as i32 LE, or empty if unknown
tag 0x04  lease    payload = 1 if this client now holds it, 0 if revoked
tag 0x05  resize   payload = cols,rows as u16 BE each; the size now applied
```

`0x04` and `0x05` travel in both directions and mean different things each
way. Outbound they are statements of fact — this is who holds the lease,
this is the size the pty now has. Inbound they are requests, honoured only
from the lease holder.

A client is told when the size changes, for the same reason it is told when
the lease moves: one effective size is the thing anchor is opinionated about
(docs/01-scope.md), so every attached client has to be able to learn the
current one without asking. Without it, a client whose session is resized by
some *other* client keeps rendering at a width the pty no longer has, and
nothing it can observe says so — output is just bytes, and geometry is not
in them.

The frame carries the size **actually applied**: after clamping, and after
the coalescing window closes. A client that sends `5000` and is told `1000`
is being told the truth rather than its own request echoed back, and the
originator is told too — a client must not assume the size it asked for is
the size it got.

The `attach` reply carries `cols` and `rows` for the same reason, so a
client starts from the real size instead of waiting for a change that may
never come.

`tag` and `off` are followed by a **`u32` payload length**: a stream has no
other boundary, and without it a reader cannot tell where one frame ends and
the next begins. `off` and `len` are big-endian — a header is read byte by
byte off a socket and network order is the convention there. The `exit`
payload stays little-endian because it is an `i32` as the host writes it.

A `u32` caps one frame at 4 GiB, which no PTY read approaches: the reader
chunks at 64 KiB.

**An inbound frame is capped at one PTY chunk — 64 KiB — and the connection
is dropped on the header alone if it claims more.** The cap is
`MAX_INPUT_FRAME_BYTES`, defined from the reader's `READ_BUFFER_SIZE` rather
than restated, so the two cannot drift apart (#18). Downstream the length is the daemon's
own; upstream it is an unverified claim the daemon would allocate on, so
`len = 0xFFFFFFFF` would be a 4 GiB allocation for free. One chunk is the
largest thing a real client has to say — a keystroke is a few bytes and a
paste is bounded by what the PTY will take — so the cap costs nothing and
closing is right: a client that sent it is not speaking this protocol.

Offsets are the recovery mechanism. A client that drops reconnects with
`from` set to **one past the last byte it received** and loses nothing.
That is `off + len` of the last frame it got — not `off + 1`: a frame's
`off` is the offset of its *first* byte, so adding one would re-request
all but the first byte of a frame already delivered, and a client that
did it would see duplicated output on every reconnect.

`from` is inclusive: it is the first byte the client wants, and the first
`OUTPUT` frame normally starts there — but see below, it need not.
`from == next_offset` is therefore legal and means "nothing yet, only what happens next". This is
the same discipline the log file uses, deliberately: **recover by offset,
never by guessing.**

**The first frame on an attach is always a `LEASE` frame, with offset 0**
(`serve_attached`, `src/stream.rs`), announcing whether this connection
holds control. It
comes before any `OUTPUT`, and a client must not read its `off` as a byte
position.

**The first `OUTPUT` frame need not start at `from`.** If the log is capped,
or catch-up gives up on a client that keeps losing ground, the daemon moves
the replay start forward and the bytes behind it are dropped
(`Attach::caught_up`, `src/session/replay.rs`). The hole is visible as a **jump in the
offset prefix**: the first frame's `off` is greater than `from`. A client
must treat `off` as authoritative, never assume `off == from`, and never
assume the stream it holds is contiguous from `from`.

The attach response carries `next_offset`, the live head at the moment of
the attach. A client that wants only what happens from now on attaches
with `from` set to it; a client catching up compares it to its own last
offset to know how far behind it is. It is a snapshot, not a promise —
the head moves on as soon as the session writes again. Attaching past it
is refused with `invalid_spec` rather than silently clamped, because a `from` beyond the head
means the client and the daemon disagree about what exists.

`control: true` requests the lease. If held, the attach still succeeds as an
observer and the response says `control:false`.

**Claiming is a `lease` frame sent upstream.** A client that wants control
from a holder sends tag `0x04` with payload `1` on its attached connection;
the daemon moves the lease and tells both sides with `lease` frames — `1` to
the new holder, `0` to the one that lost it. Attach deliberately never
preempts, so this explicit act is the only thing that does, and it is a
frame rather than a flag on `attach` because the connection is already
streaming by the time a client decides to take over.

Every attached connection is told when the lease moves, not only the two
involved: a bystander learns control is taken rather than discovering it
when its own keystrokes stop working.

**Resize travels two ways, and which one is right depends on the lease.**
One effective PTY size means a resize belongs to whoever is typing — but
the lease is per-connection, so a `resize` control op arrives on a
connection that can prove nothing about a lease held on another.

- **While someone holds the lease**, the holder resizes with a `resize`
  frame (`0x05`) on its own attached connection, where the lease is already
  known. The control op is refused with `lease_held`. `anchor attach` sends
  one of these for every `SIGWINCH` it sees, which is what keeps the PTY
  the size of the window rather than the size the window was at attach
  time.
- **While the lease is free**, the control op is accepted. That is the
  case where a client sizes a session to its terminal *before* attaching,
  and with no holder there is nobody to protect.

**The two paths treat an out-of-range size differently.** Both bounds are
`1..=1000` for each of `cols` and `rows`. The **control op** refuses a size
outside it with `invalid_spec`. The **`resize` frame** clamps it and the
applied size is reported back (the size frame above). Do not branch on one
behaviour for both.

**The lease is per-connection.** One connection is one client: the daemon
identifies a holder by the connection itself, so there is no client id on
the wire. The lease is released when that connection ends — including a
client that vanishes while the session is quiet, which is why an attached
connection ends as soon as *either* direction does, rather than waiting for
the next chunk to fail a write. A lease stranded on a dead connection would
lock every other client out of typing.

Input is checked against the lease **per frame**, so a connection preempted
mid-stream stops being able to type immediately rather than at its next
attach. An observer's input frames are read and discarded — the read is what
makes its disconnect noticeable.

A refused `attach` is an ordinary error response: the connection stays a
control connection and may try again. Only a successful one switches
framing.

Backpressure: if a subscriber's queue overflows, the daemon closes that
connection. That is normal. Reconnect and replay.

The connection is **closed, not sent `exit`**. A subscriber dropped for
falling behind is watching a session that is still running, and an `exit`
frame would send it off to render a dead session instead of reconnecting
from its last offset. `exit` means the session ended, and nothing else.

Which of the two happened is tracked explicitly, not inferred. A subscriber
dropped for falling behind and a subscriber released because the session
reached EOF both end the same way from the stream's side, and the daemon
cannot tell them apart by looking at the session's `state`: the thread that
releases subscribers at EOF and the thread that records the exit are
different threads, and nothing orders them. A stream that reads `state` to
decide would, on that race, close a connection that was owed an `exit`.

For the same reason an `exit` frame, once it is being written, is never
abandoned because something else about the connection ended — a client
typing into a session that is exiting under it still gets told how it ended.

The queue budget only protects a client that has been *registered*. One that
stops reading during catch-up has no slot to drop yet, so its write simply
blocks — the PTY is unaffected, because catch-up runs off the fan-out mutex,
but the connection would park a task indefinitely. A frame write is
therefore bounded (30s, or `ANCHOR_WRITE_TIMEOUT_MS`), and a client that
exceeds it is closed the same way and for the same reason.

## CLI

A thin client over the same socket, for humans and scripts.

```text
anchor ls [--label k=v]...        list sessions
anchor create -- <cmd> [args]     spawn (--cwd, --label, --size, --login-shell)
anchor get <id>                   one session as JSON
anchor attach <id>                raw-mode passthrough, ~. to detach
anchor logs <id> [--from N]       dump output
anchor kill <id>                  terminate
anchor purge <id>                 forget: row, labels and output
anchor label <id> k=v             set/remove labels
anchor daemon                     run anchord in the foreground
anchor service install|uninstall|status
                                  register anchord with the user's
                                  service manager (07-lifecycle.md)
```

A `terminate` reply normally means the child is dead **and** the exit is
recorded: the reported `state` is normally terminal, and a `purge` sent
immediately after it will not be refused as still-running. The two are
written by different threads, so the reply waits for the second — without
it the obvious terminate-then-purge sequence races against itself. The wait
is bounded (2 s, `TERMINATE_SETTLE`, `src/server.rs`): if the exit is
not recorded by then the reply goes out anyway, so a client must check
`state` rather than assume it, and a `purge` right after may still see
`already_exited`.

`attach` restores the terminal on the way out, including when it is
**signalled** rather than exited — `SIGTERM`, `SIGHUP`, `SIGINT` and
`SIGQUIT` restore the saved attributes and then re-raise, so the process
still looks killed to whatever was waiting on it. A terminal left raw
outlives the process that did it, and a client attached to a long-running
session is the thing someone kills from another window. The keyboard cannot
raise `SIGINT` or `SIGQUIT` here — in raw mode ^C and ^\ are bytes for the
session — but `kill -INT` from another window can (`RESTORE_SIGNALS`,
`src/tty/unix.rs`).

On Windows the same restore runs from a console control handler on close,
logoff, shutdown and Ctrl-Break; Ctrl-C is a byte for the session, as on
Unix. Raw mode there also switches the console to UTF-8 and VT input, and
output to VT processing — for an observer too, since a Windows console
interprets escape sequences only when asked (`src/tty/windows.rs`).

`attach` is raw-mode passthrough: local keystrokes go to the PTY as the exact
bytes the local terminal produced, and output goes to stdout unparsed. **No VT
emulator** — the user's terminal already is one. Lift the shape from
[teleport/cli/src/attach.rs](https://github.com/alehatsman/teleport/blob/main/cli/src/attach.rs),
including the `~.` escape and the jittered reconnect backoff.
