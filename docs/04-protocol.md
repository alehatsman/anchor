# 04 — Protocol

Unix socket, `$XDG_RUNTIME_DIR/anchor.sock`, fallback
`$XDG_DATA_HOME/anchor/anchor.sock`. Loopback equivalent; no TCP, no TLS, no
auth. Filesystem permissions (0600) are the security boundary — see
[01-scope.md](01-scope.md): remote access is a client's job.

Two framings on one socket:

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
```

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

Error codes: `not_found`, `invalid_spec`, `invalid_label`, `spawn_failed`,
`already_exited`, `lease_held`, `too_large`, `unknown_op`, `bad_request`.

An unknown `op` returns an error; it never drops the connection.

`bad_request` is for a line that is not a request object at all — malformed
JSON, or no `op`. `invalid_spec` means the request parsed and its contents
were wrong. If the line carried a usable `id` the response echoes it;
otherwise the response carries `id: 0`. A control line longer than 1 MiB is
answered `too_large`, and that connection is closed rather than left
desynchronised mid-line.

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

tag 0x01  output   off = offset of first byte
tag 0x02  input    (client → daemon, lease holder only)
tag 0x03  exit     payload = exit code as i32 LE, or empty if unknown
tag 0x04  lease    payload = 1 if this client now holds it, 0 if revoked
```

`tag` and `off` are followed by a **`u32` payload length**: a stream has no
other boundary, and without it a reader cannot tell where one frame ends and
the next begins. `off` and `len` are big-endian — a header is read byte by
byte off a socket and network order is the convention there. The `exit`
payload stays little-endian because it is an `i32` as the host writes it.

A `u32` caps one frame at 4 GiB, which no PTY read approaches: the reader
chunks at 64 KiB.

Offsets are the recovery mechanism. A client that drops reconnects with
`from = last_offset + 1` and loses nothing. This is the same discipline the
log file uses, deliberately: **recover by offset, never by guessing.**

`control: true` requests the lease. If held, the attach still succeeds as an
observer and the response says `control:false`. A client takes the lease
explicitly with a later `attach` or a dedicated `claim` — see issue.

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

## CLI

A thin client over the same socket, for humans and scripts.

```text
anchor ls [--label k=v]...        list sessions
anchor create -- <cmd> [args]     spawn (--cwd, --label, --size, --login-shell)
anchor get <id>                   one session as JSON
anchor attach <id>                raw-mode passthrough, ~. to detach
anchor logs <id> [--from N]       dump output
anchor kill <id>                  terminate
anchor label <id> k=v             set/remove labels
anchor daemon                     run anchord in the foreground
```

`attach` is raw-mode passthrough: local keystrokes go to the PTY as the exact
bytes the local terminal produced, and output goes to stdout unparsed. **No VT
emulator** — the user's terminal already is one. Lift the shape from
[teleport/cli/src/attach.rs](https://github.com/alehatsman/teleport/blob/main/cli/src/attach.rs),
including the `~.` escape and the jittered reconnect backoff.
