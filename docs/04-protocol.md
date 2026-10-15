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
{"id":2,"ok":true,"sessions":[{...}]}
{"id":6,"ok":false,"error":{"code":"not_found","message":"no such session"}}
```

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
┌────────┬──────────┬──────────────┐
│ u8 tag │ u64 off  │ payload      │
└────────┴──────────┴──────────────┘

tag 0x01  output   off = offset of first byte
tag 0x02  input    (client → daemon, lease holder only)
tag 0x03  exit     payload = exit code as i32 LE, or empty if unknown
tag 0x04  lease    payload = 1 if this client now holds it, 0 if revoked
```

Offsets are the recovery mechanism. A client that drops reconnects with
`from = last_offset + 1` and loses nothing. This is the same discipline the
log file uses, deliberately: **recover by offset, never by guessing.**

`control: true` requests the lease. If held, the attach still succeeds as an
observer and the response says `control:false`. A client takes the lease
explicitly with a later `attach` or a dedicated `claim` — see issue.

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
