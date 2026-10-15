# 02 — Model

## Session

```rust
struct Session {
    id: SessionId,              // ULID
    labels: BTreeMap<String, String>,
    spec: SpawnSpec,
    state: State,
    pid: Option<u32>,
    exit_code: Option<i32>,
    lost_reason: Option<String>,
    cols: u16,
    rows: u16,
    output_bytes: u64,          // authoritative stream length; never rewinds
    log_capped_at: Option<u64>, // where persistence stopped, if it did
    created_at: i64,            // milliseconds since the unix epoch
    exited_at: Option<i64>,
}

struct SpawnSpec {
    command: String,
    args: Vec<String>,
    cwd: PathBuf,
    env: BTreeMap<String, String>,   // overrides on top of daemon env
    cols: u16,
    rows: u16,
    login_shell: bool,
}
```

### Labels — the thing teleport does not have

Arbitrary caller-owned key/value, indexed, queryable. This is the field whose
absence in teleport forces every orchestrator to keep a side-table answering
"which of my things is this session for?".

```json
{ "wm.item": "gitlab-main:mr:218", "wm.agent": "impl-a", "wm.prompt": "implement" }
```

`list(filter)` supports exact match on any label key. That replaces the
side-table entirely.

Rules:
- keys: `[a-z0-9_.-]{1,64}`, values ≤ 256 bytes, ≤ 32 labels per session
- anchor never interprets a label. Reserved prefix: none. Namespacing is the
  caller's problem and convention (`wm.*`, `teleport.*`) is enough.
- labels are mutable after create — a client's own state may change.

### Lifecycle

```text
          spawn
            │
            ▼
        RUNNING ──────────── child exits on its own ─────────▶ EXITED
            │                                                    ▲
            │ terminate requested                                │
            ▼                                                    │
        CLOSING ── graceful → bounded wait → hard kill ──────────┘

        any state, daemon restarted with no live PTY        ──▶ LOST
```

`LOST` carries `lost_reason`: `daemon_restart`, `spawn_failed`,
`kill_timeout`.

`io_error` is the fourth value and the odd one out: it records that writing
`output.vt` failed, and it can be set **while the child is still running**.
The session continues; only persistence stopped.

**EXITED fires off the reaper's `wait()`, not off reader EOF.** A descendant
that inherited the PTY can hold the master open long after the child is gone.
These are two independent signals and conflating them is a known bug — see
teleport `docs/15-open-questions.md#s2`.

## Storage

SQLite, WAL, single writer. `$XDG_DATA_HOME/anchor/anchor.db`.

```text
sessions    (id PK, state, pid, exit_code, lost_reason, command, args_json,
             cwd, env_json, cols, rows, output_bytes, log_capped_at,
             created_at, exited_at)
labels      (session_id, key, value, PK(session_id, key))
            index on (key, value)
meta        (key, value)
```

Output logs live outside the DB, one file per session:

```text
$XDG_DATA_HOME/anchor/sessions/<id>/output.vt
```

Append-only, never rewritten. The **file is authoritative** for content;
`sessions.output_bytes` is a cached stream length reconciled against the real
file size at startup.

### The size cap

A log warns at **256 MiB** and stops growing at **1 GiB**, recording
`log_capped_at`. Past that point the file stops but **the offset stream keeps
advancing**: live streaming continues, offsets stay authoritative, and only
replay of the capped range is gone. A write failure caps the log the same way
— the hole is recorded rather than silently interleaved into a byte stream
whose offsets are an index.

This is why reconciliation takes **`max(file length, output_bytes)`** and never
the file alone. The file cannot lie about bytes it holds, but for a capped log
it stopped growing while the stream kept counting, and taking the file length
would rewind `output_bytes` below offsets live clients already hold. A stream
found ahead of its file is itself a cap, recorded if no row said so.

## Startup sequence

```text
resolve data dir
    ↓
open SQLite (WAL), migrate
    ↓
bind unix socket            ← before any write: see below
    ↓
mark sessions still "running"/"closing" as LOST (reason=daemon_restart)
    ↓
reconcile each output_bytes against actual output.vt length
    (larger of the two wins; a stream ahead of its file is capped)
    ↓
serve
```

**The bind comes before the recovery writes.** Recovery rewrites every live
session to `lost`, which is correct after a crash and catastrophic
otherwise. A second `anchord` started by accident — a stray one in a shell,
a supervisor racing itself — would otherwise mark the *running* daemon's
sessions lost and then exit, telling its clients their sessions are gone
while the PTYs are still running. Claiming the socket first makes that
impossible: whoever holds it is the only process entitled to decide what a
live row means.

## Create sequence

Order matters — the log and the row exist before the child can emit a byte.

```text
validate (command resolvable, cwd exists, 1<=cols<=1000, 1<=rows<=1000,
          labels within limits)
    ↓
allocate ULID, mkdir sessions/<id>/
    ↓
open output.vt for append
    ↓
insert row (state=running) + labels
    ↓
openpty(size)
    ↓
spawn      ← failure: row → EXITED, lost_reason="spawn_failed", 4xx to caller
    ↓
start reader / writer / control / reaper threads
    ↓
return { id, state, pid, output_offset: 0 }
```
