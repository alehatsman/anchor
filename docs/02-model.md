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
    output_bytes: u64,          // authoritative log length
    created_at: i64,
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

**EXITED fires off the reaper's `wait()`, not off reader EOF.** A descendant
that inherited the PTY can hold the master open long after the child is gone.
These are two independent signals and conflating them is a known bug — see
teleport `docs/15-open-questions.md#s2`.

## Storage

SQLite, WAL, single writer. `$XDG_DATA_HOME/anchor/anchor.db`.

```text
sessions    (id PK, state, pid, exit_code, lost_reason, command, args_json,
             cwd, env_json, cols, rows, output_bytes, created_at, exited_at)
labels      (session_id, key, value, PK(session_id, key))
            index on (key, value)
meta        (key, value)
```

Output logs live outside the DB, one file per session:

```text
$XDG_DATA_HOME/anchor/sessions/<id>/output.vt
```

Append-only, never rewritten. The **file is authoritative** for content;
`sessions.output_bytes` is a cached length reconciled against the real file
size at startup.

## Startup sequence

```text
resolve data dir
    ↓
open SQLite (WAL), migrate
    ↓
mark sessions still "running"/"closing" as LOST (reason=daemon_restart)
    ↓
reconcile each output_bytes against actual output.vt length
    ↓
bind unix socket
    ↓
serve
```

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
