//! The unix socket server.
//!
//! Newline-delimited JSON in and out, one task per connection. The socket is
//! 0600: filesystem permissions are the security boundary, so there is no
//! auth and no TCP.
//!
//! [`bind`] is deliberately synchronous and returns a `std` listener, so a
//! caller can claim the socket — and fail loudly if another daemon already
//! holds it — before any runtime exists. [`serve`] adopts it.

use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixListener as StdUnixListener;
use std::path::Path;
use std::sync::{Arc, Mutex as StdMutex};

use anyhow::{bail, Context, Result};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};

use crate::proto::{self, ErrorCode, Request, SpecWire};
use crate::session::{CreateError, SessionManager};
use crate::store::Store;

/// Everything a connection needs to answer a request.
#[derive(Clone)]
pub struct State {
    pub manager: Arc<SessionManager>,
    pub store: Arc<StdMutex<Store>>,
}

impl State {
    pub fn new(manager: Arc<SessionManager>, store: Arc<StdMutex<Store>>) -> Self {
        Self { manager, store }
    }

    /// The store, locked.
    ///
    /// **Never hold this guard across anything that touches a `Session`.**
    /// `SessionManager::label` takes a session's labels and then the store;
    /// a caller that takes them the other way round closes a cycle and
    /// deadlocks the whole daemon, which is #19: `op_list` held this guard
    /// for the arm of a `match` whose body called `Session::snapshot`.
    ///
    /// Reads that only need rows should go through [`stored_rows`] or
    /// [`stored_row`], which hand back owned data and cannot leak the guard
    /// into the caller's scope.
    ///
    /// [`stored_rows`]: Self::stored_rows
    /// [`stored_row`]: Self::stored_row
    fn store(&self) -> std::sync::MutexGuard<'_, Store> {
        match self.store.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Stored rows matching `filter`, with the store lock released before
    /// this returns.
    fn stored_rows(
        &self,
        filter: &BTreeMap<String, String>,
    ) -> anyhow::Result<Vec<crate::model::Session>> {
        self.store().list_filtered(filter)
    }

    /// One stored row, with the store lock released before this returns.
    fn stored_row(&self, id: &str) -> anyhow::Result<Option<crate::model::Session>> {
        self.store().get(id)
    }
}

/// Longest control line accepted. Beyond this the request is refused with
/// `too_large` rather than buffered.
const MAX_LINE_BYTES: u64 = 1024 * 1024;

/// Binds the socket, refusing to displace a daemon that is already running.
///
/// A socket file left behind by a dead daemon is stale: nothing accepts on
/// it, so it is removed and rebound.
/// The usable length of `sockaddr_un.sun_path`, minus the NUL.
///
/// 104 on macOS and the BSDs, 108 on Linux; the smaller is used on both
/// because a daemon that starts here and not there is worse than one that
/// gives the same answer everywhere.
const MAX_SOCKET_PATH: usize = 103;

pub fn bind(socket_path: &Path) -> Result<StdUnixListener> {
    // Checked before anything else, because the kernel's own answer is
    // `EINVAL` with no mention of length -- which reads as "the path is
    // wrong" rather than "the path is too long", and sends you looking at
    // the wrong half of it. The limit is small enough to hit by accident:
    // a data dir under a long home or a temp dir is most of the budget.
    let len = socket_path.as_os_str().as_encoded_bytes().len();
    if len > MAX_SOCKET_PATH {
        bail!(
            "socket path is {len} bytes, past the {MAX_SOCKET_PATH}-byte limit              a unix socket address can hold: {}. Use a shorter directory              (XDG_RUNTIME_DIR, or --socket).",
            socket_path.display()
        );
    }

    if socket_path.exists() {
        // The probe below cannot be the thing that decides, because the two
        // platforms disagree about what connecting to a path that is *not* a
        // socket means. macOS reports `ENOTSOCK`, which falls through to the
        // refusal. Linux reports `ECONNREFUSED` -- indistinguishable from the
        // socket a dead daemon left behind, which is the one error treated as
        // safe to delete. So on Linux the probe alone would answer "stale"
        // for any regular file here and unlink it.
        //
        // Checking the file type first is what makes the rule below mean the
        // same thing on both kernels.
        use std::os::unix::fs::FileTypeExt;
        let is_socket = std::fs::metadata(socket_path)
            .map(|m| m.file_type().is_socket())
            .unwrap_or(false);
        if !is_socket {
            bail!(
                "cannot determine whether a daemon owns {}: it exists but is \
                 not a socket. Refusing to remove it; move it aside if it \
                 really is stale.",
                socket_path.display()
            );
        }

        match std::os::unix::net::UnixStream::connect(socket_path) {
            // Someone is listening. Never remove it.
            Ok(_) => bail!("anchord is already running on {}", socket_path.display()),
            // Nothing is accepting: a socket file a dead daemon left behind.
            // This is the *only* error that means stale, and it means it
            // only because the path is known to be a socket by here.
            Err(e) if e.kind() == std::io::ErrorKind::ConnectionRefused => {
                tracing::warn!(socket = %socket_path.display(), "removing stale socket");
                std::fs::remove_file(socket_path)
                    .with_context(|| format!("removing stale socket {}", socket_path.display()))?;
            }
            // Anything else — a permissions problem, for instance — says
            // nothing about whether a daemon is alive.
            // Treating it as stale would delete a running daemon's socket
            // and silently take over its clients.
            Err(e) => bail!(
                "cannot determine whether a daemon owns {}: {e}. \
                 Refusing to remove it; move it aside if it really is stale.",
                socket_path.display()
            ),
        }
    }

    // Permissions are the security boundary here, so the window where the
    // socket exists with umask-derived permissions must be closed before
    // anything can connect. Binding under a 0077 umask means it is never
    // group- or world-accessible, even for the instant before the chmod.
    //
    // `umask` is process-global and not thread-safe, which is acceptable
    // precisely here: this runs once at startup before the daemon serves
    // anything, and the temporary value is strictly *more* restrictive — a
    // file another thread created in this window would come out tighter
    // than intended, never looser.
    //
    // SAFETY: `umask` takes and returns a mode and touches no memory.
    let previous = unsafe { libc::umask(0o077) };
    let bound = StdUnixListener::bind(socket_path);
    // SAFETY: as above, restoring what we read.
    unsafe { libc::umask(previous) };

    let listener = bound.with_context(|| format!("binding {}", socket_path.display()))?;
    std::fs::set_permissions(socket_path, std::fs::Permissions::from_mode(0o600))
        .with_context(|| format!("setting 0600 on {}", socket_path.display()))?;
    Ok(listener)
}

/// Accepts connections until the listener fails. One task per client.
///
/// Takes the `std` listener from [`bind`] and adopts it onto the runtime this
/// is polled on.
pub async fn serve(listener: StdUnixListener, state: State) -> Result<()> {
    listener
        .set_nonblocking(true)
        .context("setting the listener non-blocking")?;
    let listener = UnixListener::from_std(listener).context("adopting the listener")?;

    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let state = state.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(stream, state).await {
                        tracing::warn!(error = %e, "connection ended with an error");
                    }
                });
            }
            // Pause before trying again. An accept error is usually
            // transient, but the ones that are not -- `EMFILE` above all,
            // where the process is out of descriptors -- fail instantly
            // and repeat forever. Looping straight back pins a core and
            // writes a log line per iteration, so the daemon's response to
            // running out of fds was to burn the CPU and fill the disk.
            Err(e) => {
                tracing::warn!(error = %e, "accept failed");
                tokio::time::sleep(ACCEPT_RETRY_DELAY).await;
            }
        }
    }
}

async fn handle_connection(stream: UnixStream, state: State) -> Result<()> {
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    // Bytes, not a `String`: a `read_line` into a `String` fails the whole
    // read on invalid UTF-8, which drops the connection with no reply.
    // docs/04-protocol.md promises a line that is not a request object is
    // answered `bad_request`, and that has to include one that is not text.
    let mut raw: Vec<u8> = Vec::new();
    // One connection is one client, for the control lease. docs/04-protocol.md
    // leaves lease identity to an open issue, and per-connection is the
    // honest reading of "exactly one writer of input and size".
    let holder = crate::model::new_session_id();

    loop {
        raw.clear();
        // Bounded, so one client cannot make the daemon buffer without
        // limit. `take` counts bytes, not lines, and is reset per request.
        let read = (&mut reader)
            .take(MAX_LINE_BYTES + 1)
            .read_until(b'\n', &mut raw)
            .await
            .context("reading request")?;

        if read == 0 {
            return Ok(()); // EOF
        }
        if read as u64 > MAX_LINE_BYTES {
            respond(
                &mut write_half,
                proto::error(0, ErrorCode::TooLarge, "request line too large"),
            )
            .await?;
            // The rest of that line is unread; the client reconnects rather
            // than desynchronising the framing.
            return Ok(());
        }
        // Answered, not dropped. The whole line was consumed, so the
        // framing is still intact and the connection stays usable --
        // closing here would make a stray byte indistinguishable from the
        // daemon dying.
        let line = match std::str::from_utf8(&raw) {
            Ok(line) => line.to_string(),
            Err(_) => {
                respond(
                    &mut write_half,
                    proto::error(0, ErrorCode::BadRequest, "request line is not valid UTF-8"),
                )
                .await?;
                continue;
            }
        };

        if line.trim().is_empty() {
            continue;
        }

        // `attach` is the one op that can stop returning to this loop: on
        // success the connection carries binary frames from here on. A
        // refused attach is an ordinary error response and the connection
        // stays a control connection.
        if is_attach(&line) {
            match attach_check(&line, &state) {
                Err(err) => {
                    respond(&mut write_half, err).await?;
                    continue;
                }
                Ok((session, req)) => {
                    // The lease decision, the watchers and the guard that
                    // releases it are one step, taken before the reply: a
                    // reply that fails to send must release the lease, and
                    // the awaits between here and `serve_attached` can
                    // return first. See `Handshake`.
                    let handshake = crate::stream::Handshake::begin(&session, &holder, req.control);
                    respond(
                        &mut write_half,
                        proto::ok(
                            req.id,
                            json!({
                                "offset": req.from,
                                "control": handshake.granted(),
                                "next_offset": session.next_offset(),
                                // So a client starts from the real size
                                // rather than waiting for a change that
                                // may never come. See docs/04-protocol.md.
                                "cols": session.size().0,
                                "rows": session.size().1,
                            }),
                        ),
                    )
                    .await?;

                    // The reader may already hold bytes the client
                    // pipelined after its attach line — `into_inner` would
                    // drop them — so they are carried over.
                    let buffered = reader.buffer().to_vec();
                    return crate::stream::serve_attached(
                        write_half,
                        reader.into_inner(),
                        buffered,
                        session,
                        req.from,
                        handshake,
                    )
                    .await;
                }
            }
        }

        respond(&mut write_half, dispatch(&line, &state).await).await?;
    }
}

/// Whether this line is an `attach`, decided before the op-specific parse so
/// a malformed attach still gets a normal error response.
fn is_attach(line: &str) -> bool {
    serde_json::from_str::<Request>(line)
        .map(|r| r.op == "attach")
        .unwrap_or(false)
}

#[derive(Deserialize)]
struct AttachReq {
    id: u64,
    session: String,
    #[serde(default)]
    from: u64,
    #[serde(default)]
    control: bool,
}

/// Validates an `attach` without touching the connection, so a refusal can
/// be answered like any other error and the connection carries on.
fn attach_check(
    line: &str,
    state: &State,
) -> Result<(Arc<crate::session::Session>, AttachReq), Value> {
    let req: AttachReq = serde_json::from_str(line).map_err(|e| {
        let id = serde_json::from_str::<Request>(line)
            .map(|r| r.id)
            .unwrap_or(0);
        proto::error(id, ErrorCode::InvalidSpec, e.to_string())
    })?;

    let Some(session) = state.manager.get(&req.session) else {
        return Err(not_live(req.id, &req.session, state));
    };

    // An offset past the head is a client that has lost track of the
    // stream. Say so rather than silently rewinding it.
    let next_offset = session.next_offset();
    if req.from > next_offset {
        return Err(proto::error(
            req.id,
            ErrorCode::InvalidSpec,
            format!("from {} is ahead of next_offset {next_offset}", req.from),
        ));
    }
    Ok((session, req))
}

/// Answers one control request. Never panics, never drops the connection.
///
/// Async because two ops are genuinely slow: `terminate` runs the bounded
/// termination policy (up to ~7s of graceful wait plus hard kill) and
/// `purge` deletes a session directory. Both would otherwise hold a
/// runtime worker for their whole duration, stalling every other
/// connection that worker is responsible for — the daemon's own version of
/// the rule it enforces for PTY output: nothing slow runs where something
/// fast has to wait behind it.
async fn dispatch(line: &str, state: &State) -> Value {
    let request: Request = match serde_json::from_str(line) {
        Ok(r) => r,
        Err(e) => {
            // Salvage the id if the line was an object carrying one, so the
            // client can still correlate the failure.
            let id = serde_json::from_str::<Value>(line)
                .ok()
                .and_then(|v| v.get("id").and_then(Value::as_u64))
                .unwrap_or(0);
            return proto::error(id, ErrorCode::BadRequest, format!("malformed request: {e}"));
        }
    };

    match request.op.as_str() {
        "ping" => proto::ok(request.id, json!({ "pong": true })),
        "create" => op_create(request.id, line, state),
        "list" => op_list(request.id, line, state),
        "get" => op_get(request.id, line, state),
        "label" => op_label(request.id, line, state),
        "resize" => op_resize(request.id, line, state),
        "terminate" => op_terminate(request.id, line, state).await,
        "log" => op_log(request.id, line, state),
        "purge" => op_purge(request.id, line, state).await,
        other => proto::error(
            request.id,
            ErrorCode::UnknownOp,
            format!("unknown op {other:?}"),
        ),
    }
}

async fn respond(out: &mut (impl AsyncWriteExt + Unpin), value: Value) -> Result<()> {
    let mut buf = serde_json::to_vec(&value)?;
    buf.push(b'\n');
    out.write_all(&buf).await.context("writing response")?;
    out.flush().await.context("flushing response")?;
    Ok(())
}

/// Parses an op's own fields. The line itself already parsed, so a failure
/// here is about contents: `invalid_spec`, not `bad_request`.
fn parse<T: for<'de> Deserialize<'de>>(id: u64, line: &str) -> Result<T, Value> {
    serde_json::from_str(line).map_err(|e| proto::error(id, ErrorCode::InvalidSpec, format!("{e}")))
}

#[derive(Deserialize)]
struct CreateReq {
    spec: SpecWire,
    #[serde(default)]
    labels: BTreeMap<String, String>,
}

fn op_create(id: u64, line: &str, state: &State) -> Value {
    let req: CreateReq = match parse(id, line) {
        Ok(r) => r,
        Err(e) => return e,
    };
    let spec = match req.spec.resolve() {
        Ok(spec) => spec,
        Err(e) => return proto::error(id, ErrorCode::InvalidSpec, format!("resolving cwd: {e}")),
    };

    match state.manager.create(&spec, req.labels) {
        Ok(session) => proto::ok(
            id,
            json!({
                "session": session.id,
                "state": session.state().as_str(),
                "pid": session.pid(),
                "output_offset": 0,
            }),
        ),
        Err(e) => proto::error(id, code_for(&e), e.to_string()),
    }
}

#[derive(Deserialize)]
struct ListReq {
    #[serde(default)]
    filter: BTreeMap<String, String>,
}

fn op_list(id: u64, line: &str, state: &State) -> Value {
    let req: ListReq = match parse(id, line) {
        Ok(r) => r,
        Err(e) => return e,
    };
    // The store is the full history: live sessions plus the rows a restart
    // turned into `lost`. Live state is folded over it below.
    //
    // Through `stored_rows`, not `state.store()`: the fold calls
    // `Session::snapshot`, and a store guard still live at that point is
    // the store -> session.labels order that deadlocks against
    // `SessionManager::label` (#19). A `match` scrutinee would have held it
    // for the whole arm.
    match state.stored_rows(&req.filter) {
        Ok(sessions) => {
            let sessions: Vec<Value> = sessions
                .iter()
                .map(|row| proto::session_json(&merge_live(row, state)))
                .collect();
            proto::ok(id, json!({ "sessions": sessions }))
        }
        // The store failing to read is not a bad filter.
        Err(e) => proto::error(id, ErrorCode::Internal, e.to_string()),
    }
}

/// The code for a `CreateError`, in one place.
///
/// Shared with the label path deliberately. It used to have its own
/// catch-all that answered `invalid_spec` for everything it did not name,
/// so the same failure got one code from `create` and a wrong one from
/// `label`. Matching exhaustively here means a new variant is a compile
/// error rather than a silent reclassification.
fn code_for(e: &CreateError) -> ErrorCode {
    match e {
        CreateError::InvalidLabel(_) => ErrorCode::InvalidLabel,
        CreateError::InvalidCwd(_)
        | CreateError::ExecutableNotFound(_)
        | CreateError::InvalidSize { .. } => ErrorCode::InvalidSpec,
        CreateError::NotFound => ErrorCode::NotFound,
        CreateError::StillRunning => ErrorCode::AlreadyExited,
        // A cap refusal and a failed spawn are both "it did not start";
        // the message says which.
        CreateError::TooManySessions { .. } | CreateError::Spawn(_) => ErrorCode::SpawnFailed,
        // Opening the log is the daemon's own storage failing, not
        // anything about the request.
        CreateError::Log(_) => ErrorCode::Internal,
    }
}

#[derive(Deserialize)]
struct SessionReq {
    session: String,
}

fn op_get(id: u64, line: &str, state: &State) -> Value {
    let req: SessionReq = match parse(id, line) {
        Ok(r) => r,
        Err(e) => return e,
    };
    match lookup(&req.session, state) {
        Some(session) => proto::ok(id, json!({ "session": proto::session_json(&session) })),
        None => proto::error(id, ErrorCode::NotFound, "no such session"),
    }
}

#[derive(Deserialize)]
struct LabelReq {
    session: String,
    #[serde(default)]
    set: BTreeMap<String, String>,
    #[serde(default)]
    remove: Vec<String>,
}

fn op_label(id: u64, line: &str, state: &State) -> Value {
    let req: LabelReq = match parse(id, line) {
        Ok(r) => r,
        Err(e) => return e,
    };
    match state
        .manager
        .label(&req.session, req.set.clone(), &req.remove)
    {
        Ok(labels) => proto::ok(id, json!({ "labels": labels })),
        // No live PTY behind it. The row may still exist, and a client's own
        // state outlives the process it described.
        Err(CreateError::NotFound) => label_stored(id, &req, state),
        Err(e) => proto::error(id, code_for(&e), e.to_string()),
    }
}

fn label_stored(id: u64, req: &LabelReq, state: &State) -> Value {
    let mut store = state.store();
    let Ok(Some(row)) = store.get(&req.session) else {
        return proto::error(id, ErrorCode::NotFound, "no such session");
    };
    let mut labels = row.labels;
    for key in &req.remove {
        labels.remove(key);
    }
    labels.extend(req.set.clone());
    if let Err(e) = crate::model::labels::validate(&labels) {
        return proto::error(id, ErrorCode::InvalidLabel, e.to_string());
    }
    match store.replace_labels(&req.session, &labels) {
        Ok(()) => proto::ok(id, json!({ "labels": labels })),
        // The labels were already validated above; a failure here is the
        // store's.
        Err(e) => proto::error(id, ErrorCode::Internal, e.to_string()),
    }
}

#[derive(Deserialize)]
struct ResizeReq {
    session: String,
    cols: u16,
    rows: u16,
}

fn op_resize(id: u64, line: &str, state: &State) -> Value {
    let req: ResizeReq = match parse(id, line) {
        Ok(r) => r,
        Err(e) => return e,
    };
    // pty.rs clamps as a backstop; out of range is a protocol error here,
    // which is the layer that can actually tell the client about it.
    if !crate::session::manager::SIZE_RANGE.contains(&req.cols)
        || !crate::session::manager::SIZE_RANGE.contains(&req.rows)
    {
        return proto::error(
            id,
            ErrorCode::InvalidSpec,
            "cols and rows must be between 1 and 1000",
        );
    }
    let Some(session) = state.manager.get(&req.session) else {
        return not_live(id, &req.session, state);
    };
    // A held lease means resize belongs to its holder, who sends it as a
    // frame on the attached connection where the lease is known. This
    // control op is for the other case: sizing a session before attaching
    // to it, when nobody holds the lease and there is nobody to protect.
    if let Some(holder) = session.controller() {
        return proto::error(
            id,
            ErrorCode::LeaseHeld,
            format!("{holder} holds the lease; resize on the attached connection"),
        );
    }
    match session.resize(req.cols, req.rows) {
        Ok(()) => proto::ok(id, json!({ "cols": req.cols, "rows": req.rows })),
        Err(e) => proto::error(id, ErrorCode::AlreadyExited, e.to_string()),
    }
}

async fn op_terminate(id: u64, line: &str, state: &State) -> Value {
    let req: SessionReq = match parse(id, line) {
        Ok(r) => r,
        Err(e) => return e,
    };
    let Some(session) = state.manager.get(&req.session) else {
        return not_live(id, &req.session, state);
    };

    // Off the worker: terminate blocks for the whole graceful wait plus the
    // hard kill. That bound is what termination means, so it belongs in the
    // policy — not on a thread other connections are waiting on.
    let terminating = Arc::clone(&session);
    let result = tokio::task::spawn_blocking(move || terminating.terminate()).await;

    // `terminate` returns once the child is reaped, but the session's
    // recorded state is written by the thread draining the pty's exit
    // channel -- a different thread, with nothing ordering the two. Replying
    // before it lands means answering `"state": "closing"` to a successful
    // terminate, and, worse, refusing the `purge` that a client sends next:
    // `forget` requires a terminal state, so the obvious sequence races.
    //
    // The child is already dead here, so this is a wait for a store to
    // catch up, not for a process. Bounded anyway: a reply that is merely
    // stale beats one that never comes.
    if result.is_ok() {
        let _ = tokio::time::timeout(TERMINATE_SETTLE, session.wait_exited()).await;
    }

    // Idempotent, including against an already-exited session.
    match result {
        Ok(Ok(())) => proto::ok(
            id,
            json!({
                "state": session.state().as_str(),
                "exit_code": session.snapshot().exit_code,
            }),
        ),
        Ok(Err(e)) => proto::error(id, ErrorCode::AlreadyExited, e.to_string()),
        // The blocking task itself failed or panicked. The session's state
        // is unknown, and `already_exited` would be a claim that it is
        // gone -- the one thing this cannot know.
        Err(e) => proto::error(id, ErrorCode::Internal, format!("terminate: {e}")),
    }
}

#[derive(Deserialize)]
struct LogReq {
    session: String,
    #[serde(default)]
    from: u64,
    #[serde(default = "default_log_max")]
    max: u64,
}

fn default_log_max() -> u64 {
    64 * 1024
}

/// How long to wait after a failed `accept` before trying again. Long
/// enough that a permanent failure cannot spin, short enough that a
/// transient one costs nothing noticeable.
const ACCEPT_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(100);

/// How long `terminate` waits for the exit it already caused to be
/// recorded. Not a wait for the child -- that has been reaped by this
/// point -- only for the thread that writes the state to run.
const TERMINATE_SETTLE: std::time::Duration = std::time::Duration::from_secs(2);

/// Hard ceiling on one `log` response whatever `max` asks for: the range is
/// buffered whole into a JSON response, so it cannot be unbounded. Streaming
/// a session is what `attach` is for.
const LOG_MAX_BYTES: u64 = 4 * 1024 * 1024;

fn op_log(id: u64, line: &str, state: &State) -> Value {
    let req: LogReq = match parse(id, line) {
        Ok(r) => r,
        Err(e) => return e,
    };
    // Clamped, not refused. docs/04-protocol.md has always said one
    // response is "capped at 4 MiB whatever `max` asks for", and refusing
    // instead made that sentence false in the way that matters: a client
    // walking a long log asks for the range it wants and pages through
    // what it gets back. Refusing the first call means the loop never runs
    // at all, so a log longer than 4 MiB could not be read by range --
    // only by attaching. Asking for more than can be served is not an
    // error, it is the normal shape of paging.
    let max = req.max.min(LOG_MAX_BYTES);

    // Reads the file, not the live session: a log outlives its PTY, so an
    // exited or lost session still replays.
    let Some(row) = lookup(&req.session, state) else {
        return proto::error(id, ErrorCode::NotFound, "no such session");
    };
    let path = state
        .manager
        .root()
        .join(&row.id)
        .join(crate::log::LOG_FILE_NAME);
    let mut reader = match crate::log::LogReader::open(&path) {
        Ok(reader) => reader,
        // Only a missing file is `not_found`. A log that exists and cannot
        // be opened -- permissions, a bad disk -- is the daemon's problem,
        // and answering `not_found` for it claims the session's output is
        // gone when it is sitting right there.
        Err(e) => {
            let code = if e.kind() == std::io::ErrorKind::NotFound {
                ErrorCode::NotFound
            } else {
                ErrorCode::Internal
            };
            return proto::error(id, code, format!("opening the log: {e}"));
        }
    };
    match reader.read_range(req.from, req.from.saturating_add(max)) {
        Ok(bytes) => {
            let len = bytes.len() as u64;
            proto::ok(
                id,
                json!({
                    "from": req.from,
                    "to": req.from + len,
                    "next_offset": row.output_bytes,
                    "log_capped_at": row.log_capped_at,
                    // Raw PTY bytes are not UTF-8 in general — control
                    // sequences, and partial multi-byte characters at a
                    // chunk boundary. A lossy conversion would corrupt the
                    // stream, so they go out as a byte array.
                    "bytes": bytes,
                }),
            )
        }
        // The range was checked; this is the log file itself failing.
        Err(e) => proto::error(id, ErrorCode::Internal, format!("reading the log: {e}")),
    }
}

async fn op_purge(id: u64, line: &str, state: &State) -> Value {
    let req: SessionReq = match parse(id, line) {
        Ok(r) => r,
        Err(e) => return e,
    };
    // Nothing to forget is not a failure to forget it, but an id that was
    // never known is still a 404 — a client purging a typo should hear so.
    if lookup(&req.session, state).is_none() {
        return proto::error(id, ErrorCode::NotFound, "no such session");
    }
    // Off the worker too: forgetting a session deletes its directory, and
    // a large log on a slow disk is not something other connections should
    // queue behind.
    let manager = Arc::clone(&state.manager);
    let session = req.session.clone();
    match tokio::task::spawn_blocking(move || manager.forget(&session)).await {
        Ok(Ok(_)) => proto::ok(id, json!({ "purged": true })),
        Ok(Err(e @ CreateError::StillRunning)) => {
            proto::error(id, ErrorCode::AlreadyExited, e.to_string())
        }
        Ok(Err(e)) => proto::error(id, code_for(&e), e.to_string()),
        Err(e) => proto::error(id, ErrorCode::Internal, format!("purge: {e}")),
    }
}

/// A session by id: the live one if there is one, otherwise its stored row.
fn lookup(id: &str, state: &State) -> Option<crate::model::Session> {
    if let Some(session) = state.manager.get(id) {
        return Some(session.snapshot());
    }
    state.stored_row(id).ok().flatten()
}

/// Folds live state over a stored row, so a list never reports a running
/// session's offset as of its last throttled write.
fn merge_live(row: &crate::model::Session, state: &State) -> crate::model::Session {
    match state.manager.get(&row.id) {
        Some(session) => session.snapshot(),
        None => row.clone(),
    }
}

/// A control op that needs a live PTY, against a session that has none.
fn not_live(id: u64, session: &str, state: &State) -> Value {
    match state.stored_row(session) {
        Ok(Some(_)) => proto::error(id, ErrorCode::AlreadyExited, "session is not running"),
        _ => proto::error(id, ErrorCode::NotFound, "no such session"),
    }
}
