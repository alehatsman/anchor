//! The unix socket server.
//!
//! Newline-delimited JSON in and out, one task per connection. The socket is
//! 0600: filesystem permissions are the security boundary, so there is no
//! auth and no TCP.
//!
//! **Anything that can wait on disk, a child process or a blocked pty goes
//! through `spawn_blocking`.** A worker held by one connection stalls every
//! other connection scheduled on it. `create` (it may run the login shell),
//! `terminate`, `purge` and `log` follow the rule; the attached connection's
//! input path is in `stream.rs`.
//!
//! [`bind`] is deliberately synchronous and returns a `std` listener, so a
//! caller can claim the socket — and fail loudly if another daemon already
//! holds it — before any runtime exists. [`serve`] adopts it.
//!
//! **On Windows the socket is a named pipe** (docs/09-windows.md §3), and
//! `0600` is spelled as a DACL granting the creating user's SID and nothing
//! else (`winsec`). Still no auth and no TCP: the OS is the boundary on both
//! kernels. Two Unix behaviours are deliberately *not* ported. A stale
//! socket is probed and replaced; a taken pipe name is refused, because on
//! Windows replacing it is the same act as being squatted (§4.2). And there
//! is no length limit to check.
//!
//! The seam is a type alias per platform — [`Listener`], [`ConnRead`],
//! [`ConnWrite`] — not a trait. Every use site needs only read, write and
//! split, and a trait would buy dynamic dispatch on the attach path for an
//! abstraction nothing calls (§3.1). Platform code stays in this file and
//! in `client.rs`.

use std::collections::BTreeMap;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
#[cfg(unix)]
use std::os::unix::net::UnixListener as StdUnixListener;
use std::path::Path;
use std::sync::{Arc, Mutex as StdMutex};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
#[cfg(unix)]
use tokio::net::UnixListener;

use crate::proto::{self, ErrorCode, Request, SpecWire};
use crate::session::{CreateError, SessionManager};
use crate::store::Store;

/// What [`bind`] claims and [`serve`] accepts on.
#[cfg(unix)]
pub type Listener = StdUnixListener;
/// What [`bind`] claims and [`serve`] accepts on.
#[cfg(windows)]
pub type Listener = PipeListener;

/// The read half of one client connection.
#[cfg(unix)]
pub type ConnRead = tokio::net::unix::OwnedReadHalf;
/// The read half of one client connection.
#[cfg(windows)]
pub type ConnRead = tokio::io::ReadHalf<tokio::net::windows::named_pipe::NamedPipeServer>;

/// The write half of one client connection.
#[cfg(unix)]
pub type ConnWrite = tokio::net::unix::OwnedWriteHalf;
/// The write half of one client connection.
#[cfg(windows)]
pub type ConnWrite = tokio::io::WriteHalf<tokio::net::windows::named_pipe::NamedPipeServer>;

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

/// How much of a refused line is read and thrown away after `too_large`,
/// and for how long, so closing does not reset the reply away.
const TOO_LARGE_DRAIN_BYTES: u64 = 16 * MAX_LINE_BYTES;
const TOO_LARGE_DRAIN_TIME: std::time::Duration = std::time::Duration::from_secs(2);

/// The usable length of `sockaddr_un.sun_path`, minus the NUL.
///
/// 104 on macOS and the BSDs, 108 on Linux; the smaller is used on both
/// because a daemon that starts here and not there is worse than one that
/// gives the same answer everywhere.
#[cfg(unix)]
const MAX_SOCKET_PATH: usize = 103;

/// Binds the socket, refusing to displace a daemon that is already running.
///
/// A socket file left behind by a dead daemon is stale: nothing accepts on
/// it, so it is removed and rebound.
#[cfg(unix)]
pub fn bind(socket_path: &Path) -> Result<Listener> {
    // Checked before anything else, because the kernel's own answer is
    // `EINVAL` with no mention of length -- which reads as "the path is
    // wrong" rather than "the path is too long", and sends you looking at
    // the wrong half of it. The limit is small enough to hit by accident:
    // a data dir under a long home or a temp dir is most of the budget.
    let len = socket_path.as_os_str().as_encoded_bytes().len();
    if len > MAX_SOCKET_PATH {
        bail!(
            "socket path is {len} bytes, past the {MAX_SOCKET_PATH}-byte limit \
             a unix socket address can hold: {}. Use a shorter directory \
             (XDG_RUNTIME_DIR, or `anchor daemon --socket`).",
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

/// A claimed pipe name: its first instance, and the security every later
/// instance is created with.
///
/// The first instance is a raw handle rather than a tokio server, because
/// adopting a handle onto tokio needs a runtime and [`bind`] runs before
/// one exists — the same reason the Unix arm returns a `std` listener.
#[cfg(windows)]
pub struct PipeListener {
    name: std::ffi::OsString,
    security: crate::winsec::SecurityDescriptor,
    first: std::os::windows::io::OwnedHandle,
}

#[cfg(windows)]
impl std::fmt::Debug for PipeListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PipeListener")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

/// The pipe prefix every anchor endpoint on Windows must carry. Anything
/// else handed to `--socket` is a filesystem path, which is a mistake here.
#[cfg(windows)]
pub(crate) const PIPE_PREFIX: &str = r"\\.\pipe\";

/// Claims the pipe name, **failing if anything already holds it**.
///
/// Not the Unix arm's probe-and-replace. A pipe name that exists is either
/// a running anchord — refuse, like Unix — or a squatter: another process
/// that created the name first to receive this user's spawn requests and
/// session output (docs/09-windows.md §4.2). There is no third case to
/// tidy up: a pipe vanishes with its last handle, so "stale" does not exist.
/// `FILE_FLAG_FIRST_PIPE_INSTANCE` makes the create itself fail on a taken
/// name, so there is no window between checking and claiming.
///
/// The pipe is created with an owner-only DACL and this user as its owner
/// (`SecurityDescriptor::owner_only_pipe`), rejects remote clients, and is
/// byte mode — the protocol is a byte stream, newline-delimited.
#[cfg(windows)]
pub fn bind(pipe: &Path) -> Result<Listener> {
    let name = pipe.as_os_str().to_os_string();
    if !name.to_string_lossy().starts_with(PIPE_PREFIX) {
        bail!(
            "{} is not a named pipe: on Windows the endpoint is \\\\.\\pipe\\<name>",
            pipe.display()
        );
    }
    let user = crate::winsec::UserSid::current()?;
    let security = crate::winsec::SecurityDescriptor::owner_only_pipe(&user)?;
    match create_first_instance(&name, &security) {
        Ok(first) => Ok(PipeListener {
            name,
            security,
            first,
        }),
        Err(e) => Err(explain_taken(pipe, &e)),
    }
}

/// `CreateNamedPipeW` for the first instance, with
/// `FILE_FLAG_FIRST_PIPE_INSTANCE`. Every flag here must match what
/// `create_next_instance` asks tokio for, or Windows refuses the later ones.
#[cfg(windows)]
fn create_first_instance(
    name: &std::ffi::OsStr,
    security: &crate::winsec::SecurityDescriptor,
) -> std::io::Result<std::os::windows::io::OwnedHandle> {
    use std::os::windows::ffi::OsStrExt;
    use std::os::windows::io::{FromRawHandle, OwnedHandle};
    use windows_sys::Win32::Foundation::INVALID_HANDLE_VALUE;
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_FLAG_FIRST_PIPE_INSTANCE, FILE_FLAG_OVERLAPPED, PIPE_ACCESS_DUPLEX,
    };
    use windows_sys::Win32::System::Pipes::{
        CreateNamedPipeW, PIPE_READMODE_BYTE, PIPE_REJECT_REMOTE_CLIENTS, PIPE_TYPE_BYTE,
        PIPE_UNLIMITED_INSTANCES, PIPE_WAIT,
    };

    let wide: Vec<u16> = name.encode_wide().chain(Some(0)).collect();
    let attributes = security.attributes();
    // SAFETY: a NUL-terminated name and security attributes that outlive the
    // call; the returned handle is owned below.
    let handle = unsafe {
        CreateNamedPipeW(
            wide.as_ptr(),
            PIPE_ACCESS_DUPLEX | FILE_FLAG_OVERLAPPED | FILE_FLAG_FIRST_PIPE_INSTANCE,
            PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_WAIT | PIPE_REJECT_REMOTE_CLIENTS,
            PIPE_UNLIMITED_INSTANCES,
            PIPE_BUFFER_BYTES,
            PIPE_BUFFER_BYTES,
            0,
            &attributes,
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: a valid handle this function just created and nothing else owns.
    Ok(unsafe { OwnedHandle::from_raw_handle(handle) })
}

/// The pipe's in and out buffer sizes: tokio's own default, written down so
/// the first instance and the rest agree.
#[cfg(windows)]
const PIPE_BUFFER_BYTES: u32 = 65536;

/// Every instance after the first, with the same security as the first.
#[cfg(windows)]
fn create_next_instance(
    name: &std::ffi::OsStr,
    security: &crate::winsec::SecurityDescriptor,
) -> std::io::Result<tokio::net::windows::named_pipe::NamedPipeServer> {
    let mut attributes = security.attributes();
    // SAFETY: `attributes` points at a descriptor `security` keeps alive for
    // the duration of the call.
    unsafe {
        tokio::net::windows::named_pipe::ServerOptions::new()
            .pipe_mode(tokio::net::windows::named_pipe::PipeMode::Byte)
            .reject_remote_clients(true)
            .in_buffer_size(PIPE_BUFFER_BYTES)
            .out_buffer_size(PIPE_BUFFER_BYTES)
            .create_with_security_attributes_raw(name, std::ptr::from_mut(&mut attributes).cast())
    }
}

/// Says *who* holds a pipe name that could not be claimed, as far as this
/// user can tell, without ever displacing it.
///
/// Probing is safe in a way removing never is: a connection that closes
/// costs a real anchord nothing. The probe is the client's own check, so a
/// pipe this user owns reads as a running daemon and anything else reads as
/// what it is.
#[cfg(windows)]
fn explain_taken(pipe: &Path, error: &std::io::Error) -> anyhow::Error {
    use windows_sys::Win32::Foundation::{ERROR_ACCESS_DENIED, ERROR_PIPE_BUSY};
    let taken = matches!(
        error.raw_os_error().map(|c| c as u32),
        Some(ERROR_ACCESS_DENIED | ERROR_PIPE_BUSY)
    );
    if !taken {
        return anyhow::anyhow!("creating the pipe {}: {error}", pipe.display());
    }
    match crate::client::Stream::connect(pipe) {
        Ok(_) => anyhow::anyhow!("anchord is already running on {}", pipe.display()),
        Err(probe) => anyhow::anyhow!(
            "{} already exists and is not this user's anchord ({probe:#}). Refusing to \
             start, and not replacing it: on Windows a pipe name someone else holds is \
             a squatter, not a stale socket (docs/09-windows.md §4.2).",
            pipe.display()
        ),
    }
}

/// Accepts connections, one task per client, and never returns once setup
/// succeeds: a failed `accept` is logged and retried, because the ones that
/// matter (`EMFILE`) are transient and ending the daemon over one would end
/// every session with it. Errors only if the listener cannot be adopted.
///
/// Takes the `std` listener from [`bind`] and adopts it onto the runtime this
/// is polled on.
#[cfg(unix)]
pub async fn serve(listener: Listener, state: State) -> Result<()> {
    listener
        .set_nonblocking(true)
        .context("setting the listener non-blocking")?;
    let listener = UnixListener::from_std(listener).context("adopting the listener")?;

    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let state = state.clone();
                tokio::spawn(async move {
                    let (read, write) = stream.into_split();
                    if let Err(e) = handle_connection(read, write, state).await {
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

/// The Windows accept loop: one pipe instance waits for a client, and the
/// next is created **before** the connected one is handed off.
///
/// That order is the security property, not a detail. A pipe name exists
/// only while some instance of it does, so a moment with no listening
/// instance and no live connection is a moment the name is free — for a
/// squatter to take (§4.2). Creating the next first means the connected
/// instance keeps the name alive across the gap. A failure to create one is
/// retried, holding the connected client until it succeeds, rather than
/// risking that gap.
#[cfg(windows)]
pub async fn serve(listener: Listener, state: State) -> Result<()> {
    use std::os::windows::io::IntoRawHandle;
    use tokio::net::windows::named_pipe::NamedPipeServer;

    let PipeListener {
        name,
        security,
        first,
    } = listener;
    // Moved, never duplicated: a second handle to an instance keeps it open
    // after tokio closes its own, and the client on it would never see EOF.
    // SAFETY: an overlapped pipe handle `bind` created and owned outright;
    // ownership moves into tokio here.
    let mut server = unsafe { NamedPipeServer::from_raw_handle(first.into_raw_handle()) }
        .context("adopting the pipe")?;

    loop {
        if let Err(e) = server.connect().await {
            // A client that connected and left before this instance noticed
            // leaves it unusable until it is disconnected; recycle it.
            tracing::warn!(error = %e, "accepting on the pipe failed");
            let _ = server.disconnect();
            tokio::time::sleep(ACCEPT_RETRY_DELAY).await;
            continue;
        }
        let next = loop {
            match create_next_instance(&name, &security) {
                Ok(next) => break next,
                Err(e) => {
                    tracing::warn!(error = %e, "creating the next pipe instance failed");
                    tokio::time::sleep(ACCEPT_RETRY_DELAY).await;
                }
            }
        };
        let connected = std::mem::replace(&mut server, next);
        let state = state.clone();
        tokio::spawn(async move {
            let (read, write) = tokio::io::split(connected);
            if let Err(e) = handle_connection(read, write, state).await {
                tracing::warn!(error = %e, "connection ended with an error");
            }
        });
    }
}

async fn handle_connection(
    read_half: ConnRead,
    mut write_half: ConnWrite,
    state: State,
) -> Result<()> {
    let mut reader = BufReader::new(read_half);
    // Bytes, not a `String`: a `read_line` into a `String` fails the whole
    // read on invalid UTF-8, which drops the connection with no reply.
    // docs/04-protocol.md promises a line that is not a request object is
    // answered `bad_request`, and that has to include one that is not text.
    let mut raw: Vec<u8> = Vec::new();
    // One connection is one client, for the control lease -- see
    // `stream::serve_attached` for why that is the honest reading.
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
                &proto::error(0, ErrorCode::TooLarge, "request line too large"),
            )
            .await?;
            // The rest of that line is unread; the client reconnects rather
            // than desynchronising the framing.
            //
            // Not a bare close, though. Closing a unix socket with bytes
            // still in its receive queue makes Linux reset the connection,
            // and a reset can discard the reply above before the client
            // reads it -- `ECONNRESET` where `too_large` was owed, seen on
            // GitHub's ubuntu runner (anchor#33). So: end our half, then
            // drain what the client is still sending, bounded in bytes and
            // time so a client that never stops cannot hold the task.
            let _ = write_half.shutdown().await;
            let _ = tokio::time::timeout(
                TOO_LARGE_DRAIN_TIME,
                tokio::io::copy(
                    &mut (&mut reader).take(TOO_LARGE_DRAIN_BYTES),
                    &mut tokio::io::sink(),
                ),
            )
            .await;
            return Ok(());
        }
        // Answered, not dropped. The whole line was consumed, so the
        // framing is still intact and the connection stays usable --
        // closing here would make a stray byte indistinguishable from the
        // daemon dying.
        let line = match std::str::from_utf8(&raw) {
            Ok(line) => line,
            Err(_) => {
                respond(
                    &mut write_half,
                    &proto::error(0, ErrorCode::BadRequest, "request line is not valid UTF-8"),
                )
                .await?;
                continue;
            }
        };

        if line.trim().is_empty() {
            continue;
        }

        // Parsed once, here: the op decides the route, and a line that is
        // not a request object is answered `bad_request` for every op alike.
        let request: Request = match serde_json::from_str(line) {
            Ok(r) => r,
            Err(e) => {
                // Salvage the id if the line was an object carrying one, so
                // the client can still correlate the failure.
                let id = serde_json::from_str::<Value>(line)
                    .ok()
                    .and_then(|v| v.get("id").and_then(Value::as_u64))
                    .unwrap_or(0);
                respond(
                    &mut write_half,
                    &proto::error(id, ErrorCode::BadRequest, format!("malformed request: {e}")),
                )
                .await?;
                continue;
            }
        };

        // `attach` is the one op that can stop returning to this loop: on
        // success the connection carries binary frames from here on. A
        // refused attach is an ordinary error response and the connection
        // stays a control connection.
        if request.op == "attach" {
            match attach_check(request.id, line, &state) {
                Err(err) => {
                    respond(&mut write_half, &err).await?;
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
                        &proto::ok(
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

        // `log` answers with a typed reply that is serialised straight to
        // the wire, so it cannot ride `dispatch`'s `Value`.
        if request.op == "log" {
            match op_log(request.id, line, &state).await {
                Ok(reply) => respond(&mut write_half, &reply).await?,
                Err(err) => respond(&mut write_half, &err).await?,
            }
        } else {
            respond(&mut write_half, &dispatch(&request, line, &state).await).await?;
        }
    }
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
    id: u64,
    line: &str,
    state: &State,
) -> Result<(Arc<crate::session::Session>, AttachReq), Value> {
    let req: AttachReq = serde_json::from_str(line)
        .map_err(|e| proto::error(id, ErrorCode::InvalidSpec, e.to_string()))?;

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
async fn dispatch(request: &Request, line: &str, state: &State) -> Value {
    match request.op.as_str() {
        "ping" => proto::ok(request.id, json!({ "pong": true })),
        "create" => op_create(request.id, line, state).await,
        "list" => op_list(request.id, line, state),
        "get" => op_get(request.id, line, state),
        "label" => op_label(request.id, line, state),
        "resize" => op_resize(request.id, line, state),
        "terminate" => op_terminate(request.id, line, state).await,
        "purge" => op_purge(request.id, line, state).await,
        other => proto::error(
            request.id,
            ErrorCode::UnknownOp,
            format!("unknown op {other:?}"),
        ),
    }
}

async fn respond(out: &mut (impl AsyncWriteExt + Unpin), value: &impl Serialize) -> Result<()> {
    let mut buf = serde_json::to_vec(value)?;
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

async fn op_create(id: u64, line: &str, state: &State) -> Value {
    let req: CreateReq = match parse(id, line) {
        Ok(r) => r,
        Err(e) => return e,
    };
    let spec = match req.spec.resolve() {
        Ok(spec) => spec,
        Err(e) => return proto::error(id, ErrorCode::InvalidSpec, format!("resolving cwd: {e}")),
    };

    // Off the worker: `create` may run the user's login shell to completion
    // to resolve the command, then writes the store and opens the pty. A
    // slow `.zshrc` would otherwise hold a runtime worker, and every other
    // connection on it, for as long as the shell takes to start.
    let manager = Arc::clone(&state.manager);
    let labels = req.labels;
    let created = match tokio::task::spawn_blocking(move || manager.create(&spec, labels)).await {
        Ok(created) => created,
        Err(e) => return proto::error(id, ErrorCode::Internal, format!("create: {e}")),
    };
    match created {
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
/// Shared with the label path deliberately: a second mapping with its own
/// catch-all gave the same failure one code from `create` and a different,
/// wrong one from `label`. Matching exhaustively here means a new variant is
/// a compile error rather than a silent reclassification.
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
        CreateError::Spawn(_) => ErrorCode::SpawnFailed,
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
        Ok(Some(session)) => proto::ok(id, json!({ "session": proto::session_json(&session) })),
        Ok(None) => proto::error(id, ErrorCode::NotFound, "no such session"),
        Err(e) => store_failed(id, &e),
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
    let row = match store.get(&req.session) {
        Ok(Some(row)) => row,
        Ok(None) => return proto::error(id, ErrorCode::NotFound, "no such session"),
        Err(e) => return store_failed(id, &e),
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
            format!(
                "cols and rows must be between {} and {}",
                crate::session::manager::SIZE_RANGE.start(),
                crate::session::manager::SIZE_RANGE.end()
            ),
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

/// A `log` reply, serialised straight to the wire.
///
/// Building this as a `json!` tree turned each of the range's up to 4 MiB
/// into its own `Value` -- `size_of::<Value>()` bytes apiece -- so the cap
/// that bounds one read was not the cap on the memory it cost. Fields are in
/// alphabetical order because `Value`'s map is sorted, and the bytes on the
/// wire are meant to be exactly what they always were.
#[derive(Serialize)]
struct LogReply {
    /// Raw PTY bytes are not UTF-8 in general -- control sequences, and
    /// partial multi-byte characters at a chunk boundary. A lossy conversion
    /// would corrupt the stream, so they go out as an array of numbers.
    bytes: Vec<u8>,
    from: u64,
    id: u64,
    log_capped_at: Option<u64>,
    next_offset: u64,
    ok: bool,
    to: u64,
}

async fn op_log(id: u64, line: &str, state: &State) -> Result<LogReply, Value> {
    let req: LogReq = parse(id, line)?;
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
    let row = match lookup(&req.session, state) {
        Ok(Some(row)) => row,
        Ok(None) => return Err(proto::error(id, ErrorCode::NotFound, "no such session")),
        Err(e) => return Err(store_failed(id, &e)),
    };
    let path = state
        .manager
        .root()
        .join(&row.id)
        .join(crate::log::LOG_FILE_NAME);
    let from = req.from;
    // Off the worker: up to 4 MiB from a disk that may be slow.
    let read = tokio::task::spawn_blocking(move || {
        crate::log::LogReader::open(&path).map(|mut r| r.read_range(from, from.saturating_add(max)))
    })
    .await;
    let bytes = match read {
        Ok(Ok(Ok(bytes))) => bytes,
        // Only a missing file is `not_found`. A log that exists and cannot
        // be opened -- permissions, a bad disk -- is the daemon's problem,
        // and answering `not_found` for it claims the session's output is
        // gone when it is sitting right there.
        Ok(Err(e)) => {
            let code = if e.kind() == std::io::ErrorKind::NotFound {
                ErrorCode::NotFound
            } else {
                ErrorCode::Internal
            };
            return Err(proto::error(id, code, format!("opening the log: {e}")));
        }
        // The range was checked; this is the log file itself failing.
        Ok(Ok(Err(e))) => {
            return Err(proto::error(
                id,
                ErrorCode::Internal,
                format!("reading the log: {e}"),
            ))
        }
        Err(e) => return Err(proto::error(id, ErrorCode::Internal, format!("log: {e}"))),
    };
    Ok(LogReply {
        from,
        to: from + bytes.len() as u64,
        next_offset: row.output_bytes,
        log_capped_at: row.log_capped_at,
        id,
        ok: true,
        bytes,
    })
}

async fn op_purge(id: u64, line: &str, state: &State) -> Value {
    let req: SessionReq = match parse(id, line) {
        Ok(r) => r,
        Err(e) => return e,
    };
    // Nothing to forget is not a failure to forget it, but an id that was
    // never known is still a 404 — a client purging a typo should hear so.
    match lookup(&req.session, state) {
        Ok(Some(_)) => {}
        Ok(None) => return proto::error(id, ErrorCode::NotFound, "no such session"),
        // A failing store is not an absent record: answering `not_found`
        // would tell the client the purge is moot when nothing was checked.
        Err(e) => return store_failed(id, &e),
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
///
/// `Err` is the store failing to read, which is not the same answer as `None`
/// (no such session): `not_found` means "do not retry", and a client told
/// that by a broken database believes a record is gone that is still there.
fn lookup(id: &str, state: &State) -> anyhow::Result<Option<crate::model::Session>> {
    if let Some(session) = state.manager.get(id) {
        return Ok(Some(session.snapshot()));
    }
    state.stored_row(id)
}

/// The store failing to read: the daemon's problem, never the request's.
fn store_failed(id: u64, e: &anyhow::Error) -> Value {
    proto::error(id, ErrorCode::Internal, format!("{e:#}"))
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
        Ok(None) => proto::error(id, ErrorCode::NotFound, "no such session"),
        Err(e) => store_failed(id, &e),
    }
}
