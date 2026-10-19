//! A blocking client for the control socket.
//!
//! Synchronous on purpose: the consumers are a CLI and its tests, where a
//! runtime buys nothing and a thread per direction is the simpler shape.
//!
//! The transport is [`Stream`]: a `UnixStream` on Unix, a [`PipeStream`] on
//! Windows. A type alias rather than a trait (docs/09-windows.md §3.1): the
//! pipe type offers the handful of `UnixStream` methods this file calls —
//! `connect`, `try_clone`, `shutdown`, `Read`/`Write` — under the same
//! names, so nothing below is platform-conditional.

use std::io::{BufRead, BufReader, Read, Write};
use std::path::Path;

use anyhow::{bail, Context, Result};
use serde_json::{json, Value};

use crate::proto::frame;

/// One connection to anchord, as the platform spells it.
#[cfg(unix)]
pub type Stream = std::os::unix::net::UnixStream;
/// One connection to anchord, as the platform spells it.
#[cfg(windows)]
pub type Stream = pipe::PipeStream;

#[cfg(windows)]
pub use pipe::PipeStream;

/// The largest frame the daemon can legitimately send.
///
/// Live output is one pty chunk (64 KiB), but a catch-up frame carries a
/// whole replay round, which is `LIVE_GAP_BYTES`. Deriving it from that
/// constant rather than picking a round number means the cap moves with
/// the thing it is a cap on, instead of silently becoming wrong when the
/// gap is retuned.
const MAX_FRAME_BYTES: u64 = crate::session::LIVE_GAP_BYTES;

/// Reads one binary frame from `src`.
///
/// Shared by both readers on purpose: this existed twice, and the copy the
/// `attach` path actually uses was the one that would have been missed.
fn read_frame_from(src: &mut impl Read) -> Result<(u8, u64, Vec<u8>)> {
    let mut header = [0u8; frame::HEADER_LEN];
    src.read_exact(&mut header)
        .context("reading a frame header")?;
    let (tag, offset, len) = frame::decode_header(&header);
    // The daemon is trusted, but the number is not: a desynchronised
    // stream makes payload bytes look like a header, and a `u32` read out
    // of ordinary terminal output is a 4 GiB allocation and an abort --
    // a far worse way to learn the stream is out of step than an error
    // saying exactly that.
    if len as u64 > MAX_FRAME_BYTES {
        bail!(
            "the daemon sent a {len}-byte frame, past the {MAX_FRAME_BYTES}-byte maximum; \
             the stream is out of step"
        );
    }
    let mut payload = vec![0u8; len];
    src.read_exact(&mut payload)
        .context("reading a frame payload")?;
    Ok((tag, offset, payload))
}

/// One connection to `anchord`.
pub struct Client {
    reader: BufReader<Stream>,
    writer: Stream,
    next_id: u64,
}

impl Client {
    /// Connects to the daemon's socket.
    pub fn connect(socket: &Path) -> Result<Self> {
        let stream = Stream::connect(socket)
            .with_context(|| format!("connecting to {} — is anchord running?", socket.display()))?;
        Ok(Self {
            reader: BufReader::new(stream.try_clone().context("cloning the socket")?),
            writer: stream,
            next_id: 0,
        })
    }

    /// Sends a control request and returns its response, failing on an
    /// error response rather than handing back `ok:false`.
    pub fn call(&mut self, op: &str, body: Value) -> Result<Value> {
        let value = self.call_raw(op, body)?;
        if value["ok"] == json!(false) {
            let code = value["error"]["code"].as_str().unwrap_or("error");
            let message = value["error"]["message"].as_str().unwrap_or("");
            bail!("{code}: {message}");
        }
        Ok(value)
    }

    /// As [`call`](Self::call), but an error response is a value, not a
    /// failure — for a caller that wants to inspect the code.
    pub fn call_raw(&mut self, op: &str, mut body: Value) -> Result<Value> {
        self.next_id += 1;
        body["id"] = json!(self.next_id);
        body["op"] = json!(op);

        let mut line = serde_json::to_vec(&body)?;
        line.push(b'\n');
        let sent = self
            .writer
            .write_all(&line)
            .and_then(|()| self.writer.flush());

        // Read before acting on a failed write. The daemon answers a line it
        // refuses (`too_large`) and can close mid-send, so the write is the
        // part that fails and the reply is the part that says why --
        // docs/04-protocol.md says to read it first.
        let mut response = String::new();
        let read = self.reader.read_line(&mut response);
        // A line cut short by the daemon closing is not a reply: `read_line`
        // hands back what it got, and parsing half a line reports a
        // malformed response where the truth is a closed connection.
        if !matches!(read, Ok(n) if n > 0) || !response.ends_with('\n') {
            sent.context("sending a request")?;
            read.context("reading a response")?;
            bail!("the daemon closed the connection");
        }
        let value: Value =
            serde_json::from_str(&response).context("the daemon sent a malformed response")?;
        // Id 0 is the daemon's answer to a line it could not attribute to a
        // request -- too large, not UTF-8, not JSON -- so it can only be an
        // error, and it is the error the caller most needs to see.
        let unattributed = value["id"].as_u64() == Some(0) && value["ok"] == json!(false);
        if value["id"].as_u64() != Some(self.next_id) && !unattributed {
            bail!(
                "response id {} does not match request {}",
                value["id"],
                self.next_id
            );
        }
        Ok(value)
    }

    /// Reads one binary frame. Only valid after a successful `attach`.
    pub fn read_frame(&mut self) -> Result<(u8, u64, Vec<u8>)> {
        read_frame_from(&mut self.reader)
    }

    /// Writes an input frame. Only valid after a successful `attach` that
    /// granted control.
    pub fn write_input(&mut self, bytes: &[u8]) -> Result<()> {
        self.writer
            .write_all(&frame::encode(frame::INPUT, 0, bytes))
            .context("sending input")?;
        self.writer.flush().context("flushing input")?;
        Ok(())
    }

    /// Wraps an already-connected socket.
    pub fn from_connected(stream: Stream) -> Self {
        let reader = BufReader::new(stream.try_clone().expect("cloning a connected socket"));
        Self {
            reader,
            writer: stream,
            next_id: 0,
        }
    }

    /// Splits an attached connection into its reading and writing halves.
    ///
    /// The frame stream carries whatever this client's reader has already
    /// buffered past the attach response. A `BufReader` fills in chunks, so
    /// the first binary frames routinely arrive in the same read as the
    /// JSON line — building a fresh reader on the raw socket silently drops
    /// them, and the stream resumes mid-frame.
    pub fn into_attached(self) -> Result<(FrameStream, Stream)> {
        let buffered = self.reader.buffer().to_vec();
        let reading = self.reader.into_inner();
        Ok((FrameStream::new(buffered, reading)?, self.writer))
    }
}

/// The read half of an attached connection.
///
/// Owns any bytes the control reader had already buffered, so no frame is
/// lost at the handover from request/response to streaming.
pub struct FrameStream {
    inner: Box<dyn BufRead + Send>,
    /// Kept so the stream can be shut down from another thread, which is
    /// what unblocks a read that would otherwise wait forever on an idle
    /// session.
    socket: Stream,
}

impl std::fmt::Debug for FrameStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FrameStream")
    }
}

impl FrameStream {
    fn new(buffered: Vec<u8>, socket: Stream) -> Result<Self> {
        let replay = std::io::Cursor::new(buffered);
        let reader = BufReader::new(socket.try_clone().context("cloning the socket")?);
        Ok(Self {
            inner: Box::new(replay.chain(reader)),
            socket,
        })
    }

    /// Reads one binary frame.
    pub fn read_frame(&mut self) -> Result<(u8, u64, Vec<u8>)> {
        read_frame_from(&mut self.inner)
    }

    /// A handle that ends this stream's blocking read from another thread.
    pub fn interrupt_handle(&self) -> Result<Interrupt> {
        Ok(Interrupt {
            socket: self.socket.try_clone().context("cloning the socket")?,
        })
    }
}

/// Ends a [`FrameStream`]'s read, so a detaching client is not left waiting
/// on a session that happens to be quiet.
#[derive(Debug)]
pub struct Interrupt {
    socket: Stream,
}

impl Interrupt {
    /// Shuts the socket down in both directions. The blocked read returns
    /// immediately rather than waiting for output that may never come.
    ///
    /// A named pipe has no `shutdown` (docs/09-windows.md §4.1), so on
    /// Windows the same call signals the stream's stop event and the blocked
    /// read cancels its own I/O (`CancelIoEx`) — see [`PipeStream`].
    pub fn stop(&self) {
        let _ = self.socket.shutdown(std::net::Shutdown::Both);
    }
}

/// The Windows transport: a named-pipe client, verified before it is used.
#[cfg(windows)]
mod pipe {
    use std::io;
    use std::os::windows::ffi::OsStrExt;
    use std::os::windows::io::{
        AsHandle, AsRawHandle, BorrowedHandle, FromRawHandle, OwnedHandle, RawHandle,
    };
    use std::path::Path;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use windows_sys::Win32::Foundation::{
        ERROR_BROKEN_PIPE, ERROR_IO_PENDING, ERROR_OPERATION_ABORTED, ERROR_PIPE_BUSY,
        ERROR_PIPE_NOT_CONNECTED, GENERIC_READ, GENERIC_WRITE, HANDLE, INVALID_HANDLE_VALUE,
        WAIT_OBJECT_0, WAIT_TIMEOUT,
    };
    use windows_sys::Win32::Storage::FileSystem::{
        CreateFileW, GetFileType, ReadFile, WriteFile, FILE_FLAG_OVERLAPPED, FILE_TYPE_PIPE,
        OPEN_EXISTING, SECURITY_IDENTIFICATION, SECURITY_SQOS_PRESENT,
    };
    use windows_sys::Win32::System::Pipes::WaitNamedPipeW;
    use windows_sys::Win32::System::Threading::{
        CreateEventW, SetEvent, WaitForMultipleObjects, INFINITE,
    };
    use windows_sys::Win32::System::IO::{CancelIoEx, GetOverlappedResult, OVERLAPPED};

    /// Every anchor endpoint on Windows lives under this prefix.
    const PIPE_PREFIX: &str = r"\\.\pipe\";

    /// How many times a busy pipe is waited on before giving up. A round
    /// count, so it is a convergence bound and not a guess about a machine:
    /// anchord always has a listening instance between connections, so busy
    /// is a handoff in progress and one wait almost always clears it.
    const BUSY_ROUNDS: u32 = 10;
    /// How long one of those waits may take.
    const BUSY_WAIT_MS: u32 = 2000;

    /// A connected named-pipe client.
    ///
    /// **Overlapped, and that is load-bearing.** A pipe handle opened for
    /// synchronous I/O serialises every operation on it, so the attach
    /// path's blocked read on one thread would hold the keystroke writes on
    /// the other until output happened to arrive -- for a quiet session,
    /// never. Overlapped handles do not, and they are what makes a read
    /// stoppable: each read and write waits on its own completion event
    /// *and* on a shared stop event, and a stop cancels that operation's own
    /// I/O (`CancelIoEx`) and waits for the cancellation to land before the
    /// buffer is released.
    ///
    /// Clones share one handle and one stop event, which is what makes
    /// `shutdown` on any clone end a read blocked on another -- the same
    /// semantics as `UnixStream::shutdown` on a cloned fd.
    #[derive(Debug, Clone)]
    pub struct PipeStream {
        inner: Arc<Inner>,
    }

    #[derive(Debug)]
    struct Inner {
        handle: OwnedHandle,
        /// Manual-reset: once stopped, every later operation fails fast too.
        stop: OwnedHandle,
        /// 0 means none, as `UnixStream::set_read_timeout(None)`.
        read_timeout_ms: AtomicU64,
    }

    impl PipeStream {
        /// Opens `path` as an anchor pipe and verifies it is this user's
        /// before anything is sent (docs/04-protocol.md#windows-a-named-pipe).
        ///
        /// `SECURITY_IDENTIFICATION` is requested explicitly: the default
        /// lets the server impersonate the client fully, and a squatted or
        /// compromised server could then act as the user who connected
        /// (docs/09-windows.md §4.3).
        pub fn connect(path: impl AsRef<Path>) -> io::Result<Self> {
            let path = path.as_ref();
            if !path.as_os_str().to_string_lossy().starts_with(PIPE_PREFIX) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "{} is not a named pipe: on Windows the endpoint is \\\\.\\pipe\\<name>",
                        path.display()
                    ),
                ));
            }
            let wide: Vec<u16> = path.as_os_str().encode_wide().chain(Some(0)).collect();
            let handle = open(&wide)?;
            // SAFETY: a handle this process owns and has not yet wrapped.
            if unsafe { GetFileType(handle.as_raw_handle()) } != FILE_TYPE_PIPE {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("{} is not a pipe", path.display()),
                ));
            }
            verify_owner(&handle, path)?;
            Self::from_handle(handle)
        }

        fn from_handle(handle: OwnedHandle) -> io::Result<Self> {
            Ok(Self {
                inner: Arc::new(Inner {
                    handle,
                    stop: event()?,
                    read_timeout_ms: AtomicU64::new(0),
                }),
            })
        }

        /// Another handle on the same connection, as `UnixStream::try_clone`.
        pub fn try_clone(&self) -> io::Result<Self> {
            Ok(self.clone())
        }

        /// Ends the connection for every clone: a read or write blocked on
        /// any of them returns `ConnectionAborted`, and so does every later
        /// one. `how` is ignored, because a pipe cannot half-close.
        pub fn shutdown(&self, _how: std::net::Shutdown) -> io::Result<()> {
            // SAFETY: a valid event handle this stream owns.
            if unsafe { SetEvent(self.inner.stop.as_raw_handle()) } == 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        }

        /// As `UnixStream::set_read_timeout`: a read waiting longer fails
        /// with `TimedOut`. Shared by every clone, like the socket option.
        pub fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
            let ms = timeout.map_or(0, |t| {
                u64::try_from(t.as_millis()).unwrap_or(u64::MAX).max(1)
            });
            self.inner.read_timeout_ms.store(ms, Ordering::Relaxed);
            Ok(())
        }

        /// A connected pair, for tests that need a pipe with no daemon on
        /// the other end -- the Windows `UnixStream::pair`.
        #[cfg(test)]
        pub fn pair() -> io::Result<(Self, Self)> {
            use windows_sys::Win32::Storage::FileSystem::{
                FILE_FLAG_FIRST_PIPE_INSTANCE, PIPE_ACCESS_DUPLEX,
            };
            use windows_sys::Win32::System::Pipes::{
                CreateNamedPipeW, PIPE_READMODE_BYTE, PIPE_REJECT_REMOTE_CLIENTS, PIPE_TYPE_BYTE,
                PIPE_WAIT,
            };
            let name = format!(r"\\.\pipe\anchor-test-{}", crate::model::new_session_id());
            let wide: Vec<u16> = name.encode_utf16().chain(Some(0)).collect();
            let user = crate::winsec::UserSid::current().map_err(io::Error::other)?;
            let sd = crate::winsec::SecurityDescriptor::owner_only_pipe(&user)
                .map_err(io::Error::other)?;
            let attributes = sd.attributes();
            // SAFETY: a NUL-terminated name and attributes that outlive the call.
            let server = unsafe {
                CreateNamedPipeW(
                    wide.as_ptr(),
                    PIPE_ACCESS_DUPLEX | FILE_FLAG_OVERLAPPED | FILE_FLAG_FIRST_PIPE_INSTANCE,
                    PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_WAIT | PIPE_REJECT_REMOTE_CLIENTS,
                    1,
                    65536,
                    65536,
                    0,
                    &attributes,
                )
            };
            if server == INVALID_HANDLE_VALUE {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: just created, owned by nothing else.
            let server = unsafe { OwnedHandle::from_raw_handle(server) };
            // The client's open completes the connection; the server side
            // needs no ConnectNamedPipe once a client is already attached.
            let client = Self::connect(&name)?;
            Ok((client, Self::from_handle(server)?))
        }

        pub(crate) fn raw(&self) -> HANDLE {
            self.inner.handle.as_raw_handle()
        }

        /// Runs one overlapped operation to completion, or until stopped or
        /// timed out -- in which case it is cancelled, and the cancellation
        /// is waited for, before the buffer it was given is released.
        fn overlapped(
            &self,
            timeout_ms: u64,
            start: impl FnOnce(HANDLE, *mut OVERLAPPED) -> i32,
        ) -> io::Result<usize> {
            let done = event()?;
            // SAFETY: a zeroed OVERLAPPED is the documented initial state.
            let mut ov: OVERLAPPED = unsafe { std::mem::zeroed() };
            ov.hEvent = done.as_raw_handle();

            if self.stopped() {
                return Err(stopped());
            }
            if start(self.raw(), &mut ov) == 0 {
                let err = io::Error::last_os_error();
                if err.raw_os_error() != Some(ERROR_IO_PENDING as i32) {
                    return Err(err);
                }
                let waits = [done.as_raw_handle(), self.inner.stop.as_raw_handle()];
                let timeout = if timeout_ms == 0 {
                    INFINITE
                } else {
                    u32::try_from(timeout_ms).unwrap_or(INFINITE - 1)
                };
                // SAFETY: two valid event handles.
                let woke = unsafe { WaitForMultipleObjects(2, waits.as_ptr(), 0, timeout) };
                if woke != WAIT_OBJECT_0 {
                    // Stopped, or timed out: cancel this operation only, and
                    // let the cancellation land so `ov` and the buffer are
                    // no longer the kernel's when this returns.
                    // SAFETY: `ov` is the in-flight operation on this handle.
                    unsafe { CancelIoEx(self.raw(), &ov) };
                    let mut n = 0u32;
                    // SAFETY: waits for that same operation to finish.
                    let finished = unsafe { GetOverlappedResult(self.raw(), &ov, &mut n, 1) };
                    if finished != 0 {
                        // It completed before the cancel took: keep the data.
                        return Ok(n as usize);
                    }
                    return Err(if woke == WAIT_TIMEOUT {
                        io::Error::new(io::ErrorKind::TimedOut, "read timed out")
                    } else {
                        stopped()
                    });
                }
            }
            let mut n = 0u32;
            // SAFETY: the operation has completed; this only collects it.
            if unsafe { GetOverlappedResult(self.raw(), &ov, &mut n, 1) } == 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(n as usize)
        }

        fn stopped(&self) -> bool {
            use windows_sys::Win32::System::Threading::WaitForSingleObject;
            // SAFETY: polling a valid event without waiting.
            unsafe { WaitForSingleObject(self.inner.stop.as_raw_handle(), 0) == WAIT_OBJECT_0 }
        }
    }

    /// The pipe handle, borrowed -- so a caller can ask the OS about the
    /// connection (its owner, its DACL) the way it would an fd.
    impl AsRawHandle for PipeStream {
        fn as_raw_handle(&self) -> RawHandle {
            self.inner.handle.as_raw_handle()
        }
    }

    impl AsHandle for PipeStream {
        fn as_handle(&self) -> BorrowedHandle<'_> {
            self.inner.handle.as_handle()
        }
    }

    impl io::Read for &PipeStream {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            if buf.is_empty() {
                return Ok(0);
            }
            let len = u32::try_from(buf.len()).unwrap_or(u32::MAX);
            let ptr = buf.as_mut_ptr();
            let timeout = self.inner.read_timeout_ms.load(Ordering::Relaxed);
            match self.overlapped(timeout, |h, ov| {
                // SAFETY: `ptr` is valid for `len` bytes until `overlapped`
                // returns, which waits out the operation in every path.
                unsafe { ReadFile(h, ptr, len, std::ptr::null_mut(), ov) }
            }) {
                // The server closed its end: the stream's EOF.
                Err(e)
                    if matches!(
                        e.raw_os_error().map(|c| c as u32),
                        Some(ERROR_BROKEN_PIPE | ERROR_PIPE_NOT_CONNECTED)
                    ) =>
                {
                    Ok(0)
                }
                other => other,
            }
        }
    }

    impl io::Read for PipeStream {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            (&*self).read(buf)
        }
    }

    impl io::Write for &PipeStream {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            if buf.is_empty() {
                return Ok(0);
            }
            let len = u32::try_from(buf.len()).unwrap_or(u32::MAX);
            let ptr = buf.as_ptr();
            self.overlapped(0, |h, ov| {
                // SAFETY: as for `read`: the buffer outlives the operation.
                unsafe { WriteFile(h, ptr, len, std::ptr::null_mut(), ov) }
            })
            .map_err(|e| {
                if e.raw_os_error().map(|c| c as u32) == Some(ERROR_BROKEN_PIPE) {
                    io::Error::new(io::ErrorKind::BrokenPipe, e)
                } else {
                    e
                }
            })
        }

        /// Nothing to do: a completed write is already in the pipe.
        /// `FlushFileBuffers` would block until the *daemon* had read it all.
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl io::Write for PipeStream {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            (&*self).write(buf)
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn stopped() -> io::Error {
        io::Error::new(
            io::ErrorKind::ConnectionAborted,
            io::Error::from_raw_os_error(ERROR_OPERATION_ABORTED as i32),
        )
    }

    /// A manual-reset, initially unsignalled event.
    fn event() -> io::Result<OwnedHandle> {
        // SAFETY: no attributes, no name; the handle is owned below.
        let h = unsafe { CreateEventW(std::ptr::null(), 1, 0, std::ptr::null()) };
        if h.is_null() {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: a valid handle nothing else owns.
        Ok(unsafe { OwnedHandle::from_raw_handle(h as RawHandle) })
    }

    /// `CreateFileW`, waiting out a busy pipe for a bounded number of rounds.
    fn open(wide: &[u16]) -> io::Result<OwnedHandle> {
        for _ in 0..BUSY_ROUNDS {
            // SAFETY: a NUL-terminated name; the handle is owned below.
            let h = unsafe {
                CreateFileW(
                    wide.as_ptr(),
                    GENERIC_READ | GENERIC_WRITE,
                    0,
                    std::ptr::null(),
                    OPEN_EXISTING,
                    FILE_FLAG_OVERLAPPED | SECURITY_SQOS_PRESENT | SECURITY_IDENTIFICATION,
                    std::ptr::null_mut(),
                )
            };
            if h != INVALID_HANDLE_VALUE {
                // SAFETY: a valid handle nothing else owns.
                return Ok(unsafe { OwnedHandle::from_raw_handle(h) });
            }
            let err = io::Error::last_os_error();
            if err.raw_os_error() != Some(ERROR_PIPE_BUSY as i32) {
                return Err(err);
            }
            // SAFETY: a NUL-terminated name.
            unsafe { WaitNamedPipeW(wide.as_ptr(), BUSY_WAIT_MS) };
        }
        Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "every instance of the pipe stayed busy",
        ))
    }

    /// Refuses a pipe this user did not create.
    ///
    /// anchord writes its user as the pipe's owner, and only a process
    /// running as that user -- or holding a privilege that already owns the
    /// machine -- can create a pipe owned by them. Anything else at this
    /// name is a squatter, and checking *before* the first request is the
    /// point: the request is what it wants.
    fn verify_owner(handle: &OwnedHandle, path: &Path) -> io::Result<()> {
        let me = crate::winsec::UserSid::current()
            .and_then(|sid| sid.to_sid_string())
            .map_err(io::Error::other)?;
        check_owner(handle.as_handle(), path, &me)
    }

    /// The comparison itself, against an expected SID in string form --
    /// split out so a test can expect someone else and watch it refuse,
    /// which a suite running as one user cannot otherwise arrange. String
    /// forms are canonical (`ConvertSidToStringSidW`), so equal strings are
    /// equal SIDs.
    pub(super) fn check_owner(
        handle: BorrowedHandle<'_>,
        path: &Path,
        expected: &str,
    ) -> io::Result<()> {
        let owner = crate::winsec::handle_owner(handle).map_err(io::Error::other)?;
        if owner == expected {
            return Ok(());
        }
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "{} is owned by {owner}, not by this user ({expected}): refusing to talk \
                 to a pipe another user created (docs/09-windows.md §4.2)",
                path.display(),
            ),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// The handover must not lose bytes that arrived with the response.
    ///
    /// A `BufReader` fills in chunks, so a daemon that writes the attach
    /// response and the first frames in quick succession can land both in
    /// one read. Rebuilding a reader on the raw socket silently discards
    /// whatever was already buffered, and the stream resumes mid-frame.
    ///
    /// Written against a socket pair rather than a live daemon on purpose:
    /// coalescing is a scheduling accident, so an end-to-end test passes
    /// whether or not the bug is present. Here it is guaranteed — one
    /// `write` of both parts.
    #[test]
    fn the_attach_handover_keeps_bytes_that_arrived_with_the_response() {
        let (mine, theirs) = Stream::pair().expect("socket pair");
        // Bounded, so losing the frame fails the assertion instead of
        // blocking forever on bytes that were discarded.
        mine.set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .expect("read timeout");
        let mut client = Client::from_connected(mine);

        // One write: a response line immediately followed by a frame.
        let payload = b"hello";
        let mut wire = Vec::new();
        wire.extend_from_slice(b"{\"id\":1,\"ok\":true}\n");
        wire.extend_from_slice(&frame::encode(frame::OUTPUT, 0, payload));
        (&theirs).write_all(&wire).expect("write");
        (&theirs).flush().expect("flush");

        let response = client
            .call_raw("attach", serde_json::json!({}))
            .expect("attach");
        assert_eq!(response["ok"], serde_json::json!(true));

        // The frame arrived in the same read as the line above, so it is
        // sitting in the reader's buffer right now.
        let (mut frames, _writer) = client.into_attached().expect("split");
        let (tag, offset, bytes) = frames
            .read_frame()
            .expect("a frame buffered with the response was lost at the handover");
        assert_eq!(tag, frame::OUTPUT);
        assert_eq!(offset, 0);
        assert_eq!(
            bytes, payload,
            "a frame buffered with the response was lost"
        );
    }

    /// A frame header claiming more than the daemon can produce is an
    /// error, not an allocation.
    ///
    /// `len` is a `u32` read off the wire, so the worst case is a 4 GiB
    /// request. The daemon is trusted, but this number is not reached by
    /// trusting it -- a stream that has slipped out of step turns ordinary
    /// terminal output into a header, and the client aborts on an
    /// allocation instead of saying what went wrong. Both readers are
    /// checked: this lived in two copies, and the one `attach` actually
    /// uses was the easier to miss.
    #[test]
    fn a_frame_larger_than_the_daemon_can_send_is_refused() {
        for split in [false, true] {
            let (mine, theirs) = Stream::pair().expect("socket pair");
            mine.set_read_timeout(Some(std::time::Duration::from_secs(5)))
                .expect("read timeout");
            let mut client = Client::from_connected(mine);

            let mut wire = Vec::new();
            if split {
                wire.extend_from_slice(b"{\"id\":1,\"ok\":true}\n");
            }
            // A header alone: the point is that nothing is allocated for
            // the payload it promises, so the payload is never sent.
            let mut header = frame::encode(frame::OUTPUT, 0, &[]);
            let at = header.len() - 4;
            header[at..].copy_from_slice(&u32::MAX.to_be_bytes());
            wire.extend_from_slice(&header);
            (&theirs).write_all(&wire).expect("write");
            (&theirs).flush().expect("flush");

            let err = if split {
                client
                    .call_raw("attach", serde_json::json!({}))
                    .expect("attach");
                let (mut frames, _writer) = client.into_attached().expect("split");
                frames.read_frame().unwrap_err()
            } else {
                client.read_frame().unwrap_err()
            };
            assert!(
                err.to_string().contains("out of step"),
                "expected a desync complaint, got: {err}"
            );
        }
    }

    /// The other side of the cap: a frame at exactly the maximum is legal.
    ///
    /// A catch-up frame carries a whole replay round, so the largest
    /// legitimate frame is much bigger than a live one. A cap set to the
    /// live chunk size would look correct in every test that only attaches
    /// to a quiet session, and break the moment a client reconnected far
    /// enough behind to need a full round -- which is the case the offsets
    /// exist for.
    #[test]
    fn a_frame_at_exactly_the_maximum_is_not_refused() {
        let (mine, theirs) = Stream::pair().expect("socket pair");
        mine.set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .expect("read timeout");
        let mut client = Client::from_connected(mine);

        // Deliberately `LIVE_GAP_BYTES` and not `MAX_FRAME_BYTES`: a test
        // written against the constant it is checking passes for any value
        // of it, including a wrong one. This asserts against the thing the
        // cap is a cap *on* -- the largest round the daemon will ever send
        // -- so setting the cap below it fails here.
        let largest = crate::session::LIVE_GAP_BYTES;
        let mut header = frame::encode(frame::OUTPUT, 0, &[]);
        let at = header.len() - 4;
        header[at..].copy_from_slice(&(largest as u32).to_be_bytes());
        (&theirs).write_all(&header).expect("write");
        drop(theirs); // no payload follows: the read ends, it is not refused

        let err = client.read_frame().unwrap_err().to_string();
        assert!(
            !err.contains("out of step"),
            "the maximum legal frame was refused as a desync: {err}"
        );
        assert!(
            err.contains("frame payload"),
            "expected the truncated payload to be what failed, got: {err}"
        );
    }
}

/// The Windows transport's own properties, against a real pipe pair with no
/// daemon on the other end.
#[cfg(all(test, windows))]
mod pipe_tests {
    use super::*;
    use std::os::windows::io::AsHandle;
    use std::sync::mpsc;
    use std::time::Duration;

    /// docs/09-windows.md §4.1: a named pipe has no `shutdown`, and this is
    /// the one place the Unix design needed a different mechanism. A detach
    /// from a quiet session must end the read that is waiting on it.
    #[test]
    fn a_stop_ends_a_read_blocked_on_a_quiet_pipe() {
        let (mine, _theirs) = Stream::pair().expect("pipe pair");
        let interrupt = Interrupt {
            socket: mine.try_clone().unwrap(),
        };
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let mut buf = [0u8; 16];
            let _ = tx.send((&mine).read(&mut buf));
        });
        // Nothing will ever arrive: the read must be blocked, not finished.
        assert!(
            rx.recv_timeout(Duration::from_millis(300)).is_err(),
            "the read returned with nothing to read, so stopping it proves nothing"
        );
        interrupt.stop();
        let ended = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("stop() did not end a read blocked on a quiet pipe");
        assert_eq!(
            ended
                .expect_err("a stopped read is an error, not data")
                .kind(),
            std::io::ErrorKind::ConnectionAborted
        );
    }

    /// A synchronous pipe handle serialises every operation on it, so a
    /// read blocked on one thread holds a write on another -- the attach
    /// path's keystrokes would wait for output that a quiet session never
    /// produces. This is why the client is overlapped.
    #[test]
    fn a_write_is_not_held_behind_a_blocked_read() {
        let (mine, theirs) = Stream::pair().expect("pipe pair");
        let reader = mine.try_clone().unwrap();
        std::thread::spawn(move || {
            let mut buf = [0u8; 16];
            let _ = (&reader).read(&mut buf);
        });
        std::thread::sleep(Duration::from_millis(200)); // let the read block
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let _ = tx.send((&mine).write_all(b"typed while a read waits"));
        });
        rx.recv_timeout(Duration::from_secs(5))
            .expect("the write waited behind the blocked read")
            .expect("write");
        let mut got = [0u8; 24];
        (&theirs).read_exact(&mut got).unwrap();
        assert_eq!(&got, b"typed while a read waits");
    }

    /// docs/09-windows.md §4.2, the client's half: a pipe whose owner is
    /// not the caller is refused, naming both. The pipe here is this user's
    /// own -- creating one owned by another user needs a privilege this
    /// suite does not have -- so the check is told to expect SYSTEM, and
    /// the owner it reads off the kernel must disagree.
    #[test]
    fn a_pipe_owned_by_someone_else_is_refused() {
        let (mine, _theirs) = Stream::pair().expect("pipe pair");
        let path = Path::new(r"\\.\pipe\test");
        let me = crate::winsec::UserSid::current()
            .unwrap()
            .to_sid_string()
            .unwrap();
        pipe::check_owner(mine.as_handle(), path, &me).expect("this user's own pipe is accepted");

        let err = pipe::check_owner(mine.as_handle(), path, "S-1-5-18")
            .expect_err("a pipe owned by this user passed as SYSTEM's");
        assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied);
        assert!(
            err.to_string().contains(&me) && err.to_string().contains("S-1-5-18"),
            "the refusal should name the owner and the expected user: {err}"
        );
    }

    /// docs/09-windows.md §4.3: the client connects at
    /// `SECURITY_IDENTIFICATION`, so the server end can learn who connected
    /// but cannot act as them. Asked of the kernel, from the server's side.
    #[test]
    fn the_server_can_identify_the_client_but_not_impersonate_it() {
        use windows_sys::Win32::Security::{
            GetTokenInformation, RevertToSelf, SecurityIdentification, TokenImpersonationLevel,
            TOKEN_QUERY,
        };
        use windows_sys::Win32::System::Pipes::ImpersonateNamedPipeClient;
        use windows_sys::Win32::System::Threading::{GetCurrentThread, OpenThreadToken};

        let (mine, theirs) = Stream::pair().expect("pipe pair");
        // Impersonation needs something read from the pipe first.
        (&mine).write_all(b"x").unwrap();
        let mut one = [0u8; 1];
        (&theirs).read_exact(&mut one).unwrap();

        // On a thread of its own, so the impersonation cannot leak into
        // whichever test runs next on this one.
        let level = std::thread::spawn(move || {
            // SAFETY: the server end of a connected pipe.
            assert_ne!(unsafe { ImpersonateNamedPipeClient(theirs.raw()) }, 0);
            let mut token = std::ptr::null_mut();
            // SAFETY: this thread's token, opened as itself (`TRUE`).
            let opened = unsafe { OpenThreadToken(GetCurrentThread(), TOKEN_QUERY, 1, &mut token) };
            let mut level = -1i32;
            let mut len = 0u32;
            if opened != 0 {
                // SAFETY: a token handle and an i32-sized out-buffer.
                unsafe {
                    GetTokenInformation(
                        token,
                        TokenImpersonationLevel,
                        std::ptr::from_mut(&mut level).cast(),
                        4,
                        &mut len,
                    );
                    windows_sys::Win32::Foundation::CloseHandle(token);
                }
            }
            // SAFETY: ends this thread's impersonation.
            unsafe { RevertToSelf() };
            level
        })
        .join()
        .unwrap();
        assert_eq!(
            level, SecurityIdentification,
            "the server got more than identification: it could act as the client"
        );
    }
}
