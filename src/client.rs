//! A blocking client for the control socket.
//!
//! Synchronous on purpose: the consumers are a CLI and its tests, where a
//! runtime buys nothing and a thread per direction is the simpler shape.

use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;

use anyhow::{bail, Context, Result};
use serde_json::{json, Value};

use crate::proto::frame;

/// One connection to `anchord`.
pub struct Client {
    reader: BufReader<UnixStream>,
    writer: UnixStream,
    next_id: u64,
}

impl Client {
    /// Connects to the daemon's socket.
    pub fn connect(socket: &Path) -> Result<Self> {
        let stream = UnixStream::connect(socket)
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
        self.writer.write_all(&line).context("sending a request")?;
        self.writer.flush()?;

        let mut response = String::new();
        if self
            .reader
            .read_line(&mut response)
            .context("reading a response")?
            == 0
        {
            bail!("the daemon closed the connection");
        }
        let value: Value =
            serde_json::from_str(&response).context("the daemon sent a malformed response")?;
        if value["id"].as_u64() != Some(self.next_id) {
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
        let mut header = [0u8; frame::HEADER_LEN];
        self.reader
            .read_exact(&mut header)
            .context("reading a frame header")?;
        let (tag, offset, len) = frame::decode_header(&header);
        let mut payload = vec![0u8; len];
        self.reader
            .read_exact(&mut payload)
            .context("reading a frame payload")?;
        Ok((tag, offset, payload))
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
    pub fn from_connected(stream: UnixStream) -> Self {
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
    pub fn into_attached(self) -> Result<(FrameStream, UnixStream)> {
        let buffered = self.reader.buffer().to_vec();
        let reading = self.reader.into_inner();
        Ok((FrameStream::new(buffered, reading), self.writer))
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
    socket: UnixStream,
}

impl std::fmt::Debug for FrameStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FrameStream")
    }
}

impl FrameStream {
    fn new(buffered: Vec<u8>, socket: UnixStream) -> Self {
        let replay = std::io::Cursor::new(buffered);
        let reader = BufReader::new(
            socket
                .try_clone()
                .unwrap_or_else(|_| socket.try_clone().expect("cloning the socket")),
        );
        Self {
            inner: Box::new(replay.chain(reader)),
            socket,
        }
    }

    /// Reads one binary frame.
    pub fn read_frame(&mut self) -> Result<(u8, u64, Vec<u8>)> {
        let mut header = [0u8; frame::HEADER_LEN];
        self.inner
            .read_exact(&mut header)
            .context("reading a frame header")?;
        let (tag, offset, len) = frame::decode_header(&header);
        let mut payload = vec![0u8; len];
        self.inner
            .read_exact(&mut payload)
            .context("reading a frame payload")?;
        Ok((tag, offset, payload))
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
    socket: UnixStream,
}

impl Interrupt {
    /// Shuts the socket down in both directions. The blocked read returns
    /// immediately rather than waiting for output that may never come.
    pub fn stop(&self) {
        let _ = self.socket.shutdown(std::net::Shutdown::Both);
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
        let (mine, theirs) = UnixStream::pair().expect("socket pair");
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
}
