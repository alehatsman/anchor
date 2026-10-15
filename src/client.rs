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

    /// Wraps an already-connected socket — the reader thread's half of an
    /// attached connection.
    pub fn from_stream(stream: UnixStream) -> Result<Self> {
        Ok(Self {
            reader: BufReader::new(stream.try_clone().context("cloning the socket")?),
            writer: stream,
            next_id: 0,
        })
    }

    /// A second handle on the same socket, for a reader thread.
    pub fn try_clone_stream(&self) -> Result<UnixStream> {
        self.writer.try_clone().context("cloning the socket")
    }
}
