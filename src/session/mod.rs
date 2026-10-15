//! Session ownership: the output log, the subscriber fan-out, and the PTY
//! behind them.
//!
//! See docs/03-pty.md. The one rule everything here serves: **the PTY reader
//! must never wait for a subscriber.**

pub mod fanout;
pub mod replay;
pub mod types;

pub use fanout::{Subscription, MAX_QUEUE_BYTES};
pub use replay::{Attach, AttachError, Replay, ReplayStep};
pub use types::Chunk;
