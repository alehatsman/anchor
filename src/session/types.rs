//! Plain data shared across the `session` module.
//!
//! Session identity, state and lost reasons live in [`crate::model`], which
//! is already the widened version of what teleport keeps here — this file
//! holds only what the fan-out needs.

use std::sync::Arc;

/// One chunk of output, tagged with the offset of its first byte — the
/// contract a subscriber needs in order to reconnect without a gap or a
/// duplicate.
///
/// `bytes` is an `Arc<[u8]>` so fanning out to N subscribers costs N refcount
/// clones, not N copies of the chunk.
#[derive(Debug, Clone)]
pub struct Chunk {
    /// Offset of `bytes`'s first byte in the session's output stream.
    pub offset: u64,
    /// The chunk's payload.
    pub bytes: Arc<[u8]>,
}
