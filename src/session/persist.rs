//! Off-thread persistence for the hot path.
//!
//! The PTY reader thread must never wait on anything, and a `rusqlite` write
//! is a wait on a disk. Cached-offset updates therefore go onto a bounded
//! channel with `try_send` and are applied by this thread; a full channel
//! drops the update rather than stalling the reader, which is safe because
//! the value is a cache — startup reconciles it against the log file, and
//! the file is authoritative.
//!
//! Control-path writes (create, exit) are not routed through here. They run
//! on the caller's thread, where blocking on SQLite is fine.

use std::sync::mpsc::{self, SyncSender};
use std::sync::{Arc, Mutex};

use crate::store::Store;

/// Queued offset updates. Each is a whole-value overwrite, so dropping one
/// only costs precision until the next update or the next restart.
const QUEUE_CAPACITY: usize = 256;

/// One update: where a session's output stream has reached.
struct Update {
    id: String,
    output_bytes: u64,
    log_capped_at: Option<u64>,
}

/// The handle the reader thread holds. Cloneable and cheap.
#[derive(Clone)]
pub struct OutputPersister {
    tx: SyncSender<Update>,
}

impl std::fmt::Debug for OutputPersister {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("OutputPersister")
    }
}

impl OutputPersister {
    /// Starts the writer thread. It exits when every handle is dropped.
    pub fn start(store: Arc<Mutex<Store>>) -> Self {
        let (tx, rx) = mpsc::sync_channel::<Update>(QUEUE_CAPACITY);
        std::thread::Builder::new()
            .name("session-persist".into())
            .spawn(move || {
                for update in rx {
                    let store = match store.lock() {
                        Ok(store) => store,
                        Err(poisoned) => poisoned.into_inner(),
                    };
                    if let Err(e) = store.set_output_bytes(
                        &update.id,
                        update.output_bytes,
                        update.log_capped_at,
                    ) {
                        tracing::warn!(
                            session = %update.id,
                            error = %e,
                            "persisting the output offset failed"
                        );
                    }
                }
            })
            .expect("spawning the session-persist thread");
        Self { tx }
    }

    /// Queues an offset update. Never blocks: a full queue drops the update.
    pub fn note_output_bytes(&self, id: &str, output_bytes: u64, log_capped_at: Option<u64>) {
        let _ = self.tx.try_send(Update {
            id: id.to_string(),
            output_bytes,
            log_capped_at,
        });
    }
}
