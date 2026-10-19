//! The control lease: exactly one writer of input and size.
//!
//! See docs/01-scope.md — one effective PTY size is the thing anchor is
//! allowed to be opinionated about, because two clients at different widths
//! cannot arbitrate it between themselves.
//!
//! A lease is `(holder, epoch)`. The epoch is what tells two connections
//! sharing one holder id apart: only the one holding the *current* epoch
//! counts, so a stale release from a connection that already lost the lease
//! cannot clear whoever holds it now.
//!
//! teleport's disconnect-grace window is deliberately not ported: a grace
//! period is policy about which *client* a reconnect belongs to, and anchor's
//! lease is per-connection. teleport keeps its own lease layer above this one
//! and that is what shipped through M4 and M5 — see docs/06-teleport-seam.md,
//! which carries the one question still open here.

use super::Session;

impl Session {
    /// Takes the lease if it is free, or renews it if `holder` already has
    /// it. Never preempts: an attach that steals control out from under
    /// another client is how two clients end up fighting over one PTY.
    ///
    /// Returns the epoch this caller now holds, or `None` if someone else
    /// holds the lease.
    pub fn take_control(&self, holder: &str) -> Option<u64> {
        let mut lease = self.control.lock();
        match lease.holder.as_deref() {
            Some(current) if current != holder => None,
            _ => {
                lease.holder = Some(holder.to_string());
                lease.epoch += 1;
                Some(lease.epoch)
            }
        }
    }

    /// Takes the lease unconditionally, preempting the current holder.
    /// Returns the epoch this caller now holds.
    ///
    /// Announces the change, so the connection that just lost it is told
    /// rather than left to discover it when its next keystroke vanishes.
    pub fn claim_control(&self, holder: &str) -> u64 {
        let epoch = {
            let mut lease = self.control.lock();
            lease.holder = Some(holder.to_string());
            lease.epoch += 1;
            lease.epoch
        };
        // Fails only when nothing is attached, which needs no announcement.
        let _ = self.lease_changes.send(holder.to_string());
        epoch
    }

    /// Releases the lease, if `holder` still holds it at `epoch`.
    pub fn release_control(&self, holder: &str, epoch: u64) {
        let mut lease = self.control.lock();
        if lease.holder.as_deref() == Some(holder) && lease.epoch == epoch {
            lease.holder = None;
        }
    }

    /// Whether `holder` at `epoch` is the current controller.
    pub fn is_controller(&self, holder: &str, epoch: u64) -> bool {
        let lease = self.control.lock();
        lease.holder.as_deref() == Some(holder) && lease.epoch == epoch
    }

    /// The current lease holder, if any.
    pub fn controller(&self) -> Option<String> {
        self.control.lock().holder.clone()
    }

    /// Checks the lease and writes under one acquisition of it.
    ///
    /// Checking and writing as two calls leaves a window where a concurrent
    /// claim moves the lease in between, and a just-preempted client's
    /// keystrokes still reach the PTY. `Err(None)` means not the controller;
    /// `Err(Some(e))` means the write itself failed.
    pub fn write_if_controller(
        &self,
        holder: &str,
        epoch: u64,
        bytes: &[u8],
    ) -> Result<(), Option<anyhow::Error>> {
        let lease = self.control.lock();
        if lease.holder.as_deref() != Some(holder) || lease.epoch != epoch {
            return Err(None);
        }
        // Still holding the lease: a concurrent claim blocks on this same
        // mutex and cannot move the holder until this write has gone out.
        self.write(bytes).map_err(Some)
    }

    /// Resizes under the same rule as [`write_if_controller`](Self::write_if_controller).
    pub fn resize_if_controller(
        self: &std::sync::Arc<Self>,
        holder: &str,
        epoch: u64,
        cols: u16,
        rows: u16,
    ) -> Result<(), Option<anyhow::Error>> {
        {
            let lease = self.control.lock();
            if lease.holder.as_deref() != Some(holder) || lease.epoch != epoch {
                return Err(None);
            }
        }
        // The lease is released before resizing: unlike a write, a resize
        // can be deferred by coalescing, and holding the lease across that
        // wait would block every other lease operation for the window.
        self.resize(cols, rows).map_err(Some)
    }
}
