//! Raw mode for `anchor attach`.
//!
//! The user's terminal already is a VT emulator, so attach is byte
//! passthrough in both directions: local keystrokes go to the PTY as exactly
//! the bytes the terminal produced, and output goes to stdout unparsed.
//! There is no VT parser here and there must never be one.
//!
//! One interface, two halves: termios, `SIGWINCH` and `poll` on Unix
//! (`tty/unix.rs`); console modes, a polled window size and a reader thread
//! on Windows (`tty/windows.rs`, docs/09-windows.md §4.5). `attach` calls
//! the same five names on both -- [`RawMode`], [`Keyboard`], [`output`],
//! [`vt_output`] and the resize pair -- and the `~.` scanner below is
//! shared, because what a keystroke means does not depend on the kernel.

#[cfg(unix)]
mod unix;
#[cfg(unix)]
pub use unix::*;

#[cfg(windows)]
mod windows;
#[cfg(windows)]
pub use windows::*;

/// What one [`Keyboard::read`] found.
#[derive(Debug, PartialEq, Eq)]
pub enum Input {
    /// This many bytes were read into the buffer.
    Bytes(usize),
    /// Nothing arrived within the wait; ask again.
    Idle,
    /// stdin is gone, or failed in a way retrying will not fix.
    Closed,
}

/// Scans keystrokes for the `~.` detach escape.
///
/// Recognised only at the start of a line — right after a newline, or as the
/// very first thing typed — which is the ssh convention. That is what keeps
/// a literal `~.` in the middle of a command from detaching the session.
#[derive(Debug, Default)]
pub struct EscapeScanner {
    at_line_start: bool,
    saw_tilde: bool,
}

impl EscapeScanner {
    pub fn new() -> Self {
        Self {
            at_line_start: true,
            saw_tilde: false,
        }
    }

    /// Feeds input, returning the bytes to forward and whether to detach.
    ///
    /// A pending `~` is held back rather than forwarded: forwarding it and
    /// then detaching would leave a stray tilde in the session.
    pub fn feed(&mut self, bytes: &[u8]) -> (Vec<u8>, bool) {
        let mut out = Vec::with_capacity(bytes.len());
        for &byte in bytes {
            if self.saw_tilde {
                self.saw_tilde = false;
                if byte == b'.' {
                    return (out, true);
                }
                // Not the escape: the held tilde was literal after all.
                out.push(b'~');
                out.push(byte);
                self.at_line_start = matches!(byte, b'\r' | b'\n');
                continue;
            }
            if byte == b'~' && self.at_line_start {
                self.saw_tilde = true;
                continue;
            }
            self.at_line_start = matches!(byte, b'\r' | b'\n');
            out.push(byte);
        }
        (out, false)
    }
}

#[cfg(test)]
mod tests {
    use super::EscapeScanner;

    #[test]
    fn detaches_on_tilde_dot_at_a_line_start() {
        let mut scanner = EscapeScanner::new();
        let (out, detach) = scanner.feed(b"~.");
        assert!(detach);
        assert!(out.is_empty(), "the escape itself is never forwarded");
    }

    #[test]
    fn a_tilde_mid_line_is_literal() {
        let mut scanner = EscapeScanner::new();
        let (out, detach) = scanner.feed(b"ls ~/src\n");
        assert!(!detach);
        assert_eq!(out, b"ls ~/src\n");
    }

    #[test]
    fn a_tilde_then_something_else_forwards_both() {
        let mut scanner = EscapeScanner::new();
        let (out, detach) = scanner.feed(b"~x");
        assert!(!detach);
        assert_eq!(out, b"~x");
    }

    #[test]
    fn the_escape_is_recognised_after_a_newline() {
        let mut scanner = EscapeScanner::new();
        let (out, detach) = scanner.feed(b"echo hi\n");
        assert!(!detach);
        assert_eq!(out, b"echo hi\n");
        let (out, detach) = scanner.feed(b"~.");
        assert!(detach);
        assert!(out.is_empty());
    }

    #[test]
    fn a_split_escape_still_detaches() {
        // Keystrokes arrive one byte at a time in raw mode.
        let mut scanner = EscapeScanner::new();
        let (out, detach) = scanner.feed(b"~");
        assert!(!detach);
        assert!(out.is_empty(), "a pending tilde is held, not forwarded");
        let (out, detach) = scanner.feed(b".");
        assert!(detach);
        assert!(out.is_empty());
    }
}
