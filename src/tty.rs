//! Raw mode for `anchor attach`.
//!
//! The user's terminal already is a VT emulator, so attach is byte
//! passthrough in both directions: local keystrokes go to the PTY as exactly
//! the bytes the terminal produced, and output goes to stdout unparsed.
//! There is no VT parser here and there must never be one.
//!
//! `libc` directly rather than a terminal crate: `tcgetattr`/`tcsetattr` and
//! `TIOCGWINSZ` are the whole requirement, and they are already available.

use std::os::fd::{AsRawFd, RawFd};

use anyhow::{Context, Result};

/// Puts the terminal in raw mode and restores it on drop — including on a
/// panic or an error return, so a failure never leaves the user with a
/// terminal that does not echo.
pub struct RawMode {
    fd: RawFd,
    original: libc::termios,
}

impl RawMode {
    /// Enters raw mode on stdin. `None` when stdin is not a terminal, which
    /// is the piped/scripted case — there is nothing to put into raw mode.
    pub fn enter() -> Result<Option<Self>> {
        let fd = std::io::stdin().as_raw_fd();
        // SAFETY: isatty on a valid fd; no memory involved.
        if unsafe { libc::isatty(fd) } != 1 {
            return Ok(None);
        }

        // SAFETY: tcgetattr fills a termios we own.
        let mut original: libc::termios = unsafe { std::mem::zeroed() };
        if unsafe { libc::tcgetattr(fd, &mut original) } != 0 {
            return Err(std::io::Error::last_os_error()).context("reading terminal attributes");
        }

        let mut raw = original;
        // SAFETY: cfmakeraw only rewrites the termios we own.
        unsafe { libc::cfmakeraw(&mut raw) };
        // SAFETY: tcsetattr with a termios we just built.
        if unsafe { libc::tcsetattr(fd, libc::TCSANOW, &raw) } != 0 {
            return Err(std::io::Error::last_os_error()).context("entering raw mode");
        }
        Ok(Some(Self { fd, original }))
    }
}

impl Drop for RawMode {
    fn drop(&mut self) {
        // SAFETY: restoring the attributes we read in `enter`.
        unsafe {
            libc::tcsetattr(self.fd, libc::TCSANOW, &self.original);
        }
    }
}

/// The terminal's current size, for the initial resize and for SIGWINCH.
/// `None` when stdout is not a terminal.
pub fn window_size() -> Option<(u16, u16)> {
    let fd = std::io::stdout().as_raw_fd();
    // SAFETY: isatty on a valid fd.
    if unsafe { libc::isatty(fd) } != 1 {
        return None;
    }
    // SAFETY: TIOCGWINSZ fills a winsize we own.
    let mut size: libc::winsize = unsafe { std::mem::zeroed() };
    if unsafe { libc::ioctl(fd, libc::TIOCGWINSZ, &mut size) } != 0 {
        return None;
    }
    if size.ws_col == 0 || size.ws_row == 0 {
        return None;
    }
    Some((size.ws_col, size.ws_row))
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
