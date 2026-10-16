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

/// The terminal state to restore, readable from a signal handler.
///
/// A handler cannot take a lock or allocate, so the saved attributes live
/// in a static behind an atomic fd: `-1` means "nothing to restore", and
/// the fd is published only after the attributes are written. `tcsetattr`
/// itself is async-signal-safe, which is the whole reason this can be done
/// from a handler at all.
struct Saved {
    original: std::cell::UnsafeCell<std::mem::MaybeUninit<libc::termios>>,
}

// SAFETY: the `UnsafeCell` is written exactly once, before `RESTORE_FD` is
// set to anything but -1, and only read after observing a non-negative
// `RESTORE_FD` with `Acquire`.
unsafe impl Sync for Saved {}

/// Uninitialised until `enter_on` writes it, which is exactly what
/// `MaybeUninit` is for — a zeroed `termios` would be a lie about a value
/// that must never be read before it is set.
static SAVED: Saved = Saved {
    original: std::cell::UnsafeCell::new(std::mem::MaybeUninit::uninit()),
};

static RESTORE_FD: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(-1);

/// Restores the saved attributes, if any. Async-signal-safe: one atomic
/// load and one `tcsetattr`, no allocation and no locks.
extern "C" fn restore_and_reraise(signum: libc::c_int) {
    restore_saved();
    // Die of the signal rather than exiting 0: a process killed by SIGTERM
    // must still look killed to whatever is waiting on it.
    // SAFETY: restoring the default disposition and re-raising is the
    // standard way to do that.
    unsafe {
        libc::signal(signum, libc::SIG_DFL);
        libc::raise(signum);
    }
}

/// The restoring half, without the re-raise, so it can be called from a
/// test that would rather not die of the signal it is testing.
fn restore_saved() {
    let fd = RESTORE_FD.load(std::sync::atomic::Ordering::Acquire);
    if fd >= 0 {
        // SAFETY: `fd` was published after `original` was written, so the
        // value behind the cell is initialised and never mutated again.
        unsafe { libc::tcsetattr(fd, libc::TCSANOW, (*SAVED.original.get()).as_ptr()) };
    }
}

/// Puts the terminal in raw mode and restores it on drop — including on a
/// panic or an error return, so a failure never leaves the user with a
/// terminal that does not echo.
///
/// Drop does not run when the process is *signalled*, though, and a client
/// attached to a long-running session is exactly the thing someone kills
/// from another window. That would leave the terminal with no echo and no
/// line editing, needing a blind `reset` to recover, so the same restore
/// is also wired to the signals that would otherwise end the process
/// without unwinding.
pub struct RawMode {
    fd: RawFd,
    original: libc::termios,
}

impl RawMode {
    /// Enters raw mode on stdin. `None` when stdin is not a terminal, which
    /// is the piped/scripted case — there is nothing to put into raw mode.
    pub fn enter() -> Result<Option<Self>> {
        Self::enter_on(std::io::stdin().as_raw_fd())
    }

    /// Enters raw mode on an arbitrary terminal fd. Exists so the restore
    /// path can be tested against a pty of the test's own rather than the
    /// test runner's stdin.
    fn enter_on(fd: RawFd) -> Result<Option<Self>> {
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

        // Publish for the signal handler only after raw mode is actually
        // on: a handler that fired before this point would have nothing to
        // undo, and restoring attributes the terminal still has is worse
        // than doing nothing only if the fd is wrong.
        // SAFETY: single write before `RESTORE_FD` is published; attach is
        // the only caller and enters raw mode once.
        unsafe { (*SAVED.original.get()).write(original) };
        RESTORE_FD.store(fd, std::sync::atomic::Ordering::Release);
        install_restore_handlers();

        Ok(Some(Self { fd, original }))
    }
}

/// Signals that would end the process without unwinding, so `Drop` never
/// runs: a terminal left raw survives the process that did it.
///
/// `SIGINT` and `SIGQUIT` are deliberately absent — raw mode means ^C and
/// ^\ are ordinary bytes forwarded to the session, not signals to this
/// process, and handling them here would swallow them. `SIGHUP` is here
/// because a closing terminal emulator sends it.
///
/// `SIGPIPE` is absent for a different reason: Rust ignores it at startup,
/// so a vanished stdout already surfaces as an ordinary `EPIPE` that
/// unwinds and runs `Drop`. Handling it would *replace* a clean exit with
/// a fatal signal — the restore would work and everything after it would
/// not.
const RESTORE_SIGNALS: [libc::c_int; 2] = [libc::SIGTERM, libc::SIGHUP];

fn install_restore_handlers() {
    for signum in RESTORE_SIGNALS {
        // SAFETY: installing a handler that only loads an atomic, calls
        // `tcsetattr`, and re-raises — all async-signal-safe.
        unsafe {
            libc::signal(
                signum,
                restore_and_reraise as *const () as libc::sighandler_t,
            )
        };
    }
}

impl Drop for RawMode {
    fn drop(&mut self) {
        // Unpublish first, so a signal arriving during or after this does
        // not set attributes on an fd this process may have since reused.
        RESTORE_FD.store(-1, std::sync::atomic::Ordering::Release);
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

#[cfg(test)]
mod raw_mode_tests {
    use super::*;

    /// `RESTORE_FD` and `SAVED` are process-wide by design -- a signal
    /// handler cannot be handed a context. These tests therefore contend
    /// for them, and run one at a time. `parking_lot` because a panicking
    /// test must not poison the lock and turn one failure into three.
    static TEST_LOCK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

    /// A pty pair, so these tests act on a terminal of their own rather
    /// than the test runner's stdin -- which is usually not a terminal at
    /// all, and must never be left raw if it is.
    fn open_pty() -> (RawFd, RawFd) {
        let mut master = 0;
        let mut slave = 0;
        // SAFETY: openpty fills two fds we own; null for the optional
        // name, termios and winsize arguments.
        let rc = unsafe {
            libc::openpty(
                &mut master,
                &mut slave,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        };
        assert_eq!(rc, 0, "openpty failed");
        (master, slave)
    }

    fn attrs(fd: RawFd) -> libc::termios {
        // SAFETY: tcgetattr fills a termios we own, on a valid fd.
        let mut t: libc::termios = unsafe { std::mem::zeroed() };
        assert_eq!(unsafe { libc::tcgetattr(fd, &mut t) }, 0);
        t
    }

    /// The whole point of the signal path: a client killed from another
    /// window must not leave the terminal raw.
    ///
    /// Exercised through `restore_saved`, the half the handler calls --
    /// the re-raise is what makes the real handler untestable in-process,
    /// and it is three lines of `SIG_DFL` and `raise` either side of this.
    #[test]
    fn the_saved_attributes_are_restored_without_the_guard_dropping() {
        let _serialised = TEST_LOCK.lock();
        let (master, slave) = open_pty();
        let before = attrs(slave);
        assert_ne!(before.c_lflag & libc::ECHO, 0, "a fresh pty echoes");

        let guard = RawMode::enter_on(slave)
            .expect("enter")
            .expect("a terminal");
        assert_eq!(
            attrs(slave).c_lflag & libc::ECHO,
            0,
            "raw mode did not take effect, so restoring it proves nothing"
        );

        // What the handler does when the process is signalled: the guard
        // is still alive and will never be dropped.
        restore_saved();
        assert_ne!(
            attrs(slave).c_lflag & libc::ECHO,
            0,
            "a signalled client would leave the terminal with no echo"
        );

        drop(guard);
        // SAFETY: fds this test opened.
        unsafe {
            libc::close(master);
            libc::close(slave);
        }
    }

    /// Dropping the guard un-publishes the fd, so a signal arriving later
    /// cannot set terminal attributes on a descriptor the process has
    /// since closed and reused for something else entirely.
    #[test]
    fn a_dropped_guard_leaves_nothing_for_a_later_signal_to_restore() {
        let _serialised = TEST_LOCK.lock();
        let (master, slave) = open_pty();
        let guard = RawMode::enter_on(slave)
            .expect("enter")
            .expect("a terminal");
        assert!(RESTORE_FD.load(std::sync::atomic::Ordering::Acquire) >= 0);
        drop(guard);
        assert_eq!(
            RESTORE_FD.load(std::sync::atomic::Ordering::Acquire),
            -1,
            "a stale fd stayed published after the guard was dropped"
        );

        // And calling the restore path now is a no-op rather than a write
        // to whatever owns that number today.
        restore_saved();

        // SAFETY: fds this test opened.
        unsafe {
            libc::close(master);
            libc::close(slave);
        }
    }

    /// Entering raw mode installs the handlers, and not for the signals
    /// raw mode turns into ordinary bytes.
    #[test]
    fn the_handlers_cover_the_signals_that_skip_drop() {
        let _serialised = TEST_LOCK.lock();
        let (master, slave) = open_pty();
        let guard = RawMode::enter_on(slave)
            .expect("enter")
            .expect("a terminal");

        for signum in RESTORE_SIGNALS {
            // SAFETY: querying the current disposition without changing it.
            let mut current: libc::sigaction = unsafe { std::mem::zeroed() };
            assert_eq!(
                unsafe { libc::sigaction(signum, std::ptr::null(), &mut current) },
                0
            );
            assert_eq!(
                current.sa_sigaction, restore_and_reraise as *const () as libc::sighandler_t,
                "signal {signum} would still end the process with the terminal raw"
            );
        }

        // ^C and ^\ are bytes for the session in raw mode, not signals to
        // this process; handling them here would swallow them.
        assert!(!RESTORE_SIGNALS.contains(&libc::SIGINT));
        assert!(!RESTORE_SIGNALS.contains(&libc::SIGQUIT));
        // And `SIGPIPE` stays ignored, so a vanished stdout unwinds and
        // runs `Drop` instead of killing the process mid-restore.
        assert!(!RESTORE_SIGNALS.contains(&libc::SIGPIPE));
        // SAFETY: querying the disposition without changing it.
        let mut pipe: libc::sigaction = unsafe { std::mem::zeroed() };
        assert_eq!(
            unsafe { libc::sigaction(libc::SIGPIPE, std::ptr::null(), &mut pipe) },
            0
        );
        assert_eq!(
            pipe.sa_sigaction,
            libc::SIG_IGN,
            "SIGPIPE stopped being ignored; a broken stdout now kills attach"
        );

        drop(guard);
        // SAFETY: fds this test opened.
        unsafe {
            libc::close(master);
            libc::close(slave);
        }
    }
}
