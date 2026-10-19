//! The Unix half of `tty`: termios raw mode, `SIGWINCH`, `TIOCGWINSZ`, and
//! `poll` on stdin.
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

        // Publish the restore state and install the handlers *before* the
        // terminal goes raw. Restoring attributes the terminal still has is a
        // no-op, so publishing early costs nothing; publishing late leaves a
        // window in which a signal finds the terminal raw and nothing
        // wired up to undo it.
        // SAFETY: single write before `RESTORE_FD` is published; attach is
        // the only caller and enters raw mode once.
        unsafe { (*SAVED.original.get()).write(original) };
        RESTORE_FD.store(fd, std::sync::atomic::Ordering::Release);
        install_restore_handlers();

        // SAFETY: tcsetattr with a termios we just built.
        if unsafe { libc::tcsetattr(fd, libc::TCSANOW, &raw) } != 0 {
            // No guard will exist to unpublish the fd.
            RESTORE_FD.store(-1, std::sync::atomic::Ordering::Release);
            return Err(std::io::Error::last_os_error()).context("entering raw mode");
        }

        Ok(Some(Self { fd, original }))
    }
}

/// Signals that would end the process without unwinding, so `Drop` never
/// runs: a terminal left raw survives the process that did it.
///
/// `SIGINT` and `SIGQUIT` are here although the keyboard cannot raise them:
/// raw mode clears `ISIG`, so ^C and ^\ are ordinary bytes forwarded to the
/// session. What can raise them is another process -- `kill -INT`, `pkill`, a
/// script's cleanup -- which is exactly the "killed from another window"
/// case, and their default disposition ends the process without unwinding.
/// Nothing is swallowed: the handler re-raises with the default disposition.
/// `SIGHUP` is here because a closing terminal emulator sends it.
///
/// `SIGPIPE` is absent for a different reason: Rust ignores it at startup,
/// so a vanished stdout already surfaces as an ordinary `EPIPE` that
/// unwinds and runs `Drop`. Handling it would *replace* a clean exit with
/// a fatal signal — the restore would work and everything after it would
/// not.
const RESTORE_SIGNALS: [libc::c_int; 4] =
    [libc::SIGTERM, libc::SIGHUP, libc::SIGINT, libc::SIGQUIT];

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

/// Set by the `SIGWINCH` handler; cleared by whoever acts on it.
///
/// A flag rather than a self-pipe because the attach loop already polls
/// stdin on a short timeout, so it has a place to ask. `signal()` installs
/// a BSD-style handler — restarting, on the platforms anchor runs on — so
/// the poll is not reliably interrupted and the flag is what carries the
/// news across. The cost is that a resize lands within one poll timeout
/// rather than instantly, which is below what a redraw is visible at.
static RESIZED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Records that the terminal changed size. Async-signal-safe: one atomic
/// store, no allocation and no locks.
extern "C" fn note_resize(_signum: libc::c_int) {
    RESIZED.store(true, std::sync::atomic::Ordering::Release);
}

/// Starts watching for terminal size changes, for [`take_resize`].
///
/// Install this **before** reading the size for the first time, not after:
/// a resize between the read and the handler would otherwise be the one
/// change nothing ever hears about, and the session would stay the wrong
/// size until the user resized again.
pub fn watch_resizes() {
    // SAFETY: installing a handler that only stores to an atomic.
    unsafe {
        libc::signal(
            libc::SIGWINCH,
            note_resize as *const () as libc::sighandler_t,
        )
    };
}

/// Takes the pending size change, if there is one, and clears it.
///
/// Coalescing is the point: a drag across the screen is hundreds of
/// signals and one size that matters — the one the terminal has when the
/// caller asks.
pub fn take_resize() -> bool {
    RESIZED.swap(false, std::sync::atomic::Ordering::AcqRel)
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

/// Keystrokes from stdin, waited for with a timeout so the caller can keep
/// checking whether the session ended.
///
/// `read` on stdin cannot be interrupted by another thread, so a blocking
/// read would outlive the session it belongs to. Polling the descriptor
/// leaves the read itself blocking-but-ready, which keeps the input path
/// byte-for-byte what it was.
///
/// **The descriptor, not `std::io::stdin()`.** `Stdin` is an 8 KiB
/// `BufReader`: a paste longer than one read lands in its buffer, where
/// `poll` on the fd cannot see it, and the rest of the paste would sit there
/// until the next keystroke happened to make the fd readable again.
#[derive(Debug, Default)]
pub struct Keyboard;

impl Keyboard {
    pub fn new() -> Self {
        Self
    }

    /// Waits up to `wait` for keystrokes and reads what is there.
    pub fn read(&mut self, buf: &mut [u8], wait: std::time::Duration) -> super::Input {
        let timeout_ms = libc::c_int::try_from(wait.as_millis()).unwrap_or(libc::c_int::MAX);
        match stdin_readable(timeout_ms) {
            Readable::Yes => {}
            Readable::NotYet => return super::Input::Idle,
            Readable::Closed => return super::Input::Closed,
        }
        loop {
            // SAFETY: reading into a buffer we own, at most its length.
            let n = unsafe { libc::read(libc::STDIN_FILENO, buf.as_mut_ptr().cast(), buf.len()) };
            if n > 0 {
                return super::Input::Bytes(n as usize);
            }
            if n < 0 && std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return super::Input::Closed;
        }
    }
}

/// Outcome of waiting for stdin to become readable.
enum Readable {
    /// There are bytes to read.
    Yes,
    /// The wait expired with nothing to read.
    NotYet,
    /// stdin is gone, or the wait failed in a way retrying will not fix.
    Closed,
}

/// Waits up to `timeout_ms` for stdin to become readable.
fn stdin_readable(timeout_ms: libc::c_int) -> Readable {
    let mut fds = libc::pollfd {
        fd: libc::STDIN_FILENO,
        events: libc::POLLIN,
        revents: 0,
    };
    // SAFETY: one initialised `pollfd`, and the count matches.
    let ready = unsafe { libc::poll(&raw mut fds, 1, timeout_ms) };
    if ready < 0 {
        // EINTR is a signal, not a failure: the caller re-checks and waits
        // again rather than treating a suspend/resume as end-of-input.
        return if std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted {
            Readable::NotYet
        } else {
            Readable::Closed
        };
    }
    if ready == 0 {
        return Readable::NotYet;
    }
    // HUP without data still means read() will report EOF, so let the
    // caller's `read` observe it rather than deciding here.
    if fds.revents & (libc::POLLIN | libc::POLLHUP | libc::POLLERR) != 0 {
        Readable::Yes
    } else {
        Readable::NotYet
    }
}

/// Where session output goes: stdout, unparsed.
pub fn output() -> Box<dyn std::io::Write> {
    Box::new(std::io::stdout().lock())
}

/// Nothing to do on Unix: the terminal interprets VT sequences in any mode.
/// Exists so `attach` can ask without knowing the platform.
pub fn vt_output() -> Result<Option<()>> {
    Ok(None)
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

    /// Entering raw mode installs the handlers for every signal that ends
    /// the process without unwinding.
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

        // A `kill -INT` from another window is as fatal as a SIGTERM; only
        // the keyboard cannot raise these in raw mode.
        assert!(RESTORE_SIGNALS.contains(&libc::SIGINT));
        assert!(RESTORE_SIGNALS.contains(&libc::SIGQUIT));
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
