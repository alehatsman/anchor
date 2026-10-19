//! The Windows half of `tty`: console modes instead of termios, a polled
//! window size instead of `SIGWINCH`, and a reader thread instead of `poll`
//! (docs/09-windows.md §4.5).
//!
//! **Raw is three mode bits and a code page.** Clearing `ENABLE_LINE_INPUT`,
//! `ENABLE_ECHO_INPUT` and `ENABLE_PROCESSED_INPUT` is what `cfmakeraw` is
//! to a Unix tty: no line editing, no local echo, and ^C a byte for the
//! session rather than a signal for this process. `ENABLE_VIRTUAL_TERMINAL_INPUT`
//! makes the console hand over arrows and function keys as the VT sequences
//! a Unix terminal would have sent, which is what the session's ConPTY
//! expects -- without it, an arrow key arrives as nothing at all. And both
//! code pages go to UTF-8 (65001), because ConPTY speaks UTF-8 and the
//! console's default is an OEM code page that would mangle every non-ASCII
//! byte in both directions.
//!
//! Output needs `ENABLE_VIRTUAL_TERMINAL_PROCESSING` even for an observer:
//! a Unix terminal interprets escape sequences in any mode, a Windows console
//! only when asked. That is [`vt_output`], separate from [`RawMode`] for
//! exactly that reason.

use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::time::Duration;

use anyhow::Result;
use windows_sys::Win32::Foundation::HANDLE;
use windows_sys::Win32::System::Console::{
    GetConsoleCP, GetConsoleMode, GetConsoleOutputCP, GetConsoleScreenBufferInfo, GetStdHandle,
    SetConsoleCP, SetConsoleCtrlHandler, SetConsoleMode, SetConsoleOutputCP,
    CONSOLE_SCREEN_BUFFER_INFO, DISABLE_NEWLINE_AUTO_RETURN, ENABLE_ECHO_INPUT, ENABLE_LINE_INPUT,
    ENABLE_PROCESSED_INPUT, ENABLE_PROCESSED_OUTPUT, ENABLE_VIRTUAL_TERMINAL_INPUT,
    ENABLE_VIRTUAL_TERMINAL_PROCESSING, STD_INPUT_HANDLE, STD_OUTPUT_HANDLE,
};

const CP_UTF8: u32 = 65001;

fn std_handle(which: u32) -> HANDLE {
    // SAFETY: GetStdHandle takes a constant and returns a borrowed handle.
    unsafe { GetStdHandle(which) }
}

/// The console mode of `handle`, or `None` when it is not a console -- a
/// pipe or a file, the scripted case, where there is nothing to change.
fn console_mode(handle: HANDLE) -> Option<u32> {
    let mut mode = 0;
    // SAFETY: an out-pointer we own; a non-console handle just fails.
    (unsafe { GetConsoleMode(handle, &mut mode) } != 0).then_some(mode)
}

/// What a guard restores, readable from the console control handler.
///
/// Process-wide by necessity: the handler runs on a thread the system
/// creates and cannot be handed a context. `u32::MAX` means "nothing saved".
static SAVED_IN_MODE: AtomicU32 = AtomicU32::new(u32::MAX);
static SAVED_OUT_MODE: AtomicU32 = AtomicU32::new(u32::MAX);
static SAVED_IN_CP: AtomicU32 = AtomicU32::new(u32::MAX);
static SAVED_OUT_CP: AtomicU32 = AtomicU32::new(u32::MAX);
static HANDLER_INSTALLED: AtomicBool = AtomicBool::new(false);

/// Puts whatever was saved back. Called by the guards' `Drop` and by the
/// control handler, so a console closed or logged off under an attached
/// client is not left raw for the next program in it.
fn restore_saved() {
    let restore = |saved: &AtomicU32, apply: &dyn Fn(u32)| {
        let value = saved.swap(u32::MAX, Ordering::AcqRel);
        if value != u32::MAX {
            apply(value);
        }
    };
    // SAFETY (all four): restoring values read from this same console.
    restore(&SAVED_IN_MODE, &|m| unsafe {
        SetConsoleMode(std_handle(STD_INPUT_HANDLE), m);
    });
    restore(&SAVED_OUT_MODE, &|m| unsafe {
        SetConsoleMode(std_handle(STD_OUTPUT_HANDLE), m);
    });
    restore(&SAVED_IN_CP, &|cp| unsafe {
        SetConsoleCP(cp);
    });
    restore(&SAVED_OUT_CP, &|cp| unsafe {
        SetConsoleOutputCP(cp);
    });
}

/// Restores on Ctrl-Break, close, logoff and shutdown, then lets the
/// default handling end the process -- the Windows counterpart of the Unix
/// half's restore-and-reraise. Ctrl-C does not come here in raw mode: with
/// `ENABLE_PROCESSED_INPUT` off it is a byte for the session.
unsafe extern "system" fn on_console_event(_event: u32) -> i32 {
    restore_saved();
    0 // not handled: continue to the default, which ends the process
}

fn install_restore_handler() {
    if !HANDLER_INSTALLED.swap(true, Ordering::AcqRel) {
        // SAFETY: registering a handler that only restores console state.
        unsafe { SetConsoleCtrlHandler(Some(on_console_event), 1) };
    }
}

/// Raw console input, restored on drop -- including on a panic or an error
/// return, and (through the control handler) when the console closes.
#[derive(Debug)]
pub struct RawMode {
    _private: (),
}

impl RawMode {
    /// Enters raw mode on the console's input. `None` when stdin is not a
    /// console, which is the piped/scripted case.
    pub fn enter() -> Result<Option<Self>> {
        let input = std_handle(STD_INPUT_HANDLE);
        let Some(mode) = console_mode(input) else {
            return Ok(None);
        };
        // Published before the change, as on Unix: restoring a mode the
        // console still has is a no-op, and a close event landing in
        // between must find something to put back.
        SAVED_IN_MODE.store(mode, Ordering::Release);
        // SAFETY: GetConsoleCP takes nothing.
        SAVED_IN_CP.store(unsafe { GetConsoleCP() }, Ordering::Release);
        install_restore_handler();

        let raw = (mode & !(ENABLE_LINE_INPUT | ENABLE_ECHO_INPUT | ENABLE_PROCESSED_INPUT))
            | ENABLE_VIRTUAL_TERMINAL_INPUT;
        // SAFETY: setting a mode on the console input handle we just read.
        if unsafe { SetConsoleMode(input, raw) } == 0 {
            let err = std::io::Error::last_os_error();
            // Nothing changed yet; unpublish rather than restore, so a live
            // VtOutput guard is not undone with it.
            SAVED_IN_MODE.store(u32::MAX, Ordering::Release);
            SAVED_IN_CP.store(u32::MAX, Ordering::Release);
            return Err(anyhow::Error::new(err).context("entering raw mode"));
        }
        // SAFETY: SetConsoleCP takes a code page.
        unsafe { SetConsoleCP(CP_UTF8) };
        Ok(Some(Self { _private: () }))
    }
}

impl Drop for RawMode {
    fn drop(&mut self) {
        let mode = SAVED_IN_MODE.swap(u32::MAX, Ordering::AcqRel);
        if mode != u32::MAX {
            // SAFETY: restoring what `enter` read from this console.
            unsafe { SetConsoleMode(std_handle(STD_INPUT_HANDLE), mode) };
        }
        let cp = SAVED_IN_CP.swap(u32::MAX, Ordering::AcqRel);
        if cp != u32::MAX {
            // SAFETY: as above.
            unsafe { SetConsoleCP(cp) };
        }
    }
}

/// VT processing and UTF-8 on the console's output, restored on drop.
/// `None` when stdout is not a console.
#[derive(Debug)]
pub struct VtOutput {
    _private: (),
}

/// Asks the console to interpret the escape sequences the session writes,
/// for controller and observer alike (see the module doc).
pub fn vt_output() -> Result<Option<VtOutput>> {
    let output = std_handle(STD_OUTPUT_HANDLE);
    let Some(mode) = console_mode(output) else {
        return Ok(None);
    };
    SAVED_OUT_MODE.store(mode, Ordering::Release);
    // SAFETY: GetConsoleOutputCP takes nothing.
    SAVED_OUT_CP.store(unsafe { GetConsoleOutputCP() }, Ordering::Release);
    install_restore_handler();
    // DISABLE_NEWLINE_AUTO_RETURN: a bare `\n` from the session is a line
    // feed, not a carriage return and a line feed, as on a Unix terminal.
    let vt = mode
        | ENABLE_PROCESSED_OUTPUT
        | ENABLE_VIRTUAL_TERMINAL_PROCESSING
        | DISABLE_NEWLINE_AUTO_RETURN;
    // SAFETY: setting a mode on the console output handle we just read.
    if unsafe { SetConsoleMode(output, vt) } == 0 {
        let err = std::io::Error::last_os_error();
        SAVED_OUT_MODE.store(u32::MAX, Ordering::Release);
        SAVED_OUT_CP.store(u32::MAX, Ordering::Release);
        return Err(anyhow::Error::new(err).context("enabling VT output"));
    }
    // SAFETY: SetConsoleOutputCP takes a code page.
    unsafe { SetConsoleOutputCP(CP_UTF8) };
    Ok(Some(VtOutput { _private: () }))
}

impl Drop for VtOutput {
    fn drop(&mut self) {
        let mode = SAVED_OUT_MODE.swap(u32::MAX, Ordering::AcqRel);
        if mode != u32::MAX {
            // SAFETY: restoring what `vt_output` read from this console.
            unsafe { SetConsoleMode(std_handle(STD_OUTPUT_HANDLE), mode) };
        }
        let cp = SAVED_OUT_CP.swap(u32::MAX, Ordering::AcqRel);
        if cp != u32::MAX {
            // SAFETY: as above.
            unsafe { SetConsoleOutputCP(cp) };
        }
    }
}

/// The last size seen, packed `cols << 16 | rows`; 0 means none yet.
static LAST_SIZE: AtomicU32 = AtomicU32::new(0);

fn pack((cols, rows): (u16, u16)) -> u32 {
    u32::from(cols) << 16 | u32::from(rows)
}

/// Starts watching for size changes. There is no `SIGWINCH`: a console
/// reports a resize as an input record, and reading input records would
/// mean giving up `ReadFile`'s bytes for `ReadConsoleInput`'s events. The
/// size is compared on each of the attach loop's wakeups instead, which is
/// the same latency the Unix flag already has -- one poll interval.
pub fn watch_resizes() {
    LAST_SIZE.store(window_size().map_or(0, pack), Ordering::Release);
}

/// Whether the size changed since it was last looked at.
pub fn take_resize() -> bool {
    let Some(size) = window_size() else {
        return false;
    };
    let now = pack(size);
    LAST_SIZE.swap(now, Ordering::AcqRel) != now
}

/// The console window's size, from its visible rectangle rather than its
/// buffer: the buffer is thousands of rows of scrollback, the window is the
/// part a program is drawing into. `None` when stdout is not a console.
pub fn window_size() -> Option<(u16, u16)> {
    // SAFETY: a zeroed out-struct for GetConsoleScreenBufferInfo to fill.
    let mut info: CONSOLE_SCREEN_BUFFER_INFO = unsafe { std::mem::zeroed() };
    // SAFETY: as above; a non-console handle just fails.
    if unsafe { GetConsoleScreenBufferInfo(std_handle(STD_OUTPUT_HANDLE), &mut info) } == 0 {
        return None;
    }
    let cols = info.srWindow.Right - info.srWindow.Left + 1;
    let rows = info.srWindow.Bottom - info.srWindow.Top + 1;
    match (u16::try_from(cols), u16::try_from(rows)) {
        (Ok(cols), Ok(rows)) if cols > 0 && rows > 0 => Some((cols, rows)),
        _ => None,
    }
}

/// Keystrokes from stdin, with a timeout.
///
/// A reader thread, because a console has nothing like `poll`: its handle
/// is signalled by focus and mouse records that `ReadFile` then discards
/// while it blocks, so waiting on the handle and then reading can block
/// anyway. The thread blocks in `ReadFile` instead and hands bytes over a
/// channel; this side waits on the channel with the timeout the attach loop
/// needs. When attach returns the thread is still parked in its read, and
/// it goes with the process, which exits next -- nothing it could read
/// after that point belongs to a session.
///
/// Raw bytes through `File`, not `std::io::stdin()`: the latter decodes
/// console input as UTF-16 and refuses what is not valid UTF-8, where this
/// must hand the session exactly the bytes the console produced.
#[derive(Debug)]
pub struct Keyboard {
    rx: Option<Receiver<Vec<u8>>>,
    pending: Vec<u8>,
}

impl Default for Keyboard {
    fn default() -> Self {
        Self::new()
    }
}

impl Keyboard {
    pub fn new() -> Self {
        Self {
            rx: None,
            pending: Vec::new(),
        }
    }

    fn start() -> Receiver<Vec<u8>> {
        let (tx, rx) = mpsc::sync_channel::<Vec<u8>>(16);
        // A thread that cannot be spawned drops `tx` with its closure, so
        // the first read reports Closed rather than hanging.
        let _ = std::thread::Builder::new()
            .name("anchor-stdin".into())
            .spawn(move || {
                let mut stdin = borrowed_file(std_handle(STD_INPUT_HANDLE));
                let mut buf = [0u8; 1024];
                loop {
                    match std::io::Read::read(&mut *stdin, &mut buf) {
                        Ok(0) | Err(_) => return, // dropping `tx` reads as Closed
                        Ok(n) => {
                            if tx.send(buf[..n].to_vec()).is_err() {
                                return;
                            }
                        }
                    }
                }
            });
        rx
    }

    /// Waits up to `wait` for keystrokes and copies what is there.
    pub fn read(&mut self, buf: &mut [u8], wait: Duration) -> super::Input {
        if self.pending.is_empty() {
            let rx = self.rx.get_or_insert_with(Self::start);
            match rx.recv_timeout(wait) {
                Ok(bytes) => self.pending = bytes,
                Err(RecvTimeoutError::Timeout) => return super::Input::Idle,
                Err(RecvTimeoutError::Disconnected) => return super::Input::Closed,
            }
        }
        let n = self.pending.len().min(buf.len());
        buf[..n].copy_from_slice(&self.pending[..n]);
        self.pending.drain(..n);
        super::Input::Bytes(n)
    }
}

/// A `File` over a standard handle that never closes it: the handle is the
/// process's, not this value's.
fn borrowed_file(handle: HANDLE) -> std::mem::ManuallyDrop<std::fs::File> {
    use std::os::windows::io::FromRawHandle;
    // SAFETY: a valid standard handle; ManuallyDrop keeps `File` from
    // closing it.
    std::mem::ManuallyDrop::new(unsafe { std::fs::File::from_raw_handle(handle) })
}

/// Where session output goes: the standard output handle, as raw bytes.
///
/// Not `std::io::stdout()`, which on a console converts to UTF-16 and fails
/// on bytes that are not valid UTF-8 -- and a passthrough that can refuse a
/// byte is not a passthrough. `WriteFile` hands the bytes to the console in
/// its output code page, which `vt_output` set to UTF-8.
pub fn output() -> Box<dyn Write> {
    Box::new(RawStdout)
}

struct RawStdout;

impl Write for RawStdout {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        borrowed_file(std_handle(STD_OUTPUT_HANDLE)).write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
