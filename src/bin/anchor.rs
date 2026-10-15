//! `anchor` — the CLI. A thin client over the control socket, for humans
//! and scripts.

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use serde_json::{json, Value};

use anchor::client::Client;
use anchor::paths::Paths;
use anchor::proto::frame;
use anchor::tty::{self, EscapeScanner, RawMode};

#[derive(Parser)]
#[command(name = "anchor", about = "Talk to anchord", version)]
struct Cli {
    /// Socket to connect to. Defaults to the usual resolution.
    #[arg(long, global = true)]
    socket: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// List sessions, optionally filtered by label.
    Ls {
        /// Filter by label, as k=v. Repeatable; every pair must match.
        #[arg(long = "label", value_name = "K=V")]
        labels: Vec<String>,
        /// Print the raw JSON response.
        #[arg(long)]
        json: bool,
    },
    /// Spawn a session.
    Create {
        #[arg(long)]
        cwd: Option<PathBuf>,
        /// Set a label, as k=v. Repeatable.
        #[arg(long = "label", value_name = "K=V")]
        labels: Vec<String>,
        /// Initial size, as COLSxROWS.
        #[arg(long, value_name = "COLSxROWS")]
        size: Option<String>,
        /// Run the command through the user's login shell.
        #[arg(long)]
        login_shell: bool,
        /// The command and its arguments, after `--`.
        #[arg(last = true, required = true)]
        argv: Vec<String>,
    },
    /// Print one session as JSON.
    Get { id: String },
    /// Attach to a session: raw-mode passthrough, `~.` to detach.
    Attach {
        id: String,
        /// Attach without taking the control lease.
        #[arg(long)]
        observe: bool,
        /// Start from this byte offset instead of replaying everything.
        #[arg(long)]
        from: Option<u64>,
    },
    /// Dump a session's output.
    Logs {
        id: String,
        #[arg(long, default_value_t = 0)]
        from: u64,
    },
    /// Terminate a session.
    Kill { id: String },
    /// Set or remove labels. `k=v` sets, `k-` removes.
    Label {
        id: String,
        #[arg(required = true, value_name = "K=V|K-")]
        changes: Vec<String>,
    },
    /// Run anchord in the foreground.
    Daemon,
    /// Register anchord with the user's service manager, so it outlives
    /// any client (docs/07-lifecycle.md).
    Service {
        #[command(subcommand)]
        action: ServiceAction,
    },
}

#[derive(Subcommand)]
enum ServiceAction {
    /// Install the registration. Idempotent.
    Install,
    /// Remove it. Not an error when nothing is installed.
    Uninstall,
    /// Show what is registered and whether it is running.
    Status,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Service management touches the user's service manager, not the
    // daemon -- it has to work when nothing is running, which is the
    // usual case the first time anyone runs it.
    if let Command::Service { action } = &cli.command {
        use anchor::service::Outcome;
        match action {
            ServiceAction::Install => match anchor::service::install()? {
                Outcome::Installed(path) => println!("installed {}", path.display()),
                other => println!("{other:?}"),
            },
            ServiceAction::Uninstall => match anchor::service::uninstall()? {
                Outcome::Uninstalled(path) => println!("removed {}", path.display()),
                Outcome::NotInstalled => println!("not installed; nothing to remove"),
                other => println!("{other:?}"),
            },
            ServiceAction::Status => println!("{}", anchor::service::status()?),
        }
        return Ok(());
    }

    // The daemon runs the same startup as `anchord`; it does not connect to
    // a socket, it binds one.
    if let Command::Daemon = cli.command {
        anchor::daemon::init_tracing();
        let paths = match cli.socket {
            Some(socket) => {
                let data = Paths::resolve()?.data_dir().to_path_buf();
                Paths::new(data, socket)
            }
            None => Paths::resolve()?,
        };
        return tokio::runtime::Runtime::new()?.block_on(anchor::daemon::run(&paths));
    }

    let socket = match cli.socket {
        Some(socket) => socket,
        None => Paths::resolve()?.socket_path().to_path_buf(),
    };
    let mut client = Client::connect(&socket)?;

    match cli.command {
        Command::Ls { labels, json } => ls(&mut client, &labels, json),
        Command::Create {
            cwd,
            labels,
            size,
            login_shell,
            argv,
        } => create(
            &mut client,
            cwd,
            &labels,
            size.as_deref(),
            login_shell,
            &argv,
        ),
        Command::Get { id } => {
            let response = client.call("get", json!({ "session": id }))?;
            println!("{}", serde_json::to_string_pretty(&response["session"])?);
            Ok(())
        }
        Command::Attach { id, observe, from } => attach(client, &socket, &id, !observe, from),
        Command::Logs { id, from } => logs(&mut client, &id, from),
        Command::Kill { id } => {
            let response = client.call("terminate", json!({ "session": id }))?;
            println!("{}", response["state"].as_str().unwrap_or("exited"));
            Ok(())
        }
        Command::Label { id, changes } => label(&mut client, &id, &changes),
        Command::Daemon => unreachable!("handled above"),
        Command::Service { .. } => unreachable!("handled above"),
    }
}

/// Parses repeated `k=v` arguments.
fn parse_labels(pairs: &[String]) -> Result<BTreeMap<String, String>> {
    pairs
        .iter()
        .map(|pair| {
            pair.split_once('=')
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .with_context(|| format!("expected k=v, got {pair:?}"))
        })
        .collect()
}

fn ls(client: &mut Client, labels: &[String], as_json: bool) -> Result<()> {
    let filter = parse_labels(labels)?;
    let response = client.call("list", json!({ "filter": filter }))?;
    if as_json {
        println!("{}", serde_json::to_string_pretty(&response["sessions"])?);
        return Ok(());
    }

    let sessions = response["sessions"].as_array().cloned().unwrap_or_default();
    if sessions.is_empty() {
        return Ok(());
    }
    println!(
        "{:<26}  {:<8}  {:>8}  {:<9}  COMMAND",
        "ID", "STATE", "BYTES", "EXIT"
    );
    for session in sessions {
        let exit = match (&session["exit_code"], &session["lost_reason"]) {
            (Value::Number(code), _) => code.to_string(),
            (_, Value::String(reason)) => reason.clone(),
            _ => "-".to_string(),
        };
        let argv = std::iter::once(session["command"].as_str().unwrap_or("").to_string())
            .chain(
                session["args"]
                    .as_array()
                    .map(|a| {
                        a.iter()
                            .map(|v| v.as_str().unwrap_or("").to_string())
                            .collect()
                    })
                    .unwrap_or_else(Vec::new),
            )
            .collect::<Vec<_>>()
            .join(" ");
        println!(
            "{:<26}  {:<8}  {:>8}  {:<9}  {}",
            session["id"].as_str().unwrap_or("?"),
            session["state"].as_str().unwrap_or("?"),
            session["output_bytes"].as_u64().unwrap_or(0),
            exit,
            argv
        );
    }
    Ok(())
}

fn create(
    client: &mut Client,
    cwd: Option<PathBuf>,
    labels: &[String],
    size: Option<&str>,
    login_shell: bool,
    argv: &[String],
) -> Result<()> {
    let (cols, rows) = match size {
        Some(size) => {
            let (cols, rows) = size
                .split_once(['x', 'X'])
                .with_context(|| format!("expected COLSxROWS, got {size:?}"))?;
            (cols.parse::<u16>()?, rows.parse::<u16>()?)
        }
        // The caller's own terminal is the best default for a session it is
        // about to attach to.
        None => tty::window_size().unwrap_or((80, 24)),
    };
    let cwd = match cwd {
        Some(cwd) => cwd,
        None => std::env::current_dir().context("resolving the current directory")?,
    };

    let response = client.call(
        "create",
        json!({
            "spec": {
                "command": argv[0],
                "args": argv[1..],
                "cwd": cwd,
                "cols": cols,
                "rows": rows,
                "login_shell": login_shell,
            },
            "labels": parse_labels(labels)?,
        }),
    )?;
    println!("{}", response["session"].as_str().unwrap_or("?"));
    Ok(())
}

fn logs(client: &mut Client, id: &str, mut from: u64) -> Result<()> {
    let mut stdout = std::io::stdout().lock();
    loop {
        let response = client.call(
            "log",
            json!({ "session": id, "from": from, "max": 1024 * 1024 }),
        )?;
        let bytes: Vec<u8> = response["bytes"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|b| b.as_u64())
                    .map(|b| b as u8)
                    .collect()
            })
            .unwrap_or_default();
        if bytes.is_empty() {
            return Ok(());
        }
        stdout.write_all(&bytes)?;
        from = response["to"].as_u64().unwrap_or(from + bytes.len() as u64);
    }
}

fn label(client: &mut Client, id: &str, changes: &[String]) -> Result<()> {
    let mut set = BTreeMap::new();
    let mut remove = Vec::new();
    for change in changes {
        match change.split_once('=') {
            Some((k, v)) => {
                set.insert(k.to_string(), v.to_string());
            }
            // `k-` removes, so a label can be dropped without a second flag.
            None if change.ends_with('-') => remove.push(change[..change.len() - 1].to_string()),
            None => bail!("expected k=v to set or k- to remove, got {change:?}"),
        }
    }
    let response = client.call(
        "label",
        json!({ "session": id, "set": set, "remove": remove }),
    )?;
    println!("{}", serde_json::to_string_pretty(&response["labels"])?);
    Ok(())
}

/// Raw-mode passthrough. No VT emulator: the user's terminal already is one.
fn attach(
    mut client: Client,
    socket: &std::path::Path,
    id: &str,
    want_control: bool,
    from: Option<u64>,
) -> Result<()> {
    // Match the session to this terminal before any output arrives, so the
    // replay is painted at the width it will keep.
    if want_control {
        if let Some((cols, rows)) = tty::window_size() {
            let mut control = Client::connect(socket)?;
            let _ = control.call(
                "resize",
                json!({ "session": id, "cols": cols, "rows": rows }),
            );
        }
    }

    let from = from.unwrap_or(0);
    let ready = client.call(
        "attach",
        json!({ "session": id, "from": from, "control": want_control }),
    )?;
    let has_control = ready["control"].as_bool().unwrap_or(false);
    if want_control && !has_control {
        eprintln!("anchor: another client holds the control lease; attached as an observer");
    }

    // Raw mode restores itself on every exit path, including an error.
    let _raw = RawMode::enter()?;

    // Split rather than re-wrap: the control reader may already hold the
    // first binary frames, and a fresh reader on the socket would drop them
    // and resume mid-frame.
    let (frames, mut writer) = client.into_attached()?;
    // Taken before the stream moves to its thread: after `~.` this is what
    // ends its blocking read, which otherwise waits for output a quiet
    // session may never produce.
    let interrupt = frames.interrupt_handle()?;

    // The reader half runs on its own thread: stdin and the socket both
    // block, and neither may wait for the other.
    //
    // `ended` is how the stdin loop below learns the session is over.
    // Nothing wakes a blocking `read` on stdin, so without it a controller
    // sits there after the session exited until the user happens to press
    // a key -- the session is gone, the output thread has finished, and
    // the terminal still looks attached.
    let ended = Arc::new(AtomicBool::new(false));
    let output = {
        let ended = Arc::clone(&ended);
        std::thread::Builder::new()
            .name("anchor-output".into())
            .spawn(move || {
                let result = pump_output(frames);
                ended.store(true, Ordering::Release);
                result
            })
            .context("spawning the output thread")?
    };

    if has_control {
        let mut scanner = EscapeScanner::new();
        let mut stdin = std::io::stdin().lock();
        let mut buf = [0u8; 1024];
        loop {
            // Wait for a keystroke *or* for the session to end, rather
            // than only for a keystroke. The poll timeout is what bounds
            // how long the check is deferred; it is not a poll interval
            // for input, which arrives as readability.
            if ended.load(Ordering::Acquire) {
                break;
            }
            match stdin_readable(STDIN_POLL) {
                Readable::Yes => {}
                Readable::NotYet => continue,
                Readable::Closed => break,
            }
            let read = match stdin.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => n,
            };
            let (forward, detach) = scanner.feed(&buf[..read]);
            if !forward.is_empty() && write_input(&mut writer, &forward).is_err() {
                break;
            }
            if detach {
                break;
            }
        }
    } else {
        // An observer has nothing to send; it ends when the stream does.
        let _ = output.join();
        return Ok(());
    }

    // Shut the socket down explicitly. Dropping the writer leaves the
    // reader's own cloned fd open, so a detach from a quiet session would
    // block here until the session happened to produce output — which for
    // an idle shell is never.
    interrupt.stop();
    drop(writer);
    let _ = output.join();
    Ok(())
}

/// How long [`stdin_readable`] waits before returning so the caller can
/// re-check whether the session ended. Short enough that a finished
/// session closes the terminal promptly, long enough not to spin.
const STDIN_POLL: libc::c_int = 100;

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
///
/// `read` on stdin cannot be interrupted by another thread, so a blocking
/// read would outlive the session it belongs to. Polling the descriptor
/// leaves the read itself blocking-but-ready, which keeps the input path
/// byte-for-byte what it was.
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

/// Writes one input frame to the PTY.
fn write_input(writer: &mut std::os::unix::net::UnixStream, bytes: &[u8]) -> Result<()> {
    use std::io::Write;
    writer
        .write_all(&frame::encode(frame::INPUT, 0, bytes))
        .context("sending input")?;
    writer.flush().context("flushing input")
}

/// Writes output frames to stdout, unparsed, until the stream ends.
fn pump_output(mut frames: anchor::client::FrameStream) -> Result<()> {
    let mut stdout = std::io::stdout().lock();
    loop {
        let (tag, _offset, payload) = match frames.read_frame() {
            Ok(frame) => frame,
            // The daemon went away or we detached; either way, done.
            Err(_) => return Ok(()),
        };
        match tag {
            frame::OUTPUT => {
                stdout.write_all(&payload)?;
                stdout.flush()?;
            }
            frame::EXIT => {
                let code = if payload.len() == 4 {
                    i32::from_le_bytes(payload.try_into().expect("4 bytes")).to_string()
                } else {
                    "unknown".to_string()
                };
                // On stderr, so a piped transcript stays the session's own
                // bytes and nothing else.
                eprintln!("\r\nanchor: session exited ({code})\r");
                return Ok(());
            }
            frame::LEASE if payload.first() == Some(&0) => {
                eprintln!("\r\nanchor: control lease revoked\r");
            }
            _ => {}
        }
    }
}
