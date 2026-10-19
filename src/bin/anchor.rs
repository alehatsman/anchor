//! `anchor` — the CLI. A thin client over the control socket, for humans
//! and scripts.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use serde_json::{json, Value};

use anchor::client::{Client, Stream};
use anchor::paths::Paths;
use anchor::proto::frame;
use anchor::tty::{self, EscapeScanner, RawMode};

#[derive(Parser)]
#[command(name = "anchor", about = "Talk to anchord", version)]
struct Cli {
    /// Control socket: the one to connect to, or for `daemon` the one to
    /// bind. `service` ignores it. Defaults to the usual resolution. On
    /// Windows, a named pipe: `\\.\pipe\<name>`.
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
        /// Print the sessions as a JSON array.
        #[arg(long)]
        json: bool,
    },
    /// Spawn a session.
    Create {
        /// Working directory for the session. Relative paths resolve
        /// against the current directory. Defaults to the current directory.
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
    Get {
        /// Session id, as shown by `anchor ls`.
        id: String,
    },
    /// Attach to a session: raw-mode passthrough, `~.` to detach.
    Attach {
        /// Session id, as shown by `anchor ls`.
        id: String,
        /// Attach without taking the control lease.
        #[arg(long)]
        observe: bool,
        /// Start from this byte offset of the session's output instead of
        /// replaying everything.
        #[arg(long)]
        from: Option<u64>,
    },
    /// Dump a session's output.
    Logs {
        /// Session id, as shown by `anchor ls`.
        id: String,
        /// Start from this byte offset of the session's output.
        #[arg(long, default_value_t = 0)]
        from: u64,
    },
    /// Terminate a session.
    Kill {
        /// Session id, as shown by `anchor ls`.
        id: String,
    },
    /// Forget a session: its row, its labels and its output. Refuses a
    /// session that is still running — `kill` it first.
    Purge {
        /// Session id, as shown by `anchor ls`.
        id: String,
    },
    /// Set or remove labels. `k=v` sets, `k-` removes.
    Label {
        /// Session id, as shown by `anchor ls`.
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

    // Rust ignores SIGPIPE at startup, which turns `anchor ls | head` into a
    // panic (exit 101) and `anchor logs | head -c 5` into "Broken pipe" and
    // exit 1. Restoring the default makes a closed pipe end the process
    // quietly, like `cat`. `attach` must keep it ignored: a vanished stdout
    // has to surface as an ordinary error so the raw-mode guard unwinds
    // (src/tty.rs). `daemon` keeps it too: a client hanging up must be an
    // error on that connection, never the death of the daemon.
    //
    // Windows has no SIGPIPE: a write to a closed pipe is already an error.
    #[cfg(unix)]
    if !matches!(cli.command, Command::Attach { .. } | Command::Daemon) {
        // SAFETY: restoring the default disposition, before any thread runs.
        unsafe { libc::signal(libc::SIGPIPE, libc::SIG_DFL) };
    }

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
            let response = session_call(&mut client, "get", &id, json!({ "session": id }))?;
            println!("{}", serde_json::to_string_pretty(&response["session"])?);
            Ok(())
        }
        Command::Attach { id, observe, from } => attach(client, &socket, &id, !observe, from),
        Command::Logs { id, from } => logs(&mut client, &id, from),
        Command::Kill { id } => {
            let response = session_call(&mut client, "terminate", &id, json!({ "session": id }))?;
            println!("{}", response["state"].as_str().unwrap_or("exited"));
            Ok(())
        }
        // The daemon already answers `already_exited` for a running session
        // and `not_found` for an unknown id, and `call` turns either into an
        // error carrying the code. Adding logic here would only give the two
        // a second opinion.
        Command::Purge { id } => {
            session_call(&mut client, "purge", &id, json!({ "session": id }))?;
            println!("purged");
            Ok(())
        }
        Command::Label { id, changes } => label(&mut client, &id, &changes),
        Command::Daemon => unreachable!("handled above"),
        Command::Service { .. } => unreachable!("handled above"),
    }
}

/// `client.call` for an op on one session.
///
/// The daemon's `not_found` says "no such session" without saying which, so
/// with several ids in flight the user is left guessing, and nothing points
/// at `anchor ls`. The caller has the id in hand; this adds it. The error
/// code stays the leading token -- a script greps for it -- and anything but
/// `not_found` passes through untouched.
fn session_call(client: &mut Client, op: &str, id: &str, body: Value) -> Result<Value> {
    client.call(op, body).map_err(|err| {
        let text = err.to_string();
        if text.starts_with("not_found:") && !text.contains(id) {
            anyhow::anyhow!("{text}: {id} (`anchor ls` lists the sessions)")
        } else {
            err
        }
    })
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
            (
                cols.parse::<u16>()
                    .with_context(|| format!("--size: columns {cols:?} is not a number"))?,
                rows.parse::<u16>()
                    .with_context(|| format!("--size: rows {rows:?} is not a number"))?,
            )
        }
        // The caller's own terminal is the best default for a session it is
        // about to attach to.
        None => tty::window_size().unwrap_or((80, 24)),
    };
    // Resolved here, against *this* process's directory, before it goes
    // on the wire. The daemon's working directory is wherever it was
    // started -- `/` under launchd -- so a relative `--cwd` sent as typed
    // lands somewhere the user never named, and the session starts in a
    // directory nobody chose. The spec carries an absolute path or none.
    let cwd = match cwd {
        Some(cwd) => std::path::absolute(&cwd)
            .with_context(|| format!("resolving --cwd {}", cwd.display()))?,
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
    let mut stdout = tty::output();
    loop {
        let response = session_call(
            client,
            "log",
            id,
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
    let response = session_call(
        client,
        "label",
        id,
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
    //
    // Watching starts before the size is read, so a resize racing the
    // attach is remembered rather than lost: the stdin loop below sends
    // whatever the handler recorded.
    if want_control {
        tty::watch_resizes();
        if let Some((cols, rows)) = tty::window_size() {
            let mut control = Client::connect(socket)?;
            let _ = control.call(
                "resize",
                json!({ "session": id, "cols": cols, "rows": rows }),
            );
        }
    }

    let from = from.unwrap_or(0);
    let ready = session_call(
        &mut client,
        "attach",
        id,
        json!({ "session": id, "from": from, "control": want_control }),
    )?;
    let has_control = ready["control"].as_bool().unwrap_or(false);
    if want_control && !has_control {
        eprintln!("anchor: another client holds the control lease; attached as an observer");
    }

    // Raw mode restores itself on every exit path, including an error.
    //
    // Only a controller has anything to type, so only a controller goes raw.
    // An observer never reads stdin: in raw mode ISIG is off, ^C is an unread
    // byte, and `~.` is scanned only on the controller path -- on a quiet
    // session the only way out would be closing the window. Left cooked, ^C
    // kills it. The terminal's output post-processing then turns the
    // session's `\r\n` into `\r\r\n`, and a carriage return at column 0 is
    // a no-op, so what the observer sees is unchanged.
    //
    // Output is a separate question, and on Windows not a free one: a
    // console interprets escape sequences only when asked, so controller
    // and observer alike ask. A no-op on Unix.
    let _vt = tty::vt_output()?;
    let _raw = if has_control { RawMode::enter()? } else { None };

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
    // Set by this side before it shuts the socket down. A read that fails
    // because *we* stopped it is the design; one that fails on its own is the
    // daemon going away or the stream desynchronising, and must not be
    // reported as a clean detach.
    let stopped = Arc::new(AtomicBool::new(false));
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
        let mut keyboard = tty::Keyboard::new();
        let mut buf = [0u8; 1024];
        loop {
            // Wait for a keystroke *or* for the session to end, rather
            // than only for a keystroke. The poll timeout is what bounds
            // how long the check is deferred; it is not a poll interval
            // for input, which arrives as readability.
            if ended.load(Ordering::Acquire) {
                break;
            }
            // The window moved. One frame per observed change, carrying
            // the size the terminal has now — the daemon coalesces the
            // rest, and a drag that produced a hundred signals while this
            // was blocked in `poll` is one frame here.
            if tty::take_resize() {
                if let Some((cols, rows)) = tty::window_size() {
                    if write_resize(&mut writer, cols, rows).is_err() {
                        break;
                    }
                }
            }
            let read = match keyboard.read(&mut buf, STDIN_POLL) {
                tty::Input::Bytes(n) => n,
                tty::Input::Idle => continue,
                tty::Input::Closed => break,
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
        // An observer has nothing to send; it ends when the stream does, and
        // nothing stops it from here, so whatever ended it is the result.
        return output.join().expect("the output thread panicked");
    }

    // Shut the socket down explicitly. Dropping the writer leaves the
    // reader's own cloned fd open, so a detach from a quiet session would
    // block here until the session happened to produce output — which for
    // an idle shell is never.
    //
    // If the output thread already ended by itself, its result is the
    // answer: `ended` is set only after its read has returned, so a stop
    // issued now cannot have caused it.
    if !ended.load(Ordering::Acquire) {
        stopped.store(true, Ordering::Release);
    }
    interrupt.stop();
    drop(writer);
    let result = output.join().expect("the output thread panicked");
    if stopped.load(Ordering::Acquire) {
        return Ok(());
    }
    result
}

/// How long one wait for keystrokes lasts before the loop re-checks whether
/// the session ended. Short enough that a finished session closes the
/// terminal promptly, long enough not to spin.
const STDIN_POLL: std::time::Duration = std::time::Duration::from_millis(100);

/// Writes one input frame to the PTY.
fn write_input(writer: &mut Stream, bytes: &[u8]) -> Result<()> {
    use std::io::Write;
    writer
        .write_all(&frame::encode(frame::INPUT, 0, bytes))
        .context("sending input")?;
    writer.flush().context("flushing input")
}

/// Writes one resize frame.
///
/// A frame on the attached connection rather than the `resize` control op:
/// the lease is per-connection, and this is the connection that holds it
/// (`docs/04-protocol.md`). The op would arrive on a different one and be
/// refused with `lease_held` — by us, against ourselves.
fn write_resize(writer: &mut Stream, cols: u16, rows: u16) -> Result<()> {
    use std::io::Write;
    writer
        .write_all(&frame::encode(
            frame::RESIZE,
            0,
            &frame::encode_size(cols, rows),
        ))
        .context("sending a resize")?;
    writer.flush().context("flushing a resize")
}

/// Writes output frames to stdout, unparsed, until the session exits.
///
/// A stream that ends any other way is an error, because from in here a
/// detach, a daemon that died and a desynchronised stream all look the same:
/// a failed read. The caller knows which of them it caused.
fn pump_output(mut frames: anchor::client::FrameStream) -> Result<()> {
    let mut stdout = tty::output();
    loop {
        let (tag, _offset, payload) = frames
            .read_frame()
            .context("the connection to anchord ended before the session did")?;
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
