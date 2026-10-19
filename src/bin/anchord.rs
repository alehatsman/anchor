//! `anchord` — the daemon.
//!
//! Everything it does lives in [`anchor::daemon`], so `anchor daemon` runs
//! the identical startup sequence. It takes no arguments of its own: the
//! socket is `anchor daemon --socket`'s business, and an unknown argument is
//! refused rather than ignored, so a typo cannot quietly start a daemon.

use anyhow::Result;
use clap::Parser;

#[derive(Parser)]
#[command(name = "anchord", about = "The anchor daemon", version)]
struct Args {}

#[cfg(unix)]
#[tokio::main]
async fn main() -> Result<()> {
    Args::parse();
    anchor::daemon::init_tracing();
    let paths = anchor::paths::Paths::resolve()?;
    anchor::daemon::run(&paths).await
}

/// Refuses, and says why. The PTY layer is ported; the transport a client
/// would reach it through is not (docs/09-windows.md §5, step 4). A daemon
/// that started anyway would report healthy and host nothing.
#[cfg(windows)]
fn main() -> Result<()> {
    Args::parse();
    anyhow::bail!(
        "anchord does not run on Windows yet: the named-pipe transport is \
         docs/09-windows.md §5 step 4, and it has not landed"
    )
}
