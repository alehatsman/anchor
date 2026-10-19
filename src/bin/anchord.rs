//! `anchord` — the daemon.
//!
//! Everything it does lives in [`anchor::daemon`], so `anchor daemon` runs
//! the identical startup sequence. It takes no arguments of its own: the
//! socket is `anchor daemon --socket`'s business, and an unknown argument is
//! refused rather than ignored, so a typo cannot quietly start a daemon.

use anyhow::Result;
use clap::Parser;

use anchor::paths::Paths;

#[derive(Parser)]
#[command(name = "anchord", about = "The anchor daemon", version)]
struct Args {}

#[tokio::main]
async fn main() -> Result<()> {
    Args::parse();
    anchor::daemon::init_tracing();
    let paths = Paths::resolve()?;
    anchor::daemon::run(&paths).await
}
