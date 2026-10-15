//! `anchord` — the daemon.
//!
//! Everything it does lives in [`anchor::daemon`], so `anchor daemon` runs
//! the identical sequence.

use anyhow::Result;

use anchor::paths::Paths;

#[tokio::main]
async fn main() -> Result<()> {
    anchor::daemon::init_tracing();
    let paths = Paths::resolve()?;
    anchor::daemon::run(&paths).await
}
