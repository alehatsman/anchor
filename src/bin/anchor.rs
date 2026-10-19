//! `anchor` — the CLI. A thin client over the control socket, for humans
//! and scripts.

#[cfg(unix)]
#[path = "anchor/cli.rs"]
mod cli;

#[cfg(unix)]
fn main() -> anyhow::Result<()> {
    cli::main()
}

/// Refuses, and says why: there is no Windows transport to be a client of
/// yet, and no console input path for `attach` (docs/09-windows.md §5,
/// step 4).
#[cfg(windows)]
fn main() -> anyhow::Result<()> {
    anyhow::bail!(
        "anchor does not run on Windows yet: the named-pipe transport and the console \
         input path are docs/09-windows.md §5 step 4, and they have not landed"
    )
}
