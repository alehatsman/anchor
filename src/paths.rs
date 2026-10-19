//! Data directory and socket path resolution.
//!
//! Layout, per docs/02-model.md and docs/04-protocol.md:
//!
//! ```text
//! $XDG_DATA_HOME/anchor/anchor.db
//! $XDG_DATA_HOME/anchor/sessions/<id>/output.vt
//! $XDG_RUNTIME_DIR/anchor.sock   (fallback: $XDG_DATA_HOME/anchor/anchor.sock)
//! ```
//!
//! On Windows (docs/09-windows.md §3.2):
//!
//! ```text
//! %LOCALAPPDATA%\anchor\anchor.db
//! %LOCALAPPDATA%\anchor\sessions\<id>\output.vt
//! \\.\pipe\anchor-<SID>         (the user SID of this process's token)
//! ```
//!
//! The pipe is not a file, but it is held in the same `socket_path` field:
//! it is the endpoint, and every caller only ever hands it to `bind` or
//! `connect`, which know what it is.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

#[derive(Debug, Clone)]
pub struct Paths {
    data_dir: PathBuf,
    socket_path: PathBuf,
}

impl Paths {
    /// Resolve from the environment. Does not touch the filesystem.
    #[cfg(unix)]
    pub fn resolve() -> Result<Self> {
        let data_dir = match non_empty_env("XDG_DATA_HOME") {
            Some(xdg) => PathBuf::from(xdg).join("anchor"),
            None => {
                let home = non_empty_env("HOME")
                    .context("neither XDG_DATA_HOME nor HOME is set; cannot resolve data dir")?;
                PathBuf::from(home).join(".local/share/anchor")
            }
        };
        let socket_path = match non_empty_env("XDG_RUNTIME_DIR") {
            Some(run) => PathBuf::from(run).join("anchor.sock"),
            None => data_dir.join("anchor.sock"),
        };
        Ok(Self::new(data_dir, socket_path))
    }

    /// Resolve from the environment and this process's token. Does not touch
    /// the filesystem.
    ///
    /// `LOCALAPPDATA`, not `APPDATA`: transcripts are machine-local state
    /// and have no business roaming to a domain profile server. The XDG
    /// variables are deliberately not read here -- a Windows user who has
    /// them set for some other tool did not choose them for this one.
    #[cfg(windows)]
    pub fn resolve() -> Result<Self> {
        let data_dir = non_empty_env("LOCALAPPDATA")
            .map(|local| PathBuf::from(local).join("anchor"))
            .context("LOCALAPPDATA is not set; cannot resolve the data dir")?;
        Ok(Self::new(data_dir, default_pipe_name()?))
    }

    /// Explicit paths. Used by tests, which must not depend on process env.
    pub fn new(data_dir: impl Into<PathBuf>, socket_path: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
            socket_path: socket_path.into(),
        }
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub fn db_path(&self) -> PathBuf {
        self.data_dir.join("anchor.db")
    }

    pub fn sessions_dir(&self) -> PathBuf {
        self.data_dir.join("sessions")
    }

    /// Directory holding one session's output log.
    pub fn session_dir(&self, id: &str) -> PathBuf {
        self.sessions_dir().join(id)
    }

    /// The append-only output log for a session.
    pub fn output_log(&self, id: &str) -> PathBuf {
        self.session_dir(id).join(crate::log::LOG_FILE_NAME)
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    /// Create the data and sessions directories if they do not exist.
    ///
    /// Owner-only. The data dir holds every session's output log, which is
    /// a verbatim transcript of a terminal — anything typed or printed in
    /// it, credentials included. The socket is already 0600 and the session
    /// directories under here are 0700; leaving the root at whatever the
    /// umask gives would make all of that readable to anyone on the machine.
    ///
    /// On Windows the same claim is held by a DACL rather than a mode: the
    /// directory grants its user and nobody else, and everything beneath
    /// inherits that (`winsec::restrict_to_current_user`).
    pub fn ensure_dirs(&self) -> Result<()> {
        create_owner_only(&self.data_dir)?;
        create_owner_only(&self.sessions_dir())?;
        // A named pipe has no directory to create.
        #[cfg(unix)]
        if let Some(parent) = self.socket_path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating {}", parent.display()))?;
        }
        Ok(())
    }
}

fn non_empty_env(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.is_empty())
}

/// This user's pipe name, `\\.\pipe\anchor-<SID>` (docs/04-protocol.md).
///
/// Per user, because the pipe namespace is machine-global: one fixed name
/// would let two users' daemons collide, and whichever started first would
/// serve the other's clients.
#[cfg(windows)]
pub fn default_pipe_name() -> Result<PathBuf> {
    let sid = crate::winsec::UserSid::current()?.to_sid_string()?;
    Ok(PathBuf::from(format!(r"\\.\pipe\anchor-{sid}")))
}

/// Creates a directory and narrows it to `0700`.
///
/// The permissions are applied whether or not this call created it: a data
/// dir from an older build, or one a careless `mkdir` left at `0755`, is
/// exactly the case worth correcting.
#[cfg(unix)]
fn create_owner_only(dir: &Path) -> Result<()> {
    std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
        .with_context(|| format!("setting owner-only permissions on {}", dir.display()))
}

/// Creates a directory and narrows its DACL to this user alone -- the
/// Windows spelling of `0700`, applied whether or not it is new for the
/// same reason.
#[cfg(windows)]
fn create_owner_only(dir: &Path) -> Result<()> {
    std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    crate::winsec::restrict_to_current_user(dir)
}

#[cfg(all(test, windows))]
mod tests {
    use super::*;

    /// The pipe name is this user's, in the form clients are told to
    /// compute (docs/04-protocol.md#windows-a-named-pipe).
    #[test]
    fn the_pipe_name_is_per_user() {
        let name = default_pipe_name().unwrap();
        let name = name.to_string_lossy();
        let sid = crate::winsec::UserSid::current()
            .unwrap()
            .to_sid_string()
            .unwrap();
        assert_eq!(name, format!(r"\\.\pipe\anchor-{sid}"));
        assert!(sid.starts_with("S-1-5-"), "not a user SID: {sid}");
    }
}
