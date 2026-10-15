//! Data directory and socket path resolution.
//!
//! Layout, per docs/02-model.md and docs/04-protocol.md:
//!
//! ```text
//! $XDG_DATA_HOME/anchor/anchor.db
//! $XDG_DATA_HOME/anchor/sessions/<id>/output.vt
//! $XDG_RUNTIME_DIR/anchor.sock   (fallback: $XDG_DATA_HOME/anchor/anchor.sock)
//! ```

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

#[derive(Debug, Clone)]
pub struct Paths {
    data_dir: PathBuf,
    socket_path: PathBuf,
}

impl Paths {
    /// Resolve from the environment. Does not touch the filesystem.
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
        self.session_dir(id).join("output.vt")
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    /// Create the data and sessions directories if they do not exist.
    pub fn ensure_dirs(&self) -> Result<()> {
        std::fs::create_dir_all(self.sessions_dir())
            .with_context(|| format!("creating {}", self.sessions_dir().display()))?;
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
