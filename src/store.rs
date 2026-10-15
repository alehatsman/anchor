//! Session persistence. Single connection, single writer.
//!
//! Label *filtering* is M2; this layer stores and returns labels, nothing more.

use std::collections::BTreeMap;
use std::path::PathBuf;

use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, Row};

use crate::model::{lost_reason, Session, SessionId, SpawnSpec, State};
use crate::paths::Paths;

#[derive(Debug)]
pub struct Store {
    conn: Connection,
}

impl Store {
    pub fn open(paths: &Paths) -> Result<Self> {
        let conn = crate::db::open(&paths.db_path())?;
        Ok(Self { conn })
    }

    pub fn conn(&self) -> &Connection {
        &self.conn
    }

    /// Insert a session and its labels in one transaction.
    pub fn insert(&mut self, session: &Session) -> Result<()> {
        let tx = self.conn.transaction()?;
        tx.execute(
            "INSERT INTO sessions (
                id, state, pid, exit_code, lost_reason,
                command, args_json, cwd, env_json, login_shell,
                cols, rows, spec_cols, spec_rows,
                output_bytes, log_capped_at, created_at, exited_at
             ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18)",
            rusqlite::params![
                session.id,
                session.state.as_str(),
                session.pid,
                session.exit_code,
                session.lost_reason,
                session.spec.command,
                serde_json::to_string(&session.spec.args)?,
                path_to_text(&session.spec.cwd),
                serde_json::to_string(&session.spec.env)?,
                session.spec.login_shell as i64,
                session.cols,
                session.rows,
                session.spec.cols,
                session.spec.rows,
                session.output_bytes as i64,
                session.log_capped_at.map(|v| v as i64),
                session.created_at,
                session.exited_at,
            ],
        )
        .with_context(|| format!("inserting session {}", session.id))?;

        {
            let mut stmt =
                tx.prepare("INSERT INTO labels (session_id, key, value) VALUES (?1, ?2, ?3)")?;
            for (key, value) in &session.labels {
                stmt.execute(rusqlite::params![session.id, key, value])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub fn get(&self, id: &str) -> Result<Option<Session>> {
        let row = self
            .conn
            .query_row(
                "SELECT id, state, pid, exit_code, lost_reason, command, args_json, cwd,
                        env_json, login_shell, cols, rows, spec_cols, spec_rows,
                        output_bytes, log_capped_at, created_at, exited_at
                 FROM sessions WHERE id = ?1",
                [id],
                session_from_row,
            )
            .optional()
            .with_context(|| format!("reading session {id}"))?;

        match row {
            None => Ok(None),
            Some(mut session) => {
                session.labels = self.labels_for(&session.id)?;
                Ok(Some(session))
            }
        }
    }

    /// Sessions whose labels match every pair in `filter`, newest first.
    ///
    /// Exact match on value, AND across keys — the filter narrows, it never
    /// widens. An empty filter is every session, so this is the general form
    /// of [`list`](Self::list).
    ///
    /// This is the query that replaces an orchestrator's side-table: a client
    /// asks "which session is for X?" with the same labels it set.
    pub fn list_filtered(&self, filter: &BTreeMap<String, String>) -> Result<Vec<Session>> {
        if filter.is_empty() {
            return self.list();
        }

        // One EXISTS per pair: a session matches only if every pair is
        // present on it. Indexed by labels(key, value).
        let clauses = (0..filter.len())
            .map(|i| {
                let k = i * 2 + 1;
                format!(
                    "EXISTS (SELECT 1 FROM labels l WHERE l.session_id = s.id \
                     AND l.key = ?{} AND l.value = ?{})",
                    k,
                    k + 1
                )
            })
            .collect::<Vec<_>>()
            .join(" AND ");

        let sql = format!(
            "SELECT s.id, s.state, s.pid, s.exit_code, s.lost_reason, s.command, s.args_json,
                    s.cwd, s.env_json, s.login_shell, s.cols, s.rows, s.spec_cols, s.spec_rows,
                    s.output_bytes, s.log_capped_at, s.created_at, s.exited_at
               FROM sessions s
              WHERE {clauses}
              ORDER BY s.id DESC"
        );

        let params: Vec<&dyn rusqlite::ToSql> = filter
            .iter()
            .flat_map(|(k, v)| [k as &dyn rusqlite::ToSql, v as &dyn rusqlite::ToSql])
            .collect();

        let mut stmt = self.conn.prepare(&sql)?;
        let mut sessions = stmt
            .query_map(params.as_slice(), session_from_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        for session in &mut sessions {
            session.labels = self.labels_for(&session.id)?;
        }
        Ok(sessions)
    }

    /// Replaces a session's labels wholesale, in one transaction.
    ///
    /// Validated by the caller — the store stores what it is given.
    pub fn replace_labels(&mut self, id: &str, labels: &BTreeMap<String, String>) -> Result<()> {
        let tx = self.conn.transaction()?;
        tx.execute("DELETE FROM labels WHERE session_id = ?1", [id])?;
        {
            let mut stmt =
                tx.prepare("INSERT INTO labels (session_id, key, value) VALUES (?1, ?2, ?3)")?;
            for (key, value) in labels {
                stmt.execute(rusqlite::params![id, key, value])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// All sessions, newest first. ULIDs sort by creation time.
    pub fn list(&self) -> Result<Vec<Session>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, state, pid, exit_code, lost_reason, command, args_json, cwd,
                    env_json, login_shell, cols, rows, spec_cols, spec_rows,
                    output_bytes, log_capped_at, created_at, exited_at
             FROM sessions ORDER BY id DESC",
        )?;
        let mut sessions = stmt
            .query_map([], session_from_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        for session in &mut sessions {
            session.labels = self.labels_for(&session.id)?;
        }
        Ok(sessions)
    }

    fn labels_for(&self, id: &str) -> Result<BTreeMap<String, String>> {
        let mut stmt = self
            .conn
            .prepare("SELECT key, value FROM labels WHERE session_id = ?1")?;
        let labels = stmt
            .query_map([id], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
            })?
            .collect::<rusqlite::Result<BTreeMap<_, _>>>()?;
        Ok(labels)
    }

    /// Records a session's terminal state: the one write a live session
    /// makes on its way out. Always `exited` — `lost` only ever comes from
    /// restart recovery, on a row with no live session behind it.
    pub fn mark_exited(&self, session: &Session) -> Result<()> {
        self.conn.execute(
            "UPDATE sessions
                SET state = ?2, exit_code = ?3, lost_reason = ?4, exited_at = ?5,
                    output_bytes = ?6, log_capped_at = ?7
              WHERE id = ?1",
            rusqlite::params![
                session.id,
                State::Exited.as_str(),
                session.exit_code,
                session.lost_reason,
                session.exited_at,
                session.output_bytes as i64,
                session.log_capped_at.map(|v| v as i64),
            ],
        )?;
        Ok(())
    }

    /// Deletes a session's row. Labels go with it via `ON DELETE CASCADE`.
    ///
    /// Returns whether a row was there to delete, so a caller can tell a
    /// purge of a real session from one of an id that never existed.
    pub fn delete(&self, id: &str) -> Result<bool> {
        let rows = self
            .conn
            .execute("DELETE FROM sessions WHERE id = ?1", [id])?;
        Ok(rows > 0)
    }

    /// Records the size actually applied to a session's PTY.
    pub fn set_size(&self, id: &str, cols: u16, rows: u16) -> Result<()> {
        self.conn.execute(
            "UPDATE sessions SET cols = ?2, rows = ?3 WHERE id = ?1",
            rusqlite::params![id, cols, rows],
        )?;
        Ok(())
    }

    pub fn set_output_bytes(&self, id: &str, bytes: u64, capped_at: Option<u64>) -> Result<()> {
        self.conn.execute(
            "UPDATE sessions SET output_bytes = ?2, log_capped_at = ?3 WHERE id = ?1",
            rusqlite::params![id, bytes as i64, capped_at.map(|v| v as i64)],
        )?;
        Ok(())
    }

    /// Startup step: a daemon restart means no PTY survived, so every row
    /// still claiming to be live is `lost`. Returns the number of rows moved.
    pub fn mark_live_sessions_lost(&self) -> Result<usize> {
        let now = crate::now_ms();
        let n = self.conn.execute(
            "UPDATE sessions
                SET state = ?1, lost_reason = ?2, exited_at = COALESCE(exited_at, ?3)
              WHERE state IN ('running', 'closing')",
            rusqlite::params![State::Lost.as_str(), lost_reason::DAEMON_RESTART, now],
        )?;
        Ok(n)
    }

    /// Startup step: reconcile each session's cached offset against its log
    /// file.
    ///
    /// The larger of the two wins, never the file alone. For an uncapped log
    /// the file is authoritative and this corrects a length a crash left
    /// stale. For a capped one the file stopped growing while the stream kept
    /// counting, so taking the file length would rewind `output_bytes` below
    /// offsets clients already hold. A stream ahead of its file is itself a
    /// cap, recorded here if no row said so — same rule as `OutputLog::open`.
    ///
    /// Returns the number of rows corrected.
    pub fn reconcile_output_bytes(&self, paths: &Paths) -> Result<usize> {
        let mut stmt = self
            .conn
            .prepare("SELECT id, output_bytes, log_capped_at FROM sessions")?;
        let rows = stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, i64>(1)? as u64,
                    r.get::<_, Option<i64>>(2)?.map(|v| v as u64),
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        let mut corrected = 0;
        for (id, cached, stored_cap) in rows {
            let file_len = match std::fs::metadata(paths.output_log(&id)) {
                Ok(meta) => meta.len(),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
                Err(e) => {
                    return Err(e)
                        .with_context(|| format!("stat {}", paths.output_log(&id).display()))
                }
            };

            let output_bytes = file_len.max(cached);
            let capped_at = match stored_cap {
                Some(at) => Some(at),
                None if output_bytes > file_len => Some(file_len),
                None => None,
            };

            if output_bytes != cached || capped_at != stored_cap {
                tracing::warn!(
                    session = %id,
                    cached,
                    file_len,
                    output_bytes,
                    ?capped_at,
                    "output offset reconciled against the log file"
                );
                self.set_output_bytes(&id, output_bytes, capped_at)?;
                corrected += 1;
            }
        }
        Ok(corrected)
    }
}

fn session_from_row(row: &Row<'_>) -> rusqlite::Result<Session> {
    let state_text: String = row.get("state")?;
    let state = State::parse(&state_text).ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(
            1,
            rusqlite::types::Type::Text,
            format!("unknown session state {state_text:?}").into(),
        )
    })?;

    let args: Vec<String> = json_column(row, "args_json")?;
    let env: BTreeMap<String, String> = json_column(row, "env_json")?;

    Ok(Session {
        id: row.get::<_, SessionId>("id")?,
        labels: BTreeMap::new(), // filled in by the caller
        spec: SpawnSpec {
            command: row.get("command")?,
            args,
            cwd: PathBuf::from(row.get::<_, String>("cwd")?),
            env,
            cols: row.get("spec_cols")?,
            rows: row.get("spec_rows")?,
            login_shell: row.get::<_, i64>("login_shell")? != 0,
        },
        state,
        pid: row.get("pid")?,
        exit_code: row.get("exit_code")?,
        lost_reason: row.get("lost_reason")?,
        cols: row.get("cols")?,
        rows: row.get("rows")?,
        output_bytes: row.get::<_, i64>("output_bytes")? as u64,
        log_capped_at: row
            .get::<_, Option<i64>>("log_capped_at")?
            .map(|v| v as u64),
        created_at: row.get("created_at")?,
        exited_at: row.get("exited_at")?,
    })
}

fn json_column<T: serde::de::DeserializeOwned>(row: &Row<'_>, name: &str) -> rusqlite::Result<T> {
    let raw: String = row.get(name)?;
    serde_json::from_str(&raw).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e))
    })
}

/// Paths are stored as text; a non-UTF-8 path is stored lossily. Rejecting
/// one outright belongs in create-time validation, which arrives with `create`.
fn path_to_text(path: &std::path::Path) -> String {
    path.to_string_lossy().into_owned()
}
