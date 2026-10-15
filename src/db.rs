//! SQLite connection setup and forward-only migrations.
//!
//! One writer, WAL mode. The schema version lives in `meta`; migrations are
//! append-only and never edited once shipped.

use std::path::Path;

use anyhow::{bail, Context, Result};
use rusqlite::Connection;

/// Append only. Index in this slice + 1 is the schema version it produces.
const MIGRATIONS: &[&str] = &[
    // v1 — sessions, labels, meta
    r#"
    CREATE TABLE sessions (
        id           TEXT PRIMARY KEY,
        state        TEXT NOT NULL,
        pid          INTEGER,
        exit_code    INTEGER,
        lost_reason  TEXT,
        command      TEXT NOT NULL,
        args_json    TEXT NOT NULL,
        cwd          TEXT NOT NULL,
        env_json     TEXT NOT NULL,
        login_shell  INTEGER NOT NULL,
        cols         INTEGER NOT NULL,
        rows         INTEGER NOT NULL,
        spec_cols    INTEGER NOT NULL,
        spec_rows    INTEGER NOT NULL,
        output_bytes INTEGER NOT NULL,
        created_at   INTEGER NOT NULL,
        exited_at    INTEGER
    );

    CREATE TABLE labels (
        session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
        key        TEXT NOT NULL,
        value      TEXT NOT NULL,
        PRIMARY KEY (session_id, key)
    );

    CREATE INDEX labels_key_value ON labels(key, value);
    "#,
    // v2 — where persistence stopped, if it did. NULL means the whole byte
    // stream is still on disk. See src/log.rs and docs/02-model.md.
    r#"
    ALTER TABLE sessions ADD COLUMN log_capped_at INTEGER;
    "#,
];

/// Open the database, apply pragmas, and migrate forward.
pub fn open(path: &Path) -> Result<Connection> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    let conn = Connection::open(path).with_context(|| format!("opening {}", path.display()))?;

    conn.pragma_update(None, "journal_mode", "WAL")
        .context("enabling WAL")?;
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    conn.pragma_update(None, "foreign_keys", "ON")?;
    conn.busy_timeout(std::time::Duration::from_secs(5))?;

    migrate(&conn)?;
    Ok(conn)
}

/// Apply every migration past the stored schema version.
fn migrate(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);",
    )
    .context("creating meta table")?;

    let current = schema_version(conn)?;
    if current > MIGRATIONS.len() {
        bail!(
            "database schema version {} is newer than this build supports ({}); \
             anchor migrations are forward-only",
            current,
            MIGRATIONS.len()
        );
    }

    for (idx, sql) in MIGRATIONS.iter().enumerate().skip(current) {
        let version = idx + 1;
        tracing::info!(version, "applying migration");
        conn.execute_batch("BEGIN;")?;
        match conn
            .execute_batch(sql)
            .and_then(|_| set_schema_version(conn, version))
        {
            Ok(()) => conn.execute_batch("COMMIT;")?,
            Err(e) => {
                conn.execute_batch("ROLLBACK;")?;
                return Err(anyhow::Error::from(e))
                    .with_context(|| format!("applying migration v{version}"));
            }
        }
    }
    Ok(())
}

pub fn schema_version(conn: &Connection) -> Result<usize> {
    // `QueryReturnedNoRows` is the fresh-database case and means zero.
    // Every other error is a real failure — a corrupt file, a locked
    // database, an I/O error — and must not be reported as "version 0",
    // which would re-run every migration against a database that already
    // has them and fail far from the cause.
    let raw: Option<String> = match conn.query_row(
        "SELECT value FROM meta WHERE key = 'schema_version'",
        [],
        |r| r.get(0),
    ) {
        Ok(value) => Some(value),
        Err(rusqlite::Error::QueryReturnedNoRows) => None,
        Err(e) => return Err(e).context("reading the schema version"),
    };
    match raw {
        None => Ok(0),
        Some(s) => s
            .parse::<usize>()
            .with_context(|| format!("meta.schema_version is not a number: {s:?}")),
    }
}

fn set_schema_version(conn: &Connection, version: usize) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO meta (key, value) VALUES ('schema_version', ?1)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        [version.to_string()],
    )
    .map(|_| ())
}
