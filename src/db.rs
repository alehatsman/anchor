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
    // The database holds every session's command, cwd and environment. Create
    // it owner-only ourselves: `Connection::open` would use the umask, and
    // SQLite gives `-wal` and `-shm` the mode of the main file, so this one
    // call covers all three. On Windows all three inherit the data
    // directory's owner-only DACL instead (`winsec`), which is checked by
    // `the_data_dir_is_owner_only_on_windows` in `tests/windows_transport.rs`.
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .mode(0o600)
            .open(path)
            .with_context(|| format!("creating {}", path.display()))?;
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
                // The rollback's own failure is logged, never returned:
                // `?` here would replace the error that explains what went
                // wrong with one about the cleanup afterwards, and the
                // cause of a failed migration is the thing worth keeping.
                if let Err(rollback) = conn.execute_batch("ROLLBACK;") {
                    tracing::error!(version, error = %rollback, "rolling back a failed migration");
                }
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A v1 database, built the way a shipped build would have left one:
    /// migration 1 applied and the version recorded, nothing more.
    fn write_v1(path: &Path) {
        let conn = Connection::open(path).unwrap();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);",
        )
        .unwrap();
        conn.execute_batch(MIGRATIONS[0]).unwrap();
        set_schema_version(&conn, 1).unwrap();
        conn.execute_batch(
            "INSERT INTO sessions (id, state, command, args_json, cwd, env_json,
                                   login_shell, cols, rows, spec_cols, spec_rows,
                                   output_bytes, created_at)
             VALUES ('01OLD', 'exited', '/bin/sh', '[]', '/tmp', '{}',
                     0, 80, 24, 80, 24, 1234, 1700000000);
             INSERT INTO labels (session_id, key, value) VALUES ('01OLD', 'wm.item', 'kept');",
        )
        .unwrap();
    }

    /// The upgrade a user actually experiences: a database written by an
    /// older build is carried forward, not rebuilt. The row and its labels
    /// must still be there afterwards -- a migration that dropped them
    /// would lose every session a user had, and `CREATE TABLE` in v1 would
    /// have failed loudly where `ALTER TABLE` in v2 succeeds silently.
    #[test]
    fn a_v1_database_upgrades_to_v2_without_losing_its_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("anchor.db");
        write_v1(&path);

        let conn = open(&path).unwrap();
        assert_eq!(schema_version(&conn).unwrap(), MIGRATIONS.len());

        let (state, bytes, capped): (String, i64, Option<i64>) = conn
            .query_row(
                "SELECT state, output_bytes, log_capped_at FROM sessions WHERE id = '01OLD'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(state, "exited");
        assert_eq!(bytes, 1234);
        assert_eq!(
            capped, None,
            "a session migrated from v1 has a complete log: NULL, not 0, \
             which would claim persistence stopped at the very first byte"
        );

        let label: String = conn
            .query_row(
                "SELECT value FROM labels WHERE session_id = '01OLD' AND key = 'wm.item'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(label, "kept");
    }

    /// The database, and the `-wal`/`-shm` files WAL mode adds beside it,
    /// carry session environments; none may be readable beyond the owner,
    /// whatever the umask or the parent directory's mode.
    #[cfg(unix)]
    #[test]
    fn the_database_files_are_owner_only() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("anchor.db");
        let conn = open(&path).unwrap();
        // Force the WAL and shared-memory files into existence.
        conn.execute_batch("CREATE TABLE touch (x); INSERT INTO touch VALUES (1);")
            .unwrap();

        for suffix in ["", "-wal", "-shm"] {
            let file = dir.path().join(format!("anchor.db{suffix}"));
            let mode = std::fs::metadata(&file).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, "{} has mode {mode:o}", file.display());
        }
    }

    /// Migrations are forward-only, so a database from a newer build is
    /// unreadable by this one. Refusing is the point: running v1..v2
    /// against a v3 schema would corrupt it far from the cause.
    #[test]
    fn a_database_from_a_newer_build_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("anchor.db");
        write_v1(&path);
        {
            let conn = Connection::open(&path).unwrap();
            set_schema_version(&conn, MIGRATIONS.len() + 1).unwrap();
        }

        let err = open(&path).unwrap_err().to_string();
        assert!(
            err.contains("newer than this build supports"),
            "expected a version complaint, got: {err}"
        );
    }
}
