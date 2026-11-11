//! Keeps a domain model in a SQLite file, so an app's data outlives it.
//!
//! ```text
//! let bank = Memory::new().persist_to(Sqlite::open("bank.db")?).with::<Bank>();
//! ```
//!
//! One process opens a file at a time. Values are stored as JSON, so the file is readable with
//! any SQLite tool.

use domain_memory::store::{Commit, EntryRow, EventRow, KeyRow, ReceiptRow, ScheduleRow, Store, StoreError, ViewRows};
use rusqlite::{Connection, OptionalExtension, params};
use std::cell::RefCell;
use std::path::Path;

const SCHEMA: &str = "
    PRAGMA journal_mode = WAL;
    CREATE TABLE IF NOT EXISTS events (
        position INTEGER PRIMARY KEY AUTOINCREMENT,
        aggregate TEXT NOT NULL,
        stream TEXT NOT NULL,
        seq INTEGER NOT NULL,
        id TEXT NOT NULL,
        at INTEGER NOT NULL,
        command_id TEXT,
        event TEXT NOT NULL,
        UNIQUE (stream, seq)
    );
    CREATE INDEX IF NOT EXISTS events_by_aggregate ON events (aggregate, position);
    CREATE TABLE IF NOT EXISTS receipts (
        stream TEXT NOT NULL,
        command_id TEXT NOT NULL,
        command TEXT NOT NULL,
        receipt TEXT NOT NULL,
        PRIMARY KEY (stream, command_id)
    );
    CREATE TABLE IF NOT EXISTS sessions (n INTEGER PRIMARY KEY AUTOINCREMENT);
    CREATE TABLE IF NOT EXISTS views (name TEXT PRIMARY KEY, version INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS view_keys (
        view TEXT NOT NULL,
        key TEXT NOT NULL,
        state TEXT NOT NULL,
        PRIMARY KEY (view, key)
    );
    CREATE TABLE IF NOT EXISTS view_entries (
        view TEXT NOT NULL,
        key TEXT NOT NULL,
        cursor INTEGER NOT NULL,
        event TEXT NOT NULL,
        state TEXT NOT NULL,
        stream TEXT NOT NULL,
        seq INTEGER NOT NULL,
        at INTEGER NOT NULL,
        PRIMARY KEY (view, key, cursor)
    );
    CREATE TABLE IF NOT EXISTS schedules (
        stream TEXT NOT NULL,
        command_id TEXT NOT NULL,
        command TEXT NOT NULL,
        id TEXT NOT NULL,
        payload TEXT NOT NULL,
        at INTEGER NOT NULL,
        state TEXT NOT NULL,
        PRIMARY KEY (stream, command_id)
    );
    CREATE TABLE IF NOT EXISTS view_progress (
        view TEXT NOT NULL,
        stream TEXT NOT NULL,
        seq INTEGER NOT NULL,
        PRIMARY KEY (view, stream)
    );
";

/// A [`Store`] in one SQLite file. Pass it to `Memory::persist_to`.
pub struct Sqlite {
    connection: RefCell<Connection>,
}

impl Sqlite {
    /// Opens the file, creating it and its tables if needed.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let connection = Connection::open(path).map_err(error)?;
        connection.execute_batch(SCHEMA).map_err(error)?;
        Ok(Self { connection: RefCell::new(connection) })
    }

    fn run<T>(&self, f: impl FnOnce(&mut Connection) -> rusqlite::Result<T>) -> Result<T, StoreError> {
        f(&mut self.connection.borrow_mut()).map_err(error)
    }
}

fn error(e: rusqlite::Error) -> StoreError {
    StoreError(e.to_string())
}

fn event_row(row: &rusqlite::Row) -> rusqlite::Result<EventRow> {
    Ok(EventRow {
        stream: row.get("stream")?,
        seq: row.get("seq")?,
        id: row.get("id")?,
        at: row.get("at")?,
        command_id: row.get("command_id")?,
        event: row.get("event")?,
    })
}

// Each method writes in a savepoint: its own transaction outside a batch, and a part of the
// batch's transaction inside one, rolled back alone when the method fails.
impl Store for Sqlite {
    fn begin_batch(&self) -> Result<(), StoreError> {
        self.run(|c| c.execute_batch("BEGIN"))
    }

    fn commit_batch(&self) -> Result<(), StoreError> {
        self.run(|c| {
            c.execute_batch("COMMIT").inspect_err(|_| {
                let _ = c.execute_batch("ROLLBACK");
            })
        })
    }

    fn commit(&self, commit: &Commit) -> Result<(), StoreError> {
        self.run(|c| {
            let tx = c.savepoint()?;
            {
                let mut insert = tx.prepare_cached(
                    "INSERT INTO events (aggregate, stream, seq, id, at, command_id, event)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                )?;
                for e in &commit.events {
                    insert.execute(params![commit.aggregate, e.stream, e.seq, e.id, e.at, e.command_id, e.event])?;
                }
                if let Some(r) = &commit.receipt {
                    tx.execute(
                        "INSERT INTO receipts (stream, command_id, command, receipt) VALUES (?1, ?2, ?3, ?4)",
                        params![commit.stream, r.command_id, r.command, r.receipt],
                    )?;
                }
            }
            tx.commit()
        })
    }

    fn stream(&self, stream: &str) -> Result<Vec<EventRow>, StoreError> {
        self.run(|c| {
            let mut select = c.prepare_cached(
                "SELECT stream, seq, id, at, command_id, event FROM events WHERE stream = ?1 ORDER BY seq",
            )?;
            select.query_map([stream], event_row)?.collect()
        })
    }

    fn receipt(&self, stream: &str, command_id: &str) -> Result<Option<ReceiptRow>, StoreError> {
        self.run(|c| {
            c.query_row(
                "SELECT command_id, command, receipt FROM receipts WHERE stream = ?1 AND command_id = ?2",
                [stream, command_id],
                |row| Ok(ReceiptRow { command_id: row.get(0)?, command: row.get(1)?, receipt: row.get(2)? }),
            )
            .optional()
        })
    }

    fn history(&self, aggregate: &str, keep: &dyn Fn(&str, u64) -> bool) -> Result<Vec<EventRow>, StoreError> {
        self.run(|c| {
            let mut select = c.prepare_cached(
                "SELECT stream, seq, id, at, command_id, event FROM events WHERE aggregate = ?1 ORDER BY position",
            )?;
            let mut rows = select.query([aggregate])?;
            let mut kept = Vec::new();
            while let Some(row) = rows.next()? {
                // Check before reading the event, so skipped history costs little.
                let stream = row.get_ref("stream")?.as_str()?;
                if keep(stream, row.get("seq")?) {
                    kept.push(event_row(row)?);
                }
            }
            Ok(kept)
        })
    }

    fn versions(&self) -> Result<Vec<(String, u64)>, StoreError> {
        self.run(|c| {
            let mut select = c.prepare_cached("SELECT stream, MAX(seq) FROM events GROUP BY stream")?;
            select.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?.collect()
        })
    }

    fn new_session(&self) -> Result<u64, StoreError> {
        self.run(|c| {
            c.execute("INSERT INTO sessions DEFAULT VALUES", [])?;
            Ok(c.last_insert_rowid() as u64)
        })
    }

    fn view_version(&self, view: &str) -> Result<Option<u64>, StoreError> {
        self.run(|c| c.query_row("SELECT version FROM views WHERE name = ?1", [view], |row| row.get(0)).optional())
    }

    fn load_view(&self, view: &str) -> Result<ViewRows, StoreError> {
        self.run(|c| {
            let keys = c
                .prepare_cached("SELECT key, state FROM view_keys WHERE view = ?1")?
                .query_map([view], |row| Ok(KeyRow { key: row.get(0)?, state: row.get(1)? }))?
                .collect::<rusqlite::Result<_>>()?;
            let entries = c
                .prepare_cached(
                    "SELECT key, cursor, event, state, stream, seq, at FROM view_entries
                     WHERE view = ?1 ORDER BY key, cursor",
                )?
                .query_map([view], |row| {
                    Ok(EntryRow {
                        key: row.get(0)?,
                        cursor: row.get(1)?,
                        event: row.get(2)?,
                        state: row.get(3)?,
                        stream: row.get(4)?,
                        seq: row.get(5)?,
                        at: row.get(6)?,
                    })
                })?
                .collect::<rusqlite::Result<_>>()?;
            let progress = c
                .prepare_cached("SELECT stream, seq FROM view_progress WHERE view = ?1")?
                .query_map([view], |row| Ok((row.get(0)?, row.get(1)?)))?
                .collect::<rusqlite::Result<_>>()?;
            Ok(ViewRows { keys, entries, progress })
        })
    }

    fn save_view(&self, view: &str, rows: &ViewRows) -> Result<(), StoreError> {
        self.run(|c| {
            let tx = c.savepoint()?;
            {
                let mut key = tx.prepare_cached(
                    "INSERT INTO view_keys (view, key, state) VALUES (?1, ?2, ?3)
                     ON CONFLICT (view, key) DO UPDATE SET state = excluded.state",
                )?;
                for k in &rows.keys {
                    key.execute(params![view, k.key, k.state])?;
                }
                let mut entry = tx.prepare_cached(
                    "INSERT OR REPLACE INTO view_entries (view, key, cursor, event, state, stream, seq, at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                )?;
                for e in &rows.entries {
                    entry.execute(params![view, e.key, e.cursor, e.event, e.state, e.stream, e.seq, e.at])?;
                }
                let mut progress = tx.prepare_cached(
                    "INSERT INTO view_progress (view, stream, seq) VALUES (?1, ?2, ?3)
                     ON CONFLICT (view, stream) DO UPDATE SET seq = excluded.seq",
                )?;
                for (stream, seq) in &rows.progress {
                    progress.execute(params![view, stream, seq])?;
                }
            }
            tx.commit()
        })
    }

    fn reset_view(&self, view: &str, version: u64) -> Result<(), StoreError> {
        self.run(|c| {
            let tx = c.savepoint()?;
            for table in ["view_keys", "view_entries", "view_progress"] {
                tx.execute(&format!("DELETE FROM {table} WHERE view = ?1"), [view])?;
            }
            tx.execute(
                "INSERT INTO views (name, version) VALUES (?1, ?2)
                 ON CONFLICT (name) DO UPDATE SET version = excluded.version",
                params![view, version],
            )?;
            tx.commit()
        })
    }

    fn save_schedule(&self, row: &ScheduleRow) -> Result<(), StoreError> {
        self.run(|c| {
            c.execute(
                "INSERT OR REPLACE INTO schedules (stream, command_id, command, id, payload, at, state)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![row.stream, row.command_id, row.command, row.id, row.payload, row.at, row.state],
            )?;
            Ok(())
        })
    }

    fn schedules(&self) -> Result<Vec<ScheduleRow>, StoreError> {
        self.run(|c| {
            let mut select =
                c.prepare_cached("SELECT stream, command_id, command, id, payload, at, state FROM schedules")?;
            select
                .query_map([], |row| {
                    Ok(ScheduleRow {
                        stream: row.get(0)?,
                        command_id: row.get(1)?,
                        command: row.get(2)?,
                        id: row.get(3)?,
                        payload: row.get(4)?,
                        at: row.get(5)?,
                        state: row.get(6)?,
                    })
                })?
                .collect()
        })
    }
}
