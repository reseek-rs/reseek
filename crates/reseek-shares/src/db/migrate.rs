//! Migrations: numbered SQL files under `migrations/<database>/`, built into the program, each
//! applied once in its own transaction and recorded with a checksum in `schema_migrations`. The
//! checksum covers the statements and leaves comments and layout out, so a comment can be
//! corrected in a migration that has already run.

use std::path::Path;
use std::time::Instant;

use chrono::Utc;
use rusqlite::{Connection, OptionalExtension, params};
use sha2::{Digest, Sha256};
use tracing::{debug, info, warn};

use super::{DbError, timestamp};

/// A database's name, which is also its file's stem, and its migrations in order.
#[derive(Debug)]
pub struct Schema {
  /// the name, such as `auth` for `auth.db`
  pub name: &'static str,
  /// the migrations, oldest first
  pub migrations: &'static [Migration],
  /// the file holds credentials, so on Unix it is created readable by its owner only
  pub private: bool,
}

/// One migration file.
#[derive(Debug)]
pub struct Migration {
  /// the file's stem, such as `0001_initial`: the version, an underscore and a name
  pub file: &'static str,
  /// the SQL it runs
  pub sql: &'static str,
}

impl Migration {
  /// The version, the number the file's stem starts with.
  pub fn version(&self) -> u32 {
    self
      .file
      .split_once('_')
      .and_then(|(number, _)| number.parse().ok())
      .unwrap_or_else(|| panic!("migration file {} has no version", self.file))
  }

  /// The name after the version.
  pub fn name(&self) -> &'static str {
    self
      .file
      .split_once('_')
      .map_or(self.file, |(_, name)| name)
  }

  fn checksum(&self) -> String {
    sha256(&statements(self.sql))
  }

  /// Whether `recorded` is a checksum of the whole file, comments included, as older versions
  /// recorded it: of this text, or of an earlier text listed in [`WHOLE_FILE_BEFORE`].
  fn recorded_whole(&self, database: &str, recorded: &str) -> bool {
    sha256(self.sql) == recorded
      || WHOLE_FILE_BEFORE
        .iter()
        .any(|(file, sum)| *file == format!("{database}/{}", self.file) && *sum == recorded)
  }
}

/// Checksums of whole migration files as older versions recorded them, for the files whose
/// comments have been corrected since: the file under `migrations/` and the checksum of its
/// earlier text.
const WHOLE_FILE_BEFORE: &[(&str, &str)] = &[];

fn sha256(text: &str) -> String {
  let digest = Sha256::digest(text.as_bytes());
  digest.iter().map(|b| format!("{b:02x}")).collect()
}

/// `sql` as SQLite reads it: comments left out and each run of whitespace and comments outside
/// quotes made one space, so only a change to the statements changes it.
fn statements(sql: &str) -> String {
  let mut out = String::with_capacity(sql.len());
  let mut chars = sql.chars().peekable();
  // a space is written only before the next token, so none leads or trails
  let mut gap = false;
  while let Some(c) = chars.next() {
    let comment = match (c, chars.peek()) {
      ('-', Some('-')) => {
        chars.find(|&c| c == '\n');
        true
      }
      ('/', Some('*')) => {
        chars.next();
        let mut star = false;
        // an unclosed comment runs to the end, as in SQLite
        for c in chars.by_ref() {
          if star && c == '/' {
            break;
          }
          star = c == '*';
        }
        true
      }
      _ => false,
    };
    if comment || c.is_ascii_whitespace() {
      gap = true;
      continue;
    }
    if gap && !out.is_empty() {
      out.push(' ');
    }
    gap = false;
    out.push(c);
    // a quoted string or name is kept as written; a doubled closing quote inside it is a quote
    let close = match c {
      '\'' | '"' | '`' => c,
      '[' => ']',
      _ => continue,
    };
    while let Some(c) = chars.next() {
      out.push(c);
      if c == close {
        if close != ']' && chars.peek() == Some(&close) {
          out.extend(chars.next());
          continue;
        }
        break;
      }
    }
  }
  out
}

/// A database's [`Schema`], its migration files read from `migrations/<name>/` at build time:
/// `schema!("events", ["0001_initial"])`, or `schema!("auth", ["0001_initial"], private)` for
/// a database only its owner may read.
#[macro_export]
macro_rules! schema {
  ($name:literal, [$($file:literal),* $(,)?]) => {
    $crate::db::schema!(@build $name, [$($file),*], false)
  };
  ($name:literal, [$($file:literal),* $(,)?], private) => {
    $crate::db::schema!(@build $name, [$($file),*], true)
  };
  (@build $name:literal, [$($file:literal),*], $private:literal) => {
    $crate::db::Schema {
      name: $name,
      migrations: &[$($crate::db::Migration {
        file: $file,
        sql: include_str!(concat!(
          env!("CARGO_MANIFEST_DIR"), "/../../migrations/", $name, "/", $file, ".sql"
        )),
      }),*],
      private: $private,
    }
  };
}

const RECORD: &str = "CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, name TEXT \
                      NOT NULL, applied_at TEXT NOT NULL, checksum TEXT NOT NULL)";

/// Brings the database on `conn` to the newest migration of `schema` and returns how many
/// migrations it applied. A database that has data is copied into `backups` first, when given;
/// `path` names the file in messages and backups.
pub(super) fn run(
  schema: &Schema,
  conn: &mut Connection,
  path: &Path,
  backups: Option<&Path>,
) -> Result<usize, DbError> {
  let objects: i64 = conn.query_row(
    "SELECT count(*) FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'",
    [],
    |row| row.get(0),
  )?;
  let fresh = objects == 0;
  let applied = if fresh {
    conn.execute_batch(RECORD)?;
    Vec::new()
  } else {
    let recorded: Option<String> = conn
      .query_row(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'schema_migrations'",
        [],
        |row| row.get(0),
      )
      .optional()?;
    if recorded.is_none() {
      return Err(DbError::NotOurs(path.to_path_buf()));
    }
    let mut statement =
      conn.prepare("SELECT version, name, checksum FROM schema_migrations ORDER BY version")?;
    statement
      .query_map([], |row| {
        Ok((
          row.get::<_, u32>(0)?,
          row.get::<_, String>(1)?,
          row.get::<_, String>(2)?,
        ))
      })?
      .collect::<rusqlite::Result<Vec<_>>>()?
  };
  let mut whole = Vec::new();
  for (version, name, checksum) in &applied {
    let Some(known) = schema.migrations.iter().find(|m| m.version() == *version) else {
      return Err(DbError::Newer {
        database: schema.name,
        version: *version,
      });
    };
    if known.checksum() == *checksum {
      continue;
    }
    if !known.recorded_whole(schema.name, checksum) {
      return Err(DbError::Checksum {
        path: path.to_path_buf(),
        database: schema.name,
        version: *version,
        name: name.clone(),
      });
    }
    whole.push(known);
  }
  if !whole.is_empty() {
    let tx = conn.transaction()?;
    for migration in &whole {
      tx.execute(
        "UPDATE schema_migrations SET checksum = ?1 WHERE version = ?2",
        params![migration.checksum(), migration.version()],
      )?;
    }
    tx.commit()?;
    info!(
      database = schema.name,
      count = whole.len(),
      "migration checksums recorded again without comments"
    );
  }
  let current = applied.last().map_or(0, |(version, ..)| *version);
  let pending: Vec<&Migration> = schema
    .migrations
    .iter()
    .filter(|m| m.version() > current)
    .collect();
  if pending.is_empty() {
    return Ok(0);
  }
  if let Some(backups) = backups.filter(|_| !fresh) {
    backup(conn, schema, current, backups)?;
  }
  // a fresh database, such as an in-memory one at every start, is being made rather than
  // upgraded, so its migrations are told at debug
  macro_rules! told {
    ($($line:tt)*) => {
      if fresh { debug!($($line)*) } else { info!($($line)*) }
    };
  }
  told!(
    database = schema.name,
    count = pending.len(),
    "outstanding database migration(s) to apply"
  );
  let total = pending.len();
  for (index, migration) in pending.into_iter().enumerate() {
    let started = Instant::now();
    told!(
      database = schema.name,
      migration = migration.file,
      step = index + 1,
      of = total,
      "Applying migration"
    );
    let tx = conn.transaction()?;
    tx.execute_batch(migration.sql)?;
    tx.execute(
      "INSERT INTO schema_migrations (version, name, applied_at, checksum) VALUES (?1, ?2, ?3, \
       ?4)",
      params![
        migration.version(),
        migration.name(),
        timestamp(Utc::now()),
        migration.checksum()
      ],
    )?;
    tx.commit()?;
    told!(
      database = schema.name,
      migration = migration.file,
      ms = started.elapsed().as_millis(),
      "migration applied"
    );
  }
  Ok(total)
}

/// Copies the database as it stands into `<backups>/<name>.db.<version>.bak.db`, a consistent
/// copy even with the write-ahead log. The copy of a private database is private too.
fn backup(conn: &Connection, schema: &Schema, version: u32, backups: &Path) -> Result<(), DbError> {
  let name = schema.name;
  let target = backups.join(format!("{name}.db.{version:04}.bak.db"));
  // VACUUM INTO refuses a file that exists, and an older backup of the same version is stale
  match std::fs::remove_file(&target) {
    Ok(()) => {}
    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
    Err(e) => {
      warn!(path = %target.display(), error = %e, "old database backup not removed");
    }
  }
  // VACUUM INTO writes into an empty file that exists and keeps its mode
  if schema.private {
    super::create_private(&target)?;
  }
  conn.execute("VACUUM INTO ?1", [target.to_string_lossy()])?;
  info!(database = name, path = %target.display(), "database backed up before migrating");
  Ok(())
}

#[cfg(test)]
pub(super) mod tests {
  use super::*;
  use crate::db::{Database, Location};

  /// A database only its owner may read, made with the migrations of `auth.db`.
  pub static PRIVATE: Schema = crate::schema!("auth", ["0001_initial"], private);

  use PRIVATE as TEST;

  #[tokio::test]
  async fn a_fresh_database_is_migrated_and_reopens_without_changes() {
    let dir = tempfile::tempdir().unwrap();
    let location = Location::on_disk(dir.path());
    let db = Database::open(&TEST, &location).await.unwrap();
    let tables: Vec<String> = db
      .read(|conn| {
        conn
          .prepare("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")?
          .query_map([], |row| row.get(0))?
          .collect()
      })
      .await
      .unwrap();
    assert_eq!(
      tables,
      [
        "api_tokens",
        "identities",
        "keys",
        "schema_migrations",
        "sessions"
      ]
    );
    let mode: String = db
      .read(|conn| conn.pragma_query_value(None, "journal_mode", |row| row.get(0)))
      .await
      .unwrap();
    assert_eq!(mode, "wal");
    db.close().await;
    drop(db);
    Database::open(&TEST, &location).await.unwrap();
    assert_eq!(
      std::fs::read_dir(&location.backups).unwrap().count(),
      0,
      "nothing was pending, so nothing was backed up"
    );
  }

  #[tokio::test]
  async fn foreign_files_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let location = Location::on_disk(dir.path());
    let foreign = Connection::open(location.data.join("auth.db")).unwrap();
    foreign
      .execute_batch("CREATE TABLE Events (Id TEXT)")
      .unwrap();
    drop(foreign);
    assert!(matches!(
      Database::open(&TEST, &location).await,
      Err(DbError::NotOurs(_))
    ));
  }

  #[tokio::test]
  async fn a_database_made_with_other_migrations_is_refused_untouched() {
    static CHAIN: Schema = Schema {
      private: false,
      name: "auth",
      migrations: &[
        Migration {
          file: "0001_initial",
          sql: "CREATE TABLE a (x INTEGER)",
        },
        Migration {
          file: "0002_more",
          sql: "CREATE TABLE b (y INTEGER)",
        },
      ],
    };
    static FOLDED: Schema = Schema {
      private: false,
      name: "auth",
      migrations: &[Migration {
        file: "0001_initial",
        sql: "CREATE TABLE a (x INTEGER); CREATE TABLE b (y INTEGER)",
      }],
    };
    let dir = tempfile::tempdir().unwrap();
    let location = Location::on_disk(dir.path());
    let db = Database::open(&CHAIN, &location).await.unwrap();
    db.write(|conn| conn.execute("INSERT INTO a VALUES (7)", []))
      .await
      .unwrap();
    db.close().await;
    drop(db);

    let refused = Database::open(&FOLDED, &location).await.err().unwrap();
    assert!(
      matches!(refused, DbError::Checksum { version: 1, .. }),
      "{refused:?}"
    );
    let message = refused.to_string();
    assert!(
      message.contains(&location.data.join("auth.db").display().to_string())
        && message.contains("data directory"),
      "the message names the file and what to do: {message}"
    );
    let kept = Connection::open(location.data.join("auth.db")).unwrap();
    let versions: Vec<u32> = kept
      .prepare("SELECT version FROM schema_migrations ORDER BY version")
      .unwrap()
      .query_map([], |row| row.get(0))
      .unwrap()
      .collect::<rusqlite::Result<_>>()
      .unwrap();
    assert_eq!(versions, [1, 2], "nothing is recorded on top");
    let x: i64 = kept
      .query_row("SELECT x FROM a", [], |row| row.get(0))
      .unwrap();
    assert_eq!(x, 7);
    assert_eq!(
      std::fs::read_dir(&location.backups).unwrap().count(),
      0,
      "nothing was applied, so nothing was backed up"
    );
  }

  #[tokio::test]
  async fn a_database_with_data_is_backed_up_for_its_owner_before_a_pending_migration() {
    static TWO: Schema = Schema {
      private: true,
      name: "auth",
      migrations: &[
        Migration {
          file: "0001_initial",
          sql: "CREATE TABLE a (x INTEGER)",
        },
        Migration {
          file: "0002_more",
          sql: "CREATE TABLE b (y INTEGER)",
        },
      ],
    };
    static ONE: Schema = Schema {
      private: true,
      name: "auth",
      migrations: &[Migration {
        file: "0001_initial",
        sql: "CREATE TABLE a (x INTEGER)",
      }],
    };
    let dir = tempfile::tempdir().unwrap();
    let location = Location::on_disk(dir.path());
    let db = Database::open(&ONE, &location).await.unwrap();
    db.write(|conn| conn.execute("INSERT INTO a VALUES (7)", []))
      .await
      .unwrap();
    drop(db);
    let db = Database::open(&TWO, &location).await.unwrap();
    let versions: Vec<u32> = db
      .read(|conn| {
        conn
          .prepare("SELECT version FROM schema_migrations")?
          .query_map([], |row| row.get(0))?
          .collect()
      })
      .await
      .unwrap();
    assert_eq!(versions, [1, 2]);
    let copy = location.backups.join("auth.db.0001.bak.db");
    #[cfg(unix)]
    {
      use std::os::unix::fs::PermissionsExt;
      let mode = std::fs::metadata(&copy).unwrap().permissions().mode();
      assert_eq!(
        mode & 0o777,
        0o600,
        "the copy of a private database is private"
      );
    }
    let backup = Connection::open(&copy).unwrap();
    let kept: i64 = backup
      .query_row("SELECT x FROM a", [], |row| row.get(0))
      .unwrap();
    assert_eq!(kept, 7);
    drop(db);
    assert!(matches!(
      Database::open(&ONE, &location).await,
      Err(DbError::Newer { version: 2, .. })
    ));
  }

  #[test]
  fn a_fresh_database_is_migrated_at_debug_and_one_with_data_at_info() {
    static ONE: Schema = Schema {
      private: false,
      name: "auth",
      migrations: &[Migration {
        file: "0001_initial",
        sql: "CREATE TABLE a (x INTEGER)",
      }],
    };
    static TWO: Schema = Schema {
      private: false,
      name: "auth",
      migrations: &[
        Migration {
          file: "0001_initial",
          sql: "CREATE TABLE a (x INTEGER)",
        },
        Migration {
          file: "0002_more",
          sql: "CREATE TABLE b (y INTEGER)",
        },
      ],
    };
    let mut conn = Connection::open_in_memory().unwrap();
    let path = Path::new("auth.db");
    let (applied, made) = crate::testing::logged(|| run(&ONE, &mut conn, path, None).unwrap());
    assert_eq!(applied, 1);
    let (applied, upgraded) = crate::testing::logged(|| run(&TWO, &mut conn, path, None).unwrap());
    assert_eq!(applied, 1);
    let levels = |text: &str| -> Vec<&str> {
      text
        .lines()
        .filter(|line| line.contains("migration"))
        .map(|line| {
          ["DEBUG", "INFO", "WARN"]
            .into_iter()
            .find(|level| line.contains(level))
            .unwrap()
        })
        .collect()
    };
    assert_eq!(levels(&made), ["DEBUG"; 3], "{made}");
    assert_eq!(levels(&upgraded), ["INFO"; 3], "{upgraded}");
  }

  #[test]
  fn statements_leave_comments_and_layout_out_and_keep_quotes() {
    let commented = "-- makes the table\nCREATE TABLE a (\n  x TEXT, /* the name */\n  y \
                     INTEGER -- the size\n);\n";
    let reworded = "-- makes the table a,\n-- over two lines\nCREATE  TABLE a (x TEXT,\ty \
                    INTEGER);";
    assert_eq!(
      statements(commented),
      "CREATE TABLE a ( x TEXT, y INTEGER );"
    );
    assert_eq!(statements(reworded), "CREATE TABLE a (x TEXT, y INTEGER);");
    let quoted = "INSERT INTO \"a  -- b\" VALUES ('it''s  /* not */ -- one', [c  d], `e``  f`)";
    assert_eq!(statements(quoted), quoted, "quoted text is kept as written");
    assert_eq!(statements("SELECT 1 /* open to the end"), "SELECT 1");
    assert_ne!(
      statements("SELECT 'a  b'"),
      statements("SELECT 'a b'"),
      "spaces inside a string are part of it"
    );
  }

  #[test]
  fn a_comment_corrected_after_its_migration_ran_changes_nothing() {
    static WRITTEN: Schema = Schema {
      private: false,
      name: "auth",
      migrations: &[Migration {
        file: "0001_initial",
        sql: "-- the rows are moved first\nCREATE TABLE a (x INTEGER);\n",
      }],
    };
    static CORRECTED: Schema = Schema {
      private: false,
      name: "auth",
      migrations: &[Migration {
        file: "0001_initial",
        sql: "-- the table starts empty,\n-- nothing is moved\nCREATE TABLE a (x INTEGER);\n",
      }],
    };
    let mut conn = Connection::open_in_memory().unwrap();
    let path = Path::new("auth.db");
    run(&WRITTEN, &mut conn, path, None).unwrap();
    assert_eq!(run(&CORRECTED, &mut conn, path, None).unwrap(), 0);
  }

  #[test]
  fn a_database_recorded_with_whole_file_checksums_is_recorded_again_once() {
    static ONE: Schema = Schema {
      private: false,
      name: "auth",
      migrations: &[Migration {
        file: "0001_initial",
        sql: "-- the first table\nCREATE TABLE a (x INTEGER);\n",
      }],
    };
    let path = Path::new("auth.db");
    let recorded = |conn: &Connection| -> String {
      conn
        .query_row("SELECT checksum FROM schema_migrations", [], |row| {
          row.get(0)
        })
        .unwrap()
    };
    let mut conn = Connection::open_in_memory().unwrap();
    run(&ONE, &mut conn, path, None).unwrap();
    conn
      .execute(
        "UPDATE schema_migrations SET checksum = ?1",
        [sha256(ONE.migrations[0].sql)],
      )
      .unwrap();
    let (applied, logs) = crate::testing::logged(|| run(&ONE, &mut conn, path, None).unwrap());
    assert_eq!(applied, 0);
    assert_eq!(recorded(&conn), ONE.migrations[0].checksum());
    assert!(
      logs.contains("INFO") && logs.contains("recorded again without comments"),
      "{logs}"
    );
    let (_, logs) = crate::testing::logged(|| run(&ONE, &mut conn, path, None).unwrap());
    assert!(!logs.contains("recorded again"), "only once: {logs}");

    conn
      .execute(
        "UPDATE schema_migrations SET checksum = ?1",
        [sha256("CREATE TABLE a (x TEXT);\n")],
      )
      .unwrap();
    assert!(
      matches!(
        run(&ONE, &mut conn, path, None),
        Err(DbError::Checksum { version: 1, .. })
      ),
      "the whole-file checksum of other statements is still refused"
    );
  }

  #[tokio::test]
  async fn a_volatile_database_lives_in_memory() {
    let dir = tempfile::tempdir().unwrap();
    let mut location = Location::on_disk(dir.path());
    location.volatile = true;
    let db = Database::open(&TEST, &location).await.unwrap();
    let count: i64 = db
      .read(|conn| conn.query_row("SELECT count(*) FROM sessions", [], |row| row.get(0)))
      .await
      .unwrap();
    assert_eq!(count, 0);
    assert_eq!(
      std::fs::read_dir(&location.data).unwrap().count(),
      0,
      "nothing reaches the disk"
    );
  }
}
