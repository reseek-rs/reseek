//! A new password for a local account ends that account's sessions, all but the one that set
//! it, once the configuration with it stands: which accounts a change gave a password, the
//! password each account had when the options last stood, kept in `auth.db` so a change made
//! while the daemon was stopped is found at the next start, and which browser sessions set one
//! through the API.

use std::collections::HashMap;
use std::ops::Range;

use chrono::{DateTime, Utc};
use rusqlite::params;
use tokio::sync::MutexGuard;

use super::local::{self, subject_of};
use crate::config::{Authentication, LocalUser};
use crate::db::{Database, DbError, timestamp};

/// A local account's password as the configuration holds it. Not `Debug`, so it is never
/// logged.
#[derive(Clone, PartialEq)]
enum Password {
  /// the hash it is checked against
  Hash(String),
  /// the password itself
  Plain(String),
}

impl Password {
  /// The password of an account with `hash` and `plain`: the hash when one is set. `None` for
  /// neither, which the configuration refuses when it loads.
  fn of(hash: Option<&String>, plain: Option<&String>) -> Option<Self> {
    match (hash, plain) {
      (Some(hash), _) => Some(Self::Hash(hash.clone())),
      (None, Some(plain)) => Some(Self::Plain(plain.clone())),
      (None, None) => None,
    }
  }

  fn user(user: &LocalUser) -> Option<Self> {
    Self::of(user.password_hash.as_ref(), user.password.as_ref())
  }

  /// What is kept of it: a configured hash as it is, a plain password as an argon2 hash with a
  /// fresh salt, so the kept value tells no more of the password than the configuration does.
  /// Slow for a plain one.
  fn fingerprint(&self) -> String {
    match self {
      Self::Hash(hash) => format!("hash:{hash}"),
      Self::Plain(plain) => format!("argon2:{}", local::hash(plain)),
    }
  }

  /// Whether `fingerprint` was kept of this password. Slow for a plain one.
  fn kept_as(&self, fingerprint: &str) -> bool {
    match self {
      Self::Hash(hash) => fingerprint.strip_prefix("hash:") == Some(hash.as_str()),
      Self::Plain(plain) => fingerprint
        .strip_prefix("argon2:")
        .is_some_and(|phc| local::verify(phc, plain)),
    }
  }
}

/// The local accounts of `authentication` by subject, with their passwords.
fn accounts(authentication: &Authentication) -> HashMap<String, Password> {
  let built_in = Password::of(
    authentication.password_hash.as_ref(),
    Some(&authentication.password),
  );
  let users = authentication
    .local
    .users
    .iter()
    .filter_map(|(name, user)| Some((subject_of(name), Password::user(user)?)));
  // the built-in account wins a name it shares with an extra one, as when checking a password
  users
    .chain(built_in.map(|password| (subject_of(&authentication.username), password)))
    .collect()
}

/// The subjects of the local accounts `current` gives a password `previous` does not give
/// them: a new password, or an account added or renamed.
pub fn given(previous: &Authentication, current: &Authentication) -> Vec<String> {
  let before = accounts(previous);
  let mut subjects: Vec<String> = accounts(current)
    .into_iter()
    .filter(|(subject, password)| before.get(subject) != Some(password))
    .map(|(subject, _)| subject)
    .collect();
  subjects.sort();
  subjects
}

/// The passwords the local accounts were given since the options last stood, each from the time
/// it was given until the next. A session begun while an account had a password other than the
/// one that stands ends.
#[derive(Default)]
pub struct History(HashMap<String, Given>);

/// The passwords one account was given, each with when, none for its removal.
type Given = Vec<(DateTime<Utc>, Option<Password>)>;

impl History {
  /// The history of a start at `started` with `authentication`, which may be a file read half
  /// written: each account has its password from then.
  pub fn started(started: DateTime<Utc>, authentication: &Authentication) -> Self {
    Self(
      accounts(authentication)
        .into_iter()
        .map(|(subject, password)| (subject, vec![(started, Some(password))]))
        .collect(),
    )
  }

  /// Notes the change at `at` from `previous` to `current`: the accounts given another password,
  /// added, renamed or removed.
  pub fn note(&mut self, at: DateTime<Utc>, previous: &Authentication, current: &Authentication) {
    let mut before = accounts(previous);
    for (subject, password) in accounts(current) {
      if before.remove(&subject).as_ref() != Some(&password) {
        self
          .0
          .entry(subject)
          .or_default()
          .push((at, Some(password)));
      }
    }
    for subject in before.into_keys() {
      self.0.entry(subject).or_default().push((at, None));
    }
  }

  /// Forgets all but the accounts of `kept`, whose sessions are still to be ended.
  pub fn keep_only(&mut self, kept: &[String]) {
    self.0.retain(|subject, _| kept.contains(subject));
  }
}

/// A local account's password compared with the one kept for it.
pub struct Compared {
  /// the account
  pub subject: String,
  /// whether the password kept for it is another, or nothing was kept and it was given one
  /// while the options did not stand
  pub changed: bool,
  /// when it was given the password that stands, for a changed one
  pub changed_at: DateTime<Utc>,
  /// the times a session that ends began in: before the wait for a changed one, and while it
  /// had another password
  pub ended: Vec<Range<DateTime<Utc>>>,
  /// what is to be kept for it now, `None` when the kept one stands
  fingerprint: Option<String>,
}

/// The local accounts of `authentication` compared with the passwords kept for them, those
/// with nothing kept as well. `history` says which passwords they were given while the options
/// did not stand; a changed account given none changed before `started`, while stopped.
pub async fn compare(
  db: &Database,
  authentication: &Authentication,
  history: &History,
  started: DateTime<Utc>,
) -> Result<Vec<Compared>, DbError> {
  let kept: HashMap<String, String> = db
    .read(|conn| {
      let mut statement = conn.prepare("SELECT subject, fingerprint FROM passwords")?;
      statement
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect()
    })
    .await?;
  let accounts: Vec<(String, Password, Given)> = accounts(authentication)
    .into_iter()
    .map(|(subject, password)| {
      let given = history.0.get(&subject).cloned().unwrap_or_default();
      (subject, password, given)
    })
    .collect();
  // argon2 takes a while for each plain password
  tokio::task::spawn_blocking(move || {
    accounts
      .into_iter()
      .map(|(subject, password, given)| {
        let (changed, fingerprint) = match kept.get(&subject) {
          Some(kept) if password.kept_as(kept) => (false, None),
          Some(_) => (true, Some(password.fingerprint())),
          None => (!given.is_empty(), Some(password.fingerprint())),
        };
        let first = given.first().map_or(started, |(at, _)| *at);
        let mut ended = Vec::new();
        if changed {
          ended.push(DateTime::<Utc>::MIN_UTC..first);
        }
        for ((from, had), (to, _)) in given.iter().zip(given.iter().skip(1)) {
          if had.as_ref() != Some(&password) {
            ended.push(*from..*to);
          }
        }
        Compared {
          subject,
          changed,
          changed_at: given.last().map_or(started, |(at, _)| *at),
          ended,
          fingerprint,
        }
      })
      .collect()
  })
  .await
  .map_err(|e| DbError::Task(e.to_string()))
}

/// Keeps what `compared`, every account the options give, has to keep, with the time each
/// changed, or now for one kept first, but for the accounts of `failed`, whose old password
/// stays kept. What was kept for an account the options no longer have is deleted.
pub async fn keep(db: &Database, compared: &[Compared], failed: &[String]) -> Result<(), DbError> {
  let now = Utc::now();
  let rows: Vec<(String, String, String)> = compared
    .iter()
    .filter(|c| !failed.contains(&c.subject))
    .filter_map(|c| {
      let at = if c.changed { c.changed_at } else { now };
      Some((c.subject.clone(), c.fingerprint.clone()?, timestamp(at)))
    })
    .collect();
  let standing: Vec<String> = compared.iter().map(|c| c.subject.clone()).collect();
  db.write(move |conn| {
    let tx = conn.transaction()?;
    let kept: Vec<String> = {
      let mut statement = tx.prepare("SELECT subject FROM passwords")?;
      statement
        .query_map([], |row| row.get(0))?
        .collect::<rusqlite::Result<_>>()?
    };
    for subject in kept.iter().filter(|s| !standing.contains(s)) {
      tx.execute("DELETE FROM passwords WHERE subject = ?1", params![subject])?;
    }
    for (subject, fingerprint, at) in rows {
      tx.execute(
        "INSERT INTO passwords (subject, fingerprint, changed_at) VALUES (?1, ?2, ?3) ON \
         CONFLICT (subject) DO UPDATE SET fingerprint = excluded.fingerprint, changed_at = \
         excluded.changed_at",
        params![subject, fingerprint, at],
      )?;
    }
    tx.commit()
  })
  .await
}

/// Keeps the passwords `authentication` gives the local accounts nothing is kept for, as at the
/// first start, so a change made before the options first stand is told from them.
pub async fn keep_first(db: &Database, authentication: &Authentication) -> Result<(), DbError> {
  let kept: Vec<String> = db
    .read(|conn| {
      let mut statement = conn.prepare("SELECT subject FROM passwords")?;
      statement.query_map([], |row| row.get(0))?.collect()
    })
    .await?;
  let missing: Vec<(String, Password)> = accounts(authentication)
    .into_iter()
    .filter(|(subject, _)| !kept.contains(subject))
    .collect();
  if missing.is_empty() {
    return Ok(());
  }
  let rows: Vec<(String, String)> = tokio::task::spawn_blocking(move || {
    missing
      .into_iter()
      .map(|(subject, password)| (subject, password.fingerprint()))
      .collect()
  })
  .await
  .map_err(|e| DbError::Task(e.to_string()))?;
  let now = timestamp(Utc::now());
  db.write(move |conn| {
    let tx = conn.transaction()?;
    for (subject, fingerprint) in rows {
      tx.execute(
        "INSERT INTO passwords (subject, fingerprint, changed_at) VALUES (?1, ?2, ?3) ON \
         CONFLICT (subject) DO NOTHING",
        params![subject, fingerprint, now],
      )?;
    }
    tx.commit()
  })
  .await
}

/// A browser session making a change to the configuration: its id, and the subject it is for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Changer {
  /// the session's id
  pub session: String,
  /// who the session is for
  pub subject: String,
}

/// A browser session that gave a local account a new password.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Setter {
  /// the session
  pub changer: Changer,
  /// the subject of the account it gave one
  pub account: String,
}

impl Setter {
  /// Whether this is the session `session`, which set the password of `account`.
  pub fn set(&self, session: &str, account: &str) -> bool {
    self.changer.session == session && self.account == account
  }
}

/// The turn for the changes browser sessions make, so the configuration is not taken to stand
/// while one is half recorded. The sessions that set a password are kept in `auth.db` until the
/// options stand, so they are kept across a restart.
#[derive(Default)]
pub struct Setters {
  turn: tokio::sync::Mutex<()>,
}

impl Setters {
  /// Takes the turn for a change `changer` is about to make, `None` for one no browser session
  /// makes; the change is recorded through [`Setting::made`].
  pub async fn begin(&self, changer: Option<Changer>) -> Setting<'_> {
    Setting {
      changer,
      _turn: self.turn.lock().await,
    }
  }

  /// The sessions recorded in `db` as having set a password, taken once no change is being
  /// made and `stands` holds; `None`, keeping them, when it does not.
  pub async fn take_if(
    &self,
    db: &Database,
    stands: impl FnOnce() -> bool,
  ) -> Result<Option<Vec<Setter>>, DbError> {
    let _turn = self.turn.lock().await;
    if !stands() {
      return Ok(None);
    }
    db.write(|conn| {
      let tx = conn.transaction()?;
      let setters = {
        let mut statement =
          tx.prepare("SELECT session_id, subject, account FROM password_setters")?;
        statement
          .query_map([], |row| {
            Ok(Setter {
              changer: Changer {
                session: row.get(0)?,
                subject: row.get(1)?,
              },
              account: row.get(2)?,
            })
          })?
          .collect::<rusqlite::Result<Vec<_>>>()?
      };
      tx.execute("DELETE FROM password_setters", [])?;
      tx.commit()?;
      Ok(setters)
    })
    .await
    .map(Some)
  }
}

/// The turn of a change being made.
pub struct Setting<'a> {
  changer: Option<Changer>,
  _turn: MutexGuard<'a, ()>,
}

impl Setting<'_> {
  /// Records in `db` the change made from `previous` to `current` authentication options, when
  /// it gave a local account a new password.
  pub async fn made(
    self,
    db: &Database,
    previous: &Authentication,
    current: &Authentication,
  ) -> Result<(), DbError> {
    let Some(changer) = self.changer else {
      return Ok(());
    };
    let accounts = given(previous, current);
    if accounts.is_empty() {
      return Ok(());
    }
    db.write(move |conn| {
      let tx = conn.transaction()?;
      for account in accounts {
        tx.execute(
          "INSERT INTO password_setters (session_id, subject, account) VALUES (?1, ?2, ?3)",
          params![changer.session, changer.subject, account],
        )?;
      }
      tx.commit()
    })
    .await
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::auth::store::SCHEMA;

  #[test]
  fn a_password_or_its_hash_changed_or_an_account_added_is_a_new_password() {
    let mut before = Authentication::default();
    before.local.users.insert(
      "viewer".into(),
      LocalUser {
        password: Some("look".into()),
        ..LocalUser::default()
      },
    );
    let mut after = before.clone();
    assert!(given(&before, &after).is_empty());
    after.password = "new".into();
    after.local.users.get_mut("viewer").unwrap().password = Some("seen".into());
    assert_eq!(given(&before, &after), ["local:reseek", "local:viewer"]);

    // a hash set takes the place of the password, which then changes nothing
    before.password_hash = Some("$argon2id$a".into());
    after.password_hash = Some("$argon2id$a".into());
    assert_eq!(given(&before, &after), ["local:viewer"]);
    after.password_hash = Some("$argon2id$b".into());
    assert_eq!(given(&before, &after), ["local:reseek", "local:viewer"]);

    // added or renamed, an account has a password it had not
    let renamed = Authentication {
      username: "admin".into(),
      ..before.clone()
    };
    assert_eq!(given(&before, &renamed), ["local:admin"]);
    let mut without = before.clone();
    without.local.users.clear();
    assert_eq!(given(&without, &before), ["local:viewer"]);
    // removed, it has no role any more, which ends its sessions anyway
    assert!(given(&before, &without).is_empty());
  }

  #[tokio::test]
  async fn a_password_is_kept_as_its_hash_or_a_salted_hash_and_compared() {
    let db = Database::memory(&SCHEMA).await;
    let plain = Authentication::default();
    let hashed = Authentication {
      password_hash: Some(local::hash("s3cret")),
      ..Authentication::default()
    };
    let started: DateTime<Utc> = "2026-11-14T11:00:00Z".parse().unwrap();
    let at: DateTime<Utc> = "2026-11-14T12:00:00Z".parse().unwrap();
    // `given`: the accounts were given their passwords at `at`, while the options did not stand
    let compare = async |authentication: &Authentication, given: bool| {
      let history = if given {
        History::started(at, authentication)
      } else {
        History::default()
      };
      let compared = compare(&db, authentication, &history, started)
        .await
        .unwrap();
      keep(&db, &compared, &[]).await.unwrap();
      let mut compared: Vec<_> = compared
        .iter()
        .map(|c| (c.subject.clone(), c.changed))
        .collect();
      compared.sort();
      compared
    };
    let kept = async || {
      db.read(|conn| {
        conn.query_row("SELECT fingerprint, changed_at FROM passwords", [], |row| {
          Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
      })
      .await
      .unwrap()
    };
    let unchanged = vec![("local:reseek".to_string(), false)];
    let changed = vec![("local:reseek".to_string(), true)];
    assert_eq!(
      compare(&plain, false).await,
      unchanged,
      "nothing kept yet, and no password given while the options did not stand"
    );
    let (fingerprint, _) = kept().await;
    assert!(
      fingerprint.starts_with("argon2:$argon2id$"),
      "never the password itself"
    );
    assert!(!fingerprint.contains("reseek"));
    assert_eq!(compare(&plain, false).await, unchanged);
    assert_eq!(compare(&hashed, true).await, changed);
    assert_eq!(kept().await.1, timestamp(at));
    assert_eq!(
      kept().await.0,
      format!("hash:{}", hashed.password_hash.clone().unwrap())
    );
    assert_eq!(compare(&hashed, true).await, unchanged);
    assert_eq!(compare(&plain, false).await, changed);

    // an account with nothing kept that was given a password while the options did not stand
    let mut added = plain.clone();
    added.local.users.insert(
      "viewer".into(),
      LocalUser {
        password: Some("look".into()),
        ..LocalUser::default()
      },
    );
    assert_eq!(
      compare(&added, true).await,
      [
        ("local:reseek".to_string(), false),
        ("local:viewer".to_string(), true)
      ]
    );

    // removed, an account has nothing kept
    assert_eq!(compare(&plain, false).await, unchanged);
    let subjects: Vec<String> = db
      .read(|conn| {
        conn
          .prepare("SELECT subject FROM passwords")?
          .query_map([], |row| row.get(0))?
          .collect()
      })
      .await
      .unwrap();
    assert_eq!(subjects, ["local:reseek"]);
  }

  fn changer(session: &str) -> Changer {
    Changer {
      session: session.into(),
      subject: "local:reseek".into(),
    }
  }

  #[tokio::test]
  async fn the_setters_are_taken_once_no_change_is_being_made_and_the_options_stand() {
    use futures_util::FutureExt;

    let db = Database::memory(&SCHEMA).await;
    let before = Authentication::default();
    let after = Authentication {
      password: "new".into(),
      ..Authentication::default()
    };
    let setters = Setters::default();
    let setting = setters.begin(Some(changer("a"))).await;
    let mut taking = Box::pin(setters.take_if(&db, || true));
    assert!(
      (&mut taking).now_or_never().is_none(),
      "a change is being made"
    );
    setting.made(&db, &before, &after).await.unwrap();
    let taken = taking.await.unwrap().unwrap();
    assert_eq!(taken.len(), 1);
    assert!(taken[0].set("a", "local:reseek"));
    assert!(
      !taken[0].set("a", "local:viewer"),
      "only the account it set"
    );
    assert!(!taken[0].set("b", "local:reseek"));

    let setting = setters.begin(Some(changer("b"))).await;
    setting.made(&db, &before, &after).await.unwrap();
    // a change to nothing that gives a new password is no setter
    let setting = setters.begin(Some(changer("c"))).await;
    setting.made(&db, &before, &before).await.unwrap();
    assert_eq!(
      setters.take_if(&db, || false).await.unwrap(),
      None,
      "kept while it does not stand"
    );
    let taken = setters.take_if(&db, || true).await.unwrap().unwrap();
    assert_eq!(taken.len(), 1);
    assert!(taken[0].set("b", "local:reseek"));
    assert_eq!(
      setters.take_if(&db, || true).await.unwrap(),
      Some(Vec::new())
    );
  }
}
