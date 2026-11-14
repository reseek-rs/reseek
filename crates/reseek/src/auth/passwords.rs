//! A new password for a local account ends that account's sessions, all but the one that set
//! it: which accounts a configuration change gave a new password, and which browser session made
//! each change through the API.

use std::collections::HashMap;
use std::sync::Mutex;

use reseek_shares::lock;
use tokio::sync::MutexGuard;

use super::local::subject_of;
use crate::config::{Authentication, LocalUser};

/// What a password is checked against: its hash when one is set, else the password itself.
#[derive(PartialEq)]
enum Checked<'a> {
  Hash(&'a str),
  Plain(Option<&'a str>),
}

impl<'a> Checked<'a> {
  fn of(hash: Option<&'a str>, plain: Option<&'a str>) -> Self {
    match hash {
      Some(hash) => Self::Hash(hash),
      None => Self::Plain(plain),
    }
  }

  fn built_in(authentication: &'a Authentication) -> Self {
    Self::of(
      authentication.password_hash.as_deref(),
      Some(authentication.password.as_str()),
    )
  }

  fn user(user: &'a LocalUser) -> Self {
    Self::of(user.password_hash.as_deref(), user.password.as_deref())
  }
}

/// The subjects of the local accounts that `previous` and `current` both have, under the same
/// name, whose password differs. An account renamed or removed has no role, which ends its
/// sessions anyway.
pub fn changed(previous: &Authentication, current: &Authentication) -> Vec<String> {
  let mut subjects = Vec::new();
  if previous.username == current.username
    && Checked::built_in(previous) != Checked::built_in(current)
  {
    subjects.push(subject_of(&current.username));
  }
  for (name, user) in &current.local.users {
    if previous
      .local
      .users
      .get(name)
      .is_some_and(|before| Checked::user(before) != Checked::user(user))
    {
      subjects.push(subject_of(name));
    }
  }
  subjects
}

/// A browser session that changed the configuration: its id, and the subject it is for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Setter {
  /// the session's id
  pub session: String,
  /// who the session is for
  pub subject: String,
}

/// The configuration changes browser sessions made through the API, so the sessions a new
/// password ends keep the one that set it.
#[derive(Default)]
pub struct Setters {
  /// held from before a session makes a change until the change is recorded, so the change is
  /// not acted on before it is
  turn: tokio::sync::Mutex<()>,
  /// the session that made each change, by the revision the change gave the configuration
  made: Mutex<HashMap<String, Setter>>,
}

impl Setters {
  /// Takes the turn for a change `setter` is about to make, `None` for one no browser session
  /// makes; the change is recorded through [`Setting::made`].
  pub async fn begin(&self, setter: Option<Setter>) -> Setting<'_> {
    Setting {
      setters: self,
      setter,
      _turn: self.turn.lock().await,
    }
  }

  /// The session that made the change that gave the configuration `revision`, once a change
  /// being made is recorded; `None` for a change no session made through the API.
  pub async fn made_by(&self, revision: &str) -> Option<Setter> {
    let _turn = self.turn.lock().await;
    lock(&self.made).remove(revision)
  }

  /// Every session recorded as having made a change not asked about yet, for changes missed.
  pub async fn take_all(&self) -> Vec<Setter> {
    let _turn = self.turn.lock().await;
    lock(&self.made).drain().map(|(_, setter)| setter).collect()
  }
}

/// The turn of a change being made.
pub struct Setting<'a> {
  setters: &'a Setters,
  setter: Option<Setter>,
  _turn: MutexGuard<'a, ()>,
}

impl Setting<'_> {
  /// Records that the change gave the configuration `revision`; `None` when it changed nothing.
  pub fn made(self, revision: Option<&str>) {
    if let (Some(setter), Some(revision)) = (self.setter, revision) {
      lock(&self.setters.made).insert(revision.to_string(), setter);
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn a_password_or_its_hash_changed_is_a_new_password() {
    let mut before = Authentication::default();
    before.local.users.insert(
      "viewer".into(),
      LocalUser {
        password: Some("look".into()),
        ..LocalUser::default()
      },
    );
    let mut after = before.clone();
    assert!(changed(&before, &after).is_empty());
    after.password = "new".into();
    after.local.users.get_mut("viewer").unwrap().password = Some("seen".into());
    assert_eq!(changed(&before, &after), ["local:reseek", "local:viewer"]);

    // a hash set takes the place of the password, which then changes nothing
    before.password_hash = Some("$argon2id$a".into());
    after.password_hash = Some("$argon2id$a".into());
    assert_eq!(changed(&before, &after), ["local:viewer"]);
    after.password_hash = Some("$argon2id$b".into());
    assert_eq!(changed(&before, &after), ["local:reseek", "local:viewer"]);

    // renamed or removed, it has no role any more, which ends its sessions anyway
    after.username = "admin".into();
    after.local.users.clear();
    assert!(changed(&before, &after).is_empty());
  }
}
