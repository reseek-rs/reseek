//! A new password for a local account ends that account's sessions, all but the one that set
//! it, once the configuration with it stands: which accounts a change gave a new password, and
//! which browser sessions set one through the API.

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

/// A browser session making a change to the configuration: its id, and the subject it is for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Changer {
  /// the session's id
  pub session: String,
  /// who the session is for
  pub subject: String,
}

/// A browser session that gave local accounts a new password.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Setter {
  /// the session
  pub changer: Changer,
  /// the subjects of the accounts it gave one
  pub accounts: Vec<String>,
}

impl Setter {
  /// Whether this is the session `session`, which set the password of `account`.
  pub fn set(&self, session: &str, account: &str) -> bool {
    self.changer.session == session && self.accounts.iter().any(|a| a == account)
  }
}

/// The browser sessions that set a new password through the API since the configuration last
/// stood, so the sessions a new password ends keep the one that set it.
#[derive(Default)]
pub struct Setters {
  /// held from before a session makes a change until the change is recorded, so the
  /// configuration is not taken to stand while a change is half recorded
  turn: tokio::sync::Mutex<()>,
  /// the sessions that set a new password, oldest first
  made: Mutex<Vec<Setter>>,
}

impl Setters {
  /// Takes the turn for a change `changer` is about to make, `None` for one no browser session
  /// makes; the change is recorded through [`Setting::made`].
  pub async fn begin(&self, changer: Option<Changer>) -> Setting<'_> {
    Setting {
      setters: self,
      changer,
      _turn: self.turn.lock().await,
    }
  }

  /// The sessions that set a new password since the last time this returned them, taken once
  /// no change is being made and `stands` holds; `None`, keeping them, when it does not.
  pub async fn take_if(&self, stands: impl FnOnce() -> bool) -> Option<Vec<Setter>> {
    let _turn = self.turn.lock().await;
    stands().then(|| std::mem::take(&mut *lock(&self.made)))
  }
}

/// The turn of a change being made.
pub struct Setting<'a> {
  setters: &'a Setters,
  changer: Option<Changer>,
  _turn: MutexGuard<'a, ()>,
}

impl Setting<'_> {
  /// Records the change made from `previous` to `current` authentication options, when it gave
  /// a local account a new password.
  pub fn made(self, previous: &Authentication, current: &Authentication) {
    let Some(changer) = self.changer else {
      return;
    };
    let accounts = changed(previous, current);
    if !accounts.is_empty() {
      lock(&self.setters.made).push(Setter { changer, accounts });
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

  fn changer(session: &str) -> Changer {
    Changer {
      session: session.into(),
      subject: "local:reseek".into(),
    }
  }

  #[tokio::test]
  async fn the_setters_are_taken_once_no_change_is_being_made_and_the_options_stand() {
    use futures_util::FutureExt;

    let before = Authentication::default();
    let after = Authentication {
      password: "new".into(),
      ..Authentication::default()
    };
    let setters = Setters::default();
    let setting = setters.begin(Some(changer("a"))).await;
    let mut taking = Box::pin(setters.take_if(|| true));
    assert!(
      (&mut taking).now_or_never().is_none(),
      "a change is being made"
    );
    setting.made(&before, &after);
    let taken = taking.await.unwrap();
    assert_eq!(taken.len(), 1);
    assert!(taken[0].set("a", "local:reseek"));
    assert!(
      !taken[0].set("a", "local:viewer"),
      "only the account it set"
    );
    assert!(!taken[0].set("b", "local:reseek"));

    setters
      .begin(Some(changer("b")))
      .await
      .made(&before, &after);
    // a change to nothing that gives a new password is no setter
    setters
      .begin(Some(changer("c")))
      .await
      .made(&before, &before);
    assert_eq!(
      setters.take_if(|| false).await,
      None,
      "kept while it does not stand"
    );
    let taken = setters.take_if(|| true).await.unwrap();
    assert_eq!(taken.len(), 1);
    assert!(taken[0].set("b", "local:reseek"));
    assert_eq!(setters.take_if(|| true).await, Some(Vec::new()));
  }
}
