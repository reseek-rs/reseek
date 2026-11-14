//! Which users downloads wait on are offline. A download whose user goes offline stops asking
//! and waits, and one whose try finds them offline does not count the try: both wait until the
//! user is heard of again, from a status the server sends, a new login or the status the
//! waiting downloads ask for now and then, and then try again.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use reseek_shares::lock;
use tokio::sync::watch;
use tokio::time::Instant;

/// One user the downloads follow.
struct Followed {
  /// whether the last word on them was that they are offline
  offline: watch::Sender<bool>,
  /// when their status was last asked for by a download waiting for them
  asked: Option<Instant>,
}

/// By user, whether the last word on them was that they are offline, for the downloads that
/// follow them. A user no download follows any more is dropped.
#[derive(Default)]
pub(super) struct Away {
  users: Mutex<HashMap<String, Followed>>,
}

impl Away {
  /// Follows whether `username` is offline, as far as is known: not at first.
  pub fn follow(&self, username: &str) -> watch::Receiver<bool> {
    lock(&self.users)
      .entry(username.to_string())
      .or_insert_with(|| Followed {
        offline: watch::Sender::new(false),
        asked: None,
      })
      .offline
      .subscribe()
  }

  /// Takes word that `username` is `offline` or not, for the downloads that follow them.
  pub fn heard(&self, username: &str, offline: bool) {
    let mut users = lock(&self.users);
    users.retain(|_, user| user.offline.receiver_count() > 0);
    if let Some(user) = users.get(username) {
      user
        .offline
        .send_if_modified(|was| std::mem::replace(was, offline) != offline);
    }
  }

  /// Forgets that anyone is offline, after a login, so the downloads waiting on a user try
  /// them again in the new session.
  pub fn logged_in(&self) {
    let mut users = lock(&self.users);
    users.retain(|_, user| user.offline.receiver_count() > 0);
    for user in users.values() {
      user
        .offline
        .send_if_modified(|was| std::mem::replace(was, false));
    }
  }

  /// Whether a download waiting for `username` is to ask for their status now: none of their
  /// downloads asked within `every`. Taken as asked when it is.
  pub fn ask_due(&self, username: &str, every: Duration) -> bool {
    let now = Instant::now();
    let mut users = lock(&self.users);
    let Some(user) = users.get_mut(username) else {
      return false;
    };
    if user.asked.is_some_and(|asked| now < asked + every) {
      return false;
    }
    user.asked = Some(now);
    true
  }
}
