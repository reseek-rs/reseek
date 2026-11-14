//! Which users downloads wait on are offline. A download whose user goes offline stops asking
//! and waits, and one whose try finds them offline does not count the try: both wait until the
//! user is heard of again, from a status the server sends or a new login, and then try again.

use std::collections::HashMap;
use std::sync::Mutex;

use reseek_shares::lock;
use tokio::sync::watch;

/// By user, whether the last word on them was that they are offline, for the downloads that
/// follow them. A user no download follows any more is dropped.
#[derive(Default)]
pub(super) struct Away {
  users: Mutex<HashMap<String, watch::Sender<bool>>>,
}

impl Away {
  /// Follows whether `username` is offline, as far as is known: not at first.
  pub fn follow(&self, username: &str) -> watch::Receiver<bool> {
    lock(&self.users)
      .entry(username.to_string())
      .or_insert_with(|| watch::Sender::new(false))
      .subscribe()
  }

  /// Takes word that `username` is `offline` or not, for the downloads that follow them.
  pub fn heard(&self, username: &str, offline: bool) {
    let mut users = lock(&self.users);
    users.retain(|_, user| user.receiver_count() > 0);
    if let Some(user) = users.get(username) {
      user.send_if_modified(|was| std::mem::replace(was, offline) != offline);
    }
  }

  /// Forgets that anyone is offline, after a login, so the downloads waiting on a user try
  /// them again in the new session.
  pub fn logged_in(&self) {
    let mut users = lock(&self.users);
    users.retain(|_, user| user.receiver_count() > 0);
    for user in users.values() {
      user.send_if_modified(|was| std::mem::replace(was, false));
    }
  }
}
