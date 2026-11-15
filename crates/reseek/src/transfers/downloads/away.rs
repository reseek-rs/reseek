//! What is known of the users downloads wait on. A download whose user goes offline stops
//! asking and waits, and one whose try finds them offline does not count the try: both wait
//! until the user is heard of again, from a status the server sends, a new login or the status
//! the waiting downloads ask for now and then, and then try again. A user the server says does
//! not exist ends their downloads. A user who left a file's ask unsettled, neither offering it
//! nor saying its place, is not waited for again until they offer a file or say a place.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use reseek_shares::lock;
use tokio::sync::{OwnedMutexGuard, watch};
use tokio::time::Instant;

/// The last word on a user.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Heard {
  /// nothing that keeps their downloads from asking
  Nothing,
  /// they are offline
  Offline,
  /// the server says there is no such user
  Missing,
}

/// One user the downloads follow.
struct Followed {
  heard: watch::Sender<Heard>,
  /// when their status was last asked for by one of their downloads, held while one asks
  asked: Arc<tokio::sync::Mutex<Option<Instant>>>,
  /// they neither offered a file nor said its place within the window of its ask
  silent: bool,
  /// when they last offered a file or said a file's place
  heard_at: Option<Instant>,
}

/// By user, the last word on them, for the downloads that follow them. A user no download
/// follows any more is dropped.
#[derive(Default)]
pub(super) struct Away {
  users: Mutex<HashMap<String, Followed>>,
}

impl Away {
  /// Follows the word on `username`: nothing at first.
  pub fn follow(&self, username: &str) -> watch::Receiver<Heard> {
    lock(&self.users)
      .entry(username.to_string())
      .or_insert_with(|| Followed {
        heard: watch::Sender::new(Heard::Nothing),
        asked: Arc::default(),
        silent: false,
        heard_at: None,
      })
      .heard
      .subscribe()
  }

  /// Takes word that `username` is `offline` or not, for the downloads that follow them. A
  /// user who does not exist stays so.
  pub fn heard(&self, username: &str, offline: bool) {
    let now = if offline {
      Heard::Offline
    } else {
      Heard::Nothing
    };
    let mut users = lock(&self.users);
    users.retain(|_, user| user.heard.receiver_count() > 0);
    if let Some(user) = users.get(username) {
      user
        .heard
        .send_if_modified(|was| *was != Heard::Missing && std::mem::replace(was, now) != now);
    }
  }

  /// Takes word that `username` does not exist; true the first time, while downloads follow
  /// them.
  pub fn missing(&self, username: &str) -> bool {
    lock(&self.users).get(username).is_some_and(|user| {
      user
        .heard
        .send_if_modified(|was| std::mem::replace(was, Heard::Missing) != Heard::Missing)
    })
  }

  /// The last word on `username`, nothing when no download follows them.
  pub fn now(&self, username: &str) -> Heard {
    lock(&self.users)
      .get(username)
      .map_or(Heard::Nothing, |user| *user.heard.borrow())
  }

  /// Forgets that anyone is offline, after a login, so the downloads waiting on a user try
  /// them again in the new session.
  pub fn logged_in(&self) {
    let mut users = lock(&self.users);
    users.retain(|_, user| user.heard.receiver_count() > 0);
    for user in users.values() {
      user.heard.send_if_modified(|was| {
        *was == Heard::Offline && std::mem::replace(was, Heard::Nothing) != Heard::Nothing
      });
    }
  }

  /// Takes word that `username` left a file's place question, asked at `asked`, unanswered for
  /// its whole window; they are silent unless they offered a file or said a place since the
  /// question, a late answer to it included. True when they became silent, while downloads
  /// follow them.
  pub fn went_silent(&self, username: &str, asked: Instant) -> bool {
    lock(&self.users).get_mut(username).is_some_and(|user| {
      let heard = user.heard_at.is_some_and(|at| at >= asked);
      !heard && !std::mem::replace(&mut user.silent, true)
    })
  }

  /// Takes word that `username` offered a file or said a file's place now, so their asks are
  /// waited for again; true when they were silent, while downloads follow them.
  pub fn answered(&self, username: &str) -> bool {
    lock(&self.users).get_mut(username).is_some_and(|user| {
      user.heard_at = Some(Instant::now());
      std::mem::replace(&mut user.silent, false)
    })
  }

  /// Whether `username` left the ask of a file unsettled for its whole window and has offered
  /// nothing and said no place since, while downloads followed them.
  pub fn silent(&self, username: &str) -> bool {
    lock(&self.users)
      .get(username)
      .is_some_and(|user| user.silent)
  }

  /// The turn of a download of `username` to ask for their status, with when one of their
  /// downloads last did; downloads asking at once take it in turn. None when no download
  /// follows them.
  pub async fn ask_turn(&self, username: &str) -> Option<OwnedMutexGuard<Option<Instant>>> {
    let asked = lock(&self.users).get(username)?.asked.clone();
    Some(asked.lock_owned().await)
  }
}
