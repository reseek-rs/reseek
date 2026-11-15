//! One at a time per name, a user's or a room's: a caller takes the name's turn, and a second
//! caller for the same name waits until the first lets go of it. A name's entry is kept only
//! while someone holds or waits for its turn.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use reseek_shares::lock;
use tokio::sync::OwnedMutexGuard;

type Map = HashMap<String, Arc<tokio::sync::Mutex<()>>>;

/// Every name's turn. Cloning gives another handle to the same turns.
#[derive(Clone, Default)]
pub struct Turns {
  map: Arc<Mutex<Map>>,
}

impl Turns {
  /// Waits for `name`'s turn and holds it until the returned turn is dropped.
  pub async fn take(&self, name: &str) -> Turn {
    self.wait(name).take().await
  }

  /// Gets in line for `name`'s turn: from here whoever holds it sees someone waiting. The turn
  /// is taken with [`Waiting::take`].
  pub fn wait(&self, name: &str) -> Waiting {
    let mutex = lock(&self.map).entry(name.to_string()).or_default().clone();
    Waiting {
      map: self.map.clone(),
      name: name.to_string(),
      mutex,
    }
  }
}

/// Whether `mutex`, a name's entry, is referred to only by the map and one other: the caller's
/// turn or place in line.
fn only_one(mutex: &Arc<tokio::sync::Mutex<()>>) -> bool {
  Arc::strong_count(mutex) == 2
}

/// A place in line for one name's turn; the name's entry goes when it is dropped untaken and
/// nobody else holds or waits for it.
pub struct Waiting {
  map: Arc<Mutex<Map>>,
  name: String,
  mutex: Arc<tokio::sync::Mutex<()>>,
}

impl Waiting {
  /// Waits for the turn and holds it until the returned turn is dropped.
  pub async fn take(self) -> Turn {
    let guard = self.mutex.clone().lock_owned().await;
    Turn {
      map: self.map.clone(),
      name: self.name.clone(),
      guard: Some(guard),
    }
  }
}

impl Drop for Waiting {
  fn drop(&mut self) {
    let mut map = lock(&self.map);
    // a turn taken from this place holds the guard's reference besides
    if map.get(&self.name).is_some_and(only_one) {
      map.remove(&self.name);
    }
  }
}

/// One name's turn, held until dropped; the name's entry goes once nobody holds or waits for
/// it.
pub struct Turn {
  map: Arc<Mutex<Map>>,
  name: String,
  guard: Option<OwnedMutexGuard<()>>,
}

impl Turn {
  /// Whether nobody else waits for this name's turn.
  pub fn alone(&self) -> bool {
    lock(&self.map).get(&self.name).is_some_and(only_one)
  }
}

impl Drop for Turn {
  fn drop(&mut self) {
    let mut map = lock(&self.map);
    let Some(mutex) = map.get(&self.name) else {
      return;
    };
    // the guard's is the other reference when nobody waits
    let idle = only_one(mutex);
    self.guard.take();
    if idle {
      map.remove(&self.name);
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::time::Duration;

  #[tokio::test]
  async fn a_second_caller_waits_and_the_entry_goes_with_the_last() {
    let turns = Turns::default();
    let first = turns.take("alice").await;
    let waiting = tokio::spawn({
      let turns = turns.clone();
      async move { turns.take("alice").await }
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!waiting.is_finished(), "alice's turn is held");
    assert!(!first.alone(), "the second caller waits for it");
    let other = turns.take("bob").await;
    assert!(other.alone());
    drop(first);
    let second = waiting.await.unwrap();
    assert!(second.alone());
    assert_eq!(lock(&turns.map).len(), 2);
    drop((second, other));
    assert!(lock(&turns.map).is_empty());
  }

  #[tokio::test]
  async fn a_place_in_line_counts_as_waiting_and_goes_untaken() {
    let turns = Turns::default();
    let held = turns.take("alice").await;
    let place = turns.wait("alice");
    assert!(!held.alone());
    drop(place);
    assert!(held.alone());
    drop(held);
    drop(turns.wait("bob"));
    assert!(lock(&turns.map).is_empty());
  }
}
