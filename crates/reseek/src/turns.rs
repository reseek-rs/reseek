//! One at a time per name, a user's or a room's: a caller takes the name's turn, and a second
//! caller for the same name waits until the first lets go of it. A name's entry counts the
//! callers holding or waiting for its turn, and goes when the last one does.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use reseek_shares::lock;
use tokio::sync::OwnedMutexGuard;

/// One name's turn and how many callers hold or wait for it.
struct Entry {
  turn: Arc<tokio::sync::Mutex<()>>,
  callers: usize,
}

type Map = HashMap<String, Entry>;

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

  /// Whether someone holds or waits for `name`'s turn.
  pub fn held(&self, name: &str) -> bool {
    lock(&self.map).contains_key(name)
  }

  /// Gets in line for `name`'s turn: from here whoever holds it sees someone waiting. The turn
  /// is taken with [`Waiting::take`].
  pub fn wait(&self, name: &str) -> Waiting {
    let mut map = lock(&self.map);
    let entry = map.entry(name.to_string()).or_insert_with(|| Entry {
      turn: Arc::default(),
      callers: 0,
    });
    entry.callers += 1;
    Waiting {
      map: self.map.clone(),
      name: name.to_string(),
      turn: entry.turn.clone(),
    }
  }
}

/// A caller's place for one name's turn, in line and then holding it; the name's entry goes
/// with the last place.
pub struct Waiting {
  map: Arc<Mutex<Map>>,
  name: String,
  turn: Arc<tokio::sync::Mutex<()>>,
}

impl Waiting {
  /// Waits for the turn and holds it until the returned turn is dropped.
  pub async fn take(self) -> Turn {
    let guard = self.turn.clone().lock_owned().await;
    Turn {
      _guard: guard,
      place: self,
    }
  }

  /// Whether this is the only caller holding or waiting for the name's turn.
  fn alone(&self) -> bool {
    lock(&self.map)
      .get(&self.name)
      .is_some_and(|entry| entry.callers == 1)
  }
}

impl Drop for Waiting {
  fn drop(&mut self) {
    let mut map = lock(&self.map);
    let entry = map
      .get_mut(&self.name)
      .expect("a name's entry lives while a place for its turn does");
    entry.callers -= 1;
    if entry.callers == 0 {
      map.remove(&self.name);
    }
  }
}

/// One name's turn, held until dropped.
pub struct Turn {
  // let go before the place, so the turn is free by the time its entry may go
  _guard: OwnedMutexGuard<()>,
  place: Waiting,
}

impl Turn {
  /// Whether nobody else waits for this name's turn.
  pub fn alone(&self) -> bool {
    self.place.alone()
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::task::Poll;
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
    assert!(turns.held("alice") && turns.held("bob"));
    drop((second, other));
    assert!(!turns.held("alice") && !turns.held("bob"));
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
    assert!(!turns.held("alice") && !turns.held("bob"));
  }

  #[tokio::test]
  async fn a_take_given_up_leaves_no_entry() {
    let turns = Turns::default();
    let held = turns.take("alice").await;
    {
      let mut taking = std::pin::pin!(turns.take("alice"));
      let pending = std::future::poll_fn(|cx| Poll::Ready(taking.as_mut().poll(cx).is_pending()));
      assert!(pending.await, "in line behind the holder");
      assert!(!held.alone());
    }
    assert!(held.alone(), "given up");
    drop(held);
    assert!(!turns.held("alice"));
  }
}
