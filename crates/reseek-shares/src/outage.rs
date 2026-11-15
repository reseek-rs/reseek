//! The reasons a loop that keeps failing was warned about, so each is warned about once until
//! the loop succeeds again.

/// How many reasons an outage remembers; the oldest is forgotten past it, so a failure whose
/// text differs every time cannot grow the list without end.
const REASONS: usize = 16;

/// The reasons a failure that repeats through an outage, such as each attempt to reach a server
/// that is down, was warned about: each one once until the outage ends, so two reasons that take
/// turns are not warned about at every attempt. The owner ends the outage, usually once an
/// attempt succeeds.
#[derive(Debug, Default)]
pub struct Outage {
  warned: Vec<String>,
}

impl Outage {
  /// No outage.
  pub const fn new() -> Self {
    Self { warned: Vec::new() }
  }

  /// Whether `reason` is new to the outage, which it then belongs to: a warning tells a new
  /// reason and debug one seen before.
  pub fn new_reason(&mut self, reason: &str) -> bool {
    if self.warned.iter().any(|warned| warned == reason) {
      return false;
    }
    if self.warned.len() == REASONS {
      self.warned.remove(0);
    }
    self.warned.push(reason.to_string());
    true
  }

  /// Whether something failed since the outage began, so an attempt now is a retry.
  pub fn ongoing(&self) -> bool {
    !self.warned.is_empty()
  }

  /// Ends the outage: every reason is new again.
  pub fn end(&mut self) {
    self.warned.clear();
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn an_outage_warns_about_each_reason_once_until_it_ends() {
    let mut outage = Outage::new();
    assert!(!outage.ongoing());
    let told: Vec<bool> = ["refused", "timed out", "refused", "timed out"]
      .into_iter()
      .map(|reason| outage.new_reason(reason))
      .collect();
    assert_eq!(told, [true, true, false, false]);
    assert!(outage.ongoing());
    outage.end();
    assert!(!outage.ongoing());
    assert!(outage.new_reason("refused"), "a new outage warns again");
  }

  #[test]
  fn an_outage_forgets_its_oldest_reason_past_the_limit() {
    let mut outage = Outage::new();
    for n in 0..=REASONS {
      assert!(outage.new_reason(&n.to_string()));
    }
    assert!(outage.new_reason("0"), "the oldest was forgotten");
    assert!(!outage.new_reason(&REASONS.to_string()));
  }
}
