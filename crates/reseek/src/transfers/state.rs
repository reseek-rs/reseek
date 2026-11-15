//! A transfer's state as the daemon keeps it, with the data that belongs to a state in it, the
//! moves the daemon may make between states, and the integer the `State` column holds for each.
//!
//! The daemon follows the client's moves, which only go forward and end in `Completed`, and
//! adds two of its own: any state but a completed one may be written again with new figures,
//! and may go back to waiting here, for a retry, a pause or a resume. Nothing leaves a
//! completed state, so a write that races a transfer which completed meanwhile is refused.

use std::fmt;

use api_types::transfers::{TransferOutcome, TransferStates};
use chrono::{DateTime, Utc};

/// Where a transfer is.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum State {
  /// our request was written to the peer and is not acknowledged yet
  Requested,
  /// waiting here for a slot, a retry, a resume or the enqueue throttle
  QueuedLocally {
    /// when a download waiting to be retried tries again
    next_attempt_at: Option<DateTime<Utc>>,
  },
  /// the peer acknowledged and queued us
  QueuedRemotely {
    /// the place in the peer's queue it last reported
    place_in_queue: Option<u32>,
  },
  /// accepted, the transfer connection is being made
  Initializing,
  /// bytes are flowing
  InProgress,
  /// finished, with how it ended
  Completed(TransferOutcome),
}

impl State {
  /// Waiting here, with no retry to wait for.
  pub const WAITING: Self = Self::QueuedLocally {
    next_attempt_at: None,
  };

  /// The state as the API and the database name it.
  pub fn kind(self) -> TransferStates {
    match self {
      Self::Requested => TransferStates::Requested,
      Self::QueuedLocally { .. } => TransferStates::QueuedLocally,
      Self::QueuedRemotely { .. } => TransferStates::QueuedRemotely,
      Self::Initializing => TransferStates::Initializing,
      Self::InProgress => TransferStates::InProgress,
      Self::Completed(outcome) => TransferStates::Completed(outcome),
    }
  }

  /// The state `kind` with none of its data.
  pub fn of(kind: TransferStates) -> Self {
    match kind {
      TransferStates::Requested => Self::Requested,
      TransferStates::QueuedLocally => Self::WAITING,
      TransferStates::QueuedRemotely => Self::QueuedRemotely {
        place_in_queue: None,
      },
      TransferStates::Initializing => Self::Initializing,
      TransferStates::InProgress => Self::InProgress,
      TransferStates::Completed(outcome) => Self::Completed(outcome),
    }
  }

  /// Whether the transfer has finished, whatever the outcome.
  pub fn is_completed(self) -> bool {
    matches!(self, Self::Completed(_))
  }
}

impl fmt::Display for State {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str(self.kind().as_str())
  }
}

/// A move the state machine does not allow.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Refused {
  /// the state the transfer is in
  pub from: TransferStates,
  /// the state it was to move to
  pub to: TransferStates,
}

impl fmt::Display for Refused {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(
      f,
      "a transfer cannot move from {} to {}",
      self.from, self.to
    )
  }
}

impl std::error::Error for Refused {}

/// Whether a transfer in `from` may be written in `to`.
pub fn allows(from: TransferStates, to: TransferStates) -> bool {
  use TransferStates::*;
  match (from, to) {
    (Completed(_), _) => false,
    (_, Completed(_) | QueuedLocally) => true,
    (from, to) if from == to => true,
    (QueuedLocally, Requested | Initializing)
    | (Requested, QueuedRemotely | Initializing)
    | (QueuedRemotely, Initializing)
    | (Initializing, InProgress) => true,
    _ => false,
  }
}

/// Every state a transfer may be in for it to be written in `to`.
pub fn entered_from(to: TransferStates) -> impl Iterator<Item = TransferStates> {
  TransferStates::ALL
    .into_iter()
    .filter(move |from| allows(*from, to))
}

/// The state the client reports as `state`.
pub fn reported(state: soulseek_client::TransferState) -> TransferStates {
  use soulseek_client::{TransferOutcome as Client, TransferState as Reported};
  match state {
    Reported::QueuedLocally => TransferStates::QueuedLocally,
    Reported::Requested => TransferStates::Requested,
    Reported::QueuedRemotely => TransferStates::QueuedRemotely,
    Reported::Initializing => TransferStates::Initializing,
    Reported::InProgress => TransferStates::InProgress,
    Reported::Completed(outcome) => TransferStates::Completed(match outcome {
      Client::Succeeded => TransferOutcome::Succeeded,
      Client::Canceled => TransferOutcome::Cancelled,
      Client::TimedOut => TransferOutcome::TimedOut,
      Client::Errored => TransferOutcome::Errored,
      Client::Rejected => TransferOutcome::Rejected,
      Client::Aborted => TransferOutcome::Aborted,
    }),
  }
}

/// The integer the `State` column holds for `state`: the bits of the flags its name is made
/// of, `Completed` 16 with one outcome bit, `Queued` 2 with `Locally` or `Remotely`.
pub fn column(state: TransferStates) -> u32 {
  match state {
    TransferStates::Requested => 1,
    TransferStates::QueuedLocally => 2 | 2048,
    TransferStates::QueuedRemotely => 2 | 4096,
    TransferStates::Initializing => 4,
    TransferStates::InProgress => 8,
    TransferStates::Completed(outcome) => {
      16 | match outcome {
        TransferOutcome::Succeeded => 32,
        TransferOutcome::Cancelled => 64,
        TransferOutcome::TimedOut => 128,
        TransferOutcome::Errored => 256,
        TransferOutcome::Rejected => 512,
        TransferOutcome::Aborted => 1024,
      }
    }
  }
}

/// The state the `State` column's `value` holds.
pub fn from_column(value: u32) -> Option<TransferStates> {
  TransferStates::ALL
    .into_iter()
    .find(|state| column(*state) == value)
}

/// The state in the `State` column `i` of `row`.
pub fn state_at(row: &rusqlite::Row, i: usize) -> rusqlite::Result<TransferStates> {
  let value = row.get::<_, u32>(i)?;
  from_column(value).ok_or_else(|| {
    rusqlite::Error::FromSqlConversionFailure(
      i,
      rusqlite::types::Type::Integer,
      format!("{value} is not a transfer state").into(),
    )
  })
}

/// An SQL condition that holds for a row of `Transfers` in one of `states`.
pub fn in_states(states: impl IntoIterator<Item = TransferStates>) -> String {
  let values: Vec<String> = states
    .into_iter()
    .map(|state| column(state).to_string())
    .collect();
  format!("(State IN ({}))", values.join(", "))
}

#[cfg(test)]
pub(crate) mod tests {
  use super::*;
  use TransferOutcome::*;
  use TransferStates::*;

  const OPEN: [TransferStates; 5] = [
    Requested,
    QueuedLocally,
    QueuedRemotely,
    Initializing,
    InProgress,
  ];

  #[test]
  fn the_daemon_moves_forward_waits_again_or_completes_and_never_leaves_completed() {
    let forward = [
      (QueuedLocally, Requested),
      (QueuedLocally, Initializing),
      (Requested, QueuedRemotely),
      (Requested, Initializing),
      (QueuedRemotely, Initializing),
      (Initializing, InProgress),
    ];
    for from in TransferStates::ALL {
      for to in TransferStates::ALL {
        let open = OPEN.contains(&from);
        let allowed = forward.contains(&(from, to))
          || (open && from == to)
          || (open && to == QueuedLocally)
          || (open && to.is_completed());
        assert_eq!(allows(from, to), allowed, "{from} to {to}");
      }
    }
    for refused in [
      (Completed(Succeeded), Completed(Cancelled)),
      (Completed(Succeeded), Completed(Succeeded)),
      (Completed(Errored), QueuedLocally),
      (InProgress, Requested),
      (QueuedRemotely, Requested),
      (Initializing, QueuedRemotely),
    ] {
      assert!(
        !allows(refused.0, refused.1),
        "{} to {}",
        refused.0,
        refused.1
      );
    }
  }

  #[test]
  fn every_state_has_its_own_column_value_and_reads_back() {
    let values: Vec<u32> = TransferStates::ALL.into_iter().map(column).collect();
    assert_eq!(
      values,
      [1, 2050, 4098, 4, 8, 48, 80, 144, 272, 528, 1040],
      "the values rows were written with"
    );
    for state in TransferStates::ALL {
      assert_eq!(from_column(column(state)), Some(state));
      assert_eq!(State::of(state).kind(), state);
    }
    for value in [0, 2, 16, 2 | 2048 | 4096, 16 | 32 | 64] {
      assert_eq!(from_column(value), None, "{value}");
    }
  }

  /// The names of a schema's values in the committed API contract.
  pub(crate) fn contract_values(schema: &str) -> Vec<String> {
    let contract: serde_json::Value =
      serde_json::from_str(include_str!("../../../../api/openapi.json")).unwrap();
    contract["components"]["schemas"][schema]["enum"]
      .as_array()
      .unwrap_or_else(|| panic!("{schema} has values in the contract"))
      .iter()
      .map(|value| value.as_str().unwrap().to_string())
      .collect()
  }

  #[test]
  fn every_client_state_is_reported_by_the_name_and_column_the_daemon_keeps() {
    use soulseek_client::{TransferOutcome as Outcome, TransferState as Reported};
    let rows = [
      (Reported::QueuedLocally, "Queued, Locally", 2 | 2048),
      (Reported::Requested, "Requested", 1),
      (Reported::QueuedRemotely, "Queued, Remotely", 2 | 4096),
      (Reported::Initializing, "Initializing", 4),
      (Reported::InProgress, "InProgress", 8),
      (
        Reported::Completed(Outcome::Succeeded),
        "Completed, Succeeded",
        16 | 32,
      ),
      (
        Reported::Completed(Outcome::Canceled),
        "Completed, Cancelled",
        16 | 64,
      ),
      (
        Reported::Completed(Outcome::TimedOut),
        "Completed, TimedOut",
        16 | 128,
      ),
      (
        Reported::Completed(Outcome::Errored),
        "Completed, Errored",
        16 | 256,
      ),
      (
        Reported::Completed(Outcome::Rejected),
        "Completed, Rejected",
        16 | 512,
      ),
      (
        Reported::Completed(Outcome::Aborted),
        "Completed, Aborted",
        16 | 1024,
      ),
    ];
    let named = contract_values("TransferStates");
    for (state, name, stored) in rows {
      assert_eq!(reported(state).as_str(), name, "{state:?}");
      assert_eq!(column(reported(state)), stored, "{state:?}");
      assert!(
        named.iter().any(|n| n == name),
        "{name} is not in the contract"
      );
    }
  }
}
