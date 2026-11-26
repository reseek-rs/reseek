//! The retention sweep: completed transfers past the age their outcome is kept for are removed
//! from the listings, uploads first, then downloads, and, when set, transfers removed long
//! enough ago are deleted for good with their state history, but for the canceled downloads and
//! the uploads that users' requests are still checked against.

use std::time::Duration;

use api_types::transfers::TransferDirection;
use chrono::Utc;
use rusqlite::params;
use tracing::debug;

use super::categories::{CANCELLED, Condition, ERRORED, FAILED, SUCCESSFUL};
use super::removal::{self, Removed};
use super::{CANCEL_REMEMBERED, UPLOADS_COUNTED, direction_text};
use crate::config::{TransferRetention, TransfersRetention};
use crate::db::{Database, DbError, shifted, timestamp};
use crate::live::Live;

/// The time `age` before now, as the database stores times.
fn before_now(age: Duration) -> String {
  let age = chrono::Duration::from_std(age).unwrap_or(chrono::Duration::MAX);
  timestamp(shifted(Utc::now(), -age))
}

/// The outcomes of one direction with how long each is kept, in the order they are swept.
fn outcomes(
  retention: &TransferRetention,
) -> [(&'static str, Option<Duration>, &'static Condition); 4] {
  [
    ("succeeded", retention.succeeded, &SUCCESSFUL),
    ("canceled", retention.canceled, &CANCELLED),
    ("errored", retention.errored, &ERRORED),
    ("failed", retention.failed, &FAILED),
  ]
}

/// What a sweep did.
pub struct Pruned {
  /// how many transfers were removed or deleted
  pub count: usize,
  /// the downloads removed, which the download engine is told of, since their jobs follow them
  pub downloads: Vec<Removed>,
  /// the first failure, which did not stop the rest of the sweep
  pub error: Option<DbError>,
}

/// Removes what `retention` no longer keeps and deletes what was removed long enough ago. The
/// uploads removed are pushed here; the downloads removed are returned for the download engine
/// to tell of.
pub async fn prune(db: &Database, live: &Live, retention: &TransfersRetention) -> Pruned {
  let mut first_error = None;
  let mut count = 0;
  let mut downloads = Vec::new();
  for (direction, kept) in [
    (TransferDirection::Upload, &retention.upload),
    (TransferDirection::Download, &retention.download),
  ] {
    for (outcome, age, condition) in outcomes(kept) {
      let Some(age) = age else { continue };
      match expire(db, direction, condition, age).await {
        Ok(removed) => {
          if !removed.is_empty() {
            debug!(
              ?direction,
              outcome,
              removed = removed.len(),
              "transfers past retention removed"
            );
          }
          count += removed.len();
          match direction {
            TransferDirection::Upload => removal::push(live, direction, &removed),
            TransferDirection::Download => downloads.extend(removed),
          }
        }
        Err(e) => {
          debug!(?direction, outcome, error = %e, "transfers past retention not removed");
          first_error.get_or_insert(e);
        }
      }
    }
  }
  if let Some(after) = retention.purge_removed_after {
    match purge(db, after).await {
      Ok(deleted) => {
        if deleted > 0 {
          debug!(deleted, "removed transfers deleted for good");
        }
        count += deleted;
      }
      Err(e) => {
        first_error.get_or_insert(e);
      }
    }
  }
  Pruned {
    count,
    downloads,
    error: first_error,
  }
}

/// Removes the transfers of `direction` in `condition` that ended more than `age` ago.
async fn expire(
  db: &Database,
  direction: TransferDirection,
  condition: &'static Condition,
  age: Duration,
) -> Result<Vec<Removed>, DbError> {
  let cutoff = before_now(age);
  db.write(move |conn| {
    removal::remove_where(
      conn,
      direction,
      &format!("EndedAt IS NOT NULL AND EndedAt < ?1 AND {condition}"),
      &cutoff,
    )
  })
  .await
}

/// Deletes the transfers removed more than `after` ago with their state history, but for a
/// download canceled within [`CANCEL_REMEMBERED`], which its user's offer of the file unasked
/// is checked against, and an upload started within [`UPLOADS_COUNTED`], which the upload
/// limits count; returns how many were deleted.
async fn purge(db: &Database, after: Duration) -> Result<usize, DbError> {
  let cutoff = before_now(after);
  let canceled_since = before_now(CANCEL_REMEMBERED);
  let counted_since = before_now(UPLOADS_COUNTED);
  db.write(move |conn| {
    let tx = conn.transaction()?;
    let removed_long_ago = format!(
      "Removed = 1 AND RemovedAt IS NOT NULL AND RemovedAt < ?1 \
       AND NOT (Direction = ?3 AND {CANCELLED} AND EndedAt > ?2) \
       AND NOT (Direction = ?4 AND StartedAt IS NOT NULL AND StartedAt >= ?5)"
    );
    let download = direction_text(TransferDirection::Download);
    let upload = direction_text(TransferDirection::Upload);
    let bound = params![cutoff, canceled_since, download, upload, counted_since];
    tx.execute(
      &format!(
        "DELETE FROM transfers_state_history WHERE transfer_id IN \
         (SELECT Id FROM Transfers WHERE {removed_long_ago})"
      ),
      bound,
    )?;
    let deleted = tx.execute(
      &format!("DELETE FROM Transfers WHERE {removed_long_ago}"),
      bound,
    )?;
    tx.commit()?;
    Ok(deleted)
  })
  .await
}

#[cfg(test)]
mod tests {
  use api_types::transfers::{TransferOutcome, TransferStates};

  use super::*;
  use crate::transfers::fixtures::{Row, insert, transfers};
  use crate::transfers::store;
  use tokio_util::sync::CancellationToken;

  const HOUR: Duration = Duration::from_secs(3600);

  fn ended(user: &str, state: TransferStates, hours_ago: i64) -> Row {
    let mut row = Row::new(user, TransferDirection::Download, state);
    row.ended_at = Some(Utc::now() - chrono::Duration::hours(hours_ago));
    row
  }

  async fn removed(db: &Database) -> Vec<String> {
    let mut names: Vec<String> = db
      .read(|conn| {
        conn
          .prepare("SELECT Username FROM Transfers WHERE Removed = 1")?
          .query_map([], |row| row.get(0))?
          .collect()
      })
      .await
      .unwrap();
    names.sort();
    names
  }

  #[tokio::test]
  async fn each_outcome_is_removed_past_its_own_age_and_running_ones_never() {
    let db = transfers().await;
    let live = Live::new(CancellationToken::new());
    insert(
      &db,
      vec![
        ended(
          "old-success",
          TransferStates::Completed(TransferOutcome::Succeeded),
          3,
        ),
        ended(
          "new-success",
          TransferStates::Completed(TransferOutcome::Succeeded),
          1,
        ),
        ended(
          "old-rejected",
          TransferStates::Completed(TransferOutcome::Rejected),
          3,
        ),
        ended(
          "old-canceled",
          TransferStates::Completed(TransferOutcome::Cancelled),
          3,
        ),
        Row::new(
          "running",
          TransferDirection::Download,
          TransferStates::InProgress,
        ),
      ],
    )
    .await;
    let retention = TransfersRetention {
      download: TransferRetention {
        succeeded: Some(2 * HOUR),
        errored: Some(2 * HOUR),
        ..TransferRetention::default()
      },
      ..TransfersRetention::default()
    };
    assert!(prune(&db, &live, &retention).await.error.is_none());
    assert_eq!(removed(&db).await, ["old-rejected", "old-success"]);
  }

  #[tokio::test]
  async fn removed_transfers_are_deleted_once_removed_long_enough_with_their_history() {
    let db = transfers().await;
    let live = Live::new(CancellationToken::new());
    let mut long_ago = ended(
      "long-ago",
      TransferStates::Completed(TransferOutcome::Succeeded),
      3,
    );
    long_ago.removed = true;
    let mut lately = ended(
      "lately",
      TransferStates::Completed(TransferOutcome::Succeeded),
      3,
    );
    lately.removed = true;
    let kept = ended(
      "kept",
      TransferStates::Completed(TransferOutcome::Succeeded),
      3,
    );
    let (gone, stays) = (long_ago.id, lately.id);
    insert(&db, vec![long_ago, lately, kept]).await;
    db.write(move |conn| {
      let three_hours_ago = timestamp(Utc::now() - chrono::Duration::hours(3));
      conn.execute(
        "UPDATE Transfers SET RemovedAt = ?2 WHERE Id = ?1",
        rusqlite::params![gone.to_string(), three_hours_ago],
      )?;
      conn.execute(
        "UPDATE Transfers SET RemovedAt = ?2 WHERE Id = ?1",
        rusqlite::params![stays.to_string(), timestamp(Utc::now())],
      )?;
      store::entered(conn, gone, TransferStates::InProgress, Utc::now())
    })
    .await
    .unwrap();
    let retention = TransfersRetention {
      purge_removed_after: Some(HOUR),
      ..TransfersRetention::default()
    };
    assert!(prune(&db, &live, &retention).await.error.is_none());
    let left: Vec<String> = db
      .read(|conn| {
        conn
          .prepare("SELECT Username FROM Transfers ORDER BY Username")?
          .query_map([], |row| row.get(0))?
          .collect()
      })
      .await
      .unwrap();
    assert_eq!(left, ["kept", "lately"]);
    let history = db
      .read(move |conn| store::history(conn, gone))
      .await
      .unwrap();
    assert!(history.is_empty());
  }

  #[tokio::test]
  async fn a_canceled_download_removed_long_enough_is_kept_until_a_day_after_its_cancel() {
    let db = transfers().await;
    let live = Live::new(CancellationToken::new());
    let canceled = TransferStates::Completed(TransferOutcome::Cancelled);
    let mut lately = ended("lately", canceled, 3);
    lately.removed = true;
    let mut long_ago = ended("long-ago", canceled, 25);
    long_ago.removed = true;
    let mut upload = ended("upload", canceled, 3);
    upload.direction = TransferDirection::Upload;
    upload.removed = true;
    let ids = [lately.id, long_ago.id, upload.id];
    insert(&db, vec![lately, long_ago, upload]).await;
    db.write(move |conn| {
      let three_hours_ago = timestamp(Utc::now() - chrono::Duration::hours(3));
      for id in ids {
        conn.execute(
          "UPDATE Transfers SET RemovedAt = ?2 WHERE Id = ?1",
          rusqlite::params![id.to_string(), three_hours_ago],
        )?;
      }
      Ok(())
    })
    .await
    .unwrap();
    let retention = TransfersRetention {
      purge_removed_after: Some(HOUR),
      ..TransfersRetention::default()
    };
    assert!(prune(&db, &live, &retention).await.error.is_none());
    assert_eq!(removed(&db).await, ["lately"]);
  }

  #[tokio::test]
  async fn a_removed_upload_is_kept_while_the_upload_limits_count_it() {
    let db = transfers().await;
    let live = Live::new(CancellationToken::new());
    let succeeded = TransferStates::Completed(TransferOutcome::Succeeded);
    let upload = |user: &str, started_hours_ago: i64| {
      let mut row = ended(user, succeeded, started_hours_ago);
      row.direction = TransferDirection::Upload;
      row.started_at = row.ended_at;
      row.removed = true;
      row
    };
    let lately = upload("lately", 3);
    let long_ago = upload("long-ago", 8 * 24);
    let ids = [lately.id, long_ago.id];
    insert(&db, vec![lately, long_ago]).await;
    db.write(move |conn| {
      let three_hours_ago = timestamp(Utc::now() - chrono::Duration::hours(3));
      for id in ids {
        conn.execute(
          "UPDATE Transfers SET RemovedAt = ?2 WHERE Id = ?1",
          rusqlite::params![id.to_string(), three_hours_ago],
        )?;
      }
      Ok(())
    })
    .await
    .unwrap();
    let retention = TransfersRetention {
      purge_removed_after: Some(HOUR),
      ..TransfersRetention::default()
    };
    assert!(prune(&db, &live, &retention).await.error.is_none());
    assert_eq!(removed(&db).await, ["lately"]);
  }
}
