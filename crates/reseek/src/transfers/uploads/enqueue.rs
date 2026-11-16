//! Recording an accepted upload request: unless an upload of the same file to the same user is
//! queued or running already, a record is written, the user's earlier records of the file are
//! taken out of the listings, the upload enters the queue, and its task starts. An upload whose
//! client side has ended does not stand for a new request, though its task may not have written
//! how it ended yet: a peer told the upload failed asks again at once.

use std::collections::{HashMap, HashSet};

use api_types::live::LiveFrame;
use api_types::transfers::TransferDirection;
use reseek_shares::lock;
use rusqlite::{Connection, params};
use tokio_util::sync::CancellationToken;
use tracing::info;
use uuid::Uuid;

use super::intake::Located;
use super::{InFlight, Uploads};
use crate::transfers::categories::UNFINISHED;
use crate::transfers::removal::{self, Removed};
use crate::transfers::{Record, direction_text, store};
use crate::users::Touch;

/// Why a record of an upload the client already runs is not written.
const DUPLICATE: &str =
  "A duplicate upload of the same file to the same user is already registered";

/// What the transaction found or wrote.
enum Written {
  /// an upload of the file to the user is queued or running already: these
  Running(Vec<String>),
  /// the client runs an upload of the file to the user that no record knows of
  Duplicate,
  /// the record, and the user's earlier records of the file taken out of the listings
  Inserted(Vec<Removed>),
}

/// What an enqueue did with the file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Enqueued {
  /// a record was written and its upload queued
  Queued,
  /// an upload of the file to the user was queued or running already, and stands for it
  Already,
}

/// The ids of the unfinished uploads of `filename` to `username` among `live`, the uploads
/// whose client side has not ended.
fn running(
  conn: &Connection,
  username: &str,
  filename: &str,
  live: &HashSet<String>,
) -> rusqlite::Result<Vec<String>> {
  Ok(
    conn
      .prepare_cached(&format!(
        "SELECT Id FROM Transfers WHERE Username = ?1 AND Filename = ?2 AND Direction = ?3 \
         AND {UNFINISHED}"
      ))?
      .query_map(
        params![
          username,
          filename,
          direction_text(TransferDirection::Upload)
        ],
        |row| row.get(0),
      )?
      .collect::<rusqlite::Result<Vec<String>>>()?
      .into_iter()
      .filter(|id| live.contains(id))
      .collect(),
  )
}

/// Writes `record` and takes the user's earlier records of the file out of the listings, in
/// one transaction, unless an unfinished upload of the file to the user is among `live`, the
/// uploads whose client side has not ended, or `tracked` says the client runs one.
fn write(
  conn: &mut Connection,
  record: &Record,
  tracked: bool,
  live: &HashSet<String>,
) -> rusqlite::Result<Written> {
  let tx = conn.transaction()?;
  let running = running(&tx, &record.username, &record.filename, live)?;
  if !running.is_empty() {
    return Ok(Written::Running(running));
  }
  if tracked {
    return Ok(Written::Duplicate);
  }
  store::insert(&tx, record)?;
  let superseded = removal::supersede(&tx, record)?;
  tx.commit()?;
  Ok(Written::Inserted(superseded))
}

impl Uploads {
  /// The ids of the uploads whose client side has not ended, from `in_flight`, locked.
  fn live(in_flight: &HashMap<Uuid, InFlight>) -> HashSet<String> {
    in_flight
      .iter()
      .filter(|(_, entry)| !entry.ended)
      .map(|(id, _)| id.to_string())
      .collect()
  }

  /// Whether an upload of `filename` to `username` is queued or running already, which stands
  /// for the file asked for or sent again; read under the user's turn, as an enqueue is. When
  /// the user `asked` for it, the uploads found are theirs to be told about.
  pub(super) async fn sending_already(
    &self,
    username: &str,
    filename: &str,
    asked: bool,
  ) -> Result<bool, String> {
    let live = Self::live(&lock(&self.in_flight));
    let (name, file) = (username.to_string(), filename.to_string());
    let running = self
      .db
      .read(move |conn| running(conn, &name, &file, &live))
      .await
      .map_err(|e| format!("the uploads were not read: {e}"))?;
    if asked {
      self.asked_for(&running);
    }
    Ok(!running.is_empty())
  }

  /// Marks the uploads `ids`, in flight, as asked for by their user.
  fn asked_for(&self, ids: &[String]) {
    let mut in_flight = lock(&self.in_flight);
    for id in ids {
      // the ids are the in-flight entries' own, so they parse; an entry gone since has ended
      if let Some(entry) = id.parse().ok().and_then(|id| in_flight.get_mut(&id)) {
        entry.asked = true;
      }
    }
  }

  /// Records the upload of `filename` to `username` from `located`, queues it and starts its
  /// task; an upload of the file to the user queued or running already stands for it. Runs
  /// under the user's turn, on the task of the request or send that holds it; `asked` when the
  /// user asked for the file, rather than our sending it.
  pub(super) async fn enqueue(
    &self,
    username: &str,
    filename: &str,
    located: Located,
    asked: bool,
  ) -> Result<Enqueued, String> {
    let client = self.client()?;
    let record = Record::upload(username, filename, located.size);
    let id = record.id;
    // the handle exists before the record can be seen, so a cancel always finds it. It is not
    // the daemon's stop, which would tell the peer the upload was refused: a stop cuts the
    // upload when the client stops, and the peer asks again
    let cancel = CancellationToken::new();
    let live: HashSet<String> = {
      let mut in_flight = lock(&self.in_flight);
      let live = Self::live(&in_flight);
      in_flight.insert(
        id,
        InFlight {
          cancel: cancel.clone(),
          remove: false,
          reason: None,
          ended: false,
          asked,
        },
      );
      live
    };
    // read after the snapshot: an upload is marked ended just before the client lets go of it,
    // so one the snapshot saw end is no longer the client's either
    let tracked = client
      .transfers()
      .uploads()
      .iter()
      .any(|t| t.username == username && t.filename.as_str() == filename);
    let written = record.clone();
    let result = self
      .db
      .write(move |conn| write(conn, &written, tracked, &live))
      .await;
    let superseded = match result {
      Ok(Written::Inserted(superseded)) => superseded,
      Ok(Written::Running(ids)) => {
        lock(&self.in_flight).remove(&id);
        info!(
          username,
          filename,
          ?ids,
          "upload already queued or in progress"
        );
        return Ok(Enqueued::Already);
      }
      Ok(Written::Duplicate) => {
        lock(&self.in_flight).remove(&id);
        return Err(DUPLICATE.to_string());
      }
      Err(e) => {
        lock(&self.in_flight).remove(&id);
        return Err(format!("the upload record was not written: {e}"));
      }
    };
    removal::push(&self.live, TransferDirection::Upload, &superseded);
    self.users.touch(username, Touch::Transfer).await;
    self
      .live
      .publish(LiveFrame::TransfersAdded(Box::new(record.transfer())));
    self.queue.enqueue(id, username, filename);
    self.spawn(record, located, cancel);
    Ok(Enqueued::Queued)
  }
}
