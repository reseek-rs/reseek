//! One upload's task: the user is watched and the client's upload run with the queue's slot,
//! the group's speed limit and every state written as it comes, the progress at most every
//! [`PROGRESS_EVERY`]. How it ended is written, counted and, for a success, raised as
//! `UploadFileComplete`; whatever the end, the upload leaves the queue. The file is found again
//! when the slot comes: a file no longer shared to the user then fails the upload, and a peer
//! that asked for it is told it is not shared. A stop of the daemon leaves the record as it is,
//! for the stop to end.

use std::sync::Arc;
use std::time::Duration;

use api_types::events::{Event, UploadFileComplete};
use api_types::live::LiveFrame;
use api_types::relay::RelayUploadOutcome;
use api_types::transfers::{TransferDirection, TransferOutcome, TransferStates};
use chrono::Utc;
use reseek_shares::LogLimiter;
use reseek_shares::lock;
use soulseek_client::transfer::{Handle, Slot, Source, Transfer as Snapshot, Upload, UploadNotice};
use soulseek_client::{Error as ClientError, ErrorKind};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, info, info_span, trace, warn};
use uuid::Uuid;

use super::Uploads;
use super::intake::{Located, Locating, NOT_SHARED};
use crate::relay::Controller;
use crate::shares::LOCAL_HOST;
use crate::transfers::state::{self, State};
use crate::transfers::{PROGRESS_EVERY, Record, removal, store};

/// Why an upload whose file is gone when its slot comes ended.
const FILE_GONE: &str = "File could not be found";

/// Why an upload that was canceled ended.
pub(super) const CANCELED: &str = "the operation was cancelled";

/// Progress not written, which a failing database would repeat four times a second.
static PROGRESS_UNWRITTEN: LogLimiter = LogLimiter::new();

/// How an upload ended without succeeding.
#[derive(Debug)]
enum Failed {
  /// the client's upload ended with this error
  Client(ClientError),
  /// something here failed, as said, before the client had the upload
  Here(String),
  /// its file is gone from the shares
  Gone,
}

/// The I/O error from the input that `error` ended with, when it ended with one: a relay agent's
/// file that is not there, or did not come in time, fails its upload as such.
fn input_error(error: &ClientError) -> Option<&std::io::Error> {
  if error.kind() != ErrorKind::Io {
    return None;
  }
  std::error::Error::source(error).and_then(|source| source.downcast_ref::<std::io::Error>())
}

/// How an upload that ended as `failed` ended.
fn outcome(failed: &Failed) -> TransferOutcome {
  let error = match failed {
    Failed::Client(error) => error,
    Failed::Here(_) => return TransferOutcome::Errored,
    Failed::Gone => return TransferOutcome::Aborted,
  };
  match error.kind() {
    ErrorKind::Canceled => TransferOutcome::Cancelled,
    ErrorKind::Timeout => TransferOutcome::TimedOut,
    ErrorKind::TransferRejected => TransferOutcome::Rejected,
    ErrorKind::FileNotFound => TransferOutcome::Aborted,
    _ => match input_error(error).map(std::io::Error::kind) {
      Some(std::io::ErrorKind::NotFound) => TransferOutcome::Aborted,
      Some(std::io::ErrorKind::TimedOut) => TransferOutcome::TimedOut,
      _ => TransferOutcome::Errored,
    },
  }
}

/// Why an upload that ended as `failed` failed, as its record shows it: the peer's refusal as
/// it sent it, why a relay agent's file did not come, [`CANCELED`], else the error's text.
fn reason(failed: &Failed) -> String {
  let error = match failed {
    Failed::Client(error) => error,
    Failed::Here(reason) => return reason.clone(),
    Failed::Gone => return FILE_GONE.to_string(),
  };
  match error.kind() {
    ErrorKind::Canceled => CANCELED.to_string(),
    _ => match (error.reason(), input_error(error)) {
      (Some(reason), _) => reason.to_string(),
      (None, Some(input)) => input.to_string(),
      (None, None) => error.to_string(),
    },
  }
}

/// How the relay controller tells the agent an upload of its file ended.
fn relayed(result: Result<&Snapshot, &ClientError>, record: &Record) -> (RelayUploadOutcome, u64) {
  match result {
    Ok(transfer) => (RelayUploadOutcome::Succeeded, transfer.bytes_transferred),
    Err(error) if error.is_canceled() => (RelayUploadOutcome::Cancelled, record.bytes_transferred),
    Err(_) => (RelayUploadOutcome::Failed, record.bytes_transferred),
  }
}

/// The slot of an upload in the queue, given back when the upload lets go of it.
struct Held {
  uploads: Arc<Uploads>,
  id: Uuid,
}

impl Drop for Held {
  fn drop(&mut self) {
    self.uploads.queue.complete(self.id);
  }
}

impl Uploads {
  /// Starts the task of the upload `record` of the file `located`, which stops when `cancel`
  /// is canceled.
  pub(super) fn spawn(&self, record: Record, located: Located, cancel: CancellationToken) {
    let this = self.this();
    let span = info_span!(
      "transfer",
      id = %record.id,
      direction = "upload",
      username = record.username
    );
    self.tasks.spawn(
      async move {
        let id = record.id;
        this.run(record, located, &cancel).await;
        this.forget(id).await;
      }
      .instrument(span),
    );
  }

  /// The upload from its start to how it ended, written.
  async fn run(&self, mut record: Record, located: Located, cancel: &CancellationToken) {
    let client = match self.client() {
      Ok(client) => client,
      Err(e) => return self.fail(&mut record, &Failed::Here(e), None).await,
    };
    self.users.watch(&client, &record.username).await;
    info!(filename = record.filename, "Initializing upload");
    let username = record.username.clone();
    let users = self.users.clone();
    let (governor, reporter) = self
      .governor
      .pace(Arc::new(move || users.speed_group(&username)))
      .callbacks();
    let upload = client
      .upload(record.username.clone(), record.filename.clone())
      .governor(governor)
      .reporter(reporter)
      .slot(self.slot(record.id))
      .ended(self.ended(record.id));
    let (upload, relay) = self.input(upload, &record, &located);
    let result = match upload.start().await {
      Ok(handle) => self.follow(&mut record, handle, cancel).await,
      Err(error) => Err((error, None)),
    };
    if let Some(relay) = relay {
      let (outcome, bytes) = relayed(result.as_ref().map_err(|(error, _)| error), &record);
      relay.finished(record.id, outcome, bytes);
    }
    if self.shutdown.is_cancelled() {
      return debug!("upload left for the stop to end");
    }
    match result {
      Ok(transfer) => self.succeeded(&mut record, &transfer, &located).await,
      Err((error, ended)) => {
        let gone = lock(&self.in_flight)
          .get(&record.id)
          .is_some_and(|entry| entry.gone);
        let failed = if gone {
          Failed::Gone
        } else {
          Failed::Client(error)
        };
        let ended = ended.map(|snapshot| {
          let snapshot = *snapshot;
          let outcome = match state::reported(snapshot.state) {
            TransferStates::Completed(outcome) if !gone => outcome,
            _ => outcome(&failed),
          };
          (snapshot, outcome)
        });
        self.fail(&mut record, &failed, ended.as_ref()).await;
      }
    }
  }

  /// Follows the upload `handle` of `record` to its end, writing each state the client reports
  /// and the progress at most every [`PROGRESS_EVERY`], and canceling it when `cancel` is. Its
  /// error comes with the client's last snapshot, when the upload had one that ended it.
  async fn follow(
    &self,
    record: &mut Record,
    handle: Handle,
    cancel: &CancellationToken,
  ) -> Result<Snapshot, (ClientError, Option<Box<Snapshot>>)> {
    // every state the upload enters, in order, with its progress between them
    let mut changes = handle.changes();
    // the state of the last snapshot written, so one that only moved bytes is progress
    let mut written = None;
    let throttle = tokio::time::sleep(Duration::ZERO);
    tokio::pin!(throttle);
    let mut throttled = false;
    let mut canceled = false;
    let mut ended: Option<Box<Snapshot>> = None;
    loop {
      tokio::select! {
        biased;
        () = cancel.cancelled(), if !canceled => {
          canceled = true;
          handle.cancel();
        }
        snapshot = changes.next() => {
          let Some(snapshot) = snapshot else { break };
          if snapshot.state.is_terminal() {
            self.shown.ended(&snapshot);
            self.mark_ended(record.id);
            ended = Some(Box::new(snapshot));
          } else if written != Some(snapshot.state) {
            written = Some(snapshot.state);
            self.state_changed(record, snapshot).await;
          } else if !throttled {
            let speed = self.shown.progressed(&snapshot);
            self.progressed(record, snapshot.bytes_transferred, speed).await;
            throttled = true;
            throttle.as_mut().reset(Instant::now() + PROGRESS_EVERY);
          }
        }
        () = &mut throttle, if throttled => throttled = false,
      }
    }
    match handle.await {
      Ok(transfer) => Ok(transfer),
      Err(error) => Err((error, ended)),
    }
  }

  /// Marks the upload `id` as one the client let go of, so a request for its file is not taken
  /// for this one.
  fn mark_ended(&self, id: Uuid) {
    if let Some(entry) = lock(&self.in_flight).get_mut(&id) {
      entry.ended = true;
    }
  }

  /// `upload` of `record` reading the local file, or the bytes the relay agent holding it
  /// sends from the offset the peer asks for, with the controller then.
  fn input(
    &self,
    upload: Upload,
    record: &Record,
    located: &Located,
  ) -> (Upload<Source>, Option<Arc<Controller>>) {
    if located.resolved.host == LOCAL_HOST {
      return (upload.from_file(located.resolved.path.clone()), None);
    }
    let relay = self
      .relay
      .clone()
      .expect("only a relay controller has agents' shares");
    let (agent, filename, transfer) = (
      located.resolved.host.clone(),
      record.filename.clone(),
      record.id,
    );
    let opening = relay.clone();
    // the agent sends its file from the offset the peer asked for
    let upload = upload.from_source(located.size, move |start| async move {
      opening
        .open_stream(&agent, &filename, start, transfer)
        .await
    });
    (upload, Some(relay))
  }

  /// What the client waits on for the upload `id`'s slot: the queue's start, then the file
  /// found again, which fails the upload when it is no longer shared to the user. The slot goes
  /// back to the queue once the upload lets go of it.
  fn slot(
    &self,
    id: Uuid,
  ) -> impl Fn(&Snapshot) -> std::pin::Pin<Box<dyn std::future::Future<Output = Slot> + Send>>
  + Send
  + Sync
  + 'static {
    let this = self.this();
    move |transfer| {
      let start = this.queue.await_start(id);
      let uploads = this.clone();
      let (username, filename) = (transfer.username.clone(), transfer.filename.as_str().to_owned());
      Box::pin(async move {
        let start = match start {
          Ok(start) => start,
          Err(e) => return Slot::Fail(e.to_string()),
        };
        if start.await.is_err() {
          return Slot::Fail(format!("upload {id} left the queue before its slot"));
        }
        let held = Held { uploads, id };
        if held.uploads.gone(id, &username, &filename).await {
          // the slot goes back to the queue as `held` drops
          return Slot::Fail(FILE_GONE.to_string());
        }
        Slot::granted(held)
      })
    }
  }

  /// Whether `filename` of the upload `id` is no longer shared to `username`, which marks the
  /// upload as gone. The request was confirmed when it was queued, and the file goes to the user
  /// the server knows by the name.
  async fn gone(&self, id: Uuid, username: &str, filename: &str) -> bool {
    let sees = self.users.sees(username);
    match self.locate(&sees, filename).await {
      Ok(Locating::Found(_)) => false,
      // the agent is asked for the file again as the peer asks for its bytes, and the peer is
      // told the upload failed when it cannot send it then
      Ok(Locating::Later) => {
        debug!("the agent holding the file cannot answer now; the upload goes on");
        false
      }
      Err(e) => {
        warn!(error = %e, "share index not read; the file found when the request was accepted is sent");
        false
      }
      Ok(Locating::NotShared) => {
        if let Some(entry) = lock(&self.in_flight).get_mut(&id) {
          entry.gone = true;
        }
        true
      }
    }
  }

  /// What the client asks before it tells the peer an upload `id` that did not succeed ended:
  /// the upload is marked ended first, since the peer may ask again at once. A peer that asked
  /// for a file gone is told it is not shared, the answer its request would get now, so it stops
  /// waiting for it; one sent the file unasked is told nothing. Otherwise what the peer is told
  /// is left to the client.
  fn ended(
    &self,
    id: Uuid,
  ) -> impl Fn(&Snapshot, &ClientError) -> Option<UploadNotice> + Send + Sync + 'static {
    let this = self.this();
    move |snapshot, _| {
      this.shown.ended(snapshot);
      this.mark_ended(id);
      let in_flight = lock(&this.in_flight);
      let entry = in_flight.get(&id).filter(|entry| entry.gone)?;
      Some(if entry.asked {
        UploadNotice::Denied(NOT_SHARED.to_string())
      } else {
        UploadNotice::Nothing
      })
    }
  }

  /// Takes a state the client reported into `record` and writes it; a final state is left for
  /// the task to write.
  async fn state_changed(&self, record: &mut Record, snapshot: Snapshot) {
    let state = state::reported(snapshot.state);
    if let TransferStates::Completed(_) = state {
      return;
    }
    // records are left as they are once the daemon stops
    if self.shutdown.is_cancelled() {
      return;
    }
    let previous = record.state.kind();
    if let Err(refused) = record.enter(State::of(state)) {
      return warn!(%refused, "upload state not recorded");
    }
    record.take_figures(&snapshot);
    if state == TransferStates::Initializing && previous != state {
      // the server never pushes what a user shares, and their group depends on it
      self.users.refetch_statistics(&record.username);
    }
    if state == TransferStates::InProgress && previous != state {
      info!(
        filename = record.filename,
        start = snapshot.start_offset,
        size = record.size,
        "upload started"
      );
    }
    self.save(record, previous != state).await;
  }

  /// Writes and pushes the bytes sent and the speed of an upload in progress.
  async fn progressed(&self, record: &mut Record, bytes: u64, speed: f64) {
    if record.state != State::InProgress || self.shutdown.is_cancelled() {
      return;
    }
    record.bytes_transferred = bytes;
    record.average_speed = speed;
    let id = record.id;
    match self
      .db
      .write(move |conn| store::progress(conn, id, bytes, speed))
      .await
    {
      Ok(true) => {
        trace!(bytes, speed, "upload progress");
        self
          .live
          .publish(LiveFrame::TransfersUpdated(Box::new(record.transfer())));
      }
      // the record left `InProgress` in the database meanwhile
      Ok(false) => {}
      Err(e) => {
        if let Some(suppressed) = PROGRESS_UNWRITTEN.allow() {
          warn!(error = %e, suppressed, "upload progress not written");
        }
      }
    }
  }

  /// Writes an upload that succeeded, counts it and raises `UploadFileComplete`.
  async fn succeeded(&self, record: &mut Record, transfer: &Snapshot, located: &Located) {
    record.take_figures(transfer);
    if let Err(refused) = record.enter(State::Completed(TransferOutcome::Succeeded)) {
      return debug!(%refused, "upload ended already");
    }
    self.save(record, true).await;
    self
      .telemetry
      .metrics()
      .transfers()
      .uploads
      .succeeded(record.bytes_transferred, record.average_speed);
    info!(
      filename = record.filename,
      bytes = record.bytes_transferred,
      speed = record.average_speed,
      "upload completed"
    );
    let ended_at = record.ended_at.unwrap_or_else(Utc::now);
    self.events.raise_at(
      Event::UploadFileComplete(UploadFileComplete {
        version: 0,
        local_filename: located.resolved.path.display().to_string(),
        remote_filename: record.filename.clone(),
        transfer: record.transfer(),
      }),
      ended_at,
    );
  }

  /// Writes an upload that ended with `error` as completed with its outcome, unless its record
  /// ended already, and counts it as failed. The client's last snapshot, `ended`, gives the
  /// outcome and the times when there is one; without, the upload never started.
  async fn fail(
    &self,
    record: &mut Record,
    failed: &Failed,
    ended: Option<&(Snapshot, TransferOutcome)>,
  ) {
    let canceled_for = lock(&self.in_flight)
      .get(&record.id)
      .and_then(|entry| entry.reason);
    let outcome = ended.map_or_else(|| outcome(failed), |(_, outcome)| *outcome);
    let exception = match canceled_for {
      Some(why) if outcome == TransferOutcome::Cancelled => why.to_string(),
      _ => reason(failed),
    };
    if let Err(refused) = record.complete(outcome, Some(exception)) {
      return debug!(%refused, "upload ended already");
    }
    if let Some((snapshot, _)) = ended {
      record.take_figures(snapshot);
    }
    self.save(record, true).await;
    self.telemetry.metrics().transfers().uploads.failed();
    info!(
      filename = record.filename,
      state = %record.state,
      reason = record.exception,
      "upload failed"
    );
  }

  /// Lets go of the upload `id` once its task has written how it ended: it leaves the queue if
  /// it has not, its user's next upload takes the turn, and it is removed from the listings when
  /// that was asked for meanwhile.
  async fn forget(&self, id: Uuid) {
    if self.queue.returned(id) {
      debug!("upload taken out of the queue at its end");
    }
    let entry = lock(&self.in_flight).remove(&id);
    if entry.is_some_and(|entry| entry.remove) {
      match removal::remove(&self.db, TransferDirection::Upload, id).await {
        Ok(Some(taken)) => removal::push(&self.live, TransferDirection::Upload, &[taken]),
        Ok(None) => debug!(%id, "upload to remove not removed; it has not completed"),
        Err(e) => warn!(%id, error = %e, "ended upload not removed"),
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn a_file_gone_aborts_its_upload_and_a_failure_here_errs() {
    assert_eq!(outcome(&Failed::Gone), TransferOutcome::Aborted);
    assert_eq!(reason(&Failed::Gone), "File could not be found");
    let here = Failed::Here("the client has been shut down".into());
    assert_eq!(outcome(&here), TransferOutcome::Errored);
    assert_eq!(reason(&here), "the client has been shut down");
  }

  #[test]
  fn an_input_whose_file_is_gone_or_late_aborts_or_times_out_its_upload() {
    use std::io::{Error, ErrorKind as IoKind};
    let read =
      |error: Error| Failed::Client(ClientError::new(ErrorKind::Io, "read").with_source(error));
    let gone = read(Error::new(IoKind::NotFound, "no such file"));
    assert_eq!(outcome(&gone), TransferOutcome::Aborted);
    assert_eq!(reason(&gone), "no such file");
    let late = read(Error::new(IoKind::TimedOut, "too slow"));
    assert_eq!(outcome(&late), TransferOutcome::TimedOut);
    let other = read(Error::other("disk"));
    assert_eq!(outcome(&other), TransferOutcome::Errored);
  }

  #[test]
  fn a_refused_upload_shows_the_peers_reason() {
    let refused = ClientError::new(ErrorKind::TransferRejected, "transfer rejected: Queue full")
      .with_reason("Queue full");
    let failed = Failed::Client(refused);
    assert_eq!(outcome(&failed), TransferOutcome::Rejected);
    assert_eq!(reason(&failed), "Queue full");
  }

  #[test]
  fn a_canceled_upload_says_so_as_it_always_has() {
    let canceled = ClientError::new(ErrorKind::Canceled, "the operation was canceled");
    assert_eq!(
      reason(&Failed::Client(canceled)),
      "the operation was cancelled"
    );
  }
}
