//! One download's task: it waits its turn in the local queue, checks its record, then tries
//! the download, waiting longer before each new try, until the file arrives, the download fails
//! for good or it is canceled, and writes how it ended. Each try that asks its peer waits for
//! a free download slot in the local queue first. A try that finds its user offline, or that
//! their going offline stops, does not count: the download waits for them to be back and tries
//! again. A file its user offered unasked is tried at once, taking the offer, since the sender
//! waits for our answer; a later try asks for it like any other. A stop of the daemon leaves the
//! record as it is, for the stop to end.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use api_types::events::{DownloadFailureStage, DownloadFileFailed, Event};
use api_types::transfers::TransferOutcome;
use chrono::Utc;
use reseek_shares::{backoff, lock};
use tokio::sync::watch;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, info, info_span, warn};
use uuid::Uuid;

use super::attempt::{Attempted, DUPLICATE, Failure};
use super::away::Heard;
use super::destination::incomplete_path;
use super::starts::{Ask, Grant, Pending, Turn};
use super::{Downloads, InFlight};
use crate::files::WriteGuard;
use crate::safety::Os;
use crate::transfers::record::USER_OFFLINE;
use crate::transfers::state::State;
use crate::transfers::store::Saved;
use crate::transfers::{Record, removal, store};

/// Why a download whose user the server says does not exist ended.
const USER_MISSING: &str = "The user doesn't exist";

/// How a wait for a user ended.
enum Waited {
  /// they are no longer known to be offline
  Back,
  /// the server says they do not exist
  Missing,
  /// the download was canceled meanwhile
  Canceled,
}

/// How long a download may hold its turn among the files of its request before its peer has
/// it queued.
pub(super) const HARD_LIMIT: Duration = Duration::from_secs(180);

/// What a try that asks its peer holds: the free slot it asks in, until the peer offers the
/// file or is known to have it queued, and for the first try its turn among the files of its
/// request, until the peer has it queued, for [`HARD_LIMIT`] at most.
pub(super) struct Gate {
  pub pending: Option<Pending>,
  pub turn: Option<Turn>,
  /// when the download is stopped if it still holds its request's turn
  pub deadline: Option<Instant>,
}

impl Gate {
  /// What `grant` gives a try to hold while it asks.
  fn of(grant: Grant) -> (Self, Ask) {
    let deadline = grant.turn.is_some().then(|| Instant::now() + HARD_LIMIT);
    let gate = Self {
      pending: Some(grant.pending),
      turn: grant.turn,
      deadline,
    };
    (gate, grant.ask)
  }
}

/// How a download task ended.
enum Ended {
  /// the file arrived and is in the incomplete file, still marked as written; the record is
  /// filled in, to be written once the file is moved
  Succeeded(Record, PathBuf, WriteGuard),
  /// the download failed for good
  Failed(Failed),
  /// the daemon is stopping; the record is left as it is
  Abandoned,
}

/// A download that failed for good: its record when there is one, why, and its incomplete file.
struct Failed {
  record: Option<Record>,
  failure: Failure,
  incomplete: Option<PathBuf>,
}

impl Ended {
  fn failed(record: Option<Record>, failure: Failure, incomplete: Option<PathBuf>) -> Self {
    Self::Failed(Failed {
      record,
      failure,
      incomplete,
    })
  }
}

/// The wait before try `attempt`, the second or a later one: `delay`, doubled for every try
/// between the first and this one, plus the shared jitter, at most `max` in all.
fn retry_wait(attempt: u32, delay: Duration, max: Duration) -> Duration {
  backoff::wait(attempt - 1, backoff::doubling(delay), max).min(max)
}

impl Downloads {
  /// Starts the task of the download `record`, which waits at `position` in the local queue,
  /// takes a turn of `request`, whose files hold that many at once, and stops when `cancel` is
  /// canceled. With `offered`, its first try takes the offer its user made unasked.
  pub(super) fn spawn(
    &self,
    record: Record,
    position: f64,
    request: (u64, usize),
    cancel: CancellationToken,
    offered: bool,
  ) {
    let this = self.this();
    let span = info_span!(
      "transfer",
      id = %record.id,
      direction = "download",
      username = record.username
    );
    // in the queue before the task runs, so downloads spawned in turn are granted in turn
    let first = (!offered).then(|| {
      self
        .starts
        .wait(record.id, &record.username, position, Some(request))
    });
    self.tasks.spawn(
      async move { this.run(record.id, position, first, cancel, offered).await }.instrument(span),
    );
  }

  async fn run(
    self: Arc<Self>,
    id: Uuid,
    position: f64,
    first: Option<impl Future<Output = Grant>>,
    cancel: CancellationToken,
    offered: bool,
  ) {
    match self.download(id, position, first, &cancel, offered).await {
      Ended::Succeeded(record, incomplete, writing) => {
        let this = self.clone();
        let moving = async move {
          this.complete(record, incomplete, writing).await;
          this.forget(id).await;
        };
        if self.spawn_move(moving.in_current_span()) {
          return;
        }
        // the file stays whole in the incomplete directory, where the resume after the next
        // start finds it arrived
        info!("download arrived after the stop's limit; its move is left to the next start");
      }
      Ended::Failed(failed) => self.fail(failed).await,
      Ended::Abandoned => debug!("download left for the stop to end"),
    }
    self.forget(id).await;
  }

  /// The download's tries, from its turn in the local queue, which `first` waits for, to how
  /// it ended; with `offered`, the first try takes the offer its user made unasked, without a
  /// turn.
  async fn download(
    &self,
    id: Uuid,
    position: f64,
    first: Option<impl Future<Output = Grant>>,
    cancel: &CancellationToken,
    mut offered: bool,
  ) -> Ended {
    let mut first = match first {
      None => None,
      Some(first) => Some(tokio::select! {
        grant = first => grant,
        () = cancel.cancelled() => return self.canceled(id, None).await,
      }),
    };
    let mut record = match self.db.read(move |conn| store::get(conn, id)).await {
      Ok(Some(record)) => record,
      Ok(None) => {
        return Ended::failed(
          None,
          Failure::terminal(format!("Transfer with ID {id} not found")),
          None,
        );
      }
      Err(e) => return Ended::failed(None, Failure::terminal(e.to_string()), None),
    };
    if record.state != State::WAITING {
      let message = format!(
        "Invalid starting state for download; expected {}, encountered {}",
        State::WAITING,
        record.state
      );
      return Ended::failed(Some(record), Failure::terminal(message), None);
    }
    let tracked = self
      .network
      .client()
      .transfers()
      .downloads()
      .iter()
      .any(|t| t.username == record.username && t.filename.as_str() == record.filename);
    if tracked {
      return Ended::failed(Some(record), Failure::terminal(DUPLICATE.to_string()), None);
    }
    let incomplete = match incomplete_path(
      &self.incomplete,
      &record.username,
      &record.filename,
      Os::CURRENT,
    ) {
      Ok((path, shortened)) => {
        if shortened {
          warn!(path = %path.display(), "incomplete path too long; the remote directories are left out");
        }
        path
      }
      Err(e) => return Ended::failed(Some(record), Failure::terminal(e), None),
    };
    // a file sent unasked is downloaded at the size its offer was checked for, on every try
    let pushed = self.pushed(&record).await;
    let mut away = self.away.follow(&record.username);
    record.attempts = 1;
    self.save(&record, false).await;
    let mut attempt = 1;
    // the wait before the try about to start, none before the first
    let mut delay: Option<Duration> = None;
    loop {
      if let Some(delay) = delay.take() {
        info!(
          attempt,
          seconds = delay.as_secs_f64().ceil() as u64,
          "Waiting to retry the download"
        );
        tokio::select! {
          () = tokio::time::sleep(delay) => {}
          () = cancel.cancelled() => return self.canceled(id, Some(incomplete)).await,
        }
        record
          .enter(State::WAITING)
          .expect("a download waiting to retry may wait again");
        self.save(&record, false).await;
      }
      // a user known to be offline is not asked; an offer they made unasked is taken
      let mut heard = *away.borrow_and_update();
      if !offered && heard == Heard::Offline {
        // the first grant is given up, so its slot and turn go to others while the user is
        // away, and the hard limit does not run meanwhile
        first = None;
        heard = match self.wait_for_user(&mut record, &mut away, cancel).await {
          Waited::Back => Heard::Nothing,
          Waited::Missing => Heard::Missing,
          Waited::Canceled => return self.canceled(id, Some(incomplete)).await,
        };
      }
      if heard == Heard::Missing {
        let missing = Failure::terminal(USER_MISSING.to_string());
        return Ended::failed(Some(record), missing, Some(incomplete));
      }
      let grant = match first.take() {
        Some(grant) => Some(grant),
        None if offered => None,
        None => {
          // a try stopped for a user who is back already still holds the state it was in
          if record.state != State::WAITING {
            record
              .enter(State::WAITING)
              .expect("a download between tries may wait here");
            self.save(&record, true).await;
          }
          tokio::select! {
            grant = self.starts.wait(id, &record.username, position, None) => Some(grant),
            () = cancel.cancelled() => return self.canceled(id, Some(incomplete)).await,
          }
        }
      };
      let (mut gate, ask) = grant.map(Gate::of).unzip();
      self.watch_user(&record.username);
      let taking = std::mem::take(&mut offered);
      let attempted = self
        .attempt(
          &mut record,
          &incomplete,
          cancel,
          &mut gate,
          taking,
          pushed,
          ask,
          &mut away,
        )
        .await;
      match attempted {
        Attempted::Succeeded(writing) => return Ended::Succeeded(record, incomplete, writing),
        Attempted::Abandoned => return Ended::Abandoned,
        // the wait for the user is written at the top of the loop
        Attempted::UserOffline => {}
        Attempted::Failed(failure) => {
          warn!(
            attempt,
            filename = record.filename,
            outcome = ?failure.outcome,
            reason = failure.message,
            "Failed attempt to download the file"
          );
          // read at each failure, so a change reaches the downloads already running
          let retry = self
            .config
            .current()
            .values
            .transfers
            .download
            .retry
            .clone();
          if !failure.retryable || attempt >= retry.attempts {
            return Ended::failed(Some(record), failure, Some(incomplete));
          }
          // waiting to retry: queued here again, with the reason it failed shown meanwhile
          attempt += 1;
          let wait = retry_wait(attempt, retry.delay, retry.max_delay);
          let next_attempt_at =
            Some(Utc::now() + chrono::Duration::from_std(wait).unwrap_or(chrono::Duration::MAX));
          record
            .enter(State::QueuedLocally { next_attempt_at })
            .expect("a try that failed may wait to be retried");
          record.attempts = attempt;
          record.exception = Some(failure.message);
          self.save(&record, true).await;
          delay = Some(wait);
        }
      }
    }
  }

  /// Writes the download `record` as waiting for its user, who is offline, and waits until
  /// `away` no longer says they are, then writes it as waiting for its turn. A status the
  /// server pushes ends the wait, and so does one asked for as often as the download would
  /// retry, since a watch lost, refused or never answered pushes nothing.
  async fn wait_for_user(
    &self,
    record: &mut Record,
    away: &mut watch::Receiver<Heard>,
    cancel: &CancellationToken,
  ) -> Waited {
    if !record.waits_for_user() {
      let entered = record.state != State::WAITING;
      record
        .enter(State::WAITING)
        .expect("a download not completed may wait here");
      record.exception = Some(USER_OFFLINE.to_string());
      record.waiting_for_user = true;
      self.save(record, entered).await;
    }
    self.watch_user(&record.username);
    debug!("download waits for its user to come back online");
    let mut checks = 1;
    let heard = loop {
      checks += 1;
      let retry = self
        .config
        .current()
        .values
        .transfers
        .download
        .retry
        .clone();
      let every = retry_wait(checks, retry.delay, retry.max_delay);
      let checked = async {
        tokio::time::sleep(every).await;
        // the reply's status event ends the wait
        self.check_user(&record.username, every).await;
      };
      tokio::select! {
        // the user's entry is kept while this follows it, so the wait ends with word of them
        heard = away.wait_for(|heard| *heard != Heard::Offline) => {
          if let Ok(heard) = heard {
            break *heard;
          }
        }
        () = cancel.cancelled() => return Waited::Canceled,
        () = checked => {}
      }
    };
    if heard == Heard::Missing {
      return Waited::Missing;
    }
    debug!("the user of a waiting download is back; trying it again");
    record
      .enter(State::WAITING)
      .expect("a download waiting for its user may wait here");
    self.save(record, false).await;
    Waited::Back
  }

  /// Whether `username` is offline: asked of the server unless one of their downloads asked
  /// within `fresh`, whose answer stands then, and watched again; none when it cannot be told.
  /// Their downloads asking at once ask once.
  pub(super) async fn check_user(&self, username: &str, fresh: Duration) -> Option<bool> {
    let mut asked = self.away.ask_turn(username).await?;
    if asked.is_some_and(|at| at.elapsed() < fresh) {
      return Some(self.away.now(username) == Heard::Offline);
    }
    *asked = Some(Instant::now());
    self.watch_user(username);
    self.status_offline(username).await
  }

  /// Asks the server to tell us of `username`'s status, so their downloads hear when they go
  /// offline or come back; once a session, on a task of its own, so no try waits for it. A
  /// user the server says does not exist ends their downloads, said once.
  pub(super) fn watch_user(&self, username: &str) {
    let client = self.network.client();
    if !client.state().is_logged_in() || self.users.kept_watched(username) {
      return;
    }
    let this = self.this();
    let (client, username) = (client.clone(), username.to_string());
    self.tasks.spawn(
      async move {
        match this.users.try_watch(&client, &username).await {
          Ok(()) => {}
          Err(e) if e.kind() == soulseek_client::ErrorKind::UserNotFound => {
            if this.away.missing(&username) {
              info!(
                username,
                "a user downloads wait on does not exist; they end"
              );
            }
          }
          // asked again at the next check
          Err(e) => debug!(username, error = %e, "user of downloads not watched"),
        }
      }
      .in_current_span(),
    );
  }

  /// The end of a download canceled while it was not trying: left for the stop when the
  /// daemon is stopping, else failed as canceled.
  async fn canceled(&self, id: Uuid, incomplete: Option<PathBuf>) -> Ended {
    if self.shutdown.is_cancelled() {
      return Ended::Abandoned;
    }
    match self.db.read(move |conn| store::get(conn, id)).await {
      Ok(record) => Ended::failed(record, Failure::canceled(), incomplete),
      Err(e) => {
        warn!(error = %e, "canceled download not read");
        Ended::failed(None, Failure::canceled(), incomplete)
      }
    }
  }

  /// Whether the download `id` is to leave the listings once it has ended.
  pub(super) fn removal_pending(&self, id: Uuid) -> bool {
    lock(&self.in_flight)
      .get(&id)
      .is_some_and(InFlight::removing)
  }

  /// Writes a download that failed for good as completed with its outcome, unless it has ended
  /// already, counts it as failed, and, unless it was canceled, raises `DownloadFileFailed`.
  async fn fail(&self, failed: Failed) {
    let Failed {
      record,
      failure,
      incomplete,
    } = failed;
    let Some(mut record) = record else {
      return warn!(reason = failure.message, "download without a record failed");
    };
    let canceled = failure.outcome == TransferOutcome::Cancelled;
    if canceled && self.take_pause(record.id) {
      if let Err(refused) = record.enter(State::WAITING) {
        return debug!(%refused, "download ended already");
      }
      record.paused = true;
      return match self.save(&record, true).await {
        Some(Saved::Written) => info!("download paused"),
        // a cancel ended it first, and its end stands
        Some(Saved::Refused(refused)) => debug!(%refused, "download not paused; it was canceled"),
        // logged by the save
        Some(Saved::Gone) | None => {}
      };
    }
    let reason = lock(&self.in_flight)
      .get(&record.id)
      .and_then(|entry| entry.reason);
    let exception = match reason {
      Some(reason) if canceled => reason.to_string(),
      _ => failure.message,
    };
    if let Err(refused) = record.complete(failure.outcome, Some(exception)) {
      return debug!(%refused, "download ended already");
    }
    record.removed = self.removal_pending(record.id);
    self.save(&record, true).await;
    self.telemetry.metrics().transfers().downloads.failed();
    if canceled {
      return info!(reason = record.exception, "download canceled");
    }
    info!(state = %record.state, reason = record.exception, "download failed");
    let local_filename = match incomplete {
      // a file whose presence cannot be read is not named
      Some(path) if tokio::fs::try_exists(&path).await.unwrap_or(false) => {
        Some(path.display().to_string())
      }
      _ => None,
    };
    let ended_at = record.ended_at.unwrap_or_else(Utc::now);
    self.events.raise_at(
      Event::DownloadFileFailed(DownloadFileFailed {
        version: 0,
        stage: DownloadFailureStage::Download,
        reason: record.exception.clone().unwrap_or_default(),
        local_filename,
        remote_filename: record.filename.clone(),
        transfer: record.transfer(),
      }),
      ended_at,
    );
  }

  /// Lets go of the download `id` once its task has written how it ended, removing it from the
  /// listings when that was asked for meanwhile.
  async fn forget(&self, id: Uuid) {
    let entry = lock(&self.in_flight).remove(&id);
    lock(&self.asked).remove(&id);
    self.starts.forget(id);
    if entry.is_some_and(|entry| entry.removing()) {
      let download = api_types::transfers::TransferDirection::Download;
      match removal::remove(&self.db, download, id).await {
        Ok(Some(taken)) => self.taken_out(&[taken]).await,
        // its end was written removed already, or the task wrote it paused and the cancel ends
        // and removes the row itself
        Ok(None) => {}
        Err(e) => warn!(%id, error = %e, "ended download not removed"),
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  const SECOND: Duration = Duration::from_secs(1);

  #[test]
  fn each_retry_waits_twice_as_long_and_the_jitter_never_takes_it_past_the_most() {
    let whole = |attempt| retry_wait(attempt, 5 * SECOND, 60 * SECOND).as_secs();
    let waits: Vec<u64> = (2..=9).map(whole).collect();
    assert_eq!(
      waits,
      [5, 10, 20, 40, 60, 60, 60, 60],
      "under a second of jitter"
    );
    for _ in 0..100 {
      assert_eq!(retry_wait(9, 5 * SECOND, 60 * SECOND), 60 * SECOND);
    }
  }
}
