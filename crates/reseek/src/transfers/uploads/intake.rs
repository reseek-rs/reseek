//! Deciding a peer's request to queue an upload from us, once the network's edge passed it.
//! Requests are decided one at a time per user and [`DECIDING`] at a time over every user; a
//! request waits for its user's turn before it takes one of those, so one user cannot hold them
//! all. When the requests waiting would take more than [`BACKLOG_MILLIS`] at the pace of the
//! last ones accepted, a new request is dropped, since its peer would have given up by then.
//! A request for a file queued to its user already is answered as queued. One for a file not
//! shared, or shared and gone from disk, is refused, and so is one past the limits of its user's
//! group, unless the user is privileged. A request for a file of a relay agent that cannot say
//! now whether it has it, being away, loading its shares or slow to answer, is failed instead,
//! so the peer asks again.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use api_types::transfers::{EnqueueFailure, SendFilesResponse};
use chrono::Utc;
use reseek_shares::LogLimiter;
use reseek_shares::lock;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, SemaphorePermit};
use tracing::{Instrument, debug, info, trace, warn};

use super::enqueue::Enqueued;
use super::{Uploads, limits};
use crate::shares::{LOCAL_HOST, Resolved};
use crate::telemetry::millis;

/// What a peer is told when too many requests wait to be decided.
pub const OVERWHELMED: &str = "Overwhelmed with requests; try again later.";

/// What a peer is told for a file we do not share to them.
pub const NOT_SHARED: &str = "File not shared.";

/// What a peer is told when the queue takes no new user now. Clients show a reason they do not
/// know as the request's end and do not ask again on their own.
pub const QUEUE_FULL: &str = "Queue full";

/// What a peer is told when deciding its request failed here.
pub const ENQUEUE_FAILED: &str = "Enqueue failed due to internal error";

/// Why an upload is not queued.
#[derive(Debug)]
pub enum NotQueued {
  /// refused, with the reason the peer is told as written
  Refused(String),
  /// the file cannot be queued now; the peer is told the upload failed, so it asks again
  Later,
  /// something here failed, as said
  Failed(String),
}

impl From<String> for NotQueued {
  fn from(error: String) -> Self {
    Self::Failed(error)
  }
}

impl std::fmt::Display for NotQueued {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::Refused(reason) => f.write_str(reason),
      Self::Later => f.write_str("the file cannot be queued now"),
      Self::Failed(error) => f.write_str(error),
    }
  }
}

/// How many users may have uploads in the queue at once. A new user past them is refused until
/// one leaves, which bounds the queue over every user, and with it the users cache, which keeps
/// every user the queue holds.
pub const QUEUED_USERS: usize = 10_000;

/// How many of the users in the queue may have asked from one address. A peer names itself as it
/// likes, so without it one host could hold every place under names it makes up.
pub const QUEUED_PER_ADDRESS: usize = 100;

/// The longest the requests waiting may take, at the pace of the last ones, in milliseconds.
const BACKLOG_MILLIS: f64 = 180_000.0;

/// How many requests are decided at once over every user.
const DECIDING: usize = 10;

/// Requests dropped for too many waiting, which come in bursts.
static DROPPED: LogLimiter = LogLimiter::new();
/// Requests refused, which a peer can repeat at any rate.
static REFUSED: LogLimiter = LogLimiter::new();
/// Shared files missing or changed on disk, which every request for them finds again.
static DRIFTED: LogLimiter = LogLimiter::new();

/// The turns requests take and how long the last ones took.
pub(super) struct Intake {
  /// one request per user at a time; a turn is made when a request first needs it
  turns: Mutex<HashMap<String, Arc<Semaphore>>>,
  /// the requests decided at once over every user
  deciding: Semaphore,
  /// requests waiting for either
  pub(super) waiting: AtomicUsize,
  /// the address each user whose request was queued asked from, kept while the queue holds them
  addresses: Mutex<HashMap<String, IpAddr>>,
  /// the moving average of the time accepted requests took, in milliseconds, as `f64` bits
  pub(super) latency: AtomicU64,
}

impl Default for Intake {
  fn default() -> Self {
    Self {
      turns: Mutex::new(HashMap::new()),
      deciding: Semaphore::new(DECIDING),
      waiting: AtomicUsize::new(0),
      addresses: Mutex::new(HashMap::new()),
      latency: AtomicU64::new(0f64.to_bits()),
    }
  }
}

/// A request's turn: its user's, then one of the deciding turns.
struct Turn<'a> {
  _user: OwnedSemaphorePermit,
  _deciding: SemaphorePermit<'a>,
}

/// Counts a request as waiting while it lives, so one given up still leaves the count.
struct Waiting<'a>(&'a AtomicUsize);

impl Drop for Waiting<'_> {
  fn drop(&mut self) {
    self.0.fetch_sub(1, Ordering::AcqRel);
  }
}

impl Intake {
  /// Waits for `username`'s turn, then for one of the deciding turns; returns the turn and how
  /// many requests still wait.
  async fn turn(&self, username: &str) -> (Turn<'_>, usize) {
    self.waiting.fetch_add(1, Ordering::AcqRel);
    let waiting = Waiting(&self.waiting);
    // taken under the lock, so the collection never drops a turn a request is about to wait on
    let user = lock(&self.turns)
      .entry(username.to_string())
      .or_insert_with(|| Arc::new(Semaphore::new(1)))
      .clone();
    let user = user
      .acquire_owned()
      .await
      .expect("a user's turn is never closed");
    let deciding = self
      .deciding
      .acquire()
      .await
      .expect("the deciding turns are never closed");
    drop(waiting);
    let left = self.waiting.load(Ordering::Acquire);
    (
      Turn {
        _user: user,
        _deciding: deciding,
      },
      left,
    )
  }

  /// How long, in milliseconds, the requests waiting would take at the pace of the last ones.
  fn backlog(&self) -> f64 {
    let latency = f64::from_bits(self.latency.load(Ordering::Acquire));
    self.waiting.load(Ordering::Acquire) as f64 * latency
  }

  /// Drops the turns of users nobody holds or waits for; skipped when a request is taking a
  /// turn right now.
  pub(super) fn collect_idle(&self) {
    let Ok(mut turns) = self.turns.try_lock() else {
      return debug!("user enqueue turns in use; collected at the next minute");
    };
    let before = turns.len();
    // a request holds a clone from before it waits until it is decided, so a turn only the
    // map refers to is neither held nor waited for
    turns.retain(|_, turn| Arc::strong_count(turn) > 1);
    let dropped = before - turns.len();
    if dropped > 0 {
      debug!(dropped, "idle user enqueue turns dropped");
    }
  }
}

/// Why a request is not queued.
pub(super) enum Refusal {
  /// the peer is told this
  Peer(&'static str),
  /// the file is a relay agent's that is away; the peer is told to ask again later
  Later,
  /// something here failed, as said; the peer is told only that
  Failed(String),
}

/// Where a requested file stands.
pub(super) enum Locating {
  /// shared and on disk
  Found(Located),
  /// not shared, or gone from disk
  NotShared,
  /// a relay agent's that cannot say now whether it has it: away, loading its shares, or too
  /// slow to answer
  Later,
}

/// A shared file found on disk.
#[derive(Clone, Debug)]
pub(super) struct Located {
  /// where the share index has it
  pub resolved: Resolved,
  /// its size on disk now
  pub size: u64,
}

impl Uploads {
  /// Counts a request a peer sent to queue an upload.
  pub fn request_received(&self) {
    self.telemetry.metrics().enqueue.requests_received.inc();
  }

  /// Counts a request the network's edge refused.
  pub fn request_refused(&self) {
    self.telemetry.metrics().enqueue.requests_rejected.inc();
  }

  /// Whether the requests waiting would take more than `BACKLOG_MILLIS` at the pace of the
  /// last ones; a request that finds them so is counted as dropped and refused.
  pub fn overwhelmed(&self, username: &str) -> bool {
    let backlog = self.intake.backlog();
    if backlog <= BACKLOG_MILLIS {
      return false;
    }
    let enqueue = &self.telemetry.metrics().enqueue;
    enqueue.requests_dropped.inc();
    enqueue.requests_rejected.inc();
    if let Some(suppressed) = DROPPED.allow() {
      warn!(
        username,
        backlog_ms = backlog as u64,
        suppressed,
        "upload request dropped; too many wait to be decided"
      );
    }
    true
  }

  /// Whether the queue takes no upload of `username`, asking from `from`, now: they have none
  /// in it, and it holds [`QUEUED_USERS`] users already, or [`QUEUED_PER_ADDRESS`] who asked
  /// from that address. A refusal is counted.
  pub fn queue_full(&self, username: &str, from: IpAddr) -> bool {
    let queued: HashSet<String> = self.queue.users().into_iter().collect();
    if queued.contains(username) {
      return false;
    }
    let from_there = {
      let mut addresses = lock(&self.intake.addresses);
      addresses.retain(|user, _| queued.contains(user));
      addresses.values().filter(|&&ip| ip == from).count()
    };
    if queued.len() < QUEUED_USERS && from_there < QUEUED_PER_ADDRESS {
      return false;
    }
    self.telemetry.metrics().enqueue.requests_rejected.inc();
    if let Some(suppressed) = REFUSED.allow() {
      warn!(
        username,
        %from,
        users = queued.len(),
        from_there,
        suppressed,
        "upload request refused; the queue takes no new user now"
      );
    }
    true
  }

  /// The request of `username`, asking from `from`, was queued: they count against that address
  /// while the queue holds them.
  pub fn queued_from(&self, username: &str, from: IpAddr) {
    lock(&self.intake.addresses).insert(username.to_string(), from);
  }

  /// Decides the request of `username` from `from`, who sees the shares of the groups `sees`,
  /// for `filename`, which passed the network's edge, and queues the upload when it is accepted.
  /// A refusal is the reason the peer is told.
  pub async fn request(
    &self,
    username: &str,
    from: IpAddr,
    filename: &str,
    sees: BTreeSet<String>,
  ) -> Result<(), NotQueued> {
    // decided on a task of its own that holds the user's turn to the end, so a caller that
    // stops waiting leaves no upload half queued and the user's next request waits for this one
    let this = self.this();
    let (username, filename) = (username.to_owned(), filename.to_owned());
    let ran = self.tasks.spawn(
      async move { this.decide_request(&username, from, &filename, &sees).await }.in_current_span(),
    );
    match ran.await {
      Ok(result) => result,
      Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
      Err(e) => Err(NotQueued::Failed(format!(
        "the upload request was not decided: {e}"
      ))),
    }
  }

  /// The request of [`Uploads::request`], on the task it runs on.
  async fn decide_request(
    &self,
    username: &str,
    from: IpAddr,
    filename: &str,
    sees: &BTreeSet<String>,
  ) -> Result<(), NotQueued> {
    let (_turn, waiting) = self.intake.turn(username).await;
    let enqueue = &self.telemetry.metrics().enqueue;
    enqueue.queue_depth.set(waiting as f64);
    // a peer asking again for a file queued to them is answered as queued, not decided again:
    // the limits count that file, so they could turn it away
    if self.sending_already(username, filename, true).await? {
      enqueue.requests_accepted.inc();
      debug!(
        username,
        filename, "upload request for a file already queued"
      );
      return Ok(());
    }
    let started = Instant::now();
    let decision = self.decide(username, Some(from), filename, sees).await;
    let decided = millis(started);
    enqueue.decision_latency.observe(decided);
    enqueue.decision_latency_current.update(decided);
    let located = match decision {
      Ok(located) => located,
      Err(Refusal::Peer(reason)) => {
        enqueue.requests_rejected.inc();
        if let Some(suppressed) = REFUSED.allow() {
          debug!(
            username,
            filename, reason, suppressed, "upload request refused"
          );
        }
        return Err(NotQueued::Refused(reason.to_string()));
      }
      Err(Refusal::Later) => {
        enqueue.requests_rejected.inc();
        if let Some(suppressed) = REFUSED.allow() {
          debug!(
            username,
            filename, suppressed, "upload request failed; the agent holding the file is away"
          );
        }
        return Err(NotQueued::Later);
      }
      Err(Refusal::Failed(error)) => return Err(NotQueued::Failed(error)),
    };
    self.enqueue(username, filename, located, true).await?;
    let total = millis(started);
    enqueue.latency.observe(total);
    let average = enqueue.latency_current.update(total);
    self
      .intake
      .latency
      .store(average.to_bits(), Ordering::Release);
    enqueue.requests_accepted.inc();
    info!(
      username,
      filename,
      ms = total as u64,
      decision_ms = decided as u64,
      "upload request accepted"
    );
    Ok(())
  }

  /// Queues `files` to be offered to `username`, who did not ask for them, each decided as a
  /// request of theirs would be, with the shares they see as a buddy when they are one; a file
  /// queued or being sent to them already is answered as such and left as it is. These are our
  /// own sends, so the figures of the requests peers send are left alone.
  pub async fn send(&self, username: &str, files: Vec<String>) -> SendFilesResponse {
    // sent on a task of its own that holds the user's turn to the end, so a caller that stops
    // waiting leaves no file half queued and no file of the list left out
    let this = self.this();
    let username = username.to_owned();
    // kept to name the files as not sent when the runtime shuts down under the send
    let asked = files.clone();
    let ran = self
      .tasks
      .spawn(async move { this.send_each(&username, files).await }.in_current_span());
    match ran.await {
      Ok(sent) => sent,
      Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
      Err(e) => SendFilesResponse {
        queued: Vec::new(),
        already_sending: Vec::new(),
        failed: asked
          .into_iter()
          .map(|filename| EnqueueFailure {
            filename,
            message: format!("the file was not queued: {e}"),
          })
          .collect(),
      },
    }
  }

  /// The send of [`Uploads::send`], on the task it runs on.
  async fn send_each(&self, username: &str, files: Vec<String>) -> SendFilesResponse {
    let (sees, _) = self.users.search_sees(username, None).await;
    let mut sent = SendFilesResponse {
      queued: Vec::new(),
      already_sending: Vec::new(),
      failed: Vec::new(),
    };
    let (_turn, _) = self.intake.turn(username).await;
    for filename in files {
      match self.send_one(username, &filename, &sees).await {
        Ok(Enqueued::Queued) => {
          info!(username, filename, "file queued to be sent");
          sent.queued.push(filename);
        }
        Ok(Enqueued::Already) => {
          info!(username, filename, "file sent again is already being sent");
          sent.already_sending.push(filename);
        }
        Err(e) => {
          debug!(username, filename, error = %e, "file not sent");
          sent.failed.push(EnqueueFailure {
            filename,
            message: e.to_string(),
          });
        }
      }
    }
    sent
  }

  /// Sends `filename` to `username`, who sees the shares of `sees`, under their turn. One being
  /// sent already is not decided again: the limits count it, so they could turn it away.
  async fn send_one(
    &self,
    username: &str,
    filename: &str,
    sees: &BTreeSet<String>,
  ) -> Result<Enqueued, NotQueued> {
    if self.sending_already(username, filename, false).await? {
      return Ok(Enqueued::Already);
    }
    match self.decide(username, None, filename, sees).await {
      Ok(located) => Ok(self.enqueue(username, filename, located, false).await?),
      Err(Refusal::Peer(reason)) => Err(NotQueued::Refused(reason.to_string())),
      Err(Refusal::Later) => Err(NotQueued::Later),
      Err(Refusal::Failed(error)) => Err(NotQueued::Failed(error)),
    }
  }

  /// Whether `username` may have `filename`: shared with them, on disk, and within the limits
  /// of their group unless they are privileged. A request `from` an address that does not
  /// count as theirs is held to a stranger's limits; `None` is a file we send of our own accord.
  async fn decide(
    &self,
    username: &str,
    from: Option<IpAddr>,
    filename: &str,
    sees: &BTreeSet<String>,
  ) -> Result<Located, Refusal> {
    let located = match self.locate(sees, filename).await.map_err(Refusal::Failed)? {
      Locating::Found(located) => located,
      Locating::NotShared => return Err(Refusal::Peer(NOT_SHARED)),
      Locating::Later => return Err(Refusal::Later),
    };
    let client = self.client().map_err(Refusal::Failed)?;
    let placement = self.users.fetch_placement(&client, username, false).await;
    let placement = match from {
      Some(from) if !self.users.counts_as(username, &placement, from).await => {
        self.users.stranger()
      }
      _ => placement,
    };
    if placement.privileged {
      debug!(username, "Limits bypassed; the user is privileged");
      return Ok(located);
    }
    let now = Utc::now();
    let name = username.to_string();
    let figures = self
      .db
      .read(move |conn| limits::statistics(conn, &name, now))
      .await
      .map_err(|e| {
        Refusal::Failed(format!(
          "the upload figures of {username} were not read: {e}"
        ))
      })?;
    trace!(username, ?figures, "upload figures");
    let options = self.config.current();
    match limits::refusal(&options.values, &placement.limits, &figures, located.size) {
      Some(reason) => Err(Refusal::Peer(reason)),
      None => Ok(located),
    }
  }

  /// Where the shared file `filename` is, with its size on disk; not shared when it is in no
  /// share, in a share the groups `sees` do not see, or gone from disk. A local file gone, or
  /// whose size changed, asks the shares for a scan; one that changed is taken at its size on
  /// disk. A relay agent's file is taken at its indexed size when the options trust agents'
  /// listings, else the agent is asked.
  pub(super) async fn locate(
    &self,
    sees: &BTreeSet<String>,
    filename: &str,
  ) -> Result<Locating, String> {
    let view = self.shares.view(sees);
    let resolved = self
      .shares
      .resolve_file(filename, &view)
      .await
      .map_err(|e| format!("the share index could not be read: {e}"))?;
    let Some(resolved) = resolved else {
      if self.agent_away(sees, filename).await {
        return Ok(Locating::Later);
      }
      trace!(filename, "requested file not shared");
      return Ok(Locating::NotShared);
    };
    if resolved.host != LOCAL_HOST {
      return Ok(self.locate_on_agent(filename, resolved).await);
    }
    let size = match tokio::fs::metadata(&resolved.path).await {
      Ok(metadata) if metadata.is_file() => metadata.len(),
      found => {
        if let Some(suppressed) = DRIFTED.allow() {
          let error = found.err().map(|e| e.to_string());
          warn!(
            path = %resolved.path.display(),
            error,
            suppressed,
            "The shared file could not be located on disk. A share scan should be performed"
          );
        }
        self.scan_needed().await;
        return Ok(Locating::NotShared);
      }
    };
    if size != resolved.size {
      if let Some(suppressed) = DRIFTED.allow() {
        warn!(
          path = %resolved.path.display(),
          indexed = resolved.size,
          size,
          suppressed,
          "a shared file's size changed since the scan; a share scan should be performed"
        );
      }
      self.scan_needed().await;
    }
    Ok(Locating::Found(Located { resolved, size }))
  }

  /// The relay agent's file `filename`, `resolved` from its index: not shared when the agent
  /// says it is gone, later when the agent is not ready or does not answer in time.
  async fn locate_on_agent(&self, filename: &str, resolved: Resolved) -> Locating {
    let relay = self
      .relay
      .as_ref()
      .expect("only a relay controller has agents' shares");
    let agent = resolved.host.as_str();
    if !relay.ready(agent) {
      debug!(agent, filename, "the agent holding the file is not ready");
      return Locating::Later;
    }
    if self.trusts_agents {
      let size = resolved.size;
      return Locating::Found(Located { resolved, size });
    }
    let Some((exists, length)) = relay.file_info(agent, filename).await else {
      return Locating::Later;
    };
    if !exists || length == 0 {
      debug!(agent, filename, "The file could not be located on Agent");
      return Locating::NotShared;
    }
    if length != resolved.size {
      warn!(
        agent,
        filename,
        indexed = resolved.size,
        length,
        "The file's size differs between the share and disk. A share scan should be performed"
      );
    }
    Locating::Found(Located {
      resolved,
      size: length,
    })
  }

  /// Whether `filename`, which no loaded share holds, was in the last index of a relay agent
  /// that is away, in a share the groups `sees` see.
  async fn agent_away(&self, sees: &BTreeSet<String>, filename: &str) -> bool {
    match &self.relay {
      Some(relay) => relay.away_holds(filename, &self.shares.view(sees)).await,
      None => false,
    }
  }

  /// Asks the shares for a scan, after a shared file was found missing or changed.
  async fn scan_needed(&self) {
    if let Err(e) = self.shares.request_scan().await {
      warn!(error = %e, "share scan not requested");
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[tokio::test]
  async fn a_user_waits_for_their_own_turn_before_one_of_the_ten() {
    let intake = Arc::new(Intake::default());
    let (first, left) = intake.turn("alice").await;
    assert_eq!(left, 0);
    // alice's second request waits on alice, holding none of the ten
    let second = tokio::spawn({
      let intake = intake.clone();
      async move {
        let (_turn, _) = intake.turn("alice").await;
      }
    });
    tokio::task::yield_now().await;
    // so the other nine are free, and each who takes one finds alice's second still waiting
    let mut others = Vec::new();
    for n in 1..DECIDING {
      let name = format!("user{n}");
      let turn = tokio::time::timeout(std::time::Duration::from_secs(5), intake.turn(&name))
        .await
        .expect("a request waiting for its user holds one of the ten");
      assert_eq!(turn.1, 1, "{name}");
      others.push(turn);
    }
    drop(first);
    second.await.unwrap();
    drop(others);
    let (_bob, left) = intake.turn("bob").await;
    assert_eq!(left, 0);
  }

  #[tokio::test]
  async fn idle_turns_are_collected_and_held_ones_kept() {
    let intake = Intake::default();
    let (held, _) = intake.turn("alice").await;
    {
      let (_done, _) = intake.turn("bob").await;
    }
    intake.collect_idle();
    let turns: Vec<String> = intake.turns.lock().unwrap().keys().cloned().collect();
    assert_eq!(turns, ["alice"]);
    drop(held);
    intake.collect_idle();
    assert!(intake.turns.lock().unwrap().is_empty());
  }

  #[test]
  fn the_backlog_is_the_waiting_requests_at_the_pace_of_the_last_ones() {
    let intake = Intake::default();
    intake.waiting.store(4, Ordering::Release);
    assert_eq!(intake.backlog(), 0.0, "no pace before an accepted request");
    intake.latency.store(45_001f64.to_bits(), Ordering::Release);
    assert!(intake.backlog() > BACKLOG_MILLIS);
    intake.latency.store(45_000f64.to_bits(), Ordering::Release);
    assert!(intake.backlog() <= BACKLOG_MILLIS);
  }

  #[test]
  fn a_file_not_queued_says_why_as_a_send_shows_it() {
    assert_eq!(
      NotQueued::Refused("Queue full".into()).to_string(),
      "Queue full"
    );
    assert_eq!(
      NotQueued::Later.to_string(),
      "the file cannot be queued now"
    );
    let failed = "the queue was not written".to_string();
    assert_eq!(
      NotQueued::Failed(failed).to_string(),
      "the queue was not written"
    );
  }
}
