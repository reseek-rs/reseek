//! Keeping the server connection up. The watchdog runs from the start and after a connection
//! is lost, and stops once the client is connected, when asked to disconnect, and at shutdown;
//! a refused login does not start it again, and once a disconnect was asked for, at a start
//! with `--no-connect`, or at shutdown, a lost connection or new credentials do not either: only
//! a start does. One connect loop runs at a
//! time: the first attempt at once, then each after an exponential back-off with jitter, or,
//! after a login the server never answered, 15 minutes, an hour and then 6 hours apart. A timer
//! starts a new loop while the watchdog is on and none runs. The back-off goes on across loops
//! until a session stays logged in for [`SETTLED`] or the watchdog is restarted, so a server that
//! drops us during or right after every login is not tried again at once each time. A restart
//! cancels the running turn, and what
//! that turn still reports after, a refused login or its session's connect, leaves the watchdog
//! on.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use api_types::state::ConnectionWatchdogState;
use cascade::Watcher;
use chrono::Utc;
use reseek_shares::{Outage, backoff, lock};
use soulseek_client::{Client, Error as ClientError, ErrorKind};
use tokio::sync::watch;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Span, debug, error, info, info_span, warn};

use crate::config::Options;
use crate::shares::Shares;
use crate::state::StateOwner;

/// How often the timer starts a loop while the watchdog is on and no loop runs.
const BACKSTOP: Duration = Duration::from_millis(100);

/// The longest wait between two attempts after transport failures.
pub const BACKOFF_MAX: Duration = Duration::from_secs(300);

/// How long a session stays logged in before its loss counts as a new outage: one lost sooner
/// counts as a failed attempt, so the back-off goes on.
const SETTLED: Duration = Duration::from_secs(60);

/// The waits after one, two, and three or more logins in a row the server did not answer.
const UNANSWERED: [Duration; 3] = [
  Duration::from_secs(15 * 60),
  Duration::from_secs(60 * 60),
  Duration::from_secs(6 * 60 * 60),
];

/// The wait after `unanswered` logins in a row went unanswered, `None` after none.
pub fn unanswered_wait(unanswered: u32) -> Option<Duration> {
  let index = usize::try_from(unanswered.checked_sub(1)?).unwrap_or(usize::MAX);
  Some(UNANSWERED[index.min(UNANSWERED.len() - 1)])
}

/// The connect loop and its timer. Cloning gives another handle to the same watchdog.
#[derive(Clone)]
pub struct Watchdog {
  inner: Arc<Inner>,
}

struct Inner {
  client: Client,
  config: Watcher<Options>,
  state: StateOwner,
  shares: Arc<Shares>,
  shutdown: CancellationToken,
  /// whether the watchdog is on, and what may turn it on
  mode: watch::Sender<Mode>,
  /// held by the loop that runs
  running: Arc<tokio::sync::Mutex<()>>,
  /// the running turn and the counts its wait is read from
  turns: Mutex<Turns>,
  /// whether a loop runs and when its next attempt starts
  status: Mutex<Status>,
  /// the ways attempts failed since the last login, each warned about once
  outage: Mutex<Outage>,
  /// the username and password of the latest attempt, which a session it logged in has
  login: Mutex<Option<(String, String)>>,
  /// the span of the timer and of every connect loop
  span: Span,
}

/// Whether the watchdog is on, and what may turn it on
#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
  /// the timer runs and the loop keeps trying
  On,
  /// off while connected or after a login it cannot retry; a lost connection turns it on
  Off,
  /// off since a disconnect was asked for, the daemon started with `--no-connect`, or it began
  /// to stop; only a start turns it on
  Held,
}

/// The running turn's token, the failures counted so far and the sessions the last restart left
/// behind, under one lock, so a restart falls wholly before or after a turn's read of the counts,
/// its count, and a turn's result turning the watchdog off
#[derive(Default)]
struct Turns {
  /// cancels the running turn's wait or attempt
  token: Option<CancellationToken>,
  /// the client's session number when the watchdog was last restarted: the sessions up to it
  /// began before the restart, so their connect leaves the watchdog on, and one of them still
  /// up is found connected by the next loop
  superseded: u64,
  /// attempts that failed since the last login or restart
  failures: u32,
  /// logins in a row the server left unanswered
  unanswered: u32,
  /// when the session that is up logged in
  logged_in_at: Option<Instant>,
}

impl Turns {
  /// Stores a new turn's token and returns it with the failures and unanswered logins its
  /// wait is read from
  fn begin(&mut self, token: CancellationToken) -> (CancellationToken, u32, u32) {
    self.token = Some(token.clone());
    (token, self.failures, self.unanswered)
  }

  /// Counts the failed attempt of the turn `token` belongs to, an unanswered login when
  /// `unanswered`, and returns the unanswered logins in a row: a failure of another kind ends
  /// the row. A turn whose token was canceled counts nothing and gives `None`: a restart
  /// canceled it and cleared the counts, which its failure must not bring back.
  fn count(&mut self, token: &CancellationToken, unanswered: bool) -> Option<u32> {
    if token.is_cancelled() {
      return None;
    }
    self.failures += 1;
    self.unanswered = if unanswered { self.unanswered + 1 } else { 0 };
    Some(self.unanswered)
  }

  /// A login succeeded `at`: the server answered it.
  fn logged_in(&mut self, at: Instant) {
    self.unanswered = 0;
    self.logged_in_at = Some(at);
  }

  /// The session that logged in was lost `at`: the failures are forgotten when it stayed up for
  /// [`SETTLED`], and it counts as one more otherwise.
  fn lost(&mut self, at: Instant) {
    let Some(logged_in_at) = self.logged_in_at.take() else {
      return;
    };
    if at.duration_since(logged_in_at) >= SETTLED {
      self.failures = 0;
    } else {
      self.failures += 1;
    }
  }

  fn clear(&mut self) {
    self.failures = 0;
    self.unanswered = 0;
    self.logged_in_at = None;
  }

  fn cancel(&self) {
    if let Some(token) = &self.token {
      token.cancel();
    }
  }
}

#[derive(Clone, Copy, Default)]
struct Status {
  attempting: bool,
  next_attempt_at: Option<chrono::DateTime<Utc>>,
}

impl Watchdog {
  /// A watchdog that is off, for `client`, connecting with the current `config` once `shares`
  /// are initialized. Its timer stops with `shutdown`.
  pub fn new(
    client: Client,
    config: Watcher<Options>,
    state: StateOwner,
    shares: Arc<Shares>,
    shutdown: CancellationToken,
  ) -> Self {
    let watchdog = Self {
      inner: Arc::new(Inner {
        client,
        config,
        state,
        shares,
        shutdown,
        mode: watch::Sender::new(Mode::Off),
        running: Arc::new(tokio::sync::Mutex::new(())),
        turns: Mutex::new(Turns::default()),
        status: Mutex::new(Status::default()),
        outage: Mutex::new(Outage::new()),
        login: Mutex::new(None),
        span: info_span!("connection_watchdog"),
      }),
    };
    let span = watchdog.inner.span.clone();
    tokio::spawn(watchdog.clone().backstop().instrument(span));
    watchdog
  }

  /// Whether the watchdog is on.
  pub fn is_enabled(&self) -> bool {
    *self.inner.mode.borrow() == Mode::On
  }

  /// Whether the session is logged in with `username` and `password`.
  pub fn logged_in_as(&self, username: &str, password: &str) -> bool {
    let session = self.inner.client.session();
    session.state.is_logged_in()
      && session.username.as_deref() == Some(username)
      && lock(&self.inner.login).as_ref() == Some(&(username.to_owned(), password.to_owned()))
  }

  /// Takes `password` as the one the logged in session has, once the server took it.
  pub fn password_changed(&self, password: &str) {
    let username = self.inner.client.username();
    let mut login = lock(&self.inner.login);
    if let Some((held, held_password)) = login.as_mut()
      && username.as_ref() == Some(held)
    {
      *held_password = password.to_owned();
    }
  }

  /// Turns the watchdog on and starts a loop at once, unless one runs.
  pub fn start(&self) {
    self.inner.mode.send_replace(Mode::On);
    self.publish();
    self.attempt_connection();
  }

  /// The TCP connection of the client's session `session` came up: the watchdog is turned off
  /// without canceling the login, unless the session began before the last restart.
  pub fn connected(&self, session: u64) {
    let turns = lock(&self.inner.turns);
    if session <= turns.superseded {
      drop(turns);
      return debug!(
        session,
        "the connect of a session begun before a restart is not acted on"
      );
    }
    self.switch_off();
    drop(turns);
    self.publish();
  }

  /// A session that reached the server was lost: the watchdog is turned on as by a start,
  /// unless it was held off since. A session lost within [`SETTLED`] of its login counts as a
  /// failed attempt.
  pub fn lost(&self) {
    lock(&self.inner.turns).lost(Instant::now());
    let mut held = false;
    self.inner.mode.send_if_modified(|mode| match mode {
      Mode::Held => {
        held = true;
        false
      }
      Mode::On => false,
      Mode::Off => {
        *mode = Mode::On;
        true
      }
    });
    if held {
      return debug!("the watchdog is held; the lost connection is not reconnected");
    }
    self.publish();
    self.attempt_connection();
  }

  /// Turns the watchdog off for the turn `turn`, which is connected or cannot retry, unless the
  /// turn was canceled since: a restart cancels it before turning the watchdog on.
  fn stop(&self, turn: &CancellationToken) {
    let turns = lock(&self.inner.turns);
    if turn.is_cancelled() {
      drop(turns);
      return debug!("the result of a canceled turn is not acted on");
    }
    self.switch_off();
    drop(turns);
    self.publish();
  }

  /// Turns the watchdog off, so no new loop starts and the running one ends after its attempt;
  /// a watchdog held off stays held. Called under the turns lock, which a restart cancels and
  /// supersedes under before it turns the watchdog on.
  fn switch_off(&self) {
    self.inner.mode.send_if_modified(|mode| {
      let on = *mode == Mode::On;
      if on {
        *mode = Mode::Off;
      }
      on
    });
  }

  /// Turns the watchdog off until it is started again, canceling its wait or attempt: neither a
  /// connection lost before or after nor new credentials turn it on.
  pub fn hold(&self) {
    self.inner.mode.send_replace(Mode::Held);
    self.publish();
    lock(&self.inner.turns).cancel();
  }

  /// Starts over at once: the running loop and its back-off are canceled, the waits after
  /// unanswered logins forgotten, and a new loop starts as soon as the old one has ended.
  /// Returns whether the watchdog was on.
  pub fn restart(&self) -> bool {
    let was = self.is_enabled();
    if was {
      info!("(Re)connection process restarted");
    }
    self.start_over(true);
    was
  }

  /// Turns the watchdog off and cancels its wait or attempt, for a reconnect the caller makes
  /// with [`Watchdog::resume`]; false, changing nothing, while it is held.
  pub fn pause(&self) -> bool {
    let turns = lock(&self.inner.turns);
    let mut held = false;
    self.inner.mode.send_if_modified(|mode| match mode {
      Mode::Held => {
        held = true;
        false
      }
      Mode::Off => false,
      Mode::On => {
        *mode = Mode::Off;
        true
      }
    });
    if held {
      return false;
    }
    turns.cancel();
    drop(turns);
    self.publish();
    true
  }

  /// Starts over at once as a restart does, unless the watchdog is held; whether it did.
  pub fn resume(&self) -> bool {
    self.start_over(false)
  }

  /// Turns the watchdog on, cancels the running loop and its back-off and forgets the waits
  /// after unanswered logins; a new loop starts as soon as the old one has ended. A watchdog
  /// held off stays so unless `release`; whether it turned on.
  fn start_over(&self, release: bool) -> bool {
    {
      let mut turns = lock(&self.inner.turns);
      let mut on = true;
      self.inner.mode.send_if_modified(|mode| {
        if *mode == Mode::Held && !release {
          on = false;
          return false;
        }
        let changed = *mode != Mode::On;
        *mode = Mode::On;
        changed
      });
      if !on {
        return false;
      }
      turns.clear();
      turns.cancel();
      // read after the cancel, so a session numbered above it began once the canceled turn was
      // told to stop
      turns.superseded = self.inner.client.session().number;
    }
    self.publish();
    self.attempt_connection();
    true
  }

  /// Forgets the unanswered logins and the failures warned of, once a login succeeded: a failure
  /// from then on is a new outage. The failed attempts are forgotten once the session stays up
  /// for [`SETTLED`].
  pub fn logged_in(&self) {
    lock(&self.inner.turns).logged_in(Instant::now());
    lock(&self.inner.outage).end();
    self.publish();
  }

  /// Starts a loop unless one runs.
  fn attempt_connection(&self) {
    let Ok(running) = self.inner.running.clone().try_lock_owned() else {
      return;
    };
    let watchdog = self.clone();
    let span = self.inner.span.clone();
    tokio::spawn(
      async move {
        watchdog.run().await;
        *lock(&watchdog.inner.status) = Status::default();
        lock(&watchdog.inner.turns).token = None;
        watchdog.publish();
        drop(running);
      }
      .instrument(span),
    );
  }

  /// Starts a loop every [`BACKSTOP`] while the watchdog is on and none runs.
  async fn backstop(self) {
    let mut mode = self.inner.mode.subscribe();
    loop {
      tokio::select! {
        () = self.inner.shutdown.cancelled() => return,
        on = mode.wait_for(|mode| *mode == Mode::On) => if on.is_err() { return },
      }
      let mut every = tokio::time::interval(BACKSTOP);
      every.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
      while self.is_enabled() {
        tokio::select! {
          () = self.inner.shutdown.cancelled() => return,
          _ = every.tick() => self.attempt_connection(),
        }
      }
    }
  }

  /// The connect loop: tries until connected, off, or canceled.
  async fn run(&self) {
    let inner = &self.inner;
    lock(&inner.status).attempting = true;
    while self.is_enabled() {
      // read with the token stored, under the lock a restart clears and cancels under: it
      // either cancels this turn or has already cleared what this reads
      let (attempt, failed, unanswered) = lock(&inner.turns).begin(inner.shutdown.child_token());
      // connected already, as when asked to connect while connected
      if inner.client.state().is_connected() {
        self.stop(&attempt);
        return;
      }
      // the attempt that went unanswered logged the wait
      let wait = match unanswered_wait(unanswered) {
        Some(wait) => Some(wait),
        None if failed > 0 => {
          let wait = backoff::wait(failed, backoff::AT_ONCE, BACKOFF_MAX);
          info!(
            seconds = wait.as_millis().div_ceil(1000),
            "Waiting before attempting to reconnect"
          );
          Some(wait)
        }
        None => None,
      };
      let start_at = Utc::now() + wait.unwrap_or_default();
      lock(&inner.status).next_attempt_at = Some(start_at);
      self.publish();
      if let Some(wait) = wait {
        tokio::select! {
          () = attempt.cancelled() => {
            debug!("Reconnection attempt canceled");
            return;
          }
          () = tokio::time::sleep(wait) => {}
        }
      }
      if !self.is_enabled() {
        return;
      }
      let span = info_span!("server_connect", attempt = failed + 1);
      match self.attempt(&attempt, failed == 0).instrument(span).await {
        Attempt::Connected | Attempt::Canceled => return,
        Attempt::Stop => {
          self.stop(&attempt);
          return;
        }
        Attempt::Failed => {
          if lock(&inner.turns).count(&attempt, false).is_none() {
            debug!("failed attempt of a canceled turn not counted");
          }
        }
        Attempt::Unanswered(error) => match lock(&inner.turns).count(&attempt, true) {
          None => debug!(%error, "unanswered login of a canceled turn not counted"),
          Some(row) => {
            let wait = unanswered_wait(row).expect("an unanswered login is a row of one or more");
            let minutes = wait.as_secs() / 60;
            error!(
              %error,
              minutes,
              "Failed to reconnect: the server did not answer the login; the account may be \
               banned. Trying again later"
            );
          }
        },
      }
    }
  }

  /// One attempt with the current options, once the shares are initialized: told at info when it
  /// is the `first` since the last login or restart, at debug when it retries.
  async fn attempt(&self, cancel: &CancellationToken, first: bool) -> Attempt {
    let inner = &self.inner;
    tokio::select! {
      () = cancel.cancelled() => {
        debug!("Reconnection attempt canceled");
        return Attempt::Canceled;
      }
      () = inner.shares.initialized() => {}
    }
    let options = inner.config.current();
    let soulseek = &options.values.soulseek;
    let Some((username, password)) = super::credentials(soulseek) else {
      super::warn_no_credentials();
      return Attempt::Stop;
    };
    // the client connects to the server its configuration names, which follows the options
    let config = inner.client.config();
    let (address, port) = (config.server_host(), config.server_port());
    let told = "Attempting to connect to the Soulseek server";
    if first {
      info!(address, port, username, "{told}");
    } else {
      debug!(address, port, username, "{told}");
    }
    *lock(&inner.login) = Some((username.to_owned(), password.to_owned()));
    // a connect stops when its future is dropped
    let connected = tokio::select! {
      connected = inner.client.connect(username, password) => connected,
      () = cancel.cancelled() => {
        debug!("Reconnection attempt canceled");
        return Attempt::Canceled;
      }
    };
    let Err(error) = connected else {
      return Attempt::Connected;
    };
    match error.kind() {
      ErrorKind::Canceled => {
        debug!("Reconnection attempt canceled");
        Attempt::Canceled
      }
      ErrorKind::LoginNoResponse => Attempt::Unanswered(error),
      // the disconnect handler logs why; nothing retries a refused login until asked to
      ErrorKind::LoginRejected => {
        debug!(reason = error.reason(), "login refused; not trying again");
        Attempt::Stop
      }
      ErrorKind::InvalidArgument => {
        error!(%error, "Not connecting to the Soulseek server; the options cannot log in");
        Attempt::Stop
      }
      _ => {
        self.failed(&error);
        Attempt::Failed
      }
    }
  }

  /// Logs an attempt that failed: a warning the first time it fails this way since the last
  /// login, debug after, so a server that stays down is one warning and each new reason one more.
  fn failed(&self, error: &ClientError) {
    if lock(&self.inner.outage).new_reason(&error.to_string()) {
      warn!(%error, "Failed to reconnect");
    } else {
      debug!(%error, "Failed to reconnect");
    }
  }

  /// Mirrors the watchdog into `State.connectionWatchdog`. The watchdog is read inside the
  /// update, which takes one at a time, so the last of several publishing at once reads what
  /// every one of them changed before it.
  fn publish(&self) {
    let inner = &self.inner;
    inner.state.update(|state| {
      let status = *lock(&inner.status);
      state.connection_watchdog = ConnectionWatchdogState {
        is_enabled: self.is_enabled(),
        is_attempting_connection: status.attempting,
        next_attempt_at: status
          .attempting
          .then_some(status.next_attempt_at)
          .flatten(),
        is_login_unanswered: lock(&inner.turns).unanswered > 0,
      };
    });
  }
}

/// How one attempt ended.
enum Attempt {
  /// connected and logged in
  Connected,
  /// stopped by a restart, a disconnect asked for or shutdown
  Canceled,
  /// nothing the loop can retry: no credentials, or credentials the server or the client
  /// refuses
  Stop,
  /// the login was written and never answered
  Unanswered(ClientError),
  /// anything else, retried after the back-off
  Failed,
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn a_turn_canceled_by_a_restart_counts_nothing() {
    let mut turns = Turns::default();
    let (first, _, _) = turns.begin(CancellationToken::new());
    assert_eq!(turns.count(&first, true), Some(1));
    let (second, failed, unanswered) = turns.begin(CancellationToken::new());
    assert_eq!((failed, unanswered), (1, 1));
    // a restart clears and cancels before the second turn's unanswered login is counted
    turns.clear();
    turns.cancel();
    assert_eq!(turns.count(&second, true), None);
    let (_, failed, unanswered) = turns.begin(CancellationToken::new());
    assert_eq!((failed, unanswered), (0, 0), "the next turn starts at once");
  }

  #[test]
  fn a_session_lost_soon_after_its_login_keeps_the_back_off() {
    let mut turns = Turns::default();
    let token = CancellationToken::new();
    let start = Instant::now();
    // a server that takes each login and closes the session a second later
    let mut waits = Vec::new();
    for login in 0..4u32 {
      let at = start + Duration::from_secs(10 * u64::from(login));
      turns.logged_in(at);
      turns.lost(at + Duration::from_secs(1));
      let (_, failed, _) = turns.begin(token.clone());
      waits.push(failed);
    }
    assert_eq!(waits, [1, 2, 3, 4], "each quick loss is one more failure");
    // a session that stays up starts the next outage afresh
    turns.logged_in(start);
    turns.lost(start + SETTLED);
    assert_eq!(turns.begin(token).1, 0);
  }

  #[test]
  fn another_failure_ends_a_row_of_unanswered_logins() {
    let mut turns = Turns::default();
    let token = CancellationToken::new();
    assert_eq!(turns.count(&token, true), Some(1));
    assert_eq!(turns.count(&token, true), Some(2));
    assert_eq!(turns.count(&token, false), Some(0));
    let (_, failed, unanswered) = turns.begin(token.clone());
    assert_eq!(
      (failed, unanswered),
      (3, 0),
      "the refused connection sets the wait"
    );
    assert_eq!(turns.count(&token, true), Some(1));
  }

  #[tokio::test]
  async fn a_refused_login_of_a_turn_a_restart_canceled_leaves_the_watchdog_on() {
    let app = crate::testing::app(crate::testing::TRUSTED, &[]).await;
    let watchdog = app.state.network.watchdog();
    // the test holds the loop's lock and runs the turn itself, so neither a start nor the timer
    // runs one
    let _running = watchdog.inner.running.clone().try_lock_owned().unwrap();
    watchdog.start();
    let (turn, _, _) = lock(&watchdog.inner.turns).begin(watchdog.inner.shutdown.child_token());
    // the restart comes while the turn's login is refused
    watchdog.restart();
    watchdog.stop(&turn);
    assert!(
      watchdog.is_enabled(),
      "a canceled turn turned the watchdog off"
    );
    // the next turn's refused login is not tried again
    let (turn, _, _) = lock(&watchdog.inner.turns).begin(watchdog.inner.shutdown.child_token());
    watchdog.stop(&turn);
    assert!(!watchdog.is_enabled());
  }

  #[tokio::test]
  async fn each_failure_warns_once_until_a_login_and_then_again() {
    use api_types::logs::LogLevel;

    let app = crate::testing::app(crate::testing::TRUSTED, &[]).await;
    let watchdog = app.state.network.watchdog();
    use soulseek_client::error::ConnectionKind;
    let refused = ClientError::new(ErrorKind::Connection(ConnectionKind::Other), "refused");
    let unreachable = ClientError::new(ErrorKind::Connection(ConnectionKind::Other), "unreachable");
    let (logs, capturing) = crate::testing::captured_logs();
    // two reasons taking turns, as a server that is down can give
    for error in [&refused, &unreachable, &refused, &unreachable] {
      watchdog.failed(error);
    }
    watchdog.logged_in();
    watchdog.failed(&unreachable);
    drop(capturing);
    let told: Vec<(LogLevel, String)> = logs
      .snapshot()
      .into_iter()
      .filter(|record| record.message.starts_with("Failed to reconnect"))
      .map(|record| (record.level, record.message))
      .collect();
    let line = |error: &str| format!("Failed to reconnect error={error}");
    assert_eq!(
      told,
      [
        (LogLevel::Warning, line("refused")),
        (LogLevel::Warning, line("unreachable")),
        (LogLevel::Debug, line("refused")),
        (LogLevel::Debug, line("unreachable")),
        (LogLevel::Warning, line("unreachable")),
      ]
    );
  }

  #[tokio::test]
  async fn a_server_that_stays_down_is_one_info_line_per_attempt() {
    use api_types::logs::LogLevel;

    let (logs, _capturing) = crate::testing::captured_logs();
    // bound and not listening, so the port refuses connections and nothing else takes it
    let held = tokio::net::TcpSocket::new_v4().unwrap();
    held.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let closed = held.local_addr().unwrap().port();
    let _app = crate::testing::soulseek::app(&format!(
      "[soulseek]\naddress = \"127.0.0.1\"\nport = {closed}\nusername = \"me\"\n\
       password = \"secret\"\n"
    ))
    .await;
    let failures = || {
      logs
        .snapshot()
        .iter()
        .filter(|record| record.message.starts_with("Failed to reconnect"))
        .count()
    };
    // the first attempt and two retries, after waits of up to a second and up to three
    tokio::time::timeout(Duration::from_secs(15), async {
      while failures() < 3 {
        tokio::time::sleep(Duration::from_millis(50)).await;
      }
    })
    .await
    .expect("three attempts failed in time");
    let records = logs.snapshot();
    let failed: Vec<usize> = records
      .iter()
      .enumerate()
      .filter(|(_, record)| record.message.starts_with("Failed to reconnect"))
      .map(|(i, _)| i)
      .collect();
    let first: Vec<&str> = records[..failed[0]]
      .iter()
      .filter(|record| record.level == LogLevel::Information)
      .map(|record| record.message.as_str())
      .filter(|line| line.starts_with("Attempting to connect") || line.starts_with("connecting"))
      .collect();
    assert_eq!(first.len(), 1, "{first:#?}");
    let retries = &records[failed[0]..failed[2]];
    let told: Vec<&str> = retries
      .iter()
      .filter(|record| record.level == LogLevel::Information)
      .map(|record| record.message.as_str())
      .collect();
    assert_eq!(told.len(), 2, "{told:#?}");
    assert!(
      told
        .iter()
        .all(|line| line.starts_with("Waiting before attempting to reconnect")),
      "{told:#?}"
    );
  }

  #[test]
  fn unanswered_logins_back_off_hard() {
    assert_eq!(unanswered_wait(0), None);
    assert_eq!(unanswered_wait(1), Some(Duration::from_secs(900)));
    assert_eq!(unanswered_wait(2), Some(Duration::from_secs(3600)));
    assert_eq!(unanswered_wait(3), Some(Duration::from_secs(21_600)));
    assert_eq!(unanswered_wait(50), Some(Duration::from_secs(21_600)));
  }
}
