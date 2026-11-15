//! The background work every request relies on. The application state and the event stream
//! follow the configuration and the log, the log filter follows its options, the OpenID Connect
//! providers are discovered, and once the authentication options stand the sessions of users
//! they give no role are ended, as are those of a local account they give a new password. The
//! scheduler runs the rest: ending the sessions that expired and deleting old ended sessions and
//! tokens.

use std::collections::HashMap;
use std::sync::Arc;

use api_types::events::{Event, SessionRevokeReason, SessionRevoked};
use api_types::live::LiveFrame;
use chrono::{DateTime, Utc};
use soulseek_client::LogLimiter;
use tokio::sync::broadcast::error::RecvError;
use tracing::{Instrument, debug, info, info_span, warn};

use cascade::Watcher;
use tokio_util::sync::CancellationToken;

use super::AppState;
use crate::auth::{passwords, sessions, tokens};
use crate::config::{Options, view};
use crate::live::Topic;
use crate::logging::Logging;

/// How long ended sessions and tokens are kept for the audit view.
const KEPT: chrono::Duration = chrono::Duration::days(30);

static LOGS_MISSED: LogLimiter = LogLimiter::new();

/// How long the authentication options stand unchanged before the sessions of users they give
/// no role or a new password are ended, so a file read half written does not end them for good.
const SETTLE: std::time::Duration = std::time::Duration::from_secs(10);

pub(super) fn spawn(app: &AppState) {
  tokio::spawn(follow_configuration(app.clone()).instrument(info_span!("state")));
  tokio::spawn(settle_sessions(app.clone()).instrument(info_span!("sessions")));
  tokio::spawn(forward_logs(app.clone()).instrument(info_span!("log_stream")));
  tokio::spawn(crate::auth::oidc::discover_at_start(app.clone()));
}

/// Keeps the state's configuration part current and pushes the options whenever the
/// configuration changes: its values, a failed reload, the files read or what waits for a
/// reconnect.
async fn follow_configuration(app: AppState) {
  let mut loads = app.config.subscribe();
  let first = loads.borrow_and_update().clone();
  app.state.configured(&first);
  loop {
    tokio::select! {
      () = app.shutdown.cancelled() => return,
      changed = loads.changed() => if changed.is_err() { return },
    }
    let loaded = loads.borrow_and_update().clone();
    app.state.configured(&loaded);
    // a principal's role is read from the configuration, so every stream checks its own again
    app.live.credentials_changed();
    app
      .live
      .publish(LiveFrame::ApplicationOptions(view::document(&loaded)));
  }
}

/// Each time the authentication options have stood unchanged for [`SETTLE`], the ones the daemon
/// starts with included, ends the sessions of users they give no role, and those of the local
/// accounts they give a new password since they last stood, but the sessions that set it. Only
/// these options give a session its role, so another change does not start the wait over.
async fn settle_sessions(app: AppState) {
  let mut loads = app.config.subscribe();
  let mut standing = loads.borrow_and_update().values.web.authentication.clone();
  // the options as they last stood, which a new password is told from
  let mut stood_as = standing.clone();
  // when each local account's password last changed since then; a session of the account begun
  // after that may have the new password
  let mut changed_at: HashMap<String, DateTime<Utc>> = HashMap::new();
  let mut since = tokio::time::Instant::now();
  let mut settled = false;
  loop {
    let stood = async move {
      if settled {
        std::future::pending().await
      } else {
        tokio::time::sleep_until(since + SETTLE).await;
      }
    };
    // a change starts the wait over, also one that lands as the wait ends
    tokio::select! {
      biased;
      changed = loads.changed() => {
        if changed.is_err() {
          return;
        }
        let loaded = loads.borrow_and_update();
        if loaded.values.web.authentication != standing {
          let now = Utc::now();
          for account in passwords::changed(&standing, &loaded.values.web.authentication) {
            changed_at.insert(account, now);
          }
          standing = loaded.values.web.authentication.clone();
          since = tokio::time::Instant::now();
          settled = false;
        }
      }
      () = app.shutdown.cancelled() => return,
      () = stood => {
        // a change being made through the API, or made since, starts the wait over
        let stands = || app.config.current().values.web.authentication == standing;
        let Some(setters) = app.auth.setters.take_if(stands).await else {
          continue;
        };
        settled = true;
        if let Err(e) = crate::auth::end_sessions_without_role(&app).await {
          warn!(error = ?e, "the sessions of users with no role now were not ended");
        }
        // a password changed and changed back, as in a file read half written, is no new one
        let accounts = passwords::changed(&stood_as, &standing);
        for (account, at) in changed_at.drain() {
          if !accounts.contains(&account) {
            continue;
          }
          info!(account, "local account password changed; its other sessions end");
          let ended = crate::auth::end_sessions_for_new_password(&app, &account, at, &setters);
          if let Err(e) = ended.await {
            warn!(error = ?e, account, "the sessions of an account with a new password were not ended");
          }
        }
        stood_as = standing.clone();
      }
    }
  }
}

/// Applies the live parts of each configuration change that are not read per request.
pub(super) async fn follow_changes(
  watcher: Watcher<Options>,
  logging: Arc<Logging>,
  cancel: CancellationToken,
) {
  let mut changes = watcher.changes();
  loop {
    let change = tokio::select! {
      () = cancel.cancelled() => return,
      change = changes.recv() => change,
    };
    match change {
      Ok(change) => {
        if change
          .changed
          .iter()
          .any(|k| k.starts_with("logging.") || k == "soulseek.distributed_network.logging")
        {
          logging.apply(&change.current.values);
        }
        if !change.by_apply.restart.is_empty() {
          info!(
            keys = change.by_apply.restart.join(", "),
            "One or more updated options requires an application restart to take effect."
          );
        }
      }
      Err(tokio::sync::broadcast::error::RecvError::Lagged(missed)) => {
        warn!(
          missed,
          "configuration changes missed; applying the current values"
        );
        logging.apply(&watcher.current().values);
      }
      Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
    }
  }
}

/// Pushes every new log record to the streams following the log, while one does; without one
/// a record would only take a place in the resume window from the other topics.
async fn forward_logs(app: AppState) {
  let mut records = app.logs.subscribe();
  loop {
    let record = tokio::select! {
      () = app.shutdown.cancelled() => return,
      record = records.recv() => record,
    };
    match record {
      Ok(record) => {
        if app.live.followed(Topic::Logs) {
          app
            .live
            .publish(LiveFrame::LogsRecord(api_types::logs::LogRecord::clone(
              &record,
            )));
        }
      }
      Err(RecvError::Lagged(missed)) => {
        if let Some(suppressed) = LOGS_MISSED.allow() {
          warn!(
            missed,
            suppressed, "log records not pushed to the event stream"
          );
        }
      }
      Err(RecvError::Closed) => return,
    }
  }
}

/// Marks the sessions that expired or went unused as ended, raising `SessionRevoked` for each
/// and closing the event streams they opened.
pub(crate) async fn end_lapsed_sessions(app: &AppState) {
  let db = &app.auth.db;
  let ended = match sessions::lapsed(db, crate::auth::idle(app)).await {
    Ok(ids) => sessions::revoke(db, ids).await,
    Err(e) => Err(e),
  };
  match ended {
    Ok(ended) => {
      if !ended.is_empty() {
        app.live.credentials_changed();
      }
      for session in ended {
        debug!(subject = session.subject, "session ended by time");
        app.events.raise(Event::SessionRevoked(SessionRevoked {
          version: 0,
          session_id: session.id,
          subject: session.subject,
          by: None,
          reason: SessionRevokeReason::Expired,
        }));
      }
    }
    Err(e) => warn!(error = %e, "ended sessions not marked"),
  }
}

/// Deletes the sessions and tokens that ended a month ago; returns how many.
pub(crate) async fn prune_sessions(app: &AppState) -> Result<usize, crate::db::DbError> {
  let db = &app.auth.db;
  let before = Utc::now() - KEPT;
  let sessions = sessions::prune(db, before).await?;
  let tokens = tokens::prune(db, before).await?;
  Ok(sessions + tokens)
}

#[cfg(test)]
mod tests {
  use std::time::Duration;

  use serde_json::json;

  use super::SETTLE;
  use crate::auth::sessions;
  use crate::testing::{Answer, OUTSIDE, TestApp, app, request, set_cookie, with};

  const VIEWER: &str = "[web.authentication.local.users.viewer]\npassword = \"look\"\n";

  async fn signed_in(app: &TestApp) -> String {
    let login = json!({ "username": "viewer", "password": "look" });
    let answer = app
      .send_from(OUTSIDE, request("POST", "/api/v0/session", Some(login)))
      .await;
    set_cookie(&answer.headers, "reseek_session").unwrap()
  }

  /// Loads `text` as the configuration and lets the scan see the change before the clock moves.
  async fn reload(app: &TestApp, text: &str) {
    std::fs::write(app.dir.path().join("config.toml"), text).unwrap();
    app.state.config.reload().await.unwrap();
    tokio::task::yield_now().await;
  }

  async fn me(app: &TestApp, cookie: &str) -> Answer {
    let me = with(request("GET", "/api/v0/session", None), "cookie", cookie);
    app.send_from(OUTSIDE, me).await
  }

  async fn status(app: &TestApp, cookie: &str) -> u16 {
    me(app, cookie).await.status.as_u16()
  }

  async fn ended(app: &TestApp, cookie: &str) -> bool {
    let id = cookie.split_once('=').unwrap().1.split_once('.').unwrap().0;
    let session = sessions::find(&app.state.auth.db, id).await.unwrap();
    session.unwrap().revoked_at.is_some()
  }

  /// Waits up to another [`SETTLE`] until the session of `cookie` ended, which the scan does on
  /// tasks of its own.
  async fn until_ended(app: &TestApp, cookie: &str) {
    let step = Duration::from_millis(100);
    for _ in 0..SETTLE.as_millis() / step.as_millis() {
      if ended(app, cookie).await {
        return;
      }
      tokio::time::sleep(step).await;
    }
    panic!("the session was not ended");
  }

  /// Lets the clock pass `wait` and whatever that wakes run to its end: a paused clock moves on
  /// by itself only once every task waits on a timer.
  async fn pass(wait: Duration) {
    tokio::time::advance(wait).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
  }

  #[tokio::test(start_paused = true)]
  async fn a_removal_that_does_not_stand_ends_no_session() {
    let app = app(VIEWER, &[]).await;
    let cookie = signed_in(&app).await;
    // a file read half written, then whole a moment later
    reload(&app, "").await;
    let withheld = me(&app, &cookie).await;
    assert_eq!(withheld.status, 403, "refused at once");
    assert_eq!(withheld.json()["error"], "access_withheld");
    assert!(
      set_cookie(&withheld.headers, "reseek_session").is_none(),
      "the browser keeps its cookie"
    );
    pass(Duration::from_secs(1)).await;
    reload(&app, VIEWER).await;
    pass(SETTLE).await;
    assert!(!ended(&app, &cookie).await);
    assert_eq!(status(&app, &cookie).await, 200);
  }

  #[tokio::test(start_paused = true)]
  async fn a_change_late_in_a_wait_starts_it_over() {
    let app = app(VIEWER, &[]).await;
    let cookie = signed_in(&app).await;
    let other = format!("{VIEWER}[web]\nhostnames = [\"reseek.lan\"]\n");
    reload(&app, &other).await;
    pass(SETTLE - Duration::from_secs(3)).await;
    // read half written near the end of that wait, then whole after it would have ended
    reload(&app, "").await;
    pass(Duration::from_secs(3)).await;
    reload(&app, VIEWER).await;
    pass(SETTLE).await;
    assert!(!ended(&app, &cookie).await);
  }

  #[tokio::test(start_paused = true)]
  async fn a_removal_that_stands_ends_the_session() {
    let app = app(VIEWER, &[]).await;
    let cookie = signed_in(&app).await;
    reload(&app, "").await;
    pass(SETTLE - Duration::from_secs(2)).await;
    assert!(
      !ended(&app, &cookie).await,
      "the removal has not stood long enough"
    );
    pass(Duration::from_secs(2)).await;
    until_ended(&app, &cookie).await;
    reload(&app, VIEWER).await;
    assert_eq!(status(&app, &cookie).await, 401, "it takes a new sign-in");
  }

  #[tokio::test(start_paused = true)]
  async fn a_removal_stands_while_other_options_keep_changing() {
    let app = app(VIEWER, &[]).await;
    let cookie = signed_in(&app).await;
    reload(&app, "").await;
    // a change every 5 s for 30 s, which would put off the end each time
    for n in 0..6 {
      pass(Duration::from_secs(4)).await;
      reload(&app, &format!("[web]\nhostnames = [\"{n}.lan\"]\n")).await;
    }
    for _ in 0..20 {
      if ended(&app, &cookie).await {
        return;
      }
      tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("the session was not ended");
  }

  #[tokio::test(start_paused = true)]
  async fn turning_a_way_of_signing_in_off_ends_its_sessions() {
    let config = |local: bool, proxy: bool| {
      format!(
        "[web]\ntrusted_proxies = [\"127.0.0.1/32\"]\n[web.authentication.local]\n\
         enabled = {local}\n[web.authentication.proxy]\nenabled = {proxy}\n\
         [web.authentication.proxy.role_mapping]\nreadwrite = [\"users\"]\n{VIEWER}"
      )
    };
    let app = app(&config(true, true), &[]).await.with_page();
    let local = signed_in(&app).await;
    let page = with(request("GET", "/", None), "x-authentik-username", "alice");
    let page = app.send(with(page, "x-authentik-groups", "users")).await;
    let proxied = set_cookie(&page.headers, "reseek_session").unwrap();

    reload(&app, &config(false, true)).await;
    assert_eq!(status(&app, &local).await, 403, "withheld at once");
    assert_eq!(status(&app, &proxied).await, 200);
    pass(SETTLE).await;
    until_ended(&app, &local).await;
    assert!(!ended(&app, &proxied).await);

    reload(&app, &config(true, false)).await;
    assert_eq!(status(&app, &proxied).await, 403);
    pass(SETTLE).await;
    until_ended(&app, &proxied).await;
    assert_eq!(status(&app, &local).await, 401, "it takes a new sign-in");
  }

  /// Signs in as the built-in account with `password` and returns the session cookie.
  async fn admin(app: &TestApp, password: &str) -> String {
    let login = json!({ "username": "reseek", "password": password });
    let answer = app
      .send_from(OUTSIDE, request("POST", "/api/v0/session", Some(login)))
      .await;
    set_cookie(&answer.headers, "reseek_session").unwrap()
  }

  /// A change of the built-in account's password to `password`, made from the session of
  /// `cookie`.
  fn new_password(cookie: &str, password: &str) -> axum::http::Request<axum::body::Body> {
    let change = json!({ "set": { "web.authentication.password": password } });
    let change = with(
      request("PATCH", "/api/v0/options", Some(change)),
      "cookie",
      cookie,
    );
    with(change, "origin", "http://localhost")
  }

  #[tokio::test(start_paused = true)]
  async fn a_new_password_that_stands_ends_the_accounts_other_sessions() {
    let config = |viewer: &str, host: usize| {
      format!(
        "remote_configuration = true\n[web]\nhostnames = [\"{host}.lan\"]\n\
         [web.authentication.local.users.viewer]\npassword = \"{viewer}\"\n"
      )
    };
    let app = app(&config("look", 0), &[]).await;
    let setter = admin(&app, "reseek").await;
    let other = admin(&app, "reseek").await;
    let viewer = signed_in(&app).await;

    let changed = app.send_from(OUTSIDE, new_password(&setter, "n3w")).await;
    assert_eq!(changed.status, 200, "{:?}", changed.body);
    let newer = admin(&app, "n3w").await;
    // many changes meanwhile, none to the authentication options
    for host in 1..=20 {
      reload(&app, &config("look", host)).await;
    }
    assert_eq!(status(&app, &other).await, 200, "not before it stands");
    pass(SETTLE).await;
    until_ended(&app, &other).await;
    assert_eq!(
      status(&app, &setter).await,
      200,
      "the session that set it goes on"
    );
    assert_eq!(status(&app, &newer).await, 200, "as does one begun with it");
    assert_eq!(status(&app, &viewer).await, 200, "and another account's");

    // a file has no session to keep
    reload(&app, &config("seen", 0)).await;
    pass(SETTLE).await;
    until_ended(&app, &viewer).await;
    assert_eq!(status(&app, &setter).await, 200);
  }

  #[tokio::test(start_paused = true)]
  async fn a_session_begun_with_the_old_password_ends_whatever_changed_before() {
    let config = |extra: &str| format!("remote_configuration = true\n{extra}");
    let app = app(&config(""), &[]).await;
    let setter = admin(&app, "reseek").await;
    // another change to the authentication options first, in the same wait
    reload(
      &app,
      &config("[web.authentication.local.users.viewer]\npassword = \"look\"\n"),
    )
    .await;
    pass(Duration::from_secs(2)).await;
    let leaked = admin(&app, "reseek").await;
    pass(Duration::from_secs(2)).await;
    let changed = app.send_from(OUTSIDE, new_password(&setter, "n3w")).await;
    assert_eq!(changed.status, 200, "{:?}", changed.body);
    pass(SETTLE).await;
    until_ended(&app, &leaked).await;
    assert_eq!(status(&app, &setter).await, 200);
  }

  #[tokio::test(start_paused = true)]
  async fn a_session_begun_with_a_password_changed_again_ends() {
    let app = app("remote_configuration = true\n", &[]).await;
    let setter = admin(&app, "reseek").await;
    let changed = app.send_from(OUTSIDE, new_password(&setter, "mid")).await;
    assert_eq!(changed.status, 200, "{:?}", changed.body);
    pass(Duration::from_secs(2)).await;
    let between = admin(&app, "mid").await;
    pass(Duration::from_secs(2)).await;
    let changed = app.send_from(OUTSIDE, new_password(&setter, "new")).await;
    assert_eq!(changed.status, 200, "{:?}", changed.body);
    pass(SETTLE).await;
    until_ended(&app, &between).await;
    assert_eq!(status(&app, &setter).await, 200);
  }

  #[tokio::test(start_paused = true)]
  async fn a_password_change_whose_request_goes_away_keeps_the_setters_session() {
    let app = app("remote_configuration = true\n", &[]).await;
    let setter = admin(&app, "reseek").await;
    let other = admin(&app, "reseek").await;
    let mut changing = Box::pin(app.send_from(OUTSIDE, new_password(&setter, "n3w")));
    let in_effect = std::future::poll_fn(|cx| {
      if app
        .state
        .config
        .current()
        .values
        .web
        .authentication
        .password
        == "n3w"
      {
        std::task::Poll::Ready(())
      } else {
        cx.waker().wake_by_ref();
        std::task::Poll::Pending
      }
    });
    // the change is written and the request goes away before it is answered, as a tab reloaded
    tokio::select! {
      biased;
      () = in_effect => {}
      answer = &mut changing => panic!("answered before it went away: {}", answer.status),
    }
    drop(changing);
    pass(SETTLE).await;
    until_ended(&app, &other).await;
    assert_eq!(status(&app, &setter).await, 200);
  }

  #[tokio::test(start_paused = true)]
  async fn a_password_read_half_written_ends_no_session() {
    let whole = "[web.authentication]\npassword = \"s3cret\"\n";
    let app = app(whole, &[]).await;
    let cookie = admin(&app, "s3cret").await;
    // read without the password line, so with the default one, then whole a moment later
    reload(&app, "").await;
    pass(Duration::from_secs(1)).await;
    reload(&app, whole).await;
    pass(SETTLE).await;
    pass(SETTLE).await;
    assert!(!ended(&app, &cookie).await);
    assert_eq!(status(&app, &cookie).await, 200);
  }

  #[tokio::test(start_paused = true)]
  async fn a_removal_made_while_stopped_ends_the_session_after_the_start() {
    let app = app(VIEWER, &[]).await;
    let cookie = signed_in(&app).await;
    std::fs::write(app.dir.path().join("config.toml"), "").unwrap();
    let app = app.restarted(&[]).await;
    pass(SETTLE).await;
    until_ended(&app, &cookie).await;
  }
}
