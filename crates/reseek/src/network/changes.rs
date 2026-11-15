//! Following the options while running: a change to the search request filters compiles them
//! again, a change to the Soulseek, upload or download options patches the client, a change to
//! whether we take private room invitations is told to the server while logged in, a change to
//! the listener or to port mapping moves the
//! mapping on the router, new credentials log in again at once, another change that takes
//! effect only on a new session is flagged while connected, a change to how we reach the server
//! starts the watchdog over, and rooms added to the configuration are joined.

use std::sync::Arc;

use cascade::Loaded;
use soulseek_client::{Client, ClientError};
use tokio::sync::broadcast::error::RecvError;
use tracing::{debug, info, warn};

use super::options;
use crate::app::AppState;
use crate::config::Options;

/// Applies every change of the options to the client until the daemon stops. Each change is
/// compared with the options last applied, so a change that could not be applied, or one that
/// was missed, is applied with the next.
pub async fn follow_changes(app: AppState) {
  let mut changes = app.config.changes();
  // the client was built from the options the daemon started with; a change made before this
  // subscribed is applied at once
  let mut applied = app.startup.values.clone();
  let mut current = app.config.current();
  loop {
    if let Some(now) = apply(&app, &applied, &current).await {
      applied = now;
    }
    current = tokio::select! {
      () = app.shutdown.cancelled() => return,
      change = changes.recv() => match change {
        Ok(change) => change.current.clone(),
        Err(RecvError::Lagged(missed)) => {
          debug!(missed, "options changes missed; the client follows the current options");
          app.config.current()
        }
        Err(RecvError::Closed) => return,
      },
    };
  }
}

/// Applies `current` to the client, which runs with `applied`. Returns the options now in
/// effect: `current`, with the listener's address and port left as in `applied` when the
/// listener could not move, so the next change tries them again. `None` when the client could
/// not be patched at all.
async fn apply(
  app: &AppState,
  applied: &Options,
  current: &Arc<Loaded<Options>>,
) -> Option<Options> {
  let network = &app.network;
  let mut values = current.values.clone();
  if applied.filters.search.request != values.filters.search.request {
    network
      .edges
      .searches
      .refilter(&values.filters.search.request);
  }
  let mut reconnect_required = false;
  if applied.soulseek != values.soulseek
    || applied.transfers.upload != values.transfers.upload
    || applied.transfers.download != values.transfers.download
  {
    let _reconfiguring = network.reconfiguring().lock().await;
    let alternative = app.state.current().alternative_speed_limits;
    let patch = match options::patch(applied, &values, alternative) {
      Ok(patch) => patch,
      Err(e) => {
        warn!(error = e, "Failed to apply option update");
        return None;
      }
    };
    match network.client().reconfigure_options(patch).await {
      Ok(required) => reconnect_required = required,
      // the client took the rest of the patch and kept its listener where it was
      Err(e @ ClientError::Listen { .. }) => {
        warn!(error = %e, "Failed to move the listener; the other option updates are applied");
        values.soulseek.listen_ip_address = applied.soulseek.listen_ip_address.clone();
        values.soulseek.listen_port = applied.soulseek.listen_port;
      }
      Err(e) => {
        warn!(error = %e, "Failed to apply option update");
        return None;
      }
    }
  }
  let invitations = values.chat.accept_private_room_invitations;
  if applied.chat.accept_private_room_invitations != invitations
    && network.client().state().is_logged_in()
  {
    super::events::tell_invitations(network.client().clone(), invitations);
  }
  let credentials = credentials(network.client(), applied, &values);
  let relogin = credentials == Credentials::New;
  // the reconnect-class keys changed since the session began; while disconnected the next
  // connect takes them without a flag, as logging in again does
  let mut waiting = current.pending_reconnect.clone();
  if credentials == Credentials::InSession {
    if let Err(e) = app.config.reconnected_keys(&CREDENTIALS, current).await {
      warn!(error = %e, "the credentials in effect were not marked as reconnected");
    }
    waiting.retain(|key| !CREDENTIALS.contains(&key.as_str()));
  }
  let connected = network.client().state().is_connected();
  if !relogin && ((connected && !waiting.is_empty()) || reconnect_required) {
    let already = app.state.current().pending_reconnect;
    app.state.update(|state| state.pending_reconnect = true);
    if !already {
      info!(
        keys = waiting.join(", "),
        reconnect_required,
        "One or more updated Soulseek options requires the client to be disconnected, then \
         reconnected to the network to take effect."
      );
    }
  }
  let (was, now) = (&applied.soulseek, &values.soulseek);
  if was.listen_ip_address != now.listen_ip_address
    || was.listen_port != now.listen_port
    || was.port_mapping != now.port_mapping
  {
    network.listening_changed(now.port_mapping);
  }
  // stored in the poll that saw the client take the options, so before a session that began
  // with them is connected
  network.set_applied(current.clone());
  if relogin {
    if network.log_in_again().await {
      info!("Soulseek username or password changed; logging in again");
    } else {
      debug!("Soulseek username or password changed; not logging in until a connect is asked for");
    }
  } else if network.watchdog().is_enabled() && reaches_differently(applied, &values) {
    info!("Options changed, restarting (re)connection process...");
    network.watchdog().restart();
  }
  if applied.rooms != values.rooms {
    app
      .messaging
      .configured_rooms_changed(&applied.rooms, &values.rooms);
  }
  Some(values)
}

/// The options that hold the credentials.
const CREDENTIALS: [&str; 2] = ["soulseek.username", "soulseek.password"];

/// What a change of the options does to the credentials the session logs in with.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Credentials {
  /// the same as before, or ones that cannot log in
  Kept,
  /// new ones that can log in and that the session is not logged in with
  New,
  /// new ones the session is logged in with already, as once the server changed our password
  InSession,
}

/// What the change from `previous` to `current` does to the credentials `client` logs in with.
fn credentials(client: &Client, previous: &Options, current: &Options) -> Credentials {
  let (was, now) = (&previous.soulseek, &current.soulseek);
  if was.username == now.username && was.password == now.password {
    return Credentials::Kept;
  }
  match super::credentials(now) {
    None => Credentials::Kept,
    Some((username, password)) if client.logged_in_as(username, password) => Credentials::InSession,
    Some(_) => Credentials::New,
  }
}

/// Whether the server's address, our credentials or the connection options differ.
fn reaches_differently(previous: &Options, current: &Options) -> bool {
  let (previous, current) = (&previous.soulseek, &current.soulseek);
  previous.address != current.address
    || previous.port != current.port
    || previous.username != current.username
    || previous.password != current.password
    || previous.connection != current.connection
}

#[cfg(test)]
mod tests {
  use std::time::Duration;

  use api_types::state::Reachable;
  use serde_json::json;
  use soulseek_proto::ServerRequest;

  use crate::testing::soulseek::Server;
  use crate::testing::{app, request};

  #[tokio::test]
  async fn a_live_change_reaches_the_client_without_a_reconnect() {
    let app = app(
      "remote_configuration = true\n[web.authentication]\n\
       trusted_networks = [\"127.0.0.0/8\"]\n",
      &[],
    )
    .await;
    let change = json!({ "set": {
      "transfers.upload.speed_limit": 64,
      "transfers.upload.slots": 3,
    } });
    let answer = app
      .send(request("PATCH", "/api/v0/options", Some(change)))
      .await;
    assert_eq!(answer.status, 200, "{:?}", answer.body);
    let client = app.state.network.client();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while client.options().maximum_upload_speed != 64 {
      assert!(tokio::time::Instant::now() < deadline, "never applied");
      tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(client.options().maximum_concurrent_uploads, 3);
    assert!(!app.state.state.current().pending_reconnect);
  }

  #[tokio::test]
  async fn a_new_listening_port_is_shown_and_judged_anew() {
    let app = app(
      "remote_configuration = true\n[web.authentication]\n\
       trusted_networks = [\"127.0.0.0/8\"]\n",
      &[],
    )
    .await;
    let network = &app.state.network;
    let state = &app.state.state;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let waiting = |what: &str, ready: &dyn Fn(&api_types::state::ListenState) -> bool| {
      assert!(tokio::time::Instant::now() < deadline, "never {what}");
      !ready(&state.current().server.listen)
    };
    network.reachability.logged_in();
    for token in 0..5 {
      network.reachability.search_answered(token);
    }
    while waiting("unreachable", &|listen| listen.reachable == Reachable::No) {
      tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let change = json!({ "set": { "soulseek.listen_port": 50311 } });
    let answer = app
      .send(request("PATCH", "/api/v0/options", Some(change)))
      .await;
    assert_eq!(answer.status, 200, "{:?}", answer.body);
    while waiting("judged anew on the new port", &|listen| {
      listen.port == 50311 && listen.reachable == Reachable::Unknown
    }) {
      tokio::time::sleep(Duration::from_millis(20)).await;
    }
  }

  #[tokio::test]
  async fn a_listener_that_cannot_move_does_not_keep_a_room_added_later_from_being_joined() {
    let listen = crate::testing::ports::free_port();
    let app = app(
      &format!(
        "remote_configuration = true\n[web.authentication]\n\
         trusted_networks = [\"127.0.0.0/8\"]\n\
         [soulseek]\nlisten_port = {listen}\nport_mapping = false\n"
      ),
      &[],
    )
    .await;
    let mut server = Server::silent().await;
    server.login(&app).await;
    let taken = std::net::TcpListener::bind("0.0.0.0:0").unwrap();
    let changes = [
      json!({ "set": { "soulseek.listen_port": taken.local_addr().unwrap().port() } }),
      json!({ "set": { "rooms": ["lobby"] } }),
    ];
    for change in changes {
      let answer = app
        .send(request("PATCH", "/api/v0/options", Some(change)))
        .await;
      assert_eq!(answer.status, 200, "{:?}", answer.body);
    }
    let joined = server
      .receives(
        Duration::from_secs(5),
        |request| matches!(request, ServerRequest::JoinRoom(join) if join.room == "lobby"),
      )
      .await;
    assert!(joined, "the room was not joined");
  }

  #[tokio::test]
  async fn new_credentials_log_in_again_at_once_unless_a_disconnect_was_asked_for() {
    use soulseek_proto::ServerResponse;
    use soulseek_proto::server::NewPassword;

    let (logs, _capturing) = crate::testing::captured_logs();
    let mut server = Server::start(|request| match request {
      ServerRequest::NewPassword(asked) => vec![ServerResponse::NewPassword(NewPassword {
        new_password: asked.new_password.clone(),
      })],
      _ => Vec::new(),
    })
    .await;
    // no password, so nothing connects at the start
    let app = crate::testing::soulseek::app(&format!(
      "remote_configuration = true\n[web.authentication]\n\
       trusted_networks = [\"127.0.0.0/8\"]\n\
       [soulseek]\naddress = \"127.0.0.1\"\nport = {}\nusername = \"me\"\n",
      server.port
    ))
    .await;
    let send = async |method: &str, uri: &str, body: serde_json::Value| {
      let answer = app.send(request(method, uri, Some(body))).await;
      assert!(answer.status.is_success(), "{uri}: {:?}", answer.body);
    };
    let set = async |key: &str, value: &str| {
      send("PATCH", "/api/v0/options", json!({ "set": { key: value } })).await;
    };
    let client = app.state.network.client();
    let logged_in = async || {
      let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
      while !client.state().is_logged_in() {
        assert!(tokio::time::Instant::now() < deadline, "never logged in");
        tokio::time::sleep(Duration::from_millis(20)).await;
      }
    };
    let login = |request: &ServerRequest| match request {
      ServerRequest::Login(login) => Some(login.username.clone()),
      _ => None,
    };
    let another_login = |request: &ServerRequest| matches!(request, ServerRequest::Login(_));

    set("soulseek.password", "secret").await;
    assert_eq!(server.received(login).await, "me");
    logged_in().await;
    set("soulseek.username", "other").await;
    assert_eq!(server.received(login).await, "other");
    logged_in().await;
    let flagged = logs.snapshot().into_iter().any(|record| {
      record
        .message
        .starts_with("One or more updated Soulseek options requires")
    });
    assert!(!flagged, "a change that logs in again is not flagged");

    // the server changed the password of the session, which goes on
    let change = json!({ "password": "n3w-password" });
    send("PUT", "/api/v0/application/password", change).await;
    let wait = Duration::from_secs(1);
    assert!(!server.receives(wait, another_login).await);

    app.state.network.disconnect(None).await;
    set("soulseek.username", "third").await;
    assert!(
      !server.receives(wait, another_login).await,
      "the disconnect asked for stays"
    );
  }

  #[tokio::test]
  async fn with_no_connect_nothing_connects_until_a_connect_is_asked_for() {
    let (logs, _capturing) = crate::testing::captured_logs();
    let mut server = Server::silent().await;
    let listen = crate::testing::ports::free_port();
    let app = crate::testing::app(
      &format!(
        "remote_configuration = true\n[web.authentication]\n\
         trusted_networks = [\"127.0.0.0/8\"]\n\
         [soulseek]\naddress = \"127.0.0.1\"\nport = {}\nusername = \"me\"\n\
         password = \"secret\"\nlisten_ip_address = \"127.0.0.1\"\nlisten_port = {listen}\n\
         port_mapping = false\n",
        server.port
      ),
      &[("RESEEK_NO_CONNECT", "true")],
    )
    .await;
    let network = &app.state.network;
    let send = async |method: &str, uri: &str, body: Option<serde_json::Value>| {
      let answer = app.send(request(method, uri, body)).await;
      assert!(answer.status.is_success(), "{uri}: {:?}", answer.body);
    };
    let set = async |key: &str, value: serde_json::Value| {
      send(
        "PATCH",
        "/api/v0/options",
        Some(json!({ "set": { key: value } })),
      )
      .await;
    };
    // each change is applied in the poll that records it, so once it is recorded what it
    // started has begun
    let applied = async || {
      let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
      while network.applied().revision != app.state.config.current().revision {
        assert!(tokio::time::Instant::now() < deadline, "never applied");
        tokio::time::sleep(Duration::from_millis(20)).await;
      }
    };
    assert!(!network.watchdog().is_enabled());

    set("soulseek.password", json!("other")).await;
    let told = async || {
      let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
      loop {
        let said = |start: &str| {
          logs
            .snapshot()
            .iter()
            .any(|record| record.message.starts_with(start))
        };
        if said("Soulseek username or password changed; logging in again") {
          return true;
        }
        if said("Soulseek username or password changed; not logging in") {
          return false;
        }
        assert!(tokio::time::Instant::now() < deadline, "never told");
        tokio::time::sleep(Duration::from_millis(20)).await;
      }
    };
    assert!(!told().await, "saving credentials does not connect");
    set("soulseek.server_keepalive.probes", json!(7)).await;
    applied().await;
    assert!(
      !network.watchdog().is_enabled(),
      "nor does a change that waits for a reconnect"
    );
    assert_eq!(
      network.client().state(),
      soulseek_client::ClientState::Disconnected
    );

    send("PUT", "/api/v0/server", None).await;
    let login = |request: &ServerRequest| match request {
      ServerRequest::Login(login) => Some(login.password.expose().to_string()),
      _ => None,
    };
    assert_eq!(server.received(login).await, "other");
    // logged in first, so what drops is a session that reached the server
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !network.client().state().is_logged_in() {
      assert!(tokio::time::Instant::now() < deadline, "never logged in");
      tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // from then on a lost connection is reconnected as usual
    server.close();
    assert_eq!(server.received(login).await, "other");
  }

  #[tokio::test]
  async fn a_password_the_server_changed_waits_for_no_reconnect() {
    use soulseek_proto::ServerResponse;
    use soulseek_proto::server::NewPassword;

    let server = Server::start(|request| match request {
      ServerRequest::NewPassword(asked) => vec![ServerResponse::NewPassword(NewPassword {
        new_password: asked.new_password.clone(),
      })],
      _ => Vec::new(),
    })
    .await;
    let app = crate::testing::soulseek::app(&format!(
      "remote_configuration = true\n[web.authentication]\n\
       trusted_networks = [\"127.0.0.0/8\"]\n\
       [soulseek]\naddress = \"127.0.0.1\"\nport = {}\nusername = \"me\"\npassword = \"secret\"\n",
      server.port
    ))
    .await;
    let network = &app.state.network;
    let until = async |what: &str, done: &dyn Fn() -> bool| {
      let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
      while !done() {
        assert!(tokio::time::Instant::now() < deadline, "never {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
      }
    };
    until("logged in", &|| network.client().state().is_logged_in()).await;
    let change = json!({ "password": "n3w-password" });
    let answer = app
      .send(request("PUT", "/api/v0/application/password", Some(change)))
      .await;
    assert_eq!(answer.status, 204, "{:?}", answer.body);
    // the flag is settled in the poll that records the change as applied
    until("applied", &|| {
      network.applied().values.soulseek.password.as_deref() == Some("n3w-password")
    })
    .await;
    assert!(network.client().state().is_logged_in());
    assert!(!app.state.state.current().pending_reconnect);
    let (_, view) = app.call("GET", "/api/v0/options/view", None).await;
    assert_eq!(view["pendingReconnect"], json!([]));
  }
}
