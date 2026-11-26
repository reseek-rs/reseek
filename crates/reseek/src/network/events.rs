//! What the daemon does with each client event: the server connection's state into
//! `State.server` and `State.user`, starting and stopping the watchdog, what follows a login,
//! the distributed network into `State.distributedNetwork` and its metrics, the server's
//! messages to everyone, the presence of watched users, and the log lines of the rest. Events
//! about messages and rooms, the login and the disconnect go on to the messaging engine in the
//! order they came; events whose engine is another part of the daemon are handed on where that
//! engine takes them.

use api_types::activity::{ActivityKind, PrivateRoomChange};
use api_types::events::{Event as BusEvent, EventError, NoFields, SoulseekClientDisconnected};
use api_types::live::LiveFrame;
use api_types::messaging::ServerMessage;
use api_types::state::{DistributedNetworkState, ServerConnectionState};
use api_types::users::UserPresence;
use chrono::Utc;
use reseek_shares::LogLimiter;
use soulseek_client::session::State as ClientState;
use soulseek_client::soulseek_proto;
use soulseek_client::transfer::{Direction, State as TransferState, Transfer};
use soulseek_client::{
  Client, Error as ClientError, ErrorKind, Event, Events, Lagged, PrivateMessage, distributed,
  room, search, session, transfer, user,
};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, error, info, trace, warn};

use super::Intent;
use crate::app::AppState;
use crate::transfers::state::reported;

static LAGGED: LogLimiter = LogLimiter::new();

/// How far a server session got, in the order its edges come.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Phase {
  /// begun, not yet at the server
  Attempting,
  /// its TCP connection is up
  Connected,
  /// the server accepted its login
  LoggedIn,
  /// it ended
  Ended,
}

/// The server session the handled events are about and how far it got. Sessions are numbered
/// by the client, so each edge of each session is handled once, whether its event came or it
/// was taken for missed first.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Session {
  number: u64,
  phase: Phase,
}

impl Default for Session {
  /// before the first session: nothing to end
  fn default() -> Self {
    Self {
      number: 0,
      phase: Phase::Ended,
    }
  }
}

/// An edge of a session that was missed, to be handled as if its event had come.
#[derive(Debug)]
enum Missed {
  /// the session ended as `end` says; `reached` when it had reached the server
  Ended { end: session::End, reached: bool },
  /// the handled session ended and a newer one is under way; `end` is the latest end before
  /// the newer session, the handled one's or one after it
  Superseded(session::End),
  /// the newest session's TCP connection came up
  Connected,
  /// the newest session logged in as `username`
  LoggedIn { username: String },
}

impl Session {
  /// Moves on to `phase` of session `number`, returning the phase left (`Attempting` for a new
  /// session), or `None` when that edge was handled already.
  fn advance(&mut self, number: u64, phase: Phase) -> Option<Phase> {
    let next = Self { number, phase };
    if next <= *self {
      return None;
    }
    let left = if number == self.number {
      self.phase
    } else {
      Phase::Attempting
    };
    *self = next;
    Some(left)
  }

  /// Moves on to where the client's session stands in `status`, returning the edges skipped on
  /// the way, in order. Several sessions that came and went unseen count as one end.
  fn catch_up(&mut self, status: &session::Status) -> Vec<Missed> {
    // the current session's own end, once it has one
    let ended = status
      .last_end
      .as_ref()
      .filter(|end| end.session == status.number);
    let phase = if ended.is_some() {
      Phase::Ended
    } else if status.state.is_logged_in() {
      Phase::LoggedIn
    } else if status.reached {
      Phase::Connected
    } else {
      Phase::Attempting
    };
    let handled = *self;
    let mut missed = Vec::new();
    if self.advance(status.number, phase).is_none() {
      return missed;
    }
    if let Some(end) = ended {
      missed.push(Missed::Ended {
        end: end.clone(),
        reached: status.reached,
      });
      return missed;
    }
    // a newer session began, so the handled one ended and an end is kept
    if handled.phase != Phase::Ended
      && handled.number < status.number
      && let Some(end) = &status.last_end
    {
      missed.push(Missed::Superseded(end.clone()));
    }
    let from = if handled.number == status.number {
      handled.phase
    } else {
      Phase::Attempting
    };
    if from < Phase::Connected && phase >= Phase::Connected {
      missed.push(Missed::Connected);
    }
    if from < Phase::LoggedIn
      && phase >= Phase::LoggedIn
      && let Some(username) = status.username.clone()
    {
      missed.push(Missed::LoggedIn { username });
    }
    missed
  }
}

/// Handles the client's events in order until `drained` is canceled and every event raised
/// before has been handled.
pub(super) async fn follow(app: AppState, mut events: Events, drained: CancellationToken) {
  let mut session = Session::default();
  loop {
    let event = tokio::select! {
      biased;
      event = events.next() => event,
      () = drained.cancelled() => return,
    };
    match event {
      Some(Ok(event)) => handle(&app, event, &mut session).await,
      Some(Err(Lagged(missed))) => caught_up(&app, missed, &mut session).await,
      None => return,
    }
  }
}

/// Events were missed: the client's state is read again, and each edge of its session that
/// nothing handled is handled now, so a lost connection is reconnected and what follows a
/// login is not skipped. Within one session, the messaging engine marks the loss in the rooms'
/// history; a session that ended or began meanwhile leaves a gap there already.
async fn caught_up(app: &AppState, missed: u64, session: &mut Session) {
  if let Some(suppressed) = LAGGED.allow() {
    warn!(
      missed,
      suppressed, "client events missed; the client's state is read again"
    );
  }
  let client = app.network.client();
  let status = client.session();
  server_state(app, status.state);
  distributed_network(app, &client.distributed().info());
  let number = status.number;
  let edges = session.catch_up(&status);
  if edges.is_empty() {
    app.messaging.missed().await;
  }
  for edge in edges {
    match edge {
      Missed::Ended { end, reached } => {
        info!(session = number, "a missed disconnect is handled now");
        ended(app, end, reached).await;
      }
      Missed::Superseded(end) => {
        info!(
          session = end.session,
          "a missed disconnect is handled now; a newer session is under way"
        );
        let intent = app.network.take_intent(end.session);
        let intent = end.error.is_none().then_some(intent).flatten();
        raise_disconnected(app, &end.message, end.error.as_ref(), intent);
        logged_out(app, end).await;
      }
      Missed::Connected => {
        info!(session = number, "a missed connect is handled now");
        connected(app, number).await;
      }
      Missed::LoggedIn { username } => {
        info!(session = number, username, "a missed login is handled now");
        login(app, number, username).await;
      }
    }
  }
}

/// The login of session `number` as `username`: what follows a login runs, and the messaging
/// engine opens its session.
async fn login(app: &AppState, number: u64, username: String) {
  logged_in(app, &username);
  app.network.mapper.logged_in(true);
  app.network.reachability.logged_in();
  app
    .messaging
    .take(Event::Session(session::Event::LoggedIn {
      session: number,
      username,
    }))
    .await;
}

/// A session ended as `end` says, after reaching the server when `reached`.
async fn ended(app: &AppState, end: session::End, reached: bool) {
  disconnected(app, end.session, &end.message, end.error.as_ref(), reached);
  logged_out(app, end).await;
}

/// What a session's end ends besides the connection: the searches waiting for a slot end, the
/// port mapping and reachability stop counting on a login, and the messaging engine closes its
/// session when it ended.
async fn logged_out(app: &AppState, end: session::End) {
  app.searches.session_ended();
  app.network.mapper.logged_in(false);
  app.network.reachability.logged_out();
  app
    .messaging
    .take(Event::Session(session::Event::Disconnected {
      session: end.session,
      message: end.message,
      error: end.error,
      at: end.at,
    }))
    .await;
}

async fn handle(app: &AppState, event: Event, session: &mut Session) {
  match event {
    Event::Session(event) => session_event(app, event, session).await,
    Event::PrivateMessage(message) => private_message(app, message).await,
    Event::Transfer(transfer::Event::StateChanged { previous, transfer }) => {
      transfer_state(app, previous, &transfer);
    }
    // a download these end logs its own end; they also come for a file we have no download
    // of, such as one sent unasked that we refused
    Event::Transfer(transfer::Event::DownloadDenied {
      username,
      filename,
      message,
    }) => debug!(
      username,
      %filename,
      message,
      "Download was denied by the remote user"
    ),
    Event::Transfer(transfer::Event::PlaceInQueue {
      username,
      filename,
      place,
    }) => {
      app
        .downloads
        .place_sent(username, filename.as_str().to_string(), place)
        .await;
    }
    Event::Transfer(transfer::Event::DownloadFailed { username, filename }) => debug!(
      username,
      %filename,
      "Download was reported as failed by the remote user"
    ),
    Event::Room(
      room @ (room::Event::MembershipAdded(_)
      | room::Event::MembershipRemoved(_)
      | room::Event::OperatorshipAdded(_)
      | room::Event::OperatorshipRemoved(_)),
    ) => {
      private_room(app, &room);
      app.messaging.take(Event::Room(room)).await;
    }
    Event::Room(room::Event::PublicChat {
      room,
      username,
      message,
    }) => app.messaging.public_message(room, username, message),
    Event::Room(room::Event::List(list)) => app.messaging.room_list_received(list).await,
    Event::Room(room) => app.messaging.take(Event::Room(room)).await,
    Event::User(user::Event::StatusChanged { status, asked }) => {
      user_status(app, &status, asked).await;
    }
    Event::User(user::Event::StatisticsChanged { statistics, asked }) => {
      app
        .users
        .statistics_received(&statistics.username, &statistics.stats, asked)
        .await;
    }
    Event::Distributed(distributed::Event::StateChanged(info)) => {
      distributed_network(app, &info);
    }
    Event::SearchReply(search::ReplyEvent::Delivered(delivery)) => {
      // the daemon answers with the files themselves, never a frame encoded ahead, so the count
      // is always known
      let results = delivery.reply.file_count().unwrap_or(0);
      app.activity.record(
        Some(&delivery.username),
        ActivityKind::Search {
          query: delivery.query,
          results: u32::try_from(results).unwrap_or(u32::MAX),
        },
      );
    }
    Event::SearchReply(search::ReplyEvent::Dropped(dropped)) => {
      super::resolvers::dropped_by_client(&app.network.edges, &dropped);
    }
    // the transfer, search, room, user and distributed engines follow these themselves, and the
    // rest change nothing the daemon keeps
    Event::User(user::Event::CannotConnect { .. })
    | Event::Distributed(
      distributed::Event::Reset
      | distributed::Event::ParentAdopted(_)
      | distributed::Event::ParentDisconnected(_)
      | distributed::Event::ChildAdded { .. }
      | distributed::Event::ChildDisconnected { .. }
      | distributed::Event::Promoted
      | distributed::Event::Demoted,
    )
    | Event::SearchReply(search::ReplyEvent::NotDelivered(_)) => {}
    // an event a newer client raises that the daemon has no use for yet
    other => trace!(?other, "client event not followed"),
  }
}

/// An event about the server session.
async fn session_event(app: &AppState, event: session::Event, session: &mut Session) {
  match event {
    session::Event::StateChanged { state, .. } => server_state(app, state),
    session::Event::Connected { session: number } => {
      if session.advance(number, Phase::Connected).is_some() {
        connected(app, number).await;
      } else {
        debug!(
          session = number,
          "connect handled already when it was taken for missed"
        );
      }
    }
    session::Event::LoggedIn {
      session: number,
      username,
    } => {
      if session.advance(number, Phase::LoggedIn).is_some() {
        login(app, number, username).await;
      } else {
        debug!(
          session = number,
          username, "login handled already when it was taken for missed"
        );
      }
    }
    session::Event::Disconnected {
      session: number,
      message,
      error,
      at,
    } => match session.advance(number, Phase::Ended) {
      Some(left) => {
        let end = session::End {
          session: number,
          message,
          error,
          at,
        };
        ended(app, end, left >= Phase::Connected).await;
      }
      None => debug!(
        session = number,
        "disconnect handled already when it was taken for missed"
      ),
    },
    session::Event::Kicked => debug!("kicked from the server; the disconnect follows"),
    session::Event::ExcludedSearchPhrases(phrases) => {
      debug!(
        count = phrases.len(),
        ?phrases,
        "excluded search phrases received"
      );
      app.network.exclude(phrases);
    }
    session::Event::GlobalMessage(message) => server_message(app, message),
    session::Event::PrivilegedUsers(usernames) => app.users.privileged_listed(&usernames),
    session::Event::PrivilegeNotification { username, id } => {
      if let Some(id) = id {
        acknowledge_privileges(app, id);
      }
      privileges_granted(app, username);
    }
    session::Event::ServerInfo(_) => {}
    other => trace!(?other, "client event not followed"),
  }
}

/// A user's presence, which the server sends for watched users, for room members and in reply
/// to our own requests, `asked`. Their downloads follow it, a watched user's row, when they have
/// one, is touched with it, which pushes their summary, and their statistics are asked for again
/// when the server sent it on its own, since it pushes their presence and not what they share;
/// a buddy whose presence it moved is told of.
async fn user_status(app: &AppState, status: &soulseek_proto::server::UserStatus, asked: bool) {
  let username = &status.username;
  let Some(presence) = UserPresence::known(status.presence) else {
    let presence = format!("{:?}", status.presence);
    return debug!(
      username,
      presence, asked, "user status with a presence the protocol does not name, not taken"
    );
  };
  trace!(username, ?presence, asked, "user status changed");
  app.messaging.member_status(username, presence);
  app.downloads.presence(username, presence);
  let received = app
    .users
    .status_received(username, presence, status.is_privileged, asked)
    .await;
  if !received.watched {
    return;
  }
  let touch = crate::users::Touch::Status {
    presence,
    is_privileged: status.is_privileged,
  };
  app.users.touch_watched(username, &received, touch).await;
  if !asked {
    app.users.refetch_statistics(username);
    if let Some(previous) = received.previous
      && previous != presence
    {
      app.users.presence_moved(username, previous, presence);
    }
  }
}

/// Acknowledges the privilege notification `id`, as the server asks.
fn acknowledge_privileges(app: &AppState, id: u32) {
  let client = app.network.client().clone();
  tokio::spawn(
    async move {
      if let Err(e) = client
        .account()
        .acknowledge_privilege_notification(id)
        .await
      {
        debug!(error = %e, id, "privilege notification not acknowledged");
      }
    }
    .in_current_span(),
  );
}

/// The server says `username` was granted privileges: they are privileged from now on, and when
/// they are us, our own privileges are read again so the time we have shows at once.
fn privileges_granted(app: &AppState, username: String) {
  debug!(username, "privileges granted");
  app.users.privileges_granted(&username);
  if app.network.client().username().as_deref() != Some(username.as_str()) {
    return;
  }
  let network = app.network.clone();
  tokio::spawn(
    async move {
      if let Err(e) = network.refresh_user_statistics(true).await {
        warn!(error = %e, "our privileges not read again after they were granted");
      }
    }
    .in_current_span(),
  );
}

/// `State.server` and our username as the client has them in `state`. The client may have
/// moved on since, so its address and username are written only where `state` has them.
fn server_state(app: &AppState, state: ClientState) {
  let client = app.network.client();
  let username = state.is_logged_in().then(|| client.username()).flatten();
  let (address, ip_end_point) = if state.is_connected() {
    (
      client.server_host(),
      client.server_address().map(|e| e.to_string()),
    )
  } else {
    (None, None)
  };
  app.state.update(|current| {
    let server = &mut current.server;
    server.address = address;
    server.ip_end_point = ip_end_point;
    server.state = connection_state(state);
    server.is_connected = state.is_connected();
    server.is_connecting = state == ClientState::Connecting;
    server.is_logged_in = state.is_logged_in();
    server.is_logging_in = state == ClientState::LoggingIn;
    server.is_transitioning = state.is_transitioning();
    current.user.username = username;
  });
}

/// The client's session state as the API writes it.
fn connection_state(state: ClientState) -> ServerConnectionState {
  match state {
    ClientState::Disconnected => ServerConnectionState::Disconnected,
    ClientState::Connecting => ServerConnectionState::Connecting,
    ClientState::Connected => ServerConnectionState::Connected,
    ClientState::LoggingIn => ServerConnectionState::LoggingIn,
    ClientState::LoggedIn => ServerConnectionState::LoggedIn,
    ClientState::Disconnecting => ServerConnectionState::Disconnecting,
  }
}

/// The TCP connection of session `number` is up, before the login: the watchdog stops without
/// canceling the login unless a restart came since the session began, the options the client
/// had when the session began are in effect now, and the users the last session watched are
/// watched no more.
async fn connected(app: &AppState, number: u64) {
  app.network.watchdog().connected(number);
  app.users.session_ended();
  info!("Connected to the Soulseek server");
  app
    .events
    .raise(BusEvent::SoulseekClientConnected(NoFields { version: 0 }));
  // an option change the client waits to take while the session begins is not in effect
  if let Err(e) = app.config.reconnected(&app.network.applied()).await {
    warn!(error = %e, "the options in effect were not marked as reconnected");
  }
  app.network.forget_reconnect();
}

/// The server accepted our login: the server is told whether we take private room
/// invitations, the downloads waiting for a user who was offline try them again, and what needs
/// the session runs in the background.
fn logged_in(app: &AppState, username: &str) {
  info!(username, "Logged in to the Soulseek server");
  app.network.watchdog().logged_in();
  let invitations = app
    .config
    .current()
    .values
    .chat
    .accept_private_room_invitations;
  tell_invitations(app.network.client().clone(), invitations);
  app.downloads.login();
  tokio::spawn(after_login(app.clone()).in_current_span());
}

/// Tells the server whether other users may add us to their private rooms, on a task of its own.
pub(super) fn tell_invitations(client: Client, accept: bool) {
  tokio::spawn(
    async move {
      match client.rooms().accept_invitations(accept).await {
        Ok(confirmed) => debug!(accept, confirmed, "private room invitations set"),
        Err(e) => warn!(error = %e, accept, "private room invitations not set"),
      }
    }
    .in_current_span(),
  );
}

/// Once the shares are initialized: the server is told what we share and what we like and hate,
/// our statistics are fetched, the users with queued uploads, the members of the user-defined
/// groups and our buddies are watched, and the downloads a stop interrupted that no login has
/// enqueued yet are enqueued again. The messaging engine joins the rooms.
async fn after_login(app: AppState) {
  app.shares.initialized().await;
  let client = app.network.client();
  let counts = *app.shares.shared_counts().borrow();
  if let Err(e) = client
    .account()
    .set_shared_counts(counts.directories, counts.files)
    .await
  {
    warn!(error = %e, "Failed to execute post-login actions: shared counts not sent");
  }
  app.users.send_interests(client).await;
  if let Err(e) = app.network.refresh_user_statistics(true).await {
    warn!(error = %e, "Failed to execute post-login actions: statistics not fetched");
  }
  let uploads = app.uploads.clone();
  tokio::spawn(async move { uploads.logged_in().await }.in_current_span());
  let (users, watching) = (app.users.clone(), client.clone());
  tokio::spawn(async move { users.watch_buddies(&watching).await }.in_current_span());
  app.downloads.resume().await;
}

/// Tells the server what we share whenever that changes while logged in, and fetches our
/// statistics again, which the server counts from it. The login tells it what we share then.
pub async fn follow_shared_counts(app: AppState) {
  let mut counts = app.shares.shared_counts();
  counts.borrow_and_update();
  loop {
    tokio::select! {
      () = app.shutdown.cancelled() => return,
      changed = counts.changed() => if changed.is_err() { return },
    }
    let current = *counts.borrow_and_update();
    let client = app.network.client();
    if !client.state().is_logged_in() {
      continue;
    }
    if let Err(e) = client
      .account()
      .set_shared_counts(current.directories, current.files)
      .await
    {
      warn!(error = %e, "shared counts not sent");
      continue;
    }
    if let Err(e) = app.network.refresh_user_statistics(true).await {
      warn!(error = %e, "statistics not fetched after the shares changed");
    }
  }
}

/// The server session ended: why is logged, the watchdog starts again after a session that
/// reached the server unless a disconnect was asked for, by this end or since, the end is for
/// logging in again, which starts it itself, the server refused our login or another session
/// took our name, and the event is raised.
fn disconnected(
  app: &AppState,
  session: u64,
  message: &str,
  error: Option<&ClientError>,
  reached: bool,
) {
  app.network.forget_reconnect();
  // the daemon's own disconnects end the session with no error
  let intent = app.network.take_intent(session);
  let intent = error.is_none().then_some(intent).flatten();
  match error.map(ClientError::kind) {
    Some(ErrorKind::Shutdown) => {
      info!("Disconnected from the Soulseek server: the client is shutting down");
    }
    _ if intent == Some(Intent::Shutdown) => {
      info!("Disconnected from the Soulseek server: the client is shutting down");
    }
    // the change of options that asked for it logged why
    _ if intent == Some(Intent::Relogin) => {
      debug!("Disconnected from the Soulseek server: logging in again with new credentials");
    }
    // the request that asked for it logged who asked and why
    _ if intent == Some(Intent::User) => {
      debug!(
        message,
        "Disconnected from the Soulseek server: disconnected by the user"
      );
    }
    // a restart or a disconnect asked for canceled the attempt
    Some(ErrorKind::Canceled) => debug!("connection attempt canceled"),
    Some(ErrorKind::LoginRejected) => {
      error!(
        reason = error.and_then(ClientError::reason),
        "Disconnected from the Soulseek server: invalid username or password"
      );
    }
    Some(ErrorKind::Kicked) => error!(
      "Disconnected from the Soulseek server: another client logged in using the same username"
    ),
    // the watchdog's own attempt failed before reaching the server; its loop goes on
    _ if !reached => debug!(message, "connection attempt failed"),
    _ => {
      warn!(message, "Disconnected from the Soulseek server");
      app.network.watchdog().lost();
    }
  }
  raise_disconnected(app, message, error, intent);
}

/// Raises the disconnect on the event bus.
fn raise_disconnected(
  app: &AppState,
  message: &str,
  error: Option<&ClientError>,
  intent: Option<Intent>,
) {
  let exception = match (error, intent) {
    (Some(error), _) => Some(EventError {
      kind: error_kind(error).to_string(),
      message: error.to_string(),
    }),
    (None, Some(intent)) => Some(EventError {
      kind: intent_kind(intent).to_string(),
      message: intent.to_string(),
    }),
    (None, None) => None,
  };
  app.events.raise(BusEvent::SoulseekClientDisconnected(
    SoulseekClientDisconnected {
      version: 0,
      message: message.to_string(),
      exception,
    },
  ));
}

/// The short name of the daemon's reason for a disconnect, as the disconnect event carries it.
fn intent_kind(intent: Intent) -> &'static str {
  match intent {
    Intent::User => "IntentionalDisconnect",
    Intent::Shutdown => "ApplicationShutdown",
    Intent::Relogin => "Relogin",
  }
}

/// The short name of an error's kind, as the disconnect event carries it.
fn error_kind(error: &ClientError) -> &'static str {
  match error.kind() {
    ErrorKind::InvalidArgument => "InvalidArgument",
    ErrorKind::InvalidOperation => "InvalidOperation",
    ErrorKind::Timeout => "Timeout",
    ErrorKind::Canceled => "Cancelled",
    ErrorKind::Address => "Address",
    ErrorKind::Listen => "Listen",
    ErrorKind::LoginRejected => "LoginRejected",
    ErrorKind::LoginNoResponse => "LoginNoResponse",
    ErrorKind::Kicked => "KickedFromServer",
    ErrorKind::Connection(_) => "Connection",
    ErrorKind::Message(_) => "Message",
    ErrorKind::Shutdown => "Disposed",
    _ => "Client",
  }
}

/// Our place in the distributed network into `State.distributedNetwork` and its metrics.
fn distributed_network(app: &AppState, info: &distributed::Info) {
  let metrics = &app.telemetry.metrics().distributed;
  metrics.children.set(info.children.len() as f64);
  metrics.child_limit.set(f64::from(info.child_limit));
  metrics
    .has_parent
    .set(if info.parent.is_some() { 1.0 } else { 0.0 });
  metrics.branch_level.set(f64::from(info.branch_level));
  let named = |name: &str| (!name.is_empty()).then(|| name.to_string());
  let network = DistributedNetworkState {
    branch_level: i32::try_from(info.branch_level).unwrap_or(i32::MAX),
    branch_root: named(&info.branch_root),
    can_accept_children: info.can_accept_children,
    child_limit: info.child_limit,
    children: info
      .children
      .iter()
      .map(|child| child.username.clone())
      .collect(),
    has_parent: info.parent.is_some(),
    is_branch_root: info.is_branch_root,
    parent: info
      .parent
      .as_ref()
      .and_then(|parent| named(&parent.username)),
  };
  app
    .state
    .update(|state| state.distributed_network = network);
}

/// A private message: one from a user blocked from messages is acknowledged so the server
/// stops replaying it, and dropped; the others go to the messaging engine, which keeps them
/// unacknowledged until they are read.
async fn private_message(app: &AppState, message: PrivateMessage) {
  if app.users.is_blocked(
    &message.username,
    None,
    api_types::users::blocking::BlockScope::Messages,
  ) {
    debug!(
      username = message.username,
      "private message from a blocked user dropped"
    );
    let client = app.network.client().clone();
    tokio::spawn(
      async move {
        if let Err(e) = client.users().acknowledge_message(message.id).await {
          debug!(error = %e, "dropped private message not acknowledged");
        }
      }
      .in_current_span(),
    );
    return;
  }
  app.messaging.take(Event::PrivateMessage(message)).await;
}

/// A transfer moved on: logged, the upload gauges counted again for an upload, and an upload
/// that succeeded reports its speed to the server, which keeps our average speed from it. The
/// download gauges count the downloads waiting here too, which only the database holds, so
/// [`observe_transfers`] alone counts them.
fn transfer_state(app: &AppState, previous: Option<TransferState>, transfer: &Transfer) {
  let name = transfer.filename.as_str();
  let basename = name.rsplit(['\\', '/']).next().unwrap_or(name);
  let direction = match transfer.direction {
    Direction::Download => "Download",
    Direction::Upload => "Upload",
  };
  info!(
    direction,
    username = transfer.username,
    file = basename,
    previous = previous.map(|state| reported(state).as_str()),
    state = reported(transfer.state).as_str(),
    bytes = transfer.bytes_transferred,
    size = transfer.size,
    speed = transfer.average_speed,
    "transfer state changed"
  );
  if transfer.direction != Direction::Upload {
    return;
  }
  upload_activity(app, previous, transfer);
  let client = app.network.client();
  app
    .telemetry
    .metrics()
    .transfers()
    .uploads
    .count(&client.transfers().uploads());
  if transfer.state.succeeded() && transfer.average_speed > 0.0 {
    // servers read the speed as a signed 32-bit number
    let speed = transfer.average_speed.ceil().min(f64::from(i32::MAX)) as u32;
    let client = client.clone();
    tokio::spawn(
      async move {
        if let Err(e) = client.account().report_upload_speed(speed).await {
          warn!(error = %e, speed, "Failed to report upload speed");
        }
      }
      .in_current_span(),
    );
  }
}

/// An upload that started sending, or one that ended after it began, into the activity log. An
/// upload that ends while still queued was never under way, so it is not noted.
fn upload_activity(app: &AppState, previous: Option<TransferState>, transfer: &Transfer) {
  let begun = matches!(
    previous,
    Some(TransferState::Initializing | TransferState::InProgress)
  );
  let kind =
    if transfer.state == TransferState::InProgress && previous != Some(TransferState::InProgress) {
      ActivityKind::UploadStarted {
        filename: transfer.filename.to_string(),
        size: transfer.size,
      }
    } else if transfer.state.is_terminal() && begun {
      ActivityKind::UploadEnded {
        filename: transfer.filename.to_string(),
        state: reported(transfer.state),
        bytes: transfer.bytes_transferred,
      }
    } else {
      return;
    };
  app.activity.record(Some(&transfer.username), kind);
}

/// A change to our own membership of a private room, into the activity log.
fn private_room(app: &AppState, event: &room::Event) {
  let (room, change) = match event {
    room::Event::MembershipAdded(room) => (room, PrivateRoomChange::Added),
    room::Event::MembershipRemoved(room) => (room, PrivateRoomChange::Removed),
    room::Event::OperatorshipAdded(room) => (room, PrivateRoomChange::Operator),
    room::Event::OperatorshipRemoved(room) => (room, PrivateRoomChange::NoLongerOperator),
    _ => return,
  };
  app.activity.record(
    None,
    ActivityKind::PrivateRoom {
      room: room.clone(),
      change,
    },
  );
}

/// A message from the server's administrators to every user: logged and pushed.
fn server_message(app: &AppState, message: String) {
  info!(message, "Server message");
  app
    .live
    .publish(LiveFrame::ApplicationServerMessage(ServerMessage {
      message,
      received_at: Utc::now(),
    }));
}

/// Reads into the metrics what only the client knows: the distributed network's broadcast
/// latency and how many search responses wait for their searcher; run once a minute.
pub fn observe_client(app: &AppState) {
  let client = app.network.client();
  let latency = client
    .distributed()
    .info()
    .average_broadcast_latency
    .map_or(0.0, |latency| latency.as_secs_f64() * 1000.0);
  let metrics = app.telemetry.metrics();
  metrics.distributed.broadcast_latency.observe(latency);
  metrics.distributed.broadcast_latency_current.set(latency);
  let kept = client.undelivered_search_responses();
  metrics.search.response_cache_size.set(kept as f64);
  trace!(latency, kept, "client figures observed");
}

/// Counts the uploads queued and in progress again from the client's and the downloads from the
/// database, and moves both directions' speed averages; run every five seconds.
pub async fn observe_transfers(app: &AppState) {
  // downloads waiting here are queued too, which the client does not hold yet
  let tallies = app.downloads.tallies().await;
  let client = app.network.client();
  let transfers = client.transfers();
  let (uploads, downloads) = (transfers.uploads(), transfers.downloads());
  let family = app.telemetry.metrics().transfers();
  family.uploads.count(&uploads);
  let shown = app.network.shown_speeds();
  family.uploads.pace(&uploads, &shown);
  family.downloads.pace(&downloads, &shown);
  match tallies {
    Ok((queued, in_progress)) => family.downloads.set_counts(queued, in_progress),
    Err(e) => debug!(error = %e, "download counts not read; the gauges keep the last ones"),
  }
}

#[cfg(test)]
mod tests {
  use soulseek_client::error::ConnectionKind;

  use super::*;

  fn end(session: u64) -> session::End {
    session::End {
      session,
      message: "Connection reset".into(),
      error: None,
      at: std::time::SystemTime::now(),
    }
  }

  fn status(number: u64, state: ClientState, reached: bool) -> session::Status {
    session::Status {
      number,
      state,
      reached,
      username: state.is_logged_in().then(|| "me".into()),
      last_end: if state == ClientState::Disconnected {
        Some(end(number))
      } else {
        number.checked_sub(1).filter(|n| *n > 0).map(end)
      },
    }
  }

  fn logged_in(number: u64) -> Session {
    Session {
      number,
      phase: Phase::LoggedIn,
    }
  }

  #[test]
  fn caught_up_disconnect_lost_in_a_lag() {
    let mut session = logged_in(1);
    let missed = session.catch_up(&status(1, ClientState::Disconnected, true));
    assert!(
      matches!(missed.as_slice(), [Missed::Ended { reached: true, .. }]),
      "{missed:?}"
    );
    // the event itself, still queued behind the lag, is not handled twice
    assert_eq!(session.advance(1, Phase::Ended), None);
  }

  #[test]
  fn caught_up_reconnect_and_login_inside_one_lag() {
    let mut session = logged_in(1);
    let missed = session.catch_up(&status(2, ClientState::LoggedIn, true));
    assert!(
      matches!(
        missed.as_slice(),
        [
          Missed::Superseded(session::End { session: 1, .. }),
          Missed::Connected,
          Missed::LoggedIn { .. }
        ]
      ),
      "{missed:?}"
    );
    // the old session's queued disconnect and the new one's queued login are handled already
    assert_eq!(session.advance(1, Phase::Ended), None);
    assert_eq!(session.advance(2, Phase::LoggedIn), None);
  }

  #[test]
  fn caught_up_session_that_came_and_went() {
    let mut session = Session::default();
    session.advance(1, Phase::Ended);
    let missed = session.catch_up(&status(2, ClientState::Disconnected, true));
    assert!(
      matches!(missed.as_slice(), [Missed::Ended { reached: true, .. }]),
      "{missed:?}"
    );
  }

  #[test]
  fn caught_up_nothing_missed() {
    let mut session = logged_in(3);
    assert!(
      session
        .catch_up(&status(3, ClientState::LoggedIn, true))
        .is_empty()
    );
    // a disconnect under way is not taken for over, and its reach is known when it ends
    let mut session = logged_in(3);
    assert!(
      session
        .catch_up(&status(3, ClientState::Disconnecting, true))
        .is_empty()
    );
    assert_eq!(session.advance(3, Phase::Ended), Some(Phase::LoggedIn));
  }

  /// a download as the client reports it while its peer has it queued
  fn queued_download() -> Transfer {
    let mut transfer = crate::testing::soulseek::transfer_snapshot();
    transfer.state = TransferState::QueuedRemotely;
    transfer
  }

  #[tokio::test]
  async fn a_download_changing_state_leaves_the_queued_gauge_counting_the_downloads_waiting_here() {
    use api_types::transfers::{TransferDirection, TransferStates};

    use crate::transfers::fixtures::{Row, insert};

    let app = crate::testing::app(crate::testing::TRUSTED, &[]).await;
    let waiting = || {
      Row::new(
        "alice",
        TransferDirection::Download,
        TransferStates::QueuedLocally,
      )
    };
    insert(&app.state.transfers, vec![waiting(), waiting()]).await;
    let queued = || {
      let family = app.state.telemetry.metrics().transfers();
      family.downloads.figures().queued.files
    };
    observe_transfers(&app.state).await;
    assert_eq!(queued(), 2);
    // the client holds none of the downloads waiting here
    let transfer = queued_download();
    let (logs, _capturing) = crate::testing::captured_logs();
    transfer_state(&app.state, None, &transfer);
    assert_eq!(queued(), 2);
    let logged = logs.snapshot();
    assert!(
      logged
        .iter()
        .any(|line| line.message.contains("transfer state changed")
          && line.message.contains("Queued, Remotely")),
      "the state is logged by the name the daemon keeps: {logged:?}"
    );
  }

  #[tokio::test]
  async fn a_disconnect_asked_for_is_not_undone_by_a_lost_connection_handled_after_it() {
    let app = crate::testing::app(crate::testing::TRUSTED, &[]).await;
    app.state.network.disconnect(None).await;
    // the end of the session the disconnect found gone already, handled after it
    let lost = ClientError::new(ErrorKind::Connection(ConnectionKind::Read), "Read error");
    let session = app.state.network.client().session().number;
    disconnected(&app.state, session, "Read error", Some(&lost), true);
    assert!(!app.state.network.watchdog().is_enabled());
  }

  #[tokio::test]
  async fn a_disconnect_asked_for_marks_only_the_session_it_was_asked_in() {
    let app = crate::testing::app(crate::testing::TRUSTED, &[]).await;
    let network = &app.state.network;
    let asked_in = network.client().session().number;
    network.disconnect(None).await;
    // a later session that ends with no error was not ended by that disconnect
    assert_eq!(network.take_intent(asked_in + 1), None);
    assert_eq!(
      network.take_intent(asked_in),
      None,
      "gone with the later end"
    );
    network.intend(Intent::Relogin);
    assert_eq!(network.take_intent(asked_in), Some(Intent::Relogin));
  }

  #[tokio::test]
  async fn a_connect_of_a_turn_a_restart_canceled_leaves_the_watchdog_reconnecting() {
    use std::time::Duration;

    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;

    // a server that takes connections and never answers the login
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (accepted, mut accepts) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
      while let Ok((mut stream, _)) = listener.accept().await {
        if accepted.send(()).is_err() {
          return;
        }
        tokio::spawn(async move {
          let mut sink = [0; 1024];
          while stream.read(&mut sink).await.is_ok_and(|read| read > 0) {}
        });
      }
    });
    let config = format!(
      "{}[soulseek]\naddress = \"127.0.0.1\"\nport = {port}\nusername = \"me\"\n\
       password = \"secret\"\n",
      crate::testing::TRUSTED
    );
    let app = crate::testing::soulseek::app(&config).await;
    let within = Duration::from_secs(5);
    let first = tokio::time::timeout(within, accepts.recv()).await;
    assert!(
      matches!(first, Ok(Some(()))),
      "the watchdog never connected"
    );
    let client = app.state.network.client();
    let logging_in = tokio::time::timeout(within, async {
      while client.state() != ClientState::LoggingIn {
        tokio::time::sleep(Duration::from_millis(5)).await;
      }
    })
    .await;
    assert!(logging_in.is_ok(), "the login never began");
    let canceled = client.session().number;
    // a restart cancels the turn whose login is under way, and its connect is handled after
    app.state.network.connect();
    connected(&app.state, canceled).await;
    let again = tokio::time::timeout(within, accepts.recv()).await;
    assert!(
      matches!(again, Ok(Some(()))),
      "the watchdog never connected again"
    );
    // the connect of the session the next turn began stops it
    let watchdog = app.state.network.watchdog();
    let stopped = tokio::time::timeout(within, async {
      while watchdog.is_enabled() {
        tokio::time::sleep(Duration::from_millis(5)).await;
      }
    })
    .await;
    assert!(
      stopped.is_ok(),
      "the next session's connect left the watchdog on"
    );
  }

  #[tokio::test]
  async fn a_disconnect_handled_once_the_client_is_logged_in_again_shows_no_server_or_user() {
    use crate::testing::soulseek::{Server, app};

    let app = app(crate::testing::TRUSTED).await;
    let server = Server::silent().await;
    server.login(&app).await;
    let logged_in = tokio::time::timeout(std::time::Duration::from_secs(5), async {
      while !app.state.state.current().server.is_logged_in {
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
      }
    })
    .await;
    assert!(logged_in.is_ok(), "the login never reached the state");
    // the end of the session before, handled after this one logged in
    server_state(&app.state, ClientState::Disconnected);
    let current = app.state.state.current();
    assert_eq!(current.server.state, ServerConnectionState::Disconnected);
    assert_eq!(current.server.address, None);
    assert_eq!(current.server.ip_end_point, None);
    assert_eq!(current.user.username, None);
  }

  #[tokio::test]
  async fn a_stream_is_told_the_options_once_a_reconnect_put_the_waiting_ones_in_effect() {
    use crate::testing::request;

    let config = format!("remote_configuration = true\n{}", crate::testing::TRUSTED);
    let app = crate::testing::app(&config, &[]).await;
    let live = request("GET", "/api/v0/live?topics=application", None);
    let mut stream = app.stream_from("127.0.0.1:1", live).await;
    stream.until("application.options").await;
    let change = serde_json::json!({ "set": { "soulseek.port": 2243 } });
    let (status, _) = app.call("PATCH", "/api/v0/options", Some(change)).await;
    assert_eq!(status, 200);
    stream.until("application.options").await;
    let waiting = || async {
      let (_, view) = app.call("GET", "/api/v0/options/view", None).await;
      view["pendingReconnect"].clone()
    };
    assert_eq!(waiting().await, serde_json::json!(["soulseek.port"]));
    // the client takes an option change in its own task, after the frame is sent, and the
    // session must begin with the change taken
    let taken = tokio::time::timeout(std::time::Duration::from_secs(5), async {
      while app.state.network.applied().values.soulseek.port != 2243 {
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
      }
    })
    .await;
    assert!(taken.is_ok(), "the client never took the port change");

    connected(&app.state, 1).await;
    assert_eq!(waiting().await, serde_json::json!([]));
    assert!(!app.state.state.current().pending_reconnect);
    let told = tokio::time::timeout(
      std::time::Duration::from_secs(2),
      stream.until("application.options"),
    )
    .await;
    assert!(
      told.is_ok(),
      "no application.options frame after the reconnect"
    );
  }

  #[tokio::test]
  async fn a_change_the_client_has_not_taken_when_a_session_begins_still_waits_for_a_reconnect() {
    let config = format!("remote_configuration = true\n{}", crate::testing::TRUSTED);
    let app = crate::testing::app(&config, &[]).await;
    // the client's options are being patched, so the change below waits to reach it
    let patching = app.state.network.reconfiguring().lock().await;
    let change = serde_json::json!({ "set": { "soulseek.server_keepalive.probes": 7 } });
    let (status, _) = app.call("PATCH", "/api/v0/options", Some(change)).await;
    assert_eq!(status, 200);
    connected(&app.state, 1).await;
    assert_eq!(
      app.state.config.current().pending_reconnect,
      ["soulseek.server_keepalive.probes"]
    );
    drop(patching);
  }

  #[tokio::test]
  async fn the_server_is_told_whether_we_take_invitations_at_each_login_and_on_a_change() {
    use soulseek_proto::server::PrivateRoomToggle;
    use soulseek_proto::{ServerRequest, ServerResponse};

    use crate::testing::soulseek::{Server, app};

    let config = format!("remote_configuration = true\n{}", crate::testing::TRUSTED);
    let app = app(&config).await;
    // the server confirms the setting as it took it
    let mut server = Server::start(|request| match request {
      ServerRequest::PrivateRoomToggle(toggle) => {
        vec![ServerResponse::PrivateRoomToggle(toggle.clone())]
      }
      _ => Vec::new(),
    })
    .await;
    let told = |request: &ServerRequest| match request {
      ServerRequest::PrivateRoomToggle(PrivateRoomToggle { accept_invitations }) => {
        Some(*accept_invitations)
      }
      _ => None,
    };
    server.login(&app).await;
    assert!(server.received(told).await, "the default takes them");
    let change = serde_json::json!({ "set": { "chat.accept_private_room_invitations": false } });
    let (status, _) = app.call("PATCH", "/api/v0/options", Some(change)).await;
    assert_eq!(status, 200);
    assert!(!server.received(told).await, "the change, while logged in");
    server.drop_session(&app).await;
    server.login(&app).await;
    assert!(!server.received(told).await, "the next login");
  }

  #[tokio::test]
  async fn a_privilege_notification_is_acknowledged_as_the_server_asks() {
    use soulseek_proto::server::{AddPrivilegedUser, NotifyPrivileges};
    use soulseek_proto::{ServerRequest, ServerResponse};

    use crate::testing::soulseek::{Server, app};

    let app = app(crate::testing::TRUSTED).await;
    let mut server = Server::silent().await;
    server.login(&app).await;
    server.send(vec![
      ServerResponse::AddPrivilegedUser(AddPrivilegedUser {
        username: "bob".into(),
      }),
      ServerResponse::NotifyPrivileges(NotifyPrivileges {
        id: 9,
        username: "carol".into(),
      }),
    ]);
    // the added user wants no acknowledgment, so the first one is carol's
    let acknowledged = server
      .received(|request| match request {
        ServerRequest::AcknowledgeNotifyPrivileges(ack) => Some(ack.id),
        _ => None,
      })
      .await;
    assert_eq!(acknowledged, 9);
  }
}
