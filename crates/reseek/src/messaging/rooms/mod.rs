//! Rooms: joining and leaving, at every login (the configured rooms, then every room someone
//! asked to join and nobody left, before a restart too) and when the configuration adds a room,
//! what is said and who is there, the tickers, the private rooms we belong to and the server's room
//! list. Each room's stored history is read in `stored`.

mod events;
pub mod history;
mod private;
mod stored;
pub mod tracker;

pub use events::room_of;

use soulseek_client::soulseek_proto;
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use api_types::live::LiveFrame;
use api_types::messaging::NotifyLevel;
use api_types::messaging::rooms::{
  JoinRoom, JoinedRoom, Room, RoomInfo, RoomMessagesQuery, RoomTicker, RoomUser,
};
use api_types::users::blocking::BlockScope;
use chrono::Utc;
use reseek_shares::lock;
use soulseek_client::{Client, ClientError, RoomErrorKind};
use soulseek_proto::RoomType;
use soulseek_proto::server::{RoomData, RoomList};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, info, info_span, warn};

use super::Messaging;
use super::conversations::unsent;
use crate::api::error::ApiError;
use crate::db::DbError;
use crate::services::Joined;
use crate::turns::Turns;
use tracker::{Tracker, country_of};

/// How long the server's room list is served again before it is asked for anew.
const LIST_FOR: Duration = Duration::from_secs(60);

/// How many of a room's latest messages a room is shown with.
const LATEST: u32 = 250;

/// How long a room the server refused after a login waits before each of the tries after the
/// first; one refused every time is left for the user to join.
const REJOIN_AGAIN: [Duration; 2] = [Duration::from_secs(10), Duration::from_secs(30)];

/// The rooms we are in and the server's room list as it last sent it.
#[derive(Default)]
pub struct Rooms {
  tracker: Mutex<Tracker>,
  available: tokio::sync::Mutex<Option<(Instant, Arc<RoomList>)>>,
  /// one join of a room at a time, by room name
  joins: Turns,
  /// held from reading the rooms we are in to pushing them, so the last push has the newest
  pushing: tokio::sync::Mutex<()>,
  /// canceled to stop the rooms being joined again after a loss of client events
  reading_again: Mutex<CancellationToken>,
}

impl Rooms {
  pub(in crate::messaging) fn tracker(&self) -> MutexGuard<'_, Tracker> {
    lock(&self.tracker)
  }
}

/// The name the session is logged in as, which `verb` needs.
fn own_username(client: &Client, verb: &str) -> Result<String, ClientError> {
  client.username().ok_or_else(|| {
    ClientError::InvalidOperation(format!(
      "the server connection must be connected and logged in to {verb} (currently: {})",
      client.state()
    ))
  })
}

fn internal(error: DbError) -> ApiError {
  ApiError::internal(error)
}

fn not_in(room: &str) -> ApiError {
  ApiError::not_found(format!("The room {room} is not joined"))
}

/// The answer to a join the server refused.
fn refused(error: ClientError) -> ApiError {
  match error {
    ClientError::Room {
      kind: RoomErrorKind::JoinForbidden,
      ..
    } => ApiError::forbidden("A private room with this name already exists"),
    ClientError::InvalidArgument(message) => ApiError::bad_request(message),
    other => unsent(other),
  }
}

impl Messaging {
  /// Whether the room `name` is tracked.
  fn tracks(&self, name: &str) -> Result<(), ApiError> {
    match self.rooms.tracker().get(name) {
      Some(_) => Ok(()),
      None => Err(not_in(name)),
    }
  }

  /// Whether we can say something in the room `name`: the server has us in it, or we are not
  /// logged in, which the client then says.
  fn speaks_in(&self, name: &str) -> Result<(), ApiError> {
    let joined = match self.rooms.tracker().get(name) {
      Some(room) => room.joined,
      None => return Err(not_in(name)),
    };
    if !joined && self.logged_in_now() {
      return Err(ApiError::conflict(
        format!("The room {name} has not been joined again since the connection was lost"),
        None,
      ));
    }
    Ok(())
  }

  /// `State.rooms` as the tracker has them; the tracker is held until the state has them, so
  /// the last of two changes made at once is the one kept.
  fn rooms_changed(&self) {
    let tracker = self.rooms.tracker();
    let joined = tracker.joined_names();
    self.state.update(|state| state.rooms = joined);
  }

  /// The connection was lost: no room has us until we join again.
  pub(super) fn rooms_disconnected(&self) {
    self.rooms.tracker().disconnected();
    self.rooms_changed();
  }

  /// Joins `name`, unless the server has us in it already in this session, which is `None`. It
  /// is asked for as a private room when `private` or when the server last let us into it as
  /// one, so a private room gone meanwhile is made again as ours, never as a public room. A
  /// server that has us in the room although we do not know it, which it says by not answering,
  /// is made to forget it by leaving, and the room is joined again once. A join someone `asked`
  /// for through the UI or the API is remembered, so the room is joined again after a start
  /// until it is left. Joins of one room go one at a time.
  async fn join_now(
    &self,
    name: &str,
    private: bool,
    asked: bool,
  ) -> Result<Option<Room>, ClientError> {
    self
      .join_room(name, private, asked)
      .instrument(info_span!("room", room = name))
      .await
  }

  async fn join_room(
    &self,
    name: &str,
    private: bool,
    asked: bool,
  ) -> Result<Option<Room>, ClientError> {
    let _turn = self.rooms.joins.take(name).await;
    self.join_in_turn(name, private, asked).await
  }

  /// [`Messaging::join_room`] for a caller holding the room's turn.
  async fn join_in_turn(
    &self,
    name: &str,
    private: bool,
    asked: bool,
  ) -> Result<Option<Room>, ClientError> {
    if self
      .rooms
      .tracker()
      .get(name)
      .is_some_and(|room| room.joined)
    {
      return Ok(None);
    }
    let client = self.client();
    // the session's name marks us among the members; taken before the join is sent
    let us = own_username(client, &format!("join chat room {name}"))?;
    let leaves = self.rooms.tracker().leaves(name);
    let private = private || self.joined_private(name).await;
    let data = match self.ask_to_join(name, private).await {
      Ok(data) => data,
      Err(e) => {
        self.rooms.tracker().not_joined(name);
        return Err(e);
      }
    };
    let room = {
      let mut tracker = self.rooms.tracker();
      // a leave while the join was under way wins: the room stays left
      if tracker.leaves(name) == leaves {
        tracker.joined(&data, &us);
        Some(tracker.room(name).expect("the room was just tracked"))
      } else {
        tracker.not_joined(name);
        None
      }
    }
    .map(|room| self.marked(room));
    let Some(room) = room else {
      debug!(
        room = name,
        "the room was left while it was being joined; left again"
      );
      if let Err(e) = client.leave_room(name).await {
        debug!(room = name, error = %e, "room joined after it was left not left again");
      }
      return Err(ClientError::Canceled);
    };
    self.rooms_changed();
    // the server's list taken before we created a private room does not list it as ours
    if data
      .private
      .as_ref()
      .is_some_and(|private| private.owner == us)
    {
      self.forget_room_list().await;
    }
    let countries = data
      .users
      .iter()
      .filter_map(|user| Some((user.username.clone(), country_of(user)?.to_string())))
      .collect();
    self.users.countries_seen(countries).await;
    let room_name = name.to_string();
    let now = Utc::now();
    let private = data.private.is_some();
    if let Err(e) = self
      .db
      .write(move |conn| history::joined(conn, &room_name, now, asked, private))
      .await
    {
      warn!(room = name, error = %e, "room join not noted");
    }
    // the room as it is now, with what came in it since the join
    self.push_room(name);
    self.push_rooms().await;
    Ok(Some(room))
  }

  /// Whether the server last let us into `name` as a private room; a read that fails is taken
  /// for no.
  async fn joined_private(&self, name: &str) -> bool {
    let room = name.to_string();
    match self
      .db
      .read(move |conn| history::private(conn, &room))
      .await
    {
      Ok(private) => private,
      Err(e) => {
        warn!(room = name, error = %e, "whether the room is private not read; joined as asked");
        false
      }
    }
  }

  /// Sends the join of `name` and waits for its answer. What the server says in the room from
  /// the moment the join is sent is held for it.
  async fn ask_to_join(&self, name: &str, private: bool) -> Result<RoomData, ClientError> {
    let room_type = if private {
      RoomType::Private
    } else {
      RoomType::Public
    };
    let client = self.client();
    self.rooms.tracker().joining(name);
    match client.join_room(name, room_type).await {
      Err(ClientError::NoResponse(_)) => {
        debug!(
          room = name,
          "the server has us in the room already; left and joined again"
        );
        if let Err(e) = client.leave_room(name).await {
          debug!(room = name, error = %e, "room not left before joining it again");
        }
        // what the server said while it had us in the room belongs to no join of ours
        self.rooms.tracker().joining(name);
        client.join_room(name, room_type).await
      }
      joined => joined,
    }
  }

  /// Forgets the server's room list after a change to our rooms it predates, so the next look
  /// at it asks the server.
  async fn forget_room_list(&self) {
    *self.rooms.available.lock().await = None;
  }

  /// Keeps the room list the server sent, asked for or not, as the list served for the next
  /// while. The server sends one on its own after a login and after a change to our private
  /// rooms, and that one lists the change already.
  pub async fn room_list_received(&self, list: Arc<RoomList>) {
    debug!(
      public = list.public.len(),
      owned = list.owned.len(),
      member = list.private.len(),
      "room list received"
    );
    *self.rooms.available.lock().await = Some((Instant::now(), list));
  }

  /// Pushes the rooms we are in. One push at a time, each reading the rooms after the one
  /// before it was sent, so a push read before a change is never sent after it.
  async fn push_rooms(&self) {
    let _pushing = self.rooms.pushing.lock().await;
    match self.joined_rooms().await {
      Ok(rooms) => self.live.publish(LiveFrame::RoomsList(rooms)),
      Err(e) => warn!(error = ?e, "rooms not read to push them"),
    }
    self.unread.changed();
  }

  /// After a login: the configured rooms are joined, then the rooms we were in before the
  /// connection was lost, then the rooms someone asked to join before the last stop and nobody
  /// left, and the gap since the last session is pushed. None of these joins is remembered
  /// anew. A room the server refuses stays among the rooms, not joined, and is tried twice more
  /// a little later; one refused every time is marked refused, its history kept, until someone
  /// joins or leaves it. The tries stop with the session.
  pub(super) fn rejoin(&self) {
    let configured = self.config.current().values.rooms.clone();
    let tracked: Vec<String> = self
      .rooms
      .tracker()
      .rooms()
      .iter()
      .map(|room| room.name.clone())
      .collect();
    let session = lock(&self.session_ends).clone();
    let this = self.this();
    self.tasks.spawn(async move {
      let remembered = match this.db.read(history::remembered).await {
        Ok(remembered) => remembered,
        Err(e) => {
          warn!(error = %e, "the rooms joined before the last stop not read; not joined again");
          Vec::new()
        }
      };
      let mut rooms = configured;
      for room in tracked.into_iter().chain(remembered) {
        if !rooms.contains(&room) {
          rooms.push(room);
        }
      }
      let refused = this.rejoin_rooms(rooms, &session).await;
      this.push_gap().await;
      let again = |rooms| this.rejoin_rooms(rooms, &session);
      this.try_again(refused, &session, again).await;
    });
  }

  /// The tries after a login's first for the rooms it left `refused`: after each of the waits,
  /// the rooms still among ours and not joined are tried with `again`. Those refused at every try are
  /// marked refused, left for the user to join. Stops when `session` ends, as the next login
  /// tries them.
  async fn try_again<F>(
    &self,
    mut refused: Vec<(String, ClientError)>,
    session: &CancellationToken,
    mut again: impl FnMut(Vec<String>) -> F,
  ) where
    F: Future<Output = Vec<(String, ClientError)>>,
  {
    for wait in REJOIN_AGAIN {
      if refused.is_empty() {
        return;
      }
      debug!(
        rooms = refused.len(),
        after = %humantime::format_duration(wait),
        "rooms not joined after the login; trying again after a wait"
      );
      tokio::select! {
        () = session.cancelled() => {
          return debug!("session ended; the rooms refused are joined at the next login");
        }
        () = tokio::time::sleep(wait) => {}
      }
      // a room left or joined meanwhile is not tried again
      refused.retain(|(room, _)| {
        let tracker = self.rooms.tracker();
        tracker.get(room).is_some_and(|tracked| !tracked.joined)
      });
      let rooms = refused.into_iter().map(|(room, _)| room).collect();
      refused = again(rooms).await;
    }
    let mut marked = false;
    for (room, error) in refused {
      if self.rooms.tracker().mark_refused(&room) {
        warn!(
          room,
          tries = REJOIN_AGAIN.len() + 1,
          %error,
          "room not joined at any try after the login; left for the user to join"
        );
        marked = true;
      }
    }
    if marked {
      self.push_rooms().await;
    }
  }

  /// Whether `session` is over, so a join that failed was cut short by its end rather than
  /// refused: it ended, or the client is no longer logged in.
  fn session_over(&self, session: &CancellationToken) -> bool {
    session.is_cancelled() || !self.logged_in_now()
  }

  /// Joins `rooms` in turn after a login and returns those not joined, each with why and kept
  /// among the rooms, not joined. Stops once `session` ended, as the next login joins them.
  async fn rejoin_rooms(
    &self,
    rooms: Vec<String>,
    session: &CancellationToken,
  ) -> Vec<(String, ClientError)> {
    let mut refused = Vec::new();
    let mut kept = false;
    for room in rooms {
      let leaves = self.rooms.tracker().leaves(&room);
      match self.join_now(&room, false, false).await {
        Ok(Some(_)) => info!(room, "room joined"),
        Ok(None) => debug!(room, "room joined already"),
        Err(ClientError::Canceled) => debug!(room, "room left while it was being joined"),
        Err(e) if self.session_over(session) => {
          debug!(room, error = %e, "room not joined; the session ended");
          break;
        }
        Err(e) => {
          debug!(room, error = %e, "room not joined after the login");
          kept |= self.rooms.tracker().keep(&room, leaves);
          refused.push((room, e));
        }
      }
    }
    if kept {
      self.push_rooms().await;
    }
    refused
  }

  /// Client events were lost within the session `row` began, so who came and went in the rooms,
  /// their presence and the tickers may be out of date, and only a join makes the server send
  /// them again: each room the server has us in is left and joined again, one after another.
  /// Once they all are, the session is taken to begin then, as we were out of the rooms until
  /// then, and its gap is pushed again. A loss after this one stops these joins between two
  /// rooms and starts its own; so does the end of the session.
  pub(super) fn read_rooms_again(&self, row: i64) {
    let rooms = self.rooms.tracker().joined_names();
    let reading = {
      let mut reading = lock(&self.rooms.reading_again);
      reading.cancel();
      *reading = lock(&self.session_ends).child_token();
      reading.clone()
    };
    let session = lock(&self.session_ends).clone();
    let this = self.this();
    self.tasks.spawn(
      async move {
        for room in rooms {
          if reading.is_cancelled() {
            return debug!("rooms no longer read again; the session ended or more was lost");
          }
          this
            .join_again(&room, &session)
            .instrument(info_span!("room", room))
            .await;
        }
        this.rooms_read_again(row).await;
      }
      .in_current_span(),
    );
  }

  /// Leaves `name` and joins it again, unless it is not joined or is left meanwhile. A join the
  /// server refuses leaves the room not joined, marked refused for the user to join; one cut
  /// short by the end of `session` is left to the next login.
  async fn join_again(&self, name: &str, session: &CancellationToken) {
    let _turn = self.rooms.joins.take(name).await;
    if !self
      .rooms
      .tracker()
      .get(name)
      .is_some_and(|room| room.joined)
    {
      return;
    }
    match self.client().leave_room(name).await {
      // the server answers nothing for a room it does not have us in, so we are out either way
      Ok(()) | Err(ClientError::NoResponse(_)) => {}
      Err(e) => return debug!(room = name, error = %e, "room not left to join it again"),
    }
    // a leave meanwhile took the room off the tracker, and only a join, which waits for the turn
    // held here, tracks it again
    if !self.rooms.tracker().out_of(name) {
      return debug!(room = name, "room left while it was joined again");
    }
    match self.join_in_turn(name, false, false).await {
      Ok(_) => debug!(
        room = name,
        "room joined again after client events were lost"
      ),
      Err(ClientError::Canceled) => debug!(room = name, "room left while it was being joined"),
      Err(e) if self.session_over(session) => {
        debug!(room = name, error = %e, "room not joined again; the session ended");
      }
      Err(e) => {
        warn!(
          room = name,
          error = %e,
          "room not joined again after client events were lost; left for the user to join"
        );
        self.rooms.tracker().mark_refused(name);
        self.rooms_changed();
        self.push_rooms().await;
      }
    }
  }

  /// The configured rooms changed from `previous` to `current`: the rooms added are joined
  /// now while logged in; the rooms taken off are not left.
  pub fn configured_rooms_changed(&self, previous: &[String], current: &[String]) {
    let added: Vec<String> = current
      .iter()
      .filter(|room| !previous.contains(room))
      .cloned()
      .collect();
    if added.is_empty() {
      return;
    }
    info!(
      rooms = added.join(", "),
      "Room configuration changed.  Joining any newly added rooms."
    );
    if !self.logged_in_now() {
      return debug!("not logged in; the rooms added are joined at the next login");
    }
    let this = self.this();
    self.tasks.spawn(async move {
      for room in added {
        match this.join_now(&room, false, false).await {
          Ok(Some(_)) => info!(room, "room joined"),
          Ok(None) => debug!(room, "room joined already"),
          Err(ClientError::Canceled) => debug!(room, "room left while it was being joined"),
          Err(e) => warn!(room, error = %e, "room not joined"),
        }
      }
    });
  }

  /// Joins the room `request` names for the API. The join runs to its end on the engine's task
  /// set, so a request dropped while the server is asked leaves no join half done.
  pub(super) async fn join(&self, request: JoinRoom) -> Result<Joined, ApiError> {
    let this = self.this();
    let joining = self.tasks.spawn(
      async move { this.join_now(&request.name, request.private, true).await }.in_current_span(),
    );
    let room = joining
      .await
      .map_err(ApiError::internal)?
      .map_err(refused)?;
    Ok(room.map_or(Joined::Already, Joined::New))
  }

  pub(super) async fn leave(&self, room: &str) -> Result<(), ApiError> {
    self
      .leave_room(room)
      .instrument(info_span!("room", room))
      .await
  }

  async fn leave_room(&self, room: &str) -> Result<(), ApiError> {
    let joined = match self.rooms.tracker().get(room) {
      Some(tracked) => tracked.joined,
      None => return Err(not_in(room)),
    };
    // a room the lost connection took us out of is only forgotten
    if joined {
      match self.client().leave_room(room).await {
        Ok(()) => {}
        // the server answers a leave of a room it does not have us in with nothing, so we are
        // out of the room either way
        Err(ClientError::NoResponse(detail)) => {
          debug!(room, %detail, "the leave went unanswered; the room is taken for left");
        }
        Err(e) => return Err(unsent(e)),
      }
    }
    self.forgotten(room).await;
    Ok(())
  }

  /// Takes `room` off the rooms we are in and the ones joined again after a start.
  async fn forgotten(&self, room: &str) {
    self.rooms.tracker().left(room);
    self.rooms_changed();
    self.note_left(room).await;
    self.push_rooms().await;
  }

  /// Notes that we left `room`, so it is not joined again after a start.
  async fn note_left(&self, room: &str) {
    let room_name = room.to_string();
    let now = Utc::now();
    if let Err(e) = self
      .db
      .write(move |conn| history::left(conn, &room_name, now))
      .await
    {
      warn!(room, error = %e, "room leave not noted");
    }
  }

  pub(super) async fn send_room(&self, room: &str, message: String) -> Result<(), ApiError> {
    self.speaks_in(room)?;
    self
      .client()
      .send_room_message(room, &message)
      .await
      .map_err(unsent)
  }

  pub(super) async fn set_ticker(&self, room: &str, ticker: String) -> Result<(), ApiError> {
    self.speaks_in(room)?;
    self
      .client()
      .set_room_ticker(room, &ticker)
      .await
      .map_err(unsent)
  }

  /// Of the users with `tickers`, those whose room messages are hidden.
  pub(super) fn hidden_of<'a>(
    &self,
    tickers: impl IntoIterator<Item = &'a String>,
  ) -> BTreeSet<String> {
    tickers
      .into_iter()
      .filter(|username| self.users.is_blocked(username, None, BlockScope::Rooms))
      .cloned()
      .collect()
  }

  /// `room` with the tickers of users whose room messages are hidden marked hidden, as it is
  /// handed out; the room keeps who was marked, so a block that changes nothing pushes nothing.
  pub(super) fn marked(&self, room: Room) -> Room {
    self.marked_in(&mut self.rooms.tracker(), room)
  }

  /// [`Messaging::marked`] with the tracker held already.
  pub(super) fn marked_in(&self, tracker: &mut Tracker, mut room: Room) -> Room {
    let hidden = self.hidden_of(room.tickers.iter().map(|ticker| &ticker.username));
    for ticker in &mut room.tickers {
      ticker.hidden = hidden.contains(&ticker.username);
    }
    tracker.handed_out_hidden(&room.name, hidden);
    room
  }

  pub(super) fn tickers(&self, room: &str) -> Result<Vec<RoomTicker>, ApiError> {
    let tracked = self.rooms.tracker().room(room);
    tracked
      .map(|room| self.marked(room).tickers)
      .ok_or_else(|| not_in(room))
  }

  pub(super) async fn room(&self, name: &str) -> Result<Room, ApiError> {
    let tracked = self.rooms.tracker().room(name);
    let mut room = tracked
      .map(|room| self.marked(room))
      .ok_or_else(|| not_in(name))?;
    let query = RoomMessagesQuery {
      before: None,
      after: None,
      limit: LATEST,
      include_hidden: false,
    };
    let room_name = name.to_string();
    let (messages, _) = self
      .db
      .read(move |conn| history::page(conn, &room_name, &query))
      .await
      .map_err(internal)?;
    room.messages = Some(messages);
    Ok(room)
  }

  pub(super) fn room_users(&self, room: &str) -> Result<Vec<RoomUser>, ApiError> {
    let tracker = self.rooms.tracker();
    tracker
      .room(room)
      .map(|room| room.users)
      .ok_or_else(|| not_in(room))
  }

  pub(super) async fn joined_rooms(&self) -> Result<Vec<JoinedRoom>, ApiError> {
    let rooms: Vec<(String, u32, bool)> = self
      .rooms
      .tracker()
      .rooms()
      .iter()
      .map(|room| {
        (
          room.name.clone(),
          u32::try_from(room.users.len()).unwrap_or(u32::MAX),
          room.refused,
        )
      })
      .collect();
    self
      .db
      .read(move |conn| {
        rooms
          .into_iter()
          .map(|(name, user_count, refused)| {
            let unread = history::unread(conn, &name)?;
            Ok(JoinedRoom {
              notify: history::notify(conn, &name)?,
              last_message_at: history::newest(conn, &name)?,
              last_message: history::latest_shown(conn, &name)?,
              last_read_id: history::read_marker(conn, &name)?,
              unread: unread.unread,
              mentions: unread.mentions,
              user_count,
              refused,
              name,
            })
          })
          .collect()
      })
      .await
      .map_err(internal)
  }

  pub(super) async fn available_rooms(&self) -> Result<Vec<RoomInfo>, ApiError> {
    let list = {
      let mut cached = self.rooms.available.lock().await;
      match cached.as_ref().filter(|(at, _)| at.elapsed() < LIST_FOR) {
        Some((_, list)) => list.clone(),
        None => {
          // the answer is kept here at once, and once more as a list received
          let list = self.client().get_room_list().await.map_err(unsent)?;
          *cached = Some((Instant::now(), list.clone()));
          list
        }
      }
    };
    let tracker = self.rooms.tracker();
    let info = |room: &soulseek_proto::server::RoomInfo, private: bool, owned: bool| RoomInfo {
      name: room.name.clone(),
      user_count: room.user_count,
      is_private: private,
      is_owned: owned,
      is_moderated: list.moderated_room_names.contains(&room.name),
      joined: tracker.get(&room.name).is_some_and(|r| r.joined),
    };
    let mut rooms: Vec<RoomInfo> = list.public.iter().map(|r| info(r, false, false)).collect();
    rooms.extend(list.private.iter().map(|r| info(r, true, false)));
    rooms.extend(list.owned.iter().map(|r| info(r, true, true)));
    Ok(rooms)
  }

  pub(super) async fn mark_read(&self, room: &str, local_id: i64) -> Result<(), ApiError> {
    self.tracks(room)?;
    let name = room.to_string();
    self
      .db
      .write(move |conn| history::mark_read(conn, &name, local_id))
      .await
      .map_err(internal)?;
    self.unread.changed();
    Ok(())
  }

  pub(super) async fn set_room_notify(
    &self,
    room: &str,
    notify: NotifyLevel,
  ) -> Result<(), ApiError> {
    self.tracks(room)?;
    let name = room.to_string();
    self
      .db
      .write(move |conn| history::set_notify(conn, &name, notify))
      .await
      .map_err(internal)?;
    self.push_rooms().await;
    Ok(())
  }
}

#[cfg(test)]
mod tests {
  use soulseek_client::soulseek_proto;
  use std::sync::Arc;
  use std::sync::atomic::{AtomicBool, Ordering};

  use std::time::Duration;

  use api_types::messaging::rooms::HistoryGapReason;
  use serde_json::{Value, json};
  use soulseek_proto::RoomType;
  use soulseek_proto::server::{
    CannotJoinRoom, JoinRoomRequest, LeaveRoom, PrivateRoomData, PrivateRoomRemoved, RoomData,
    RoomInfo as Listed, RoomList, RoomTicker, RoomTickers,
  };
  use soulseek_proto::{ServerRequest, ServerResponse};
  use tokio::time::Instant;

  use soulseek_client::ClientError;
  use tokio_util::sync::CancellationToken;

  use super::{REJOIN_AGAIN, history};
  use crate::testing::soulseek::{self, Server, US, member};
  use crate::testing::{TRUSTED, TestApp, app};

  /// The server's answer that lets us into the room `join` asks for, where alice is.
  fn let_in(join: &JoinRoomRequest) -> ServerResponse {
    ServerResponse::JoinRoom(RoomData {
      name: join.room.clone(),
      users: vec![member(US), member("alice")],
      private: None,
    })
  }

  fn joins(room: &'static str) -> impl Fn(&ServerRequest) -> Option<()> {
    move |request| {
      matches!(request, ServerRequest::JoinRoom(join) if join.room == room).then_some(())
    }
  }

  /// Joins `room` through the API once the server took the request.
  async fn join(app: &TestApp, server: &mut Server, room: &'static str) -> Value {
    let request = Some(json!({ "name": room }));
    let (status, joined) = app.call("POST", "/api/v0/rooms/joined", request).await;
    assert_eq!(status, 201, "{joined}");
    server.received(joins(room)).await;
    joined
  }

  /// The names `GET /rooms/joined` lists.
  async fn listed(app: &TestApp) -> Vec<String> {
    let (status, rooms) = app.call("GET", "/api/v0/rooms/joined", None).await;
    assert_eq!(status, 200);
    let rooms = rooms.as_array().unwrap().iter();
    rooms
      .map(|room| room["name"].as_str().unwrap().to_string())
      .collect()
  }

  /// A daemon in the room `lobby` whose session was lost and that logged in again to a server
  /// that does not answer its join of the room, which is on its way.
  async fn back_without_the_room() -> (TestApp, Server) {
    let app = soulseek::app(TRUSTED).await;
    let answering = Arc::new(AtomicBool::new(true));
    let answers = answering.clone();
    let mut server = Server::start(move |request| match request {
      ServerRequest::JoinRoom(join) if answers.load(Ordering::SeqCst) => vec![let_in(join)],
      ServerRequest::LeaveRoom(leave) => vec![ServerResponse::LeaveRoom(leave.clone())],
      _ => Vec::new(),
    })
    .await;
    server.login(&app).await;
    join(&app, &mut server, "lobby").await;
    answering.store(false, Ordering::SeqCst);
    server.drop_session(&app).await;
    server.login(&app).await;
    server.received(joins("lobby")).await;
    (app, server)
  }

  #[tokio::test]
  async fn a_message_to_a_room_not_joined_again_since_the_reconnect_is_refused() {
    let (app, _server) = back_without_the_room().await;
    let said = Some(json!("hello"));
    let (status, _) = app
      .call("POST", "/api/v0/rooms/joined/lobby/messages", said)
      .await;
    assert!(app.state.network.client().state().is_logged_in());
    assert_ne!(status, 201, "the server has us in no room to say it in");
  }

  /// What `GET /rooms/joined` says of `room`: whether it is listed and whether it is refused.
  async fn standing(app: &TestApp, room: &str) -> Option<bool> {
    let (status, rooms) = app.call("GET", "/api/v0/rooms/joined", None).await;
    assert_eq!(status, 200);
    let rooms = rooms.as_array().unwrap();
    let listed = rooms.iter().find(|listed| listed["name"] == room)?;
    Some(listed["refused"].as_bool().unwrap())
  }

  /// A daemon in the room `lobby` whose session was lost and that logged in again to a server
  /// that refuses joins while the flag returned is set, as it is now.
  async fn refused_after_a_reconnect() -> (TestApp, Server, Arc<AtomicBool>) {
    let app = soulseek::app(TRUSTED).await;
    let refusing = Arc::new(AtomicBool::new(false));
    let refuses = refusing.clone();
    let mut server = Server::start(move |request| match request {
      ServerRequest::JoinRoom(join) if refuses.load(Ordering::SeqCst) => {
        vec![ServerResponse::CannotJoinRoom(CannotJoinRoom {
          room: join.room.clone(),
        })]
      }
      ServerRequest::JoinRoom(join) => vec![let_in(join)],
      _ => Vec::new(),
    })
    .await;
    server.login(&app).await;
    join(&app, &mut server, "lobby").await;
    refusing.store(true, Ordering::SeqCst);
    server.drop_session(&app).await;
    server.login(&app).await;
    (app, server, refusing)
  }

  /// What the tries after a login's first do with `lobby`, refused at the first, when every try
  /// refuses it and the session ends after `ends`, if ever: when each try came, by the clock.
  async fn tried_again(ends: Option<Duration>) -> (TestApp, Vec<Duration>) {
    let app = app(TRUSTED, &[]).await;
    let messaging = app.state.messaging.clone();
    let leaves = messaging.rooms.tracker().leaves("lobby");
    messaging.rooms.tracker().keep("lobby", leaves);
    let session = CancellationToken::new();
    if let Some(ends) = ends {
      let ending = session.clone();
      tokio::spawn(async move {
        tokio::time::sleep(ends).await;
        ending.cancel();
      });
    }
    let started = Instant::now();
    let mut tries = Vec::new();
    let refused = |rooms: Vec<String>| -> Vec<(String, ClientError)> {
      let refusal = |room| (room, ClientError::NoResponse("refused".into()));
      rooms.into_iter().map(refusal).collect()
    };
    let again = |rooms| {
      tries.push(started.elapsed());
      std::future::ready(refused(rooms))
    };
    let first = refused(vec!["lobby".to_string()]);
    messaging.try_again(first, &session, again).await;
    (app, tries)
  }

  // the clock stands still while anything runs, so the waits are the shipped ones
  #[tokio::test(start_paused = true)]
  async fn a_room_refused_after_a_login_is_tried_twice_more_then_left_to_join() {
    let (app, tries) = tried_again(None).await;
    let ten = REJOIN_AGAIN[0];
    assert_eq!(tries, [ten, ten + REJOIN_AGAIN[1]]);
    assert_eq!(standing(&app, "lobby").await, Some(true), "shown refused");
    let (status, _) = app
      .call("GET", "/api/v0/rooms/joined/lobby/messages", None)
      .await;
    assert_eq!(status, 200, "its history stays readable");
  }

  #[tokio::test(start_paused = true)]
  async fn the_tries_after_a_login_stop_when_its_session_ends() {
    let ends = REJOIN_AGAIN[0] + Duration::from_secs(5);
    let (app, tries) = tried_again(Some(ends)).await;
    assert_eq!(tries, [REJOIN_AGAIN[0]], "no try after the session ended");
    assert_eq!(
      standing(&app, "lobby").await,
      Some(false),
      "not marked refused"
    );
  }

  #[tokio::test]
  async fn a_room_refused_after_a_reconnect_stays_listed_until_the_user_joins_it() {
    let (app, mut server, refusing) = refused_after_a_reconnect().await;
    server.received(joins("lobby")).await;
    tokio::time::timeout(Duration::from_secs(5), async {
      while standing(&app, "lobby").await != Some(false) {
        tokio::time::sleep(Duration::from_millis(10)).await;
      }
    })
    .await
    .expect("the room refused was not kept");
    assert_eq!(app.state.state.current().rooms, Vec::<String>::new());
    let (status, _) = app
      .call("GET", "/api/v0/rooms/joined/lobby/messages", None)
      .await;
    assert_eq!(status, 200, "its history stays readable");

    // the user joins it again
    refusing.store(false, Ordering::SeqCst);
    join(&app, &mut server, "lobby").await;
    assert_eq!(standing(&app, "lobby").await, Some(false));
    assert_eq!(app.state.state.current().rooms, ["lobby"]);
  }

  /// The kind of room each join of `room` asks for.
  fn joins_as(room: &'static str) -> impl Fn(&ServerRequest) -> Option<RoomType> {
    move |request| match request {
      ServerRequest::JoinRoom(join) if join.room == room => Some(join.room_type),
      _ => None,
    }
  }

  /// The server's refusal of the join `join` asks for.
  fn refuse(join: &JoinRoomRequest) -> ServerResponse {
    ServerResponse::CannotJoinRoom(CannotJoinRoom {
      room: join.room.clone(),
    })
  }

  #[tokio::test]
  async fn a_private_room_is_joined_as_private_after_a_start_and_a_refusal_leaves_it_not_joined() {
    let app = soulseek::app(TRUSTED).await;
    // an earlier run was let into the private room club through the UI
    app
      .state
      .messaging
      .database()
      .write(|conn| history::joined(conn, "club", chrono::Utc::now(), true, true))
      .await
      .unwrap();
    // someone else's private room of that name by now
    let mut server = Server::start(|request| match request {
      ServerRequest::JoinRoom(join) => vec![refuse(join)],
      _ => Vec::new(),
    })
    .await;
    server.login(&app).await;
    let asked = server.received(joins_as("club")).await;
    assert_eq!(asked, RoomType::Private, "joined as it was last joined");
    tokio::time::timeout(Duration::from_secs(5), async {
      while standing(&app, "club").await.is_none() {
        tokio::time::sleep(Duration::from_millis(10)).await;
      }
    })
    .await
    .expect("the room refused was not kept");
    // the Join button asks for the room as the UI knows it, a public one
    let request = Some(json!({ "name": "club" }));
    let (status, _) = app.call("POST", "/api/v0/rooms/joined", request).await;
    assert_eq!(status, 403);
    assert_eq!(server.received(joins_as("club")).await, RoomType::Private);
    assert_eq!(
      standing(&app, "club").await,
      Some(false),
      "listed, not joined"
    );
    assert_eq!(app.state.state.current().rooms, Vec::<String>::new());
  }

  #[tokio::test]
  async fn a_room_is_joined_again_as_the_server_last_let_us_in() {
    let app = soulseek::app(TRUSTED).await;
    let private = Arc::new(AtomicBool::new(true));
    let answers = private.clone();
    let mut server = Server::start(move |request| match request {
      ServerRequest::JoinRoom(join) => {
        let mut data = RoomData {
          name: join.room.clone(),
          users: vec![member(US), member("alice")],
          private: None,
        };
        if answers.load(Ordering::SeqCst) {
          data.private = Some(PrivateRoomData {
            owner: "alice".into(),
            operators: Vec::new(),
          });
        }
        vec![ServerResponse::JoinRoom(data)]
      }
      _ => Vec::new(),
    })
    .await;
    server.login(&app).await;
    // asked for as public, let in as alice's private room
    join(&app, &mut server, "club").await;
    // she gave it up since, and the server lets us in as a public room
    private.store(false, Ordering::SeqCst);
    server.drop_session(&app).await;
    server.login(&app).await;
    assert_eq!(server.received(joins_as("club")).await, RoomType::Private);
    server.drop_session(&app).await;
    server.login(&app).await;
    assert_eq!(server.received(joins_as("club")).await, RoomType::Public);
  }

  #[tokio::test]
  async fn the_rooms_are_joined_again_after_client_events_were_lost() {
    let app = soulseek::app(TRUSTED).await;
    // who is in lobby besides us, and their tickers
    let here = Arc::new(std::sync::Mutex::new(vec!["alice"]));
    let present = here.clone();
    let mut server = Server::start(move |request| match request {
      ServerRequest::JoinRoom(join) => {
        let here = present.lock().unwrap().clone();
        let mut users = vec![member(US)];
        users.extend(here.iter().map(|name| member(name)));
        let tickers = here
          .iter()
          .map(|name| RoomTicker {
            username: name.to_string(),
            message: format!("{name} here"),
          })
          .collect();
        vec![
          ServerResponse::JoinRoom(RoomData {
            name: join.room.clone(),
            users,
            private: None,
          }),
          ServerResponse::RoomTickers(RoomTickers {
            room: join.room.clone(),
            tickers,
          }),
        ]
      }
      ServerRequest::LeaveRoom(leave) => vec![ServerResponse::LeaveRoom(leave.clone())],
      _ => Vec::new(),
    })
    .await;
    server.login(&app).await;
    join(&app, &mut server, "lobby").await;
    // alice left and bob came, which the events lost said
    *here.lock().unwrap() = vec!["bob"];
    let messaging = &app.state.messaging;
    messaging.missed().await;
    let leaves = |request: &ServerRequest| {
      matches!(request, ServerRequest::LeaveRoom(leave) if leave.room == "lobby").then_some(())
    };
    server.received(leaves).await;
    server.received(joins("lobby")).await;
    let joined_at = chrono::Utc::now();
    let read = tokio::time::timeout(Duration::from_secs(5), async {
      loop {
        let room = messaging.rooms.tracker().room("lobby").unwrap();
        let users: Vec<String> = room.users.into_iter().map(|u| u.username).collect();
        let tickers: Vec<String> = room.tickers.into_iter().map(|t| t.ticker).collect();
        if users == ["bob", US] && tickers == ["bob here"] {
          break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
      }
    })
    .await;
    assert!(
      read.is_ok(),
      "{:?}",
      messaging.rooms.tracker().room("lobby")
    );
    // the gap says why, and lasts until the room was ours again
    let gap = tokio::time::timeout(Duration::from_secs(5), async {
      loop {
        let gaps = messaging.history_gaps().await;
        if let Some(gap) = gaps.last().filter(|gap| gap.to > Some(joined_at)) {
          break *gap;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
      }
    })
    .await
    .expect("the gap does not reach past the join");
    assert_eq!(gap.reason, HistoryGapReason::FellBehind);
    assert_eq!(app.state.state.current().rooms, ["lobby"]);
  }

  /// Waits until `logs` hold a line saying `said`.
  async fn logged(logs: &crate::logging::LogBuffer, said: &str) {
    tokio::time::timeout(Duration::from_secs(15), async {
      while !logs
        .snapshot()
        .iter()
        .any(|line| line.message.contains(said))
      {
        tokio::time::sleep(Duration::from_millis(10)).await;
      }
    })
    .await
    .unwrap_or_else(|_| panic!("never logged: {said}"));
  }

  #[tokio::test]
  async fn a_room_refused_after_a_reconnect_waits_for_its_next_try() {
    let (logs, _capturing) = crate::testing::captured_logs();
    let (_app, mut server, _refusing) = refused_after_a_reconnect().await;
    server.received(joins("lobby")).await;
    // the tries after the first, whose waits run as shipped in the tests on a paused clock
    logged(
      &logs,
      "rooms not joined after the login; trying again after a wait",
    )
    .await;
  }

  /// What the server does with the joins and leaves of each room, as a test sets it.
  #[derive(Default)]
  struct Rooms {
    /// rooms whose joins go unanswered
    joins_held: Vec<&'static str>,
    /// rooms whose leaves go unanswered
    leaves_held: Vec<&'static str>,
    /// rooms whose joins are refused
    refused: Vec<&'static str>,
  }

  /// A daemon logged in and in `rooms`, on a server that lets us into a room and answers its
  /// leave unless the [`Rooms`] returned says otherwise.
  async fn in_rooms(rooms: &[&'static str]) -> (TestApp, Server, Arc<std::sync::Mutex<Rooms>>) {
    let app = soulseek::app(TRUSTED).await;
    let script = Arc::new(std::sync::Mutex::new(Rooms::default()));
    let says = script.clone();
    let mut server = Server::start(move |request| {
      let says = says.lock().unwrap();
      match request {
        ServerRequest::JoinRoom(join) if says.refused.contains(&join.room.as_str()) => {
          vec![refuse(join)]
        }
        ServerRequest::JoinRoom(join) if says.joins_held.contains(&join.room.as_str()) => {
          Vec::new()
        }
        ServerRequest::JoinRoom(join) => vec![let_in(join)],
        ServerRequest::LeaveRoom(leave) if says.leaves_held.contains(&leave.room.as_str()) => {
          Vec::new()
        }
        ServerRequest::LeaveRoom(leave) => vec![ServerResponse::LeaveRoom(leave.clone())],
        _ => Vec::new(),
      }
    })
    .await;
    server.login(&app).await;
    for room in rooms {
      join(&app, &mut server, room).await;
    }
    (app, server, script)
  }

  fn leaves(room: &'static str) -> impl Fn(&ServerRequest) -> Option<()> {
    move |request| {
      matches!(request, ServerRequest::LeaveRoom(leave) if leave.room == room).then_some(())
    }
  }

  #[tokio::test]
  async fn a_second_loss_stops_the_first_ones_joins_between_two_rooms() {
    let (app, mut server, script) = in_rooms(&["a", "b"]).await;
    script.lock().unwrap().joins_held.push("a");
    let messaging = &app.state.messaging;
    messaging.missed().await;
    server.received(joins("a")).await;
    // more is lost while the first reading waits on a's join; the second reading finds a out of
    // the rooms we are in, and reads b alone
    messaging.missed().await;
    tokio::time::timeout(Duration::from_secs(5), async {
      while messaging.history_gaps().await.len() < 2 {
        tokio::time::sleep(Duration::from_millis(10)).await;
      }
    })
    .await
    .expect("the second loss was never marked");
    server.received(joins("b")).await;
    let answer = JoinRoomRequest {
      room: "a".into(),
      room_type: RoomType::Public,
    };
    server.send(vec![let_in(&answer)]);
    let b = |request: &ServerRequest| joins("b")(request).or(leaves("b")(request)).is_some();
    let touched_b = server.receives(Duration::from_secs(1), b).await;
    assert!(!touched_b, "the first reading went on to b");
  }

  #[tokio::test]
  async fn a_room_dropped_while_its_leave_is_answered_is_not_joined_again() {
    let (app, mut server, script) = in_rooms(&["club"]).await;
    script.lock().unwrap().leaves_held.push("club");
    app.state.messaging.missed().await;
    server.received(leaves("club")).await;
    // we are taken out of the private room meanwhile
    server.send(vec![ServerResponse::PrivateRoomRemoved(
      PrivateRoomRemoved {
        room: "club".into(),
      },
    )]);
    tokio::time::timeout(Duration::from_secs(5), async {
      while standing(&app, "club").await.is_some() {
        tokio::time::sleep(Duration::from_millis(10)).await;
      }
    })
    .await
    .expect("the room was never dropped");
    server.send(vec![ServerResponse::LeaveRoom(LeaveRoom {
      room: "club".into(),
    })]);
    let joined = server
      .receives(Duration::from_secs(1), |request| {
        joins("club")(request).is_some()
      })
      .await;
    assert!(!joined, "joined again after it was dropped");
    assert_eq!(standing(&app, "club").await, None);
  }

  #[tokio::test]
  async fn a_read_cut_short_by_the_sessions_end_is_left_to_the_next_login() {
    let (logs, _capturing) = crate::testing::captured_logs();
    let (app, mut server, script) = in_rooms(&["lobby"]).await;
    script.lock().unwrap().joins_held.push("lobby");
    app.state.messaging.missed().await;
    server.received(joins("lobby")).await;
    server.drop_session(&app).await;
    logged(&logs, "room not joined again; the session ended").await;
    let refused = logs
      .snapshot()
      .into_iter()
      .find(|line| line.message.contains("left for the user to join"));
    assert!(refused.is_none(), "{refused:?}");
    assert_eq!(
      standing(&app, "lobby").await,
      Some(false),
      "not shown refused"
    );
  }

  #[tokio::test]
  async fn a_room_refused_when_it_is_read_again_is_shown_refused() {
    let (app, _server, script) = in_rooms(&["lobby"]).await;
    script.lock().unwrap().refused.push("lobby");
    app.state.messaging.missed().await;
    tokio::time::timeout(Duration::from_secs(5), async {
      while standing(&app, "lobby").await != Some(true) {
        tokio::time::sleep(Duration::from_millis(10)).await;
      }
    })
    .await
    .expect("the room was never shown refused");
    assert_eq!(app.state.state.current().rooms, Vec::<String>::new());
  }

  #[tokio::test]
  async fn a_room_whose_leave_the_server_never_answers_is_listed_no_more() {
    let app = soulseek::app(TRUSTED).await;
    let mut server = Server::start(|request| match request {
      ServerRequest::JoinRoom(join) => vec![let_in(join)],
      _ => Vec::new(),
    })
    .await;
    server.login(&app).await;
    join(&app, &mut server, "lobby").await;
    app.call("DELETE", "/api/v0/rooms/joined/lobby", None).await;
    assert_eq!(listed(&app).await, Vec::<String>::new());
    assert_eq!(app.state.state.current().rooms, Vec::<String>::new());
  }

  #[tokio::test]
  async fn a_join_whose_request_is_dropped_holds_nothing_for_the_room_once_it_ends() {
    let app = soulseek::app(TRUSTED).await;
    let mut server = Server::start(|_| Vec::new()).await;
    server.login(&app).await;
    let request = Some(json!({ "name": "lobby" }));
    tokio::select! {
      (status, _) = app.call("POST", "/api/v0/rooms/joined", request) => {
        panic!("answered {status} before the server did");
      }
      () = server.received(joins("lobby")) => {}
    }
    // the request is gone; the server refuses the join it sent
    server.send(vec![ServerResponse::CannotJoinRoom(CannotJoinRoom {
      room: "lobby".into(),
    })]);
    let messaging = &app.state.messaging;
    let ended = tokio::time::timeout(std::time::Duration::from_secs(5), async {
      while messaging.rooms.tracker().hears("lobby") {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
      }
    })
    .await;
    assert!(ended.is_ok(), "the join left the room heard");
    assert!(messaging.rooms.tracker().get("lobby").is_none());
  }

  #[tokio::test]
  async fn a_room_list_the_server_sends_on_its_own_is_served_without_asking() {
    // the daemon is not connected, so a list it had to ask for would be refused
    let app = app("", &[]).await;
    let messaging = &app.state.messaging;
    let listed = |name: &str| Listed {
      name: name.into(),
      user_count: 1,
    };
    let list = RoomList {
      public: vec![listed("indie")],
      owned: vec![listed("ours")],
      private: Vec::new(),
      moderated_room_names: Vec::new(),
    };
    messaging.room_list_received(list.into()).await;
    let rooms = messaging.available_rooms().await.unwrap();
    let shown: Vec<(&str, bool)> = rooms
      .iter()
      .map(|room| (room.name.as_str(), room.is_owned))
      .collect();
    assert_eq!(shown, [("indie", false), ("ours", true)]);
  }
}
