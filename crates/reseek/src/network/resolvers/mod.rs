//! How we answer what other users ask of us: an upload queued, our share listing, a folder's
//! contents, a search, our user info, a file's place in our upload queue and a file they send
//! unasked. Each answer passes the checks at the network's edge first: a name with a `.` or `..`
//! part is refused, and a user blocked from what they ask for gets what a blocked user gets.
//! Listings, folders and searches are answered from what we share; uploads, places in the upload
//! queue and the queue figures from the upload engine; files sent unasked by the download
//! engine.

mod browse;
mod search;

use soulseek_client::soulseek_proto;
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use api_types::activity::ActivityKind;
use api_types::users::blocking::BlockScope;
use cascade::Watcher;
use reseek_shares::LogLimiter;
use reseek_shares::{lock, traverses};
use soulseek_client::{ClientError, Peer, Resolvers, UploadAsk, UploadDecision};
use soulseek_proto::peer::UserInfo;
use soulseek_proto::{RemoteFilename, SearchQuery};
use tracing::{debug, info, warn};

pub use browse::{browse, directory_contents};
pub use search::{IncomingSearches, search};

use super::Network;
use crate::activity::Activity;
use crate::config::Options;
use crate::shares::Shares;
use crate::telemetry::Telemetry;
use crate::transfers::downloads::Downloads;
use crate::transfers::uploads::{ENQUEUE_FAILED, NotQueued, QUEUE_FULL, Uploads};
pub use crate::transfers::uploads::{Forecast, NOT_SHARED, OVERWHELMED};
use crate::users::{Directory as Users, upload_permitted};

/// Requests with a `.` or `..` part, which a peer could send at any rate.
static TRAVERSING: LogLimiter = LogLimiter::new();
/// Requests from blocked users, likewise.
static BLOCKED: LogLimiter = LogLimiter::new();

/// What the answers are made from.
pub struct Edges {
  /// who is blocked from what
  users: Arc<Users>,
  /// where what other users ask of us is noted
  activity: Arc<Activity>,
  /// what we share
  shares: Arc<Shares>,
  /// the options, read once per request
  config: Watcher<Options>,
  /// the metrics the answers are counted in
  telemetry: Arc<Telemetry>,
  /// the network the answers go out on, for the requester's address and our own figures
  network: Weak<Network>,
  /// the workers, the waiting count and the request filters of other users' searches
  pub(super) searches: IncomingSearches,
  /// the browse caches being written on a browse, one lock per view, so browses of a view
  /// that find its cache missing or behind write it once and other views' browses never wait
  rebuilding: browse::Rewrites,
  /// the upload engine, attached once while the daemon is built
  uploads: OnceLock<Arc<Uploads>>,
  /// the download engine, attached with the upload engine
  downloads: OnceLock<Arc<Downloads>>,
  /// the users each of our running searches of users asks, by token
  user_searches: Mutex<HashMap<u32, Vec<String>>>,
}

impl Edges {
  /// The answers from `users`, `shares` and `config`, counted in `telemetry` and sent through
  /// `network`, with `searches` working out the searches.
  pub(super) fn new(
    users: Arc<Users>,
    activity: Arc<Activity>,
    shares: Arc<Shares>,
    config: Watcher<Options>,
    telemetry: Arc<Telemetry>,
    network: Weak<Network>,
    searches: IncomingSearches,
  ) -> Self {
    Self {
      users,
      activity,
      shares,
      config,
      telemetry,
      network,
      searches,
      rebuilding: browse::Rewrites::default(),
      uploads: OnceLock::new(),
      downloads: OnceLock::new(),
      user_searches: Mutex::default(),
    }
  }

  /// Answers upload requests, places in the queue and the queue figures with `uploads`, and
  /// files sent unasked with `downloads`, from now on; done once, while the daemon is built.
  pub(super) fn attach(&self, uploads: Arc<Uploads>, downloads: Arc<Downloads>) {
    assert!(
      self.uploads.set(uploads).is_ok() && self.downloads.set(downloads).is_ok(),
      "the transfer engines are attached once"
    );
  }

  /// Whether a search in `username`'s name is one in our own name that we did not send to
  /// ourselves: our own searches come back through the server and the distributed parent, and
  /// only one we sent to users we are among is answered.
  fn own_unasked(&self, username: &str, token: u32) -> bool {
    let own = self
      .network
      .upgrade()
      .and_then(|network| network.client().username());
    if own.as_deref() != Some(username) {
      return false;
    }
    !lock(&self.user_searches)
      .get(&token)
      .is_some_and(|users| users.iter().any(|user| user == username))
  }

  /// Knows the search `token` of ours as one asking `users` until the returned guard drops.
  pub(super) fn user_search(self: &Arc<Self>, token: u32, users: Vec<String>) -> UserSearch {
    lock(&self.user_searches).insert(token, users);
    UserSearch {
      edges: self.clone(),
      token,
    }
  }

  fn uploads(&self) -> &Uploads {
    self
      .uploads
      .get()
      .expect("the upload engine is attached while the daemon is built, before peers reach it")
  }
}

/// A running search of ours that asks users, known to the answers while it lives.
pub struct UserSearch {
  edges: Arc<Edges>,
  token: u32,
}

impl Drop for UserSearch {
  fn drop(&mut self) {
    lock(&self.edges.user_searches).remove(&self.token);
  }
}

/// The resolvers the client answers peers with.
pub fn resolvers(edges: Arc<Edges>) -> Resolvers {
  let receiving = edges.clone();
  let enqueue = edges.clone();
  let browsing = edges.clone();
  let folder = edges.clone();
  let searching = edges.clone();
  let info = edges.clone();
  let place = edges;
  // a peer's `confirmed` is not read: whether a request counts as its user's is decided by
  // `Directory::confirms` alone, which asks the server for the address at the time
  Resolvers {
    upload_request: Some(Arc::new(move |peer: Peer, ask: UploadAsk| {
      let edges = enqueue.clone();
      Box::pin(async move {
        let queued = enqueue_download(&edges, &peer.username, peer.endpoint, &ask.filename).await;
        Ok(decision(queued, &ask.filename))
      })
    })),
    browse_response: Some(Arc::new(move |peer: Peer| {
      let edges = browsing.clone();
      Box::pin(async move { Ok(Some(browse(&edges, &peer.username, peer.endpoint).await)) })
    })),
    folder_contents: Some(Arc::new(move |peer: Peer, _token, directory| {
      let edges = folder.clone();
      Box::pin(async move {
        Ok(directory_contents(&edges, &peer.username, peer.endpoint, &directory).await)
      })
    })),
    search_response: Some(Arc::new(move |username, token, text| {
      let edges = searching.clone();
      Box::pin(
        async move { Ok(search(&edges, &username, token, &SearchQuery::parse(&text)).await) },
      )
    })),
    user_info: Some(Arc::new(move |peer: Peer| {
      let edges = info.clone();
      Box::pin(async move { Ok(Some(user_info(&edges, &peer.username, peer.endpoint).await)) })
    })),
    place_in_queue: Some(Arc::new(move |peer: Peer, filename| {
      let edges = place.clone();
      Box::pin(
        async move { Ok(place_in_queue(&edges, &peer.username, peer.endpoint, &filename).await) },
      )
    })),
    unasked_offer: Some(Arc::new(move |peer: Peer, filename, size| {
      let edges = receiving.clone();
      Box::pin(async move {
        receive_offer(&edges, &peer.username, peer.endpoint, &filename, size)
          .await
          .map_err(not_taken)
      })
    })),
    // every relayed request is dialed, the parent nearest the top of its branch is taken and
    // every child is let in while a slot is free
    connection_request: None,
    distributed_parent: None,
    distributed_child: None,
  }
}

/// What the sender of a file offered unasked is told when it is refused, whatever the reason.
const NOT_TAKEN: &str = "Cancelled";

/// The error a file offered unasked is refused with: our reason is logged here and the sender is
/// told [`NOT_TAKEN`]. Any other error the client logs and answers the same way.
fn not_taken(error: ClientError) -> ClientError {
  match error {
    ClientError::TransferRejected(reason) => {
      debug!(reason, "file offered unasked refused");
      ClientError::TransferRejected(NOT_TAKEN.into())
    }
    other => other,
  }
}

/// What the peer is told of its request for `filename`, as `queued` went: a failure here is
/// logged and refused with [`ENQUEUE_FAILED`].
fn decision(queued: Result<(), NotQueued>, filename: &RemoteFilename) -> UploadDecision {
  match queued {
    Ok(()) => UploadDecision::Queue,
    Err(NotQueued::Refused(reason)) => UploadDecision::Deny(reason),
    Err(NotQueued::Later) => UploadDecision::Later,
    Err(NotQueued::Failed(error)) => {
      warn!(%filename, %error, "upload request resolver failed");
      UploadDecision::Deny(ENQUEUE_FAILED.into())
    }
  }
}

/// Logs, at most once a minute with a count, a request with a `.` or `..` part.
fn suspicious(username: &str, what: &str, name: &str) {
  if let Some(suppressed) = TRAVERSING.allow() {
    warn!(
      username,
      what, name, suppressed, "Suspicious request from a user containing unsafe path segments"
    );
  }
}

/// Logs, at most once a minute with a count, a request refused because `username` is blocked
/// from `scope`.
fn log_refused(username: &str, scope: BlockScope) {
  if let Some(suppressed) = BLOCKED.allow() {
    info!(username, %scope, suppressed, "request from a blocked user refused");
  }
}

/// Whether `username` from `endpoint` is blocked from `scope`, logged at most once a minute
/// with a count when they are.
fn blocked(edges: &Edges, username: &str, endpoint: Option<SocketAddr>, scope: BlockScope) -> bool {
  let blocked = edges
    .users
    .is_blocked(username, endpoint.map(|e| e.ip()), scope);
  if blocked {
    log_refused(username, scope);
  }
  blocked
}

/// A peer asks us to queue an upload of `filename`: a name with a `.` or `..` part is refused,
/// a request finding too many waiting to be decided is dropped, one from a new user is refused
/// when the queue holds as many users as it takes, overall or from the peer's address, and a
/// user blocked from uploads is told `blocking.refusal_message`; the upload engine decides the
/// rest.
pub async fn enqueue_download(
  edges: &Edges,
  username: &str,
  endpoint: SocketAddr,
  filename: &RemoteFilename,
) -> Result<(), NotQueued> {
  let uploads = edges.uploads();
  uploads.request_received();
  if traverses(filename.as_str()) {
    suspicious(username, "enqueue", filename.as_str());
    uploads.request_refused();
    return Err(NotQueued::Refused(NOT_SHARED.into()));
  }
  if uploads.overwhelmed(username) {
    return Err(NotQueued::Refused(OVERWHELMED.into()));
  }
  if uploads.queue_full(username, endpoint.ip()) {
    return Err(NotQueued::Refused(QUEUE_FULL.into()));
  }
  if blocked(edges, username, Some(endpoint), BlockScope::Uploads) {
    uploads.request_refused();
    let refusal = edges
      .config
      .current()
      .values
      .blocking
      .refusal_message
      .clone();
    return Err(NotQueued::Refused(refusal));
  }
  let sees = edges.users.sees_from(username, endpoint.ip()).await;
  uploads
    .request(username, endpoint.ip(), filename.as_str(), sees)
    .await?;
  uploads.queued_from(username, endpoint.ip());
  edges.activity.queued(username, filename.as_str());
  Ok(())
}

/// A peer offers to send us `filename` unasked: a name with a `.` or `..` part and a user blocked
/// from downloads are refused, and the download engine decides the rest. The client logs the
/// refusal.
pub async fn receive_offer(
  edges: &Edges,
  username: &str,
  endpoint: SocketAddr,
  filename: &RemoteFilename,
  size: u64,
) -> Result<(), ClientError> {
  if traverses(filename.as_str()) {
    return Err(ClientError::TransferRejected(
      "the file name has a . or .. part".into(),
    ));
  }
  if edges
    .users
    .is_blocked(username, Some(endpoint.ip()), BlockScope::Downloads)
  {
    return Err(ClientError::TransferRejected(
      "the sender is blocked".into(),
    ));
  }
  edges
    .downloads
    .get()
    .expect("the download engine is attached while the daemon is built, before peers reach it")
    .receive(username, endpoint.ip(), filename.as_str(), size)
    .await
}

/// The forecast for a new upload request from `username`, by the group known for them now.
pub fn forecast(edges: &Edges, username: &str) -> Forecast {
  edges.uploads().forecast(username)
}

/// A peer asks for our user info: a blocked user is shown no free slot and the longest queue.
pub async fn user_info(edges: &Edges, username: &str, endpoint: SocketAddr) -> UserInfo {
  if blocked(edges, username, Some(endpoint), BlockScope::Uploads) {
    let options = edges.config.current();
    let local = crate::profile::local_info(&options.values).await;
    return UserInfo {
      description: local.description,
      picture: local.picture,
      upload_slots: 0,
      queue_length: i32::MAX as u32,
      has_free_upload_slot: false,
      upload_permitted: Some(upload_permitted(&options.values)),
    };
  }
  edges.activity.record(Some(username), ActivityKind::Profile);
  profile(edges, username, endpoint.ip()).await
}

/// Our user info: our description and picture, who may send us files, and the slots of the
/// group `username` would be queued in and the place they would take, or a stranger's when the
/// request from `from` does not count as theirs. Their group is asked of the server when nothing
/// is known of them, so a leecher is known as one.
async fn profile(edges: &Edges, username: &str, from: IpAddr) -> UserInfo {
  let options = edges.config.current();
  let local = crate::profile::local_info(&options.values).await;
  let forecast = match edges.network.upgrade() {
    Some(network) => {
      let users = &edges.users;
      let placement = users
        .fetch_placement(network.client(), username, false)
        .await;
      let placement = if users.counts_as(username, &placement, from).await {
        placement
      } else {
        users.stranger()
      };
      debug!(username, queue = placement.queue, "user info asked for");
      edges.uploads().forecast_in(&placement.queue)
    }
    // the network goes only when the daemon stops
    None => forecast(edges, username),
  };
  UserInfo {
    description: local.description,
    picture: local.picture,
    upload_slots: forecast.total_slots,
    queue_length: forecast.queue_length,
    has_free_upload_slot: forecast.free_slots > 0,
    upload_permitted: Some(upload_permitted(&options.values)),
  }
}

/// A peer asks for its place in our upload queue: a blocked user is not answered, nor a file
/// the queue does not hold, nor a request that does not count as the user's, since the queue
/// holds nothing of a stranger's under their name.
pub async fn place_in_queue(
  edges: &Edges,
  username: &str,
  endpoint: SocketAddr,
  filename: &RemoteFilename,
) -> Option<u32> {
  if blocked(edges, username, Some(endpoint), BlockScope::Uploads) {
    return None;
  }
  let users = &edges.users;
  if !users
    .counts_as(username, &users.placement(username), endpoint.ip())
    .await
  {
    return None;
  }
  edges.uploads().place(username, filename.as_str())
}

#[cfg(test)]
pub(crate) mod tests {
  use super::*;
  use crate::testing::{TestApp, app, request, sent_jobs};
  use serde_json::json;

  const CONFIG: &str = "[web.authentication]\ntrusted_networks = [\"127.0.0.0/8\"]\n\
                        [transfers.groups.blacklisted]\ncidrs = [\"192.0.2.0/24\"]\n";

  pub(crate) fn from(ip: &str) -> SocketAddr {
    format!("{ip}:2234").parse().unwrap()
  }

  fn name(text: &str) -> RemoteFilename {
    RemoteFilename::from(text)
  }

  /// The answers of the daemon `app`.
  pub(crate) fn edges(app: &TestApp) -> Arc<Edges> {
    app.state.network.edges.clone()
  }

  /// Every metric of `app` as the exposition writes it.
  pub(crate) fn exposed(app: &TestApp) -> String {
    let metrics = app.state.telemetry.metrics();
    crate::telemetry::registry::text(&metrics.registry.families())
  }

  fn refused(result: Result<(), NotQueued>) -> bool {
    matches!(result, Err(NotQueued::Refused(reason)) if reason == NOT_SHARED)
  }

  fn empty_share(reply: soulseek_client::BrowseReply) -> bool {
    matches!(reply, soulseek_client::BrowseReply::Response(r) if r.directories.is_empty())
  }

  #[tokio::test]
  async fn a_blocked_user_gets_what_a_blocked_user_gets_at_every_edge() {
    let app = app(CONFIG, &[]).await;
    let block = json!({ "scopes": ["uploads", "searches", "browse"] });
    let answer = app
      .send(request("PUT", "/api/v0/users/mallory/block", Some(block)))
      .await;
    assert_eq!(answer.status, 200);
    let edges = edges(&app);
    let peer = from("198.51.100.7");
    let file = name("@@music\\a.mp3");

    assert!(refused(
      enqueue_download(&edges, "mallory", peer, &file).await
    ));
    assert!(empty_share(browse(&edges, "mallory", peer).await));
    let folder = directory_contents(&edges, "mallory", peer, &name("@@music\\album")).await;
    let folder = folder.expect("a blocked user is answered");
    assert_eq!(folder.len(), 1);
    assert_eq!(folder[0].name.as_str(), "@@music\\album");
    assert!(folder[0].files.is_empty());
    let info = user_info(&edges, "mallory", peer).await;
    assert_eq!(info.description, "A reseek user.");
    assert_eq!(
      (
        info.upload_slots,
        info.queue_length,
        info.has_free_upload_slot
      ),
      (0, i32::MAX as u32, false)
    );
    assert_eq!(place_in_queue(&edges, "mallory", peer, &file).await, None);

    // an address the configuration blocks is blocked whoever connects from it
    let blocked_network = from("192.0.2.9");
    assert!(empty_share(browse(&edges, "alice", blocked_network).await));
    assert_eq!(
      directory_contents(&edges, "alice", blocked_network, &name("@@music"))
        .await
        .map(|d| d.len()),
      Some(1)
    );
    assert_eq!(
      user_info(&edges, "alice", blocked_network)
        .await
        .queue_length,
      i32::MAX as u32
    );
  }

  #[tokio::test]
  async fn a_file_sent_unasked_is_refused_for_a_name_of_no_file_or_a_blocked_sender() {
    let config = format!("{CONFIG}[transfers.download.received]\nfrom = [\"default\"]\n");
    let app = app(&config, &[]).await;
    let block = json!({ "scopes": ["downloads"] });
    let answer = app
      .send(request("PUT", "/api/v0/users/mallory/block", Some(block)))
      .await;
    assert_eq!(answer.status, 200);
    let edges = edges(&app);
    let peer = from("198.51.100.7");
    let rejected = |result| matches!(result, Err(ClientError::TransferRejected(_)));
    for no_file in ["", "\\", "@@s\\folder\\", " ", "\t"] {
      let offered = receive_offer(&edges, "alice", peer, &name(no_file), 3).await;
      assert!(rejected(offered), "{no_file:?}");
    }
    let file = name("@@s\\a.flac");
    assert!(rejected(
      receive_offer(&edges, "mallory", peer, &file, 3).await
    ));
    assert!(
      rejected(receive_offer(&edges, " ", peer, &file, 3).await),
      "a sender named with whitespace alone"
    );
    assert_eq!(sent_jobs(&app).await, 0, "a refused offer makes no job");
    assert!(
      receive_offer(&edges, "alice", peer, &file, 3).await.is_ok(),
      "the same file from someone else is taken"
    );
    assert_eq!(sent_jobs(&app).await, 1);
  }

  #[tokio::test]
  async fn an_address_blocked_with_a_user_is_refused_with_the_refusal_message() {
    let app = app(
      &format!("{CONFIG}[blocking]\nrefusal_message = \"Banned\"\n"),
      &[],
    )
    .await;
    let block = json!({ "scopes": ["uploads"], "addresses": ["198.51.100.0/24"] });
    let answer = app
      .send(request("PUT", "/api/v0/users/mallory/block", Some(block)))
      .await;
    assert_eq!(answer.status, 200, "{:?}", answer.body);
    assert_eq!(answer.json()["addresses"], json!(["198.51.100.0/24"]));
    let edges = edges(&app);
    let file = name("@@music\\a.mp3");
    let banned = |result| matches!(result, Err(NotQueued::Refused(reason)) if reason == "Banned");
    assert!(banned(
      enqueue_download(&edges, "eve", from("198.51.100.9"), &file).await
    ));
    assert!(!banned(
      enqueue_download(&edges, "eve", from("203.0.113.9"), &file).await
    ));
    let bad = json!({ "addresses": ["mallory's house"] });
    let answer = app
      .send(request("PUT", "/api/v0/users/mallory/block", Some(bad)))
      .await;
    assert_eq!(answer.status, 400);
  }

  #[tokio::test]
  async fn a_name_that_climbs_out_of_the_share_is_refused() {
    let app = app(CONFIG, &[]).await;
    let edges = edges(&app);
    let peer = from("198.51.100.7");
    for traversing in ["@@music\\..\\secret.txt", "@@music/./a.mp3", ".."] {
      assert!(
        refused(enqueue_download(&edges, "alice", peer, &name(traversing)).await),
        "{traversing}"
      );
      assert_eq!(
        directory_contents(&edges, "alice", peer, &name(traversing)).await,
        Some(Vec::new()),
        "{traversing}"
      );
    }
  }

  #[tokio::test]
  async fn user_info_carries_the_upload_forecast() {
    let app = app(
      "[transfers.upload]\nslots = 4\n[transfers.groups.default.upload]\nslots = 3\n",
      &[],
    )
    .await;
    let info = user_info(&edges(&app), "alice", from("198.51.100.7")).await;
    assert_eq!(info.description, "A reseek user.");
    assert_eq!(
      (
        info.upload_slots,
        info.queue_length,
        info.has_free_upload_slot
      ),
      (3, 0, true),
      "the group's own slots, all free, with no one waiting"
    );
  }

  #[tokio::test]
  async fn user_info_says_who_may_send_us_files() {
    let friends = "[transfers.groups.user_defined.friends.upload]\n";
    for (groups, permitted) in [
      ("[]", 0),
      ("[\"frends\"]", 0),
      ("[\"default\"]", 1),
      ("[\"buddies\", \"default\"]", 1),
      ("[\"buddies\"]", 2),
      ("[\"buddies\", \"frends\"]", 2),
      ("[\"friends\"]", 3),
      ("[\"buddies\", \"privileged\"]", 3),
    ] {
      let config = format!("{CONFIG}{friends}[transfers.download.received]\nfrom = {groups}\n");
      let edges = edges(&app(&config, &[]).await);
      let asked = user_info(&edges, "alice", from("198.51.100.7")).await;
      assert_eq!(asked.upload_permitted, Some(permitted), "from = {groups}");
      let blocked = user_info(&edges, "alice", from("192.0.2.9")).await;
      assert_eq!(
        blocked.upload_permitted,
        Some(permitted),
        "blocked, from = {groups}"
      );
    }
  }

  #[test]
  fn a_request_is_answered_as_it_was_decided_and_a_failure_here_is_a_refusal() {
    let file = name("@@music\\a.mp3");
    assert_eq!(decision(Ok(()), &file), UploadDecision::Queue);
    assert_eq!(
      decision(Err(NotQueued::Refused("Banned".into())), &file),
      UploadDecision::Deny("Banned".into())
    );
    assert_eq!(
      decision(Err(NotQueued::Later), &file),
      UploadDecision::Later
    );
    let failed = ClientError::client("the queue was not written", None);
    assert_eq!(
      decision(Err(NotQueued::Failed(failed)), &file),
      UploadDecision::Deny("Enqueue failed due to internal error".into())
    );
  }

  #[tokio::test]
  async fn our_own_search_is_answered_only_while_a_search_of_ours_asks_us() {
    use crate::testing::soulseek::{self, Server, US};
    use soulseek_proto::ServerRequest;

    let app = soulseek::app(CONFIG).await;
    let mut server = Server::silent().await;
    server.login(&app).await;
    let edges = edges(&app);
    assert!(!edges.own_unasked("alice", 7), "another user's search");
    assert!(edges.own_unasked(US, 7), "ours, which asked no one");
    let others = app.state.network.user_search(7, vec!["bob".into()]);
    assert!(edges.own_unasked(US, 7), "ours, which asked others");
    drop(others);
    let ours = app
      .state
      .network
      .user_search(7, vec!["bob".into(), US.into()]);
    assert!(!edges.own_unasked(US, 7), "ours, which asked us");
    assert!(edges.own_unasked(US, 8), "another of ours");
    drop(ours);
    assert!(edges.own_unasked(US, 7), "once it ended");

    // a search of users started through the API is known while it runs
    let body = json!({
      "searchText": "pink floyd",
      "searchTimeout": 60_000,
      "scope": { "type": "user", "subjects": [US] }
    });
    let started = app
      .send(request("POST", "/api/v0/searches", Some(body)))
      .await;
    assert_eq!(started.status, 200, "{:?}", started.body);
    let token = server
      .received(|request| match request {
        ServerRequest::UserSearch(search) => Some(search.token),
        _ => None,
      })
      .await;
    assert!(!edges.own_unasked(US, token));
  }
}
