//! The upload engine against the in-memory daemon, which is not connected: every refusal a
//! peer can be told and what triggers it, a privileged user past the limits, the upload calls
//! of the API, a group we put a user in taking effect at once, and the queued uploads of a user
//! canceled when the user is blocked.

use soulseek_client::soulseek_proto;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::task::Poll;

use api_types::transfers::{TransferDirection, TransferOutcome, TransferStates, UserResponse};
use reseek_shares::lock;
use serde_json::{Value, json};
use soulseek_proto::RemoteFilename;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::intake::{QUEUED_PER_ADDRESS, QUEUED_USERS};
use super::{InFlight, NotQueued};
use crate::network::resolvers::tests::{edges, exposed, from};
use crate::network::resolvers::{enqueue_download, place_in_queue, user_info};
use crate::testing::soulseek::Server;
use crate::testing::{TestApp, app};
use crate::transfers::fixtures::{Row, insert};
use crate::transfers::state::State;
use crate::transfers::{Record, store};

const SHARED: &str = "Music\\Album\\01.mp3";

/// A configuration sharing `dir/music` as `Music`, with loopback an administrator, and `extra`.
fn config(dir: &Path, extra: &str) -> String {
  let album = dir.join("music").join("Album");
  std::fs::create_dir_all(&album).unwrap();
  std::fs::write(album.join("01.mp3"), "one").unwrap();
  std::fs::write(album.join("02.mp3"), "two").unwrap();
  format!(
    "[shares]\ndirectories = [\"[Music]{}\"]\n\
     [web.authentication]\ntrusted_networks = [\"127.0.0.0/8\"]\n{extra}",
    dir.join("music").display()
  )
}

async fn shared_app(extra: &str) -> (tempfile::TempDir, TestApp) {
  let files = tempfile::tempdir().unwrap();
  let app = app(&config(files.path(), extra), &[]).await;
  (files, app)
}

/// Where [`asked`] asks from, and where [`reported_app`]'s server says every user is.
const REPORTED: &str = "198.51.100.7";

/// What `username` asking for `filename` is told: `None` when it is queued.
async fn asked(app: &TestApp, username: &str, filename: &str) -> Option<String> {
  asked_from(app, REPORTED, username, filename).await
}

/// [`asked`], the request coming from `ip`.
async fn asked_from(app: &TestApp, ip: &str, username: &str, filename: &str) -> Option<String> {
  let peer = from(ip);
  match enqueue_download(&edges(app), username, peer, &RemoteFilename::from(filename)).await {
    Ok(()) => None,
    Err(NotQueued::Refused(reason)) => Some(reason),
    Err(other) => panic!("not a refusal: {other}"),
  }
}

/// [`shared_app`] logged in to a scripted server that says every user is online, not
/// privileged, and at [`REPORTED`].
async fn reported_app(extra: &str) -> (tempfile::TempDir, TestApp, Server) {
  use soulseek_proto::server::{GetPeerAddressResponse, UserStatus};
  use soulseek_proto::{ServerRequest, ServerResponse, UserPresence};

  let files = tempfile::tempdir().unwrap();
  let app = crate::testing::soulseek::app(&config(files.path(), extra)).await;
  let server = Server::start(|request| match request {
    ServerRequest::GetPeerAddress(asked) => {
      vec![ServerResponse::GetPeerAddress(GetPeerAddressResponse {
        username: asked.username.clone(),
        ip: REPORTED.parse().unwrap(),
        port: 2234,
        obfuscation: None,
      })]
    }
    ServerRequest::GetStatus(asked) => vec![ServerResponse::GetStatus(UserStatus {
      username: asked.username.clone(),
      presence: UserPresence::Online,
      is_privileged: false,
    })],
    _ => Vec::new(),
  })
  .await;
  server.login(&app).await;
  (files, app, server)
}

/// The value of the metric `name` in `app`'s exposition.
fn metric(app: &TestApp, name: &str) -> String {
  exposed(app)
    .lines()
    .find_map(|line| line.strip_prefix(&format!("{name} ")).map(str::to_string))
    .unwrap_or_else(|| panic!("no {name}"))
}

#[tokio::test]
async fn every_refusal_a_peer_can_be_told_and_what_triggers_it() {
  let (files, app) =
    shared_app("[transfers.upload.limits]\nqueued = { files = 1 }\ndaily = { failures = 1 }\n")
      .await;
  let not_shared = Some("File not shared.".to_string());
  assert_eq!(
    asked(&app, "alice", "Music\\..\\secret.txt").await,
    not_shared
  );
  assert_eq!(
    asked(&app, "alice", "Music\\Album\\03.mp3").await,
    not_shared
  );
  let block = json!({ "scopes": ["uploads"] });
  let (status, _) = app
    .call("PUT", "/api/v0/users/mallory/block", Some(block))
    .await;
  assert_eq!(status, 200);
  assert_eq!(asked(&app, "mallory", SHARED).await, not_shared);

  // a shared file gone from disk is not shared, and the shares are told to scan
  std::fs::remove_file(files.path().join("music/Album/02.mp3")).unwrap();
  assert_eq!(
    asked(&app, "alice", "Music\\Album\\02.mp3").await,
    not_shared
  );
  assert!(app.state.state.current().shares.scan_pending);

  let mut queued = Row::new(
    "carol",
    TransferDirection::Upload,
    TransferStates::QueuedLocally,
  );
  queued.filename = "Music\\Album\\02.mp3".to_string();
  let mut failed = Row::new(
    "dave",
    TransferDirection::Upload,
    TransferStates::Completed(TransferOutcome::Cancelled),
  );
  failed.started_at = Some(chrono::Utc::now());
  failed.ended_at = failed.started_at;
  insert(&app.state.transfers, vec![queued, failed]).await;
  assert_eq!(
    asked(&app, "carol", SHARED).await.as_deref(),
    Some("Too many files")
  );
  assert_eq!(
    asked(&app, "dave", SHARED).await.as_deref(),
    Some("Too many failed transfers today")
  );

  // too many waiting, at the pace of the last ones
  let intake = &app.state.uploads.intake;
  intake.waiting.store(2, Ordering::Release);
  intake.latency.store(90_001f64.to_bits(), Ordering::Release);
  assert_eq!(
    asked(&app, "erin", SHARED).await.as_deref(),
    Some("Overwhelmed with requests; try again later.")
  );
  intake.waiting.store(0, Ordering::Release);

  assert_eq!(metric(&app, "reseek_enqueue_requests_received"), "7");
  assert_eq!(metric(&app, "reseek_enqueue_requests_rejected"), "7");
  assert_eq!(metric(&app, "reseek_enqueue_requests_dropped"), "1");
}

#[tokio::test]
async fn a_new_user_finding_the_queue_holding_its_users_is_refused_for_good() {
  let (_files, app) = shared_app("").await;
  // asked() asks from 198.51.100.7; these came from all over
  let elsewhere = |n: usize| std::net::IpAddr::from([203, 0, (n / 256) as u8, (n % 256) as u8]);
  let uploads = &app.state.uploads;
  for n in 0..QUEUED_USERS {
    let username = format!("user{n}");
    uploads.queue.enqueue(Uuid::new_v4(), &username, "file");
    uploads.queued_from(&username, elsewhere(n));
  }
  assert_eq!(
    asked(&app, "newcomer", SHARED).await.as_deref(),
    Some("Queue full")
  );
  let cached = app.state.users.cached_users();
  assert!(
    cached.iter().all(|user| user.username != "newcomer"),
    "the users cache took them in"
  );
  assert_eq!(
    asked(&app, "user7", SHARED).await,
    None,
    "one of them queues more"
  );
}

#[tokio::test]
async fn one_address_holds_no_more_than_its_share_of_the_queue() {
  let (_files, app) = shared_app("").await;
  // names one host made up, all asking from where asked() asks
  let here = from("198.51.100.7").ip();
  let uploads = &app.state.uploads;
  for n in 0..QUEUED_PER_ADDRESS {
    let username = format!("made-up{n}");
    uploads.queue.enqueue(Uuid::new_v4(), &username, "file");
    uploads.queued_from(&username, here);
  }
  assert_eq!(
    asked(&app, "another", SHARED).await.as_deref(),
    Some("Queue full")
  );
  assert_eq!(
    asked(&app, "made-up3", SHARED).await,
    None,
    "one of them queues more"
  );
  let other = from("203.0.113.9");
  let queued = enqueue_download(
    &edges(&app),
    "elsewhere",
    other,
    &RemoteFilename::from(SHARED),
  )
  .await;
  assert!(queued.is_ok(), "{queued:?}");
}

/// What [`silent_app`] runs: the shared files, the daemon, its server and the users' clients.
struct Silent {
  files: tempfile::TempDir,
  app: TestApp,
  _server: Server,
  /// where every user's client is: a socket that takes connections and never answers, so the
  /// one upload running waits for its answer and keeps the queue as it is
  _clients: tokio::net::TcpListener,
}

/// [`shared_app`] with one upload slot, logged in to a scripted server that says every user is
/// online, shares enough not to be a leecher, and is at a client that never answers.
async fn silent_app() -> Silent {
  use soulseek_proto::server::{
    GetPeerAddressResponse, UserStatistics, UserStats, UserStatus, WatchUserResponse, WatchedUser,
  };
  use soulseek_proto::{ServerRequest, ServerResponse, UserPresence};

  let silent = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
  let port = silent.local_addr().unwrap().port();
  let files = tempfile::tempdir().unwrap();
  let extra = "[transfers.upload]\nslots = 1\n\
               [soulseek.connection.timeout]\ntransfer_response = \"1h\"\n";
  let app = crate::testing::soulseek::app(&config(files.path(), extra)).await;
  let stats = UserStats {
    average_speed: 1000,
    upload_count: 10,
    unknown: 0,
    file_count: 1000,
    directory_count: 100,
  };
  let server = crate::testing::soulseek::Server::start(move |request| match request {
    ServerRequest::GetUserStats(asked) => vec![ServerResponse::GetUserStats(UserStatistics {
      username: asked.username.clone(),
      stats,
    })],
    ServerRequest::GetStatus(asked) => vec![ServerResponse::GetStatus(UserStatus {
      username: asked.username.clone(),
      presence: UserPresence::Online,
      is_privileged: false,
    })],
    ServerRequest::WatchUser(asked) => vec![ServerResponse::WatchUser(WatchUserResponse {
      username: asked.username.clone(),
      user: Some(WatchedUser {
        status: UserPresence::Online,
        stats,
        country_code: None,
      }),
    })],
    ServerRequest::GetPeerAddress(asked) => {
      vec![ServerResponse::GetPeerAddress(GetPeerAddressResponse {
        username: asked.username.clone(),
        ip: std::net::Ipv4Addr::LOCALHOST,
        port: port.into(),
        obfuscation: None,
      })]
    }
    _ => Vec::new(),
  })
  .await;
  server.login(&app).await;
  Silent {
    files,
    app,
    _server: server,
    _clients: silent,
  }
}

/// What `username` asking `app` for [`SHARED`] from [`REPORTED`] is told: `None` when it is
/// queued.
async fn asked_owned(app: &TestApp, username: String) -> Option<String> {
  asked(app, &username, SHARED).await
}

#[tokio::test]
async fn one_address_queues_its_share_of_users_and_more_once_they_leave() {
  let silent = silent_app().await;
  let app = &silent.app;
  // names one host makes up, all asking from one address
  let ask = |username: String| asked_owned(app, username);
  for n in 0..QUEUED_PER_ADDRESS {
    assert_eq!(ask(format!("made-up{n}")).await, None, "made-up{n}");
  }
  assert_eq!(ask("one-more".into()).await.as_deref(), Some("Queue full"));

  // they all leave the queue
  let (status, listed) = app.call("GET", "/api/v0/transfers/uploads", None).await;
  assert_eq!(status, 200);
  for user in listed.as_array().unwrap() {
    let username = user["username"].as_str().unwrap();
    for directory in user["directories"].as_array().unwrap() {
      for file in directory["files"].as_array().unwrap() {
        let id = file["id"].as_str().unwrap();
        let path = format!("/api/v0/transfers/uploads/{username}/{id}");
        app.call("DELETE", &path, None).await;
      }
    }
  }
  let uploads = &app.state.uploads;
  tokio::time::timeout(std::time::Duration::from_secs(10), async {
    while !uploads.queue.users().is_empty() {
      tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
  })
  .await
  .expect("the uploads never left the queue");
  assert_eq!(
    ask("one-more".into()).await,
    None,
    "the address holds no place now"
  );
}

/// A file that leaves its user's view while their upload waits for its slot fails the upload as
/// gone when the slot comes.
#[tokio::test]
async fn a_file_gone_while_its_upload_waits_for_its_slot_fails_it_when_the_slot_comes() {
  let silent = silent_app().await;
  let app = &silent.app;
  let second = "Music\\Album\\02.mp3";
  // alice's upload takes the one slot and waits on her client
  assert_eq!(asked(app, "alice", SHARED).await, None);
  assert_eq!(asked(app, "bob", second).await, None);
  let uploads = &app.state.uploads;
  let client = uploads.client().unwrap();
  let within = std::time::Duration::from_secs(10);
  tokio::time::timeout(within, async {
    while !client
      .transfers()
      .uploads()
      .iter()
      .any(|t| t.username == "bob")
    {
      tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
  })
  .await
  .expect("bob's upload waits for its slot in the client");
  std::fs::remove_file(silent.files.path().join("music/Album/02.mp3")).unwrap();

  let listed = async |username: &str| -> Vec<api_types::transfers::Transfer> {
    let path = format!("/api/v0/transfers/uploads/{username}");
    let (_, body) = app.call("GET", &path, None).await;
    let user: UserResponse = serde_json::from_value(body).unwrap();
    user.directories.into_iter().flat_map(|d| d.files).collect()
  };
  let alices = listed("alice").await[0].id;
  let (status, _) = app
    .call(
      "DELETE",
      &format!("/api/v0/transfers/uploads/alice/{alices}"),
      None,
    )
    .await;
  assert_eq!(status, 204);
  let ended = tokio::time::timeout(within, async {
    loop {
      if let Some(ended) = listed("bob")
        .await
        .into_iter()
        .find(|t| t.ended_at.is_some())
      {
        return ended;
      }
      tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
  })
  .await
  .expect("bob's upload ends");
  assert_eq!(
    (ended.state, ended.exception.as_deref()),
    (
      TransferStates::Completed(TransferOutcome::Aborted),
      Some("File could not be found")
    )
  );
}

/// Names one host makes up, asking all at once, are queued no further than the address's share:
/// the places of the requests being decided count as taken.
#[tokio::test]
async fn names_asking_at_once_from_one_address_get_only_its_share() {
  let silent = silent_app().await;
  let app = &silent.app;
  let over = 5;
  let answers = futures_util::future::join_all(
    (0..QUEUED_PER_ADDRESS + over).map(|n| asked_owned(app, format!("made-up{n}"))),
  )
  .await;
  let queued = answers.iter().filter(|answer| answer.is_none()).count();
  let full = answers
    .iter()
    .filter(|answer| answer.as_deref() == Some("Queue full"))
    .count();
  assert_eq!((queued, full), (QUEUED_PER_ADDRESS, over), "{answers:?}");
}

#[tokio::test]
async fn a_privileged_user_is_queued_past_the_limits() {
  let (_files, app, _server) =
    reported_app("[transfers.upload.limits]\nqueued = { files = 1 }\n").await;
  let mut queued = Row::new(
    "vip",
    TransferDirection::Upload,
    TransferStates::QueuedLocally,
  );
  queued.filename = "Music\\Album\\02.mp3".to_string();
  insert(&app.state.transfers, vec![queued]).await;
  assert_eq!(
    asked(&app, "vip", SHARED).await.as_deref(),
    Some("Too many files")
  );
  app.state.users.privileged_listed(&["vip".to_string()]);
  assert_eq!(asked(&app, "vip", SHARED).await, None);
  let (status, body) = app.call("GET", "/api/v0/transfers/uploads/vip", None).await;
  assert_eq!(status, 200);
  let user: UserResponse = serde_json::from_value(body).unwrap();
  let files: Vec<&str> = user.directories[0]
    .files
    .iter()
    .map(|f| f.filename.as_str())
    .collect();
  assert!(files.contains(&SHARED), "{files:?}");
  assert_eq!(metric(&app, "reseek_enqueue_requests_accepted"), "1");
}

#[tokio::test]
async fn a_name_in_no_group_is_answered_without_asking_where_it_is() {
  use soulseek_proto::server::{UserStatistics, UserStats, UserStatus};
  use soulseek_proto::{ServerRequest, ServerResponse, UserPresence};

  let files = tempfile::tempdir().unwrap();
  let app = crate::testing::soulseek::app(&config(files.path(), "")).await;
  // everyone shares enough not to be a leecher, and nobody is privileged
  let mut server = Server::start_scripting_statistics(|request| match request {
    ServerRequest::GetUserStats(asked) => vec![ServerResponse::GetUserStats(UserStatistics {
      username: asked.username.clone(),
      stats: UserStats {
        file_count: 10,
        directory_count: 1,
        ..UserStats::default()
      },
    })],
    ServerRequest::GetStatus(asked) => vec![ServerResponse::GetStatus(UserStatus {
      username: asked.username.clone(),
      presence: UserPresence::Online,
      is_privileged: false,
    })],
    _ => Vec::new(),
  })
  .await;
  server.login(&app).await;
  let info = user_info(&edges(&app), "nobody", from("203.0.113.50")).await;
  assert_eq!(
    info.upload_slots,
    app.state.uploads.default_forecast().total_slots
  );
  let looked_up = server
    .receives(std::time::Duration::from_millis(500), |request| {
      matches!(request, ServerRequest::GetPeerAddress(_))
    })
    .await;
  assert!(!looked_up, "a stranger's place is a stranger's anyway");
}

#[tokio::test]
async fn a_request_in_a_buddys_name_from_another_address_is_a_strangers() {
  let (_files, app, _server) = reported_app(
    "[transfers.upload]\nslots = 6\n\
     [transfers.upload.limits]\nqueued = { files = 1 }\n\
     [transfers.groups.default.upload]\nslots = 2\n\
     [transfers.groups.buddies.upload]\nslots = 5\n\
     [transfers.groups.buddies.upload.limits.queued]\nfiles = 0\n",
  )
  .await;
  let (status, _) = app.call("PUT", "/api/v0/users/bob/buddy", None).await;
  assert_eq!(status, 204);
  let mut queued = Row::new(
    "bob",
    TransferDirection::Upload,
    TransferStates::QueuedLocally,
  );
  queued.filename = "Music\\Album\\02.mp3".to_string();
  insert(&app.state.transfers, vec![queued]).await;
  let edges = edges(&app);
  let stranger = "203.0.113.50";

  // as a stranger, bob's name is held to everyone's limits, waits where everyone waits, and is
  // told no place
  assert_eq!(
    asked_from(&app, stranger, "bob", SHARED).await.as_deref(),
    Some("Too many files")
  );
  let info = user_info(&edges, "bob", from(stranger)).await;
  assert_eq!(info.upload_slots, 2);
  // from where the server says bob is, he is a buddy
  assert_eq!(asked(&app, "bob", SHARED).await, None);
  let info = user_info(&edges, "bob", from(REPORTED)).await;
  assert_eq!(info.upload_slots, 5);
  let file = RemoteFilename::from(SHARED);
  assert_eq!(
    place_in_queue(&edges, "bob", from(REPORTED), &file).await,
    Some(1)
  );
  assert_eq!(
    place_in_queue(&edges, "bob", from(stranger), &file).await,
    None
  );
}

/// A request in a buddy's name from another address is answered for a file shared only to
/// buddies as one not shared, though that file is queued to the buddy: the answer would say it
/// exists, and the request does not stand for the buddy's.
#[tokio::test]
async fn a_strangers_request_for_a_buddys_queued_hidden_file_is_not_shared() {
  let (_files, app, _server) = reported_app("[shares.visible_to]\nMusic = [\"buddies\"]\n").await;
  let (status, _) = app.call("PUT", "/api/v0/users/bob/buddy", None).await;
  assert_eq!(status, 204);
  let mut queued = Row::new(
    "bob",
    TransferDirection::Upload,
    TransferStates::QueuedLocally,
  );
  queued.filename = SHARED.to_string();
  let id = queued.id;
  insert(&app.state.transfers, vec![queued]).await;
  let uploads = &app.state.uploads;
  lock(&uploads.in_flight).insert(
    id,
    InFlight {
      cancel: CancellationToken::new(),
      remove: false,
      reason: None,
      ended: false,
      asked: false,
      gone: false,
    },
  );
  let asked_for = || lock(&uploads.in_flight)[&id].asked;

  assert_eq!(
    asked_from(&app, "203.0.113.50", "bob", SHARED)
      .await
      .as_deref(),
    Some("File not shared.")
  );
  assert!(!asked_for(), "the stranger's request is not bob's");
  assert_eq!(asked(&app, "bob", SHARED).await, None);
  assert!(asked_for(), "bob's own request stands on his upload");
}

#[tokio::test]
async fn a_running_upload_stands_for_a_new_request_and_an_ended_one_is_superseded() {
  let (_files, app) = shared_app("").await;
  let mut queued = Row::new(
    "alice",
    TransferDirection::Upload,
    TransferStates::QueuedLocally,
  );
  queued.filename = SHARED.to_string();
  let old = queued.id;
  insert(&app.state.transfers, vec![queued]).await;
  let uploads = &app.state.uploads;
  lock(&uploads.in_flight).insert(
    old,
    InFlight {
      cancel: CancellationToken::new(),
      remove: false,
      reason: None,
      ended: false,
      asked: true,
      gone: false,
    },
  );
  let listed = async || -> Vec<Uuid> {
    let (_, body) = app
      .call("GET", "/api/v0/transfers/uploads/alice", None)
      .await;
    let user: UserResponse = serde_json::from_value(body).unwrap();
    user
      .directories
      .iter()
      .flat_map(|d| d.files.iter().map(|f| f.id))
      .collect()
  };
  assert_eq!(asked(&app, "alice", SHARED).await, None);
  assert_eq!(listed().await, [old], "the running upload stands for it");

  // the client ended the upload and told the peer, which asks again before the task wrote it
  lock(&uploads.in_flight).get_mut(&old).unwrap().ended = true;
  assert_eq!(asked(&app, "alice", SHARED).await, None);
  let now = listed().await;
  assert_eq!(now.len(), 1);
  assert_ne!(now[0], old, "a new upload");
  let superseded = app
    .state
    .transfers
    .read(move |conn| store::get(conn, old))
    .await
    .unwrap()
    .unwrap();
  assert!(superseded.removed);
}

#[tokio::test]
async fn a_peer_first_in_line_is_told_place_one() {
  let (_files, app) = shared_app("").await;
  app
    .state
    .uploads
    .queue
    .enqueue(Uuid::new_v4(), "alice", SHARED);
  let peer = from("198.51.100.7");
  let edges = edges(&app);
  let place = |filename: &str| {
    let filename = RemoteFilename::from(filename);
    let edges = edges.clone();
    async move { place_in_queue(&edges, "alice", peer, &filename).await }
  };
  assert_eq!(place(SHARED).await, Some(1));
  assert_eq!(place("Music\\Album\\02.mp3").await, None);
}

#[tokio::test]
async fn the_upload_calls_find_an_upload_by_its_user_and_id() {
  let (_files, app) = shared_app("").await;
  let (status, body) = app
    .call("GET", "/api/v0/transfers/uploads/nobody", None)
    .await;
  assert_eq!(status, 200, "a user without uploads has none");
  assert_eq!(body, json!({ "username": "nobody", "directories": [] }));

  let mut done = Record::upload("alice", SHARED, 3);
  done.state = State::Completed(TransferOutcome::Succeeded);
  done.ended_at = Some(chrono::Utc::now());
  let written = done.clone();
  app
    .state
    .transfers
    .write(move |conn| store::insert(conn, &written))
    .await
    .unwrap();
  let id = done.id;
  let (status, _) = app
    .call("GET", &format!("/api/v0/transfers/uploads/bob/{id}"), None)
    .await;
  assert_eq!(status, 404, "another user's id");
  let (status, why) = app
    .call(
      "GET",
      &format!("/api/v0/transfers/uploads/alice/{id}/why"),
      None,
    )
    .await;
  assert_eq!(status, 200);
  assert_eq!(why["position"], Value::Null, "not in the queue");
  assert_eq!(why["group"], "default");
  assert_eq!(why["limits"]["queued"]["failures"], Value::Null);
  assert_eq!(
    why["limits"]["daily"]["files"],
    json!({ "used": 0, "limit": null })
  );
  let (status, _) = app
    .call(
      "POST",
      &format!("/api/v0/transfers/uploads/alice/{id}/bump"),
      None,
    )
    .await;
  assert_eq!(status, 409, "a finished upload waits for nothing");

  let (status, _) = app
    .call(
      "DELETE",
      &format!("/api/v0/transfers/uploads/alice/{id}?remove=true"),
      None,
    )
    .await;
  assert_eq!(status, 204);
  let removed = app
    .state
    .transfers
    .read(move |conn| store::get(conn, id))
    .await
    .unwrap()
    .unwrap();
  assert!(removed.removed);
}

#[tokio::test]
async fn a_group_we_put_a_user_in_takes_effect_at_once() {
  let (_files, app) = shared_app("[transfers.groups.user_defined.vip.upload]\nslots = 5\n").await;
  assert_eq!(app.state.users.queue_group("zoe"), "default");
  let (status, _) = app
    .call(
      "PUT",
      "/api/v0/users/zoe/groups",
      Some(json!({ "groups": ["vip"] })),
    )
    .await;
  assert_eq!(status, 204);
  assert_eq!(app.state.users.queue_group("zoe"), "vip");
  assert_eq!(app.state.uploads.forecast("zoe").group, "vip");
  let (status, _) = app
    .call(
      "PUT",
      "/api/v0/users/zoe/groups",
      Some(json!({ "groups": [] })),
    )
    .await;
  assert_eq!(status, 204);
  assert_eq!(app.state.users.queue_group("zoe"), "default");
}

#[tokio::test]
async fn blocking_a_user_cancels_their_queued_uploads() {
  let (_files, app) = shared_app("").await;
  let uploads = &app.state.uploads;
  let (queued, other) = (Uuid::new_v4(), Uuid::new_v4());
  let (cancel, kept) = (CancellationToken::new(), CancellationToken::new());
  for (id, username, cancel) in [(queued, "mallory", &cancel), (other, "alice", &kept)] {
    uploads.queue.enqueue(id, username, SHARED);
    lock(&uploads.in_flight).insert(
      id,
      InFlight {
        cancel: cancel.clone(),
        remove: false,
        reason: None,
        ended: false,
        asked: true,
      },
    );
  }
  let block = json!({ "scopes": ["uploads"] });
  let (status, _) = app
    .call("PUT", "/api/v0/users/mallory/block", Some(block))
    .await;
  assert_eq!(status, 200);
  assert!(cancel.is_cancelled());
  assert!(!kept.is_cancelled());
  assert_eq!(
    lock(&uploads.in_flight).get(&queued).unwrap().reason,
    Some("Blocked")
  );
}

#[tokio::test]
async fn a_file_sent_to_a_user_unasked_is_refused_unless_it_is_shared() {
  let (_files, app) = shared_app("").await;
  let (status, answer) = app
    .call(
      "POST",
      "/api/v0/transfers/uploads/alice",
      Some(json!({ "files": [SHARED, "@@nope\\a.flac"] })),
    )
    .await;
  assert_eq!(status, 200, "{answer}");
  assert_eq!(answer["queued"], json!([SHARED]));
  assert_eq!(
    answer["failed"],
    json!([{ "filename": "@@nope\\a.flac", "message": "File not shared." }])
  );
}

/// Writes an upload of `SHARED` to alice queued here, with its task running; returns its id.
async fn sending_shared(app: &TestApp) -> Uuid {
  let mut sending = Record::upload("alice", SHARED, 3);
  sending.state = State::QueuedLocally {
    next_attempt_at: None,
  };
  let id = sending.id;
  app
    .state
    .transfers
    .write(move |conn| store::insert(conn, &sending))
    .await
    .unwrap();
  lock(&app.state.uploads.in_flight).insert(
    id,
    InFlight {
      cancel: CancellationToken::new(),
      remove: false,
      reason: None,
      ended: false,
      asked: false,
      gone: false,
    },
  );
  id
}

/// Sends `files` to alice through the API and returns the answer.
async fn send_to_alice(app: &TestApp, files: &[&str]) -> Value {
  let (status, answer) = app
    .call(
      "POST",
      "/api/v0/transfers/uploads/alice",
      Some(json!({ "files": files })),
    )
    .await;
  assert_eq!(status, 200, "{answer}");
  answer
}

/// A file already queued or being sent to the user is answered apart from those the send
/// queued, and its upload stands for it: no second record is written.
#[tokio::test]
async fn sending_a_file_already_being_sent_to_the_user_says_so_apart_from_the_new_ones() {
  let (_files, app) = shared_app("").await;
  let id = sending_shared(&app).await;
  let second = "Music\\Album\\02.mp3";
  let answer = send_to_alice(&app, &[SHARED, second]).await;
  assert_eq!(answer["queued"], json!([second]));
  assert_eq!(answer["alreadySending"], json!([SHARED]));
  assert_eq!(answer["failed"], json!([]));
  let ids: Vec<Uuid> = uploads_of_shared(&app).await.iter().map(|r| r.id).collect();
  assert_eq!(ids, [id], "the upload under way stands for the file");
}

/// The limits count the file being sent already, so they would turn it away were it decided
/// as a new one.
#[tokio::test]
async fn a_file_already_being_sent_is_not_turned_away_by_the_limits_it_fills() {
  let (_files, app) = shared_app("[transfers.upload.limits]\nqueued = { files = 1 }\n").await;
  sending_shared(&app).await;
  let second = "Music\\Album\\02.mp3";
  let answer = send_to_alice(&app, &[SHARED, second]).await;
  assert_eq!(answer["queued"], json!([]));
  assert_eq!(answer["alreadySending"], json!([SHARED]));
  assert_eq!(
    answer["failed"],
    json!([{ "filename": second, "message": "Too many files" }])
  );
}

/// A peer asking again for a file already queued to them is answered as queued, though the
/// limits count that file and turn a new one away.
#[tokio::test]
async fn a_file_asked_for_again_is_answered_queued_past_the_limits_it_fills() {
  let (_files, app) = shared_app("[transfers.upload.limits]\nqueued = { files = 1 }\n").await;
  let id = sending_shared(&app).await;
  assert_eq!(asked(&app, "alice", SHARED).await, None);
  assert_eq!(
    asked(&app, "alice", "Music\\Album\\02.mp3")
      .await
      .as_deref(),
    Some("Too many files")
  );
  let ids: Vec<Uuid> = uploads_of_shared(&app).await.iter().map(|r| r.id).collect();
  assert_eq!(ids, [id], "the upload queued stands for the file");
  assert!(
    lock(&app.state.uploads.in_flight)[&id].asked,
    "the file sent unasked is one alice asked for now"
  );
  assert_eq!(metric(&app, "reseek_enqueue_requests_accepted"), "1");
}

/// The uploads of `SHARED` to alice, removed ones included, oldest first.
async fn uploads_of_shared(app: &TestApp) -> Vec<Record> {
  app
    .state
    .transfers
    .read(|conn| {
      let mut ids = conn.prepare(
        "SELECT Id FROM Transfers WHERE Username = 'alice' AND Filename = ?1 ORDER BY rowid",
      )?;
      let ids = ids
        .query_map([SHARED], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
      ids
        .into_iter()
        .map(|id| store::get(conn, id.parse().unwrap()).map(Option::unwrap))
        .collect()
    })
    .await
    .unwrap()
}

/// A send dropped while the transfers database is held, before its record can be written,
/// still queues its file: the record is written and a task runs the upload to its end.
#[tokio::test]
async fn a_send_dropped_before_its_record_is_written_still_queues_the_file() {
  let (_files, app) = shared_app("").await;
  let uploads = &app.state.uploads;
  // in-memory databases send reads through the writer too, so while it is held the send can
  // neither read the user's limits nor write the record
  let (release, released) = std::sync::mpsc::channel::<()>();
  let (holding, held) = tokio::sync::oneshot::channel();
  let transfers = app.state.transfers.clone();
  let hold = tokio::spawn(async move {
    transfers
      .write(move |_| {
        holding.send(()).unwrap();
        released.recv().unwrap();
        Ok(())
      })
      .await
  });
  held.await.unwrap();
  let mut sending = Box::pin(uploads.send("alice", vec![SHARED.to_string()]));
  for _ in 0..5 {
    tokio::task::yield_now().await;
    let polled = std::future::poll_fn(|cx| Poll::Ready(sending.as_mut().poll(cx))).await;
    assert!(polled.is_pending());
  }
  drop(sending);
  release.send(()).unwrap();
  hold.await.unwrap().unwrap();
  let settled = tokio::time::timeout(std::time::Duration::from_secs(5), async {
    loop {
      let written = uploads_of_shared(&app).await;
      if written.iter().any(Record::is_completed) && lock(&uploads.in_flight).is_empty() {
        return written;
      }
      tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
  })
  .await;
  let Ok(written) = settled else {
    panic!(
      "the dropped send left {:?} with {} in flight",
      uploads_of_shared(&app)
        .await
        .iter()
        .map(|r| r.state.kind())
        .collect::<Vec<_>>(),
      lock(&uploads.in_flight).len()
    );
  };
  assert_eq!(written.len(), 1);
  let id = written[0].id;

  let uri = format!("/api/v0/transfers/uploads/alice/{id}");
  let (status, _) = app.call("DELETE", &uri, None).await;
  assert_eq!(status, 204);
  let sent = uploads.send("alice", vec![SHARED.to_string()]).await;
  assert_eq!(sent.queued, [SHARED]);
  let now = uploads_of_shared(&app).await;
  assert_eq!(
    now.len(),
    2,
    "a new upload, not the dropped one standing for it"
  );
}

#[tokio::test]
async fn canceling_an_upload_no_task_runs_ends_it() {
  let (_files, app) = shared_app("").await;
  let waiting = Row::new(
    "alice",
    TransferDirection::Upload,
    TransferStates::QueuedLocally,
  );
  let id = waiting.id;
  insert(&app.state.transfers, vec![waiting]).await;
  let uri = format!("/api/v0/transfers/uploads/alice/{id}");
  let (status, _) = app.call("DELETE", &uri, None).await;
  assert_eq!(status, 204);
  let ended = app
    .state
    .transfers
    .read(move |conn| store::get(conn, id))
    .await
    .unwrap()
    .unwrap();
  assert_eq!(
    ended.state.kind(),
    TransferStates::Completed(TransferOutcome::Cancelled)
  );
  assert!(ended.ended_at.is_some());
  assert_eq!(
    ended.exception.as_deref(),
    Some("the operation was cancelled")
  );
}

#[tokio::test]
async fn a_waiting_leecher_stays_in_leechers_however_many_names_the_cache_takes() {
  use api_types::state::UserStatistics;
  use api_types::users::StatisticsSnapshot;

  let (_files, app) = shared_app("[transfers.groups.leechers.upload]\nslots = 1\n").await;
  let users = &app.state.users;
  let reported = users.reported();
  let shares_nothing = StatisticsSnapshot {
    statistics: UserStatistics {
      average_speed: 0,
      directory_count: 0,
      file_count: 0,
      upload_count: 0,
    },
    updated_at: chrono::Utc::now(),
  };
  reported.update("leecher", true, |facts| {
    facts.statistics = Some(shares_nothing);
  });
  assert_eq!(users.queue_group("leecher"), "leechers");
  app
    .state
    .uploads
    .queue
    .enqueue(Uuid::new_v4(), "leecher", SHARED);
  // names a peer makes up, many times what the cache keeps
  for n in 0..30_000 {
    reported.update(&format!("fresh{n}"), true, |_| {});
  }
  assert!(reported.of("leecher").is_some(), "forgotten while queued");
  assert_eq!(users.queue_group("leecher"), "leechers");
}

#[tokio::test]
async fn a_forecast_for_someone_not_queued_asks_nobody_about_them() {
  use soulseek_proto::ServerRequest;

  let app = crate::testing::soulseek::app("").await;
  let mut server = crate::testing::soulseek::Server::silent().await;
  server.login(&app).await;
  assert_eq!(app.state.uploads.forecast("stranger").group, "default");
  let asked = server
    .receives(std::time::Duration::from_millis(500), |request| {
      matches!(request, ServerRequest::GetUserStats(asked) if asked.username == "stranger")
    })
    .await;
  assert!(!asked, "the stranger's statistics were asked for");
}

#[tokio::test]
async fn a_queued_user_the_cache_does_not_know_is_asked_about_again() {
  use soulseek_proto::ServerRequest;

  let app = crate::testing::soulseek::app("").await;
  let mut server = crate::testing::soulseek::Server::silent().await;
  server.login(&app).await;
  // bob's upload waits in the queue, and the cache knows nothing of him
  let uploads = &app.state.uploads;
  uploads.queue.enqueue(Uuid::new_v4(), "bob", SHARED);
  assert_eq!(uploads.queue.place("bob", SHARED), Some(1));
  server
    .received(|request| {
      matches!(request, ServerRequest::GetUserStats(asked) if asked.username == "bob").then_some(())
    })
    .await;
}

#[tokio::test]
async fn an_uploads_chunks_are_held_to_the_global_limit_as_well_as_its_groups() {
  let (_files, app) = shared_app("[transfers.upload]\nspeed_limit = 1\n").await;
  let pace = app.state.uploads.governor.pace(std::sync::Arc::new(|| {
    crate::config::DEFAULT_GROUP.to_string()
  }));
  // the default group sets no limit, so 1 KiB/s is the global one: 102 bytes a 100 ms
  let granted = pace.chunk(50_000, &CancellationToken::new()).await;
  assert_eq!(granted, 102);
}

/// An upload whose row cannot be read fails only itself in a bulk call, and a cancel that
/// removes it takes it out of the listings.
#[tokio::test]
async fn an_upload_whose_row_cannot_be_read_is_taken_out_by_a_cancel_that_removes_it() {
  let (_files, app) = shared_app("").await;
  let ended = || {
    let state = TransferStates::Completed(TransferOutcome::Succeeded);
    let mut row = Row::new("alice", TransferDirection::Upload, state);
    row.ended_at = Some(chrono::Utc::now());
    row
  };
  let (known, odd) = (ended(), ended());
  let (known, odd) = {
    let ids = (known.id, odd.id);
    insert(&app.state.transfers, vec![known, odd]).await;
    ids
  };
  let row = odd.to_string();
  app
    .state
    .transfers
    .write(move |conn| conn.execute("UPDATE Transfers SET State = 2 WHERE Id = ?1", [row]))
    .await
    .unwrap();

  let bulk = json!({ "ids": [known, odd], "action": "remove" });
  let (status, body) = app
    .call("POST", "/api/v0/transfers/uploads/bulk", Some(bulk))
    .await;
  assert_eq!(status, 200, "{body}");
  assert_eq!(body["ok"], json!([known]), "{body}");
  assert_eq!(body["failed"][0]["id"], json!(odd), "{body}");

  let uri = format!("/api/v0/transfers/uploads/alice/{odd}?remove=true");
  let (status, body) = app.call("DELETE", &uri, None).await;
  assert_eq!(status, 204, "{body}");
  let id = odd.to_string();
  let removed: bool = app
    .state
    .transfers
    .read(move |conn| {
      conn.query_row("SELECT Removed FROM Transfers WHERE Id = ?1", [id], |row| {
        row.get(0)
      })
    })
    .await
    .unwrap();
  assert!(removed);
}
