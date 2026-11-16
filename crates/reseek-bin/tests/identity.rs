//! Someone connecting to the daemon first under another user's name, over soulfind: a browse
//! and a download of that user go to the user and never to the stranger, and the user's own
//! request to have the daemon connect is still taken.

mod support;

use soulseek_client::soulseek_proto;
use std::net::Ipv4Addr;
use std::sync::Arc;

use api_types::transfers::{TransferOutcome, TransferStates};
use soulseek_client::handler::{self, BrowseReply, Handler, Peer, UploadReply};
use soulseek_client::{Client, Config};
use soulseek_proto::PeerMessage;
use soulseek_proto::frame::FrameKind;
use soulseek_proto::peer::{BrowseResponse, Directory, File};
use support::client::Queueing;
use support::peer::PeerSide;
use support::scripted::introduced;
use support::soulfind::{self, Soulfind, User};
use support::transfers::{self, Asked, asked, content, file, scratch};
use support::{
  PATIENCE, daemon_listening, download_when, enqueue, free_port, logged_in, signed_in, until,
};

const SIZE: usize = 200_000;
const WANTED: &str = "@@share\\album\\01 song.flac";

/// an address on loopback the server gives for nobody
const ELSEWHERE: Ipv4Addr = Ipv4Addr::new(127, 0, 0, 2);

/// The user's answers: one shared folder, and every file asked of them queued.
struct Sharing {
  queueing: Queueing,
  shared: BrowseResponse,
}

impl Handler for Sharing {
  async fn browse(&self, _: &Client, _: &Peer) -> Option<BrowseReply> {
    Some(BrowseReply::Response(self.shared.clone()))
  }

  async fn upload(
    &self,
    client: &Client,
    peer: &Peer,
    request: &handler::UploadRequest,
  ) -> UploadReply {
    self.queueing.upload(client, peer, request).await
  }
}

/// The user the stranger names, sharing one folder and queueing every file asked of them.
async fn user(server: &Arc<Soulfind>) -> (User, Asked) {
  let (queueing, requests) = transfers::queueing();
  let shared = BrowseResponse {
    directories: vec![Directory {
      name: "@@share\\album".into(),
      files: vec![File {
        code: 1,
        filename: "01 song.flac".into(),
        size: SIZE as u64,
        extension: "flac".into(),
        attributes: Vec::new(),
      }],
    }],
    locked_directories: Vec::new(),
  };
  let options = Config::builder()
    .listen_port(free_port())
    .handler(Sharing { queueing, shared });
  (server.login("user", options).await, requests)
}

/// Fails with what the stranger was sent, should the daemon send it anything before `until`
/// ends.
async fn unasked<T>(stranger: &mut PeerSide, until: impl Future<Output = T>) -> T {
  tokio::select! {
    done = until => done,
    sent = stranger.frame_or_close(FrameKind::Peer) => {
      let sent = sent.map(|frame| PeerMessage::decode(&frame));
      panic!("the daemon sent the stranger {sent:?}")
    }
  }
}

#[tokio::test]
async fn a_browse_and_a_download_go_to_the_user_a_stranger_named_first() {
  let Some(server) = soulfind::server() else {
    return;
  };
  let (user, mut requests) = user(&server).await;
  let dir = tempfile::tempdir().unwrap();
  let name = soulfind::username("daemon");
  let listen = free_port();
  let daemon = daemon_listening(dir.path(), &server, &name, listen, &[]);
  let api = signed_in(&daemon).await;
  until(&api, "logged in", logged_in).await;
  // macOS answers on 127.0.0.1 alone unless given an alias, so there this test is skipped
  let Some(mut stranger) = introduced(listen, &user.username, ELSEWHERE).await else {
    return;
  };

  let browsed = unasked(&mut stranger, api.browse(&user.username))
    .await
    .unwrap();
  let folders: Vec<&str> = browsed
    .directories
    .iter()
    .map(|directory| directory.name.as_str())
    .collect();
  assert_eq!(folders, ["@@share\\album"]);

  let id = enqueue(&api, &user.username, WANTED, SIZE as u64).await;
  let (from, asked_for) = unasked(&mut stranger, asked(&mut requests)).await;
  assert_eq!((from.as_str(), asked_for.as_str()), (name.as_str(), WANTED));
  // the user sends it, and it is theirs that lands
  let source = file(&scratch(), "song.flac", SIZE);
  user
    .client
    .upload(&name, WANTED)
    .from_file(source)
    .await
    .unwrap();
  let done = download_when(&api, &user.username, id, "landed", |t| {
    t.local_path.is_some()
  })
  .await;
  assert_eq!(
    done.state,
    TransferStates::Completed(TransferOutcome::Succeeded)
  );
  let landed = std::fs::read(done.local_path.unwrap()).unwrap();
  assert_eq!(landed, content(SIZE));
}

#[tokio::test]
async fn the_users_request_to_connect_is_taken_while_a_stranger_holds_the_name() {
  let Some(server) = soulfind::server() else {
    return;
  };
  let (user, _requests) = user(&server).await;
  let dir = tempfile::tempdir().unwrap();
  let name = soulfind::username("daemon");
  let listen = free_port();
  let daemon = daemon_listening(dir.path(), &server, &name, listen, &[]);
  let api = signed_in(&daemon).await;
  until(&api, "logged in", logged_in).await;
  let Some(mut stranger) = introduced(listen, &user.username, ELSEWHERE).await else {
    return;
  };
  // soulfind gives out the daemon's LAN address, where it does not listen, so the user reaches
  // it only by having the server ask it to connect
  let info = tokio::time::timeout(PATIENCE, user.client.users().info(&name).into_future())
    .await
    .expect("the daemon answers the user");
  assert!(info.is_ok(), "{info:?}");
  assert!(
    stranger
      .quiet_for(std::time::Duration::from_millis(100))
      .await
  );
}
