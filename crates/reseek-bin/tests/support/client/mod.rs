//! A fake Soulseek server over loopback, handlers for the clients tests log in, and a helper to
//! wait for what a client raises.

#![allow(dead_code)] // each test binary uses its own subset of these helpers

pub mod peer;
#[path = "../../../../reseek/src/testing/ports.rs"]
mod ports;
pub mod soulfind;
pub mod transfers;

pub use ports::free_port;

use soulseek_client::soulseek_proto;
use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use soulseek_client::config::ConfigBuilder;
use soulseek_client::handler::{self, BrowseReply, Handler, Peer, UploadReply};
use soulseek_client::{Client, Config, Event, Events};
use soulseek_proto::RemoteFilename;
use soulseek_proto::frame::{self, FrameKind};
use soulseek_proto::peer::{BrowseResponse, UserInfo};
use soulseek_proto::server::{LoginRequest, LoginResponse};
use soulseek_proto::{Limits, Secret, ServerRequest, ServerResponse};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

pub const USERNAME: &str = "alice";
pub const PASSWORD: &str = "hunter2";
pub const PUBLIC_IP: Ipv4Addr = Ipv4Addr::new(203, 0, 113, 7);

/// how long a test waits for something that should happen promptly
pub const PATIENCE: Duration = Duration::from_secs(10);

/// A handler that answers a browse with an empty listing and a user info request with a blank
/// info, for the tests that ask either to see a connection carry messages
#[derive(Clone, Default)]
pub struct Answering;

impl Handler for Answering {
  async fn browse(&self, _: &Client, _: &Peer) -> Option<BrowseReply> {
    Some(BrowseReply::Response(BrowseResponse {
      directories: Vec::new(),
      locked_directories: Vec::new(),
    }))
  }

  async fn user_info(&self, _: &Client, _: &Peer) -> Option<UserInfo> {
    Some(blank_info())
  }
}

/// A handler that queues every upload a peer asks for and hands the request on, and answers
/// nothing else
#[derive(Clone)]
pub struct Queueing {
  pub asked: mpsc::UnboundedSender<(String, RemoteFilename)>,
}

impl Handler for Queueing {
  async fn upload(&self, _: &Client, peer: &Peer, request: &handler::UploadRequest) -> UploadReply {
    // the test may have stopped listening; the request is queued either way
    let _ = self
      .asked
      .send((peer.username.clone(), request.filename.clone()));
    UploadReply::Queue
  }
}

/// the user info [`Answering`] gives
pub fn blank_info() -> UserInfo {
  UserInfo {
    description: String::new(),
    picture: None,
    upload_slots: 0,
    queue_length: 0,
    has_free_upload_slot: false,
    upload_permitted: Some(0),
  }
}

/// a configuration for a client that does not listen, so tests need no free port
pub fn options() -> ConfigBuilder {
  Config::builder().listener(false).handler(Answering)
}

/// The server's listening socket
pub struct FakeServer {
  listener: TcpListener,
}

impl FakeServer {
  pub async fn start() -> Self {
    Self {
      listener: TcpListener::bind("127.0.0.1:0").await.unwrap(),
    }
  }

  pub fn address(&self) -> SocketAddr {
    self.listener.local_addr().unwrap()
  }

  pub async fn accept(&self) -> ServerSide {
    let (stream, _) = tokio::time::timeout(PATIENCE, self.listener.accept())
      .await
      .expect("the client connects")
      .unwrap();
    ServerSide {
      stream,
      buffer: Vec::new(),
    }
  }
}

/// The server's end of one client connection
pub struct ServerSide {
  pub stream: TcpStream,
  buffer: Vec<u8>,
}

impl ServerSide {
  /// the next frame the client sent, whole
  pub async fn frame(&mut self) -> Vec<u8> {
    loop {
      if let Some((_, consumed)) =
        frame::split(FrameKind::Server, &self.buffer, &Limits::DEFAULT).unwrap()
      {
        return self.buffer.drain(..consumed).collect();
      }
      let mut chunk = [0; 4096];
      let read = tokio::time::timeout(PATIENCE, self.stream.read(&mut chunk))
        .await
        .expect("the client sends a message")
        .unwrap();
      assert!(read > 0, "the client closed the connection");
      self.buffer.extend_from_slice(&chunk[..read]);
    }
  }

  /// the next message `pick` accepts, skipping the others
  pub async fn request_matching<T>(
    &mut self,
    mut pick: impl FnMut(ServerRequest) -> Option<T>,
  ) -> T {
    loop {
      if let Some(picked) = pick(self.request().await) {
        return picked;
      }
    }
  }

  /// the next message the client sent, skipping write-liveness pings
  pub async fn request(&mut self) -> ServerRequest {
    loop {
      let request = ServerRequest::decode(&self.frame().await).unwrap();
      if request != ServerRequest::Ping {
        return request;
      }
    }
  }

  pub async fn send(&mut self, response: &ServerResponse) {
    self.send_bytes(&response.encode()).await;
  }

  pub async fn send_bytes(&mut self, bytes: &[u8]) {
    self.stream.write_all(bytes).await.unwrap();
  }

  /// reads the login and the listen port written with it
  pub async fn read_login(&mut self) -> (LoginRequest, u32) {
    let ServerRequest::Login(login) = self.request().await else {
      panic!("the first message is the login");
    };
    let ServerRequest::SetListenPort(port) = self.request().await else {
      panic!("the listen port follows the login");
    };
    (login, port.port)
  }

  pub async fn accept_login(&mut self) {
    self.read_login().await;
    self.send(&login_success()).await;
  }
}

pub fn login_success() -> ServerResponse {
  ServerResponse::Login(LoginResponse::Success {
    greeting: "welcome".into(),
    ip: PUBLIC_IP,
    hash: Secret::new("0123456789abcdef0123456789abcdef"),
    is_supporter: true,
  })
}

/// the next event that `pick` accepts, skipping the others and any lag
pub async fn next_event<T>(events: &mut Events, mut pick: impl FnMut(Event) -> Option<T>) -> T {
  tokio::time::timeout(PATIENCE, async {
    loop {
      match events.next().await.expect("the client is alive") {
        Ok(event) => {
          if let Some(picked) = pick(event) {
            return picked;
          }
        }
        Err(lagged) => eprintln!("{lagged}"),
      }
    }
  })
  .await
  .expect("the event is raised")
}
