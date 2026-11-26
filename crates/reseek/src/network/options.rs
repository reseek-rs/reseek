//! The client's configuration as the options give it. One mapping makes the configuration the
//! client starts with and the change a new set of options applies, so a client that started with
//! some options and one changed to them run with the same configuration.

use soulseek_client::soulseek_proto;
use std::net::IpAddr;
use std::time::Duration;

use soulseek_client::config::{ConfigBuilder, ConnectionBuilder, KeepAlive, Proxy};
use soulseek_client::transfer::RequestStyle;
use soulseek_proto::Limits;

use crate::config::{IncomingSearchThrottling, Options, Soulseek, enums};
use crate::network::resolvers::QUERY_BUDGET;

/// How many writes may wait on a server, peer or transfer connection. They are meant to have no
/// bound; this one only stops a stuck connection from growing memory without limit, and leaves
/// room for the distributed tree refusing a backlog of up to 1024 child requests at once on the
/// server connection.
pub const UNBOUNDED_WRITE_QUEUE: u32 = 10_000;

/// How long a request waits for the server's or a peer's reply.
const MESSAGE_TIMEOUT: Duration = Duration::from_secs(5);

/// The bytes of IPv4 and TCP headers, taken off the MTU for the server socket's segment size.
const HEADERS: u32 = 40;

/// The probes a distributed connection's keep-alive sends before it gives up.
const DISTRIBUTED_PROBES: u32 = 3;

/// The client settings `options` call for. The handler is set once in `Network::new`. Fails only
/// for options that did not pass validation.
pub fn settings(options: &Options) -> Result<Settings, String> {
  let soulseek = &options.soulseek;
  let listen_address: IpAddr = soulseek
    .listen_ip_address
    .parse()
    .map_err(|e| format!("soulseek.listen_ip_address: {e}"))?;
  let browse_message_bytes = u32::try_from(soulseek.limits.max_message_size.as_u64())
    .map_err(|_| "soulseek.limits.max_message_size: more than 4 GiB".to_string())?;
  Ok(Settings {
    soulseek: soulseek.clone(),
    incoming: options.throttling.search.incoming.clone(),
    listen_address,
    proxy: proxy(soulseek)?,
    decode: Limits {
      browse_message_bytes,
      ..Limits::DEFAULT
    },
  })
}

/// The client settings the options call for, checked, to apply to a client's configuration.
pub struct Settings {
  soulseek: Soulseek,
  incoming: IncomingSearchThrottling,
  listen_address: IpAddr,
  proxy: Option<Proxy>,
  decode: Limits,
}

impl Settings {
  /// `config` with every setting the options decide set as they say.
  pub fn apply(self, config: ConfigBuilder) -> ConfigBuilder {
    let soulseek = &self.soulseek;
    let limits = &soulseek.limits;
    let distributed = &soulseek.distributed_network;
    let proxy = &self.proxy;
    let (answers, answer_timeout) = search_answers(&self.incoming);
    config
      .server(soulseek.address.clone(), soulseek.port)
      .listener(true)
      .listen_address(self.listen_address)
      .listen_port(soulseek.listen_port)
      .distributed_network(!distributed.disabled)
      .distributed_children(!distributed.disable_children)
      .max_distributed_children(distributed.child_limit)
      .request_timeout(MESSAGE_TIMEOUT)
      .transfer_response_timeout(soulseek.connection.timeout.transfer_response)
      .server_connection(|c| connection(c, soulseek, proxy, Kind::Server))
      .peer_connection(|c| connection(c, soulseek, proxy, Kind::Peer))
      .transfer_connection(|c| connection(c, soulseek, proxy, Kind::Transfer))
      .incoming_connection(|c| connection(c, soulseek, proxy, Kind::Peer))
      .distributed_connection(|c| connection(c, soulseek, proxy, Kind::Distributed))
      .limits(|l| {
        l.decode(self.decode)
          .max_concurrent_peer_connects(limits.max_concurrent_peer_connects)
          .max_cached_peer_connections(limits.max_cached_peer_connections)
          .max_cached_search_responses(limits.search_response_cache_size)
          .max_cached_addresses(limits.user_endpoint_cache_size)
          .distributed_parent_negotiation_timeout(limits.distributed_parent_negotiation_timeout)
          .max_concurrent_search_answers(answers)
          .search_answer_timeout(answer_timeout)
      })
  }
}

/// How every download asks the peer for its file under `options`.
pub fn request_style(options: &Options) -> RequestStyle {
  match options.transfers.download.request_style {
    enums::RequestStyle::Queue => RequestStyle::Queue,
    enums::RequestStyle::TransferRequest => RequestStyle::TransferRequest,
  }
}

/// How many of other users' searches the client puts to the daemon at once, and how long it
/// gives each answer: room for every search the daemon lets in, its workers' and those waiting
/// in its queue, twice over for the ones it is refusing at the moment, so its own breaker, which
/// counts and logs a drop, is the one that drops; and time for a search at the back of a full
/// queue to wait its turn and be answered, so its own query budget is the one that gives up.
fn search_answers(incoming: &IncomingSearchThrottling) -> (u32, Duration) {
  let let_in = incoming
    .circuit_breaker
    .saturating_add(incoming.concurrency);
  let turns = incoming.circuit_breaker / incoming.concurrency.max(1) + 1;
  // the query, then the requester's address and our own figures, each one request
  let answer = QUERY_BUDGET + MESSAGE_TIMEOUT * 3;
  (let_in.saturating_mul(2), answer * turns)
}

/// The kinds of connection whose settings differ.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
  Server,
  Peer,
  Transfer,
  Distributed,
}

/// The settings of a `kind` of connection. Peer connections take the base settings; the server
/// connection never times out for inactivity, has `server_keepalive`, which notices a dead one,
/// and the MTU's segment size; transfers have their own buffers and timeout; distributed
/// connections have the configured write queue and a keep-alive from the inactivity timeout,
/// since a parent may be quiet.
fn connection(
  builder: ConnectionBuilder,
  soulseek: &Soulseek,
  proxy: &Option<Proxy>,
  kind: Kind,
) -> ConnectionBuilder {
  let connection = &soulseek.connection;
  let timeout = &connection.timeout;
  let buffer = &connection.buffer;
  let base = builder
    .read_buffer_size(buffer.read as usize)
    .write_buffer_size(buffer.write as usize)
    .write_queue_size(UNBOUNDED_WRITE_QUEUE)
    .connect_timeout(timeout.connect)
    .inactivity_timeout(timeout.inactivity)
    .proxy(proxy.clone())
    .keepalive(None)
    .max_segment_size(None);
  match kind {
    Kind::Peer => base,
    Kind::Server => {
      let keepalive = &soulseek.server_keepalive;
      base
        .inactivity_timeout(None)
        .max_segment_size(connection.mtu.map(|mtu| mtu - HEADERS))
        .keepalive(
          keepalive
            .enabled
            .then(|| KeepAlive::new(keepalive.idle, keepalive.interval, keepalive.probes)),
        )
    }
    Kind::Transfer => base
      .read_buffer_size(buffer.transfer as usize)
      .write_buffer_size(buffer.transfer as usize)
      .inactivity_timeout(timeout.transfer),
    Kind::Distributed => {
      // keep-alive takes whole seconds
      let seconds = Duration::from_secs(timeout.inactivity.as_secs());
      base
        .write_queue_size(buffer.write_queue)
        .keepalive(KeepAlive::new(seconds, seconds, DISTRIBUTED_PROBES))
    }
  }
}

/// The SOCKS5 proxy every outbound connection goes through, when it is on.
fn proxy(soulseek: &Soulseek) -> Result<Option<Proxy>, String> {
  let proxy = &soulseek.connection.proxy;
  if !proxy.enabled {
    return Ok(None);
  }
  let (Some(address), Some(port)) = (&proxy.address, proxy.port) else {
    return Err("soulseek.connection.proxy: enabled without an address and a port".into());
  };
  let options = Proxy::new(address.clone(), port);
  match (&proxy.username, &proxy.password) {
    (Some(username), Some(password)) => Ok(Some(options.credentials(username, password))),
    (None, None) => Ok(Some(options)),
    _ => Err("soulseek.connection.proxy: a username without a password or the reverse".into()),
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use soulseek_client::{Client, Config};

  fn options(toml: &str) -> Options {
    toml::from_str(toml).unwrap()
  }

  fn config(options: &Options) -> Config {
    settings(options)
      .unwrap()
      .apply(Config::builder())
      .build()
      .unwrap()
  }

  #[test]
  fn connection_sets_follow_the_kind_of_connection() {
    let client = config(&options(
      "[soulseek.connection]\nmtu = 1500\n[soulseek.connection.buffer]\ntransfer = 100000\n\
       write_queue = 75\n[soulseek.connection.timeout]\ninactivity = \"20s\"\ntransfer = \"45s\"\n",
    ));
    let server = client.server_connection();
    assert_eq!(server.inactivity_timeout(), None);
    assert_eq!(server.max_segment_size(), Some(1460));
    assert_eq!(server.write_queue_size(), UNBOUNDED_WRITE_QUEUE);
    let peer = client.peer_connection();
    assert_eq!(peer.write_queue_size(), UNBOUNDED_WRITE_QUEUE);
    assert_eq!(peer.inactivity_timeout(), Some(Duration::from_secs(20)));
    assert_eq!(peer.max_segment_size(), None);
    assert_eq!(client.incoming_connection(), peer);
    let transfer = client.transfer_connection();
    assert_eq!(
      (transfer.read_buffer_size(), transfer.write_buffer_size()),
      (100_000, 100_000)
    );
    assert_eq!(transfer.inactivity_timeout(), Some(Duration::from_secs(45)));
    assert_eq!(transfer.write_queue_size(), UNBOUNDED_WRITE_QUEUE);
    let distributed = client.distributed_connection();
    assert_eq!(distributed.write_queue_size(), 75);
    assert_eq!(
      distributed.keepalive(),
      Some(&KeepAlive::new(
        Duration::from_secs(20),
        Duration::from_secs(20),
        3
      ))
    );
  }

  #[test]
  fn the_client_leaves_dropping_and_giving_up_on_searches_to_the_daemon() {
    let client = config(&options(
      "[throttling.search.incoming]\nconcurrency = 10\ncircuit_breaker = 500\n",
    ));
    let limits = client.limits();
    assert_eq!(limits.max_concurrent_search_answers(), 1020);
    // 51 turns of a 10 s query and three 5 s requests
    assert_eq!(limits.search_answer_timeout(), Duration::from_secs(51 * 25));
  }

  /// A client started with the options of `previous` and changed to `current` runs with the
  /// configuration a client started with `current` has.
  async fn changed_like_started(previous: &Options, current: &Options) {
    let client = Client::new(config(previous));
    let applied = client
      .reconfigure(|config| settings(current).unwrap().apply(config))
      .await
      .unwrap();
    assert!(
      !applied.after_reconnect,
      "a disconnected client never needs a reconnect"
    );
    assert_eq!(
      format!("{:?}", client.config()),
      format!("{:?}", config(current))
    );
    client.shutdown().await;
  }

  #[tokio::test]
  async fn the_change_gives_the_configuration_a_start_would() {
    let previous = Options::default();
    for current in [
      "[soulseek.connection.buffer]\ntransfer = 90000\nwrite_queue = 9\n",
      "[soulseek.connection.timeout]\ninactivity = \"30s\"\nconnect = \"3s\"\n",
      "[soulseek.connection]\nmtu = 1400\n",
      "[soulseek.connection.proxy]\nenabled = true\naddress = \"proxy\"\nport = 1080\n\
       username = \"u\"\npassword = \"p\"\n",
      "[soulseek.distributed_network]\ndisabled = true\nchild_limit = 5\n",
      "[transfers.upload]\nslots = 2\nspeed_limit = 50\n[transfers.download]\nslots = 7\n",
      "[soulseek.server_keepalive]\nprobes = 9\n[soulseek.limits]\nmax_cached_peer_connections = 7\n",
      "[soulseek]\nlisten_port = 50301\n",
      "[soulseek]\naddress = \"127.0.0.1\"\nport = 2242\n",
      "[throttling.search.incoming]\ncircuit_breaker = 40\n",
    ] {
      changed_like_started(&previous, &options(current)).await;
      changed_like_started(&options(current), &previous).await;
    }
  }
}
