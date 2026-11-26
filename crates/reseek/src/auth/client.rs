//! Who is on the other end of a request: the client's address and the host it asked for, after
//! the headers of trusted reverse proxies.

use std::net::IpAddr;

use axum::http::{HeaderMap, Uri, header};

use crate::config::cidr::{IpRange, unmapped};
use crate::http::{Peer, Transport};

/// The client of a request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Client {
  /// the client's address; a request over the unix socket comes from `127.0.0.1`
  pub ip: IpAddr,
  /// the host the request was sent to, without its port
  pub host: Option<String>,
  /// the host the request was sent to as the client wrote it, with any port
  pub authority: Option<String>,
  /// whether the client spoke HTTPS, to us or to the proxy in front
  pub https: bool,
}

/// The client of a request that reached us from `peer`. `X-Forwarded-For`,
/// `X-Forwarded-Proto` and `X-Forwarded-Host` are believed only when the peer is one of
/// `proxies`. The client is then the rightmost address of `X-Forwarded-For`, read across all
/// its header lines, that is not a trusted proxy; a hop that is not an address stops the walk
/// at the trusted address to its right.
pub fn resolve(peer: &Peer, uri: &Uri, headers: &HeaderMap, proxies: &[IpRange]) -> Client {
  let peer_ip = peer.ip();
  let host_header = headers
    .get(header::HOST)
    .and_then(|h| h.to_str().ok())
    .map(str::to_string)
    .or_else(|| uri.authority().map(|a| a.to_string()));
  let direct = Client {
    ip: peer_ip,
    host: host_header.as_deref().map(without_port),
    authority: host_header,
    https: peer.transport == Transport::Tls,
  };
  let trusted = |ip: IpAddr| proxies.iter().any(|range| range.contains(ip));
  if !trusted(peer_ip) {
    return direct;
  }
  // a proxy may add its own header line instead of joining the existing one, so the last line
  // is the one the nearest proxy wrote
  let forwarded = |name: &str| {
    headers
      .get_all(name)
      .iter()
      .next_back()
      .and_then(|h| h.to_str().ok())
      .map(str::trim)
      .filter(|v| !v.is_empty())
  };
  let mut ip = peer_ip;
  let hops = headers
    .get_all("x-forwarded-for")
    .iter()
    .rev()
    // a line that is not text reads as a single hop that is not an address
    .flat_map(|line| line.to_str().unwrap_or("").rsplit(','));
  // walk from the nearest hop and stop at the first one that is not a trusted proxy. a hop that
  // is not an address ends the walk, since nothing to its left was written by a trusted proxy
  for hop in hops {
    let Ok(hop) = without_port(hop.trim()).parse::<IpAddr>() else {
      break;
    };
    ip = unmapped(hop);
    if !trusted(ip) {
      break;
    }
  }
  let https = match forwarded("x-forwarded-proto") {
    Some(proto) => proto.eq_ignore_ascii_case("https"),
    None => direct.https,
  };
  let (host, authority) = match forwarded("x-forwarded-host") {
    Some(forwarded) => (Some(without_port(forwarded)), Some(forwarded.to_string())),
    None => (direct.host, direct.authority),
  };
  Client {
    ip,
    host,
    authority,
    https,
  }
}

/// `host` without a `:port`, and without the brackets of an IPv6 address.
pub fn without_port(host: &str) -> String {
  if host.parse::<IpAddr>().is_ok() {
    return host.to_string();
  }
  if let Some(rest) = host.strip_prefix('[') {
    return rest.split(']').next().unwrap_or(rest).to_string();
  }
  match host.rsplit_once(':') {
    Some((name, port)) if port.chars().all(|c| c.is_ascii_digit()) => name.to_string(),
    _ => host.to_string(),
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use axum::http::HeaderValue;
  use std::net::{Ipv4Addr, SocketAddr};

  fn peer(addr: &str) -> Peer {
    Peer {
      remote: Some(addr.parse::<SocketAddr>().unwrap()),
      transport: Transport::Plain,
    }
  }

  fn headers(items: &[(&'static str, &'static str)]) -> HeaderMap {
    let mut map = HeaderMap::new();
    for (name, value) in items {
      map.append(*name, HeaderValue::from_static(value));
    }
    map
  }

  #[test]
  fn forwarded_headers_count_only_from_trusted_proxies() {
    let proxies = vec![
      "127.0.0.1/32".parse().unwrap(),
      "10.0.0.0/8".parse().unwrap(),
    ];
    let h = headers(&[
      ("host", "127.0.0.1:5030"),
      ("x-forwarded-for", "203.0.113.9, 198.51.100.1, 10.0.0.2"),
      ("x-forwarded-proto", "https"),
      ("x-forwarded-host", "reseek.example.net"),
    ]);
    let uri = Uri::from_static("/api/v0/options");
    let via_proxy = resolve(&peer("127.0.0.1:40000"), &uri, &h, &proxies);
    assert_eq!(via_proxy.ip, "198.51.100.1".parse::<IpAddr>().unwrap());
    assert_eq!(via_proxy.host.as_deref(), Some("reseek.example.net"));
    assert_eq!(via_proxy.authority.as_deref(), Some("reseek.example.net"));
    assert!(via_proxy.https);
    let direct = resolve(&peer("192.0.2.7:40000"), &uri, &h, &proxies);
    assert_eq!(direct.ip, "192.0.2.7".parse::<IpAddr>().unwrap());
    assert_eq!(direct.host.as_deref(), Some("127.0.0.1"));
    assert_eq!(direct.authority.as_deref(), Some("127.0.0.1:5030"));
    assert!(!direct.https);
  }

  #[test]
  fn every_forwarded_for_line_counts_and_a_bad_hop_ends_the_walk() {
    let proxies = vec!["127.0.0.1/32".parse().unwrap()];
    let uri = Uri::from_static("/");
    let from_proxy = |h: &HeaderMap| resolve(&peer("127.0.0.1:40000"), &uri, h, &proxies).ip;
    let ip = |s: &str| s.parse::<IpAddr>().unwrap();
    // the first line is the client's own, the second the proxy's
    let two_lines = headers(&[
      ("x-forwarded-for", "192.168.1.50"),
      ("x-forwarded-for", "203.0.113.9"),
      ("x-forwarded-proto", "http"),
      ("x-forwarded-proto", "https"),
    ]);
    assert_eq!(from_proxy(&two_lines), ip("203.0.113.9"));
    assert!(resolve(&peer("127.0.0.1:40000"), &uri, &two_lines, &proxies).https);
    let ported = headers(&[("x-forwarded-for", "192.168.1.50, 203.0.113.9:51234")]);
    assert_eq!(from_proxy(&ported), ip("203.0.113.9"));
    let ported_v6 = headers(&[("x-forwarded-for", "192.168.1.50, [2001:db8::1]:443")]);
    assert_eq!(from_proxy(&ported_v6), ip("2001:db8::1"));
    let unknown = headers(&[("x-forwarded-for", "192.168.1.50, unknown")]);
    assert_eq!(from_proxy(&unknown), ip("127.0.0.1"));
    let behind_trusted = headers(&[("x-forwarded-for", "192.168.1.50, unknown, 127.0.0.1")]);
    assert_eq!(from_proxy(&behind_trusted), ip("127.0.0.1"));
  }

  #[test]
  fn the_unix_socket_is_local_and_hosts_lose_their_port() {
    let unix = Peer {
      remote: None,
      transport: Transport::Unix,
    };
    let client = resolve(
      &unix,
      &Uri::from_static("/"),
      &headers(&[("host", "[::1]:5030")]),
      &[],
    );
    assert_eq!(client.ip, IpAddr::V4(Ipv4Addr::LOCALHOST));
    assert_eq!(client.host.as_deref(), Some("::1"));
    assert_eq!(without_port("localhost:5030"), "localhost");
    assert_eq!(without_port("example.net"), "example.net");
    assert_eq!(without_port("[::1]"), "::1");
    assert_eq!(without_port("[fe80::1]:5030"), "fe80::1");
    assert_eq!(without_port("::1"), "::1");
    assert_eq!(without_port("192.0.2.1:80"), "192.0.2.1");
  }
}
