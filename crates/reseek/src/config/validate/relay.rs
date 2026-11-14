//! Relay mode: a controller's agents, and the timeouts and limits it holds them to.

use std::collections::BTreeMap;
use std::time::Duration;

use reseek_relay_proto::check_instance_name;

use super::Report;
use super::fields::{length, required};
use crate::config::cidr::parse_trimmed_list;
use crate::config::{Options, Relay};

pub(super) fn check(options: &Options, report: &mut Report) {
  let relay = &options.relay;
  check_agents(relay, report);
  let timeouts = &relay.controller_timeouts;
  for (key, value) in [
    ("relay.controller_timeouts.file_info", timeouts.file_info),
    ("relay.controller_timeouts.file_ack", timeouts.file_ack),
    (
      "relay.controller_timeouts.file_stream",
      timeouts.file_stream,
    ),
    (
      "relay.controller_timeouts.sync_deferral",
      timeouts.sync_deferral,
    ),
  ] {
    if value < Duration::from_millis(1) {
      report.add(key, format!("The field {key} must not be empty."));
    }
  }
}

fn check_agents(relay: &Relay, report: &mut Report) {
  let mut names: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
  for (name, agent) in &relay.agents {
    let key = &format!("relay.agents.{name}");
    let field = |f: &str| format!("{key}.{f}");
    let instance = agent.instance_name.as_deref();
    report.check(key, required(&field("instance_name"), instance));
    report.check(key, length(&field("instance_name"), instance, 1, 255));
    if let Some(instance) = instance.filter(|i| !i.is_empty()) {
      report.check(key, check_instance_name(&field("instance_name"), instance));
    }
    report.check(key, required(&field("secret"), agent.secret.as_deref()));
    report.check(
      key,
      length(&field("secret"), agent.secret.as_deref(), 16, 255),
    );
    report.check(
      key,
      parse_trimmed_list(&agent.cidr)
        .map(drop)
        .map_err(|(element, e)| format!("CIDR {element} is invalid: {e}.")),
    );
    if let Some(instance) = instance {
      names.entry(instance).or_default().push(name);
    }
  }
  for agents in names.values().filter(|agents| agents.len() > 1) {
    for name in agents {
      report.add(
        &format!("relay.agents.{name}"),
        "One or more agent instance names are duplicated. Instance names must be unique.",
      );
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::config::RelayAgent;

  fn problems(options: &Options) -> Vec<(String, String)> {
    let mut report = Report::default();
    check(options, &mut report);
    report.0.into_iter().map(|i| (i.key, i.message)).collect()
  }

  #[test]
  fn agents_need_unique_names_and_secrets() {
    let mut options = Options::default();
    let agent = |name: &str| RelayAgent {
      instance_name: Some(name.into()),
      secret: Some("0123456789abcdef".into()),
      ..RelayAgent::default()
    };
    options.relay.agents.insert("a".into(), agent("same"));
    options.relay.agents.insert("b".into(), agent("same"));
    options.relay.agents.insert(
      "c".into(),
      RelayAgent {
        cidr: "10.0.0.0/8, ::ffff:0:0/96".into(),
        ..RelayAgent::default()
      },
    );
    let duplicated =
      "One or more agent instance names are duplicated. Instance names must be unique.";
    assert_eq!(
      problems(&options),
      [
        (
          "relay.agents.c",
          "The field relay.agents.c.instance_name must contain a value."
        ),
        (
          "relay.agents.c",
          "The field relay.agents.c.secret must contain a value."
        ),
        (
          "relay.agents.c",
          "CIDR ::ffff:0:0/96 is invalid: IPv4 mapped IPv6 addresses are not allowed."
        ),
        ("relay.agents.a", duplicated),
        ("relay.agents.b", duplicated),
      ]
      .map(|(key, message)| (key.to_string(), message.to_string())),
      "each agent is named, both of a duplicate"
    );
  }
}
