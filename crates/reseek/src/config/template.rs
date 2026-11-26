//! The commented-out configuration file written on first start: every option with its help and
//! its default, grouped by table, so the file documents itself.

use cascade::{Field, Kind, Schema};
use serde_json::Value;

/// The template's text.
pub fn render(schema: &Schema) -> String {
  let mut text = String::from(
    "# reseek configuration.\n#\n# Every option is listed below with its default, commented out. \
     Uncomment a line and\n# change its value to set it. Files in config.d/ next to this one \
     are read after it, in\n# name order, and win over it. Environment variables and \
     command-line flags are listed by\n# `reseek --envars` and `reseek --help`.\n\n",
  );
  // each table's keys are written together under one header, before the user-keyed maps in
  // it, since TOML refuses a table header given twice
  let mut tables: Vec<(&str, Vec<&Field>, Vec<&Field>)> = Vec::new();
  for field in schema.fields() {
    let section = field
      .key
      .rsplit_once('.')
      .map_or("", |(section, _)| section);
    let at = match tables.iter().position(|(name, ..)| *name == section) {
      Some(at) => at,
      None => {
        tables.push((section, Vec::new(), Vec::new()));
        tables.len() - 1
      }
    };
    let (_, keys, maps) = &mut tables[at];
    if field.kind == Kind::Map {
      maps.push(field);
    } else {
      keys.push(field);
    }
  }
  // keys outside every table must come before the first table header
  tables.sort_by_key(|(section, ..)| !section.is_empty());
  for (section, keys, maps) in tables {
    if !section.is_empty() && !keys.is_empty() {
      text.push_str(&format!("\n# [{section}]\n"));
    }
    for field in keys {
      let name = field
        .key
        .rsplit_once('.')
        .map_or(field.key.as_str(), |(_, name)| name);
      doc(&mut text, field);
      text.push_str(&format!("# {name} = {}\n", toml_value(&field.default)));
    }
    for field in maps {
      text.push('\n');
      doc(&mut text, field);
      text.push_str(&format!("# [{}.<name>]\n", field.key));
      for entry in &field.entries {
        text.push_str(&format!(
          "# {} = {}\n",
          entry.key,
          toml_value(&entry.default)
        ));
      }
    }
  }
  text
}

fn doc(text: &mut String, field: &Field) {
  for line in field.doc.lines().filter(|l| !l.trim().is_empty()) {
    text.push_str(&format!("# {}\n", line.trim()));
  }
}

/// A default as TOML; a key with no default is left without a value to fill in.
fn toml_value(value: &Value) -> String {
  match value {
    Value::Null => String::new(),
    Value::Object(_) => "{}".to_string(),
    other => other.to_string(),
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::config::Options;

  #[test]
  fn the_template_is_all_comments_and_lists_each_option_under_its_table() {
    let text = render(&Schema::of::<Options>());
    assert!(text.parse::<toml::Table>().unwrap().is_empty());
    let web = text.find("# [web]\n").unwrap();
    let port = text.find("# port = 5030\n").unwrap();
    assert!(web < port);
    assert!(text.contains("# [web.authentication.api_keys.<name>]\n"));
    assert!(text.contains("# debug = false\n"));
    let headers: Vec<&str> = text.lines().filter(|l| l.starts_with("# [")).collect();
    let mut unique = headers.clone();
    unique.sort_unstable();
    unique.dedup();
    assert_eq!(
      headers.len(),
      unique.len(),
      "a table header is written twice"
    );
  }
}
