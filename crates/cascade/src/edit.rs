//! Key-level edits to configuration files that keep their comments and formatting.

use std::collections::BTreeMap;

use serde_json::Value;
use toml_edit::{ArrayOfTables, DocumentMut, Item, Table};

use crate::schema::Schema;
use crate::{Error, Key, tree};

/// Edits configuration files as text, keeping their comments and formatting.
#[derive(Clone, Copy, Debug)]
pub struct FileEditor<'a> {
  schema: &'a Schema,
}

impl<'a> FileEditor<'a> {
  /// An editor for files of the configuration `schema` describes.
  pub fn new(schema: &'a Schema) -> Self {
    Self { schema }
  }

  /// The file `name` holding `text`, with each key of `set` given its value and each key of
  /// `unset` removed. Only those keys change; tables are added where needed.
  ///
  /// For a secret key, `*****` (what a secret is shown as) leaves the value as it is and an
  /// empty string removes it. Null removes any key.
  pub fn patch(
    &self,
    name: &str,
    text: &str,
    set: &BTreeMap<Key, Value>,
    unset: &[Key],
  ) -> Result<String, Error> {
    let mut doc: DocumentMut = text
      .parse()
      .map_err(|e: toml_edit::TomlError| Error::Parse {
        file: name.to_string(),
        message: tree::parse_error(text, e.message(), e.span()),
      })?;
    let had_items = !doc.is_empty();
    let had_values = doc.iter().any(|(_, item)| item.is_value());
    let mut removals: Vec<&str> = unset.iter().map(String::as_str).collect();
    // the first table this edit adds at the top level, which goes below comments that end the
    // file
    let mut first_new: Option<String> = None;
    for (key, value) in set {
      let field = self
        .schema
        .field(key)
        .ok_or_else(|| Error::UnknownKey(key.clone()))?;
      if field.keeps(value) {
        continue;
      }
      if value.is_null() || (field.secret && value.as_str() == Some("")) {
        removals.push(key);
        continue;
      }
      let item = to_item(key, value)?;
      let added = set_item(&mut doc, name, key, item)?;
      if let Some(top) = added {
        first_new.get_or_insert(top);
      }
    }
    for key in removals {
      let parts: Vec<&str> = key.split('.').collect();
      remove_key(&mut doc, &parts);
    }
    let first_value = (!had_values)
      .then(|| {
        doc
          .iter()
          .find(|(_, item)| item.is_value())
          .map(|(k, _)| k.to_string())
      })
      .flatten();
    if let Some(key) = first_value {
      keep_banner_first(&mut doc, &key, had_items);
    } else if let Some(top) = first_new {
      move_trailing_comments(&mut doc, &top);
    }
    Ok(doc.to_string())
  }

  /// Whether `text` sets any secret to a non-empty value. Text that does not parse counts as
  /// holding one, so it is never written readable by others.
  pub(crate) fn holds_secrets(&self, text: &str) -> bool {
    let tree = match text.parse::<toml::Table>() {
      Ok(table) => tree::from_toml(toml::Value::Table(table)),
      Err(_) => return true,
    };
    match tree {
      Ok(tree) => self.schema.redact(&tree) != tree,
      Err(_) => true,
    }
  }
}

/// Puts `item` at the dotted `key`, adding the tables on the way as `[section]` headers.
/// Returns the top-level table's name when this added one.
fn set_item(
  doc: &mut DocumentMut,
  name: &str,
  key: &str,
  item: Item,
) -> Result<Option<String>, Error> {
  let parts: Vec<&str> = key.split('.').collect();
  let (last, tables) = parts.split_last().expect("split always yields one part");
  let mut node = doc.as_item_mut();
  let mut added_top = None;
  for (depth, part) in tables.iter().enumerate() {
    // indexing into a value as a table panics
    if node
      .get(part)
      .is_some_and(|i| !i.is_none() && !i.is_table_like())
    {
      return Err(Error::Value {
        key: key.to_string(),
        by: name.to_string(),
        message: format!("`{part}` is not a section in the file"),
      });
    }
    if node.is_table() && node.get(part).is_none_or(Item::is_none) {
      // a real `[section]` after its parent's, implicit while it only holds sections so it
      // prints no empty header; inside an inline table, indexing adds an inline table instead
      let mut table = Table::new();
      table.set_implicit(depth + 1 < tables.len());
      node[part] = Item::Table(table);
      if depth == 0 {
        added_top = Some(part.to_string());
      }
    }
    node = &mut node[part];
  }
  if node.get(last).is_none_or(Item::is_none)
    && item.is_table()
    && node.is_table()
    && tables.is_empty()
  {
    added_top = Some(last.to_string());
  }
  let in_inline = node.is_inline_table();
  let path: Vec<Step> = tables.iter().map(|t| Step::Table(t.to_string())).collect();
  if table_at(doc, &path).is_some() {
    merge(doc, &path, last, item);
    return Ok(added_top);
  }
  let node = tables
    .iter()
    .fold(doc.as_item_mut(), |node, part| &mut node[part]);
  let item = match item {
    // a table inside an inline table has to be inline too
    Item::Table(table) if in_inline => {
      Item::Value(toml_edit::Value::InlineTable(table.into_inline_table()))
    }
    Item::ArrayOfTables(tables) if in_inline => {
      Item::Value(toml_edit::Value::Array(tables.into_array()))
    }
    other => other,
  };
  let item = with_old_decor(item, node.get(last));
  node[last] = item;
  Ok(added_top)
}

/// `new` with the spacing and trailing comment of `old` when both are values.
fn with_old_decor(new: Item, old: Option<&Item>) -> Item {
  match (new, old.and_then(Item::as_value)) {
    (Item::Value(mut value), Some(old)) => {
      *value.decor_mut() = old.decor().clone();
      Item::Value(value)
    }
    (new, _) => new,
  }
}

/// A step from a table to one under it: a `[section]`, or an element of a `[[section]]` list.
#[derive(Clone, Debug, PartialEq)]
enum Step {
  Table(String),
  Element(String, usize),
}

impl Step {
  fn key(&self) -> &str {
    match self {
      Step::Table(key) | Step::Element(key, _) => key,
    }
  }
}

/// A line of the file that the comments above it belong to: the header of the table at a path,
/// or a `key = value` in the table at a path.
#[derive(Clone, Debug, PartialEq)]
enum Line {
  Header(Vec<Step>),
  Value(Vec<Step>, String),
}

impl Line {
  /// Whether the line is `key` of the table at `path`, or inside it.
  fn is_under(&self, path: &[Step], key: &str) -> bool {
    let below = |steps: &[Step]| {
      steps.len() > path.len() && steps.starts_with(path) && steps[path.len()].key() == key
    };
    match self {
      Line::Header(steps) => below(steps),
      Line::Value(steps, name) => (steps == path && name == key) || below(steps),
    }
  }
}

/// The table at `path`, a `[section]` or an element of a `[[section]]` list.
fn table_at<'a>(doc: &'a DocumentMut, path: &[Step]) -> Option<&'a Table> {
  path
    .iter()
    .try_fold(doc.as_table(), |table, step| match step {
      Step::Table(key) => table.get(key)?.as_table(),
      Step::Element(key, index) => table.get(key)?.as_array_of_tables()?.get(*index),
    })
}

/// [`table_at`], to change the table.
fn table_at_mut<'a>(doc: &'a mut DocumentMut, path: &[Step]) -> Option<&'a mut Table> {
  path
    .iter()
    .try_fold(doc.as_table_mut(), |table, step| match step {
      Step::Table(key) => table.get_mut(key)?.as_table_mut(),
      Step::Element(key, index) => table
        .get_mut(key)?
        .as_array_of_tables_mut()?
        .get_mut(*index),
    })
}

/// The headers and values of the file in the order they print. toml_edit orders tables by
/// where each was read, a table added since coming right after the one visited before it.
fn printed(doc: &DocumentMut) -> Vec<Line> {
  let mut tables = Vec::new();
  let mut position = 0;
  collect_tables(doc.as_table(), &mut Vec::new(), &mut position, &mut tables);
  tables.sort_by_key(|(position, path, _)| (!path.is_empty(), *position));
  let mut lines = Vec::new();
  for (_, path, table) in tables {
    // an implicit table prints no header until it holds a value
    if !path.is_empty() && !(table.is_implicit() && table.get_values().is_empty()) {
      lines.push(Line::Header(path.clone()));
    }
    for (key, item) in table.iter() {
      if item.is_value() {
        lines.push(Line::Value(path.clone(), key.to_string()));
      }
    }
  }
  lines
}

/// Each table at or under `table` that is not dotted, with its path and the position it prints
/// at.
fn collect_tables<'a>(
  table: &'a Table,
  path: &mut Vec<Step>,
  position: &mut isize,
  tables: &mut Vec<(isize, Vec<Step>, &'a Table)>,
) {
  if !table.is_dotted() {
    if let Some(at) = table.position() {
      *position = at;
    }
    tables.push((*position, path.clone(), table));
  }
  for (key, item) in table.iter() {
    match item {
      Item::Table(inner) => {
        path.push(Step::Table(key.to_string()));
        collect_tables(inner, path, position, tables);
        path.pop();
      }
      Item::ArrayOfTables(list) => {
        for (index, inner) in list.iter().enumerate() {
          path.push(Step::Element(key.to_string(), index));
          collect_tables(inner, path, position, tables);
          path.pop();
        }
      }
      _ => {}
    }
  }
}

/// The comments and blank lines above `line`.
fn prefix(doc: &DocumentMut, line: &Line) -> String {
  let decor = match line {
    Line::Header(path) => table_at(doc, path).map(Table::decor),
    Line::Value(path, key) => table_at(doc, path)
      .and_then(|table| table.key(key))
      .map(toml_edit::Key::leaf_decor),
  };
  decor
    .and_then(|decor| decor.prefix())
    .and_then(|prefix| prefix.as_str())
    .unwrap_or_default()
    .to_string()
}

fn set_prefix(doc: &mut DocumentMut, line: &Line, prefix: String) {
  match line {
    Line::Header(path) => {
      if let Some(table) = table_at_mut(doc, path) {
        table.decor_mut().set_prefix(prefix);
      }
    }
    Line::Value(path, key) => {
      if let Some(mut key) = table_at_mut(doc, path).and_then(|table| table.key_mut(key)) {
        key.leaf_decor_mut().set_prefix(prefix);
      }
    }
  }
}

/// Splits the comments above an item after its last blank line: what comes before is not about
/// the item, and is empty when it holds no comment.
fn split_at_gap(prefix: &str) -> (&str, &str) {
  let mut end = 0;
  let mut offset = 0;
  for line in prefix.split_inclusive('\n') {
    offset += line.len();
    if line.ends_with('\n') && line.trim().is_empty() {
      end = offset;
    }
  }
  let (before, after) = prefix.split_at(end);
  if before.trim().is_empty() {
    ("", after)
  } else {
    (before, after)
  }
}

/// Puts `comments`, which end in a blank line, above `line`, or at the end of the file when it
/// is `None`. The blank lines that opened what was there go, so the gap is not doubled.
fn put_above(doc: &mut DocumentMut, line: Option<&Line>, comments: &str) {
  if comments.is_empty() {
    return;
  }
  let below = match line {
    Some(line) => prefix(doc, line),
    None => doc.trailing().as_str().unwrap_or_default().to_string(),
  };
  let mut rest = below.as_str();
  while let Some(blank) = rest
    .split_inclusive('\n')
    .next()
    .filter(|first| first.ends_with('\n') && first.trim().is_empty())
  {
    rest = &rest[blank.len()..];
  }
  let joined = format!("{comments}{rest}");
  match line {
    Some(line) => set_prefix(doc, line, joined),
    None => doc.set_trailing(joined),
  }
}

/// Sets `key` in the table at `path` to `new`, changing only what differs so the comments on
/// what stays are kept: a table is changed key by key, a list of tables of the same length
/// element by element.
fn merge(doc: &mut DocumentMut, path: &[Step], key: &str, new: Item) {
  let table = table_at_mut(doc, path).expect("the caller walked or made the path");
  match (table.get_mut(key), new) {
    (Some(Item::Table(old)), Item::Table(new)) if !old.is_dotted() => {
      // an empty table, or one that holds values, prints its header
      if !new.is_implicit() {
        old.set_implicit(false);
      }
      let mut inner = path.to_vec();
      inner.push(Step::Table(key.to_string()));
      merge_table(doc, &inner, new);
    }
    (Some(Item::ArrayOfTables(old)), Item::ArrayOfTables(new)) if old.len() == new.len() => {
      for (index, new) in new.into_iter().enumerate() {
        let mut inner = path.to_vec();
        inner.push(Step::Element(key.to_string(), index));
        merge_table(doc, &inner, new);
      }
    }
    (Some(old @ Item::Value(_)), new @ Item::Value(_)) => {
      let new = with_old_decor(new, Some(old));
      *old = new;
    }
    (None, new) => {
      table.insert(key, new);
    }
    (Some(_), new) => replace(doc, path, key, new),
  }
}

/// [`merge`] for each key of `new` into the table at `path`, then the removal of those `new`
/// lacks.
fn merge_table(doc: &mut DocumentMut, path: &[Step], new: Table) {
  let old = table_at(doc, path).expect("the caller found the table");
  let gone: Vec<String> = old
    .iter()
    .map(|(key, _)| key.to_string())
    .filter(|key| !new.contains_key(key))
    .collect();
  for (key, item) in new {
    merge(doc, path, &key, item);
  }
  for key in gone {
    remove_entry(doc, path, &key);
  }
}

/// Replaces `key` in the table at `path` with `new`, of another shape. The comments above the
/// old item go above the new one, but when one is a header and the other a value, which print
/// in different places, those a blank line sets apart from the old item stay where it was.
fn replace(doc: &mut DocumentMut, path: &[Step], key: &str, mut new: Item) {
  let lines = printed(doc);
  let first = lines.iter().position(|line| line.is_under(path, key));
  let old_prefix = first.map(|i| prefix(doc, &lines[i])).unwrap_or_default();
  let next = first.and_then(|i| {
    lines[i + 1..]
      .iter()
      .find(|line| !line.is_under(path, key))
      .cloned()
  });
  // the position of the old item's first header, when it printed one
  let old_header = first.and_then(|i| match &lines[i] {
    Line::Header(steps) => Some(table_at(doc, steps).and_then(Table::position)),
    Line::Value(..) => None,
  });
  let new_header = match &mut new {
    Item::Table(table) => Some(table),
    Item::ArrayOfTables(list) => list.iter_mut().next(),
    _ => None,
  };
  let same_place = old_header.is_some() == new_header.is_some();
  // a header put in place of another prints where that one did
  if let (Some(Some(position)), Some(table)) = (old_header, new_header) {
    table.set_position(Some(position));
  }
  let (stays, travels) = if same_place {
    ("", old_prefix.as_str())
  } else {
    split_at_gap(&old_prefix)
  };
  let table = table_at_mut(doc, path).expect("the caller found the table");
  if old_header.is_some()
    && new.is_value()
    && let Some(mut name) = table.key_mut(key)
  {
    // a header's key is spaced for inside its brackets
    name.leaf_decor_mut().clear();
  }
  table.insert(key, new);
  if let Some(line) = printed(doc).iter().find(|line| line.is_under(path, key)) {
    set_prefix(doc, line, travels.to_string());
  }
  put_above(doc, next.as_ref(), stays);
}

/// Removes `key` from the table at `path`. The comments above it that a blank line sets apart
/// from it are not about it, and go above what follows.
fn remove_entry(doc: &mut DocumentMut, path: &[Step], key: &str) {
  let lines = printed(doc);
  let first = lines.iter().position(|line| line.is_under(path, key));
  let stays = first
    .map(|i| split_at_gap(&prefix(doc, &lines[i])).0.to_string())
    .unwrap_or_default();
  let next = first.and_then(|i| {
    lines[i + 1..]
      .iter()
      .find(|line| !line.is_under(path, key))
      .cloned()
  });
  if let Some(table) = table_at_mut(doc, path) {
    table.remove(key);
  }
  put_above(doc, next.as_ref(), &stays);
}

/// A file that ends in comments (often all of it, a file of commented-out examples) keeps them
/// at the end: they move in front of the first header the edit added under `top`.
fn move_trailing_comments(doc: &mut DocumentMut, top: &str) {
  let trailing = doc.trailing().as_str().unwrap_or_default().to_string();
  if trailing.trim().is_empty() {
    return;
  }
  let Some(table) = doc.get_mut(top).and_then(|item| first_header(item)) else {
    return;
  };
  let prefix = table
    .decor()
    .prefix()
    .and_then(|p| p.as_str())
    .unwrap_or_default()
    .to_string();
  // a blank line between the comments and the table
  let gap = if trailing.ends_with("\n\n") { "" } else { "\n" };
  table
    .decor_mut()
    .set_prefix(format!("{trailing}{gap}{prefix}"));
  doc.set_trailing("");
}

/// A value added at the top level of a file that had none prints above everything else, so the
/// comments that opened the file move in front of it: the whole text of a file that was only
/// comments, else what comes before the first blank line above the first table.
fn keep_banner_first(doc: &mut DocumentMut, key: &str, had_items: bool) {
  let banner = if had_items {
    let Some(table) = first_printed_table(doc) else {
      return;
    };
    let prefix = table
      .decor()
      .prefix()
      .and_then(|p| p.as_str())
      .unwrap_or_default()
      .to_string();
    let Some(end) = prefix.rfind("\n\n") else {
      return;
    };
    table.decor_mut().set_prefix(prefix[end + 2..].to_string());
    prefix[..end + 2].to_string()
  } else {
    let trailing = doc.trailing().as_str().unwrap_or_default().to_string();
    doc.set_trailing("");
    let gap = if trailing.is_empty() || trailing.ends_with("\n\n") {
      ""
    } else {
      "\n"
    };
    format!("{trailing}{gap}")
  };
  if let Some(mut key) = doc.as_table_mut().key_mut(key) {
    let decor = key.leaf_decor_mut();
    let existing = decor
      .prefix()
      .and_then(|p| p.as_str())
      .unwrap_or_default()
      .to_string();
    decor.set_prefix(format!("{banner}{existing}"));
  }
}

/// The table whose header comes first in the file.
fn first_printed_table(doc: &mut DocumentMut) -> Option<&mut Table> {
  let first = doc
    .iter()
    .filter_map(|(key, item)| Some((header_position(item)?, key.to_string())))
    .min()?
    .1;
  doc.get_mut(&first).and_then(first_header)
}

/// Where the first header at or under `item` is in the file it was read from.
fn header_position(item: &Item) -> Option<isize> {
  match item {
    Item::Table(table) if table.is_implicit() => table
      .iter()
      .filter_map(|(_, inner)| header_position(inner))
      .min(),
    Item::Table(table) => table.position(),
    Item::ArrayOfTables(tables) => tables.iter().next().and_then(Table::position),
    _ => None,
  }
}

/// The first table at or under `item` that prints a header; an implicit table prints none.
fn first_header(item: &mut Item) -> Option<&mut Table> {
  match item {
    Item::Table(table) => {
      if table.is_implicit() {
        table.iter_mut().find_map(|(_, inner)| first_header(inner))
      } else {
        Some(table)
      }
    }
    Item::ArrayOfTables(tables) => tables.iter_mut().next(),
    _ => None,
  }
}

/// Removes the dotted key `parts`; under `[section]` tables with [`remove_entry`], so the
/// comments that are not about the key stay.
fn remove_key(doc: &mut DocumentMut, parts: &[&str]) {
  let (last, tables) = parts.split_last().expect("split always yields one part");
  let path: Vec<Step> = tables.iter().map(|t| Step::Table(t.to_string())).collect();
  if table_at(doc, &path).is_some() {
    remove_entry(doc, &path, last);
  } else {
    remove_table_like(doc.as_table_mut(), parts);
  }
}

/// Removes the dotted key `parts` under inline tables too.
fn remove_table_like(table: &mut dyn toml_edit::TableLike, parts: &[&str]) {
  match parts {
    [last] => {
      table.remove(last);
    }
    [first, rest @ ..] => {
      if let Some(inner) = table.get_mut(first).and_then(Item::as_table_like_mut) {
        remove_table_like(inner, rest);
      }
    }
    [] => {}
  }
}

/// A tree value as a TOML item: tables of tables and lists of tables become `[section]` and
/// `[[section]]` blocks, everything else a value.
fn to_item(key: &str, value: &Value) -> Result<Item, Error> {
  match value {
    Value::Object(entries) => {
      let mut table = Table::new();
      for (k, v) in entries {
        if v.is_null() {
          continue;
        }
        table.insert(k, to_item(key, v)?);
      }
      let only_tables = !table.is_empty()
        && table
          .iter()
          .all(|(_, item)| item.is_table() || item.is_array_of_tables());
      table.set_implicit(only_tables);
      Ok(Item::Table(table))
    }
    Value::Array(items) if !items.is_empty() && items.iter().all(Value::is_object) => {
      let mut tables = ArrayOfTables::new();
      for item in items {
        match to_item(key, item)? {
          Item::Table(table) => tables.push(table),
          _ => unreachable!("an object becomes a table"),
        }
      }
      Ok(Item::ArrayOfTables(tables))
    }
    other => Ok(Item::Value(to_value(key, other)?)),
  }
}

fn to_value(key: &str, value: &Value) -> Result<toml_edit::Value, Error> {
  let unrepresentable = |message: String| Error::Value {
    key: key.to_string(),
    by: "the edit".to_string(),
    message,
  };
  Ok(match value {
    Value::Bool(b) => (*b).into(),
    Value::String(s) => s.as_str().into(),
    Value::Number(n) => match n.as_i64() {
      Some(i) => i.into(),
      None if n.is_f64() => n
        .as_f64()
        .ok_or_else(|| unrepresentable(format!("{n} has no TOML form")))?
        .into(),
      None => return Err(unrepresentable(format!("{n} is too large for TOML"))),
    },
    Value::Array(items) => {
      let mut array = toml_edit::Array::new();
      for item in items {
        array.push(to_value(key, item)?);
      }
      toml_edit::Value::Array(array)
    }
    Value::Object(entries) => {
      let mut table = toml_edit::InlineTable::new();
      for (k, v) in entries {
        if !v.is_null() {
          table.insert(k, to_value(key, v)?);
        }
      }
      toml_edit::Value::InlineTable(table)
    }
    Value::Null => return Err(unrepresentable("a list cannot hold null".to_string())),
  })
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::schema::REDACTED;
  use crate::testing::example_schema;
  use serde_json::json;

  fn patch(text: &str, set: &[(&str, Value)], unset: &[&str]) -> Result<String, Error> {
    let schema = example_schema();
    let set = set
      .iter()
      .map(|(k, v)| (k.to_string(), v.clone()))
      .collect();
    let unset: Vec<Key> = unset.iter().map(|k| k.to_string()).collect();
    FileEditor::new(&schema).patch("config.toml", text, &set, &unset)
  }

  #[test]
  fn only_the_named_keys_change() {
    let text = "# the server\n[server]\nport = 2242   # the usual port\naddress = \"x\" # where\n\n[shares]\n# none yet\ndirectories = []\n";
    let edited = patch(text, &[("server.port", json!(5000))], &[]).unwrap();
    assert_eq!(
      edited,
      text.replace("2242", "5000"),
      "comments and spacing stay"
    );

    let edited = patch(text, &[], &["server.address"]).unwrap();
    assert_eq!(
      edited,
      "# the server\n[server]\nport = 2242   # the usual port\n\n[shares]\n# none yet\ndirectories = []\n"
    );
  }

  #[test]
  fn new_sections_go_below_comments_that_end_the_file() {
    let banner = "# Written by the application\n\n# [server]\n# port = 2242\n";
    let edited = patch(banner, &[("server.port", json!(5353))], &[]).unwrap();
    assert!(edited.starts_with(banner), "{edited}");
    assert!(edited.ends_with("\n[server]\nport = 5353\n"), "{edited}");

    let edited = patch(banner, &[("shares.cache.workers", json!(2))], &[]).unwrap();
    assert!(edited.starts_with(banner), "{edited}");
    assert!(
      edited.ends_with("\n[shares.cache]\nworkers = 2\n"),
      "{edited}"
    );
    assert!(
      !edited.contains("\n[shares]\n"),
      "no empty parent header: {edited}"
    );

    let text = "[server]\nport = 1\n\n[flags]\nvolatile = true\n# the end\n";
    let edited = patch(text, &[("shares.cache.workers", json!(2))], &[]).unwrap();
    assert!(
      edited.ends_with("# the end\n\n[shares.cache]\nworkers = 2\n"),
      "{edited}"
    );
    let parsed: toml::Table = edited.parse().unwrap();
    assert_eq!(parsed["flags"]["volatile"].as_bool(), Some(true));
    assert_eq!(parsed["shares"]["cache"]["workers"].as_integer(), Some(2));
  }

  #[test]
  fn a_top_level_value_goes_below_the_banner() {
    let banner = "# Written by the application\n# [server]\n# port = 2242\n";
    let edited = patch(banner, &[("debug", json!(true))], &[]).unwrap();
    assert_eq!(edited, format!("{banner}\ndebug = true\n"));

    let text = "# mine\n\n# the server\n[server]\nport = 1\n";
    let edited = patch(text, &[("debug", json!(true))], &[]).unwrap();
    assert_eq!(
      edited,
      "# mine\n\ndebug = true\n# the server\n[server]\nport = 1\n"
    );

    let text = "debug = false\n# mine\n[server]\nport = 1\n";
    let edited = patch(text, &[("instance_name", json!("x"))], &[]).unwrap();
    assert_eq!(
      edited,
      "debug = false\ninstance_name = \"x\"\n# mine\n[server]\nport = 1\n"
    );
  }

  #[test]
  fn under_an_inline_table_new_tables_are_inline_too() {
    let edited = patch(
      "server = { port = 1 }\n",
      &[("server.address", json!("a"))],
      &[],
    )
    .unwrap();
    let parsed: toml::Table = edited.parse().unwrap();
    assert_eq!(parsed["server"]["port"].as_integer(), Some(1));
    assert_eq!(parsed["server"]["address"].as_str(), Some("a"));

    let edited = patch(
      "shares = { directories = [] }\n",
      &[("shares.cache.workers", json!(3))],
      &[],
    )
    .unwrap();
    let parsed: toml::Table = edited.parse().unwrap();
    assert_eq!(parsed["shares"]["cache"]["workers"].as_integer(), Some(3));
  }

  #[test]
  fn editing_under_a_value_is_refused() {
    let e = patch("server = 5\n", &[("server.port", json!(1))], &[]).unwrap_err();
    assert!(e.to_string().contains("`server` is not a section"), "{e}");
    let e = patch("", &[("server.nope", json!(1))], &[]).unwrap_err();
    assert!(matches!(e, Error::UnknownKey(k) if k == "server.nope"));
    let e = patch("[server\n", &[], &[]).unwrap_err();
    assert!(matches!(e, Error::Parse { .. }));
  }

  #[test]
  fn a_typo_can_be_removed() {
    let edited = patch("[server]\nprot = 1\nport = 2\n", &[], &["server.prot"]).unwrap();
    assert_eq!(edited, "[server]\nport = 2\n");
  }

  #[test]
  fn secrets_keep_their_value_unless_changed() {
    let text = "[server]\npassword = \"hunter2\"\n";
    let kept = patch(text, &[("server.password", json!(REDACTED))], &[]).unwrap();
    assert_eq!(kept, text);
    let cleared = patch(text, &[("server.password", json!(""))], &[]).unwrap();
    assert_eq!(cleared, "[server]\n");
    let changed = patch(text, &[("server.password", json!("new"))], &[]).unwrap();
    assert_eq!(changed, "[server]\npassword = \"new\"\n");
    let removed = patch(text, &[("server.password", Value::Null)], &[]).unwrap();
    assert_eq!(removed, "[server]\n");
    let blank = patch(
      "[server]\nusername = \"a\"\n",
      &[("server.username", json!(""))],
      &[],
    );
    assert_eq!(
      blank.unwrap(),
      "[server]\nusername = \"\"\n",
      "an empty string is a value for a key that is not secret"
    );
  }

  #[test]
  fn maps_and_lists_of_tables_are_written_as_sections() {
    let edited = patch(
      "# keys\n",
      &[
        (
          "api_keys",
          json!({ "mine": { "key": "k1", "role": "admin" }, "my.other": { "key": "k2", "role": null } }),
        ),
        ("webhooks", json!([{ "url": "http://a" }, { "url": "http://b", "token": "t" }])),
        ("shares.limits", json!([1, 2])),
      ],
      &[],
    )
    .unwrap();
    let parsed: toml::Table = edited.parse().unwrap();
    assert_eq!(parsed["api_keys"]["mine"]["role"].as_str(), Some("admin"));
    assert_eq!(parsed["api_keys"]["my.other"]["key"].as_str(), Some("k2"));
    assert!(parsed["api_keys"]["my.other"].get("role").is_none());
    assert_eq!(parsed["webhooks"].as_array().unwrap().len(), 2);
    assert!(edited.contains("[api_keys.mine]"), "{edited}");
    assert!(edited.contains("[[webhooks]]"), "{edited}");
    assert!(!edited.contains("\n[api_keys]\n"), "{edited}");
    assert!(edited.starts_with("# keys\n"), "{edited}");

    let replaced = patch(&edited, &[("api_keys", json!({}))], &[]).unwrap();
    let parsed: toml::Table = replaced.parse().unwrap();
    assert!(parsed["api_keys"].as_table().unwrap().is_empty());
  }

  #[test]
  fn changing_a_map_keeps_the_comments_of_what_stays() {
    let banner = "# my reseek config\n# see docs\n\n";
    let text = format!("{banner}[api_keys.laptop]\nkey = \"1\"\n");
    let both = json!({ "laptop": { "key": "1" }, "phone": { "key": "3" } });
    let edited = patch(&text, &[("api_keys", both.clone())], &[]).unwrap();
    assert_eq!(edited, format!("{text}\n[api_keys.phone]\nkey = \"3\"\n"));

    let text = "[server]\nport = 1\n\n# keys for my devices\n[api_keys.laptop]\nkey = \"1\" # rotate in May\n";
    let edited = patch(text, &[("api_keys", both)], &[]).unwrap();
    assert_eq!(edited, format!("{text}\n[api_keys.phone]\nkey = \"3\"\n"));

    let text = format!(
      "{banner}# old laptop\n[api_keys.laptop]\nkey = \"1\"\n\n[api_keys.phone]\nkey = \"3\"\n"
    );
    let phone = json!({ "phone": { "key": "3" } });
    let edited = patch(&text, &[("api_keys", phone)], &[]).unwrap();
    assert_eq!(
      edited,
      format!("{banner}[api_keys.phone]\nkey = \"3\"\n"),
      "the banner stays, the comment on the removed entry goes with it"
    );

    let text = "[[webhooks]]\n# the main one\nurl = \"a\" # mine\n";
    let edited = patch(text, &[("webhooks", json!([{ "url": "b" }]))], &[]).unwrap();
    assert_eq!(edited, text.replace("\"a\"", "\"b\""));
  }

  #[test]
  fn a_list_of_tables_emptied_keeps_the_banner() {
    let text = "# top\n\n# hooks\n[[webhooks]]\nurl = \"a\"\n\n[server]\nport = 1\n";
    let edited = patch(text, &[("webhooks", json!([]))], &[]).unwrap();
    assert_eq!(
      edited,
      "# top\n\n# hooks\nwebhooks = []\n[server]\nport = 1\n"
    );
  }

  #[test]
  fn removing_a_key_leaves_the_comments_not_about_it() {
    let text = "# banner\n\n# about debug\ndebug = true\ninstance_name = \"x\"\n";
    assert_eq!(
      patch(text, &[], &["debug"]).unwrap(),
      "# banner\n\ninstance_name = \"x\"\n"
    );
    assert_eq!(
      patch(text, &[("debug", Value::Null)], &[]).unwrap(),
      "# banner\n\ninstance_name = \"x\"\n"
    );
    let text = "# banner\n\ndebug = true\n\n[server]\nport = 1\n";
    assert_eq!(
      patch(text, &[], &["debug"]).unwrap(),
      "# banner\n\n[server]\nport = 1\n"
    );
    let text = "[server]\nport = 1\n\n# the end\n\naddress = \"a\"\n";
    assert_eq!(
      patch(text, &[], &["server.address"]).unwrap(),
      "[server]\nport = 1\n\n# the end\n\n",
      "with nothing after it, they end the file"
    );
  }

  #[test]
  fn secrets_are_found_anywhere() {
    let schema = example_schema();
    let editor = FileEditor::new(&schema);
    assert!(!editor.holds_secrets("[server]\nport = 1\npassword = \"\"\n"));
    assert!(editor.holds_secrets("[server]\npassword = \"x\"\n"));
    assert!(editor.holds_secrets("[api_keys.a]\nkey = \"k\"\n"));
    assert!(editor.holds_secrets("[[webhooks]]\nurl = \"u\"\ntoken = \"t\"\n"));
    assert!(
      editor.holds_secrets("[server\n"),
      "unparsable text counts as secret"
    );
  }
}
