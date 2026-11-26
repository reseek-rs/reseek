//! Keeps a configuration current: reloads it when its files change or when asked, applies and
//! expires runtime overrides, writes the files, and reports each change grouped by when it
//! takes effect.

mod edits;
mod events;
mod reload;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde_json::Value;
use tokio::sync::{Notify, broadcast, watch};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, info, info_span, warn};

use crate::files::{Entry, LoadedFile};
use crate::layers::{Checked, Failure, Layers};
use crate::loaded::{Loaded, NotApplied};
use crate::overrides::Lasts;
use crate::schema::{keep_secrets, keeps_hidden};
use crate::{Apply, Error, Key, Root, lock, tree};
use events::{FileEvents, SETTLE, Settle, Writing, quiet};
use reload::{Read, pending};

/// How long to wait before reading the files again after a reload failed, since an editor that
/// truncates and then writes leaves a broken file for a moment.
const RETRY: Duration = Duration::from_millis(500);

/// How many changes a slow listener may fall behind by before it misses some.
const CHANGES: usize = 16;

/// How the files are watched for changes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Watch {
  /// the operating system's file notifications
  Events,
  /// reading the files' metadata at this interval, for mounts where notifications do not arrive
  Poll(Duration),
  /// not at all; [`Watcher::reload`] still reloads
  Off,
}

/// The keys of a change, by when each takes effect.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ByApply {
  /// in effect now
  pub live: Vec<Key>,
  /// in effect after the next reconnect
  pub reconnect: Vec<Key>,
  /// in effect after a restart
  pub restart: Vec<Key>,
}

/// A change to the values in effect.
#[derive(Debug)]
pub struct Change<T> {
  /// the leaf keys whose values changed
  pub changed: Vec<Key>,
  /// the same keys by when each takes effect
  pub by_apply: ByApply,
  /// the configuration before the change
  pub previous: Arc<Loaded<T>>,
  /// the configuration after it
  pub current: Arc<Loaded<T>>,
}

/// The result of writing a file.
#[derive(Debug)]
pub struct Written<T> {
  /// the file's new revision
  pub revision: String,
  /// what the reload after the write changed, `None` when nothing did
  pub change: Option<Arc<Change<T>>>,
}

/// The current configuration and the task that keeps it current. Cloning gives another handle
/// to the same configuration.
pub struct Watcher<T> {
  shared: Arc<Shared<T>>,
}

impl<T> Clone for Watcher<T> {
  fn clone(&self) -> Self {
    Self {
      shared: self.shared.clone(),
    }
  }
}

struct Shared<T> {
  /// the files and patterns the configuration is read from, which do not change
  entries: Vec<Entry>,
  /// only locked on blocking threads
  state: Mutex<State<T>>,
  current: watch::Sender<Arc<Loaded<T>>>,
  changes: broadcast::Sender<Arc<Change<T>>>,
  /// the runtime overrides changed, so the next expiry may have moved
  overrides_changed: Notify,
}

struct State<T> {
  layers: Layers<T>,
  /// the values at the start, which `restart` keys keep running with
  startup: Value,
  /// the values at the last reconnect
  reconnected: Value,
  /// the files as the last reload read them, to skip events that changed nothing
  attempted: Vec<LoadedFile>,
  /// why the last reading of the files failed, kept until one succeeds, so a change that does
  /// not read them, such as an override, still shows it
  failed: Option<Failed>,
  /// a change was reported as not standing yet and has not loaded since, so files as last
  /// attempted are still to be loaded
  waiting: bool,
}

/// A reading of the files that failed, as the configuration in effect shows it.
#[derive(Clone)]
struct Failed {
  error: String,
  at: DateTime<Utc>,
  not_applied: BTreeMap<Key, NotApplied>,
  /// the files as the failed reading found them, so a fix can be written against them
  files: Vec<LoadedFile>,
}

impl Failed {
  /// Shows the failure on `loaded`, whose values are the ones from before it.
  fn show<T>(&self, loaded: &mut Loaded<T>) {
    loaded.load_error = Some(self.error.clone());
    loaded.load_error_at = Some(self.at);
    loaded.not_applied = self.not_applied.clone();
    loaded.files = self.files.clone();
  }
}

/// What one reading of the files came to.
enum Attempt<T> {
  /// the files were as the last attempt read them, so they were not loaded
  Unchanged,
  Done(Option<Arc<Change<T>>>),
  Failed(Failure),
}

/// Refuses a change made against the values of revision `read` once those in effect are others;
/// no `read` is no check. Called with the state locked, so nothing changes them before the
/// change is made.
fn unchanged_since<T>(shared: &Shared<T>, read: Option<&str>) -> Result<(), Error> {
  let current = shared.current.borrow();
  match read {
    Some(read) if read != current.revision => Err(Error::Outdated(current.revision.clone())),
    _ => Ok(()),
  }
}

/// Lists in the values in effect the keys that wait for a reconnect since the values of the
/// last one, when that list changed. Called with the state locked.
fn show_pending_reconnect<T: Clone>(state: &State<T>, shared: &Shared<T>) {
  let current = shared.current.borrow().clone();
  let waiting = pending(
    &current.schema,
    &state.reconnected,
    &current.tree,
    Apply::Reconnect,
  );
  if waiting != current.pending_reconnect {
    let mut loaded = (*current).clone();
    loaded.pending_reconnect = waiting;
    shared.current.send_replace(Arc::new(loaded));
  }
}

/// `set` with the secrets its lists of tables and user-keyed maps show as `*****` put back
/// from the values in effect, and without the secret lists and tables sent back as they were
/// shown. Called with the state locked, so the secrets put back are the ones the change is made
/// over.
fn with_entry_secrets<T>(
  shared: &Shared<T>,
  set: BTreeMap<Key, Value>,
) -> Result<BTreeMap<Key, Value>, Error> {
  let current = shared.current.borrow().clone();
  let mut kept = BTreeMap::new();
  for (key, mut value) in set {
    if let Some(field) = current.schema().field(&key) {
      let in_effect = tree::get(&current.tree, &key);
      let unchanged = if field.secret {
        keeps_hidden(field, &value, in_effect)
      } else {
        keep_secrets(field, &mut value, in_effect).map(|()| false)
      }
      .map_err(|invalid| Error::Invalid(vec![invalid]))?;
      if unchanged {
        continue;
      }
    }
    kept.insert(key, value);
  }
  Ok(kept)
}

/// How a file is to be changed.
enum FileEdit {
  Patch {
    set: BTreeMap<Key, Value>,
    unset: Vec<Key>,
  },
  Replace(String),
}

impl<T: Root> Watcher<T> {
  /// Loads the configuration, reading the files on this thread. An error here is a
  /// configuration the application cannot start with. Call [`Watcher::start`] to have it kept
  /// current.
  pub fn new(mut layers: Layers<T>) -> Result<Self, Error> {
    let loaded = layers.load()?;
    for file in &loaded.files {
      let state = if file.revision.is_some() {
        "read"
      } else {
        "absent"
      };
      info!(file = file.name, path = %file.path.display(), state, "configuration file");
    }
    let entries = layers.entries().to_vec();
    let state = State {
      startup: loaded.tree.clone(),
      reconnected: loaded.tree.clone(),
      attempted: loaded.files.clone(),
      failed: None,
      waiting: false,
      layers,
    };
    Ok(Self {
      shared: Arc::new(Shared {
        entries,
        state: Mutex::new(state),
        current: watch::Sender::new(Arc::new(loaded)),
        changes: broadcast::Sender::new(CHANGES),
        overrides_changed: Notify::new(),
      }),
    })
  }

  /// Starts the task that reloads on file changes (as `watch` says) and ends runtime
  /// overrides when they expire. It stops when `cancel` is canceled. Call it once. A changed
  /// file is read once it stands, so one a slow writer is still writing is not loaded half
  /// written. When the files are watched, they are read again before this returns if they
  /// changed since they were loaded, so an edit made before the watch began is not missed.
  pub async fn start(&self, watch: Watch, cancel: CancellationToken) {
    let span = info_span!("config_watcher");
    let events = Arc::new(Notify::new());
    let poll = match watch {
      Watch::Events => Some(None),
      Watch::Poll(interval) => Some(Some(interval)),
      Watch::Off => None,
    };
    // watched before the files are read again, so no change made after the load is missed
    let files = span.in_scope(|| {
      poll.and_then(|poll| {
        match FileEvents::new(poll, self.shared.entries.clone(), events.clone()) {
          Ok(files) => Some(files),
          Err(e) => {
            warn!(error = %e, "configuration files are not watched; a change takes effect on the next reload");
            None
          }
        }
      })
    });
    if files.is_some() {
      // a failure is logged and kept in the current state where it happens
      drop(
        self
          .reload_files(true, || false)
          .instrument(span.clone())
          .await,
      );
    }
    let this = self.clone();
    tokio::spawn(this.run(files, events, cancel).instrument(span));
  }

  /// The configuration in effect.
  pub fn current(&self) -> Arc<Loaded<T>> {
    self.shared.current.borrow().clone()
  }

  /// Every new state of the configuration, including failed reloads, which keep the values
  /// and set `load_error`.
  pub fn subscribe(&self) -> watch::Receiver<Arc<Loaded<T>>> {
    self.shared.current.subscribe()
  }

  /// Every change to the values in effect.
  pub fn changes(&self) -> broadcast::Receiver<Arc<Change<T>>> {
    self.shared.changes.subscribe()
  }

  /// Reads the files again, as a change to them would: a failure is tried once more after a
  /// moment, then reported, and the values in effect stay. Returns what changed.
  pub async fn reload(&self) -> Result<Option<Arc<Change<T>>>, Error> {
    self.reload_files(false, || false).await
  }

  /// What the configuration would load with if the file `name` held `text`: its warnings, and
  /// why it would not load.
  pub async fn check_file(&self, name: &str, text: &str) -> Result<Checked, Error> {
    let (name, text) = (name.to_string(), text.to_string());
    self
      .blocking(move |state, _| state.layers.check_file(&name, &text))
      .await
  }

  /// Sets the keys of `set` and removes those of `unset` in the file `name`, keeping its
  /// comments, then reloads. `revision` is the file's revision the edit was made against,
  /// `None` when there was no file, and `read` the revision of the values in effect it was made
  /// against, when it is to be checked.
  ///
  /// Refused, in this order, when the values in effect changed since `read`, when the file
  /// changed since `revision`, cannot be written, would
  /// leave a configuration that does not load, or when a key's value comes from an env var or
  /// flag, which nothing at run time changes, or from a later file, which this file would not
  /// beat. For a secret, `*****` keeps the value and an empty string removes it; the same goes
  /// for a secret in an entry of a list of tables or a user-keyed map.
  pub async fn patch_file(
    &self,
    name: &str,
    revision: Option<&str>,
    read: Option<&str>,
    set: BTreeMap<Key, Value>,
    unset: Vec<Key>,
  ) -> Result<Written<T>, Error> {
    self
      .write_file(name, revision, read, FileEdit::Patch { set, unset })
      .await
  }

  /// Replaces the whole text of the file `name`, with the checks of [`Watcher::patch_file`]
  /// but that of the values in effect: the text is the file's whole, checked by `revision`.
  pub async fn replace_file(
    &self,
    name: &str,
    revision: Option<&str>,
    text: String,
  ) -> Result<Written<T>, Error> {
    self
      .write_file(name, revision, None, FileEdit::Replace(text))
      .await
  }

  /// Sets runtime overrides for the keys of `set`, lasting as `lasts` says, and removes those
  /// for `unset`, saving them before they take effect. Refused when the values in effect
  /// changed since `read`, the revision the change was made against, when one is given; for a
  /// key that only takes effect on restart; or for one whose value comes from an env var or
  /// flag.
  pub async fn set_overrides(
    &self,
    set: BTreeMap<Key, Value>,
    unset: Vec<Key>,
    set_by: &str,
    lasts: Lasts,
    read: Option<&str>,
  ) -> Result<Option<Arc<Change<T>>>, Error> {
    let set_by = set_by.to_string();
    let read = read.map(str::to_string);
    self
      .blocking(move |state, shared| {
        unchanged_since(shared, read.as_deref())?;
        let set = with_entry_secrets(shared, set)?;
        state.change_overrides(shared, set, &unset, &set_by, lasts)
      })
      .await?
  }

  /// Removes the runtime override for `key`; refused when the values in effect changed since
  /// `read`, the revision the removal was made against, when one is given.
  pub async fn remove_override(
    &self,
    key: &str,
    read: Option<&str>,
  ) -> Result<Option<Arc<Change<T>>>, Error> {
    let key = key.to_string();
    let read = read.map(str::to_string);
    self
      .blocking(move |state, shared| {
        unchanged_since(shared, read.as_deref())?;
        if !state.layers.overrides.items().contains_key(&key) {
          return Err(Error::NoOverride(key));
        }
        state.change_overrides(shared, BTreeMap::new(), &[key], "", Lasts::UntilRemoved)
      })
      .await?
  }

  /// Marks `with`, a configuration this watcher gave out, as the one the application
  /// reconnected with: `pending_reconnect` then lists the keys changed since.
  pub async fn reconnected(&self, with: &Loaded<T>) -> Result<(), Error> {
    let tree = with.tree.clone();
    self
      .blocking(move |state, shared| {
        state.reconnected = tree;
        show_pending_reconnect(state, shared);
      })
      .await
  }

  /// Marks the values `with` gives `keys` as ones the application's connection runs with
  /// already, as a reconnect does for every key, for a connection that took them without one.
  pub async fn reconnected_keys(&self, keys: &[&str], with: &Loaded<T>) -> Result<(), Error> {
    let taken: Vec<(String, Option<Value>)> = keys
      .iter()
      .map(|key| (key.to_string(), tree::get(&with.tree, key).cloned()))
      .collect();
    self
      .blocking(move |state, shared| {
        for (key, value) in taken {
          match value {
            Some(value) => tree::set(&mut state.reconnected, &key, value),
            None => tree::remove(&mut state.reconnected, &key),
          }
        }
        show_pending_reconnect(state, shared);
      })
      .await
  }

  async fn run(
    self,
    mut files: Option<FileEvents>,
    events: Arc<Notify>,
    cancel: CancellationToken,
  ) {
    loop {
      let next_expiry = self
        .current()
        .overrides
        .values()
        .filter_map(|o| o.expires_at)
        .min();
      let expiry = async {
        match next_expiry {
          // an expiry already past is a negative wait, which ends at once
          Some(at) => tokio::time::sleep((at - Utc::now()).to_std().unwrap_or_default()).await,
          None => std::future::pending().await,
        }
      };
      tokio::select! {
        () = cancel.cancelled() => break,
        () = events.notified() => {
          if let Some(files) = &mut files
            && !self.reload_settled(files, &events, &cancel, SETTLE).await
          {
            break;
          }
        }
        () = expiry => {
          let reapplied = self.blocking(|state, shared| state.reapply(shared)).await;
          // a failure is logged and kept in the current state where it happens
          drop(reapplied);
        }
        () = self.shared.overrides_changed.notified() => {}
      }
    }
    debug!("configuration watcher stopped");
  }

  /// Reloads the files once what they hold stands: quiet for `settle.debounce`, then, where the
  /// system says when a writer closes a file, with no writer left, and elsewhere with the same
  /// bytes on two reads `settle.again` apart. A file that does not stand within `settle.limit`
  /// is reported and not applied; its next change starts the wait over. False when canceled.
  async fn reload_settled(
    &self,
    files: &mut FileEvents,
    events: &Notify,
    cancel: &CancellationToken,
    settle: Settle,
  ) -> bool {
    let limit = tokio::time::Instant::now() + settle.limit;
    loop {
      match tokio::time::timeout_at(limit, quiet(events, settle.debounce, cancel)).await {
        Ok(true) => {}
        Ok(false) => return false,
        Err(_) => break,
      }
      files.watch_new_dirs();
      match files.writing() {
        Writing::Closed => {}
        Writing::Unknown => match self.moving(settle.again, cancel).await {
          Some(moving) if moving.is_empty() => {}
          Some(moving) => {
            debug!(
              files = moving.join(", "),
              "configuration file still changing"
            );
            continue;
          }
          None => return false,
        },
        Writing::Open(open) => {
          debug!(files = ?open, "configuration file still open to a writer");
          // the writer's close is an event
          tokio::select! {
            () = cancel.cancelled() => return false,
            () = events.notified() => {}
            () = tokio::time::sleep_until(limit) => break,
          }
          continue;
        }
      }
      // a failure is logged and kept in the current state where it happens
      drop(self.reload_files(true, files.writer_open()).await);
      if !matches!(files.writing(), Writing::Open(_)) {
        return true;
      }
    }
    let open = match files.writing() {
      Writing::Open(open) => open,
      Writing::Closed | Writing::Unknown => Vec::new(),
    };
    // an error only means the runtime is stopping
    self
      .blocking(move |state, shared| state.unsettled(shared, &open))
      .await
      .is_ok()
  }

  /// The names of the files whose bytes differ between two reads `again` apart, none when the
  /// first finds them as the last reload read them. `None` when canceled.
  async fn moving(&self, again: Duration, cancel: &CancellationToken) -> Option<Vec<String>> {
    let Some(first) = self.blocking(|state, _| state.changed_files()).await.ok()? else {
      return Some(Vec::new());
    };
    tokio::select! {
      () = cancel.cancelled() => return None,
      () = tokio::time::sleep(again) => {}
    }
    let Some(second) = self.blocking(|state, _| state.changed_files()).await.ok()? else {
      return Some(Vec::new());
    };
    let mut names: Vec<String> = first
      .iter()
      .filter(|f| !second.contains(f))
      .chain(second.iter().filter(|f| !first.contains(f)))
      .map(|f| f.name.clone())
      .collect();
    names.sort();
    names.dedup();
    Some(names)
  }

  /// Reloads the files, once more after [`RETRY`] if that fails, then reports the failure; when
  /// `writer_open` says a writer has a file open by then, the failure is not reported and the
  /// files are not read again. With `only_if_changed`, files as the last attempt read them are
  /// not loaded again.
  async fn reload_files(
    &self,
    only_if_changed: bool,
    writer_open: impl Fn() -> bool,
  ) -> Result<Option<Arc<Change<T>>>, Error> {
    let first = self
      .blocking(move |state, shared| state.attempt(shared, only_if_changed))
      .await?;
    let failure = match first {
      Attempt::Unchanged => return Ok(None),
      Attempt::Done(change) => return Ok(change),
      Attempt::Failed(failure) => failure,
    };
    debug!(error = %failure.error, "configuration reload failed, trying again");
    tokio::time::sleep(RETRY).await;
    if writer_open() {
      debug!("configuration file opened by a writer, read again once it closes");
      return Ok(None);
    }
    self
      .blocking(move |state, shared| match state.attempt(shared, true) {
        Attempt::Unchanged => Err(state.report(shared, failure, Read::Files)),
        Attempt::Done(change) => Ok(change),
        Attempt::Failed(again) => Err(state.report(shared, again, Read::Files)),
      })
      .await?
  }

  async fn write_file(
    &self,
    name: &str,
    revision: Option<&str>,
    read: Option<&str>,
    edit: FileEdit,
  ) -> Result<Written<T>, Error> {
    let name = name.to_string();
    let revision = revision.map(str::to_string);
    let read = read.map(str::to_string);
    self
      .blocking(move |state, shared| {
        unchanged_since(shared, read.as_deref())?;
        let edit = match edit {
          FileEdit::Patch { set, unset } => FileEdit::Patch {
            set: with_entry_secrets(shared, set)?,
            unset,
          },
          replace @ FileEdit::Replace(_) => replace,
        };
        state.write_file(shared, &name, revision.as_deref(), edit)
      })
      .await?
  }

  /// Runs `f` on the state on a blocking thread, since loading reads files.
  async fn blocking<R: Send + 'static>(
    &self,
    f: impl FnOnce(&mut State<T>, &Shared<T>) -> R + Send + 'static,
  ) -> Result<R, Error> {
    let shared = self.shared.clone();
    let task = tokio::task::spawn_blocking(move || {
      let mut state = lock(&shared.state);
      f(&mut state, &shared)
    });
    match task.await {
      Ok(result) => Ok(result),
      Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
      // the runtime is shutting down
      Err(_) => Err(Error::Stopped),
    }
  }
}
