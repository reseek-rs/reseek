//! Noticing that the configuration files changed: file notifications or polling, waiting for a
//! burst of them to end, and telling when a writer still has a file open.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use notify::EventKind;
use notify::event::{AccessKind, AccessMode, ModifyKind};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::files::Entry;
use crate::lock;

/// How long the files must be quiet after a change before they are read: a save is often
/// several events.
pub(super) const DEBOUNCE: Duration = Duration::from_millis(250);

/// How long the watcher waits for a changed file to stand before it reads it.
#[derive(Clone, Copy, Debug)]
pub(super) struct Settle {
  /// the quiet after the last event
  pub(super) debounce: Duration,
  /// between the two reads that must find the same bytes, where the system does not say when a
  /// writer closes a file
  pub(super) again: Duration,
  /// how long a file may stay open to a writer, or keep changing, before it is reported
  pub(super) limit: Duration,
}

pub(super) const SETTLE: Settle = Settle {
  debounce: DEBOUNCE,
  again: Duration::from_millis(500),
  limit: Duration::from_secs(10),
};

/// Waits until `events` has been quiet for `period`. False when canceled first.
pub(super) async fn quiet(events: &Notify, period: Duration, cancel: &CancellationToken) -> bool {
  loop {
    tokio::select! {
      () = cancel.cancelled() => return false,
      () = events.notified() => {}
      () = tokio::time::sleep(period) => return true,
    }
  }
}

/// The files' directories under watch.
pub(super) struct FileEvents {
  watcher: Box<dyn notify::Watcher + Send>,
  /// every directory a file or a pattern's matches live in
  dirs: Vec<PathBuf>,
  watched: BTreeSet<PathBuf>,
  seen: Arc<Seen>,
}

/// What the events say, kept between the system's thread that delivers them and the watcher.
struct Seen {
  entries: Vec<Entry>,
  dirs: Vec<PathBuf>,
  events: Arc<Notify>,
  /// directories an event was about itself, such as one removed, renamed or made again, whose
  /// watch may have ended with it
  touched: Mutex<BTreeSet<PathBuf>>,
  /// the files written to and not closed, renamed over or removed since; `None` where the
  /// system does not say when a writer closes a file
  writers: Option<Mutex<BTreeSet<PathBuf>>>,
}

/// What the events say about the writers of the files.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Writing {
  /// these files were written to and their writer has not closed them
  Open(Vec<PathBuf>),
  /// every file written to was closed since
  Closed,
  /// the system does not say when a writer closes a file
  Unknown,
}

impl FileEvents {
  /// Watches the directories of `entries`, by notifications or, with `poll`, by looking at that
  /// interval, and wakes `events` for each change to one of their files.
  pub(super) fn new(
    poll: Option<Duration>,
    entries: Vec<Entry>,
    events: Arc<Notify>,
  ) -> notify::Result<Self> {
    let mut dirs: Vec<PathBuf> = entries
      .iter()
      .filter_map(|entry| match entry.plain_path() {
        Some(path) => path.parent().map(Path::to_path_buf),
        None => entry.pattern_dir(),
      })
      .collect();
    dirs.sort();
    dirs.dedup();
    // of the backends, only inotify reports a writer closing a file
    let closes = poll.is_none()
      && <notify::RecommendedWatcher as notify::Watcher>::kind() == notify::WatcherKind::Inotify;
    let seen = Arc::new(Seen {
      entries,
      dirs: dirs.clone(),
      events,
      touched: Mutex::new(BTreeSet::new()),
      writers: closes.then(|| Mutex::new(BTreeSet::new())),
    });
    let handler = {
      let seen = seen.clone();
      move |result: notify::Result<notify::Event>| match result {
        Ok(event) => seen.note(&event),
        Err(e) => warn!(error = %e, "configuration file watch failed"),
      }
    };
    let watcher: Box<dyn notify::Watcher + Send> = match poll {
      // the poller compares modification times in whole seconds, so a save in the same second
      // as its last look would be missed for good; comparing contents catches it
      Some(interval) => Box::new(notify::PollWatcher::new(
        handler,
        notify::Config::default()
          .with_poll_interval(interval)
          .with_compare_contents(true),
      )?),
      None => Box::new(notify::recommended_watcher(handler)?),
    };
    let mut files = Self {
      watcher,
      dirs,
      watched: BTreeSet::new(),
      seen,
    };
    files.watch_new_dirs();
    Ok(files)
  }

  /// What the events say about the files' writers.
  pub(super) fn writing(&self) -> Writing {
    self.seen.writing()
  }

  /// Stops waiting for the writers of `paths`: their next write marks them open again.
  pub(super) fn forget_writers(&self, paths: &[PathBuf]) {
    if let Some(writers) = &self.seen.writers {
      let mut open = lock(writers);
      for path in paths {
        open.remove(path);
      }
    }
  }

  /// Whether a file has a writer that has not closed it, asked whenever it is called.
  pub(super) fn writer_open(&self) -> impl Fn() -> bool + use<> {
    let seen = self.seen.clone();
    move || matches!(seen.writing(), Writing::Open(_))
  }

  /// Watches each directory the files live in that exists and is not watched yet, or the
  /// parent of one that does not exist, to see it made. A directory an event was about is
  /// watched anew, since removing it ends its watch even when it is made again, and one no
  /// longer wanted is let go.
  pub(super) fn watch_new_dirs(&mut self) {
    let touched = std::mem::take(&mut *lock(&self.seen.touched));
    let wanted: BTreeSet<PathBuf> = self
      .dirs
      .iter()
      .map(|dir| match (dir.exists(), dir.parent()) {
        (false, Some(parent)) => parent.to_path_buf(),
        _ => dir.clone(),
      })
      .collect();
    let stale: Vec<PathBuf> = self
      .watched
      .iter()
      .filter(|dir| touched.contains(*dir) || !wanted.contains(*dir))
      .cloned()
      .collect();
    for dir in stale {
      self.watched.remove(&dir);
      // a removed directory's watch ended with it, which the system reports as not watched
      if let Err(e) = self.watcher.unwatch(&dir) {
        debug!(dir = %dir.display(), error = %e, "configuration directory watch already gone");
      }
    }
    for dir in wanted {
      if self.watched.contains(&dir) {
        continue;
      }
      match self
        .watcher
        .watch(&dir, notify::RecursiveMode::NonRecursive)
      {
        Ok(()) => {
          debug!(dir = %dir.display(), "watching configuration directory");
          self.watched.insert(dir);
        }
        Err(e) => warn!(dir = %dir.display(), error = %e, "cannot watch configuration directory"),
      }
    }
  }
}

impl Seen {
  fn writing(&self) -> Writing {
    let Some(writers) = &self.writers else {
      return Writing::Unknown;
    };
    let open = lock(writers);
    if open.is_empty() {
      Writing::Closed
    } else {
      Writing::Open(open.iter().cloned().collect())
    }
  }

  /// Notes what `event` says, and wakes the watcher when it changed one of the files.
  fn note(&self, event: &notify::Event) {
    // events were lost, so any file may have changed and which writers closed is not known; the
    // files are read as they are
    let lost = event.need_rescan();
    if lost && let Some(writers) = &self.writers {
      lock(writers).clear();
    }
    let mut relevant = lost;
    for path in &event.paths {
      if self.dirs.contains(path) {
        lock(&self.touched).insert(path.clone());
        relevant = true;
      } else if named(&self.entries, path) {
        if let Some(writers) = &self.writers {
          note_writer(&mut lock(writers), &event.kind, path);
        }
        relevant = true;
      }
    }
    // an open or a read changes nothing, a close after writing ends a write
    let changes = match event.kind {
      EventKind::Access(kind) => kind == AccessKind::Close(AccessMode::Write),
      _ => true,
    };
    if relevant && changes {
      self.events.notify_one();
    }
  }
}

/// Notes in `open` what `kind`, an event about the file at `path`, says about its writer. The
/// open that comes with a write says nothing more: it may be a reader's, the watcher's own
/// included, and it comes after the emptying of a file opened to be rewritten on older kernels.
fn note_writer(open: &mut BTreeSet<PathBuf>, kind: &EventKind, path: &Path) {
  match kind {
    EventKind::Modify(ModifyKind::Data(_)) => {
      open.insert(path.to_path_buf());
    }
    // the writer closed it, or the path now names another file, renamed there whole, or none
    EventKind::Access(AccessKind::Close(AccessMode::Write))
    | EventKind::Modify(ModifyKind::Name(_))
    | EventKind::Remove(_) => {
      open.remove(path);
    }
    _ => {}
  }
}

/// Whether `path` is a file one of `entries` names.
fn named(entries: &[Entry], path: &Path) -> bool {
  let (Some(dir), Some(name)) = (path.parent(), path.file_name().and_then(|n| n.to_str())) else {
    return false;
  };
  entries.iter().any(|entry| entry.names(dir, name))
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::testing::{Example, next_change, values, watched};
  use crate::{Source, Watch};
  use serde_json::json;
  use std::fs;

  #[tokio::test(start_paused = true)]
  async fn quiet_waits_for_events_to_stop() {
    let events = Arc::new(Notify::new());
    let cancel = CancellationToken::new();
    let started = tokio::time::Instant::now();
    let poker = {
      let events = events.clone();
      tokio::spawn(async move {
        for _ in 0..3 {
          events.notify_one();
          tokio::time::sleep(Duration::from_millis(100)).await;
        }
      })
    };
    tokio::task::yield_now().await;
    assert!(quiet(&events, DEBOUNCE, &cancel).await);
    poker.await.unwrap();
    let waited = started.elapsed();
    assert!(
      waited >= Duration::from_millis(200) + DEBOUNCE && waited < Duration::from_millis(600),
      "{waited:?}"
    );
    cancel.cancel();
    assert!(!quiet(&events, DEBOUNCE, &cancel).await);
  }

  #[tokio::test]
  async fn a_burst_of_saves_is_one_reload() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.toml");
    fs::write(&path, "[server]\nport = 2000\n").unwrap();
    let watcher = watched(dir.path());
    let mut changes = watcher.changes();
    let cancel = CancellationToken::new();
    watcher.start(Watch::Events, cancel.clone()).await;

    for port in 2001..=2005 {
      fs::write(&path, format!("[server]\nport = {port}\n")).unwrap();
      tokio::time::sleep(Duration::from_millis(40)).await;
    }
    let change = next_change(&mut changes).await;
    assert_eq!(change.current.values.server.port, 2005);
    assert_eq!(change.previous.values.server.port, 2000);
    let more = tokio::time::timeout(DEBOUNCE * 3, changes.recv()).await;
    assert!(more.is_err(), "one reload for the burst: {more:?}");

    // a drop-in made after the start, in a directory made after the start
    fs::create_dir(dir.path().join("config.d")).unwrap();
    tokio::time::sleep(DEBOUNCE * 2).await;
    fs::write(
      dir.path().join("config.d/10-a.toml"),
      "[server]\naddress = \"drop-in\"\n",
    )
    .unwrap();
    let change = next_change(&mut changes).await;
    assert_eq!(change.changed, ["server.address"]);
    assert_eq!(
      change.current.sources["server.address"],
      Source::File {
        file: "config.d/10-a.toml".into()
      }
    );
    cancel.cancel();
  }

  /// How a slow program writes a file: opened as `open` does, each text of `writes` written
  /// after its wait, an empty one writing nothing, and closed.
  struct SlowWriter {
    open: fn(&Path) -> fs::File,
    writes: Vec<(Duration, &'static str)>,
  }

  /// A shell redirect into the file: emptied as it is opened.
  fn emptied(path: &Path) -> fs::File {
    fs::File::create(path).unwrap()
  }

  /// A shell append to the file.
  fn appended(path: &Path) -> fs::File {
    fs::OpenOptions::new().append(true).open(path).unwrap()
  }

  /// Starts watching, as `watch` says, a `config.toml` at port 2000 and an empty `config.d`,
  /// then has `writer` write `name`. Gives the port, address and timeout in seconds of each
  /// change that followed.
  async fn slowly_written(watch: Watch, name: &str, writer: SlowWriter) -> Vec<(u16, String, u64)> {
    use std::io::Write;
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("config.toml"), "[server]\nport = 2000\n").unwrap();
    fs::create_dir(dir.path().join("config.d")).unwrap();
    let watcher = watched(dir.path());
    let mut changes = watcher.changes();
    let cancel = CancellationToken::new();
    watcher.start(watch, cancel.clone()).await;

    let mut file = (writer.open)(&dir.path().join(name));
    for (wait, text) in writer.writes {
      tokio::time::sleep(wait).await;
      if !text.is_empty() {
        file.write_all(text.as_bytes()).unwrap();
      }
    }
    drop(file);

    let mut seen = vec![next_change(&mut changes).await];
    while let Ok(change) = tokio::time::timeout(Duration::from_secs(2), changes.recv()).await {
      seen.push(change.unwrap());
    }
    cancel.cancel();
    seen
      .iter()
      .map(|c| {
        let server = &c.current.values.server;
        (
          server.port,
          server.address.clone(),
          server.timeout.as_secs(),
        )
      })
      .collect()
  }

  /// Rewrites `config.toml` as a shell redirect from a program that writes part, waits `pause`,
  /// then writes the rest.
  fn rewritten(pause: Duration) -> SlowWriter {
    SlowWriter {
      open: emptied,
      writes: vec![
        (Duration::ZERO, "[server]\nport = 3000\n"),
        (pause, "address = \"whole\"\n"),
      ],
    }
  }

  /// What [`rewritten`] leaves, read once
  fn whole() -> Vec<(u16, String, u64)> {
    vec![(3000, "whole".to_string(), 5)]
  }

  #[tokio::test]
  async fn a_file_written_with_a_pause_is_read_once_whole() {
    // longer than the quiet, shorter than the quiet and the second read
    let pause = DEBOUNCE * 2;
    for watch in [Watch::Events, Watch::Poll(Duration::from_millis(50))] {
      let read = slowly_written(watch, "config.toml", rewritten(pause)).await;
      assert_eq!(read, whole(), "{watch:?}");
    }
  }

  #[tokio::test]
  async fn a_file_that_changes_between_the_two_reads_is_read_once_whole() {
    // each write lands after the quiet, between the first read and the second
    let wait = SETTLE.debounce + SETTLE.again / 2 + Duration::from_millis(100);
    let writer = SlowWriter {
      open: emptied,
      writes: vec![
        (Duration::ZERO, "[server]\nport = 3000\n"),
        (wait, "address = \"half\"\n"),
        (wait, "timeout = \"9s\"\n"),
      ],
    };
    let poll = Watch::Poll(Duration::from_millis(50));
    let read = slowly_written(poll, "config.toml", writer).await;
    assert_eq!(read, [(3000, "half".to_string(), 9)]);
  }

  /// longer than the quiet and the second read
  #[cfg(target_os = "linux")]
  const LONG: Duration = Duration::from_millis(1500);

  #[cfg(target_os = "linux")]
  #[tokio::test]
  async fn a_writer_is_waited_for_until_it_closes_the_file() {
    let read = slowly_written(Watch::Events, "config.toml", rewritten(LONG)).await;
    assert_eq!(read, whole());
  }

  #[cfg(target_os = "linux")]
  #[tokio::test]
  async fn a_file_written_whole_and_held_is_read_at_its_close() {
    let writer = SlowWriter {
      open: emptied,
      writes: vec![
        (
          Duration::ZERO,
          "[server]\nport = 3000\naddress = \"whole\"\n",
        ),
        (LONG, ""),
      ],
    };
    let read = slowly_written(Watch::Events, "config.toml", writer).await;
    assert_eq!(read, whole());
  }

  #[cfg(target_os = "linux")]
  #[tokio::test]
  async fn a_new_drop_in_first_written_well_after_its_opening_is_read_once_whole() {
    let writer = SlowWriter {
      open: emptied,
      writes: vec![
        (Duration::from_millis(450), "[server]\nport = 3000\n"),
        (LONG, "address = \"whole\"\n"),
      ],
    };
    let read = slowly_written(Watch::Events, "config.d/10-a.toml", writer).await;
    assert_eq!(read, whole());
  }

  #[cfg(target_os = "linux")]
  #[tokio::test]
  async fn a_slow_append_is_read_once_whole() {
    let writer = SlowWriter {
      open: appended,
      writes: vec![
        (Duration::from_millis(300), "address = \"half\"\n"),
        (LONG, "timeout = \"9s\"\n"),
      ],
    };
    let read = slowly_written(Watch::Events, "config.toml", writer).await;
    assert_eq!(read, [(2000, "half".to_string(), 9)]);
  }

  #[cfg(target_os = "linux")]
  #[tokio::test]
  async fn a_rename_a_removal_or_lost_events_end_a_write() {
    use notify::event::{DataChange, Flag, RemoveKind, RenameMode};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.toml");
    let (_watcher, files, events) = settling(dir.path(), None);
    // whether the watcher was told to read the files, which takes the permit
    let woken = async || {
      tokio::time::timeout(Duration::ZERO, events.notified())
        .await
        .is_ok()
    };
    let told = |kind: EventKind| notify::Event::new(kind).add_path(path.clone());
    let written = told(EventKind::Modify(ModifyKind::Data(DataChange::Any)));
    let ends = [
      (
        "renamed onto",
        told(EventKind::Modify(ModifyKind::Name(RenameMode::To))),
      ),
      ("removed", told(EventKind::Remove(RemoveKind::File))),
      (
        "events lost",
        notify::Event::new(EventKind::Other).set_flag(Flag::Rescan),
      ),
    ];
    for (what, end) in ends {
      files.seen.note(&written);
      assert_eq!(files.writing(), Writing::Open(vec![path.clone()]), "{what}");
      assert!(woken().await, "{what}");
      files.seen.note(&end);
      assert_eq!(files.writing(), Writing::Closed, "{what}");
      assert!(woken().await, "{what}: the files are read again");
    }
  }

  /// The watcher over `dir` and its file events, watched as `poll` says
  fn settling(
    dir: &Path,
    poll: Option<Duration>,
  ) -> (crate::Watcher<Example>, FileEvents, Arc<Notify>) {
    let watcher = watched(dir);
    let events = Arc::new(Notify::new());
    let files = FileEvents::new(poll, watcher.shared.entries.clone(), events.clone()).unwrap();
    (watcher, files, events)
  }

  const SHORT: Settle = Settle {
    limit: Duration::from_secs(1),
    ..SETTLE
  };

  #[cfg(target_os = "linux")]
  #[tokio::test]
  async fn a_file_left_open_to_its_writer_is_reported_not_read() {
    use std::io::Write;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.toml");
    fs::write(&path, "[server]\nport = 2000\n").unwrap();
    fs::create_dir(dir.path().join("config.d")).unwrap();
    let (watcher, mut files, events) = settling(dir.path(), None);
    let cancel = CancellationToken::new();

    let mut file = fs::File::create(&path).unwrap();
    file.write_all(b"[server]\nport = 3000\n").unwrap();
    // changed and closed meanwhile, so not one the report names
    let drop_in = dir.path().join("config.d/10-a.toml");
    fs::write(&drop_in, "[shares]\ndirectories = [\"/music\"]\n").unwrap();
    assert!(
      watcher
        .reload_settled(&mut files, &events, &cancel, SHORT)
        .await
    );
    let current = watcher.current();
    assert_eq!(current.values.server.port, 2000);
    assert_eq!(
      current.load_error.as_deref(),
      Some(
        "config.toml still open to the program writing it, read again once it is closed; where \
         the filesystem tells of changes but not of a writer closing a file, watch the files by \
         polling"
      )
    );

    file.write_all(b"address = \"whole\"\n").unwrap();
    drop(file);
    assert!(
      watcher
        .reload_settled(&mut files, &events, &cancel, SHORT)
        .await
    );
    let current = watcher.current();
    assert_eq!(current.values.server.address, "whole");
    assert_eq!(current.load_error, None);
  }

  #[tokio::test]
  async fn a_file_that_keeps_changing_is_reported_not_read() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.toml");
    fs::write(&path, "[server]\nport = 2000\n").unwrap();
    let (watcher, mut files, events) = settling(dir.path(), Some(Duration::from_millis(50)));
    let cancel = CancellationToken::new();

    let writing = CancellationToken::new();
    let writer = tokio::spawn({
      let (path, writing) = (path.clone(), writing.clone());
      async move {
        for port in 3000.. {
          fs::write(&path, format!("[server]\nport = {port}\n")).unwrap();
          tokio::select! {
            () = writing.cancelled() => break,
            () = tokio::time::sleep(Duration::from_millis(20)) => {}
          }
        }
      }
    });
    assert!(
      watcher
        .reload_settled(&mut files, &events, &cancel, SHORT)
        .await
    );
    writing.cancel();
    writer.await.unwrap();
    let current = watcher.current();
    assert_eq!(current.values.server.port, 2000);
    assert!(
      current
        .load_error
        .as_deref()
        .unwrap()
        .starts_with("config.toml still being written"),
      "{:?}",
      current.load_error
    );

    fs::write(&path, "[server]\nport = 4000\n").unwrap();
    assert!(
      watcher
        .reload_settled(&mut files, &events, &cancel, SHORT)
        .await
    );
    assert_eq!(watcher.current().values.server.port, 4000);
    assert_eq!(watcher.current().load_error, None);
  }

  #[cfg(target_os = "linux")]
  #[tokio::test]
  async fn a_file_held_open_that_ends_as_it_was_clears_the_report() {
    use std::io::Write;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.toml");
    fs::write(&path, "[server]\nport = 2000\n").unwrap();
    let (watcher, mut files, events) = settling(dir.path(), None);
    let cancel = CancellationToken::new();

    let mut file = fs::File::create(&path).unwrap();
    file.write_all(b"[server]\n").unwrap();
    assert!(
      watcher
        .reload_settled(&mut files, &events, &cancel, SHORT)
        .await
    );
    assert!(watcher.current().load_error.is_some());

    file.write_all(b"port = 2000\n").unwrap();
    drop(file);
    assert!(
      watcher
        .reload_settled(&mut files, &events, &cancel, SHORT)
        .await
    );
    assert_eq!(watcher.current().load_error, None);
    assert_eq!(watcher.current().values.server.port, 2000);
  }

  #[tokio::test]
  async fn a_file_that_keeps_changing_and_ends_as_it_was_clears_the_report() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.toml");
    let original = "[server]\nport = 2000\n";
    fs::write(&path, original).unwrap();
    let (watcher, mut files, events) = settling(dir.path(), Some(Duration::from_millis(50)));
    let cancel = CancellationToken::new();

    let writing = CancellationToken::new();
    let writer = tokio::spawn({
      let (path, writing) = (path.clone(), writing.clone());
      async move {
        for port in 3000.. {
          fs::write(&path, format!("[server]\nport = {port}\n")).unwrap();
          tokio::select! {
            () = writing.cancelled() => break,
            () = tokio::time::sleep(Duration::from_millis(20)) => {}
          }
        }
      }
    });
    assert!(
      watcher
        .reload_settled(&mut files, &events, &cancel, SHORT)
        .await
    );
    writing.cancel();
    writer.await.unwrap();
    assert!(watcher.current().load_error.is_some());

    fs::write(&path, original).unwrap();
    assert!(
      watcher
        .reload_settled(&mut files, &events, &cancel, SHORT)
        .await
    );
    assert_eq!(watcher.current().load_error, None);
  }

  /// On kernels before 6.8 a file opened to be rewritten is emptied before its opening is told.
  /// The file is emptied where the watch does not see it, and the events are fed in that order.
  #[cfg(target_os = "linux")]
  #[tokio::test]
  async fn a_file_emptied_before_its_opening_is_told_waits_for_its_writer() {
    use notify::event::DataChange;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.toml");
    fs::write(&path, "[server]\nport = 2000\n").unwrap();
    let elsewhere = tempfile::tempdir().unwrap();
    let link = elsewhere.path().join("config.toml");
    fs::hard_link(&path, &link).unwrap();
    let (watcher, mut files, events) = settling(dir.path(), None);
    let cancel = CancellationToken::new();

    let held = emptied(&link);
    let told = |kind: EventKind| notify::Event::new(kind).add_path(path.clone());
    files
      .seen
      .note(&told(EventKind::Modify(ModifyKind::Data(DataChange::Any))));
    files
      .seen
      .note(&told(EventKind::Access(AccessKind::Open(AccessMode::Any))));
    assert_eq!(files.writing(), Writing::Open(vec![path.clone()]));
    assert!(
      watcher
        .reload_settled(&mut files, &events, &cancel, SHORT)
        .await
    );
    let current = watcher.current();
    assert_eq!(
      current.values.server.port, 2000,
      "the empty file is not read"
    );
    assert!(
      current
        .load_error
        .as_deref()
        .unwrap()
        .starts_with("config.toml still open to the program writing it"),
      "{:?}",
      current.load_error
    );
    drop(held);
  }

  /// A drop-in emptied with `truncate(2)` by its path is modified and never closed.
  #[cfg(target_os = "linux")]
  #[tokio::test]
  async fn a_file_modified_and_never_closed_holds_back_only_one_wait() {
    use notify::event::DataChange;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.toml");
    fs::write(&path, "[server]\nport = 2000\n").unwrap();
    let drop_in = dir.path().join("config.d/10-a.toml");
    fs::create_dir(drop_in.parent().unwrap()).unwrap();
    fs::write(&drop_in, "[shares]\ndirectories = [\"/music\"]\n").unwrap();
    let (watcher, mut files, events) = settling(dir.path(), None);
    let cancel = CancellationToken::new();

    let modified = notify::Event::new(EventKind::Modify(ModifyKind::Data(DataChange::Any)));
    files.seen.note(&modified.add_path(drop_in.clone()));
    assert!(
      watcher
        .reload_settled(&mut files, &events, &cancel, SHORT)
        .await
    );
    assert!(
      watcher
        .current()
        .load_error
        .as_deref()
        .unwrap()
        .starts_with("config.d/10-a.toml still open to the program writing it"),
      "{:?}",
      watcher.current().load_error
    );

    // a whole save of another file, written aside and renamed over it
    let aside = dir.path().join("config.toml.new");
    fs::write(&aside, "[server]\nport = 3000\n").unwrap();
    fs::rename(&aside, &path).unwrap();
    let started = tokio::time::Instant::now();
    assert!(
      watcher
        .reload_settled(&mut files, &events, &cancel, SHORT)
        .await
    );
    assert!(started.elapsed() < SHORT.limit, "{:?}", started.elapsed());
    let current = watcher.current();
    assert_eq!(current.values.server.port, 3000);
    assert_eq!(current.load_error, None);
  }

  #[tokio::test]
  async fn an_edit_made_before_the_watch_began_is_read_at_the_start() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.toml");
    fs::write(&path, "[server]\nport = 2000\n").unwrap();
    let watcher = watched(dir.path());
    fs::write(&path, "[server]\nport = 2001\n").unwrap();
    let cancel = CancellationToken::new();
    watcher.start(Watch::Events, cancel.clone()).await;
    assert_eq!(watcher.current().values.server.port, 2001);
    cancel.cancel();
  }

  #[tokio::test]
  async fn a_drop_in_directory_made_again_is_watched_again() {
    let dir = tempfile::tempdir().unwrap();
    let drop_ins = dir.path().join("config.d");
    fs::create_dir(&drop_ins).unwrap();
    let watcher = watched(dir.path());
    let mut changes = watcher.changes();
    let cancel = CancellationToken::new();
    watcher.start(Watch::Events, cancel.clone()).await;

    // removed and made again in one burst, which ends the old directory's watch
    fs::remove_dir_all(&drop_ins).unwrap();
    fs::create_dir(&drop_ins).unwrap();
    tokio::time::sleep(DEBOUNCE * 2).await;
    fs::write(drop_ins.join("10-a.toml"), "[server]\nport = 2001\n").unwrap();
    assert_eq!(
      next_change(&mut changes).await.current.values.server.port,
      2001
    );
    cancel.cancel();
  }

  #[tokio::test]
  async fn polling_sees_changes_too() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.toml");
    fs::write(&path, "[server]\nport = 2000\n").unwrap();
    let watcher = watched(dir.path());
    let mut changes = watcher.changes();
    let cancel = CancellationToken::new();
    watcher
      .start(Watch::Poll(Duration::from_millis(50)), cancel.clone())
      .await;
    fs::write(
      &path,
      "[server]\nport = 2001\n# longer, so the size changes too\n",
    )
    .unwrap();
    assert_eq!(
      next_change(&mut changes).await.current.values.server.port,
      2001
    );
    cancel.cancel();
  }

  #[tokio::test]
  async fn our_own_write_is_not_loaded_twice() {
    let dir = tempfile::tempdir().unwrap();
    let watcher = watched(dir.path());
    let mut changes = watcher.changes();
    let cancel = CancellationToken::new();
    watcher.start(Watch::Events, cancel.clone()).await;
    watcher
      .patch_file(
        "config.toml",
        None,
        None,
        values(&[("server.port", json!(3000))]),
        vec![],
      )
      .await
      .unwrap();
    next_change(&mut changes).await;
    let again = tokio::time::timeout(DEBOUNCE * 4, changes.recv()).await;
    assert!(again.is_err(), "{again:?}");
    cancel.cancel();
  }
}
