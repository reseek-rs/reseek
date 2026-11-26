//! Keeping the index current between scans: every share is watched with the system's change
//! notifications, and a path that has been quiet for a while after a change is applied by what
//! is on disk then. A file is read again unless its size and time are unchanged, a directory
//! the index does not have is scanned with everything under it, and a path that is gone takes
//! its rows with it. When the system says changes were dropped, the directory they were in is
//! scanned again. When they were dropped for a whole share, the index is marked drifted at
//! once and a scan of the shares starts once the shares have been quiet.
//!
//! Notifications come from the local kernel only: a change made on another machine to a
//! network or FUSE mount is not seen until a rescan.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::LogLimiter;
use notify::event::{AccessKind, AccessMode};
use notify::{
  Config, ErrorKind, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher as _,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, info, warn};

use super::phase::Edge;
use super::repository::{self, DirectoryRows};
use super::scanner::{self, Settings};
use super::share::{self, Share};
use super::walk;
use super::{ScanOrigin, Shares};
use crate::db::DbError;
use crate::entries;
use crate::lock;

/// How many notifications wait to be sorted at most; past it the notifying thread waits.
const BACKLOG: usize = 4096;

static WATCH_FAILED: LogLimiter = LogLimiter::new();

/// The shares being watched. Dropping it stops the watches and the task applying their
/// changes, which applies what was still waiting for quiet first; [`Watching::retire`] stops
/// them without applying anything more.
pub(super) struct Watching {
  watcher: RecommendedWatcher,
  /// the share directories watched
  pub(super) roots: Vec<PathBuf>,
  /// the settings changes are applied by
  pub(super) settings: Arc<Settings>,
  /// stops the task and any write it has not made yet
  retired: CancellationToken,
}

impl Watching {
  /// Stops the watches and drops what they saw without writing it, for when the index is about
  /// to be filled by other settings: a write by these settings would put back what those leave
  /// out, stamped after the scan started so its prune keeps it.
  pub(super) fn retire(self) {
    self.retired.cancel();
  }

  /// Stops watching the shares no longer in `shares`.
  pub(super) fn keep_only(&mut self, shares: &[Share]) {
    let watcher = &mut self.watcher;
    self.roots.retain(|root| {
      let kept = shares
        .iter()
        .any(|s| !s.excluded && share::same_path(&s.local_path, root));
      if !kept && let Err(e) = watcher.unwatch(root) {
        debug!(root = %root.display(), error = %e, "share watch not removed");
      }
      kept
    });
  }
}

/// Watches every share `settings` covers and starts applying their changes; `None` when no
/// watch could be made at all.
pub(super) async fn start(shares: Arc<Shares>, settings: Arc<Settings>) -> Option<Watching> {
  let (sender, receiver) = mpsc::channel(BACKLOG);
  let handler = move |event: notify::Result<Event>| {
    if event.as_ref().is_ok_and(changes_nothing) {
      return;
    }
    // the receiver is gone once the watches are dropped, and then nothing needs the event
    drop(sender.blocking_send(event));
  };
  let config = Config::default().with_follow_symlinks(settings.follow_symlinks);
  let mut watcher = match RecommendedWatcher::new(handler, config) {
    Ok(watcher) => watcher,
    Err(e) => {
      warn!(error = %e, "shares are not watched; changes are seen by a rescan");
      return None;
    }
  };
  let roots: Vec<(PathBuf, String)> = settings
    .shares
    .iter()
    .filter(|s| !s.excluded)
    .map(|s| (s.local_path.clone(), s.alias.clone()))
    .collect();
  let watched = tokio::task::spawn_blocking(move || {
    let mut watched = Vec::new();
    for (root, alias) in roots {
      if !root.is_dir() {
        // the scan warns that the share is not a directory
        debug!(share = alias, "share not watched: it is not a directory");
        continue;
      }
      match watcher.watch(&root, RecursiveMode::Recursive) {
        Ok(()) => {
          if let Some(kind) = remote_filesystem(&root) {
            info!(
              share = alias,
              filesystem = kind,
              "Share is on a remote filesystem; changes made elsewhere are not seen until a rescan"
            );
          }
          watched.push(root);
        }
        Err(e) if matches!(e.kind, ErrorKind::MaxFilesWatch) => warn!(
          share = alias,
          "share not watched: the system's limit of watches is reached; raise the \
           fs.inotify.max_user_watches sysctl. Its changes are seen by a rescan"
        ),
        Err(e) => {
          warn!(share = alias, error = %e, "share not watched; its changes are seen by a rescan")
        }
      }
    }
    (watcher, watched)
  })
  .await;
  let (watcher, roots) = match watched {
    Ok(watched) => watched,
    Err(e) => {
      warn!(error = %e, "shares are not watched; changes are seen by a rescan");
      return None;
    }
  };
  let retired = shares.shutdown.child_token();
  tokio::spawn(
    apply_changes(
      shares,
      settings.clone(),
      roots.clone(),
      retired.clone(),
      receiver,
    )
    .in_current_span(),
  );
  Some(Watching {
    watcher,
    roots,
    settings,
    retired,
  })
}

/// Whether `event` only says a file was opened, read or closed without being written, as when
/// a peer downloads it; a close after writing is a change.
fn changes_nothing(event: &Event) -> bool {
  !event.need_rescan()
    && matches!(event.kind, EventKind::Access(access)
      if access != AccessKind::Close(AccessMode::Write))
}

/// What a watched path needs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Need {
  /// it changed: apply what is on disk
  Change,
  /// changes under it were dropped: scan it again
  Rescan,
}

/// Collects notifications per path and applies each path once it has been quiet for the
/// engine's quiet time, and starts a scan once that time has passed since changes were last
/// dropped for a whole share. Ends when the watches are dropped, applying what was still
/// waiting but starting no scan, or when `retired`, applying nothing more.
pub(super) async fn apply_changes(
  shares: Arc<Shares>,
  settings: Arc<Settings>,
  roots: Vec<PathBuf>,
  retired: CancellationToken,
  mut receiver: mpsc::Receiver<notify::Result<Event>>,
) {
  let quiet = shares.quiet;
  let mut pending: HashMap<PathBuf, (Instant, Need)> = HashMap::new();
  // when changes were last dropped for a whole share, while its scan waits for quiet
  let mut missed: Option<Instant> = None;
  let mut tick = tokio::time::interval((quiet / 4).max(Duration::from_millis(10)));
  loop {
    tokio::select! {
      () = retired.cancelled() => return,
      message = receiver.recv() => {
        let Some(message) = message else {
          // the watches were replaced or dropped: what was still waiting for quiet is applied
          // now, since the watches after them saw none of it
          let due: Vec<(PathBuf, Need)> =
            pending.drain().map(|(path, (_, need))| (path, need)).collect();
          if !due.is_empty()
            && let Err(e) = apply(&shares, &settings, &roots, &retired, due).await
          {
            warn!(error = %e, "watched share changes not applied; they are seen by the next rescan");
          }
          return;
        };
        let now = Instant::now();
        let (paths, need) = match message {
          Ok(event) if event.need_rescan() => (event.paths, Need::Rescan),
          Ok(event) => (event.paths, Need::Change),
          Err(e) => {
            if let Some(suppressed) = WATCH_FAILED.allow() {
              warn!(
                error = %e,
                suppressed,
                "a share watch reported an error; what it covers is scanned again"
              );
            }
            (e.paths, Need::Rescan)
          }
        };
        if need == Need::Rescan && lost_a_share(&roots, &paths) {
          mark_missed(&shares).await;
          missed = Some(now);
        } else {
          for path in paths {
            let entry = pending.entry(path).or_insert((now, need));
            entry.0 = now;
            if need == Need::Rescan {
              entry.1 = Need::Rescan;
            }
          }
        }
      }
      _ = tick.tick() => {}
    }
    if missed.is_some_and(|at| at.elapsed() >= quiet) {
      missed = None;
      scan_missed(&shares).await;
    }
    let due: Vec<(PathBuf, Need)> = pending
      .iter()
      .filter(|(_, (at, _))| at.elapsed() >= quiet)
      .map(|(path, (_, need))| (path.clone(), *need))
      .collect();
    if due.is_empty() {
      continue;
    }
    for (path, _) in &due {
      pending.remove(path);
    }
    if let Err(e) = apply(&shares, &settings, &roots, &retired, due).await {
      warn!(error = %e, "watched share changes not applied; they are seen by the next rescan");
    }
  }
}

/// Whether changes dropped under `paths`, which are none when the system does not say where,
/// take in a whole share of `roots`.
fn lost_a_share(roots: &[PathBuf], paths: &[PathBuf]) -> bool {
  paths.is_empty()
    || paths
      .iter()
      .any(|path| roots.iter().any(|root| share::is_within(root, path)))
}

/// Marks the index drifted after changes under a whole share were dropped, so a scan running
/// now leaves a rescan pending.
async fn mark_missed(shares: &Shares) {
  if let Err(e) = shares.request_scan().await {
    warn!(error = %e, "the index not marked drifted after changes were missed");
  }
}

/// Starts a scan of the shares once they are quiet after changes were dropped; a scan already
/// running is marked drifted again and a rescan stays pending after it, as it may have read
/// the shares before the changes.
async fn scan_missed(shares: &Shares) {
  mark_missed(shares).await;
  match shares.scan(false, ScanOrigin::Missed) {
    Ok(()) => info!("Changes under the shares were missed; scanning them again"),
    Err(conflict) => debug!(
      ?conflict,
      "changes under the shares were missed during a scan; a rescan stays pending"
    ),
  }
}

/// Whether the walk's rules let `path` into the index: in a share, not excluded, neither it nor
/// a directory between it and its share hidden or filtered, and, with links followed, not
/// excluded or filtered where it really is.
fn allowed<'a>(settings: &'a Settings, path: &Path) -> Option<&'a Share> {
  let owner = share::owner(&settings.shares, path)?;
  if share::excluded(&settings.shares, path) {
    return None;
  }
  let below = share::relative(&owner.local_path, path)?;
  let mut current = owner.local_path.clone();
  for part in below {
    current.push(part);
    // only Windows keeps hidden in an attribute rather than in the name
    let metadata = if cfg!(windows) {
      std::fs::metadata(&current).ok()
    } else {
      None
    };
    if current
      .file_name()
      .is_some_and(|name| entries::left_out(name, metadata.as_ref()))
    {
      return None;
    }
    if settings.filtered(&current) {
      return None;
    }
  }
  // a path that is gone has no real path, and is let through so its rows are removed
  if settings.follow_symlinks
    && let Ok(real) = path.canonicalize()
    && walk::Real::of(settings).leaves_out(&real)
  {
    return None;
  }
  Some(owner)
}

/// One change to the index.
enum Write {
  /// a directory and its files, as read now
  Directory(DirectoryRows),
  /// a file or directory that is gone, with everything under it
  Remove(String),
  /// what is under a directory that was scanned again and was not seen
  Prune(String),
}

/// A due path as it is on disk: the share that owns it, its remote name, and its metadata,
/// `None` when it is gone or is a link not followed.
struct Looked {
  path: PathBuf,
  need: Need,
  owner: Share,
  masked: String,
  metadata: Option<std::fs::Metadata>,
}

/// Looks at each due path on disk, leaving out those no share takes; it reads the disk, so it
/// runs on a blocking thread.
fn look(settings: &Settings, due: Vec<(PathBuf, Need)>) -> Vec<Looked> {
  let mut looked = Vec::new();
  for (path, need) in due {
    let Some(owner) = allowed(settings, &path).cloned() else {
      continue;
    };
    let Some(masked) = share::mask(&owner, &path) else {
      continue;
    };
    let link = std::fs::symlink_metadata(&path).is_ok_and(|m| m.file_type().is_symlink());
    let metadata = if link && !settings.follow_symlinks {
      None
    } else {
      std::fs::metadata(&path).ok()
    };
    looked.push(Looked {
      path,
      need,
      owner,
      masked: masked.name,
      metadata,
    });
  }
  looked
}

/// Applies the due paths in one transaction, then counts the shares again; nothing is written
/// once `retired`.
pub(super) async fn apply(
  shares: &Arc<Shares>,
  settings: &Arc<Settings>,
  roots: &[PathBuf],
  retired: &CancellationToken,
  due: Vec<(PathBuf, Need)>,
) -> Result<(), scanner::ScanError> {
  let mut writes = Vec::new();
  let mut read: HashSet<PathBuf> = HashSet::new();
  let looked = {
    let settings = settings.clone();
    tokio::task::spawn_blocking(move || look(&settings, due))
      .await
      .map_err(|e| scanner::ScanError::Task(e.to_string()))?
  };
  for Looked {
    path,
    need,
    owner,
    masked,
    metadata,
  } in looked
  {
    match metadata {
      None if roots.iter().any(|root| share::same_path(root, &path)) => {
        // a share directory that went away is most likely an unmounted drive, so what was
        // indexed of it stays until a scan; the share is watched again after the next one
        warn!(
          share = owner.alias,
          "a watched share is gone; it is watched again after the next rescan"
        );
        if let Some(watching) = &mut *lock(&shares.watches) {
          watching.roots.retain(|r| !share::same_path(r, &path));
        }
      }
      None => writes.push(Write::Remove(masked)),
      Some(m) if m.is_file() => {
        if let Some(parent) = path.parent()
          && read.insert(parent.to_path_buf())
          && let Some(rows) =
            walk::read_from_disk(&shares.db, settings, parent.to_path_buf(), false).await?
        {
          writes.push(Write::Directory(rows.rows));
        }
      }
      Some(m) if m.is_dir() => {
        let name = masked.clone();
        let known = shares
          .db
          .read(move |conn| repository::has_directory(conn, &name))
          .await?;
        if need == Need::Rescan || !known {
          let walk = {
            let settings = settings.clone();
            let cancel = retired.clone();
            let path = path.clone();
            tokio::task::spawn_blocking(move || {
              let mut seen = HashSet::new();
              walk::directories_under(&settings, &path, true, &cancel, &mut seen).0
            })
            .await
            .map_err(|e| scanner::ScanError::Task(e.to_string()))?
          };
          for directory in walk {
            if read.insert(directory.clone())
              && let Some(rows) =
                walk::read_from_disk(&shares.db, settings, directory, false).await?
            {
              writes.push(Write::Directory(rows.rows));
            }
          }
          writes.push(Write::Prune(masked));
        }
      }
      Some(_) => {}
    }
  }
  if writes.is_empty() {
    return Ok(());
  }
  let timestamp = scanner::now_ms();
  let count = writes.len();
  let retired = retired.clone();
  let written = shares
    .db
    .write(move |conn| {
      // checked in the write, so a write made before a scan retired the watches is stamped no
      // later than the scan's start, and one after it is not made
      if retired.is_cancelled() {
        return Ok(false);
      }
      let tx = conn.transaction()?;
      for write in &writes {
        match write {
          Write::Directory(rows) => {
            let ids = repository::write_directory(&tx, timestamp, rows)?;
            repository::index_files(&tx, &ids, &rows.files)?;
          }
          Write::Remove(masked) => {
            repository::remove(&tx, masked)?;
          }
          Write::Prune(masked) => {
            repository::prune_under(&tx, masked, timestamp)?;
          }
        }
      }
      tx.commit().map(|()| true)
    })
    .await
    .map_err(scanner::ScanError::from)?;
  if !written {
    debug!("watched share changes dropped: the watches were retired");
    return Ok(());
  }
  debug!(changes = count, "watched share changes applied");
  counted(shares).await.map_err(scanner::ScanError::from)
}

/// Counts the shares the index was filled from after a change and lets the totals, the
/// server's counts and the browse cache follow, the last two at most once a minute.
async fn counted(shares: &Arc<Shares>) -> Result<(), DbError> {
  let totals = shares
    .count_shares(&shares.filled_from().shares)
    .await?
    .total();
  // a scan under way serves these totals when it does not complete
  shares.transition(Edge::Counted(totals));
  shares.throttled(
    |s| &s.counts_throttle,
    |shares| async move {
      // counted when the throttled send runs, so the changes since it was armed are in it
      match shares.count_shares(&shares.filled_from().shares).await {
        Ok(local) => {
          *lock(&shares.local_counts) = local;
          shares.recount_for_server();
        }
        Err(e) => warn!(error = %e, "shared counts not read for the server"),
      }
    },
  );
  shares.throttled(
    |s| &s.browse_throttle,
    |shares| async move {
      shares.refresh_browse_caches().await;
      shares.changed.send_modify(|n| *n += 1);
    },
  );
  Ok(())
}

/// The kind of filesystem `root` is on when it is one whose changes made elsewhere send no
/// notification: NFS, SMB and FUSE. Read from the mount table on Linux; elsewhere not known.
fn remote_filesystem(root: &Path) -> Option<String> {
  if !cfg!(target_os = "linux") {
    return None;
  }
  let root = root.canonicalize().ok()?;
  let table = std::fs::read_to_string("/proc/self/mountinfo").ok()?;
  let mut best: Option<(usize, String)> = None;
  for line in table.lines() {
    let fields: Vec<&str> = line.split(' ').collect();
    let Some(separator) = fields.iter().position(|f| *f == "-") else {
      continue;
    };
    let (Some(point), Some(kind)) = (fields.get(4), fields.get(separator + 1)) else {
      continue;
    };
    let point = PathBuf::from(point.replace("\\040", " "));
    let depth = point.components().count();
    if root.starts_with(&point) && best.as_ref().is_none_or(|(d, _)| depth > *d) {
      best = Some((depth, (*kind).to_string()));
    }
  }
  let (_, kind) = best?;
  let remote =
    matches!(kind.as_str(), "nfs" | "nfs4" | "cifs" | "smb3" | "smbfs") || kind.starts_with("fuse");
  remote.then_some(kind)
}

#[cfg(test)]
mod tests {
  use notify::event::{CreateKind, DataChange, Flag, ModifyKind, RemoveKind};

  use super::*;

  #[test]
  fn changes_lost_at_or_above_a_share_take_it_in() {
    let roots = [PathBuf::from("/music"), PathBuf::from("/films")];
    let lost = |paths: &[&str]| {
      let paths: Vec<PathBuf> = paths.iter().map(PathBuf::from).collect();
      lost_a_share(&roots, &paths)
    };
    assert!(lost(&[]), "the system did not say where");
    assert!(lost(&["/music"]));
    assert!(lost(&["/"]));
    assert!(!lost(&["/music/Album"]), "scanned again where it is");
    assert!(!lost(&["/musicals"]));
  }

  #[test]
  fn a_file_opened_or_read_without_writing_changes_nothing() {
    let read = [
      AccessKind::Open(AccessMode::Read),
      AccessKind::Open(AccessMode::Any),
      AccessKind::Read,
      AccessKind::Close(AccessMode::Read),
      AccessKind::Any,
    ];
    for access in read {
      assert!(
        changes_nothing(&Event::new(EventKind::Access(access))),
        "{access:?}"
      );
    }
    let changed = [
      EventKind::Access(AccessKind::Close(AccessMode::Write)),
      EventKind::Modify(ModifyKind::Data(DataChange::Any)),
      EventKind::Create(CreateKind::File),
      EventKind::Remove(RemoveKind::Any),
      EventKind::Any,
    ];
    for kind in changed {
      assert!(!changes_nothing(&Event::new(kind)), "{kind:?}");
    }
    let dropped = Event::new(EventKind::Access(AccessKind::Read)).set_flag(Flag::Rescan);
    assert!(!changes_nothing(&dropped), "a lost notice is scanned again");
  }
}
