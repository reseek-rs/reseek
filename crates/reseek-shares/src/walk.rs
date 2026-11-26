//! The walk of the shares: which directories a scan takes, and reading one directory's files
//! into rows. Hidden and system entries, links unless they are followed, directories it may
//! not read, and, with everything under them, excluded directories and directories a filter
//! matches are left out. When links are followed, a directory or file is also judged by where
//! it really is, so a link cannot share what an exclusion or a filter leaves out. A file whose
//! size and modification time match its row keeps the attributes read before, unless the scan
//! is forced.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use crate::LogLimiter;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use super::attributes;
use super::repository::{self, DirectoryRows, FileRow, Known};
use super::scanner::{ScanError, Settings};
use super::share::{self, CASE_SENSITIVE, Share};
use crate::db::Database;
use crate::entries;

/// Directories the walk cannot read, which a large share can hold many of.
static UNREADABLE: LogLimiter = LogLimiter::new();

/// The key that tells two directory paths apart by this system's case rule.
fn key(path: &Path) -> PathBuf {
  if CASE_SENSITIVE {
    path.to_path_buf()
  } else {
    PathBuf::from(path.to_string_lossy().to_lowercase())
  }
}

/// The real directories of the configured shares and exclusions, for judging a path reached
/// through links by where it really is.
pub(super) struct Real<'a> {
  settings: &'a Settings,
  roots: Vec<(PathBuf, &'a Share)>,
}

impl<'a> Real<'a> {
  /// Reads the real directory of every entry of `settings` that exists.
  pub(super) fn of(settings: &'a Settings) -> Self {
    let roots = settings
      .shares
      .iter()
      .filter_map(|share| Some((share.local_path.canonicalize().ok()?, share)))
      .collect();
    Self { settings, roots }
  }

  /// Whether `real`, a canonical path, is excluded or filtered. Under the real directory of an
  /// entry it is judged as the path it has under that entry's configured path, the closest
  /// entry when several hold it, so exclusions and filters match it as they are written.
  pub(super) fn leaves_out(&self, real: &Path) -> bool {
    let closest = self
      .roots
      .iter()
      .filter_map(|(root, share)| Some((share, root, share::relative(root, real)?)))
      .max_by_key(|(_, root, _)| root.components().count());
    let path = match closest {
      Some((share, _, _)) if share.excluded => return true,
      Some((share, _, rest)) => {
        let mut path = share.local_path.clone();
        path.extend(rest);
        path
      }
      None => real.to_path_buf(),
    };
    share::excluded(&self.settings.shares, &path) || self.settings.filtered(&path)
  }
}

/// The directories to scan under `root`, `root` first, by the walk's rules, and how many were
/// left out because they are excluded or filtered. `root` is always taken when `with_root`.
pub(super) fn directories_under(
  settings: &Settings,
  root: &Path,
  with_root: bool,
  cancel: &CancellationToken,
  seen: &mut HashSet<PathBuf>,
) -> (Vec<PathBuf>, u32) {
  let mut found = Vec::new();
  let mut left_out = 0u32;
  if with_root && seen.insert(key(root)) {
    found.push(root.to_path_buf());
  }
  let mut canonical: HashSet<PathBuf> = HashSet::new();
  if settings.follow_symlinks
    && let Ok(root) = root.canonicalize()
  {
    canonical.insert(root);
  }
  let real = settings.follow_symlinks.then(|| Real::of(settings));
  let walk = walkdir::WalkDir::new(root)
    .follow_links(settings.follow_symlinks)
    // which of two ways to one directory wins should not depend on the order the system lists
    .sort_by_file_name()
    .into_iter()
    .filter_entry(|entry| {
      if entry.depth() == 0 {
        return true;
      }
      let metadata = entry.metadata().ok();
      if !entry.file_type().is_dir() || entries::left_out(entry.file_name(), metadata.as_ref()) {
        return false;
      }
      let path = entry.path();
      if share::excluded(&settings.shares, path) {
        debug!(
          directory = %path.display(),
          "Excluding directory because it is a subdirectory of an excluded share"
        );
        left_out += 1;
        return false;
      }
      if settings.filtered(path) {
        debug!(
          directory = %path.display(),
          "Excluding directory because it matches one or more share filters"
        );
        left_out += 1;
        return false;
      }
      let Some(real) = &real else {
        return true;
      };
      let Ok(canonical_path) = path.canonicalize() else {
        return false;
      };
      if real.leaves_out(&canonical_path) {
        debug!(
          directory = %path.display(),
          "Excluding directory because a link leads to an excluded or filtered directory"
        );
        left_out += 1;
        return false;
      }
      // a directory reached a second time, through a link or to one, would be indexed twice
      // under two names, or walked forever
      canonical.insert(canonical_path)
    });
  for entry in walk {
    if cancel.is_cancelled() {
      break;
    }
    // a directory that cannot be read is left out, with everything under it. walkdir yields
    // the directory itself before it fails to list it, so it is taken back out here
    let entry = match entry {
      Ok(entry) => entry,
      Err(e) => {
        if let Some(path) = e.path() {
          found.retain(|taken| taken != path);
          seen.remove(&key(path));
        }
        if let Some(suppressed) = UNREADABLE.allow() {
          warn!(error = %e, suppressed, "a shared directory cannot be read; it is left out");
        }
        continue;
      }
    };
    if entry.depth() > 0 && seen.insert(key(entry.path())) {
      found.push(entry.into_path());
    }
  }
  (found, left_out)
}

/// Every directory to scan, each share's own first, and how many were left out.
pub(super) fn directories(settings: &Settings, cancel: &CancellationToken) -> (Vec<PathBuf>, u32) {
  let mut seen = HashSet::new();
  let mut all = Vec::new();
  let mut left_out = 0;
  for share in settings.shares.iter().filter(|s| !s.excluded) {
    if let Err(e) = std::fs::metadata(&share.local_path).and_then(|m| {
      if m.is_dir() {
        Ok(())
      } else {
        Err(std::io::Error::other("not a directory"))
      }
    }) {
      warn!(share = %share.local_path.display(), error = %e, "Failed to scan share");
      continue;
    }
    let (found, out) = directories_under(settings, &share.local_path, true, cancel, &mut seen);
    all.extend(found);
    left_out += out;
  }
  (all, left_out)
}

/// The files directly in a directory, read from disk.
#[derive(Debug)]
pub(super) struct Read {
  /// the directory and its files as the index writes them
  pub rows: DirectoryRows,
  /// files a filter or, through a link, an exclusion left out
  pub filtered: u32,
  /// files whose name was not UTF-8
  pub lossy: u32,
}

/// Reads the files directly in `directory`, which `share` owns: their size, modification time
/// and attributes, reusing those of `known` rows that did not change unless `force`. One file
/// that cannot be read is logged and left out; its siblings are read.
pub(super) fn read_directory(
  settings: &Settings,
  share: &Share,
  directory: &Path,
  known: &HashMap<String, Known>,
  force: bool,
) -> Option<Read> {
  let masked = share::mask(share, directory)?;
  let mut read = Read {
    rows: DirectoryRows {
      name: masked.name,
      files: Vec::new(),
    },
    filtered: 0,
    lossy: u32::from(masked.lossy),
  };
  let entries = match std::fs::read_dir(directory) {
    Ok(entries) => entries,
    Err(e) => {
      warn!(directory = %directory.display(), error = %e, "Failed to scan files in directory");
      return Some(read);
    }
  };
  // with links followed, each file is judged by where it really is as well
  let found_at = if settings.follow_symlinks {
    match directory.canonicalize() {
      Ok(real_directory) => Some((Real::of(settings), real_directory)),
      Err(e) => {
        warn!(directory = %directory.display(), error = %e, "Failed to scan files in directory");
        return Some(read);
      }
    }
  } else {
    None
  };
  for entry in entries {
    let entry = match entry {
      Ok(entry) => entry,
      Err(e) => {
        warn!(directory = %directory.display(), error = %e, "Failed to scan files in directory");
        continue;
      }
    };
    let path = entry.path();
    let link = entry.file_type().is_ok_and(|t| t.is_symlink());
    if link && !settings.follow_symlinks {
      continue;
    }
    let metadata = match std::fs::metadata(&path) {
      Ok(metadata) => metadata,
      Err(e) => {
        debug!(file = %path.display(), error = %e, "file not read");
        continue;
      }
    };
    if !metadata.is_file() || entries::left_out(&entry.file_name(), Some(&metadata)) {
      continue;
    }
    let really_left_out = |(real, real_directory): &(Real, PathBuf)| {
      let real_path = if link {
        path.canonicalize()
      } else {
        Ok(real_directory.join(entry.file_name()))
      };
      real_path.map_or(true, |p| real.leaves_out(&p))
    };
    if settings.filtered(&path) || found_at.as_ref().is_some_and(really_left_out) {
      read.filtered += 1;
      continue;
    }
    let Some(file) = share::mask(share, &path) else {
      continue;
    };
    read.lossy += u32::from(file.lossy);
    let touched_at = metadata
      .modified()
      .ok()
      .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
      .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX));
    let name = entry.file_name();
    let extension = attributes::extension(&name.to_string_lossy());
    let size = metadata.len();
    let attributes = match known.get(&file.name) {
      Some(row) if !force && row.size == size && row.touched_at == touched_at => {
        row.attributes.clone()
      }
      _ => attributes::read(&path, &extension),
    };
    read.rows.files.push(FileRow {
      masked: file.name,
      original: repository::path_bytes(&path),
      size,
      touched_at,
      extension,
      attributes,
    });
  }
  Some(read)
}

/// Reads one directory with its known rows from the index, on a blocking thread.
pub(super) async fn read_from_disk(
  db: &Database,
  settings: &Arc<Settings>,
  directory: PathBuf,
  force: bool,
) -> Result<Option<Read>, ScanError> {
  let Some(share) = share::owner(&settings.shares, &directory).cloned() else {
    return Ok(None);
  };
  let Some(masked) = share::mask(&share, &directory) else {
    return Ok(None);
  };
  let known = db
    .read(move |conn| repository::known_files(conn, &masked.name))
    .await?;
  let settings = settings.clone();
  tokio::task::spawn_blocking(move || {
    debug!(directory = %directory.display(), "Starting scan of directory");
    read_directory(&settings, &share, &directory, &known, force)
  })
  .await
  .map_err(|e| ScanError::Task(e.to_string()))
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::scanner::tests::settings;

  #[cfg(unix)]
  #[test]
  fn a_directory_reached_twice_through_links_is_walked_once() {
    let dir = tempfile::tempdir().unwrap();
    let music = dir.path().join("music");
    std::fs::create_dir_all(music.join("A")).unwrap();
    std::fs::create_dir_all(music.join("B")).unwrap();
    std::os::unix::fs::symlink(music.join("A"), music.join("B/link")).unwrap();
    std::os::unix::fs::symlink(&music, music.join("A/loop")).unwrap();
    let followed = settings(dir.path(), &[], &[], true);
    let (found, _) = directories_under(
      &followed,
      &music,
      true,
      &CancellationToken::new(),
      &mut HashSet::new(),
    );
    let names: Vec<_> = found
      .iter()
      .map(|p| p.strip_prefix(&music).unwrap().display().to_string())
      .collect();
    assert_eq!(names, ["", "A", "B"], "B/link is A again, A/loop the root");
  }

  #[cfg(unix)]
  #[test]
  fn a_followed_link_shares_nothing_excluded_or_filtered_where_it_leads() {
    use std::os::unix::fs::symlink;
    let dir = tempfile::tempdir().unwrap();
    // the share is itself a link, so its configured path is not its real one
    let store = dir.path().join("store");
    for sub in ["Album", "private", "Skipped"] {
      std::fs::create_dir_all(store.join(sub)).unwrap();
    }
    std::fs::write(store.join("Album/01.mp3"), "1").unwrap();
    std::fs::write(store.join("private/p.mp3"), "p").unwrap();
    std::fs::write(store.join("Skipped/s.mp3"), "s").unwrap();
    symlink(store.join("private"), store.join("Album/more")).unwrap();
    symlink(store.join("Skipped"), store.join("Album/other")).unwrap();
    symlink(store.join("private/p.mp3"), store.join("Album/02.mp3")).unwrap();
    symlink(store.join("Skipped/s.mp3"), store.join("Album/03.mp3")).unwrap();
    let music = dir.path().join("music");
    symlink(&store, &music).unwrap();
    let settings = settings(dir.path(), &["!{}/music/private"], &["Skipped"], true);
    let (found, left_out) = directories_under(
      &settings,
      &music,
      true,
      &CancellationToken::new(),
      &mut HashSet::new(),
    );
    let names: Vec<_> = found
      .iter()
      .map(|p| p.strip_prefix(&music).unwrap().display().to_string())
      .collect();
    assert_eq!(names, ["", "Album"]);
    assert_eq!(left_out, 4, "private, Skipped and the two links to them");
    let album = music.join("Album");
    let share = share::owner(&settings.shares, &album).unwrap();
    let read = read_directory(&settings, share, &album, &HashMap::new(), false).unwrap();
    let files: Vec<_> = read.rows.files.iter().map(|f| f.masked.as_str()).collect();
    assert_eq!(files, ["Music\\Album\\01.mp3"]);
    assert_eq!(read.filtered, 2);
  }

  #[cfg(unix)]
  #[test]
  fn a_directory_that_can_not_be_read_is_left_out_with_one_warning() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let music = dir.path().join("music");
    std::fs::create_dir_all(music.join("open")).unwrap();
    std::fs::create_dir_all(music.join("shut/inner")).unwrap();
    std::fs::set_permissions(music.join("shut"), std::fs::Permissions::from_mode(0o000)).unwrap();
    if std::fs::read_dir(music.join("shut")).is_ok() {
      // run with privileges that read any directory, there is nothing to leave out
      return;
    }
    let settings = settings(dir.path(), &[], &[], false);
    let ((found, _), lines) = crate::testing::logged(|| {
      directories_under(
        &settings,
        &music,
        true,
        &CancellationToken::new(),
        &mut HashSet::new(),
      )
    });
    std::fs::set_permissions(music.join("shut"), std::fs::Permissions::from_mode(0o755)).unwrap();
    let names: Vec<_> = found
      .iter()
      .map(|p| p.strip_prefix(&music).unwrap().display().to_string())
      .collect();
    assert_eq!(names, ["", "open"]);
    assert_eq!(lines.matches("cannot be read").count(), 1, "{lines}");
  }
}
