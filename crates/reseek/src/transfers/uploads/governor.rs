//! The upload speed limits of the groups: one bucket per group, filled every 100 ms with its
//! group's `speed_limit`, `privileged` with the global upload limit in force, the alternative
//! one while the alternative limits are switched on. Every chunk of an upload asks the bucket of
//! the group its user is in at that moment, then the global upload bucket for what the group
//! granted, so the slower limit governs. What each bucket granted and the chunk did not move,
//! because the global limit took less or the socket took less, goes back to it.
//!
//! A change of a group's speed or of the global speed gives every group a new full bucket at
//! once; a chunk already waiting on an old bucket is served by it within 100 ms.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock};

use reseek_shares::lock;
use soulseek_client::{TransferGovernor as ChunkGovernor, TransferReporter as Reporter};
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::config::{DEFAULT_GROUP, Groups, Options, PRIVILEGED_GROUP};
use crate::transfers::bucket::TokenBucket;

/// Looks up the group an upload's user is in now.
pub type CurrentGroup = Arc<dyn Fn() -> String + Send + Sync>;

/// The groups' buckets, and the global one every upload asks after its group's.
pub struct Governor {
  buckets: RwLock<Arc<HashMap<String, Arc<TokenBucket>>>>,
  global: Arc<TokenBucket>,
  /// what the buckets were made from
  built_from: Mutex<(Groups, Option<u32>)>,
  /// the alternative speed limits are in force
  alternative: AtomicBool,
}

/// One bucket per group for `options`, each full, with the alternative global limit when
/// `alternative`.
fn buckets(options: &Options, alternative: bool) -> HashMap<String, Arc<TokenBucket>> {
  let groups = &options.transfers.groups;
  // an unset limit is the bucket's no limit
  let bucket = |limit: Option<u32>| Arc::new(TokenBucket::for_speed(limit.unwrap_or(0)));
  let mut buckets = HashMap::from([(
    PRIVILEGED_GROUP.to_string(),
    bucket(options.transfers.upload_speed_limit(alternative)),
  )]);
  // a group's bucket is shared by its members; `0` makes one without a limit
  for name in groups.ordered().into_iter().chain([DEFAULT_GROUP]) {
    if let Some(limit) = groups.upload(name).and_then(|upload| upload.speed_limit) {
      buckets.insert(name.to_string(), bucket(Some(limit)));
    }
  }
  buckets
    .entry(DEFAULT_GROUP.to_string())
    .or_insert_with(|| bucket(None));
  buckets
}

/// What made the buckets: the groups and the global speed in force.
fn made_from(options: &Options, alternative: bool) -> (Groups, Option<u32>) {
  (
    options.transfers.groups.clone(),
    options.transfers.upload_speed_limit(alternative),
  )
}

impl Governor {
  /// The buckets for `options`, with the normal speed limits in force, asking `global` after
  /// each group's bucket.
  pub fn new(options: &Options, global: Arc<TokenBucket>) -> Self {
    Self {
      buckets: RwLock::new(Arc::new(buckets(options, false))),
      global,
      built_from: Mutex::new(made_from(options, false)),
      alternative: AtomicBool::new(false),
    }
  }

  /// Makes every bucket again, full, when a group or the global speed changed.
  pub fn configure(&self, options: &Options) {
    let mut built_from = lock(&self.built_from);
    let alternative = self.alternative.load(Ordering::Acquire);
    let wanted = made_from(options, alternative);
    if *built_from == wanted {
      return;
    }
    *self.buckets.write().unwrap_or_else(PoisonError::into_inner) =
      Arc::new(buckets(options, alternative));
    *built_from = wanted;
    debug!("upload speed limits of the groups made again");
  }

  /// Puts the alternative speed limits in force, or the normal ones, for `options`.
  pub fn switch(&self, alternative: bool, options: &Options) {
    self.alternative.store(alternative, Ordering::Release);
    self.configure(options);
  }

  /// The bucket of `group`, or the default group's for a group without one.
  pub fn bucket(&self, group: &str) -> Arc<TokenBucket> {
    let buckets = self
      .buckets
      .read()
      .unwrap_or_else(PoisonError::into_inner)
      .clone();
    buckets
      .get(group)
      .or_else(|| buckets.get(DEFAULT_GROUP))
      .expect("the default group always has a bucket")
      .clone()
  }

  /// The pace of one upload whose user is in the group `group` gives at each chunk.
  pub fn pace(self: &Arc<Self>, group: CurrentGroup) -> Pace {
    Pace {
      governor: self.clone(),
      group,
      last: Mutex::new(None),
    }
  }
}

/// The pace of one upload: each chunk asks the bucket of its user's group, then the global one.
pub struct Pace {
  governor: Arc<Governor>,
  group: CurrentGroup,
  /// the group bucket the last chunk was granted bytes by, with how many
  last: Mutex<Option<(Arc<TokenBucket>, u32)>>,
}

impl Pace {
  /// How many of the `want` bytes the next chunk may move, waiting until some may.
  pub async fn chunk(&self, want: u32, cancel: &CancellationToken) -> u32 {
    let bucket = self.governor.bucket(&(self.group)());
    // the client sees a cancel and ends the upload
    let Some(granted) = bucket.get(want, cancel).await else {
      return 0;
    };
    *lock(&self.last) = Some((bucket, granted));
    self.governor.global.get(granted, cancel).await.unwrap_or(0)
  }

  /// A chunk was granted `granted` bytes and moved `moved` of them.
  fn report(&self, granted: u32, moved: u32) {
    let taken = lock(&self.last).take();
    if let Some((bucket, group_granted)) = taken {
      bucket.give_back(group_granted.saturating_sub(moved));
    }
    self
      .governor
      .global
      .give_back(granted.saturating_sub(moved));
  }

  /// The governor and the reporter the client calls at each chunk.
  pub fn callbacks(self) -> (ChunkGovernor, Reporter) {
    let pace = Arc::new(self);
    let reporting = pace.clone();
    let chunk: ChunkGovernor = Arc::new(move |_, want, cancel| {
      let pace = pace.clone();
      Box::pin(async move { pace.chunk(want, &cancel).await })
    });
    let reporter: Reporter = Arc::new(move |_, _, granted, moved| reporting.report(granted, moved));
    (chunk, reporter)
  }
}

#[cfg(test)]
mod tests {
  use std::time::Duration;

  use tokio_util::sync::CancellationToken;

  use super::*;
  use crate::config::LEECHERS_GROUP;

  fn options(toml: &str) -> Options {
    toml::from_str(toml).unwrap()
  }

  /// The groups' buckets for `toml`, under a global limit of `global` KiB/s, 0 for none.
  fn limited(toml: &str, global: u32) -> Governor {
    Governor::new(&options(toml), Arc::new(TokenBucket::for_speed(global)))
  }

  /// The groups' buckets for `toml`, without a global limit.
  fn grouped(toml: &str) -> Governor {
    limited(toml, 0)
  }

  #[tokio::test(start_paused = true)]
  async fn a_speed_change_rebuckets_a_running_upload_at_its_next_chunk() {
    let governor = Arc::new(grouped(
      "[transfers.groups.default.upload]\nspeed_limit = 1\n",
    ));
    let pace = governor.pace(Arc::new(|| DEFAULT_GROUP.to_string()));
    let never = CancellationToken::new();
    assert_eq!(pace.chunk(50_000, &never).await, 102);
    pace.report(102, 102);
    governor.configure(&options(
      "[transfers.groups.default.upload]\nspeed_limit = 100\n",
    ));
    let before = tokio::time::Instant::now();
    assert_eq!(
      pace.chunk(50_000, &never).await,
      10_240,
      "the new bucket, full"
    );
    assert_eq!(tokio::time::Instant::now(), before, "without a wait");
    tokio::time::advance(Duration::from_millis(100)).await;
    assert_eq!(pace.chunk(50_000, &never).await, 10_240);
  }

  #[tokio::test(start_paused = true)]
  async fn what_a_chunk_did_not_move_goes_back_to_its_group() {
    let governor = Arc::new(grouped(
      "[transfers.groups.leechers.upload]\nspeed_limit = 1\n",
    ));
    let pace = governor.pace(Arc::new(|| LEECHERS_GROUP.to_string()));
    let never = CancellationToken::new();
    assert_eq!(pace.chunk(1000, &never).await, 102);
    // 30 of the 102 granted were written
    pace.report(102, 30);
    let before = tokio::time::Instant::now();
    assert_eq!(pace.chunk(1000, &never).await, 72, "102 - 30 came back");
    assert_eq!(tokio::time::Instant::now(), before);
  }

  #[test]
  fn a_group_without_a_bucket_takes_the_default_ones_and_privileged_the_global_speed() {
    let governor = grouped(
      "[transfers.upload]\nspeed_limit = 7\n[transfers.groups.default.upload]\nspeed_limit = 3\n",
    );
    assert!(Arc::ptr_eq(
      &governor.bucket("blacklisted"),
      &governor.bucket(DEFAULT_GROUP)
    ));
    assert!(!Arc::ptr_eq(
      &governor.bucket(PRIVILEGED_GROUP),
      &governor.bucket(DEFAULT_GROUP)
    ));
  }

  #[tokio::test(start_paused = true)]
  async fn the_slower_of_the_group_and_the_global_limit_governs_each_interval() {
    // a group under the global limit, then the global limit under the group
    for (group, global) in [(1, 100), (100, 1)] {
      let governor = Arc::new(limited(
        &format!("[transfers.groups.default.upload]\nspeed_limit = {group}\n"),
        global,
      ));
      let pace = governor.pace(Arc::new(|| DEFAULT_GROUP.to_string()));
      let never = CancellationToken::new();
      let before = tokio::time::Instant::now();
      for interval in 0..3 {
        let granted = pace.chunk(50_000, &never).await;
        assert_eq!(granted, 102, "group {group}, global {global}");
        pace.report(granted, granted);
        assert_eq!(
          tokio::time::Instant::now() - before,
          Duration::from_millis(100 * interval)
        );
      }
    }
  }

  #[tokio::test(start_paused = true)]
  async fn what_a_chunk_did_not_move_goes_back_to_the_group_and_the_global_limit() {
    let governor = Arc::new(limited(
      "[transfers.groups.default.upload]\nspeed_limit = 3\n",
      2,
    ));
    let pace = governor.pace(Arc::new(|| DEFAULT_GROUP.to_string()));
    let never = CancellationToken::new();
    let before = tokio::time::Instant::now();
    // the group grants 307 and the global limit 204 of them
    assert_eq!(pace.chunk(1000, &never).await, 204);
    pace.report(204, 100);
    // 207 came back to the group and 104 to the global limit
    assert_eq!(pace.chunk(1000, &never).await, 104);
    assert_eq!(tokio::time::Instant::now(), before);
  }
}
