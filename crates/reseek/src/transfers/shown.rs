//! The speed a running transfer shows: a moving average of its speed over each second, which
//! takes in a fifth of each new second's figure, and the overall average once every byte moved
//! or the transfer is over. Nothing shows for its first second.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

use reseek_shares::lock;
use soulseek_client::transfer::{Direction as TransferDirection, Transfer};
use tokio::time::Instant;

/// How long each figure of the moving average is measured over.
const SAMPLE: Duration = Duration::from_secs(1);

/// How much of each new figure the moving average takes in.
const SMOOTHING: f64 = 0.2;

/// The speed each running transfer shows, by its direction and the client's token for it.
#[derive(Default)]
pub struct ShownSpeeds(Mutex<HashMap<(TransferDirection, u32), Shown>>);

impl ShownSpeeds {
  /// The speed `transfer` shows now that the client reported its progress.
  pub fn progressed(&self, transfer: &Transfer) -> f64 {
    let progress = Progress::of(transfer);
    lock(&self.0)
      .entry((transfer.direction, transfer.token))
      .or_default()
      .take(&progress, Instant::now())
  }

  /// The speed `transfer` showed at its last progress, 0 before any.
  pub fn of(&self, transfer: &Transfer) -> f64 {
    lock(&self.0)
      .get(&(transfer.direction, transfer.token))
      .map_or(0.0, |shown| shown.speed)
  }

  /// Forgets `transfer`, which ended.
  pub fn ended(&self, transfer: &Transfer) {
    lock(&self.0).remove(&(transfer.direction, transfer.token));
  }
}

/// What a progress report of a transfer says about its speed.
struct Progress {
  /// how long bytes have been moving, none before they started
  running_for: Option<Duration>,
  start_offset: u64,
  bytes: u64,
  size: u64,
  /// the client's overall average
  overall: f64,
}

impl Progress {
  fn of(transfer: &Transfer) -> Self {
    Self {
      running_for: transfer.start_time.map(|started| {
        // a clock set back since the start counts as no time
        SystemTime::now()
          .duration_since(started)
          .unwrap_or_default()
      }),
      start_offset: transfer.start_offset,
      bytes: transfer.bytes_transferred,
      size: transfer.size,
      overall: transfer.average_speed,
    }
  }
}

/// The speed one transfer shows, and the figures it is worked out from.
#[derive(Default)]
struct Shown {
  /// when the last figure was taken, and the position then; the start until the first one
  sample: Option<(Instant, u64)>,
  /// the moving average, from the first figure on
  average: Option<f64>,
  speed: f64,
}

impl Shown {
  /// Takes the progress `progress` reported at `now`, and returns the speed shown with it.
  fn take(&mut self, progress: &Progress, now: Instant) -> f64 {
    let Some(running_for) = progress.running_for else {
      return self.speed;
    };
    if progress.bytes >= progress.size {
      self.speed = progress.overall;
      return self.speed;
    }
    let (at, at_bytes) = *self.sample.get_or_insert_with(|| {
      let started = now.checked_sub(running_for).unwrap_or(now);
      (started, progress.start_offset)
    });
    let since = now.saturating_duration_since(at);
    if since >= SAMPLE {
      let figure = progress.bytes.saturating_sub(at_bytes) as f64 / since.as_secs_f64();
      let average = match self.average {
        Some(average) => average + SMOOTHING * (figure - average),
        None => figure,
      };
      self.average = Some(average);
      self.speed = average;
      self.sample = Some((now, progress.bytes));
    }
    self.speed
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn progress(running_for: Option<Duration>, bytes: u64) -> Progress {
    Progress {
      running_for,
      start_offset: 0,
      bytes,
      size: 10_000,
      overall: 777.0,
    }
  }

  #[tokio::test]
  async fn a_report_gives_the_speed_shown_until_the_transfer_ends() {
    let shown = ShownSpeeds::default();
    let mut transfer = crate::testing::soulseek::transfer_snapshot();
    transfer.state = soulseek_client::transfer::State::InProgress;
    (transfer.size, transfer.start_offset) = (10_000, 0);
    transfer.start_time = Some(SystemTime::now() - Duration::from_secs(2));
    transfer.bytes_transferred = 3000;
    transfer.average_speed = 777.0;
    let speed = shown.progressed(&transfer);
    // the first figure, 3000 bytes over the two seconds since the start, on a running clock
    assert!((1400.0..=1500.0).contains(&speed), "{speed}");
    assert_eq!(shown.of(&transfer), speed);
    shown.ended(&transfer);
    assert_eq!(shown.of(&transfer), 0.0, "forgotten once it ended");
  }

  #[tokio::test(start_paused = true)]
  async fn nothing_for_the_first_second_then_a_moving_average_then_the_overall_one() {
    let mut shown = Shown::default();
    let started = Instant::now();
    let since = || Some(Instant::now() - started);
    assert_eq!(shown.take(&progress(None, 0), Instant::now()), 0.0);
    tokio::time::advance(Duration::from_millis(500)).await;
    assert_eq!(shown.take(&progress(since(), 500), Instant::now()), 0.0);
    tokio::time::advance(Duration::from_millis(500)).await;
    assert_eq!(shown.take(&progress(since(), 1000), Instant::now()), 1000.0);
    tokio::time::advance(Duration::from_millis(400)).await;
    assert_eq!(
      shown.take(&progress(since(), 1900), Instant::now()),
      1000.0,
      "no new figure within a second of the last"
    );
    tokio::time::advance(Duration::from_millis(600)).await;
    // 1000 + 0.2 * (2000 - 1000)
    assert_eq!(shown.take(&progress(since(), 3000), Instant::now()), 1200.0);
    assert_eq!(
      shown.take(&progress(since(), 10_000), Instant::now()),
      777.0,
      "every byte moved"
    );
  }
}
