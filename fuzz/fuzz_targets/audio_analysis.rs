#![no_main]

use std::time::{Duration, Instant};

use earwitness::engine::{self, Settings};
use earwitness::{MeasureThresholds, ReadingThresholds, reading};
use libfuzzer_sys::fuzz_target;

// a downloaded file is whatever a peer sent; the analyzer must read any bytes to a verdict
fuzz_target!(|data: &[u8]| {
  let settings = Settings {
    max_full_seconds: 900.0,
    sampled_windows: 240,
    deadline: Instant::now() + Duration::from_secs(10),
    progress: None,
    canceled: None,
    thresholds: MeasureThresholds::default(),
  };
  let Ok(measured) = engine::read_bytes(data.to_vec(), &settings) else {
    unreachable!("nothing cancels the analysis")
  };
  reading::read(&measured, &ReadingThresholds::default());
});
