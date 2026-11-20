#![no_main]

use std::time::{Duration, Instant};

use std::num::NonZeroUsize;

use choon::{Coverage, Error, ReadingThresholds, Settings};
use libfuzzer_sys::fuzz_target;

// a downloaded file is whatever a peer sent; the analyzer must read any bytes to a verdict or
// refuse them, and never panic
fuzz_target!(|data: &[u8]| {
  let settings = Settings {
    coverage: Coverage::Sampled {
      over_seconds: 900.0,
      windows: NonZeroUsize::new(240).expect("240 is not zero"),
    },
    deadline: Some(Instant::now() + Duration::from_secs(10)),
    ..Settings::default()
  };
  match choon::measure_bytes(data.to_vec(), &settings) {
    Ok(measured) => {
      measured.read(&ReadingThresholds::default());
    }
    Err(Error::Panicked) => panic!("the analyzer panicked"),
    Err(Error::Canceled) => unreachable!("nothing cancels the analysis"),
    Err(Error::Open(_) | Error::OutOfTime | Error::NotAudio(_) | Error::Unsupported(_)) => {}
  }
});
