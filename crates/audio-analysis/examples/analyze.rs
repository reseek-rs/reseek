//! Prints a reading of each audio file named, one JSON object a line, for tools that score the
//! analysis against files whose history is known.
//!
//! ```text
//! analyze [--profile FILE]... [--max-full-minutes N --sampled-windows N] FILE...
//! analyze --defaults
//! ```
//!
//! A profile is a set of thresholds as JSON, `{"measure": {...}, "reading": {...}}`, each key
//! optional and over the defaults `--defaults` prints. Every file is read with the defaults and
//! then with each profile, named after its file; profiles whose measure thresholds are the same
//! share one measuring, so a sweep of the reading thresholds decodes each file once. Every file
//! is read in full unless `--max-full-minutes` says how long a file read in full may be.
//!
//! A file that cannot be opened is named on stderr and the others are still read; the exit
//! status is then 1. A file that opens but does not decode is an inconclusive reading.

use std::borrow::Cow;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{Duration, Instant};

use audio_analysis::decode::Claimed;
use audio_analysis::engine::{self, Measured, Settings};
use audio_analysis::{MeasureThresholds, Reading, ReadingThresholds, reading};
use serde::{Deserialize, Serialize};

const USAGE: &str =
  "usage: analyze [--profile FILE]... [--max-full-minutes N --sampled-windows N] FILE...
       analyze --defaults
exits 1 when a FILE cannot be opened, after reading the others, and 2 on a usage error";

/// What a file is measured and read with.
#[derive(Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Thresholds {
  measure: MeasureThresholds,
  reading: ReadingThresholds,
}

/// What is printed for a file.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Analyzed<'a> {
  file: Cow<'a, str>,
  claimed: Option<&'a Claimed>,
  duration_seconds: f64,
  bitrate_kbps: Option<u32>,
  sampled: bool,
  windows: usize,
  readings: Vec<Read>,
}

/// One profile's reading of a file, with how long it took.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Read {
  profile: String,
  /// how long decoding and measuring the file took, shared by the profiles that measure alike
  measure_seconds: f64,
  read_seconds: f64,
  #[serde(flatten)]
  reading: Reading,
}

/// How files are read: in full up to a length, sampled past it.
struct Reach {
  max_full_seconds: f64,
  sampled_windows: usize,
}

fn main() -> ExitCode {
  match run() {
    Ok(true) => ExitCode::SUCCESS,
    Ok(false) => ExitCode::from(1),
    Err(message) => {
      eprintln!("analyze: {message}");
      ExitCode::from(2)
    }
  }
}

/// Reads every file named; false when one could not be opened.
fn run() -> Result<bool, String> {
  let mut profiles = vec![("default".to_string(), Thresholds::default())];
  let mut files = Vec::new();
  let mut max_full_minutes: Option<f64> = None;
  let mut sampled_windows: Option<usize> = None;
  let mut args = std::env::args().skip(1);
  while let Some(arg) = args.next() {
    let mut value = |flag: &str| args.next().ok_or(format!("{flag} needs a value\n{USAGE}"));
    match arg.as_str() {
      "--defaults" => {
        println!("{}", json(&Thresholds::default())?);
        return Ok(true);
      }
      "--profile" => {
        let path = PathBuf::from(value("--profile")?);
        profiles.push(profile(&path)?);
      }
      "--max-full-minutes" => {
        let minutes = value("--max-full-minutes")?;
        let minutes = minutes
          .parse()
          .map_err(|e| format!("--max-full-minutes {minutes}: {e}"))?;
        max_full_minutes = Some(minutes);
      }
      "--sampled-windows" => {
        let count = value("--sampled-windows")?;
        let count = count
          .parse()
          .map_err(|e| format!("--sampled-windows {count}: {e}"))?;
        sampled_windows = Some(count);
      }
      "-h" | "--help" => {
        println!("{USAGE}");
        return Ok(true);
      }
      flag if flag.starts_with("--") => return Err(format!("unknown option {flag}\n{USAGE}")),
      _ => files.push(PathBuf::from(arg)),
    }
  }
  if files.is_empty() {
    return Err(USAGE.to_string());
  }
  let reach = match (max_full_minutes, sampled_windows) {
    (None, None) => Reach {
      max_full_seconds: f64::INFINITY,
      sampled_windows: 0,
    },
    (Some(minutes), Some(count)) => Reach {
      max_full_seconds: minutes * 60.0,
      sampled_windows: count,
    },
    _ => {
      return Err(format!(
        "--max-full-minutes and --sampled-windows go together\n{USAGE}"
      ));
    }
  };
  let mut opened = true;
  for file in &files {
    if let Err(e) = openable(file) {
      eprintln!("analyze: {}: {e}", file.display());
      opened = false;
      continue;
    }
    println!("{}", analyzed(file, &profiles, &reach));
  }
  Ok(opened)
}

/// Whether the file at `path` can be opened for reading.
fn openable(path: &Path) -> std::io::Result<()> {
  let file = std::fs::File::open(path)?;
  if file.metadata()?.is_dir() {
    return Err(std::io::Error::other("it is a directory"));
  }
  Ok(())
}

/// The profile in the file at `path`, named after the file.
fn profile(path: &Path) -> Result<(String, Thresholds), String> {
  let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
  let thresholds = serde_json::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))?;
  let name = path.file_stem().map_or_else(
    || path.display().to_string(),
    |stem| stem.to_string_lossy().into_owned(),
  );
  Ok((name, thresholds))
}

/// The readings of the file at `path` under each of `profiles`, as a line of JSON.
fn analyzed(path: &Path, profiles: &[(String, Thresholds)], reach: &Reach) -> String {
  let mut measured: Vec<(&MeasureThresholds, Measured, f64)> = Vec::new();
  let mut readings = Vec::new();
  for (name, thresholds) in profiles {
    let at = match measured
      .iter()
      .position(|(alike, ..)| **alike == thresholds.measure)
    {
      Some(at) => at,
      None => {
        let settings = Settings {
          max_full_seconds: reach.max_full_seconds,
          sampled_windows: reach.sampled_windows,
          // a day, which no single file takes: the reading covers the whole file
          deadline: Instant::now() + Duration::from_secs(24 * 60 * 60),
          progress: None,
          canceled: None,
          thresholds: thresholds.measure.clone(),
        };
        let started = Instant::now();
        let Ok(done) = engine::run(path, &settings) else {
          unreachable!("nothing cancels the analysis")
        };
        measured.push((&thresholds.measure, done, started.elapsed().as_secs_f64()));
        measured.len() - 1
      }
    };
    let (_, done, measure_seconds) = &measured[at];
    let started = Instant::now();
    let reading = reading::read(done, &thresholds.reading);
    readings.push(Read {
      profile: name.clone(),
      measure_seconds: *measure_seconds,
      read_seconds: started.elapsed().as_secs_f64(),
      reading,
    });
  }
  // every profile reads the same windows of the file; what differs is what is made of them
  let (_, first, _) = &measured[0];
  let analyzed = Analyzed {
    file: path.to_string_lossy(),
    claimed: first.claimed.as_ref(),
    duration_seconds: first.duration,
    bitrate_kbps: first.bitrate_kbps,
    sampled: first.sampled,
    windows: first.windows.len(),
    readings,
  };
  // the readings are numbers, strings and lists of them, which always serialize
  serde_json::to_string(&analyzed).expect("a reading serializes")
}

fn json(value: &impl Serialize) -> Result<String, String> {
  serde_json::to_string(value).map_err(|e| e.to_string())
}
