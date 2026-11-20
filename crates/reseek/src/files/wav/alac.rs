//! An ALAC file open for decoding a packet at a time: the container is found by probing, each
//! packet's samples come with the sample frames the container says it covers, and reading can
//! move to the packet that holds a frame.

use std::ops::Range;

use symphonia::core::codecs::audio::well_known::CODEC_ID_ALAC;
use symphonia::core::codecs::audio::{AudioCodecParameters, AudioDecoder, AudioDecoderOptions};
use symphonia::core::errors::{Error, SeekErrorKind};
use symphonia::core::formats::probe::Hint;
use symphonia::core::formats::{FormatOptions, FormatReader, SeekMode, SeekTo, TrackType};
use symphonia::core::io::{MediaSource, MediaSourceStream};
use symphonia::core::meta::MetadataOptions;
use symphonia::core::units::{TimeBase, Timestamp};
use symphonia_common::apple::audio::alac::MagicCookie;
use tracing::{debug, trace};

/// The highest sample rate served. It also keeps a packet's place in sample frames within
/// range, which `frames_of` relies on.
const MAX_RATE: u32 = 768_000;

/// How many undecodable packets are skipped before reading stops.
const MAX_SKIPPED: u32 = 1000;

/// What an ALAC file states of its audio.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Stated {
  /// samples per second
  pub sample_rate: u32,
  /// bits per sample, when the file's magic cookie reads
  pub bit_depth: Option<u32>,
  pub channels: u32,
  /// the length in sample frames, when the container states one
  pub frames: Option<u64>,
}

/// An ALAC file open for decoding.
pub(super) struct Alac {
  format: Box<dyn FormatReader>,
  decoder: Box<dyn AudioDecoder>,
  track: u32,
  /// what the track's timestamps count, when the container says
  time_base: Option<TimeBase>,
  pub stated: Stated,
  /// packets the decoder could not read and skipped
  skipped: u32,
  /// why the last packet skipped could not be read
  pub last_skipped: Option<String>,
  /// why decoding stopped before the end of the file, when it did
  pub stopped: Option<String>,
  /// the sample frame reading is to go on from: the track's packets that end before it are
  /// passed over without being decoded
  skip_until: Option<u64>,
}

impl Alac {
  /// Opens the ALAC audio in `source`, with `hint` naming what the bytes might be, or says why
  /// it is not audio this reads.
  pub fn open(source: Box<dyn MediaSource>, hint: &Hint) -> Result<Self, String> {
    let stream = MediaSourceStream::new(source, Default::default());
    let format = symphonia::default::get_probe()
      .probe(
        hint,
        stream,
        FormatOptions::default(),
        MetadataOptions::default(),
      )
      .map_err(|e| format!("the container was not recognized: {e}"))?;
    let track = format
      .default_track(TrackType::Audio)
      .ok_or("the file holds no audio track")?;
    let params = track
      .codec_params
      .as_ref()
      .and_then(|p| p.audio())
      .ok_or("the audio track has no codec parameters")?
      .clone();
    if params.codec != CODEC_ID_ALAC {
      return Err("the audio is not ALAC".to_string());
    }
    let track_id = track.id;
    let time_base = track.time_base;
    let frames = track.num_frames;
    let decoder = symphonia::default::get_codecs()
      .make_audio_decoder(&params, &AudioDecoderOptions::default())
      .map_err(|e| format!("the decoder could not start: {e}"))?;
    let sample_rate = params
      .sample_rate
      .filter(|rate| *rate > 0)
      .ok_or("the sample rate is not stated")?;
    if sample_rate > MAX_RATE {
      return Err(format!(
        "the sample rate of {sample_rate} Hz is above the {MAX_RATE} Hz that is served"
      ));
    }
    let channels = params.channels.as_ref().map_or(0, |c| c.count());
    if channels == 0 {
      return Err("the number of channels is not stated".to_string());
    }
    debug!(sample_rate, channels, "ALAC opened");
    Ok(Self {
      format,
      decoder,
      track: track_id,
      time_base,
      stated: Stated {
        sample_rate,
        bit_depth: stated_bits(&params),
        channels: u32::try_from(channels).unwrap_or(u32::MAX),
        frames,
      },
      skipped: 0,
      last_skipped: None,
      stopped: None,
      skip_until: None,
    })
  }

  /// Moves to the packet holding sample frame `frame`; false when the container cannot place the
  /// frame among the packets it indexes, which is also so when another track of the file ends
  /// before it, since every track is moved. The container may then not read on.
  pub fn seek_frame(&mut self, frame: u64) -> Result<bool, String> {
    let (numer, denom) = self.ticks();
    let rate = u128::from(self.stated.sample_rate);
    let ticks = u128::from(frame) * denom / (numer * rate);
    let ts = i64::try_from(ticks).map_err(|_| "the position is out of range")?;
    let sought = self.format.seek(
      SeekMode::Accurate,
      SeekTo::Timestamp {
        ts: Timestamp::new(ts),
        track_id: self.track,
      },
    );
    match sought {
      Ok(_) => {
        self.decoder.reset();
        self.skip_until = None;
        Ok(true)
      }
      Err(Error::SeekError(SeekErrorKind::OutOfRange)) => Ok(false),
      Err(e) => Err(format!("seeking failed: {e}")),
    }
  }

  /// Reads on from the packet of the track that holds sample frame `frame`, or the first after
  /// it, passing over the packets before it without decoding them, so getting there takes only
  /// the time the container takes to read them. For a frame a seek cannot place: ALAC decodes
  /// each packet on its own, so the samples from there are the same as when every packet
  /// before was decoded.
  pub fn skip_to_frame(&mut self, frame: u64) {
    self.skip_until = Some(frame);
  }

  /// Decodes the next packet into `samples`, interleaved over every channel with each sample at
  /// the top of its 32 bits, and returns the sample frames the container says it covers, from
  /// its timestamp for its duration, which the samples can fall short of or run past; `None` at
  /// the end of the file or when decoding cannot go on, with the reason in `stopped`.
  pub fn next_interleaved(&mut self, samples: &mut Vec<i32>) -> Option<Range<u64>> {
    let (pts, dur) = self.step()?;
    self.decoder.last_decoded().copy_to_vec_interleaved(samples);
    let frames = self.frames_of(pts, dur);
    if frames.is_none() {
      debug!(pts, dur, "a packet's timestamp is out of range");
      self.stopped = Some("the file's timing is out of range".to_string());
    }
    frames
  }

  /// Whether every sample frame lasts a whole number of the container's timestamp ticks, so a
  /// packet's timestamp and duration place its first and last frame exactly.
  pub fn ticks_fit_frames(&self) -> bool {
    let (numer, denom) = self.ticks();
    denom % (numer * u128::from(self.stated.sample_rate)) == 0
  }

  /// The sample frames a packet at timestamp `pts` lasting `dur` covers; `None` when they are out
  /// of range.
  fn frames_of(&self, pts: i64, dur: u64) -> Option<Range<u64>> {
    let (numer, denom) = self.ticks();
    let rate = u128::from(self.stated.sample_rate);
    // a timestamp and a duration add to below 2^65, a tick's numerator is below 2^32 and the rate
    // at most `MAX_RATE`, below 2^20, so the product fits
    let frame = |ticks: u128| u64::try_from(ticks * numer * rate / denom).ok();
    let pts = u128::try_from(pts).ok()?;
    Some(frame(pts)?..frame(pts + u128::from(dur))?)
  }

  /// A timestamp tick as a fraction of a second; one sample frame when the container gives none.
  fn ticks(&self) -> (u128, u128) {
    self.time_base.map_or_else(
      || (1, u128::from(self.stated.sample_rate)),
      |base| (u128::from(base.numer.get()), u128::from(base.denom.get())),
    )
  }

  /// Decodes the next packet of the track that holds samples, which the decoder then holds, and
  /// returns its timestamp and duration; `None` at the end of the file or when decoding cannot
  /// go on, with the reason in `stopped`.
  fn step(&mut self) -> Option<(i64, u64)> {
    loop {
      let packet = match self.format.next_packet() {
        Ok(Some(packet)) => packet,
        Ok(None) => return None,
        Err(Error::IoError(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
          debug!("audio ends early");
          self.stopped = Some("the file ends before its last packet".to_string());
          return None;
        }
        Err(e) => {
          debug!(error = %e, "audio could not be read on");
          self.stopped = Some(format!("reading stopped: {e}"));
          return None;
        }
      };
      if packet.track_id != self.track {
        continue;
      }
      if let Some(until) = self.skip_until {
        // a packet whose frames are out of range is decoded, which says so
        let before = self
          .frames_of(packet.pts.get(), packet.dur.get())
          .is_some_and(|frames| frames.end <= until);
        if before {
          continue;
        }
        self.skip_until = None;
      }
      let decoded = match self.decoder.decode(&packet) {
        Ok(decoded) => decoded,
        Err(Error::DecodeError(reason)) => {
          trace!(reason, "audio packet skipped");
          if self.skip(reason.to_string()) {
            return None;
          }
          continue;
        }
        Err(Error::IoError(e)) => {
          trace!(error = %e, "audio packet skipped");
          if self.skip(e.to_string()) {
            return None;
          }
          continue;
        }
        Err(e) => {
          debug!(error = %e, "audio could not be decoded on");
          self.stopped = Some(format!("decoding stopped: {e}"));
          return None;
        }
      };
      if decoded.frames() == 0 {
        continue;
      }
      return Some((packet.pts.get(), packet.dur.get()));
    }
  }

  /// Counts a packet skipped because of `reason`; true, with the reason in `stopped`, once so
  /// many were skipped that reading on is not worth it.
  fn skip(&mut self, reason: String) -> bool {
    self.skipped += 1;
    self.last_skipped = Some(reason);
    if self.skipped < MAX_SKIPPED {
      return false;
    }
    debug!(skipped = self.skipped, "too many audio packets skipped");
    self.stopped = Some(format!(
      "reading stopped after {MAX_SKIPPED} packets could not be decoded"
    ));
    true
  }
}

/// The bits per sample `params` state. An MP4 says nothing of them for ALAC, whose magic cookie
/// does; the decoder made from `params` read the same cookie, so a cookie that does not read
/// here never gets this far.
fn stated_bits(params: &AudioCodecParameters) -> Option<u32> {
  params.bits_per_sample.or_else(|| {
    let cookie = params.extra_data.as_deref()?;
    Some(u32::from(MagicCookie::read(cookie).ok()?.bit_depth))
  })
}

#[cfg(test)]
mod tests {
  use choon::fixtures::TONE;

  use super::*;

  fn opened(bytes: Vec<u8>) -> Alac {
    let mut hint = Hint::new();
    hint.with_extension("m4a");
    Alac::open(Box::new(std::io::Cursor::new(bytes)), &hint).unwrap()
  }

  #[test]
  fn a_skip_reads_on_from_the_packet_holding_the_frame() {
    let mut alac = opened(TONE.to_vec());
    alac.skip_to_frame(12_000);
    let mut samples = Vec::new();
    assert_eq!(alac.next_interleaved(&mut samples), Some(8192..12_288));
    assert_eq!(samples.len(), 4096 * 2);
  }

  #[test]
  fn the_bit_depth_is_read_from_the_magic_cookie() {
    assert_eq!(opened(TONE.to_vec()).stated.bit_depth, Some(16));
    // the cookie is the last 24 bytes of the 36-byte `alac` box inside the sample entry; its
    // bit depth is the sixth of them
    let mut bytes = TONE.to_vec();
    let inner = bytes
      .windows(8)
      .position(|w| w == [0, 0, 0, 0x24, b'a', b'l', b'a', b'c'])
      .unwrap();
    let depth = inner + 8 + 4 + 5;
    assert_eq!(bytes[depth], 16);
    bytes[depth] = 24;
    assert_eq!(opened(bytes).stated.bit_depth, Some(24));
  }
}
