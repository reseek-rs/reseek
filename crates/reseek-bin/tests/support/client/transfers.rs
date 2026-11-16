//! What the transfer suites share: files to send, a client that queues every upload a peer asks
//! for and hands the request to the test, and the states a transfer goes through.

use soulseek_client::soulseek_proto;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};

use soulseek_client::config::ConfigBuilder;
use soulseek_client::transfer::{
  self, Direction as TransferDirection, Outcome as TransferOutcome, State as TransferState,
  Transfer,
};
use soulseek_client::{Event, Events};
use soulseek_proto::RemoteFilename;
use tokio::sync::mpsc;

use super::{PATIENCE, Queueing};

static DIRECTORIES: AtomicU32 = AtomicU32::new(0);

/// an empty directory of its own under the target directory
pub fn scratch() -> PathBuf {
  let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
    "transfers-{}-{}",
    std::process::id(),
    DIRECTORIES.fetch_add(1, Ordering::Relaxed)
  ));
  // a directory left by an earlier run of the same process id goes first
  let _ = std::fs::remove_dir_all(&dir);
  std::fs::create_dir_all(&dir).unwrap();
  dir
}

/// `len` bytes that differ from one offset to the next, so a byte out of place shows
pub fn content(len: usize) -> Vec<u8> {
  (0..len).map(|n| (n * 7 % 251) as u8).collect()
}

/// a file of [`content`] in `dir`
pub fn file(dir: &std::path::Path, name: &str, len: usize) -> PathBuf {
  let path = dir.join(name);
  std::fs::write(&path, content(len)).unwrap();
  path
}

/// A peer's request to queue an upload, as the client's handler saw it
pub type Asked = mpsc::UnboundedReceiver<(String, RemoteFilename)>;

/// The handler of a client that queues every upload a peer asks for and hands the request to the
/// returned receiver, which is where a test starts the upload
pub fn queueing() -> (Queueing, Asked) {
  let (asked, requests) = mpsc::unbounded_channel();
  (Queueing { asked }, requests)
}

/// `options` with a handler that queues every upload as [`queueing`] says
pub fn queueing_with(options: ConfigBuilder) -> (ConfigBuilder, Asked) {
  let (handler, requests) = queueing();
  (options.handler(handler), requests)
}

/// the next upload a peer asked to queue
pub async fn asked(requests: &mut Asked) -> (String, RemoteFilename) {
  tokio::time::timeout(PATIENCE, requests.recv())
    .await
    .expect("the peer asks for the file")
    .unwrap()
}

/// Every state the next transfer going `direction` reported, until it ended, and its last
/// snapshot
pub async fn states(
  events: &mut Events,
  direction: TransferDirection,
) -> (Vec<TransferState>, Transfer) {
  let mut states = Vec::new();
  let mut token = None;
  tokio::time::timeout(PATIENCE * 3, async {
    loop {
      let event = events.next().await.expect("the client is alive");
      let Ok(Event::Transfer(transfer::Event::StateChanged { transfer, .. })) = event else {
        continue;
      };
      if transfer.direction != direction || token.is_some_and(|token| token != transfer.token) {
        continue;
      }
      token = Some(transfer.token);
      states.push(transfer.state);
      if transfer.state.is_terminal() {
        return (states, *transfer);
      }
    }
  })
  .await
  .expect("the transfer ends")
}

/// waits until the progress of the transfer `changes` follows reaches `bytes`
pub async fn reached(changes: &mut transfer::Changes, bytes: u64) {
  tokio::time::timeout(PATIENCE, async {
    while let Some(transfer) = changes.next().await {
      if transfer.bytes_transferred >= bytes {
        return;
      }
    }
    panic!("the transfer ended before {bytes} bytes");
  })
  .await
  .expect("the transfer reaches the bytes");
}

/// the states a download goes through when it succeeds
pub fn download() -> Vec<TransferState> {
  vec![
    TransferState::QueuedLocally,
    TransferState::Requested,
    TransferState::QueuedRemotely,
    TransferState::Initializing,
    TransferState::InProgress,
    TransferState::Completed(TransferOutcome::Succeeded),
  ]
}

/// the states an upload goes through when it succeeds
pub fn upload() -> Vec<TransferState> {
  vec![
    TransferState::QueuedLocally,
    TransferState::Requested,
    TransferState::Initializing,
    TransferState::InProgress,
    TransferState::Completed(TransferOutcome::Succeeded),
  ]
}
