//! What the transfer suites share: files to send, and a client that queues every upload a peer
//! asks for and hands the request to the test.

use soulseek_client::soulseek_proto;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};

use soulseek_client::config::ConfigBuilder;
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
