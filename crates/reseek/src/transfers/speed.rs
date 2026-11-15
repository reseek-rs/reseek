//! Which pair of speed limits is in force, the normal or the alternative one: kept in
//! `transfers.db` so it lasts across restarts, shown in the state document, and in force for the
//! global limits of each direction and the uploads' privileged bucket.

use std::sync::Arc;

use rusqlite::{Connection, OptionalExtension};
use soulseek_client::{Governor, Reporter};
use tracing::{debug, info};

use super::bucket::TokenBucket;
use crate::api::error::ApiError;
use crate::app::AppState;
use crate::config::Options;
use crate::db::{Database, DbError};

/// The global upload and download speed limits: one bucket per direction, which every transfer
/// going that way asks after its own limits.
pub struct Global {
  upload: Arc<TokenBucket>,
  download: Arc<TokenBucket>,
}

/// The upload and download limits `options` set for the pair `alternative` names.
fn pair(options: &Options, alternative: bool) -> (Option<u32>, Option<u32>) {
  let transfers = &options.transfers;
  (
    transfers.upload_speed_limit(alternative),
    transfers.download_speed_limit(alternative),
  )
}

impl Global {
  /// The limits `options` set for the pair `alternative` names, each bucket full.
  pub fn new(options: &Options, alternative: bool) -> Self {
    let (upload, download) = pair(options, alternative);
    // the bucket takes 0 as no limit
    let bucket = |limit: Option<u32>| Arc::new(TokenBucket::for_speed(limit.unwrap_or(0)));
    Self {
      upload: bucket(upload),
      download: bucket(download),
    }
  }

  /// Puts the limits `options` set for the pair `alternative` names in force from the buckets'
  /// next 100 ms on.
  pub fn set(&self, options: &Options, alternative: bool) {
    let (upload, download) = pair(options, alternative);
    self.upload.set_speed(upload.unwrap_or(0));
    self.download.set_speed(download.unwrap_or(0));
    debug!(?upload, ?download, "global speed limits set");
  }

  /// The bucket every upload asks after its group's.
  pub fn upload(&self) -> Arc<TokenBucket> {
    self.upload.clone()
  }

  /// The governor and the reporter of one download: each chunk asks the download bucket, and
  /// what it granted and the chunk did not move goes back to it.
  pub fn download_pace(&self) -> (Governor, Reporter) {
    let bucket = self.download.clone();
    let governor: Governor = Arc::new(move |_, want, cancel| {
      let bucket = bucket.clone();
      Box::pin(async move {
        // the client sees the cancel and ends the download
        bucket.get(want, &cancel).await.unwrap_or(0)
      })
    });
    let bucket = self.download.clone();
    let reporter: Reporter = Arc::new(move |_, _, granted, moved| {
      bucket.give_back(granted.saturating_sub(moved));
    });
    (governor, reporter)
  }
}

fn read(conn: &Connection) -> rusqlite::Result<bool> {
  let stored = conn
    .query_row(
      "SELECT alternative FROM speed_limits WHERE id = 1",
      [],
      |row| row.get(0),
    )
    .optional()?;
  // there is no row until the first switch, and until then the normal limits are in force
  Ok(stored.unwrap_or(false))
}

fn write(conn: &Connection, alternative: bool) -> rusqlite::Result<()> {
  conn.execute(
    "INSERT INTO speed_limits (id, alternative) VALUES (1, ?1) \
     ON CONFLICT (id) DO UPDATE SET alternative = excluded.alternative",
    [alternative],
  )?;
  Ok(())
}

/// Whether the alternative speed limits were in force when last switched.
pub async fn stored(db: &Database) -> Result<bool, DbError> {
  db.read(read).await
}

/// Puts the alternative speed limits in force, or the normal ones, `by` naming who asked: the
/// global limits first, then kept, shown in the state and applied to the uploads' buckets. A
/// database that refuses leaves the pair in force as it was.
pub async fn switch(app: &AppState, alternative: bool, by: &str) -> Result<(), ApiError> {
  let network = &app.network;
  let _reconfiguring = network.reconfiguring().lock().await;
  let was = app.state.current().alternative_speed_limits;
  let options = app.config.current();
  network.speeds().set(&options.values, alternative);
  let kept = app
    .transfers
    .write(move |conn| write(conn, alternative))
    .await;
  if let Err(e) = kept {
    network.speeds().set(&options.values, was);
    return Err(ApiError::internal(e));
  }
  app
    .state
    .update(|state| state.alternative_speed_limits = alternative);
  app.uploads.switch_speed_limits(alternative);
  app.users.speed_limits_switched().await;
  info!(by, alternative, "speed limits switched");
  Ok(())
}

#[cfg(test)]
mod tests {
  use serde_json::json;

  use crate::testing::{TestApp, app, grants, request};

  /// What the global upload and download buckets grant in 100 ms: 6553 bytes for 64 KiB/s, 1638
  /// for 16, 819 for 8, and `u32::MAX` for no limit.
  async fn limits(app: &TestApp) -> (u32, u32) {
    let speeds = app.state.network.speeds();
    (grants(&speeds.upload).await, grants(&speeds.download).await)
  }

  const NONE: u32 = u32::MAX;

  #[tokio::test]
  async fn the_switch_reaches_the_global_limits_and_lasts_across_a_restart() {
    let app = app(
      "[web.authentication]\ntrusted_networks = [\"127.0.0.0/8\"]\n\
       [transfers.upload]\nspeed_limit = 64\nalt_speed_limit = 16\n\
       [transfers.download]\nalt_speed_limit = 8\n",
      &[],
    )
    .await;
    assert_eq!(limits(&app).await, (6553, NONE));
    let on = json!({ "alternative": true });
    let answer = app
      .send(request("PUT", "/api/v0/transfers/speed-limits", Some(on)))
      .await;
    assert_eq!(answer.status, 204, "{:?}", answer.body);
    assert!(app.state.state.current().alternative_speed_limits);
    assert_eq!(limits(&app).await, (1638, 819));

    let app = app.restarted(&[]).await;
    assert!(app.state.state.current().alternative_speed_limits);
    assert_eq!(limits(&app).await, (1638, 819));
    let off = json!({ "alternative": false });
    let answer = app
      .send(request("PUT", "/api/v0/transfers/speed-limits", Some(off)))
      .await;
    assert_eq!(answer.status, 204);
    assert_eq!(limits(&app).await, (6553, NONE));
  }

  #[tokio::test]
  async fn a_privileged_users_speed_follows_the_switch() {
    let app = app(
      "[web.authentication]\ntrusted_networks = [\"127.0.0.0/8\"]\n\
       [transfers.upload]\nspeed_limit = 64\nalt_speed_limit = 16\n",
      &[],
    )
    .await;
    let now = crate::db::timestamp(chrono::Utc::now());
    app
      .state
      .users
      .database()
      .write(move |conn| {
        conn.execute(
          "INSERT INTO users (username, first_seen_at, last_seen_at, is_privileged) \
           VALUES ('alice', ?1, ?1, 1)",
          [now],
        )
      })
      .await
      .unwrap();
    let speed = async || {
      let (_, summary) = app.call("GET", "/api/v0/users/alice/summary", None).await;
      summary["uploads"]["speedLimit"].clone()
    };
    assert_eq!(speed().await, 64);
    let mut stream = app
      .stream_from(
        "127.0.0.1:1",
        request("GET", "/api/v0/live?topics=users", None),
      )
      .await;
    stream.until("hello").await;
    let on = json!({ "alternative": true });
    let answer = app
      .send(request("PUT", "/api/v0/transfers/speed-limits", Some(on)))
      .await;
    assert_eq!(answer.status, 204);
    let pushed = stream.until("users.updated").await.pop().unwrap();
    assert_eq!(
      (
        &pushed.data["username"],
        &pushed.data["uploads"]["speedLimit"]
      ),
      (&json!("alice"), &json!(16))
    );
    assert_eq!(speed().await, 16, "the summary read before is not kept");
  }

  #[tokio::test(start_paused = true)]
  async fn a_download_takes_what_the_global_limit_grants_and_gives_back_what_it_did_not_move() {
    use soulseek_client::{Transfer, TransferState};
    use soulseek_proto::TransferDirection;
    use tokio::time::Instant;
    use tokio_util::sync::CancellationToken;

    let options: crate::config::Options =
      toml::from_str("[transfers.download]\nspeed_limit = 1\n").unwrap();
    let (chunk, report) = super::Global::new(&options, false).download_pace();
    let transfer = Transfer {
      direction: TransferDirection::Download,
      username: "alice".into(),
      filename: "a.flac".into(),
      token: 1,
      remote_token: None,
      state: TransferState::InProgress,
      size: 1_000_000,
      start_offset: 0,
      bytes_transferred: 0,
      bytes_remaining: 1_000_000,
      percent_complete: 0.0,
      average_speed: 0.0,
      start_time: None,
      end_time: None,
      elapsed_time: None,
      remaining_time: None,
      ip_endpoint: None,
      error: None,
    };
    let never = CancellationToken::new();
    let before = Instant::now();
    assert_eq!(chunk(&transfer, 1000, never.clone()).await, 102, "1 KiB/s");
    // 50 of them read
    report(&transfer, 1000, 102, 50);
    assert_eq!(
      chunk(&transfer, 1000, never.clone()).await,
      52,
      "what came back"
    );
    assert_eq!(Instant::now(), before);
    report(&transfer, 1000, 52, 52);
    assert_eq!(chunk(&transfer, 1000, never).await, 102);
    assert_eq!(
      Instant::now() - before,
      std::time::Duration::from_millis(100)
    );
  }

  #[tokio::test]
  async fn a_switch_that_cannot_be_kept_leaves_the_limits_as_they_were() {
    let app = app(
      "[web.authentication]\ntrusted_networks = [\"127.0.0.0/8\"]\n\
       [transfers.upload]\nspeed_limit = 64\nalt_speed_limit = 16\n\
       [transfers.download]\nalt_speed_limit = 8\n",
      &[],
    )
    .await;
    app
      .state
      .transfers
      .write(|conn| conn.execute("DROP TABLE speed_limits", []))
      .await
      .unwrap();
    let on = json!({ "alternative": true });
    let answer = app
      .send(request("PUT", "/api/v0/transfers/speed-limits", Some(on)))
      .await;
    assert_eq!(answer.status, 500, "{:?}", answer.body);
    assert!(!app.state.state.current().alternative_speed_limits);
    assert_eq!(limits(&app).await, (6553, NONE));
  }
}
