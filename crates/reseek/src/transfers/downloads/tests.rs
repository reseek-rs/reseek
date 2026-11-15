//! The download calls of the API against the in-memory daemon, which is not connected: finding
//! a download by its peer and id, canceling and removing one no task runs, a bulk cancel, a
//! finished download's file served from where it was moved, the removals and updates
//! pushed on the `transfers` topic, and a job told of once its last download ended.

use std::task::Poll;

use api_types::transfers::{TransferOutcome, TransferStates, UserResponse};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::testing::{TRUSTED, TestApp, app, request};
use crate::transfers::state::State;
use crate::transfers::{Record, store};

/// Writes `record` into the daemon's `transfers.db`, at the end of the local queue when it has
/// no place, as an enqueue writes it.
async fn insert(app: &TestApp, record: &Record) {
  let mut record = record.clone();
  app
    .state
    .transfers
    .write(move |conn| {
      if record.queue_position.is_none() {
        record.queue_position = Some(store::next_position(conn)?);
      }
      store::insert(conn, &record)
    })
    .await
    .unwrap();
}

async fn read(app: &TestApp, id: Uuid) -> Record {
  app
    .state
    .transfers
    .read(move |conn| store::get(conn, id))
    .await
    .unwrap()
    .unwrap()
}

fn done(username: &str, filename: &str) -> Record {
  let mut record = Record::download(username, filename, 3);
  record.state = State::Completed(TransferOutcome::Succeeded);
  record.ended_at = Some(chrono::Utc::now());
  record
}

#[tokio::test]
async fn a_download_is_found_by_its_peer_and_id_with_its_history() {
  let app = app(TRUSTED, &[]).await;
  let record = Record::download("alice", "@@abcde\\Music\\01.flac", 10);
  insert(&app, &record).await;
  let id = record.id;
  let (status, _) = app
    .call(
      "GET",
      &format!("/api/v0/transfers/downloads/bob/{id}"),
      None,
    )
    .await;
  assert_eq!(status, 404, "the peer is part of the path");
  let uri = format!("/api/v0/transfers/downloads/alice/{id}?includeHistory=true");
  let (status, body) = app.call("GET", &uri, None).await;
  assert_eq!(status, 200);
  assert_eq!(body["stateHistory"][0]["state"], "Queued, Locally");
  assert_eq!(body["localPath"], Value::Null);
  let (status, body) = app
    .call("GET", "/api/v0/transfers/downloads/alice", None)
    .await;
  assert_eq!(status, 200);
  let user: UserResponse = serde_json::from_value(body).unwrap();
  assert_eq!(user.directories[0].directory, "@@abcde\\Music");
  let (status, _) = app
    .call("GET", "/api/v0/transfers/downloads/carol", None)
    .await;
  assert_eq!(status, 404, "a user without downloads");

  // a name no path segment can hold comes in the query
  let dots = Record::download("..", "@@abcde\\Music\\02.flac", 10);
  insert(&app, &dots).await;
  let uri = format!("/api/v0/transfers/downloads/_/{}?username=..", dots.id);
  let (status, body) = app.call("GET", &uri, None).await;
  assert_eq!(status, 200, "{body}");
  assert_eq!(body["username"], "..");
}

#[tokio::test]
async fn canceling_a_download_no_task_runs_ends_its_wait_and_can_remove_it() {
  let app = app(TRUSTED, &[]).await;
  let mut waiting = Record::download("alice", "a.flac", 10);
  waiting.state = State::QueuedLocally {
    next_attempt_at: Some(chrono::Utc::now()),
  };
  insert(&app, &waiting).await;
  let id = waiting.id;
  // a row an earlier build wrote with the place its peer last gave next to the retry time
  app
    .state
    .transfers
    .write(move |conn| {
      conn.execute(
        "UPDATE Transfers SET PlaceInQueue = 4 WHERE Id = ?1",
        [id.to_string()],
      )
    })
    .await
    .unwrap();
  let uri = format!("/api/v0/transfers/downloads/alice/{id}?remove=true");
  let (status, _) = app.call("DELETE", &uri, None).await;
  assert_eq!(status, 204);
  let columns: (Option<u32>, Option<String>) = app
    .state
    .transfers
    .read(move |conn| {
      conn.query_row(
        "SELECT PlaceInQueue, NextAttemptAt FROM Transfers WHERE Id = ?1",
        [id.to_string()],
        |row| Ok((row.get(0)?, row.get(1)?)),
      )
    })
    .await
    .unwrap();
  assert_eq!(
    columns,
    (None, None),
    "the place and the retry time go with the wait"
  );
  let ended = read(&app, id).await;
  assert_eq!(
    ended.state.kind(),
    TransferStates::Completed(TransferOutcome::Cancelled)
  );
  assert_eq!(
    ended.exception.as_deref(),
    Some("The operation was canceled.")
  );
  assert!(ended.ended_at.is_some() && ended.removed);
  assert_eq!(
    (ended.next_attempt_at(), ended.place_in_queue()),
    (None, None)
  );
  let (_, listed) = app.call("GET", "/api/v0/transfers/downloads", None).await;
  assert_eq!(listed, json!([]), "removed downloads are not listed");
  let (_, listed) = app
    .call(
      "GET",
      "/api/v0/transfers/downloads?includeRemoved=true",
      None,
    )
    .await;
  assert_eq!(listed[0]["username"], "alice");
}

#[tokio::test]
async fn a_bulk_cancel_ends_the_running_download_and_fails_the_ended_and_unknown_ones() {
  let app = app(TRUSTED, &[]).await;
  let finished = done("alice", "a.flac");
  let waiting = Record::download("alice", "b.flac", 10);
  insert(&app, &finished).await;
  insert(&app, &waiting).await;
  let request = json!({ "ids": [waiting.id, finished.id, Uuid::new_v4()], "action": "cancel" });
  let (status, result) = app
    .call("POST", "/api/v0/transfers/downloads/bulk", Some(request))
    .await;
  assert_eq!(status, 200);
  assert_eq!(result["ok"], json!([waiting.id]));
  assert_eq!(result["failed"].as_array().unwrap().len(), 2);
}

/// A cancel reads a download no task runs, a task completes the download, and then the cancel
/// writes: the download keeps how it ended and the cancel answers that it had completed.
#[tokio::test]
async fn a_cancel_that_loses_the_race_to_completion_says_the_download_completed() {
  let app = app(TRUSTED, &[]).await;
  let mut finished = done("alice", "a.flac");
  finished.local_path = Some("/music/a.flac".into());
  insert(&app, &finished).await;
  let mut stale = Record::download("alice", "a.flac", 3);
  stale.id = finished.id;
  stale.state = State::InProgress;
  let canceled = app
    .state
    .downloads
    .cancel_unrun(stale, false)
    .await
    .unwrap();
  assert!(!canceled);
  let kept = read(&app, finished.id).await;
  assert_eq!(
    kept.state.kind(),
    TransferStates::Completed(TransferOutcome::Succeeded)
  );
  assert_eq!(kept.local_path.as_deref(), Some("/music/a.flac"));
}

#[tokio::test]
async fn a_finished_downloads_file_is_served_from_where_it_was_moved() {
  let app = app(TRUSTED, &[]).await;
  let downloads = app.state.paths.downloads.clone();
  std::fs::create_dir_all(downloads.join("album")).unwrap();
  let file = downloads.join("album").join("01.flac");
  std::fs::write(&file, "abc").unwrap();
  let mut moved = done("alice", "@@abcde\\album\\01.flac");
  moved.local_path = Some(file.display().to_string());
  let unmoved = done("alice", "@@abcde\\album\\02.flac");
  insert(&app, &moved).await;
  insert(&app, &unmoved).await;
  let content = |user: &str, id: Uuid| format!("/api/v0/transfers/downloads/{user}/{id}/content");
  let answer = app
    .send(request("GET", &content("alice", moved.id), None))
    .await;
  assert_eq!(answer.status, 200);
  assert_eq!(&answer.body[..], b"abc");
  let (status, _) = app.call("GET", &content("bob", moved.id), None).await;
  assert_eq!(status, 404, "the peer is part of the path");
  let (status, _) = app.call("GET", &content("alice", unmoved.id), None).await;
  assert_eq!(status, 404, "a download without a file");
  let token = json!({ "transferId": moved.id });
  let (status, body) = app
    .call("POST", "/api/v0/session/file-token", Some(token))
    .await;
  assert_eq!(status, 200, "{body}");
  let token = body["token"].as_str().unwrap();
  let linked = format!("{}?t={token}", content("alice", moved.id));
  let answer = app
    .send_from("192.0.2.1:40000", request("GET", &linked, None))
    .await;
  assert_eq!(answer.status, 200, "the link's token alone lets it in");
  assert_eq!(&answer.body[..], b"abc");
  std::fs::remove_file(&file).unwrap();
  let (status, _) = app.call("GET", &content("alice", moved.id), None).await;
  assert_eq!(status, 404, "a file that is gone");
}

#[tokio::test]
async fn removals_and_updates_are_pushed_on_the_transfers_topic() {
  let app = app(TRUSTED, &[]).await;
  let finished = done("alice", "a.flac");
  let stuck = Record::download("alice", "b.flac", 10);
  insert(&app, &finished).await;
  insert(&app, &stuck).await;
  let mut stream = app
    .stream_from(
      "127.0.0.1:1",
      request("GET", "/api/v0/live?topics=transfers", None),
    )
    .await;
  assert_eq!(stream.next().await.unwrap().event, "hello");
  let (status, _) = app
    .call("DELETE", "/api/v0/transfers/downloads/all/completed", None)
    .await;
  assert_eq!(status, 204);
  let removed = stream.next().await.unwrap();
  assert_eq!(removed.event, "transfers.removed");
  assert_eq!(
    removed.data,
    json!({ "id": finished.id, "username": "alice", "direction": "Download" })
  );
  let uri = format!("/api/v0/transfers/downloads/alice/{}", stuck.id);
  app.call("DELETE", &uri, None).await;
  let updated = stream.next().await.unwrap();
  assert_eq!(updated.event, "transfers.updated");
  assert_eq!(updated.data["state"], "Completed, Cancelled");
}

#[tokio::test]
async fn a_job_is_told_once_by_the_cancel_that_ends_it_and_leaves_out_a_removed_download() {
  let app = app(TRUSTED, &[]).await;
  let job = Uuid::new_v4();
  let in_job = |record: Record| Record {
    job_id: Some(job),
    ..record
  };
  let first = in_job(done("alice", "@@a\\Album\\1.flac"));
  let second = in_job(Record::download("alice", "@@a\\Album\\2.flac", 10));
  let third = in_job(Record::download("alice", "@@a\\Album\\3.flac", 10));
  for record in [&first, &second, &third] {
    insert(&app, record).await;
  }
  let mut stream = app
    .stream_from(
      "127.0.0.1:1",
      request("GET", "/api/v0/live?topics=transfers", None),
    )
    .await;
  assert_eq!(stream.next().await.unwrap().event, "hello");
  let canceled = format!("/api/v0/transfers/downloads/alice/{}", third.id);
  let removed = format!(
    "/api/v0/transfers/downloads/alice/{}?remove=true",
    second.id
  );
  for uri in [canceled, removed] {
    let (status, _) = app.call("DELETE", &uri, None).await;
    assert_eq!(status, 204);
  }
  let mut frames = Vec::new();
  for _ in 0..4 {
    frames.push(stream.next().await.unwrap());
  }
  let events: Vec<&str> = frames.iter().map(|f| f.event.as_str()).collect();
  assert_eq!(
    events,
    [
      "transfers.updated",
      "transfers.updated",
      "transfers.removed",
      "transfers.job_finished"
    ],
    "the first cancel leaves a download of the job to end"
  );
  let told = &frames[3].data;
  assert_eq!(told["id"], json!(job));
  assert_eq!(told["folder"], "@@a\\Album");
  assert_eq!(
    [
      &told["fileCount"],
      &told["succeededCount"],
      &told["canceledCount"]
    ],
    [2, 1, 1],
    "the removed download is not counted"
  );
}

/// Lists the download job `job` in the jobs list, as an enqueue does.
async fn listed_job(app: &TestApp, job: Uuid, name: &str) {
  let new = crate::jobs::NewJob {
    id: job,
    ..crate::jobs::NewJob::running(
      api_types::jobs::JobKind::Download,
      name,
      api_types::jobs::JobStarter::you(),
    )
  };
  app.state.jobs.create(new).await.unwrap();
}

#[tokio::test]
async fn a_paused_download_holds_its_job_paused_and_stays_paused_across_a_stop() {
  let app = app(TRUSTED, &[]).await;
  let job = Uuid::new_v4();
  listed_job(&app, job, "Album from alice").await;
  let mut waiting = Record::download("alice", "@@a\\Album\\1.flac", 10);
  waiting.job_id = Some(job);
  insert(&app, &waiting).await;
  let pause = json!({ "ids": [waiting.id], "action": "pause" });
  let (status, answer) = app
    .call("POST", "/api/v0/transfers/downloads/bulk", Some(pause))
    .await;
  assert_eq!(status, 200, "{answer}");
  assert_eq!(answer["ok"][0], waiting.id.to_string());
  let paused = read(&app, waiting.id).await;
  assert!(paused.paused);
  assert_eq!(paused.state.kind(), TransferStates::QueuedLocally);
  let listed = app.state.jobs.get(job).await.unwrap().unwrap();
  assert_eq!(listed.state, api_types::jobs::JobState::Paused);
  assert_eq!(listed.counts.paused, 1);

  crate::transfers::cleanup::end_unfinished(&app.state.transfers)
    .await
    .unwrap();
  let kept = read(&app, waiting.id).await;
  assert!(
    kept.paused && kept.ended_at.is_none(),
    "a stop leaves it paused"
  );

  let again = json!({ "ids": [waiting.id], "action": "pause" });
  let (_, answer) = app
    .call("POST", "/api/v0/transfers/downloads/bulk", Some(again))
    .await;
  assert_eq!(
    answer["failed"][0]["error"],
    "the download is paused already"
  );
  let resume = json!({ "ids": [waiting.id], "action": "resume" });
  let (_, answer) = app
    .call("POST", "/api/v0/transfers/downloads/bulk", Some(resume))
    .await;
  assert_eq!(answer["ok"][0], waiting.id.to_string(), "{answer}");
  assert!(!read(&app, waiting.id).await.paused);
}

#[tokio::test]
async fn a_download_job_ends_in_the_list_with_its_files_named_under_its_folder() {
  let app = app(TRUSTED, &[]).await;
  let job = Uuid::new_v4();
  listed_job(&app, job, "Album from alice").await;
  let mut arrived = done("alice", "@@a\\Album\\CD1\\1.flac");
  arrived.job_id = Some(job);
  let mut waiting = Record::download("alice", "@@a\\Album\\CD2\\1.flac", 10);
  waiting.job_id = Some(job);
  insert(&app, &arrived).await;
  insert(&app, &waiting).await;
  let uri = format!("/api/v0/transfers/downloads/alice/{}", waiting.id);
  let (status, _) = app.call("DELETE", &uri, None).await;
  assert_eq!(status, 204);
  let (_, detail) = app.call("GET", &format!("/api/v0/jobs/{job}"), None).await;
  assert_eq!(detail["job"]["state"], "ended", "{detail}");
  assert_eq!(
    detail["job"]["ending"], "succeeded",
    "a canceled file was the user's choice"
  );
  let parts: Vec<(&str, &str)> = detail["parts"]
    .as_array()
    .unwrap()
    .iter()
    .map(|p| (p["name"].as_str().unwrap(), p["state"].as_str().unwrap()))
    .collect();
  assert_eq!(
    parts,
    [("CD1\\1.flac", "succeeded"), ("CD2\\1.flac", "canceled")]
  );
}

#[tokio::test]
async fn a_folder_download_skips_the_files_the_filters_match_before_asking_its_user() {
  let app = app(TRUSTED, &[]).await;
  let group = |names: &[&str]| super::Group {
    username: "alice".into(),
    files: names
      .iter()
      .map(|name| super::FileRequest {
        filename: format!("@@a\\Album\\{name}"),
        size: 1,
        position: None,
      })
      .collect(),
    destination: None,
    folder: None,
    search_id: None,
  };
  let kept = ["Thumbs.db.flac", "01 desktop.ini.mp3", "exe.flac"];
  let mut groups = vec![group(&["setup.EXE", "Thumbs.db", ".DS_Store"])];
  groups.extend(kept.map(|name| group(&[name])));
  let downloads = &app.state.downloads;
  let mut results = downloads
    .enqueue(Uuid::new_v4(), groups, true)
    .await
    .into_iter();
  let enqueued = results
    .next()
    .unwrap()
    .expect("nothing left to ask alice for");
  assert!(enqueued.enqueued.is_empty());
  let skipped: Vec<&str> = enqueued.failed.iter().map(|f| f.message.as_str()).collect();
  assert_eq!(skipped, ["Skipped: Filtered"; 3]);
  // a file the filters pass goes on to ask alice, which the missing server refuses
  for (name, result) in kept.iter().zip(results) {
    let refused = result.err().unwrap_or_else(|| panic!("{name} is skipped"));
    assert!(refused.is_unavailable(), "{name} is downloaded");
  }
}

#[tokio::test]
async fn the_gauges_count_the_downloads_waiting_here_but_not_the_paused_ones() {
  let app = app(TRUSTED, &[]).await;
  let with = |username: &str, state: TransferStates, paused: bool| {
    let mut record = Record::download(username, &format!("{username}.flac"), 100);
    record.state = State::of(state);
    record.paused = paused;
    record
  };
  for record in [
    with("alice", TransferStates::QueuedLocally, false),
    with("bob", TransferStates::QueuedRemotely, false),
    with("carol", TransferStates::QueuedLocally, true),
    with("dave", TransferStates::InProgress, false),
    done("erin", "e.flac"),
  ] {
    insert(&app, &record).await;
  }
  let (queued, in_progress) = app.state.downloads.tallies().await.unwrap();
  assert_eq!(queued, (2, 2, 200));
  assert_eq!(in_progress, (1, 1, 100));
}

/// Reads the download `id` until `done` holds for it, for ten seconds at most.
async fn read_until(app: &TestApp, id: Uuid, done: impl Fn(&Record) -> bool) -> Record {
  for _ in 0..1000 {
    let record = read(app, id).await;
    if done(&record) {
      return record;
    }
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
  }
  panic!("the download never came to the state asked for");
}

#[tokio::test]
async fn a_download_paused_while_its_task_runs_waits_here_without_failing() {
  let config = format!("[transfers.download.retry]\ndelay = \"1h\"\nmax_delay = \"1h\"\n{TRUSTED}");
  let app = app(&config, &[]).await;
  let failed = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
  let counted = failed.clone();
  app.state.events.subscribe(
    "test",
    Some(api_types::events::EventType::DownloadFileFailed),
    move |_| {
      counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
      async { Ok(()) }
    },
  );
  let mut held = Record::download("alice", "@@a\\Album\\1.flac", 10);
  held.paused = true;
  insert(&app, &held).await;
  let id = held.id;
  // resumed, its task tries the peer, which cannot be reached, and waits to try again
  let resume = json!({ "ids": [id], "action": "resume" });
  app
    .call("POST", "/api/v0/transfers/downloads/bulk", Some(resume))
    .await;
  read_until(&app, id, |r| r.next_attempt_at().is_some()).await;

  let pause = json!({ "ids": [id], "action": "pause" });
  let (_, answer) = app
    .call("POST", "/api/v0/transfers/downloads/bulk", Some(pause))
    .await;
  assert_eq!(answer["ok"][0], id.to_string(), "{answer}");
  let paused = read_until(&app, id, |r| r.paused).await;
  assert_eq!(paused.state.kind(), TransferStates::QueuedLocally);
  assert_eq!(
    (
      paused.exception.as_deref(),
      paused.next_attempt_at(),
      paused.ended_at
    ),
    (None, None, None)
  );
  assert_eq!(
    app
      .state
      .telemetry
      .metrics()
      .transfers()
      .downloads
      .completed_failed
      .get(),
    0.0,
    "a pause is no failure"
  );
  tokio::time::sleep(std::time::Duration::from_millis(100)).await;
  assert_eq!(failed.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_resume_sent_while_the_download_stops_to_be_paused_waits_and_resumes_it() {
  let config = format!("[transfers.download.retry]\ndelay = \"1h\"\nmax_delay = \"1h\"\n{TRUSTED}");
  let app = app(&config, &[]).await;
  let mut held = Record::download("alice", "@@a\\Album\\1.flac", 10);
  held.paused = true;
  insert(&app, &held).await;
  let id = held.id;
  let bulk = |action: &str| json!({ "ids": [id], "action": action });
  app
    .call(
      "POST",
      "/api/v0/transfers/downloads/bulk",
      Some(bulk("resume")),
    )
    .await;
  read_until(&app, id, |r| r.next_attempt_at().is_some()).await;
  // the pause answers once the task is told to stop, before the task has written it paused
  app
    .call(
      "POST",
      "/api/v0/transfers/downloads/bulk",
      Some(bulk("pause")),
    )
    .await;
  let (_, answer) = app
    .call(
      "POST",
      "/api/v0/transfers/downloads/bulk",
      Some(bulk("resume")),
    )
    .await;
  assert_eq!(answer["ok"][0], id.to_string(), "{answer}");
  assert!(!read(&app, id).await.paused);
}

/// Resumes a paused download of alice's whose whole file is in the incomplete directory, so it
/// arrives at once, and holds its move at the gate until the guard is dropped.
async fn moving(app: &TestApp) -> (Uuid, tokio::sync::MutexGuard<'_, ()>) {
  let downloads = &app.state.downloads;
  let mut held = Record::download("alice", "@@a\\Album\\1.flac", 10);
  held.paused = true;
  insert(app, &held).await;
  let (incomplete, _) = super::incomplete_path(
    &downloads.incomplete,
    "alice",
    &held.filename,
    crate::safety::Os::CURRENT,
  )
  .unwrap();
  std::fs::create_dir_all(incomplete.parent().unwrap()).unwrap();
  std::fs::write(&incomplete, [0; 10]).unwrap();
  let gate = downloads.move_gate.lock().await;
  let resume = json!({ "ids": [held.id], "action": "resume" });
  app
    .call("POST", "/api/v0/transfers/downloads/bulk", Some(resume))
    .await;
  for _ in 0..1000 {
    if downloads.moving.len() == 1 {
      break;
    }
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
  }
  assert_eq!(downloads.moving.len(), 1, "the move waits at the gate");
  (held.id, gate)
}

#[tokio::test]
async fn a_pause_or_cancel_during_the_move_says_the_download_has_completed() {
  let app = app(TRUSTED, &[]).await;
  let (id, gate) = moving(&app).await;
  for action in ["pause", "cancel"] {
    let (status, answer) = app
      .call(
        "POST",
        "/api/v0/transfers/downloads/bulk",
        Some(json!({ "ids": [id], "action": action })),
      )
      .await;
    assert_eq!(status, 200, "{answer}");
    assert_eq!(answer["ok"], json!([]), "{action}: {answer}");
    assert_eq!(
      answer["failed"][0]["error"], "the download has completed",
      "{action}: {answer}"
    );
  }
  drop(gate);
  let moved = read_until(&app, id, |r| r.local_path.is_some()).await;
  assert_eq!(
    moved.state.kind(),
    TransferStates::Completed(TransferOutcome::Succeeded)
  );
  assert!(!moved.paused);
}

#[tokio::test]
async fn a_stop_waits_past_its_limit_for_a_download_moving_its_file() {
  let app = app(TRUSTED, &[]).await;
  let downloads = app.state.downloads.clone();
  let (id, gate) = moving(&app).await;

  tokio::time::pause();
  let stopping = tokio::spawn({
    let downloads = downloads.clone();
    async move { downloads.stop().await }
  });
  tokio::time::sleep(super::STOPPING * 2).await;
  assert!(
    !stopping.is_finished(),
    "the stop waits for the move past its limit"
  );
  drop(gate);
  tokio::time::resume();
  stopping.await.unwrap();
  let moved = read(&app, id).await;
  assert_eq!(
    moved.state.kind(),
    TransferStates::Completed(TransferOutcome::Succeeded)
  );
  assert!(
    moved.local_path.is_some(),
    "the record is written with where the file went"
  );
}

#[tokio::test]
async fn a_reorder_of_the_local_queue_is_written_to_the_database() {
  let app = app(TRUSTED, &[]).await;
  let downloads = &app.state.downloads;
  // waiting in the local queue for a request none of whose files may take a turn
  let request = downloads.starts.request();
  let mut ids = Vec::new();
  // kept to the end, as the tasks that wait would be
  let mut _waiting = Vec::new();
  for name in ["1.flac", "2.flac", "3.flac"] {
    let record = Record::download("alice", &format!("@@a\\Album\\{name}"), 10);
    insert(&app, &record).await;
    let position = read(&app, record.id).await.queue_position.unwrap();
    let wait = downloads
      .starts
      .wait(record.id, "alice", position, Some((request, 0)));
    _waiting.push(wait);
    ids.push(record.id);
  }
  assert_eq!(downloads.queue_order(), ids);

  let last_first = json!({ "ids": [ids[2]], "before": ids[0] });
  let (status, _) = app
    .call(
      "POST",
      "/api/v0/transfers/downloads/queue",
      Some(last_first),
    )
    .await;
  assert_eq!(status, 200);
  let mut stored = Vec::new();
  for id in &ids {
    stored.push((read(&app, *id).await.queue_position.unwrap(), *id));
  }
  stored.sort_by(|a, b| a.0.total_cmp(&b.0));
  let order: Vec<Uuid> = stored.into_iter().map(|(_, id)| id).collect();
  assert_eq!(
    order,
    [ids[2], ids[0], ids[1]],
    "a restart reads the new order"
  );
}

#[tokio::test]
async fn a_download_job_that_never_had_a_download_ends_on_cancel_and_after_a_restart() {
  let app = app(TRUSTED, &[]).await;
  let (canceled, stranded) = (Uuid::new_v4(), Uuid::new_v4());
  listed_job(&app, canceled, "Album from alice").await;
  listed_job(&app, stranded, "Album from bob").await;
  let (status, _) = app
    .call("POST", &format!("/api/v0/jobs/{canceled}/cancel"), None)
    .await;
  assert_eq!(status, 204);
  let job = app.state.jobs.get(canceled).await.unwrap().unwrap();
  assert_eq!(job.ending, Some(api_types::jobs::JobEnding::Canceled));

  app.state.downloads.resume().await;
  let job = app.state.jobs.get(stranded).await.unwrap().unwrap();
  assert_eq!(job.ending, Some(api_types::jobs::JobEnding::Failed));
  assert_eq!(job.error.as_deref(), Some("Nothing was queued"));
}

#[tokio::test]
async fn a_job_refused_for_want_of_the_server_says_so_in_words() {
  let app = app(TRUSTED, &[]).await;
  let request = json!({
    "groups": [{ "username": "alice", "files": [{ "filename": "@@a\\1.flac", "size": 1 }] }]
  });
  let (_, answer) = app
    .call("POST", "/api/v0/transfers/downloads/jobs", Some(request))
    .await;
  let id: Uuid = answer["id"].as_str().unwrap().parse().unwrap();
  let job = app.state.jobs.get(id).await.unwrap().unwrap();
  assert_eq!(job.ending, Some(api_types::jobs::JobEnding::Failed));
  assert_eq!(
    job.error.as_deref(),
    Some("Not connected to the Soulseek server")
  );
}

/// The id of the download of `filename` still in the listings.
async fn listed(app: &TestApp, filename: &str) -> Uuid {
  let filename = filename.to_string();
  app
    .state
    .transfers
    .read(move |conn| {
      conn.query_row(
        "SELECT Id FROM Transfers WHERE Filename = ?1 AND Removed = 0",
        [filename],
        |row| row.get::<_, String>(0),
      )
    })
    .await
    .unwrap()
    .parse()
    .unwrap()
}

#[tokio::test]
async fn an_interrupted_download_whole_here_is_moved_though_its_user_cannot_be_reached() {
  let app = app(TRUSTED, &[]).await;
  let downloads = &app.state.downloads;
  let job = Uuid::new_v4();
  listed_job(&app, job, "Album from alice").await;
  let mut arrived = Record::download("alice", "@@a\\Album\\1.flac", 10);
  arrived.job_id = Some(job);
  let mut partial = Record::download("alice", "@@a\\Album\\2.flac", 10);
  partial.job_id = Some(job);
  for (record, bytes) in [(&arrived, 10), (&partial, 4)] {
    insert(&app, record).await;
    let (incomplete, _) = super::incomplete_path(
      &downloads.incomplete,
      "alice",
      &record.filename,
      crate::safety::Os::CURRENT,
    )
    .unwrap();
    std::fs::create_dir_all(incomplete.parent().unwrap()).unwrap();
    std::fs::write(&incomplete, vec![0; bytes]).unwrap();
  }
  crate::transfers::cleanup::end_unfinished(&app.state.transfers)
    .await
    .unwrap();

  // the daemon is not connected, so alice cannot be reached
  downloads.resume().await;
  let id = listed(&app, "@@a\\Album\\1.flac").await;
  assert_ne!(id, arrived.id, "enqueued again as a new download");
  let moved = read_until(&app, id, |r| r.local_path.is_some()).await;
  assert_eq!(
    moved.state.kind(),
    TransferStates::Completed(TransferOutcome::Succeeded)
  );
  assert_eq!(
    listed(&app, "@@a\\Album\\2.flac").await,
    partial.id,
    "the partial one waits for its user"
  );
}

/// A resume reads a paused download, a cancel ends it, and then the resume writes: the
/// download stays canceled, so the resume answers that it failed and starts no task. The
/// writes reach the one writer in the order the test queues them; the read goes to a reader.
#[tokio::test]
async fn a_resume_that_loses_the_race_to_a_cancel_fails_and_starts_no_task() {
  let app = app(TRUSTED, &[]).await;
  let mut paused = Record::download("alice", "a.flac", 10);
  paused.paused = true;
  insert(&app, &paused).await;
  let id = paused.id;
  // the writer is held while the cancel's write queues for it and then the resume reads the
  // paused row and queues its save, so the cancel ends the row before the resume writes
  let (release, released) = std::sync::mpsc::channel::<()>();
  let (holding, held) = tokio::sync::oneshot::channel();
  let transfers = app.state.transfers.clone();
  let hold = tokio::spawn(async move {
    transfers
      .write(move |_| {
        holding.send(()).unwrap();
        released.recv().unwrap();
        Ok(())
      })
      .await
  });
  held.await.unwrap();
  let (queuing, queued) = tokio::sync::oneshot::channel();
  let transfers = app.state.transfers.clone();
  let canceling = tokio::spawn(async move {
    // the write takes its place in the writer's queue at its first poll, before this task
    // gives way, and the test runs on this one thread, so the test resumes after it
    queuing.send(()).unwrap();
    transfers
      .write(move |conn| {
        let state = TransferStates::Completed(TransferOutcome::Cancelled);
        store::end_unless_ended(conn, id, state, chrono::Utc::now(), super::CANCELED)
      })
      .await
  });
  queued.await.unwrap();
  let downloads = app.state.downloads.clone();
  let resuming = tokio::spawn(async move { downloads.resume_paused(vec![id]).await });
  // the resume puts in the download's handle and queues its save in one go, after its read
  let saving = tokio::time::timeout(std::time::Duration::from_secs(5), async {
    while !reseek_shares::lock(&app.state.downloads.in_flight).contains_key(&id) {
      tokio::task::yield_now().await;
    }
  })
  .await;
  assert!(saving.is_ok(), "the resume never got to its save");
  release.send(()).unwrap();
  let outcomes = resuming.await.unwrap();
  hold.await.unwrap().unwrap();
  assert!(canceling.await.unwrap().unwrap());
  assert!(
    matches!(outcomes[..], [Err(ref failed)] if failed.id == id),
    "{outcomes:?}"
  );
  assert!(!reseek_shares::lock(&app.state.downloads.in_flight).contains_key(&id));
  assert_eq!(
    read(&app, id).await.state.kind(),
    TransferStates::Completed(TransferOutcome::Cancelled)
  );
}

#[tokio::test]
async fn a_resume_while_logged_out_leaves_the_interrupted_downloads_for_the_next_login() {
  let app = app(TRUSTED, &[]).await;
  let job = Uuid::new_v4();
  listed_job(&app, job, "Album from alice").await;
  let mut interrupted = Record::download("alice", "@@a\\Album\\1.flac", 10);
  interrupted.job_id = Some(job);
  insert(&app, &interrupted).await;
  crate::transfers::cleanup::end_unfinished(&app.state.transfers)
    .await
    .unwrap();

  // the daemon is not connected, so nothing can be enqueued
  app.state.downloads.resume().await;
  let left = crate::transfers::cleanup::take_resumable(&app.state.transfers)
    .await
    .unwrap();
  let files: Vec<&str> = left
    .get(&job)
    .into_iter()
    .flatten()
    .flat_map(|group| &group.files)
    .map(|file| file.filename.as_str())
    .collect();
  assert_eq!(files, ["@@a\\Album\\1.flac"]);
}

#[tokio::test]
async fn the_listings_answer_with_the_other_downloads_when_one_row_holds_an_unknown_state() {
  let app = app(TRUSTED, &[]).await;
  let known = Record::download("alice", "@@a\\Album\\1.flac", 10);
  let odd = Record::download("alice", "@@a\\Album\\2.flac", 10);
  insert(&app, &known).await;
  insert(&app, &odd).await;
  let id = odd.id.to_string();
  app
    .state
    .transfers
    .write(move |conn| conn.execute("UPDATE Transfers SET State = 2 WHERE Id = ?1", [id]))
    .await
    .unwrap();
  let named = |files: &[Value]| {
    files
      .iter()
      .any(|file| file["filename"] == "@@a\\Album\\1.flac")
  };

  let (status, grouped) = app.call("GET", "/api/v0/transfers/downloads", None).await;
  assert_eq!(status, 200, "{grouped}");
  assert!(
    named(grouped[0]["directories"][0]["files"].as_array().unwrap()),
    "{grouped}"
  );
  let (status, page) = app
    .call("GET", "/api/v0/transfers/downloads/list", None)
    .await;
  assert_eq!(status, 200, "{page}");
  assert!(named(page["items"].as_array().unwrap()), "{page}");
}

#[tokio::test]
async fn a_resume_dropped_once_its_record_is_written_leaves_a_download_a_cancel_ends() {
  let app = app(TRUSTED, &[]).await;
  let downloads = &app.state.downloads;
  let mut paused = Record::download("alice", "a.flac", 10);
  paused.paused = true;
  insert(&app, &paused).await;
  let id = paused.id;
  let mut resuming = Box::pin(downloads.resume_paused(vec![id]));
  // polled until it holds the download's cancel handle, which is when its record is being
  // written
  while !reseek_shares::lock(&downloads.in_flight).contains_key(&id) {
    let polled = std::future::poll_fn(|cx| Poll::Ready(resuming.as_mut().poll(cx))).await;
    assert!(polled.is_pending());
    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
  }
  // queued behind the record's write, so the record is written once this returns
  app.state.transfers.write(|_| Ok(())).await.unwrap();
  drop(resuming);
  assert!(!read(&app, id).await.paused);

  let request = json!({ "ids": [id], "action": "cancel" });
  let (status, result) = app
    .call("POST", "/api/v0/transfers/downloads/bulk", Some(request))
    .await;
  assert_eq!(status, 200);
  assert_eq!(result["ok"], json!([id]));
  let ended = tokio::time::timeout(std::time::Duration::from_secs(5), async {
    while !read(&app, id).await.is_completed() {
      tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
  })
  .await;
  assert!(ended.is_ok(), "the canceled download never ended");
}

/// Whether a download of `filename` from alice has not ended.
async fn unfinished(app: &TestApp, filename: &'static str) -> bool {
  app
    .state
    .transfers
    .read(move |conn| {
      let mut ids =
        conn.prepare("SELECT Id FROM Transfers WHERE Username = 'alice' AND Filename = ?1")?;
      let ids = ids
        .query_map([filename], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
      for id in ids {
        if !store::get(conn, id.parse().unwrap())?
          .unwrap()
          .is_completed()
        {
          return Ok(true);
        }
      }
      Ok(false)
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn a_download_interrupted_is_enqueued_at_the_login_after_a_resume_while_logged_out() {
  use soulseek_proto::server::GetPeerAddressResponse;
  use soulseek_proto::{ServerRequest, ServerResponse};

  let app = crate::testing::soulseek::app(TRUSTED).await;
  // alice's address, where connections are accepted and held open
  let peer = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
  let port = peer.local_addr().unwrap().port();
  tokio::spawn(async move {
    let mut held = Vec::new();
    while let Ok((stream, _)) = peer.accept().await {
      held.push(stream);
    }
  });
  let server = crate::testing::soulseek::Server::start(move |request| match request {
    ServerRequest::GetPeerAddress(asked) => {
      vec![ServerResponse::GetPeerAddress(GetPeerAddressResponse {
        username: asked.username.clone(),
        ip: std::net::Ipv4Addr::LOCALHOST,
        port: port.into(),
        obfuscation: None,
      })]
    }
    _ => Vec::new(),
  })
  .await;
  let job = Uuid::new_v4();
  listed_job(&app, job, "Album from alice").await;
  let mut interrupted = Record::download("alice", "@@a\\Album\\1.flac", 10);
  interrupted.job_id = Some(job);
  insert(&app, &interrupted).await;
  crate::transfers::cleanup::end_unfinished(&app.state.transfers)
    .await
    .unwrap();
  assert!(!unfinished(&app, "@@a\\Album\\1.flac").await);

  // the daemon is not connected, so nothing can be enqueued
  app.state.downloads.resume().await;
  server.login(&app).await;
  let enqueued = tokio::time::timeout(std::time::Duration::from_secs(10), async {
    while !unfinished(&app, "@@a\\Album\\1.flac").await {
      tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
  })
  .await;
  assert!(enqueued.is_ok(), "not enqueued again after the login");
}

#[tokio::test]
async fn a_purge_of_a_users_transfers_leaves_no_download_running_out_of_the_listings() {
  let config = format!("[transfers.download.retry]\ndelay = \"1h\"\nmax_delay = \"1h\"\n{TRUSTED}");
  let app = app(&config, &[]).await;
  let users = &app.state.users;
  users.set_buddy("alice", Default::default()).await.unwrap();
  let mut held = Record::download("alice", "@@a\\Album\\1.flac", 10);
  held.paused = true;
  insert(&app, &held).await;
  let id = held.id;
  // resumed, its task tries the peer, which cannot be reached, and waits to try again
  let resume = json!({ "ids": [id], "action": "resume" });
  app
    .call("POST", "/api/v0/transfers/downloads/bulk", Some(resume))
    .await;
  read_until(&app, id, |r| r.next_attempt_at().is_some()).await;

  let (status, _) = app
    .call("DELETE", "/api/v0/users/alice?purge=true", None)
    .await;
  assert_eq!(status, 204);
  let running = || reseek_shares::lock(&app.state.downloads.in_flight).contains_key(&id);
  let stopped = tokio::time::timeout(std::time::Duration::from_secs(2), async {
    while running() {
      tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
  })
  .await;
  let (_, listed) = app.call("GET", "/api/v0/transfers/downloads", None).await;
  assert!(
    stopped.is_ok() || listed != json!([]),
    "the download still runs and no listing has it"
  );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn places_a_peer_sends_on_its_own_are_kept_in_the_order_sent() {
  let app = app(TRUSTED, &[]).await;
  let queued = |filename: &str| {
    let mut record = Record::download("alice", filename, 10);
    record.state = State::QueuedRemotely {
      place_in_queue: None,
    };
    record
  };
  let (sent_often, sent_last) = (queued("a.flac"), queued("b.flac"));
  insert(&app, &sent_often).await;
  insert(&app, &sent_last).await;
  let downloads = &app.state.downloads;
  for place in 1..=20 {
    downloads
      .place_sent("alice".into(), "a.flac".into(), place)
      .await;
  }
  downloads
    .place_sent("alice".into(), "b.flac".into(), 7)
    .await;
  read_until(&app, sent_last.id, |r| r.place_in_queue() == Some(7)).await;
  assert_eq!(read(&app, sent_often.id).await.place_in_queue(), Some(20));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_albums_files_are_asked_of_their_user_in_the_order_enqueued() {
  use soulseek_proto::frame::FrameKind;
  use soulseek_proto::server::GetPeerAddressResponse;
  use soulseek_proto::{InitMessage, PeerMessage, ServerRequest, ServerResponse};

  let app = crate::testing::soulseek::app(TRUSTED).await;
  let alice = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
  let port = alice.local_addr().unwrap().port();
  let server = crate::testing::soulseek::Server::start(move |request| match request {
    ServerRequest::GetPeerAddress(asked) => {
      vec![ServerResponse::GetPeerAddress(GetPeerAddressResponse {
        username: asked.username.clone(),
        ip: std::net::Ipv4Addr::LOCALHOST,
        port: port.into(),
        obfuscation: None,
      })]
    }
    _ => Vec::new(),
  })
  .await;
  server.login(&app).await;
  let album: Vec<String> = (1..=20)
    .map(|track| format!("@@a\\Album\\{track:02}.flac"))
    .collect();
  let group = super::Group {
    username: "alice".into(),
    files: album
      .iter()
      .map(|filename| super::FileRequest {
        filename: filename.clone(),
        size: 10,
        position: None,
      })
      .collect(),
    destination: None,
    folder: None,
    search_id: None,
  };
  let downloads = app.state.downloads.clone();
  let enqueued =
    tokio::spawn(async move { downloads.enqueue(Uuid::new_v4(), vec![group], false).await });

  let within = std::time::Duration::from_secs(10);
  let (mut stream, _) = tokio::time::timeout(within, alice.accept())
    .await
    .expect("the daemon connects to alice")
    .unwrap();
  let mut buffer = Vec::new();
  let init = crate::testing::soulseek::next_frame(&mut stream, &mut buffer, FrameKind::Init).await;
  assert!(matches!(
    InitMessage::decode(&init).unwrap(),
    InitMessage::PeerInit(_)
  ));
  let mut asked = Vec::new();
  while asked.len() < album.len() {
    let frame = tokio::time::timeout(
      within,
      crate::testing::soulseek::next_frame(&mut stream, &mut buffer, FrameKind::Peer),
    )
    .await
    .expect("every file is asked for");
    match PeerMessage::decode(&frame).unwrap() {
      PeerMessage::QueueDownload(request) => asked.push(request.filename.to_string()),
      PeerMessage::TransferRequest(request) => asked.push(request.filename.to_string()),
      _ => {}
    }
  }
  assert!(enqueued.await.unwrap()[0].is_ok());
  assert_eq!(asked, album);
}

/// A peer the daemon reaches at a loopback listener of the test's, through a scripted server
/// that says where each listener's user is while they are online, and that they are offline
/// otherwise.
struct Peers {
  server: crate::testing::soulseek::Server,
  listeners: std::collections::HashMap<&'static str, tokio::net::TcpListener>,
  online: std::sync::Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
}

impl Peers {
  /// `usernames`, online, for a daemon `app` logs in to the server.
  async fn start(app: &TestApp, usernames: &[&'static str]) -> Self {
    use soulseek_proto::server::{
      GetPeerAddressResponse, UserStats, UserStatus, WatchUserResponse, WatchedUser,
    };
    use soulseek_proto::{ServerRequest, ServerResponse, UserPresence};

    let mut listeners = std::collections::HashMap::new();
    let mut ports = std::collections::HashMap::new();
    for username in usernames {
      let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
      ports.insert(username.to_string(), listener.local_addr().unwrap().port());
      listeners.insert(*username, listener);
    }
    let online = std::sync::Arc::new(std::sync::Mutex::new(
      usernames
        .iter()
        .map(|u| u.to_string())
        .collect::<std::collections::HashSet<_>>(),
    ));
    let seen = online.clone();
    let server = crate::testing::soulseek::Server::start(move |request| {
      let is_online = |username: &str| reseek_shares::lock(&seen).contains(username);
      let presence = |username: &str| {
        if is_online(username) {
          UserPresence::Online
        } else {
          UserPresence::Offline
        }
      };
      match request {
        ServerRequest::GetPeerAddress(asked) => {
          let here = is_online(&asked.username);
          vec![ServerResponse::GetPeerAddress(GetPeerAddressResponse {
            username: asked.username.clone(),
            ip: if here {
              std::net::Ipv4Addr::LOCALHOST
            } else {
              std::net::Ipv4Addr::UNSPECIFIED
            },
            port: if here {
              ports[&asked.username].into()
            } else {
              0
            },
            obfuscation: None,
          })]
        }
        ServerRequest::GetStatus(asked) => vec![ServerResponse::GetStatus(UserStatus {
          username: asked.username.clone(),
          presence: presence(&asked.username),
          is_privileged: false,
        })],
        ServerRequest::WatchUser(asked) => vec![ServerResponse::WatchUser(WatchUserResponse {
          username: asked.username.clone(),
          // a name with no listener is no user at all
          user: ports.contains_key(&asked.username).then(|| WatchedUser {
            status: presence(&asked.username),
            stats: UserStats::default(),
            country_code: None,
          }),
        })],
        _ => Vec::new(),
      }
    })
    .await;
    server.login(app).await;
    Self {
      server,
      listeners,
      online,
    }
  }

  /// Sets `username` online or offline, and has the server say so.
  fn presence(&self, username: &str, online: bool) {
    use soulseek_proto::server::UserStatus;
    use soulseek_proto::{ServerResponse, UserPresence};

    let mut users = reseek_shares::lock(&self.online);
    if online {
      users.insert(username.to_string());
    } else {
      users.remove(username);
    }
    drop(users);
    self.server.send(vec![ServerResponse::GetStatus(UserStatus {
      username: username.to_string(),
      presence: if online {
        UserPresence::Online
      } else {
        UserPresence::Offline
      },
      is_privileged: false,
    })]);
  }

  /// The daemon's next message connection to `username`, past its introduction.
  async fn connected(&self, username: &str) -> Peer {
    let within = std::time::Duration::from_secs(10);
    let (stream, _) = tokio::time::timeout(within, self.listeners[username].accept())
      .await
      .expect("the daemon connects")
      .unwrap();
    let mut peer = Peer {
      stream,
      buffer: Vec::new(),
    };
    let init = crate::testing::soulseek::next_frame(
      &mut peer.stream,
      &mut peer.buffer,
      soulseek_proto::frame::FrameKind::Init,
    )
    .await;
    assert!(matches!(
      soulseek_proto::InitMessage::decode(&init).unwrap(),
      soulseek_proto::InitMessage::PeerInit(_)
    ));
    peer
  }
}

/// Connects to `app`'s daemon as `username` for the transfer `token`, reads where it asks to
/// start, and sends the first `sent` bytes of a file of ten from there.
async fn sending(app: &TestApp, username: &str, token: u32, sent: usize) -> tokio::net::TcpStream {
  use soulseek_proto::init::{FileTransferInit, PeerInit};
  use soulseek_proto::{InitMessage, PeerConnectionType};
  use tokio::io::{AsyncReadExt, AsyncWriteExt};

  let port = app.state.config.current().values.soulseek.listen_port;
  let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
    .await
    .unwrap();
  let mut init = InitMessage::PeerInit(PeerInit {
    username: username.into(),
    connection_type: PeerConnectionType::Transfer,
    token: 0,
  })
  .encode();
  init.extend(FileTransferInit { token }.encode());
  stream.write_all(&init).await.unwrap();
  let mut offset = [0; 8];
  stream.read_exact(&mut offset).await.unwrap();
  assert_eq!(u64::from_le_bytes(offset), 0);
  stream.write_all(&[7; 10][..sent]).await.unwrap();
  stream
}

/// The test's side of a message connection the daemon made.
struct Peer {
  stream: tokio::net::TcpStream,
  buffer: Vec<u8>,
}

impl Peer {
  /// The next peer message the daemon sends, within `within`; none when none comes.
  async fn next_within(
    &mut self,
    within: std::time::Duration,
  ) -> Option<soulseek_proto::PeerMessage> {
    let frame = tokio::time::timeout(
      within,
      crate::testing::soulseek::next_frame(
        &mut self.stream,
        &mut self.buffer,
        soulseek_proto::frame::FrameKind::Peer,
      ),
    )
    .await
    .ok()?;
    Some(soulseek_proto::PeerMessage::decode(&frame).unwrap())
  }

  /// Whether the daemon sends nothing for `within`, and whether it closed the connection
  /// meanwhile; not quiet when a message comes.
  async fn quiet_for(&mut self, within: std::time::Duration) -> (bool, bool) {
    use tokio::io::AsyncReadExt;
    let mut chunk = [0; 4096];
    match tokio::time::timeout(within, self.stream.read(&mut chunk)).await {
      Err(_) => (true, false),
      Ok(Ok(0)) => (true, true),
      Ok(read) => {
        self.buffer.extend_from_slice(&chunk[..read.unwrap()]);
        (false, false)
      }
    }
  }

  /// The next peer message the daemon sends, other than a question of its place in the queue.
  async fn next(&mut self) -> soulseek_proto::PeerMessage {
    loop {
      let message = self
        .next_within(std::time::Duration::from_secs(10))
        .await
        .expect("the daemon sends a message");
      if !matches!(message, soulseek_proto::PeerMessage::PlaceInQueueRequest(_)) {
        return message;
      }
    }
  }

  /// Reads the daemon's question of the place of `filename`.
  async fn asked_place(&mut self, filename: &str) {
    match self.next_within(std::time::Duration::from_secs(10)).await {
      Some(soulseek_proto::PeerMessage::PlaceInQueueRequest(asked)) => {
        assert_eq!(asked.filename.as_str(), filename);
      }
      other => panic!("not a question of the place of {filename}: {other:?}"),
    }
  }

  /// Reads the daemon's question of the place of `filename`, and answers `place`.
  async fn place_asked(&mut self, filename: &str, place: u32) {
    use soulseek_proto::peer::PlaceInQueueResponse;
    self.asked_place(filename).await;
    let answer = soulseek_proto::PeerMessage::PlaceInQueueResponse(PlaceInQueueResponse {
      filename: filename.into(),
      place,
    });
    self.send(answer).await;
  }

  /// Reads the daemon's request for `filename`.
  async fn asked_for(&mut self, filename: &str) {
    match self.next().await {
      soulseek_proto::PeerMessage::QueueDownload(asked) => {
        assert_eq!(asked.filename.as_str(), filename);
      }
      other => panic!("not a request for {filename}: {other:?}"),
    }
  }

  async fn send(&mut self, message: soulseek_proto::PeerMessage) {
    use tokio::io::AsyncWriteExt;
    self.stream.write_all(&message.encode()).await.unwrap();
  }

  /// Offers `filename` under `token`, and returns the daemon's answer.
  async fn offer(&mut self, filename: &str, token: u32) -> soulseek_proto::peer::TransferResponse {
    use soulseek_proto::peer::TransferRequest;
    self
      .send(soulseek_proto::PeerMessage::TransferRequest(
        TransferRequest {
          direction: soulseek_proto::TransferDirection::Upload,
          token,
          filename: filename.into(),
          file_size: Some(10),
        },
      ))
      .await;
    match self.next().await {
      soulseek_proto::PeerMessage::TransferResponse(answer) => answer,
      other => panic!("not an answer to the offer: {other:?}"),
    }
  }
}

/// Enqueues `filename` from `username` and returns its download.
async fn enqueued(app: &TestApp, username: &str, filename: &str) -> Uuid {
  let group = super::Group {
    username: username.into(),
    files: vec![super::FileRequest {
      filename: filename.into(),
      size: 10,
      position: None,
    }],
    destination: None,
    folder: None,
    search_id: None,
  };
  let results = app
    .state
    .downloads
    .enqueue(Uuid::new_v4(), vec![group], false)
    .await;
  let enqueued = results.into_iter().next().unwrap().unwrap();
  enqueued.enqueued[0].id
}

/// A user who goes offline while their file waits in their queue is waited for, and the try
/// they were gone for does not count; once they are back the file is asked for again, and a
/// failure while they are online counts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_download_whose_user_goes_offline_waits_for_them_without_spending_its_tries() {
  let config = format!(
    "[transfers.download.retry]\nattempts = 2\ndelay = \"1h\"\nmax_delay = \"1h\"\n{TRUSTED}"
  );
  let app = crate::testing::soulseek::app(&config).await;
  let peers = Peers::start(&app, &["bob"]).await;
  let file = "@@b\\Album\\1.flac";
  let id = enqueued(&app, "bob", file).await;
  let mut bob = peers.connected("bob").await;
  bob.asked_for(file).await;
  read_until(&app, id, |r| {
    r.state.kind() == TransferStates::QueuedRemotely
  })
  .await;

  peers.presence("bob", false);
  let waiting = read_until(&app, id, Record::waits_for_user).await;
  assert_eq!(
    waiting.attempts, 1,
    "the try bob was gone for does not count"
  );
  assert_eq!(waiting.next_attempt_at(), None);
  assert!(waiting.transfer().waiting_for_user);

  peers.presence("bob", true);
  bob.asked_for(file).await;
  let asked = read_until(&app, id, |r| !r.waits_for_user()).await;
  assert_eq!(asked.attempts, 1);

  // bob, online, fails it: that try counts
  bob
    .send(soulseek_proto::PeerMessage::UploadFailed(
      soulseek_proto::peer::UploadFailed {
        filename: file.into(),
      },
    ))
    .await;
  let retrying = read_until(&app, id, |r| r.next_attempt_at().is_some()).await;
  assert_eq!(retrying.attempts, 2);
  assert!(!retrying.waits_for_user());
}

/// A try that finds its user offline does not count: the download waits for them to come back,
/// and is asked of them then.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_download_whose_try_finds_its_user_offline_waits_for_them() {
  let config = format!(
    "[transfers.download.retry]\nattempts = 2\ndelay = \"1s\"\nmax_delay = \"30s\"\n{TRUSTED}"
  );
  let app = crate::testing::soulseek::app(&config).await;
  let peers = Peers::start(&app, &["bob"]).await;
  // bob went offline without the server telling us
  reseek_shares::lock(&peers.online).remove("bob");
  let file = "@@b\\Album\\1.flac";
  let mut held = Record::download("bob", file, 10);
  held.paused = true;
  insert(&app, &held).await;
  let resume = json!({ "ids": [held.id], "action": "resume" });
  app
    .call("POST", "/api/v0/transfers/downloads/bulk", Some(resume))
    .await;
  let waiting = read_until(&app, held.id, Record::waits_for_user).await;
  assert_eq!(waiting.attempts, 1);
  // longer than the retries would have taken
  tokio::time::sleep(std::time::Duration::from_secs(3)).await;
  let still = read(&app, held.id).await;
  assert!(still.waits_for_user(), "{:?}", still.state);
  assert_eq!(still.attempts, 1);

  // bob comes back without the server saying so: the status the download asks for finds him
  reseek_shares::lock(&peers.online).insert("bob".into());
  let mut bob = peers.connected("bob").await;
  bob.asked_for(file).await;
}

/// Tells the downloads a user who went offline is back, as the server's word would, as soon as
/// a download's try says it stops asking for them: the one step between the try seeing them go
/// and its end, where nothing waits that a test could hold. It keys on that debug line's text,
/// so rewording the line fails the test that uses it.
struct BackAsTheTryStops {
  downloads: std::sync::Arc<std::sync::OnceLock<std::sync::Arc<super::Downloads>>>,
  username: &'static str,
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for BackAsTheTryStops {
  fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
    struct Message(bool);
    impl tracing::field::Visit for Message {
      fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
          self.0 = format!("{value:?}") == "the user went offline; the download stops asking";
        }
      }
    }
    let mut stops = Message(false);
    event.record(&mut stops);
    if let (true, Some(downloads)) = (stops.0, self.downloads.get()) {
      downloads.presence(self.username, api_types::users::UserPresence::Online);
    }
  }
}

/// A try stopped for its user going offline, who is back before the try has ended, waits for
/// its turn here behind a file that holds the only slot, and reads so, not as queued at the
/// peer it no longer asks.
#[test]
fn a_try_stopped_for_a_user_back_already_reads_queued_here_while_it_waits() {
  use soulseek_proto::peer::TransferResponse;
  use tracing_subscriber::layer::SubscriberExt;

  let downloads = std::sync::Arc::<std::sync::OnceLock<_>>::default();
  let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(BackAsTheTryStops {
    downloads: downloads.clone(),
    username: "bob",
  }));
  let threads = dispatch.clone();
  let runtime = tokio::runtime::Builder::new_multi_thread()
    .worker_threads(4)
    .enable_all()
    // the layer stays for each worker's life
    .on_thread_start(move || std::mem::forget(tracing::dispatcher::set_default(&threads)))
    .build()
    .unwrap();
  let _here = tracing::dispatcher::set_default(&dispatch);
  runtime.block_on(async {
    let config = format!(
      "[transfers.download]\nslots = 1\n[transfers.download.retry]\ndelay = \"1h\"\n\
       max_delay = \"1h\"\n{TRUSTED}"
    );
    let app = crate::testing::soulseek::app(&config).await;
    downloads.set(app.state.downloads.clone()).ok().unwrap();
    let peers = Peers::start(&app, &["alice", "bob"]).await;
    let (alices, bobs) = ("@@a\\1.flac", "@@b\\1.flac");
    let id = enqueued(&app, "bob", bobs).await;
    let mut bob = peers.connected("bob").await;
    bob.asked_for(bobs).await;
    bob.place_asked(bobs, 3).await;
    enqueued(&app, "alice", alices).await;
    let mut alice = peers.connected("alice").await;
    alice.asked_for(alices).await;
    assert!(matches!(
      alice.offer(alices, 1).await,
      TransferResponse::Allowed { token: 1, .. }
    ));
    let _moving = sending(&app, "alice", 1, 4).await;
    assert!(matches!(
      read(&app, id).await.state,
      State::QueuedRemotely { .. }
    ));

    peers.presence("bob", false);
    let waiting = read_until(&app, id, |r| r.state == State::WAITING).await;
    assert!(!waiting.waits_for_user(), "bob is back");
    assert_eq!(
      waiting.attempts, 1,
      "the try bob was gone for does not count"
    );
  });
}

/// A login has the downloads waiting for a user who was offline try them again, since the
/// session that said so is over.
#[tokio::test]
async fn a_login_has_the_downloads_waiting_for_an_offline_user_try_them_again() {
  let app = crate::testing::soulseek::app(TRUSTED).await;
  let server = crate::testing::soulseek::Server::silent().await;
  let downloads = &app.state.downloads;
  let mut away = downloads.away.follow("bob");
  downloads.away.heard("bob", true);
  assert_eq!(*away.borrow_and_update(), super::away::Heard::Offline);
  server.login(&app).await;
  let back = tokio::time::timeout(
    std::time::Duration::from_secs(5),
    away.wait_for(|heard| *heard == super::away::Heard::Nothing),
  )
  .await;
  assert!(back.is_ok(), "the login left bob offline");
}

/// A user who goes offline while their file moves does not stop it: it arrives.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_file_moving_when_its_user_goes_offline_keeps_moving() {
  use soulseek_proto::peer::TransferResponse;
  use tokio::io::AsyncWriteExt;

  let app = crate::testing::soulseek::app(TRUSTED).await;
  let peers = Peers::start(&app, &["alice"]).await;
  let file = "@@a\\Album\\1.flac";
  let id = enqueued(&app, "alice", file).await;
  let mut alice = peers.connected("alice").await;
  alice.asked_for(file).await;
  assert!(matches!(
    alice.offer(file, 1).await,
    TransferResponse::Allowed { token: 1, .. }
  ));
  let mut transfer = sending(&app, "alice", 1, 4).await;
  read_until(&app, id, |r| r.state == State::InProgress).await;
  peers.presence("alice", false);
  tokio::time::sleep(std::time::Duration::from_secs(1)).await;
  let moving = read(&app, id).await;
  assert_eq!(moving.state, State::InProgress, "stopped though it moves");
  transfer.write_all(&[7; 6]).await.unwrap();
  read_until(&app, id, |r| {
    r.state == State::Completed(TransferOutcome::Succeeded)
  })
  .await;
}

/// A file whose transfer breaks because its user logged off, before the server says so, does
/// not count the try: the server is asked, and the download waits for them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_file_cut_off_by_its_user_logging_off_waits_for_them_without_counting() {
  use soulseek_proto::peer::TransferResponse;

  let config = format!("[transfers.download.retry]\ndelay = \"1h\"\nmax_delay = \"1h\"\n{TRUSTED}");
  let app = crate::testing::soulseek::app(&config).await;
  let peers = Peers::start(&app, &["alice"]).await;
  let file = "@@a\\Album\\1.flac";
  let id = enqueued(&app, "alice", file).await;
  let mut alice = peers.connected("alice").await;
  alice.asked_for(file).await;
  assert!(matches!(
    alice.offer(file, 1).await,
    TransferResponse::Allowed { token: 1, .. }
  ));
  let transfer = sending(&app, "alice", 1, 4).await;
  read_until(&app, id, |r| r.state == State::InProgress).await;
  reseek_shares::lock(&peers.online).remove("alice");
  drop(transfer);
  let waiting = read_until(&app, id, Record::waits_for_user).await;
  assert_eq!(waiting.attempts, 1, "the try cut off does not count");
}

/// With two download slots and five idle users, each offering a file as it is asked for: two
/// are asked and move, and the other three wait here, not asked, until a slot is free. A file
/// asked for holds its slot until it is offered, not merely once its request is written.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idle_users_get_no_more_requests_than_there_are_slots() {
  use soulseek_proto::peer::TransferResponse;

  let config = format!("[transfers.download]\nslots = 2\n{TRUSTED}");
  let app = crate::testing::soulseek::app(&config).await;
  let names = ["u1", "u2", "u3", "u4", "u5"];
  let peers = Peers::start(&app, &names).await;
  let mut ids = Vec::new();
  let mut sides = Vec::new();
  for name in names {
    ids.push(enqueued(&app, name, &format!("@@{name}\\1.flac")).await);
    sides.push(peers.connected(name).await);
  }
  let idle = std::time::Duration::from_secs(1);
  let mut asked = Vec::new();
  for (n, side) in sides.iter_mut().enumerate() {
    if let Some(soulseek_proto::PeerMessage::QueueDownload(_)) = side.next_within(idle).await {
      asked.push(n);
    }
  }
  assert_eq!(asked, [0, 1], "two asked in the two slots");
  for n in [0, 1] {
    let file = format!("@@{}\\1.flac", names[n]);
    let token = n as u32 + 1;
    assert!(matches!(
      sides[n].offer(&file, token).await,
      TransferResponse::Allowed { .. }
    ));
    read_until(&app, ids[n], |r| r.state == State::Initializing).await;
  }
  for side in &mut sides[2..] {
    let early = side.next_within(idle).await;
    assert!(early.is_none(), "asked while two move: {early:?}");
  }
  app
    .state
    .downloads
    .cancel(names[0], ids[0], false)
    .await
    .unwrap();
  sides[2].asked_for("@@u3\\1.flac").await;
  let next = sides[3].next_within(idle).await;
  assert!(next.is_none(), "one asked per free slot: {next:?}");
}

/// With one download slot, a file whose peer neither offers it nor says its place holds the
/// slot for the settle window from its question, and no longer: the next file is asked then.
/// That peer's next file is not waited for again, so the file after it is asked at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_silent_peer_holds_the_asking_slot_for_the_window_only_and_once() {
  use super::attempt::SETTLED_AFTER;

  // peer connections stay open well past the window, so each peer is asked on the one its
  // enqueue made
  let config = format!(
    "[transfers.download]\nslots = 1\n[soulseek.connection.timeout]\ninactivity = \"60s\"\n{TRUSTED}"
  );
  let app = crate::testing::soulseek::app(&config).await;
  let peers = Peers::start(&app, &["alice", "bob"]).await;
  let alices = ["@@a\\1.flac", "@@a\\2.flac"];
  let bobs = "@@b\\1.flac";
  enqueued(&app, "alice", alices[0]).await;
  let mut alice = peers.connected("alice").await;
  alice.asked_for(alices[0]).await;
  let asked = tokio::time::Instant::now();
  enqueued(&app, "alice", alices[1]).await;
  enqueued(&app, "bob", bobs).await;
  let mut bob = peers.connected("bob").await;
  let margin = std::time::Duration::from_secs(2);
  let until = |at: tokio::time::Instant| at.saturating_duration_since(tokio::time::Instant::now());
  let (quiet, closed) = bob.quiet_for(until(asked + SETTLED_AFTER - margin)).await;
  assert!(quiet, "bob asked within alice's window");
  assert!(!closed, "the connection closed in the window");
  alice.asked_for(alices[1]).await;
  let after = asked.elapsed();
  assert!(
    after <= SETTLED_AFTER + margin,
    "asked {after:?} after the first, past the window"
  );
  let second = tokio::time::Instant::now();
  match bob.next_within(margin).await {
    Some(soulseek_proto::PeerMessage::QueueDownload(request)) => {
      assert_eq!(request.filename.as_str(), bobs);
    }
    other => panic!("alice's second file was waited for: {other:?}"),
  }
  assert!(second.elapsed() <= margin);
  let again = tokio::time::timeout(
    std::time::Duration::from_millis(200),
    peers.listeners["bob"].accept(),
  )
  .await;
  assert!(again.is_err(), "bob was reached on a second connection");
}

/// A peer marked silent is waited for again once it says a file's place, to the try's question
/// or to the API's, or offers a file: the next file asked of it gets its place question, which a
/// silent peer's file does not.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_silent_peer_that_answers_or_offers_is_waited_for_again() {
  use soulseek_proto::peer::{PlaceInQueueResponse, TransferResponse};

  let config = format!(
    "[transfers.download]\nslots = 2\n[transfers.download.retry]\ndelay = \"1h\"\n\
     max_delay = \"1h\"\n[soulseek.connection.timeout]\ninactivity = \"60s\"\n{TRUSTED}"
  );
  let app = crate::testing::soulseek::app(&config).await;
  let peers = Peers::start(&app, &["alice"]).await;
  let away = &app.state.downloads.away;
  // as if an earlier file's window had passed unanswered
  let mark = || assert!(away.went_silent("alice", tokio::time::Instant::now()));
  let files = ["@@a\\1.flac", "@@a\\2.flac", "@@a\\3.flac", "@@a\\4.flac"];

  // a place said to the try's question
  let first = enqueued(&app, "alice", files[0]).await;
  let mut alice = peers.connected("alice").await;
  alice.asked_for(files[0]).await;
  alice.asked_place(files[0]).await;
  mark();
  let placed = PlaceInQueueResponse {
    filename: files[0].into(),
    place: 4,
  };
  alice
    .send(soulseek_proto::PeerMessage::PlaceInQueueResponse(placed))
    .await;
  read_until(&app, first, |r| r.place_in_queue() == Some(4)).await;
  let second = enqueued(&app, "alice", files[1]).await;
  alice.asked_for(files[1]).await;
  alice.place_asked(files[1], 5).await;
  read_until(&app, second, |r| r.place_in_queue() == Some(5)).await;

  // a place said to the API's question
  mark();
  let position = format!("/api/v0/transfers/downloads/alice/{second}/position");
  let ((status, _), ()) = tokio::join!(
    app.call("GET", &position, None),
    alice.place_asked(files[1], 7)
  );
  assert_eq!(status, 200);
  let third = enqueued(&app, "alice", files[2]).await;
  alice.asked_for(files[2]).await;
  alice.place_asked(files[2], 6).await;
  read_until(&app, third, |r| r.place_in_queue() == Some(6)).await;

  // an offer
  mark();
  assert!(matches!(
    alice.offer(files[0], 1).await,
    TransferResponse::Allowed { token: 1, .. }
  ));
  read_until(&app, first, |r| r.state == State::Initializing).await;
  enqueued(&app, "alice", files[3]).await;
  alice.asked_for(files[3]).await;
  alice.place_asked(files[3], 8).await;
}

/// A peer is marked silent only for a place question written and left unanswered with no word
/// from it in the window: alice answers seven seconds late, after the question's own wait but
/// inside the window, and bob's connection closes under the question, so neither is marked and
/// their next files get their place questions.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_late_answer_or_a_question_never_answered_for_a_close_leaves_no_mark() {
  use super::attempt::SETTLED_AFTER;

  let config = format!(
    "[transfers.download]\nslots = 2\n[transfers.download.retry]\ndelay = \"1h\"\n\
     max_delay = \"1h\"\n[soulseek.connection.timeout]\ninactivity = \"60s\"\n{TRUSTED}"
  );
  let app = crate::testing::soulseek::app(&config).await;
  let peers = Peers::start(&app, &["alice", "bob"]).await;
  let (alices, bobs) = (
    ["@@a\\1.flac", "@@a\\2.flac"],
    ["@@b\\1.flac", "@@b\\2.flac"],
  );
  enqueued(&app, "alice", alices[0]).await;
  let mut alice = peers.connected("alice").await;
  alice.asked_for(alices[0]).await;
  alice.asked_place(alices[0]).await;
  let asked = tokio::time::Instant::now();
  enqueued(&app, "bob", bobs[0]).await;
  let mut bob = peers.connected("bob").await;
  bob.asked_for(bobs[0]).await;
  bob.asked_place(bobs[0]).await;
  drop(bob);
  // both slots are held by the first files' windows, so the second files wait for them
  enqueued(&app, "alice", alices[1]).await;
  enqueued(&app, "bob", bobs[1]).await;
  // past the question's own wait of five seconds, inside the window
  tokio::time::sleep_until(asked + std::time::Duration::from_secs(7)).await;
  assert!(asked.elapsed() < SETTLED_AFTER);
  let late = soulseek_proto::peer::PlaceInQueueResponse {
    filename: alices[0].into(),
    place: 4,
  };
  alice
    .send(soulseek_proto::PeerMessage::PlaceInQueueResponse(late))
    .await;
  alice.asked_for(alices[1]).await;
  alice.asked_place(alices[1]).await;
  let mut bob = peers.connected("bob").await;
  bob.asked_for(bobs[1]).await;
  bob.asked_place(bobs[1]).await;
}

/// A user the server says does not exist ends their downloads, failed with a reason that says
/// so, and is said once, not every time their status would be asked.
#[tokio::test]
async fn the_downloads_of_a_user_who_does_not_exist_end_failed() {
  let config =
    format!("[transfers.download.retry]\ndelay = \"1s\"\nmax_delay = \"30s\"\n{TRUSTED}");
  let app = crate::testing::soulseek::app(&config).await;
  let (logs, _capturing) = crate::testing::captured_logs();
  let _peers = Peers::start(&app, &["alice"]).await;
  let mut ids = Vec::new();
  for track in ["1", "2"] {
    let mut held = Record::download("ghost", &format!("@@g\\{track}.flac"), 10);
    held.paused = true;
    insert(&app, &held).await;
    ids.push(held.id);
  }
  let resume = json!({ "ids": ids, "action": "resume" });
  app
    .call("POST", "/api/v0/transfers/downloads/bulk", Some(resume))
    .await;
  for id in &ids {
    let ended = read_until(&app, *id, Record::is_completed).await;
    assert_eq!(ended.state, State::Completed(TransferOutcome::Errored));
    assert_eq!(ended.exception.as_deref(), Some("The user doesn't exist"));
  }
  tokio::time::sleep(std::time::Duration::from_secs(2)).await;
  let said: Vec<_> = logs
    .snapshot()
    .into_iter()
    .filter(|line| line.message.contains("ghost") && line.message.contains("not"))
    .filter(|line| line.message.contains("exist") || line.message.contains("watched"))
    .collect();
  assert_eq!(said.len(), 1, "{said:?}");
  assert_eq!(said[0].level, api_types::logs::LogLevel::Information);
}

/// A file sent unasked holds a slot while it moves, as any download does: with one slot,
/// nothing else is asked for meanwhile.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_file_sent_unasked_takes_a_slot_while_it_moves() {
  use soulseek_proto::init::PeerInit;
  use soulseek_proto::peer::{TransferRequest, TransferResponse};
  use soulseek_proto::{InitMessage, PeerConnectionType};
  use tokio::io::AsyncWriteExt;

  let config = format!(
    "[transfers.download]\nslots = 1\n[transfers.download.received]\nfrom = [\"default\"]\n{TRUSTED}"
  );
  let app = crate::testing::soulseek::app(&config).await;
  let peers = Peers::start(&app, &["alice", "bob"]).await;
  // alice reaches us and offers a file we did not ask for
  let port = app.state.config.current().values.soulseek.listen_port;
  let mut alice = Peer {
    stream: tokio::net::TcpStream::connect(("127.0.0.1", port))
      .await
      .unwrap(),
    buffer: Vec::new(),
  };
  let init = InitMessage::PeerInit(PeerInit {
    username: "alice".into(),
    connection_type: PeerConnectionType::Peer,
    token: 0,
  });
  alice.stream.write_all(&init.encode()).await.unwrap();
  let gift = "@@a\\gift.flac";
  alice
    .send(soulseek_proto::PeerMessage::TransferRequest(
      TransferRequest {
        direction: soulseek_proto::TransferDirection::Upload,
        token: 9,
        filename: gift.into(),
        file_size: Some(10),
      },
    ))
    .await;
  assert!(matches!(
    alice.next().await,
    soulseek_proto::PeerMessage::TransferResponse(TransferResponse::Allowed { token: 9, .. })
  ));
  let _moving = sending(&app, "alice", 9, 4).await;

  let bobs = "@@b\\1.flac";
  enqueued(&app, "bob", bobs).await;
  let mut bob = peers.connected("bob").await;
  let early = bob.next_within(std::time::Duration::from_secs(1)).await;
  assert!(early.is_none(), "asked while the gift moves: {early:?}");
}

/// Downloads of one offline user waiting for them share one status ask per period.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_downloads_waiting_for_one_user_ask_their_status_once_a_period() {
  let config =
    format!("[transfers.download.retry]\ndelay = \"2s\"\nmax_delay = \"30s\"\n{TRUSTED}");
  let app = crate::testing::soulseek::app(&config).await;
  let mut peers = Peers::start(&app, &["bob"]).await;
  reseek_shares::lock(&peers.online).remove("bob");
  let mut ids = Vec::new();
  for track in ["1", "2", "3", "4"] {
    let mut held = Record::download("bob", &format!("@@b\\{track}.flac"), 10);
    held.paused = true;
    insert(&app, &held).await;
    ids.push(held.id);
  }
  let resume = json!({ "ids": ids, "action": "resume" });
  app
    .call("POST", "/api/v0/transfers/downloads/bulk", Some(resume))
    .await;
  for id in &ids {
    read_until(&app, *id, Record::waits_for_user).await;
  }
  // each waits its first period, under a second of jitter apart
  let mut asked = 0;
  let until = tokio::time::Instant::now() + std::time::Duration::from_millis(3500);
  loop {
    let left = until.saturating_duration_since(tokio::time::Instant::now());
    let status = |request: &soulseek_proto::ServerRequest| matches!(request, soulseek_proto::ServerRequest::GetStatus(asked) if asked.username == "bob");
    if left.is_zero() || !peers.server.receives(left, status).await {
      break;
    }
    asked += 1;
  }
  assert!(asked <= 2, "{asked} status asks for one user");
}

/// With one download slot: files waiting in their users' queues hold none, so alice's and bob's
/// are both asked; an offer is accepted though the slot is taken, so both move; and a file
/// enqueued meanwhile is not asked until neither moves.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_offer_is_taken_past_the_slots_and_nothing_new_is_asked_until_one_is_free() {
  use soulseek_proto::peer::TransferResponse;

  let config = format!("[transfers.download]\nslots = 1\n{TRUSTED}");
  let app = crate::testing::soulseek::app(&config).await;
  let peers = Peers::start(&app, &["alice", "bob", "carol"]).await;
  let (alices, bobs, carols) = (
    "@@a\\Album\\1.flac",
    "@@b\\Album\\1.flac",
    "@@c\\Album\\1.flac",
  );
  let first = enqueued(&app, "alice", alices).await;
  let mut alice = peers.connected("alice").await;
  alice.asked_for(alices).await;
  // alice has it queued, so its ask is over and the slot free to ask in
  alice.place_asked(alices, 3).await;
  let second = enqueued(&app, "bob", bobs).await;
  let mut bob = peers.connected("bob").await;
  bob.asked_for(bobs).await;

  assert!(matches!(
    alice.offer(alices, 1).await,
    TransferResponse::Allowed { token: 1, .. }
  ));
  read_until(&app, first, |r| r.state == State::Initializing).await;
  assert!(matches!(
    bob.offer(bobs, 2).await,
    TransferResponse::Allowed { token: 2, .. }
  ));
  read_until(&app, second, |r| r.state == State::Initializing).await;

  enqueued(&app, "carol", carols).await;
  let mut carol = peers.connected("carol").await;
  let idle = std::time::Duration::from_secs(1);
  let early = carol.next_within(idle).await;
  assert!(early.is_none(), "asked while two move: {early:?}");
  let downloads = &app.state.downloads;
  downloads.cancel("alice", first, false).await.unwrap();
  read_until(&app, first, Record::is_completed).await;
  let early = carol.next_within(idle).await;
  assert!(early.is_none(), "asked while one moves: {early:?}");
  downloads.cancel("bob", second, false).await.unwrap();
  carol.asked_for(carols).await;
}

#[tokio::test]
async fn a_download_whose_row_cannot_be_read_is_taken_out_by_a_cancel_that_removes_it() {
  let app = app(TRUSTED, &[]).await;
  let odd = Record::download("alice", "@@a\\Album\\2.flac", 10);
  insert(&app, &odd).await;
  let id = odd.id.to_string();
  let row = id.clone();
  app
    .state
    .transfers
    .write(move |conn| conn.execute("UPDATE Transfers SET State = 2 WHERE Id = ?1", [row]))
    .await
    .unwrap();
  let uri = format!("/api/v0/transfers/downloads/alice/{id}?remove=true");
  let (status, body) = app.call("DELETE", &uri, None).await;
  assert_eq!(status, 204, "{body}");
  let removed: bool = app
    .state
    .transfers
    .read(move |conn| {
      conn.query_row("SELECT Removed FROM Transfers WHERE Id = ?1", [id], |row| {
        row.get(0)
      })
    })
    .await
    .unwrap();
  assert!(removed);
}

/// Writes an interrupted download from `username` in a listed job, as a stop leaves it;
/// returns the job and the download.
async fn interrupted_from(app: &TestApp, username: &str, filename: &str) -> (Uuid, Uuid) {
  let job = Uuid::new_v4();
  listed_job(app, job, &format!("Album from {username}")).await;
  let mut interrupted = Record::download(username, filename, 10);
  interrupted.job_id = Some(job);
  insert(app, &interrupted).await;
  (job, interrupted.id)
}

/// [`interrupted_from`] alice.
async fn interrupted_in_a_job(app: &TestApp, filename: &str) -> (Uuid, Uuid) {
  interrupted_from(app, "alice", filename).await
}

/// The job `job` as the jobs list has it: its state and how it ended.
async fn job_now(
  app: &TestApp,
  job: Uuid,
) -> (
  api_types::jobs::JobState,
  Option<api_types::jobs::JobEnding>,
) {
  let listed = app.state.jobs.get(job).await.unwrap().unwrap();
  (listed.state, listed.ending)
}

#[tokio::test]
async fn a_job_waiting_for_a_login_ends_canceled_once_its_downloads_are_removed() {
  use api_types::jobs::{JobEnding, JobState};
  let app = app(TRUSTED, &[]).await;
  let (gone, id) = interrupted_in_a_job(&app, "@@a\\Album\\1.flac").await;
  let (kept, _) = interrupted_in_a_job(&app, "@@a\\Other\\1.flac").await;
  crate::transfers::cleanup::end_unfinished(&app.state.transfers)
    .await
    .unwrap();
  // the daemon is not connected, so both wait for a login
  app.state.downloads.resume().await;
  assert_eq!(job_now(&app, gone).await, (JobState::Running, None));
  assert_eq!(job_now(&app, kept).await, (JobState::Running, None));

  let uri = format!("/api/v0/transfers/downloads/alice/{id}?remove=true");
  let (status, _) = app.call("DELETE", &uri, None).await;
  assert_eq!(status, 204);
  assert_eq!(
    job_now(&app, gone).await,
    (JobState::Ended, Some(JobEnding::Canceled)),
    "every download of it was removed"
  );
  app.state.downloads.resume().await;
  assert_eq!(job_now(&app, kept).await, (JobState::Running, None));
}

#[tokio::test]
async fn canceling_a_job_waiting_for_a_login_ends_it_canceled_and_no_login_enqueues_its_files() {
  use api_types::jobs::{JobEnding, JobState};
  let app = app(TRUSTED, &[]).await;
  let (job, _) = interrupted_in_a_job(&app, "@@a\\Album\\1.flac").await;
  crate::transfers::cleanup::end_unfinished(&app.state.transfers)
    .await
    .unwrap();
  app.state.downloads.resume().await;
  let (status, _) = app
    .call("POST", &format!("/api/v0/jobs/{job}/cancel"), None)
    .await;
  assert_eq!(status, 204);
  assert_eq!(
    job_now(&app, job).await,
    (JobState::Ended, Some(JobEnding::Canceled))
  );
  let left = crate::transfers::cleanup::take_resumable(&app.state.transfers)
    .await
    .unwrap();
  assert!(left.is_empty(), "{left:?}");
}

#[tokio::test]
async fn a_job_canceled_after_a_restart_before_any_login_is_not_enqueued_by_the_login() {
  use api_types::jobs::{JobEnding, JobState};
  let app = crate::testing::soulseek::app(TRUSTED).await;
  let server = crate::testing::soulseek::Server::silent().await;
  let (job, _) = interrupted_in_a_job(&app, "@@a\\Album\\1.flac").await;
  // the start ends what the last run left; no login has resumed anything yet
  crate::transfers::cleanup::end_unfinished(&app.state.transfers)
    .await
    .unwrap();
  let (status, _) = app
    .call("POST", &format!("/api/v0/jobs/{job}/cancel"), None)
    .await;
  assert_eq!(status, 204);
  assert_eq!(
    job_now(&app, job).await,
    (JobState::Ended, Some(JobEnding::Canceled))
  );

  server.login(&app).await;
  let downloads = &app.state.downloads;
  let resumed = tokio::time::timeout(std::time::Duration::from_secs(10), async {
    while !*downloads.resuming.lock().await {
      tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
  })
  .await;
  assert!(resumed.is_ok(), "the login's resume never ran");
  assert!(!unfinished(&app, "@@a\\Album\\1.flac").await);
}

#[tokio::test]
async fn a_purge_stops_and_removes_every_transfer_of_the_user_and_leaves_the_others() {
  let config = format!("[transfers.download.retry]\ndelay = \"1h\"\nmax_delay = \"1h\"\n{TRUSTED}");
  let app = app(&config, &[]).await;
  app
    .state
    .users
    .set_buddy("alice", Default::default())
    .await
    .unwrap();
  // one whose task tries alice, who cannot be reached, and waits to try again
  let mut running = Record::download("alice", "@@a\\Album\\1.flac", 10);
  running.paused = true;
  insert(&app, &running).await;
  let resume = json!({ "ids": [running.id], "action": "resume" });
  app
    .call("POST", "/api/v0/transfers/downloads/bulk", Some(resume))
    .await;
  read_until(&app, running.id, |r| r.next_attempt_at().is_some()).await;
  let mut paused = Record::download("alice", "@@a\\Album\\2.flac", 10);
  paused.paused = true;
  let queued_upload = Record::upload("alice", "@@shared\\x.flac", 10);
  let mut bobs = Record::download("bob", "@@b\\1.flac", 10);
  bobs.paused = true;
  for record in [
    &paused,
    &queued_upload,
    &done("alice", "@@a\\Album\\3.flac"),
    &bobs,
  ] {
    insert(&app, record).await;
  }

  let (status, _) = app
    .call("DELETE", "/api/v0/users/alice?purge=true", None)
    .await;
  assert_eq!(status, 204);
  let listed = |username: &'static str| {
    app.state.transfers.read(move |conn| {
      conn.query_row(
        "SELECT COUNT(*) FROM Transfers WHERE Username = ?1 AND Removed = 0",
        [username],
        |row| row.get::<_, i64>(0),
      )
    })
  };
  let gone = tokio::time::timeout(std::time::Duration::from_secs(5), async {
    while listed("alice").await.unwrap() > 0
      || reseek_shares::lock(&app.state.downloads.in_flight).contains_key(&running.id)
    {
      tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
  })
  .await;
  assert!(
    gone.is_ok(),
    "a transfer of alice is still listed or running"
  );
  // canceled, not merely taken out of the listings
  for id in [running.id, paused.id, queued_upload.id] {
    assert_eq!(
      read(&app, id).await.state,
      State::Completed(TransferOutcome::Cancelled),
      "{id}"
    );
  }
  assert_eq!(listed("bob").await.unwrap(), 1, "bob's download stays");
}

/// Starts a download of alice's whose task waits to try her again; returns it.
async fn waiting_to_retry(app: &TestApp) -> Uuid {
  app
    .state
    .users
    .set_buddy("alice", Default::default())
    .await
    .unwrap();
  let mut held = Record::download("alice", "@@a\\Album\\1.flac", 10);
  held.paused = true;
  insert(app, &held).await;
  let resume = json!({ "ids": [held.id], "action": "resume" });
  app
    .call("POST", "/api/v0/transfers/downloads/bulk", Some(resume))
    .await;
  read_until(app, held.id, |r| r.next_attempt_at().is_some()).await;
  held.id
}

/// [`waiting_to_retry`], then paused through the engine, which stops its task; returns it
/// before the task has written it paused, or after.
async fn paused_while_running(app: &TestApp) -> Uuid {
  let id = waiting_to_retry(app).await;
  let paused = app.state.downloads.pause(vec![id]).await;
  assert!(matches!(paused[..], [Ok(_)]), "{paused:?}");
  id
}

/// Waits for the download `id` to let go of its task, then says it ended canceled, out of the
/// listings when `removed`, and not paused.
async fn ended_canceled(app: &TestApp, id: Uuid, removed: bool) {
  let stopped = tokio::time::timeout(std::time::Duration::from_secs(5), async {
    while reseek_shares::lock(&app.state.downloads.in_flight).contains_key(&id) {
      tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
  })
  .await;
  assert!(stopped.is_ok(), "the download's task never ended");
  let ended = read(app, id).await;
  assert_eq!(
    (ended.state.kind(), ended.removed, ended.paused),
    (
      TransferStates::Completed(TransferOutcome::Cancelled),
      removed,
      false
    )
  );
}

/// [`ended_canceled`], taken out of the listings.
async fn ended_canceled_and_removed(app: &TestApp, id: Uuid) {
  ended_canceled(app, id, true).await;
}

#[tokio::test]
async fn a_purge_ends_and_removes_a_download_being_paused() {
  let config = format!("[transfers.download.retry]\ndelay = \"1h\"\nmax_delay = \"1h\"\n{TRUSTED}");
  let app = app(&config, &[]).await;
  let id = paused_while_running(&app).await;
  let (status, _) = app
    .call("DELETE", "/api/v0/users/alice?purge=true", None)
    .await;
  assert_eq!(status, 204);
  ended_canceled_and_removed(&app, id).await;
}

#[tokio::test]
async fn a_cancel_with_removal_ends_and_removes_a_download_being_paused() {
  let config = format!("[transfers.download.retry]\ndelay = \"1h\"\nmax_delay = \"1h\"\n{TRUSTED}");
  let app = app(&config, &[]).await;
  let id = paused_while_running(&app).await;
  let uri = format!("/api/v0/transfers/downloads/alice/{id}?remove=true");
  let (status, _) = app.call("DELETE", &uri, None).await;
  assert_eq!(status, 204);
  ended_canceled_and_removed(&app, id).await;
}

/// Writes an interrupted download from `username` in a job and resumes it while logged out,
/// so it waits for a login; returns the job and the download.
async fn waiting_for_a_login(app: &TestApp, username: &str, filename: &str) -> (Uuid, Uuid) {
  let waiting = interrupted_from(app, username, filename).await;
  crate::transfers::cleanup::end_unfinished(&app.state.transfers)
    .await
    .unwrap();
  app.state.downloads.resume().await;
  assert_eq!(
    job_now(app, waiting.0).await,
    (api_types::jobs::JobState::Running, None)
  );
  waiting
}

/// What the jobs list says of a job whose every download was taken out.
const REMOVED_ALL: (
  api_types::jobs::JobState,
  Option<api_types::jobs::JobEnding>,
) = (
  api_types::jobs::JobState::Ended,
  Some(api_types::jobs::JobEnding::Canceled),
);

#[tokio::test]
async fn clearing_the_completed_downloads_ends_a_job_waiting_for_a_login_canceled() {
  let app = app(TRUSTED, &[]).await;
  let (job, _) = waiting_for_a_login(&app, "alice", "@@a\\Album\\1.flac").await;
  let (status, _) = app
    .call("DELETE", "/api/v0/transfers/downloads/all/completed", None)
    .await;
  assert_eq!(status, 204);
  assert_eq!(job_now(&app, job).await, REMOVED_ALL);
}

#[tokio::test]
async fn a_purge_ends_the_users_waiting_job_canceled_and_leaves_another_users_waiting() {
  let app = app(TRUSTED, &[]).await;
  let (bobs, _) = interrupted_from(&app, "bob", "@@b\\Album\\1.flac").await;
  let (alices, _) = waiting_for_a_login(&app, "alice", "@@a\\Album\\1.flac").await;
  app
    .state
    .users
    .set_buddy("alice", Default::default())
    .await
    .unwrap();
  let (status, _) = app
    .call("DELETE", "/api/v0/users/alice?purge=true", None)
    .await;
  assert_eq!(status, 204);
  assert_eq!(job_now(&app, alices).await, REMOVED_ALL);
  assert_eq!(
    job_now(&app, bobs).await,
    (api_types::jobs::JobState::Running, None)
  );
}

#[tokio::test]
async fn the_retention_sweep_ends_a_job_waiting_for_a_login_canceled() {
  let config = format!("[retention.transfers.download]\nerrored = \"5m\"\n{TRUSTED}");
  let app = app(&config, &[]).await;
  let (job, id) = waiting_for_a_login(&app, "alice", "@@a\\Album\\1.flac").await;
  let row = id.to_string();
  app
    .state
    .transfers
    .write(move |conn| {
      let long_ago = crate::db::timestamp(chrono::Utc::now() - chrono::Duration::minutes(10));
      conn.execute(
        "UPDATE Transfers SET EndedAt = ?2 WHERE Id = ?1",
        rusqlite::params![row, long_ago],
      )
    })
    .await
    .unwrap();
  crate::jobs::maintenance::cleanup(&app.state, api_types::jobs::JobStarter::you(), false).await;
  assert!(read(&app, id).await.removed, "past its retention");
  assert_eq!(job_now(&app, job).await, REMOVED_ALL);
}

#[tokio::test]
async fn a_file_enqueued_again_in_another_job_ends_the_job_it_waited_in_canceled() {
  let app = app(TRUSTED, &[]).await;
  let filename = "@@a\\Album\\1.flac";
  let (waited_in, _) = waiting_for_a_login(&app, "alice", filename).await;
  // whole here now, so the new job needs nothing of alice, who cannot be reached
  let downloads = &app.state.downloads;
  let (incomplete, _) = super::incomplete_path(
    &downloads.incomplete,
    "alice",
    filename,
    crate::safety::Os::CURRENT,
  )
  .unwrap();
  std::fs::create_dir_all(incomplete.parent().unwrap()).unwrap();
  std::fs::write(&incomplete, [0; 10]).unwrap();
  let again = super::Group {
    username: "alice".into(),
    files: vec![super::FileRequest {
      filename: filename.into(),
      size: 10,
      position: None,
    }],
    destination: None,
    folder: None,
    search_id: None,
  };
  let results = downloads.enqueue(Uuid::new_v4(), vec![again], false).await;
  assert_eq!(results[0].as_ref().unwrap().enqueued.len(), 1);
  assert_eq!(job_now(&app, waited_in).await, REMOVED_ALL);
}

#[tokio::test]
async fn a_purge_of_a_user_forgotten_earlier_removes_their_transfers_and_answers_done() {
  let app = app(TRUSTED, &[]).await;
  app
    .state
    .users
    .set_buddy("alice", Default::default())
    .await
    .unwrap();
  let finished = done("alice", "@@a\\1.flac");
  insert(&app, &finished).await;
  let (status, _) = app.call("DELETE", "/api/v0/users/alice", None).await;
  assert_eq!(status, 204);
  assert!(
    !read(&app, finished.id).await.removed,
    "kept by a forget without purge"
  );
  let (status, _) = app
    .call("DELETE", "/api/v0/users/alice?purge=true", None)
    .await;
  assert_eq!(status, 204);
  assert!(read(&app, finished.id).await.removed);
  let (status, _) = app
    .call("DELETE", "/api/v0/users/alice?purge=true", None)
    .await;
  assert_eq!(status, 404, "nothing of alice is left");
}

/// Cancels a download whose task waits to try again, with `remove`, then pauses it before the
/// task has ended: the pause is refused and the download ends canceled.
async fn cancel_then_pause(remove: bool) {
  let config = format!("[transfers.download.retry]\ndelay = \"1h\"\nmax_delay = \"1h\"\n{TRUSTED}");
  let app = app(&config, &[]).await;
  let id = waiting_to_retry(&app).await;
  let downloads = &app.state.downloads;
  assert!(downloads.cancel("alice", id, remove).await.unwrap());
  let paused = downloads.pause(vec![id]).await;
  assert!(matches!(paused[..], [Err(_)]), "{paused:?}");
  ended_canceled(&app, id, remove).await;
}

#[tokio::test]
async fn a_pause_after_a_cancel_with_removal_is_refused_and_the_download_ends_removed() {
  cancel_then_pause(true).await;
}

#[tokio::test]
async fn a_pause_after_a_cancel_is_refused_and_the_download_ends_canceled() {
  cancel_then_pause(false).await;
}

#[tokio::test]
async fn a_bulk_pause_during_a_purge_leaves_the_download_ended_and_removed() {
  let config = format!("[transfers.download.retry]\ndelay = \"1h\"\nmax_delay = \"1h\"\n{TRUSTED}");
  let app = app(&config, &[]).await;
  let id = waiting_to_retry(&app).await;
  app.state.downloads.purge("alice").await.unwrap();
  let pause = json!({ "ids": [id], "action": "pause" });
  let (status, answer) = app
    .call("POST", "/api/v0/transfers/downloads/bulk", Some(pause))
    .await;
  assert_eq!(status, 200);
  assert_eq!(answer["ok"], json!([]), "{answer}");
  ended_canceled_and_removed(&app, id).await;
}

#[tokio::test]
async fn a_paused_download_canceled_is_pushed_as_no_longer_paused() {
  let app = app(TRUSTED, &[]).await;
  let mut paused = Record::download("alice", "@@a\\1.flac", 10);
  paused.paused = true;
  insert(&app, &paused).await;
  let mut stream = app
    .stream_from(
      "127.0.0.1:1",
      request("GET", "/api/v0/live?topics=transfers", None),
    )
    .await;
  assert_eq!(stream.next().await.unwrap().event, "hello");
  let uri = format!("/api/v0/transfers/downloads/alice/{}", paused.id);
  let (status, _) = app.call("DELETE", &uri, None).await;
  assert_eq!(status, 204);
  let updated = stream.next().await.unwrap();
  assert_eq!(updated.event, "transfers.updated");
  assert_eq!(
    (&updated.data["state"], &updated.data["paused"]),
    (&json!("Completed, Cancelled"), &json!(false))
  );
  assert!(!read(&app, paused.id).await.paused);
}
