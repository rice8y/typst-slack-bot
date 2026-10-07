use tokio::sync::oneshot;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::timeout;

use super::*;

const HTTP_TIMEOUT: Duration = Duration::from_secs(3);
const RENDER_TIMEOUT: Duration = Duration::from_secs(8);
const CHANNEL: &str = "C-render-test";
const PARENT_TS: &str = "100.001";
const REPLY_TS: &str = "100.003";

enum MockReply {
	Json(Value),
	UploadUrl(usize),
	UploadFailed,
}

struct MockStep {
	path: String,
	reply: MockReply,
}

impl MockStep {
	fn json(path: &str, body: Value) -> Self {
		Self {
			path: path.to_owned(),
			reply: MockReply::Json(body),
		}
	}
}

struct RecordedRequest {
	path: String,
	expected_path: Option<String>,
	method: Method,
	headers: hyper::HeaderMap,
	body: Bytes,
}

impl RecordedRequest {
	fn json(&self) -> Value {
		serde_json::from_slice(&self.body).expect("mock request should contain JSON")
	}

	fn form(&self) -> Vec<(String, String)> {
		let body = std::str::from_utf8(&self.body).expect("mock request should contain UTF-8");
		reqwest::Url::parse(&format!("http://mock.invalid/?{body}"))
			.unwrap()
			.query_pairs()
			.into_owned()
			.collect()
	}
}

struct MockState {
	steps: VecDeque<MockStep>,
	requests: Vec<RecordedRequest>,
}

struct MockSlack {
	client: SlackClient,
	state: Arc<Mutex<MockState>>,
	shutdown: Option<oneshot::Sender<()>>,
	server: Option<JoinHandle<()>>,
}

impl MockSlack {
	async fn start(steps: Vec<MockStep>) -> Self {
		let listener = timeout(HTTP_TIMEOUT, TcpListener::bind("127.0.0.1:0"))
			.await
			.unwrap()
			.unwrap();
		let base = format!("http://{}", listener.local_addr().unwrap());
		let client = SlackClient {
			token: Arc::from("test-token"),
			client: Client::builder()
				.no_proxy()
				.timeout(HTTP_TIMEOUT)
				.build()
				.unwrap(),
			api_base: Arc::from(format!("{base}/api")),
		};
		let state = Arc::new(Mutex::new(MockState {
			steps: steps.into(),
			requests: Vec::new(),
		}));
		let server_state = Arc::clone(&state);
		let (shutdown, mut stopped) = oneshot::channel();
		let server = tokio::spawn(async move {
			let mut connections = JoinSet::new();
			loop {
				tokio::select! {
					_ = &mut stopped => break,
					accepted = timeout(HTTP_TIMEOUT, listener.accept()) => {
						let Ok(Ok((stream, _))) = accepted else { break };
						let state = Arc::clone(&server_state);
						let base = base.clone();
						connections.spawn(async move {
							let service = service_fn(move |request| {
								handle_request(request, Arc::clone(&state), base.clone())
							});
							let _ = timeout(
								HTTP_TIMEOUT,
								http1::Builder::new().serve_connection(TokioIo::new(stream), service),
							).await;
						});
					}
					_ = connections.join_next(), if !connections.is_empty() => {}
				}
			}
			connections.abort_all();
			while connections.join_next().await.is_some() {}
		});
		Self {
			client,
			state,
			shutdown: Some(shutdown),
			server: Some(server),
		}
	}

	async fn render(
		&self,
		target: &SlackTarget,
		rendered: protocol::Rendered,
	) -> Result<(), BotError> {
		timeout(RENDER_TIMEOUT, self.client.post_rendered(target, rendered))
			.await
			.map_err(|error| -> BotError { error.into() })?
	}

	async fn finish(mut self) -> Vec<RecordedRequest> {
		let _ = self.shutdown.take().unwrap().send(());
		let mut server = self.server.take().unwrap();
		let joined = timeout(HTTP_TIMEOUT, &mut server).await;
		if joined.is_err() {
			server.abort();
			let _ = timeout(HTTP_TIMEOUT, server).await;
			panic!("mock HTTP server did not shut down in time");
		}
		joined.unwrap().expect("mock HTTP server task panicked");
		let mut state = self.state.lock().await;
		assert!(
			state.steps.is_empty(),
			"not all expected HTTP requests were made"
		);
		let requests = std::mem::take(&mut state.requests);
		for request in &requests {
			assert_eq!(
				Some(&request.path),
				request.expected_path.as_ref(),
				"unexpected HTTP request"
			);
			assert_eq!(request.method, Method::POST);
			if request.path.starts_with("/api/") {
				assert_eq!(request.headers["authorization"], "Bearer test-token");
			}
		}
		requests
	}
}

impl Drop for MockSlack {
	fn drop(&mut self) {
		// Also cancel the listener and its owned connection tasks if a test unwinds.
		if let Some(server) = &self.server {
			server.abort();
		}
	}
}

async fn handle_request(
	request: Request<Incoming>,
	state: Arc<Mutex<MockState>>,
	base: String,
) -> Result<Response<Full<Bytes>>, BotError> {
	let (parts, body) = request.into_parts();
	let body = timeout(HTTP_TIMEOUT, body.collect()).await??.to_bytes();
	let path = parts.uri.path().to_owned();
	let mut state = state.lock().await;
	let step = state.steps.pop_front();
	state.requests.push(RecordedRequest {
		path,
		expected_path: step.as_ref().map(|step| step.path.clone()),
		method: parts.method,
		headers: parts.headers,
		body,
	});
	let (status, body) = match step.map(|step| step.reply) {
		Some(MockReply::Json(body)) => (StatusCode::OK, body),
		Some(MockReply::UploadUrl(page)) => (
			StatusCode::OK,
			json!({
				"ok": true,
				"upload_url": format!("{base}/upload/{page}"),
				"file_id": format!("F{page}"),
			}),
		),
		Some(MockReply::UploadFailed) => (StatusCode::INTERNAL_SERVER_ERROR, json!({"ok": false})),
		None => (
			StatusCode::BAD_REQUEST,
			json!({"ok": false, "error": "unexpected_request"}),
		),
	};
	Ok(
		Response::builder()
			.status(status)
			.header(CONTENT_TYPE, "application/json")
			.body(Full::new(Bytes::from(serde_json::to_vec(&body)?)))?,
	)
}

fn upload_steps(pages: usize) -> Vec<MockStep> {
	(1..=pages)
		.flat_map(|page| {
			[
				MockStep {
					path: "/api/files.getUploadURLExternal".to_owned(),
					reply: MockReply::UploadUrl(page),
				},
				MockStep::json(&format!("/upload/{page}"), json!({"ok": true})),
				MockStep::json("/api/files.completeUploadExternal", json!({"ok": true})),
			]
		})
		.collect()
}

fn broadcast_steps(pages: usize) -> Vec<MockStep> {
	let mut steps = upload_steps(pages);
	steps.push(MockStep::json(
		"/api/chat.postMessage",
		json!({"ok": true, "ts": REPLY_TS}),
	));
	steps.push(MockStep::json("/api/chat.update", json!({"ok": true})));
	steps.push(MockStep::json("/api/chat.update", json!({"ok": true})));
	steps
}

fn target(broadcast: bool, parent: Option<&str>) -> SlackTarget {
	let mut target = SlackTarget::new(
		CHANNEL.to_owned(),
		"100.002".to_owned(),
		parent.map(str::to_owned),
	);
	if broadcast {
		target.reply_broadcast = true;
	}
	target
}

fn rendered(pages: usize, more_pages: usize, warnings: &str) -> protocol::Rendered {
	protocol::Rendered {
		images: (0..pages)
			.map(|page| vec![u8::try_from(page).unwrap(), 1, 2, 3])
			.collect(),
		more_pages,
		warnings: warnings.to_owned(),
	}
}

fn assert_uploads(requests: &[RecordedRequest], private: bool, thread_ts: &str) {
	for (index, page) in requests.chunks_exact(3).enumerate() {
		let number = index + 1;
		assert_eq!(page[0].path, "/api/files.getUploadURLExternal");
		assert_eq!(
			page[0].form(),
			vec![
				("filename".to_owned(), format!("page-{number}.png")),
				("length".to_owned(), "4".to_owned()),
				(
					"alt_txt".to_owned(),
					format!("Typst rendered page page-{number}.png")
				),
			]
		);
		assert_eq!(page[1].path, format!("/upload/{number}"));
		assert_eq!(page[1].headers[CONTENT_TYPE], "image/png");
		assert_eq!(
			page[1].body.as_ref(),
			&[u8::try_from(index).unwrap(), 1, 2, 3]
		);
		assert_eq!(page[2].path, "/api/files.completeUploadExternal");
		let complete = page[2].json();
		let files = json!([{"id": format!("F{number}"), "title": format!("page-{number}.png")}]);
		assert_eq!(complete["files"], files);
		assert!(complete.get("reply_broadcast").is_none());
		if private {
			assert_eq!(complete, json!({"files": files}));
		} else {
			assert_eq!(complete["channel_id"], CHANNEL);
			assert_eq!(complete["thread_ts"], thread_ts);
		}
	}
}

fn assert_broadcast_reply(
	requests: &[RecordedRequest],
	caption: &str,
	thread_ts: &str,
	file_ids: &Value,
) {
	assert_eq!(requests.len(), 3);
	assert_eq!(requests[0].path, "/api/chat.postMessage");
	let posted = requests[0].json();
	assert_eq!(posted["channel"], CHANNEL);
	assert_eq!(posted["thread_ts"], thread_ts);
	assert_eq!(posted["reply_broadcast"], false);
	assert_eq!(posted["text"], caption);
	assert!(posted.get("file_ids").is_none());
	assert_eq!(requests[1].path, "/api/chat.update");
	assert_eq!(
		requests[1].json(),
		json!({
			"channel": CHANNEL,
			"ts": REPLY_TS,
			"file_ids": file_ids,
		})
	);
	assert_eq!(requests[2].path, "/api/chat.update");
	assert_eq!(
		requests[2].json(),
		json!({
			"channel": CHANNEL,
			"ts": REPLY_TS,
			"reply_broadcast": true,
		})
	);
}

#[tokio::test]
async fn broadcast_attaches_two_private_uploads_to_one_reply_with_notes() {
	let mock = MockSlack::start(broadcast_steps(2)).await;
	let result = mock
		.render(
			&target(true, Some(PARENT_TS)),
			rendered(2, 3, "warning: test font"),
		)
		.await;
	let requests = mock.finish().await;
	result.unwrap();
	assert_eq!(requests.len(), 9);
	assert_uploads(&requests[..6], true, PARENT_TS);
	assert_broadcast_reply(
		&requests[6..],
		concat!(
			"Note: 3 more pages ignored\n",
			"Render succeeded with warnings:\n```\nwarning: test font\n```\n",
		),
		PARENT_TS,
		&json!(["F1", "F2"]),
	);
}

#[tokio::test]
async fn broadcast_without_notes_uses_typst_caption_and_source_thread() {
	let mock = MockSlack::start(broadcast_steps(1)).await;
	let result = mock.render(&target(true, None), rendered(1, 0, "")).await;
	let requests = mock.finish().await;
	result.unwrap();
	assert_eq!(requests.len(), 6);
	assert_uploads(&requests[..3], true, "100.002");
	assert_broadcast_reply(&requests[3..], "Typst", "100.002", &json!(["F1"]));
}

#[tokio::test]
async fn default_render_shares_files_in_thread_without_chat_requests() {
	let mock = MockSlack::start(upload_steps(2)).await;
	let target = target(false, Some(PARENT_TS));
	assert!(!target.reply_broadcast);
	let result = mock
		.render(&target, rendered(2, 1, "warning: test font"))
		.await;
	let requests = mock.finish().await;
	result.unwrap();
	assert_eq!(requests.len(), 6);
	assert_uploads(&requests, false, PARENT_TS);
	assert_eq!(
		requests[2].json()["initial_comment"],
		concat!(
			"Note: 1 more page ignored\n",
			"Render succeeded with warnings:\n```\nwarning: test font\n```\n",
		)
	);
	assert!(requests[5].json().get("initial_comment").is_none());
}

#[tokio::test]
async fn empty_render_broadcasts_one_text_reply_without_upload_or_update() {
	let mock = MockSlack::start(vec![MockStep::json(
		"/api/chat.postMessage",
		json!({"ok": true, "ts": REPLY_TS}),
	)])
	.await;
	let result = mock
		.render(
			&target(true, Some(PARENT_TS)),
			rendered(0, 1, "warning: test font"),
		)
		.await;
	let requests = mock.finish().await;
	result.unwrap();
	assert_eq!(requests.len(), 1);
	let message = requests[0].json();
	assert_eq!(message["channel"], CHANNEL);
	assert_eq!(message["thread_ts"], PARENT_TS);
	assert_eq!(message["reply_broadcast"], true);
	assert_eq!(
		message["text"],
		concat!(
			"Note: no pages generated\nNote: 1 more page ignored\n",
			"Render succeeded with warnings:\n```\nwarning: test font\n```\n",
		)
	);
	assert!(message.get("file_ids").is_none());
}

#[tokio::test]
async fn failed_second_upload_never_posts_or_updates_a_reply() {
	for (step, reply, expected_error) in [
		(
			3,
			MockReply::Json(json!({"ok": false, "error": "upload_url_failed"})),
			"upload_url_failed",
		),
		(
			4,
			MockReply::UploadFailed,
			"Slack file upload failed with HTTP 500",
		),
		(
			5,
			MockReply::Json(json!({"ok": false, "error": "complete_failed"})),
			"complete_failed",
		),
	] {
		let mut steps = upload_steps(2);
		steps.truncate(step + 1);
		steps[step].reply = reply;
		let mock = MockSlack::start(steps).await;
		let result = mock
			.render(&target(true, Some(PARENT_TS)), rendered(2, 0, ""))
			.await;
		let requests = mock.finish().await;
		let error = result.unwrap_err().to_string();
		assert!(error.contains(expected_error), "unexpected error: {error}");
		assert_eq!(requests.len(), step + 1);
		assert!(!requests
			.iter()
			.any(|request| request.path.starts_with("/api/chat.")));
	}
}

#[tokio::test]
async fn failed_file_attachment_prevents_broadcast_promotion() {
	let mut steps = broadcast_steps(2);
	steps.truncate(8);
	steps[7].reply = MockReply::Json(json!({"ok": false, "error": "cant_update_message"}));
	let mock = MockSlack::start(steps).await;
	let result = mock
		.render(&target(true, Some(PARENT_TS)), rendered(2, 0, ""))
		.await;
	let requests = mock.finish().await;
	let error = result.unwrap_err().to_string();
	assert!(
		error.contains("chat.update failed: cant_update_message"),
		"unexpected error: {error}"
	);
	assert_eq!(requests.len(), 8);
	assert_uploads(&requests[..6], true, PARENT_TS);
	assert_eq!(requests[6].path, "/api/chat.postMessage");
	assert_eq!(requests[6].json()["reply_broadcast"], false);
	assert_eq!(requests[7].path, "/api/chat.update");
	assert_eq!(
		requests[7].json(),
		json!({
			"channel": CHANNEL,
			"ts": REPLY_TS,
			"file_ids": ["F1", "F2"],
		})
	);
}

#[tokio::test]
async fn failed_broadcast_promotion_reports_that_reply_remains_in_thread() {
	let mut steps = broadcast_steps(2);
	steps.last_mut().unwrap().reply =
		MockReply::Json(json!({"ok": false, "error": "cant_update_message"}));
	let mock = MockSlack::start(steps).await;
	let result = mock
		.render(&target(true, Some(PARENT_TS)), rendered(2, 0, ""))
		.await;
	let requests = mock.finish().await;
	let error = result.unwrap_err().to_string();
	assert!(
		error.contains("chat.update failed: cant_update_message"),
		"unexpected error: {error}"
	);
	assert!(
		error.contains("reply remains in the thread"),
		"missing recovery context: {error}"
	);
	assert_eq!(requests.len(), 9);
	assert_uploads(&requests[..6], true, PARENT_TS);
	assert_broadcast_reply(&requests[6..], "Typst", PARENT_TS, &json!(["F1", "F2"]));
}

#[tokio::test]
async fn missing_reply_timestamp_prevents_broadcast_update() {
	let mut steps = broadcast_steps(1);
	steps.truncate(4);
	steps[3].reply = MockReply::Json(json!({"ok": true}));
	let mock = MockSlack::start(steps).await;
	let result = mock
		.render(&target(true, Some(PARENT_TS)), rendered(1, 0, ""))
		.await;
	let requests = mock.finish().await;
	let error = result.unwrap_err().to_string();
	assert!(
		error.contains("chat.postMessage response did not include ts"),
		"unexpected error: {error}"
	);
	assert_eq!(requests.len(), 4);
	assert_eq!(requests[3].path, "/api/chat.postMessage");
	assert_eq!(requests[3].json()["reply_broadcast"], false);
}
