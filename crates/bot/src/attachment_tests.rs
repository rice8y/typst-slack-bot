use std::collections::VecDeque;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use http_body_util::BodyExt as _;
use hyper::body::{Body, Frame, Incoming, SizeHint};
use hyper::header::{HeaderValue, AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE, LOCATION};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{HeaderMap, Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tokio::sync::{oneshot, Mutex};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::Sleep;

use super::*;

const HTTP_TIMEOUT: Duration = Duration::from_secs(3);
const TEST_TIMEOUT: Duration = Duration::from_secs(8);
const TOKEN: &str = "attachment-test-token";
const LIMIT_ERROR: &str = "10 MiB per-file or 20 MiB total limit";
const TRUST_ERROR: &str = "not a trusted Slack HTTPS URL";
const MAX_RECORDED_REQUESTS: usize = 64;
const CHUNK: &[u8; 8192] = &[0xa5; 8192];

// Reuse one small buffer even for the real 10/20 MiB limit tests. Unknown-size
// bodies force HTTP chunking, so Content-Length cannot mask the stream check.
struct MockBody {
	chunk: Bytes,
	remaining: usize,
	exact_size: bool,
	fail_after: Option<Pin<Box<Sleep>>>,
}

impl MockBody {
	fn bytes(bytes: impl Into<Bytes>) -> Self {
		let chunk = bytes.into();
		Self {
			remaining: chunk.len(),
			chunk,
			exact_size: true,
			fail_after: None,
		}
	}

	fn streamed(size: usize) -> Self {
		Self {
			chunk: Bytes::from_static(CHUNK),
			remaining: size,
			exact_size: false,
			fail_after: None,
		}
	}

	fn broken() -> Self {
		let mut body = Self::streamed(1);
		// Let Hyper flush the headers and first chunk before breaking the body.
		body.fail_after = Some(Box::pin(tokio::time::sleep(Duration::from_millis(50))));
		body
	}
}

impl Body for MockBody {
	type Data = Bytes;
	type Error = io::Error;

	fn poll_frame(
		mut self: Pin<&mut Self>,
		cx: &mut Context<'_>,
	) -> Poll<Option<Result<Frame<Bytes>, io::Error>>> {
		if self.remaining != 0 {
			let length = self.remaining.min(self.chunk.len());
			self.remaining -= length;
			return Poll::Ready(Some(Ok(Frame::data(self.chunk.slice(..length)))));
		}
		if let Some(delay) = &mut self.fail_after {
			if delay.as_mut().poll(cx).is_pending() {
				return Poll::Pending;
			}
			self.fail_after = None;
			return Poll::Ready(Some(Err(io::Error::new(
				io::ErrorKind::UnexpectedEof,
				"mock body failure",
			))));
		}
		Poll::Ready(None)
	}

	fn is_end_stream(&self) -> bool {
		self.remaining == 0 && self.fail_after.is_none()
	}

	fn size_hint(&self) -> SizeHint {
		let mut hint = SizeHint::new();
		if self.exact_size {
			hint.set_exact(self.remaining as u64);
		}
		hint
	}
}

enum MockReply {
	Info(Value),
	Json(Value),
	Bytes(Bytes),
	Stream(usize),
	ContentLength(usize),
	Redirect(StatusCode, Option<HeaderValue>),
	Status(StatusCode),
	MalformedJson,
	BrokenBody,
}

struct MockStep {
	path: String,
	file_id: Option<String>,
	reply: MockReply,
}

impl MockStep {
	fn info(id: &str, file: Value) -> Self {
		Self {
			path: "/api/files.info".into(),
			file_id: Some(id.into()),
			reply: MockReply::Info(file),
		}
	}

	fn api(id: &str, reply: MockReply) -> Self {
		Self {
			path: "/api/files.info".into(),
			file_id: Some(id.into()),
			reply,
		}
	}

	fn download(path: &str, reply: MockReply) -> Self {
		Self {
			path: path.into(),
			file_id: None,
			reply,
		}
	}

	fn bytes(path: &str, bytes: &'static [u8]) -> Self {
		Self::download(path, MockReply::Bytes(Bytes::from_static(bytes)))
	}

	fn redirect(path: &str, status: StatusCode, location: &str) -> Self {
		Self::download(
			path,
			MockReply::Redirect(status, Some(HeaderValue::from_str(location).unwrap())),
		)
	}
}

struct RecordedRequest {
	path: String,
	query: Vec<(String, String)>,
	method: Method,
	headers: HeaderMap,
	body: Bytes,
	expected: Option<(String, Option<String>)>,
}

struct MockState {
	steps: VecDeque<MockStep>,
	requests: Vec<RecordedRequest>,
	overflow: bool,
}

struct MockSlack {
	base: String,
	api_base: String,
	client: Client,
	state: Arc<Mutex<MockState>>,
	shutdown: Option<oneshot::Sender<()>>,
	server: Option<JoinHandle<()>>,
}

impl MockSlack {
	async fn start(steps: Vec<MockStep>) -> Self {
		assert!(steps.len() < MAX_RECORDED_REQUESTS);
		let listener = timeout(HTTP_TIMEOUT, TcpListener::bind("127.0.0.1:0"))
			.await
			.unwrap()
			.unwrap();
		let base = format!("http://{}", listener.local_addr().unwrap());
		let state = Arc::new(Mutex::new(MockState {
			steps: steps.into(),
			requests: Vec::new(),
			overflow: false,
		}));
		let server_state = Arc::clone(&state);
		let server_base = base.clone();
		let (shutdown, mut stopped) = oneshot::channel();
		let server = tokio::spawn(async move {
			let mut connections = JoinSet::new();
			loop {
				tokio::select! {
					_ = &mut stopped => break,
					accepted = listener.accept() => {
						let (stream, _) = accepted.expect("mock listener failed");
						let state = Arc::clone(&server_state);
						let base = server_base.clone();
						connections.spawn(async move {
							let service = service_fn(move |request| handle_request(request, Arc::clone(&state), base.clone()));
							let _ = timeout(TEST_TIMEOUT, http1::Builder::new().serve_connection(TokioIo::new(stream), service)).await;
						});
					}
					_ = connections.join_next(), if !connections.is_empty() => {}
				}
			}
			connections.abort_all();
			while connections.join_next().await.is_some() {}
		});
		Self {
			api_base: format!("{base}/api"),
			base,
			client: Client::builder()
				.no_proxy()
				.timeout(HTTP_TIMEOUT)
				.build()
				.unwrap(),
			state,
			shutdown: Some(shutdown),
			server: Some(server),
		}
	}

	fn loader(&self) -> AttachmentLoader<'_> {
		let mut loader = AttachmentLoader::new(&self.client, TOKEN, &self.api_base).unwrap();
		loader.test_origin = Some(Url::parse(&self.base).unwrap());
		loader
	}

	async fn load(&self, ids: &[&str]) -> Result<Vec<Attachment>, BotError> {
		let files: Vec<_> = ids
			.iter()
			.map(|id| SlackFileRef { id: (*id).into() })
			.collect();
		timeout(TEST_TIMEOUT, self.loader().load(&files))
			.await
			.expect("attachment load hung")
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
		joined.unwrap().expect("mock server panicked");
		let mut state = self.state.lock().await;
		assert!(!state.overflow, "request recording limit exceeded");
		assert!(
			state.steps.is_empty(),
			"not all expected HTTP requests were made"
		);
		let requests = std::mem::take(&mut state.requests);
		for request in &requests {
			assert_ne!(
				request.path, "/api/files.delete",
				"downloading must never delete Slack files"
			);
			let (path, file_id) = request.expected.as_ref().expect("unexpected HTTP request");
			assert_eq!(&request.path, path, "unexpected HTTP request path");
			assert_eq!(request.method, Method::GET);
			assert!(request.body.is_empty(), "GET request must not carry a body");
			if let Some(id) = file_id {
				assert_eq!(request.query, vec![("file".to_owned(), id.clone())]);
				assert_eq!(request.headers[AUTHORIZATION], format!("Bearer {TOKEN}"));
			} else {
				assert!(
					!request.headers.contains_key(AUTHORIZATION),
					"Slack token leaked to a non-Slack download host"
				);
			}
		}
		requests
	}
}

impl Drop for MockSlack {
	fn drop(&mut self) {
		if let Some(server) = &self.server {
			server.abort();
		}
	}
}

async fn handle_request(
	request: Request<Incoming>,
	state: Arc<Mutex<MockState>>,
	base: String,
) -> Result<Response<MockBody>, BotError> {
	let (parts, body) = request.into_parts();
	let body = timeout(HTTP_TIMEOUT, body.collect()).await??.to_bytes();
	let query = Url::parse(&format!("{base}{}", parts.uri))?
		.query_pairs()
		.into_owned()
		.collect();
	let step = {
		let mut state = state.lock().await;
		let step = state.steps.pop_front();
		if state.requests.len() < MAX_RECORDED_REQUESTS {
			state.requests.push(RecordedRequest {
				path: parts.uri.path().into(),
				query,
				method: parts.method,
				headers: parts.headers,
				body,
				expected: step
					.as_ref()
					.map(|step| (step.path.clone(), step.file_id.clone())),
			});
		} else {
			state.overflow = true;
		}
		step
	};
	let mut response = Response::builder();
	let body = match step.map(|step| step.reply) {
		Some(MockReply::Info(mut file)) => {
			for key in ["url_private", "url_private_download"] {
				if let Some(path) = file[key].as_str().filter(|path| path.starts_with('/')) {
					file[key] = Value::String(format!("{base}{path}"));
				}
			}
			response = response.header(CONTENT_TYPE, "application/json");
			MockBody::bytes(serde_json::to_vec(&json!({"ok": true, "file": file}))?)
		}
		Some(MockReply::Json(value)) => {
			response = response.header(CONTENT_TYPE, "application/json");
			MockBody::bytes(serde_json::to_vec(&value)?)
		}
		Some(MockReply::Bytes(bytes)) => MockBody::bytes(bytes),
		Some(MockReply::Stream(size)) => MockBody::streamed(size),
		Some(MockReply::ContentLength(size)) => {
			response = response.header(CONTENT_LENGTH, size);
			MockBody::streamed(size)
		}
		Some(MockReply::Redirect(status, location)) => {
			response = response.status(status);
			if let Some(location) = location {
				response = response.header(LOCATION, location);
			}
			MockBody::bytes(Bytes::new())
		}
		Some(MockReply::Status(status)) => {
			response = response.status(status);
			MockBody::bytes(Bytes::new())
		}
		Some(MockReply::MalformedJson) => MockBody::bytes(Bytes::from_static(b"not JSON")),
		Some(MockReply::BrokenBody) => MockBody::broken(),
		None => {
			response = response.status(StatusCode::BAD_REQUEST);
			MockBody::bytes(Bytes::new())
		}
	};
	Ok(response.body(body)?)
}

fn metadata(id: &str, name: &str, size: Option<usize>) -> Value {
	json!({"id": id, "name": name, "size": size, "url_private_download": format!("/private/{id}"), "is_external": false})
}

fn assert_error<T: std::fmt::Debug>(result: Result<T, BotError>, expected: &str) {
	let error = result
		.expect_err("operation unexpectedly succeeded")
		.to_string();
	assert!(
		error.contains(expected),
		"expected {expected:?}, got {error:?}"
	);
}

async fn assert_metadata_error(file: Value, expected: &str) {
	let mock = MockSlack::start(vec![MockStep::info("F1", file)]).await;
	assert_error(mock.load(&["F1"]).await, expected);
	assert_eq!(
		mock.finish().await.len(),
		1,
		"invalid metadata must fail before downloading"
	);
}

#[tokio::test]
async fn empty_attachment_list_makes_no_requests() {
	let mock = MockSlack::start(vec![]).await;
	assert!(mock.load(&[]).await.unwrap().is_empty());
	assert!(mock.finish().await.is_empty());
}

#[tokio::test]
async fn loads_multiple_private_files_in_order_with_unicode_names_and_binary_bytes() {
	let mut first = metadata("F1", "図表 α.png", Some(4));
	first["url_private"] = json!("https://untrusted.invalid/must-not-be-used");
	let mut second = metadata("F2", "my data.csv", None);
	second
		.as_object_mut()
		.unwrap()
		.remove("url_private_download");
	second.as_object_mut().unwrap().remove("size");
	second.as_object_mut().unwrap().remove("is_external");
	second["url_private"] = json!("/private/F2");
	let mock = MockSlack::start(vec![
		MockStep::info("F1", first),
		MockStep::bytes("/private/F1", &[0, 0xff, 0x80, 1]),
		MockStep::info("F2", second),
		MockStep::bytes("/private/F2", b"x,y\n1,2\n"),
		MockStep::info("F3", metadata("F3", "lib.typ", Some(0))),
		MockStep::bytes("/private/F3", b""),
	])
	.await;
	let attachments = mock.load(&["F1", "F2", "F3"]).await.unwrap();
	assert_eq!(attachments.len(), 3);
	assert_eq!(attachments[0].name, "図表 α.png");
	assert_eq!(attachments[0].data, [0, 0xff, 0x80, 1]);
	assert_eq!(attachments[1].name, "my data.csv");
	assert_eq!(attachments[1].data, b"x,y\n1,2\n");
	assert_eq!(attachments[2].name, "lib.typ");
	assert!(attachments[2].data.is_empty());
	assert_eq!(mock.finish().await.len(), 6);
}

#[tokio::test]
async fn file_ids_are_query_encoded_and_names_come_from_files_info() {
	let id = "F &?=+日本語";
	let mut file = metadata(id, "actual filename.typ", Some(1));
	file["url_private_download"] = json!("/private/data");
	let mock = MockSlack::start(vec![
		MockStep::info(id, file),
		MockStep::bytes("/private/data", b"x"),
	])
	.await;
	let attachments = mock.load(&[id]).await.unwrap();
	assert_eq!(attachments[0].name, "actual filename.typ");
	assert_eq!(attachments[0].data, b"x");
	mock.finish().await;
}

#[tokio::test]
async fn files_info_errors_explain_files_read_scope_and_do_not_download() {
	for error in [
		Some("missing_scope"),
		Some("file_not_found"),
		Some("not_authed"),
		None,
	] {
		let mock = MockSlack::start(vec![MockStep::api(
			"F1",
			MockReply::Json(json!({"ok": false, "error": error})),
		)])
		.await;
		let result = mock.load(&["F1"]).await;
		let message = result.unwrap_err().to_string();
		assert!(message.contains("files.info failed"));
		assert!(message.contains(error.unwrap_or("unknown error")));
		assert!(message.contains("files:read"));
		assert_eq!(mock.finish().await.len(), 1);
	}
}

#[tokio::test]
async fn oversized_files_info_responses_are_bounded_before_json_parsing() {
	for reply in [
		MockReply::ContentLength(MAX_FILE_INFO_BYTES + 1),
		MockReply::Stream(MAX_FILE_INFO_BYTES + 1),
	] {
		let mock = MockSlack::start(vec![MockStep::api("F1", reply)]).await;
		assert_error(
			mock.load(&["F1", "F2"]).await,
			"files.info response is too large",
		);
		assert_eq!(mock.finish().await.len(), 1);
	}
}

#[tokio::test]
async fn files_info_response_at_exact_metadata_limit_is_parsed() {
	// This single allocation is explicitly capped at the 1 MiB metadata limit.
	let mut body = serde_json::to_vec(&json!({"ok": false, "error": "missing_scope"})).unwrap();
	body.resize(MAX_FILE_INFO_BYTES, b' ');
	let mock = MockSlack::start(vec![MockStep::api(
		"F1",
		MockReply::Bytes(Bytes::from(body)),
	)])
	.await;
	assert_error(mock.load(&["F1"]).await, "files.info failed: missing_scope");
	assert_eq!(mock.finish().await.len(), 1);
}

#[tokio::test]
async fn too_many_attachments_is_rejected_before_any_http_request() {
	let ids: Vec<_> = (0..=MAX_ATTACHMENTS)
		.map(|index| format!("F{index}"))
		.collect();
	let refs: Vec<_> = ids.iter().map(String::as_str).collect();
	let mock = MockSlack::start(vec![]).await;
	assert_error(mock.load(&refs).await, "too many attachments");
	assert!(mock.finish().await.is_empty());
}

#[tokio::test]
async fn exactly_the_maximum_attachment_count_is_allowed() {
	let ids: Vec<_> = (0..MAX_ATTACHMENTS)
		.map(|index| format!("F{index}"))
		.collect();
	let mut steps = Vec::new();
	for id in &ids {
		steps.push(MockStep::info(
			id,
			metadata(id, &format!("{id}.typ"), Some(1)),
		));
		steps.push(MockStep::bytes(&format!("/private/{id}"), b"x"));
	}
	let mock = MockSlack::start(steps).await;
	let refs: Vec<_> = ids.iter().map(String::as_str).collect();
	let attachments = mock.load(&refs).await.unwrap();
	assert_eq!(attachments.len(), MAX_ATTACHMENTS);
	for (attachment, id) in attachments.iter().zip(&ids) {
		assert_eq!(attachment.name, format!("{id}.typ"));
		assert_eq!(attachment.data, b"x");
	}
	assert_eq!(mock.finish().await.len(), 2 * MAX_ATTACHMENTS);
}

#[tokio::test]
async fn oversized_metadata_is_rejected_before_downloading() {
	for size in [MAX_ATTACHMENT_BYTES + 1, usize::MAX] {
		assert_metadata_error(metadata("F1", "data.bin", Some(size)), LIMIT_ERROR).await;
	}
}

#[tokio::test]
async fn metadata_at_per_file_limit_does_not_require_that_many_actual_bytes() {
	let mock = MockSlack::start(vec![
		MockStep::info("F1", metadata("F1", "data.bin", Some(MAX_ATTACHMENT_BYTES))),
		MockStep::bytes("/private/F1", b"x"),
	])
	.await;
	assert_eq!(mock.load(&["F1"]).await.unwrap()[0].data, b"x");
	mock.finish().await;
}

#[tokio::test]
async fn download_content_length_enforces_real_per_file_limit() {
	let mock = MockSlack::start(vec![
		MockStep::info("F1", metadata("F1", "data.bin", Some(0))),
		MockStep::download(
			"/private/F1",
			MockReply::ContentLength(MAX_ATTACHMENT_BYTES + 1),
		),
	])
	.await;
	assert_error(mock.load(&["F1"]).await, LIMIT_ERROR);
	mock.finish().await;
}

#[tokio::test]
async fn chunked_download_enforces_per_file_limit_with_missing_or_underreported_metadata() {
	for size in [None, Some(0)] {
		let mock = MockSlack::start(vec![
			MockStep::info("F1", metadata("F1", "data.bin", size)),
			MockStep::download("/private/F1", MockReply::Stream(MAX_ATTACHMENT_BYTES + 1)),
		])
		.await;
		assert_error(mock.load(&["F1"]).await, LIMIT_ERROR);
		mock.finish().await;
	}
}

#[tokio::test]
async fn downloader_obeys_small_explicit_byte_limits_for_fixed_and_chunked_bodies() {
	for (size, chunked, succeeds) in [
		(64, false, true),
		(65, false, false),
		(64, true, true),
		(65, true, false),
		(0, true, true),
	] {
		let reply = if chunked {
			MockReply::Stream(size)
		} else {
			MockReply::ContentLength(size)
		};
		let mock = MockSlack::start(vec![MockStep::download("/data", reply)]).await;
		let result = timeout(
			TEST_TIMEOUT,
			mock
				.loader()
				.download(Url::parse(&format!("{}/data", mock.base)).unwrap(), 64),
		)
		.await
		.unwrap();
		if succeeds {
			assert_eq!(result.unwrap(), vec![0xa5; size]);
		} else {
			assert_error(result, LIMIT_ERROR);
		}
		mock.finish().await;
	}
	for size in [0, 1] {
		let mock = MockSlack::start(vec![MockStep::download("/data", MockReply::Stream(size))]).await;
		let result = timeout(
			TEST_TIMEOUT,
			mock
				.loader()
				.download(Url::parse(&format!("{}/data", mock.base)).unwrap(), 0),
		)
		.await
		.unwrap();
		if size == 0 {
			assert!(result.unwrap().is_empty());
		} else {
			assert_error(result, LIMIT_ERROR);
		}
		mock.finish().await;
	}
}

fn nearly_full_aggregate_steps() -> Vec<MockStep> {
	vec![
		MockStep::info("F1", metadata("F1", "one.bin", Some(MAX_ATTACHMENT_BYTES))),
		MockStep::download("/private/F1", MockReply::Stream(MAX_ATTACHMENT_BYTES)),
		MockStep::info("F2", metadata("F2", "two.bin", None)),
		MockStep::download(
			"/private/F2",
			MockReply::Stream(MAX_TOTAL_ATTACHMENT_BYTES - MAX_ATTACHMENT_BYTES - 1),
		),
	]
}

#[tokio::test]
async fn exact_per_file_and_aggregate_limits_are_inclusive_and_empty_files_still_fit() {
	let mut steps = nearly_full_aggregate_steps();
	steps.extend([
		MockStep::info("F3", metadata("F3", "three.bin", Some(1))),
		MockStep::bytes("/private/F3", &[0xa5]),
		MockStep::info("F4", metadata("F4", "empty.bin", Some(0))),
		MockStep::bytes("/private/F4", b""),
	]);
	let mock = MockSlack::start(steps).await;
	let attachments = mock.load(&["F1", "F2", "F3", "F4"]).await.unwrap();
	assert_eq!(attachments[0].data.len(), MAX_ATTACHMENT_BYTES);
	assert_eq!(
		attachments
			.iter()
			.map(|attachment| attachment.data.len())
			.sum::<usize>(),
		MAX_TOTAL_ATTACHMENT_BYTES
	);
	assert!(attachments[0].data.iter().all(|byte| *byte == 0xa5));
	assert!(attachments[1].data.iter().all(|byte| *byte == 0xa5));
	assert_eq!(attachments[2].data, [0xa5]);
	assert!(attachments[3].data.is_empty());
	drop(attachments);
	assert_eq!(mock.finish().await.len(), 8);
}

#[tokio::test]
async fn aggregate_metadata_limit_uses_actual_downloaded_bytes() {
	let mut steps = nearly_full_aggregate_steps();
	steps.push(MockStep::info("F3", metadata("F3", "three.bin", Some(2))));
	let mock = MockSlack::start(steps).await;
	assert_error(mock.load(&["F1", "F2", "F3"]).await, LIMIT_ERROR);
	assert_eq!(mock.finish().await.len(), 5);
}

#[tokio::test]
async fn aggregate_stream_limit_rejects_one_byte_over_even_when_metadata_fits() {
	let mut steps = nearly_full_aggregate_steps();
	steps.extend([
		MockStep::info("F3", metadata("F3", "three.bin", Some(1))),
		MockStep::download("/private/F3", MockReply::Stream(2)),
	]);
	let mock = MockSlack::start(steps).await;
	assert_error(mock.load(&["F1", "F2", "F3"]).await, LIMIT_ERROR);
	assert_eq!(mock.finish().await.len(), 6);
}

#[tokio::test]
async fn metadata_totals_do_not_replace_actual_byte_accounting() {
	let mock = MockSlack::start(vec![
		MockStep::info("F1", metadata("F1", "one.bin", Some(MAX_ATTACHMENT_BYTES))),
		MockStep::bytes("/private/F1", b"a"),
		MockStep::info("F2", metadata("F2", "two.bin", Some(MAX_ATTACHMENT_BYTES))),
		MockStep::bytes("/private/F2", b"b"),
		MockStep::info(
			"F3",
			metadata("F3", "three.bin", Some(MAX_ATTACHMENT_BYTES)),
		),
		MockStep::bytes("/private/F3", b"c"),
	])
	.await;
	let attachments = mock.load(&["F1", "F2", "F3"]).await.unwrap();
	assert_eq!(
		attachments
			.iter()
			.map(|file| file.data.len())
			.sum::<usize>(),
		3
	);
	mock.finish().await;
}

#[tokio::test]
async fn duplicate_ids_are_rejected_without_fetching_metadata_twice() {
	let mock = MockSlack::start(vec![
		MockStep::info("F1", metadata("F1", "one.typ", Some(1))),
		MockStep::bytes("/private/F1", b"x"),
	])
	.await;
	assert_error(
		mock.load(&["F1", "F1"]).await,
		"duplicate attachment file ID",
	);
	assert_eq!(mock.finish().await.len(), 2);
}

#[tokio::test]
async fn duplicate_names_are_rejected_before_second_download() {
	for name in ["same.typ", "図表.png"] {
		let mock = MockSlack::start(vec![
			MockStep::info("F1", metadata("F1", name, Some(1))),
			MockStep::bytes("/private/F1", b"x"),
			MockStep::info("F2", metadata("F2", name, Some(1))),
		])
		.await;
		assert_error(
			mock.load(&["F1", "F2"]).await,
			"duplicate attachment filename",
		);
		assert_eq!(mock.finish().await.len(), 3);
	}
}

#[tokio::test]
async fn invalid_paths_reserved_main_and_control_characters_never_download() {
	for name in [
		"",
		".",
		"..",
		"main.typ",
		"/image.png",
		"../image.png",
		"dir/image.png",
		"dir\\image.png",
		"C:\\image.png",
		"C:image.png",
		"\\\\server\\file.typ",
		"nul\0.typ",
		"line\n.typ",
		"tab\t.typ",
		"del\u{7f}.typ",
		"control\u{85}.typ",
	] {
		assert_metadata_error(metadata("F1", name, Some(1)), "invalid attachment filename").await;
	}
}

#[tokio::test]
async fn externally_hosted_slack_files_are_rejected_before_download() {
	let mut file = metadata("F1", "external.png", Some(1));
	file["is_external"] = json!(true);
	assert_metadata_error(file, "externally hosted attachments are not supported").await;
}

#[tokio::test]
async fn missing_metadata_and_mismatched_ids_are_rejected() {
	for file in [None, Some(Value::Null)] {
		let response = match file {
			None => json!({"ok": true}),
			Some(file) => json!({"ok": true, "file": file}),
		};
		let mock = MockSlack::start(vec![MockStep::api("F1", MockReply::Json(response))]).await;
		assert_error(
			mock.load(&["F1"]).await,
			"files.info response did not include file",
		);
		mock.finish().await;
	}
	assert_metadata_error(
		metadata("OTHER", "image.png", Some(1)),
		"different attachment ID",
	)
	.await;
	for omit in [false, true] {
		let mut file = metadata("F1", "image.png", Some(1));
		if omit {
			file.as_object_mut().unwrap().remove("name");
		} else {
			file["name"] = Value::Null;
		}
		assert_metadata_error(file, "attachment has no filename").await;
		let mut file = metadata("F1", "image.png", Some(1));
		if omit {
			file.as_object_mut().unwrap().remove("url_private_download");
		} else {
			file["url_private_download"] = Value::Null;
		}
		assert_metadata_error(file, "attachment has no private download URL").await;
	}
}

#[tokio::test]
async fn malformed_metadata_and_http_errors_fail_without_downloading() {
	for response in [
		json!({}),
		json!({"ok": "true"}),
		json!({"ok": true, "file": {"name": "image.png"}}),
	] {
		let mock = MockSlack::start(vec![MockStep::api("F1", MockReply::Json(response))]).await;
		assert!(mock.load(&["F1"]).await.is_err());
		assert_eq!(mock.finish().await.len(), 1);
	}
	for (key, value) in [
		("size", json!(-1)),
		("size", json!("1")),
		("name", json!(42)),
		("is_external", json!("false")),
		("url_private_download", json!([])),
	] {
		let mut file = metadata("F1", "image.png", Some(1));
		file[key] = value;
		let mock = MockSlack::start(vec![MockStep::info("F1", file)]).await;
		assert!(mock.load(&["F1"]).await.is_err());
		assert_eq!(mock.finish().await.len(), 1);
	}
	for reply in [
		MockReply::Status(StatusCode::FORBIDDEN),
		MockReply::Status(StatusCode::TOO_MANY_REQUESTS),
		MockReply::Status(StatusCode::INTERNAL_SERVER_ERROR),
		MockReply::MalformedJson,
		MockReply::BrokenBody,
	] {
		let mock = MockSlack::start(vec![MockStep::api("F1", reply)]).await;
		assert!(mock.load(&["F1"]).await.is_err());
		assert_eq!(mock.finish().await.len(), 1);
	}
}

#[tokio::test]
async fn invalid_private_urls_fail_before_any_download() {
	for raw_url in ["", "not a URL", "http://[", "/relative/path"] {
		let mut file = metadata("F1", "image.png", Some(1));
		file["url_private_download"] = json!(raw_url);
		// Use Json rather than Info so a relative path is not expanded by the mock.
		let mock = MockSlack::start(vec![MockStep::api(
			"F1",
			MockReply::Json(json!({"ok": true, "file": file})),
		)])
		.await;
		assert_error(mock.load(&["F1"]).await, "invalid attachment download URL");
		mock.finish().await;
	}
}

#[tokio::test]
async fn production_trust_policy_accepts_only_exact_slack_https_hosts_without_credentials() {
	let mock = MockSlack::start(vec![]).await;
	let loader = AttachmentLoader::new(&mock.client, TOKEN, &mock.api_base).unwrap();
	for raw_url in [
		"https://files.slack.com/a",
		"https://files.slack-edge.com/a",
		"https://downloads.slack-edge.com/a",
		"https://files.slack.com:443/a",
		"https://FILES.SLACK.COM/a?x=1#fragment",
	] {
		assert!(
			loader.trusted_url(&Url::parse(raw_url).unwrap()),
			"{raw_url}"
		);
	}
	for raw_url in [
		"http://files.slack.com/a",
		"http://files.slack-edge.com/a",
		"ftp://files.slack.com/a",
		"https://files.slack.com:444/a",
		"https://slack.com/a",
		"https://api.slack.com/a",
		"https://evil.files.slack.com/a",
		"https://files.slack.com.evil.invalid/a",
		"https://files.slack.com./a",
		"https://files-slack.com/a",
		"https://external.invalid/a",
		"https://127.0.0.1/a",
		"http://localhost/a",
		"https://user@files.slack.com/a",
		"https://user:password@files.slack.com/a",
		"https://:password@files.slack.com/a",
		"file:///etc/passwd",
	] {
		let url = Url::parse(raw_url).unwrap();
		assert!(!loader.trusted_url(&url), "{raw_url}");
		assert_error(
			timeout(TEST_TIMEOUT, loader.download(url, 64))
				.await
				.unwrap(),
			TRUST_ERROR,
		);
	}
	assert!(!loader.trusted_url(&Url::parse(&mock.base).unwrap()));
	drop(loader);
	assert!(mock.finish().await.is_empty());
}

#[tokio::test]
async fn test_origin_is_exact_and_does_not_disable_credential_or_external_origin_checks() {
	let mock = MockSlack::start(vec![]).await;
	let other = MockSlack::start(vec![]).await;
	let loader = mock.loader();
	let origin = Url::parse(&mock.base).unwrap();
	assert!(loader.trusted_url(&origin.join("/any/path?query=1").unwrap()));
	for url in [
		Url::parse(&other.base).unwrap(),
		Url::parse(&mock.base.replace("127.0.0.1", "localhost")).unwrap(),
		Url::parse(&mock.base.replace("http:", "https:")).unwrap(),
		Url::parse(&mock.base.replace("http://", "http://user@")).unwrap(),
		Url::parse(&mock.base.replace("http://", "http://:secret@")).unwrap(),
	] {
		assert!(!loader.trusted_url(&url), "{url}");
		assert_error(
			timeout(TEST_TIMEOUT, loader.download(url, 64))
				.await
				.unwrap(),
			TRUST_ERROR,
		);
	}
	drop(loader);
	assert!(mock.finish().await.is_empty());
	assert!(
		other.finish().await.is_empty(),
		"untrusted origin must not be contacted"
	);
}

#[tokio::test]
async fn untrusted_metadata_url_is_rejected_without_contacting_external_origin() {
	let external = MockSlack::start(vec![]).await;
	let mut file = metadata("F1", "image.png", Some(1));
	file["url_private_download"] = json!(format!(
		"{}/stolen",
		external.base.replace("127.0.0.1", "localhost")
	));
	let mock = MockSlack::start(vec![MockStep::info("F1", file)]).await;
	assert_error(mock.load(&["F1"]).await, TRUST_ERROR);
	assert_eq!(mock.finish().await.len(), 1);
	assert!(
		external.finish().await.is_empty(),
		"external host received a request"
	);
}

#[tokio::test]
async fn trusted_relative_redirects_follow_all_supported_statuses_without_leaking_token() {
	let mock = MockSlack::start(vec![
		MockStep::info("F1", metadata("F1", "image.png", Some(2))),
		MockStep::redirect("/private/F1", StatusCode::MOVED_PERMANENTLY, "next"),
		MockStep::redirect("/private/next", StatusCode::FOUND, "../second"),
		MockStep::redirect("/second", StatusCode::SEE_OTHER, "/third"),
		MockStep::redirect("/third", StatusCode::TEMPORARY_REDIRECT, "/fourth"),
		MockStep::redirect(
			"/fourth",
			StatusCode::PERMANENT_REDIRECT,
			"/final?signature=a%2Bb",
		),
		MockStep::bytes("/final", &[0, 0xff]),
	])
	.await;
	assert_eq!(mock.load(&["F1"]).await.unwrap()[0].data, [0, 0xff]);
	let requests = mock.finish().await;
	assert_eq!(requests.len(), 7);
	assert_eq!(
		requests.last().unwrap().query,
		vec![("signature".to_owned(), "a+b".to_owned())]
	);
}

#[tokio::test]
async fn trusted_absolute_and_scheme_relative_redirects_work() {
	for scheme_relative in [false, true] {
		let mock = MockSlack::start(vec![]).await;
		let location = format!(
			"{}/final",
			if scheme_relative {
				mock.base.trim_start_matches("http:")
			} else {
				&mock.base
			}
		);
		mock.state.lock().await.steps.extend([
			MockStep::info("F1", metadata("F1", "image.png", Some(1))),
			MockStep::redirect("/private/F1", StatusCode::FOUND, &location),
			MockStep::bytes("/final", b"x"),
		]);
		assert_eq!(mock.load(&["F1"]).await.unwrap()[0].data, b"x");
		mock.finish().await;
	}
}

#[tokio::test]
async fn redirects_to_external_hosts_are_rejected_before_contact_for_every_redirect_status() {
	for status in [
		StatusCode::MOVED_PERMANENTLY,
		StatusCode::FOUND,
		StatusCode::SEE_OTHER,
		StatusCode::TEMPORARY_REDIRECT,
		StatusCode::PERMANENT_REDIRECT,
	] {
		let external = MockSlack::start(vec![]).await;
		let location = format!("{}/stolen", external.base.replace("127.0.0.1", "localhost"));
		let mock = MockSlack::start(vec![
			MockStep::info("F1", metadata("F1", "image.png", Some(1))),
			MockStep::redirect("/private/F1", status, &location),
		])
		.await;
		assert_error(mock.load(&["F1"]).await, TRUST_ERROR);
		assert_eq!(mock.finish().await.len(), 2);
		assert!(
			external.finish().await.is_empty(),
			"redirect contacted an external host"
		);
	}
}

#[tokio::test]
async fn redirects_revalidate_scheme_port_credentials_and_host() {
	for location in [
		"https://external.invalid/file",
		"//files.slack.com.evil.invalid/file",
		"http://files.slack.com/file",
		"https://files.slack.com:444/file",
		"https://user:secret@files.slack.com/file",
		"file:///etc/passwd",
	] {
		let mock = MockSlack::start(vec![
			MockStep::info("F1", metadata("F1", "image.png", Some(1))),
			MockStep::redirect("/private/F1", StatusCode::FOUND, location),
		])
		.await;
		assert_error(mock.load(&["F1"]).await, TRUST_ERROR);
		assert_eq!(mock.finish().await.len(), 2);
	}
}

#[tokio::test]
async fn redirect_loop_stops_at_explicit_redirect_limit() {
	let mut steps = vec![MockStep::info("F1", metadata("F1", "image.png", Some(1)))];
	steps.extend(
		(0..=MAX_REDIRECTS)
			.map(|_| MockStep::redirect("/private/F1", StatusCode::FOUND, "/private/F1")),
	);
	let mock = MockSlack::start(steps).await;
	assert_error(
		mock.load(&["F1"]).await,
		"too many attachment download redirects",
	);
	assert_eq!(mock.finish().await.len(), MAX_REDIRECTS + 2);
}

#[tokio::test]
async fn missing_invalid_and_unparseable_redirect_locations_are_errors() {
	for (location, expected) in [
		(None, "attachment redirect has no valid Location"),
		(
			Some(HeaderValue::from_bytes(b"\xff").unwrap()),
			"attachment redirect has no valid Location",
		),
		(
			Some(HeaderValue::from_static("http://[")),
			"invalid attachment redirect URL",
		),
	] {
		let mock = MockSlack::start(vec![
			MockStep::info("F1", metadata("F1", "image.png", Some(1))),
			MockStep::download(
				"/private/F1",
				MockReply::Redirect(StatusCode::FOUND, location),
			),
		])
		.await;
		assert_error(mock.load(&["F1"]).await, expected);
		assert_eq!(mock.finish().await.len(), 2);
	}
}

#[tokio::test]
async fn download_http_errors_stop_without_fetching_later_attachments() {
	for status in [
		StatusCode::BAD_REQUEST,
		StatusCode::UNAUTHORIZED,
		StatusCode::FORBIDDEN,
		StatusCode::NOT_FOUND,
		StatusCode::TOO_MANY_REQUESTS,
		StatusCode::INTERNAL_SERVER_ERROR,
	] {
		let mock = MockSlack::start(vec![
			MockStep::info("F1", metadata("F1", "image.png", Some(1))),
			MockStep::download("/private/F1", MockReply::Status(status)),
		])
		.await;
		assert_error(
			mock.load(&["F1", "F2"]).await,
			&format!("attachment download failed with HTTP {status}"),
		);
		assert_eq!(mock.finish().await.len(), 2);
	}
}

#[tokio::test]
async fn interrupted_download_body_is_an_error_not_a_partial_attachment() {
	let mock = MockSlack::start(vec![
		MockStep::info("F1", metadata("F1", "image.png", Some(2))),
		MockStep::download("/private/F1", MockReply::BrokenBody),
	])
	.await;
	assert_error(
		mock.load(&["F1", "F2"]).await,
		"attachment response body failed",
	);
	assert_eq!(mock.finish().await.len(), 2);
}

#[tokio::test]
async fn connection_failure_is_reported_without_returning_attachments() {
	let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
	let address = listener.local_addr().unwrap();
	let mut file = metadata("F1", "image.png", Some(1));
	file["url_private_download"] = json!(format!("http://{address}/unavailable"));
	let mock = MockSlack::start(vec![MockStep::info("F1", file)]).await;
	let mut loader = mock.loader();
	loader.test_origin = Some(Url::parse(&format!("http://{address}")).unwrap());
	// Accept and close without sending HTTP headers; keep the port owned until
	// the downloader connects so no unrelated process can reuse it.
	let closed = tokio::spawn(async move {
		let (stream, _) = timeout(HTTP_TIMEOUT, listener.accept())
			.await
			.unwrap()
			.unwrap();
		drop(stream);
	});
	let files = [SlackFileRef { id: "F1".into() }];
	assert_error(
		timeout(TEST_TIMEOUT, loader.load(&files)).await.unwrap(),
		"attachment download request failed",
	);
	timeout(HTTP_TIMEOUT, closed).await.unwrap().unwrap();
	drop(loader);
	mock.finish().await;
}
