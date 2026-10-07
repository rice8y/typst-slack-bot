use std::collections::HashSet;
use std::time::Duration;

use protocol::{Attachment, MAX_ATTACHMENTS, MAX_ATTACHMENT_BYTES, MAX_TOTAL_ATTACHMENT_BYTES};
use reqwest::{Client, Url};
use serde::Deserialize;
use tokio::time::timeout;

use super::BotError;

const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_REDIRECTS: usize = 5;
const MAX_FILE_INFO_BYTES: usize = 1024 * 1024;

async fn bounded_body(
	response: &mut reqwest::Response,
	limit: usize,
	error: &'static str,
) -> Result<Vec<u8>, BotError> {
	if response
		.content_length()
		.is_some_and(|length| length > limit as u64)
	{
		return Err(error.into());
	}
	let mut data = Vec::new();
	while let Some(chunk) = response
		.chunk()
		.await
		.map_err(|_| "attachment response body failed")?
	{
		if chunk.len() > limit - data.len() {
			return Err(error.into());
		}
		data.extend_from_slice(&chunk);
	}
	Ok(data)
}

#[derive(Debug, Deserialize)]
pub(super) struct SlackFileRef {
	pub id: String,
}

#[derive(Deserialize)]
struct FileInfoResponse {
	ok: bool,
	error: Option<String>,
	file: Option<SlackFile>,
}

#[derive(Deserialize)]
struct SlackFile {
	id: String,
	name: Option<String>,
	size: Option<usize>,
	url_private: Option<String>,
	url_private_download: Option<String>,
	is_external: Option<bool>,
}

pub(super) struct AttachmentLoader<'a> {
	api_client: &'a Client,
	download_client: Client,
	token: &'a str,
	api_base: &'a str,
	#[cfg(test)]
	test_origin: Option<Url>,
}

impl<'a> AttachmentLoader<'a> {
	pub fn new(api_client: &'a Client, token: &'a str, api_base: &'a str) -> Result<Self, BotError> {
		Ok(Self {
			api_client,
			download_client: Client::builder()
				.redirect(reqwest::redirect::Policy::none())
				.timeout(Duration::from_secs(30))
				.build()?,
			token,
			api_base,
			#[cfg(test)]
			test_origin: None,
		})
	}

	pub async fn load(&self, files: &[SlackFileRef]) -> Result<Vec<Attachment>, BotError> {
		if files.len() > MAX_ATTACHMENTS {
			return Err(format!("too many attachments (maximum {MAX_ATTACHMENTS})").into());
		}
		timeout(DOWNLOAD_TIMEOUT, self.load_inner(files))
			.await
			.map_err(|_| "attachment download timed out")?
	}

	async fn load_inner(&self, files: &[SlackFileRef]) -> Result<Vec<Attachment>, BotError> {
		let mut attachments = Vec::with_capacity(files.len());
		let mut names = HashSet::new();
		let mut ids = HashSet::new();
		let mut total = 0;
		for file in files {
			if !ids.insert(&file.id) {
				return Err("duplicate attachment file ID".into());
			}
			let mut response = self
				.api_client
				.get(format!("{}/files.info", self.api_base))
				.bearer_auth(self.token)
				.query(&[("file", &file.id)])
				.timeout(Duration::from_secs(30))
				.send()
				.await?
				.error_for_status()?;
			let body = bounded_body(
				&mut response,
				MAX_FILE_INFO_BYTES,
				"files.info response is too large",
			)
			.await?;
			let response: FileInfoResponse = serde_json::from_slice(&body)?;
			if !response.ok {
				return Err(
					format!(
						"files.info failed: {} (attachment access requires files:read)",
						response.error.as_deref().unwrap_or("unknown error")
					)
					.into(),
				);
			}
			let metadata = response
				.file
				.ok_or("files.info response did not include file")?;
			if metadata.id != file.id {
				return Err("files.info returned a different attachment ID".into());
			}
			if metadata.is_external == Some(true) {
				return Err("externally hosted attachments are not supported".into());
			}
			let name = metadata.name.ok_or("attachment has no filename")?;
			if !protocol::is_valid_attachment_name(&name) {
				return Err(
					format!("invalid attachment filename {name:?}: use a plain filename other than main.typ")
						.into(),
				);
			}
			if !names.insert(name.clone()) {
				return Err(format!("duplicate attachment filename {name:?}").into());
			}
			let limit = MAX_ATTACHMENT_BYTES.min(MAX_TOTAL_ATTACHMENT_BYTES - total);
			if metadata.size.is_some_and(|size| size > limit) {
				return Err("attachment exceeds the 10 MiB per-file or 20 MiB total limit".into());
			}
			let raw_url = metadata
				.url_private_download
				.or(metadata.url_private)
				.ok_or("attachment has no private download URL")?;
			let url = Url::parse(&raw_url).map_err(|_| "invalid attachment download URL")?;
			let data = self.download(url, limit).await?;
			total += data.len();
			attachments.push(Attachment { name, data });
		}
		Ok(attachments)
	}

	#[cfg_attr(
		not(test),
		allow(clippy::unused_self, reason = "tests allow a local mock origin")
	)]
	fn trusted_url(&self, url: &Url) -> bool {
		#[cfg(test)]
		if self
			.test_origin
			.as_ref()
			.is_some_and(|origin| origin.origin() == url.origin())
		{
			return url.username().is_empty() && url.password().is_none();
		}
		url.scheme() == "https"
			&& url.port_or_known_default() == Some(443)
			&& url.username().is_empty()
			&& url.password().is_none()
			&& matches!(
				url.host_str(),
				Some("files.slack.com" | "files.slack-edge.com" | "downloads.slack-edge.com")
			)
	}

	async fn download(&self, mut url: Url, limit: usize) -> Result<Vec<u8>, BotError> {
		for redirects in 0..=MAX_REDIRECTS {
			if !self.trusted_url(&url) {
				return Err("attachment download URL is not a trusted Slack HTTPS URL".into());
			}
			let mut request = self.download_client.get(url.clone());
			// Do not forward Slack credentials to CDN hosts or redirected origins.
			if url.host_str() == Some("files.slack.com") {
				request = request.bearer_auth(self.token);
			}
			let mut response = request
				.send()
				.await
				.map_err(|_| "attachment download request failed")?;
			if response.status().is_redirection() {
				if redirects == MAX_REDIRECTS {
					return Err("too many attachment download redirects".into());
				}
				let location = response
					.headers()
					.get(reqwest::header::LOCATION)
					.and_then(|value| value.to_str().ok())
					.ok_or("attachment redirect has no valid Location")?;
				url = url
					.join(location)
					.map_err(|_| "invalid attachment redirect URL")?;
				continue;
			}
			if !response.status().is_success() {
				return Err(format!("attachment download failed with HTTP {}", response.status()).into());
			}
			return bounded_body(
				&mut response,
				limit,
				"attachment exceeds the 10 MiB per-file or 20 MiB total limit",
			)
			.await;
		}
		unreachable!("redirect limit returns an error")
	}
}

#[cfg(test)]
#[path = "attachment_tests.rs"]
mod tests;
