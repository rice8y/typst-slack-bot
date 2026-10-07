use std::io::{BufReader, BufWriter, Write as _};
use std::pin::pin;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::time::Duration;

use anyhow::{anyhow, bail, Context as _};
use protocol::{Request, Response};
use tokio::select;
use tokio::sync::mpsc;
use tokio::time::Instant;

fn timeout_from_env(var: &str, default_seconds: u64) -> Duration {
	std::env::var(var)
		.ok()
		.and_then(|raw| raw.parse::<u64>().ok())
		.map_or(Duration::from_secs(default_seconds), Duration::from_secs)
}

#[derive(Debug)]
pub struct Worker {
	process: Process,
}

impl Worker {
	pub async fn spawn() -> anyhow::Result<Self> {
		Ok(Self {
			process: Process::spawn().await?,
		})
	}

	async fn run(
		&mut self,
		request: Request,
		progress_channel_outer: Option<mpsc::Sender<String>>,
	) -> anyhow::Result<Response> {
		struct Timeout;

		// This timeout is reset any time a progress message is received.
		let fast_timeout = timeout_from_env("TYPST_BOT_WORKER_FAST_TIMEOUT_SECS", 60);
		// This is a universal timeout that is never reset.
		let long_timeout = timeout_from_env("TYPST_BOT_WORKER_LONG_TIMEOUT_SECS", 300);
		let retire_after_response =
			matches!(&request, Request::Render { attachments, .. } if !attachments.is_empty());
		let mut tries_left = 2;

		loop {
			let (progress_inner_send, mut progress_inner_recv) = mpsc::channel(1);

			let res = {
				let mut fut = pin!(self
					.process
					.communicate(request.clone(), Some(progress_inner_send)));
				let mut fast_timeout_fut = pin!(tokio::time::sleep(fast_timeout));
				let mut long_timeout_fut = pin!(tokio::time::sleep(long_timeout));
				'communicate: loop {
					select! {
						res = fut.as_mut() => {
							break Ok(res);
						}
						Some(progress) = progress_inner_recv.recv() => {
							fast_timeout_fut.as_mut().reset(Instant::now() + fast_timeout);
							if let Some(outer) = &progress_channel_outer {
								select! {
									_ = outer.send(progress) => {}
									res = fut.as_mut() => break 'communicate Ok(res),
									() = fast_timeout_fut.as_mut() => break 'communicate Err(Timeout),
									() = long_timeout_fut.as_mut() => break 'communicate Err(Timeout),
								}
							}
						}
						() = fast_timeout_fut.as_mut() => {
							break Err(Timeout);
						}
						() = long_timeout_fut.as_mut() => {
							break Err(Timeout);
						}
					};
				}
			};

			let error = match res {
				Ok(Ok(response)) => {
					// Parsed attachment data can outlive a request in Typst/comemo caches.
					if retire_after_response {
						self
							.process
							.replace()
							.await
							.context("replacing worker after attachment render")?;
					}
					return Ok(response);
				}
				Ok(Err(error)) => {
					self
						.process
						.replace()
						.await
						.context("replacing worker after communication failure")?;
					error
				}
				Err(Timeout) => {
					self
						.process
						.replace()
						.await
						.context("replacing worker after timeout")?;
					bail!("timeout");
				}
			};

			tries_left -= 1;
			if tries_left == 0 {
				return Err(error);
			}
		}
	}

	pub async fn render(
		&mut self,
		code: String,
		attachments: Vec<protocol::Attachment>,
		progress_channel: mpsc::Sender<String>,
	) -> anyhow::Result<protocol::Rendered> {
		let response = self
			.run(
				Request::Render { code, attachments },
				Some(progress_channel),
			)
			.await?;
		let Response::Render(response) = response else {
			bail!("expected Render response");
		};
		response.map_err(|error| anyhow!(error))
	}

	pub async fn ast(&mut self, code: String) -> anyhow::Result<protocol::AstResponse> {
		let response = self.run(Request::Ast { code }, None).await?;
		let Response::Ast(response) = response else {
			bail!("expected Ast response");
		};
		Ok(response)
	}

	pub async fn version(&mut self) -> anyhow::Result<protocol::VersionResponse> {
		let response = self.run(Request::Version, None).await?;
		let Response::Version(response) = response else {
			bail!("expected Version response");
		};
		Ok(response)
	}
}

#[derive(Debug)]
struct Process {
	child: Option<Child>,
	stdin: Option<BufWriter<ChildStdin>>,
	stdout: Option<BufReader<ChildStdout>>,
}

impl Process {
	fn from_child(mut child: Child) -> Self {
		Self {
			stdin: child.stdin.take().map(BufWriter::new),
			stdout: child.stdout.take().map(BufReader::new),
			child: Some(child),
		}
	}

	fn command() -> Command {
		// Make sure to keep in sync with the README.
		const VAR_NAME: &str = "TYPST_BOT_WORKER_PATH";
		let worker_path = std::env::var_os(VAR_NAME).unwrap_or_else(|| "./worker".into());
		Command::new(worker_path)
	}

	async fn spawn() -> anyhow::Result<Self> {
		Self::spawn_command(Self::command()).await
	}

	async fn spawn_command(mut command: Command) -> anyhow::Result<Self> {
		let worker_path = command.get_program().to_owned();
		#[allow(clippy::unnecessary_debug_formatting)]
		let child = command
			.stdin(Stdio::piped())
			.stdout(Stdio::piped())
			.stderr(Stdio::inherit())
			.spawn()
			.with_context(|| format!("spawning worker process (path={worker_path:?}).\n\ntry setting the env var TYPST_BOT_WORKER_PATH to point to the worker binary, e.g. in the cargo target directory. alternatively, follow the instructions in the README that describe how to set up a standalone installation."))?;

		let mut ret = Self::from_child(child);
		// Ask for the version and ignore it, as a health check.
		let timeout = timeout_from_env("TYPST_BOT_WORKER_FAST_TIMEOUT_SECS", 60)
			.min(timeout_from_env("TYPST_BOT_WORKER_LONG_TIMEOUT_SECS", 300));
		let response = tokio::time::timeout(timeout, ret.communicate(Request::Version, None))
			.await
			.context("initial health check timeout")?
			.context("initial health check")?;
		if !matches!(response, Response::Version(_)) {
			bail!("expected Version response during initial health check");
		}

		Ok(ret)
	}

	async fn replace(&mut self) -> anyhow::Result<()> {
		self.replace_command(Self::command()).await
	}

	async fn replace_command(&mut self, command: Command) -> anyhow::Result<()> {
		// Retire first: a failed or cancelled replacement must not retain old data.
		self.retire();
		*self = Self::spawn_command(command).await?;
		Ok(())
	}

	fn retire(&mut self) {
		if let Some(mut child) = self.child.take() {
			_ = child.kill();
			_ = child.wait();
		}
		// Kill before dropping the writer, whose Drop may flush pending bytes.
		self.stdin = None;
		self.stdout = None;
	}

	async fn communicate(
		&mut self,
		request: Request,
		progress_channel: Option<mpsc::Sender<String>>,
	) -> anyhow::Result<Response> {
		let mut guard = CommunicationGuard {
			process: self,
			complete: false,
		};
		guard
			.process
			.child
			.as_ref()
			.context("worker process is unavailable")?;
		let mut stdin = guard
			.process
			.stdin
			.take()
			.context("worker stdin is unavailable")?;
		let mut stdout = guard
			.process
			.stdout
			.take()
			.context("worker stdout is unavailable")?;
		// This receiver is dropped on cancellation, unblocking a pending progress send.
		let (progress_send, mut progress_recv) = mpsc::channel(1);
		let mut task = tokio::task::spawn_blocking(move || {
			fn inner(
				stdin: &mut BufWriter<ChildStdin>,
				stdout: &mut BufReader<ChildStdout>,
				request: &Request,
				progress_channel: Option<&mpsc::Sender<String>>,
			) -> bincode::Result<Response> {
				bincode::serialize_into(&mut *stdin, request)?;
				stdin.flush()?;
				loop {
					let response: Response = bincode::deserialize_from(&mut *stdout)?;

					if let Response::Progress(progress) = response {
						if let Some(chan) = &progress_channel {
							_ = chan.blocking_send(progress);
						}
					} else {
						break Ok(response);
					}
				}
			}
			let res = inner(&mut stdin, &mut stdout, &request, Some(&progress_send));
			(stdin, stdout, res)
		});
		let task_result = loop {
			select! {
				result = &mut task => break result,
				Some(progress) = progress_recv.recv() => {
					if let Some(channel) = &progress_channel {
						select! {
							result = &mut task => break result,
							_ = channel.send(progress) => {}
						}
					}
				}
			}
		};
		let (stdin, stdout, res) = task_result.context("joining communication task")?;
		// Preserve read-ahead for the next request, and retire before dropping
		// either buffer if communication failed.
		guard.process.stdin = Some(stdin);
		guard.process.stdout = Some(stdout);
		let response = res.context("communicating with worker")?;
		guard.complete = true;
		Ok(response)
	}
}

impl Drop for Process {
	fn drop(&mut self) {
		self.retire();
	}
}

struct CommunicationGuard<'a> {
	process: &'a mut Process,
	complete: bool,
}

impl Drop for CommunicationGuard<'_> {
	fn drop(&mut self) {
		if !self.complete {
			// Dropping a JoinHandle cannot cancel blocking I/O. Killing its child
			// closes the peer pipes, allowing the detached communication task to end.
			self.process.retire();
		}
	}
}

#[cfg(all(test, unix))]
mod tests {
	use super::*;

	const TEST_TIMEOUT: Duration = Duration::from_secs(2);
	const COMMUNICATION_TIMEOUT: Duration = Duration::from_millis(50);

	fn shell_command(script: &str) -> Command {
		let mut command = Command::new("/bin/sh");
		command.args(["-c", script]);
		command
	}

	fn process_from_command(mut command: Command) -> Process {
		Process::from_child(
			command
				.stdin(Stdio::piped())
				.stdout(Stdio::piped())
				.spawn()
				.unwrap(),
		)
	}

	fn sleeping_process() -> Process {
		// exec keeps the pipe-owning process identical to the child we kill.
		// The short sleep also bounds runtime shutdown if cancellation regresses.
		process_from_command(shell_command("exec sleep 5"))
	}

	fn response_command(responses: &[Response]) -> Command {
		use std::fmt::Write as _;

		let mut escaped = String::new();
		for response in responses {
			for byte in bincode::serialize(response).unwrap() {
				write!(&mut escaped, "\\0{byte:03o}").unwrap();
			}
		}
		shell_command(&format!("printf '%b' '{escaped}'; exec sleep 5"))
	}

	fn version_response() -> Response {
		Response::Version(protocol::VersionResponse {
			version: "test".into(),
		})
	}

	fn assert_reaped(pid: u32) {
		let status = Command::new("kill")
			.args(["-0", &pid.to_string()])
			.stdout(Stdio::null())
			.stderr(Stdio::null())
			.status()
			.unwrap();
		assert!(!status.success(), "child {pid} is still alive or unreaped");
	}

	#[tokio::test]
	async fn communication_timeout_kills_and_reaps_child() {
		let mut process = sleeping_process();
		let pid = process.child.as_ref().unwrap().id();
		assert!(tokio::time::timeout(
			COMMUNICATION_TIMEOUT,
			process.communicate(Request::Version, None),
		)
		.await
		.is_err());
		assert!(process.child.is_none());
		assert_reaped(pid);
	}

	#[tokio::test]
	async fn cancelling_blocked_buffered_write_reaps_child() {
		let mut process = sleeping_process();
		let pid = process.child.as_ref().unwrap().id();
		let request = Request::Render {
			code: String::new(),
			attachments: vec![protocol::Attachment {
				name: "large.bin".into(),
				data: vec![0; 1024 * 1024],
			}],
		};
		assert!(
			tokio::time::timeout(COMMUNICATION_TIMEOUT, process.communicate(request, None),)
				.await
				.is_err()
		);
		assert!(process.child.is_none());
		assert_reaped(pid);
	}

	#[tokio::test]
	async fn cancelling_communication_with_blocked_progress_reaps_child() {
		let mut process = process_from_command(response_command(&[
			Response::Progress("one".into()),
			Response::Progress("two".into()),
			Response::Progress("three".into()),
			Response::Progress("four".into()),
		]));
		let pid = process.child.as_ref().unwrap().id();
		let (send, mut recv) = mpsc::channel(1);
		send.send("full".into()).await.unwrap();
		assert!(tokio::time::timeout(
			COMMUNICATION_TIMEOUT,
			process.communicate(Request::Version, Some(send)),
		)
		.await
		.is_err());
		assert!(process.child.is_none());
		assert_reaped(pid);
		// Keep the external receiver alive: cancellation must close the internal one.
		assert_eq!(recv.recv().await.as_deref(), Some("full"));
	}

	#[tokio::test]
	async fn aborting_communication_task_kills_and_reaps_child() {
		let mut process = sleeping_process();
		let pid = process.child.as_ref().unwrap().id();
		let task = tokio::spawn(async move { process.communicate(Request::Version, None).await });
		tokio::time::sleep(COMMUNICATION_TIMEOUT).await;
		task.abort();
		assert!(tokio::time::timeout(TEST_TIMEOUT, task)
			.await
			.unwrap()
			.unwrap_err()
			.is_cancelled());
		assert_reaped(pid);
	}

	#[tokio::test]
	async fn successful_communication_restores_pipes_for_reuse() {
		let mut process =
			process_from_command(response_command(&[version_response(), version_response()]));
		let pid = process.child.as_ref().unwrap().id();
		for _ in 0..2 {
			let response =
				tokio::time::timeout(TEST_TIMEOUT, process.communicate(Request::Version, None))
					.await
					.unwrap()
					.unwrap();
			assert!(matches!(response, Response::Version(_)));
			assert_eq!(process.child.as_ref().unwrap().id(), pid);
		}
		drop(process);
		assert_reaped(pid);
	}

	#[tokio::test]
	async fn failed_replacement_retires_old_child() {
		let mut process = sleeping_process();
		let pid = process.child.as_ref().unwrap().id();
		let missing = std::env::temp_dir().join(format!("typst-missing-worker-{pid}"));
		let result = tokio::time::timeout(TEST_TIMEOUT, process.replace_command(Command::new(missing)))
			.await
			.unwrap();
		assert!(result.is_err());
		assert!(process.child.is_none());
		assert_reaped(pid);
	}

	#[tokio::test]
	async fn successful_replacement_reaps_old_child() {
		let mut process = sleeping_process();
		let old_pid = process.child.as_ref().unwrap().id();
		tokio::time::timeout(
			TEST_TIMEOUT,
			process.replace_command(response_command(&[version_response()])),
		)
		.await
		.unwrap()
		.unwrap();
		let new_pid = process.child.as_ref().unwrap().id();
		assert_ne!(old_pid, new_pid);
		assert_reaped(old_pid);
		drop(process);
		assert_reaped(new_pid);
	}

	#[tokio::test]
	async fn cancelled_health_check_reaps_new_child() {
		struct PidFile(std::path::PathBuf);
		impl Drop for PidFile {
			fn drop(&mut self) {
				_ = std::fs::remove_file(&self.0);
			}
		}

		let mut process = sleeping_process();
		let old_pid = process.child.as_ref().unwrap().id();
		let pid_file = PidFile(std::env::temp_dir().join(format!("typst-replacement-pid-{old_pid}")));
		std::fs::OpenOptions::new()
			.write(true)
			.create_new(true)
			.open(&pid_file.0)
			.unwrap();
		let mut command = shell_command("printf '%s\\n' $$ > \"$1\"; exec sleep 5");
		command.arg("worker-test").arg(&pid_file.0);
		let mut replacement = Box::pin(process.replace_command(command));
		let new_pid = tokio::time::timeout(TEST_TIMEOUT, async {
			select! {
				result = &mut replacement => panic!("replacement unexpectedly completed: {result:?}"),
				pid = async {
					loop {
						if let Ok(pid) = std::fs::read_to_string(&pid_file.0).unwrap().trim().parse::<u32>() {
							break pid;
						}
						tokio::time::sleep(Duration::from_millis(10)).await;
					}
				} => pid,
			}
		})
		.await
		.unwrap();
		drop(replacement);
		assert!(process.child.is_none());
		assert_reaped(new_pid);
		assert_reaped(old_pid);
	}

	#[tokio::test]
	async fn failed_health_check_retires_old_child() {
		let mut process = sleeping_process();
		let pid = process.child.as_ref().unwrap().id();
		let response = Response::Render(Err("sensitive attachment contents".into()));
		let error = tokio::time::timeout(
			TEST_TIMEOUT,
			process.replace_command(response_command(&[response])),
		)
		.await
		.unwrap()
		.unwrap_err();
		assert!(!format!("{error:?}").contains("sensitive attachment contents"));
		assert!(process.child.is_none());
		assert_reaped(pid);
	}

	#[test]
	fn dropping_idle_process_kills_and_reaps_child() {
		let process = sleeping_process();
		let pid = process.child.as_ref().unwrap().id();
		drop(process);
		assert_reaped(pid);
	}
}
