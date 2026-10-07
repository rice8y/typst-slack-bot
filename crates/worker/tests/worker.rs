use std::io::{BufReader, Cursor, Write as _};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

use protocol::{
	Attachment, Rendered, Request, Response, MAX_ATTACHMENTS, MAX_ATTACHMENT_BYTES,
	MAX_TOTAL_ATTACHMENT_BYTES,
};

struct TestCache(PathBuf);

impl TestCache {
	fn new() -> Self {
		let nonce = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.unwrap()
			.as_nanos();
		let path = std::env::temp_dir().join(format!("typst-worker-{}-{nonce}", std::process::id()));
		std::fs::create_dir_all(&path).unwrap();
		Self(path)
	}
}

impl Drop for TestCache {
	fn drop(&mut self) {
		let _ = std::fs::remove_dir_all(&self.0);
	}
}

#[test]
fn worker_compiles_with_typst_0_15_1() {
	let cache = TestCache::new();
	let package = cache.0.join("preview/worker-test/0.1.0");
	std::fs::create_dir_all(&package).unwrap();
	std::fs::write(
		package.join("typst.toml"),
		"[package]\nname = \"worker-test\"\nversion = \"0.1.0\"\nentrypoint = \"lib.typ\"\n",
	)
	.unwrap();
	std::fs::write(package.join("bad.toml"), b"\xef\xbb\xbfkey = ?").unwrap();
	std::fs::write(
		package.join("lib.typ"),
		"#let greeting = [Hello from a package]\n#let load-bad-data() = toml(\"bad.toml\")",
	)
	.unwrap();

	let mut worker = Command::new(env!("CARGO_BIN_EXE_worker"))
		.env("CACHE_DIRECTORY", &cache.0)
		.stdin(Stdio::piped())
		.stdout(Stdio::piped())
		.spawn()
		.unwrap();
	let mut stdin = worker.stdin.take().unwrap();
	let requests = [
		Request::Version,
		Request::Ast { code: "= Test".into() },
		Request::Render {
			code: "#set page(width: 150pt, height: 150pt, margin: 10pt)\n= Test\n#datetime.today()\n#datetime.today(offset: duration(hours: 5, minutes: 30))"
				.into(),
			attachments: Vec::new(),
		},
		Request::Render { code: "#unknown-symbol".into(), attachments: Vec::new() },
		Request::Render {
			code: "#import \"@preview/worker-test:0.1.0\": greeting\n#set page(width: 200pt, height: 80pt, margin: 10pt)\n#greeting"
				.into(),
			attachments: Vec::new(),
		},
		Request::Render {
			code: "#set page(width: 100pt, height: 100pt, margin: 10pt)\n#for i in range(6) { [Page #i]; if i < 5 { pagebreak() } }"
				.into(),
			attachments: Vec::new(),
		},
		Request::Render {
			code: "#import \"@preview/worker-test:0.1.0\": load-bad-data\n#load-bad-data()"
				.into(),
			attachments: Vec::new(),
		},
		Request::Render {
			code: "#let ink = color.spot(\"Test Ink\", red)\n#rect(fill: gradient.linear(\n  red,\n  blue,\n  space: ink,\n))"
				.into(),
			attachments: Vec::new(),
		},
	];
	for request in &requests {
		bincode::serialize_into(&mut stdin, request).unwrap();
	}
	stdin.flush().unwrap();
	drop(stdin);

	let output = worker.wait_with_output().unwrap();
	assert!(output.status.success());
	let mut stdout = Cursor::new(output.stdout);
	let responses: Vec<Response> = (0..requests.len())
		.map(|_| bincode::deserialize_from(&mut stdout).unwrap())
		.collect();
	assert_eq!(stdout.position(), stdout.get_ref().len() as u64);

	let Response::Version(version) = &responses[0] else {
		panic!("expected version")
	};
	assert_eq!(version.version, "0.15.1");
	let Response::Ast(ast) = &responses[1] else {
		panic!("expected AST")
	};
	assert!(ast.contains("Heading"));

	for (index, width, height) in [(2, 750, 750), (4, 1000, 400)] {
		let Response::Render(Ok(rendered)) = &responses[index] else {
			panic!("expected rendered image: {:?}", responses[index]);
		};
		assert_eq!(rendered.images.len(), 1, "request {index}");
		assert_eq!(rendered.more_pages, 0);
		assert!(rendered.warnings.is_empty());
		let image = image::load_from_memory(&rendered.images[0])
			.unwrap()
			.to_rgba8();
		assert_eq!((image.width(), image.height()), (width, height));
		assert_eq!(image.get_pixel(0, 0).0, [255, 255, 255, 255]);
		assert!(image
			.pixels()
			.any(|pixel| pixel.0[0] < 128 && pixel.0[3] > 0));
	}

	let Response::Render(Err(diagnostic)) = &responses[3] else {
		panic!("expected diagnostic");
	};
	assert!(diagnostic.contains("unknown variable: unknown-symbol"));
	assert!(diagnostic.contains("main.typ:1:"), "{diagnostic}");
	assert!(diagnostic.contains("Help:"));
	assert!(!diagnostic.contains('\u{1b}'));

	let Response::Render(Ok(rendered)) = &responses[5] else {
		panic!("expected multi-page document: {:?}", responses[5]);
	};
	assert_eq!(rendered.images.len(), 5);
	assert_eq!(rendered.more_pages, 1);

	let Response::Render(Err(diagnostic)) = &responses[6] else {
		panic!("expected external data diagnostic: {:?}", responses[6]);
	};
	assert!(diagnostic.contains("failed to parse TOML"), "{diagnostic}");
	assert!(diagnostic.contains("bad.toml:1:7"), "{diagnostic}");

	let Response::Render(Err(diagnostic)) = &responses[7] else {
		panic!("expected gradient diagnostic: {:?}", responses[7]);
	};
	assert!(diagnostic.contains("space: ink"), "{diagnostic}");
	let hint = diagnostic
		.lines()
		.find(|line| line.contains("gradient color mixing space specified here"))
		.unwrap_or_else(|| panic!("missing spanned hint: {diagnostic}"));
	assert!(!hint.contains("Help:"), "{diagnostic}");
}

struct TestWorker {
	child: Child,
	stdin: Option<ChildStdin>,
	stdout: BufReader<ChildStdout>,
}

impl TestWorker {
	fn new(cache: &TestCache, working_directory: &TestCache) -> Self {
		let mut child = Command::new(env!("CARGO_BIN_EXE_worker"))
			.env("CACHE_DIRECTORY", &cache.0)
			.current_dir(&working_directory.0)
			.stdin(Stdio::piped())
			.stdout(Stdio::piped())
			.spawn()
			.unwrap();
		Self {
			stdin: child.stdin.take(),
			stdout: BufReader::new(child.stdout.take().unwrap()),
			child,
		}
	}

	fn render(&mut self, code: &str, attachments: Vec<Attachment>) -> Result<Rendered, String> {
		{
			let mut stdin = std::io::BufWriter::new(self.stdin.as_mut().unwrap());
			bincode::serialize_into(
				&mut stdin,
				&Request::Render {
					code: code.into(),
					attachments,
				},
			)
			.unwrap();
			stdin.flush().unwrap();
		}
		loop {
			match bincode::deserialize_from(&mut self.stdout).unwrap() {
				Response::Render(result) => return result,
				Response::Progress(_) => {}
				response => panic!("expected render response: {response:?}"),
			}
		}
	}

	fn finish(mut self) {
		drop(self.stdin.take());
		assert!(self.child.wait().unwrap().success());
	}
}

impl Drop for TestWorker {
	fn drop(&mut self) {
		let _ = self.child.kill();
		let _ = self.child.wait();
	}
}

fn attachment(name: &str, data: impl Into<Vec<u8>>) -> Attachment {
	Attachment {
		name: name.into(),
		data: data.into(),
	}
}

fn png(color: [u8; 4]) -> Vec<u8> {
	let mut buffer = Cursor::new(Vec::new());
	image::write_buffer_with_format(
		&mut buffer,
		&color,
		1,
		1,
		image::ColorType::Rgba8,
		image::ImageFormat::Png,
	)
	.unwrap();
	buffer.into_inner()
}

fn assert_cache_empty(cache: &TestCache) {
	assert!(
		std::fs::read_dir(&cache.0).unwrap().next().is_none(),
		"attachments must not create files in the package cache"
	);
}

fn assert_missing(result: Result<Rendered, String>, name: &str) {
	let diagnostic = result.unwrap_err();
	assert!(diagnostic.contains("file not found"), "{diagnostic}");
	assert!(diagnostic.contains(name), "{diagnostic}");
}

const SMALL_PAGE: &str = "#set page(width: 100pt, height: 100pt, margin: 10pt)\n";

#[test]
fn worker_loads_request_local_source_image_and_csv() {
	let cache = TestCache::new();
	let mut worker = TestWorker::new(&cache, &cache);
	let code = format!("{SMALL_PAGE}#import \"lib.typ\": content\n#content");
	let rendered = worker.render(&code, vec![
		attachment("lib.typ", b"\xef\xbb\xbf#import \"value.typ\": value\n#let rows = csv(\"data.csv\")\n#assert(rows.at(1).at(0) == value)\n#let content = [#image(\"image.png\", width: 20pt) #value]".to_vec()),
		attachment("value.typ", b"#let value = \"attached\"".to_vec()),
		attachment("data.csv", b"name\nattached\n".to_vec()),
		attachment("image.png", png([255, 0, 0, 255])),
	]).unwrap();
	assert_eq!(rendered.images.len(), 1);
	assert!(rendered.warnings.is_empty());
	let image = image::load_from_memory(&rendered.images[0])
		.unwrap()
		.to_rgba8();
	assert!(image
		.pixels()
		.any(|pixel| pixel.0[0] > 200 && pixel.0[1] < 50));
	worker.finish();
	assert_cache_empty(&cache);
}

#[test]
fn worker_isolates_sequential_source_image_and_csv_attachments() {
	let cache = TestCache::new();
	let mut worker = TestWorker::new(&cache, &cache);
	let cases = [
		(
			"value.typ",
			"#import \"value.typ\": value\n#value",
			b"#let value = \"first\"".to_vec(),
			b"#let value = \"second\"".to_vec(),
		),
		(
			"image.png",
			"#image(\"image.png\", width: 30pt)",
			png([255, 0, 0, 255]),
			png([0, 0, 255, 255]),
		),
		(
			"data.csv",
			"#csv(\"data.csv\").at(0).at(0)",
			b"first\n".to_vec(),
			b"second\n".to_vec(),
		),
	];
	for (name, body, first, second) in cases {
		let code = format!("{SMALL_PAGE}{body}");
		let first = worker.render(&code, vec![attachment(name, first)]).unwrap();
		let second = worker
			.render(&code, vec![attachment(name, second)])
			.unwrap();
		assert_eq!(first.images.len(), 1);
		assert_eq!(second.images.len(), 1);
		assert_ne!(
			first.images, second.images,
			"cached contents reused for {name}"
		);
		assert_missing(worker.render(&code, Vec::new()), name);
		assert_missing(
			worker.render(&code, vec![attachment("unrelated.txt", Vec::new())]),
			name,
		);
		assert_cache_empty(&cache);
	}
	worker.finish();
	assert_cache_empty(&cache);
}

#[test]
fn worker_drops_attachments_after_compile_failure() {
	let cache = TestCache::new();
	let mut worker = TestWorker::new(&cache, &cache);
	let code = "#import \"bad.typ\": value\n#value";
	let diagnostic = worker
		.render(
			code,
			vec![attachment(
				"bad.typ",
				b"#let value = unknown-symbol".to_vec(),
			)],
		)
		.unwrap_err();
	assert!(
		diagnostic.contains("unknown variable: unknown-symbol"),
		"{diagnostic}"
	);
	assert!(diagnostic.contains("bad.typ:1:"), "{diagnostic}");
	assert_missing(worker.render(code, Vec::new()), "bad.typ");
	worker
		.render(
			code,
			vec![attachment("bad.typ", b"#let value = [Recovered]".to_vec())],
		)
		.unwrap();
	assert_missing(worker.render(code, Vec::new()), "bad.typ");
	worker.finish();
	assert_cache_empty(&cache);
}

#[test]
fn worker_formats_attached_data_diagnostics_and_drops_invalid_utf8_sources() {
	let cache = TestCache::new();
	let mut worker = TestWorker::new(&cache, &cache);
	let diagnostic = worker
		.render(
			"#toml(\"bad.toml\")",
			vec![attachment("bad.toml", b"\xef\xbb\xbfkey = ?".to_vec())],
		)
		.unwrap_err();
	assert!(diagnostic.contains("failed to parse TOML"), "{diagnostic}");
	assert!(diagnostic.contains("bad.toml:1:7"), "{diagnostic}");
	assert_missing(worker.render("#toml(\"bad.toml\")", Vec::new()), "bad.toml");
	assert!(worker
		.render(
			"#include \"invalid.typ\"",
			vec![attachment("invalid.typ", vec![0xff]),]
		)
		.is_err());
	assert_missing(
		worker.render("#include \"invalid.typ\"", Vec::new()),
		"invalid.typ",
	);
	worker.finish();
	assert_cache_empty(&cache);
}

#[test]
fn worker_rejects_invalid_attachment_names_and_duplicates() {
	let cache = TestCache::new();
	let mut worker = TestWorker::new(&cache, &cache);
	for name in [
		"",
		".",
		"..",
		"main.typ",
		"/absolute.typ",
		"../parent.typ",
		"sub/file.typ",
		"sub\\file.typ",
		"C:\\absolute.typ",
		"C:drive.typ",
		"nul\0.typ",
		"line\n.typ",
		"tab\t.typ",
		"del\u{7f}.typ",
		"control\u{85}.typ",
	] {
		let diagnostic = worker
			.render("Valid source", vec![attachment(name, Vec::new())])
			.unwrap_err();
		assert!(
			diagnostic.contains("invalid attachment name"),
			"{name:?}: {diagnostic}"
		);
	}
	let diagnostic = worker
		.render(
			"Valid source",
			vec![
				attachment("same.typ", b"First".to_vec()),
				attachment("same.typ", b"Second".to_vec()),
			],
		)
		.unwrap_err();
	assert!(
		diagnostic.contains("duplicate attachment name"),
		"{diagnostic}"
	);
	assert_missing(
		worker.render("#include \"same.typ\"", Vec::new()),
		"same.typ",
	);
	worker.render("Still alive", Vec::new()).unwrap();
	worker.finish();
	assert_cache_empty(&cache);
}

#[test]
fn worker_enforces_attachment_count_and_byte_limits() {
	let cache = TestCache::new();
	let mut worker = TestWorker::new(&cache, &cache);
	let diagnostic = worker
		.render(
			"Valid source",
			(0..=MAX_ATTACHMENTS)
				.map(|index| attachment(&format!("file{index}.txt"), Vec::new()))
				.collect(),
		)
		.unwrap_err();
	assert!(diagnostic.contains("too many attachments"), "{diagnostic}");
	let diagnostic = worker
		.render(
			"Valid source",
			vec![attachment(
				"oversize.txt",
				vec![0; MAX_ATTACHMENT_BYTES + 1],
			)],
		)
		.unwrap_err();
	assert!(diagnostic.contains("is too large"), "{diagnostic}");
	let diagnostic = worker
		.render(
			"Valid source",
			vec![
				attachment("first.bin", vec![0; MAX_ATTACHMENT_BYTES]),
				attachment("second.bin", vec![0; MAX_ATTACHMENT_BYTES]),
				attachment(
					"last.bin",
					vec![0; MAX_TOTAL_ATTACHMENT_BYTES - 2 * MAX_ATTACHMENT_BYTES + 1],
				),
			],
		)
		.unwrap_err();
	assert!(diagnostic.contains("maximum total"), "{diagnostic}");
	assert_missing(
		worker.render("#read(\"first.bin\")", Vec::new()),
		"first.bin",
	);
	worker
		.render(
			"Valid source",
			vec![
				attachment("first.bin", vec![0; MAX_ATTACHMENT_BYTES]),
				attachment("second.bin", vec![0; MAX_ATTACHMENT_BYTES]),
			],
		)
		.unwrap();
	worker
		.render(
			"Valid source",
			(0..MAX_ATTACHMENTS)
				.map(|index| attachment(&format!("file{index}.txt"), Vec::new()))
				.collect(),
		)
		.unwrap();
	worker.finish();
	assert_cache_empty(&cache);
}

#[test]
fn worker_never_reads_host_project_files() {
	let cache = TestCache::new();
	let host = TestCache::new();
	std::fs::write(host.0.join("secret.typ"), "#let secret = [Host source]").unwrap();
	std::fs::write(host.0.join("secret.txt"), "Host data").unwrap();
	std::fs::write(host.0.join("secret.csv"), "Host,data\n").unwrap();
	std::fs::write(host.0.join("secret.png"), png([255, 0, 0, 255])).unwrap();
	let mut worker = TestWorker::new(&cache, &host);
	for (code, name) in [
		("#import \"secret.typ\": secret\n#secret", "secret.typ"),
		("#read(\"secret.txt\")", "secret.txt"),
		("#csv(\"secret.csv\")", "secret.csv"),
		("#image(\"secret.png\")", "secret.png"),
	] {
		assert_missing(worker.render(code, Vec::new()), name);
		assert_missing(
			worker.render(code, vec![attachment("unrelated.txt", Vec::new())]),
			name,
		);
	}
	let absolute = format!("#read(\"{}\")", host.0.join("secret.txt").display());
	assert_missing(worker.render(&absolute, Vec::new()), "secret.txt");
	assert!(worker
		.render("#read(\"../secret.txt\")", Vec::new())
		.is_err());
	assert_missing(
		worker.render(
			"#include \"lib.typ\"",
			vec![attachment("lib.typ", b"#read(\"secret.txt\")".to_vec())],
		),
		"secret.txt",
	);
	worker.finish();
	assert_cache_empty(&cache);
}

#[test]
fn worker_keeps_attachments_separate_from_package_files() {
	let cache = TestCache::new();
	let package = cache.0.join("preview/worker-test/0.1.0");
	std::fs::create_dir_all(&package).unwrap();
	std::fs::write(
		package.join("typst.toml"),
		"[package]\nname = \"worker-test\"\nversion = \"0.1.0\"\nentrypoint = \"lib.typ\"\n",
	)
	.unwrap();
	std::fs::write(
		package.join("lib.typ"),
		"#let value = csv(\"data.csv\").at(0).at(0)",
	)
	.unwrap();
	std::fs::write(package.join("data.csv"), "package\n").unwrap();
	let mut worker = TestWorker::new(&cache, &cache);
	let code = format!("{SMALL_PAGE}#import \"@preview/worker-test:0.1.0\": value\n#assert(value == \"package\")\n#assert(csv(\"data.csv\").at(0).at(0) == \"attached\")\n#value");
	worker
		.render(
			&code,
			vec![
				attachment("lib.typ", b"#unknown-symbol".to_vec()),
				attachment("data.csv", b"attached\n".to_vec()),
			],
		)
		.unwrap();
	assert_missing(worker.render(&code, Vec::new()), "data.csv");
	worker
		.render(
			"#import \"@preview/worker-test:0.1.0\": value\n#assert(value == \"package\")\n#value",
			Vec::new(),
		)
		.unwrap();
	worker.finish();
	assert!(!cache.0.join("lib.typ").exists());
	assert!(!cache.0.join("data.csv").exists());
	let mut files = std::fs::read_dir(&package)
		.unwrap()
		.map(|entry| entry.unwrap().file_name().into_string().unwrap())
		.collect::<Vec<_>>();
	files.sort();
	assert_eq!(files, ["data.csv", "lib.typ", "typst.toml"]);
	assert_eq!(
		std::fs::read_to_string(package.join("data.csv")).unwrap(),
		"package\n"
	);
}
