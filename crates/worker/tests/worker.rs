use std::io::{Cursor, Write as _};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

use protocol::{Request, Response};

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
		},
		Request::Render { code: "#unknown-symbol".into() },
		Request::Render {
			code: "#import \"@preview/worker-test:0.1.0\": greeting\n#set page(width: 200pt, height: 80pt, margin: 10pt)\n#greeting"
				.into(),
		},
		Request::Render {
			code: "#set page(width: 100pt, height: 100pt, margin: 10pt)\n#for i in range(6) { [Page #i]; if i < 5 { pagebreak() } }"
				.into(),
		},
		Request::Render {
			code: "#import \"@preview/worker-test:0.1.0\": load-bad-data\n#load-bad-data()"
				.into(),
		},
		Request::Render {
			code: "#let ink = color.spot(\"Test Ink\", red)\n#rect(fill: gradient.linear(\n  red,\n  blue,\n  space: ink,\n))"
				.into(),
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
