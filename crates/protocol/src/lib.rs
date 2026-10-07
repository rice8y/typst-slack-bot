use serde::{Deserialize, Serialize};

pub const MAX_ATTACHMENTS: usize = 10;
pub const MAX_ATTACHMENT_BYTES: usize = 10 * 1024 * 1024;
pub const MAX_TOTAL_ATTACHMENT_BYTES: usize = 20 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Attachment {
	pub name: String,
	pub data: Vec<u8>,
}

/// Accept only plain basenames, including on platforms with drive-letter paths.
pub fn is_valid_attachment_name(name: &str) -> bool {
	!name.is_empty()
		&& !matches!(name, "." | ".." | "main.typ")
		&& !name
			.chars()
			.any(|ch| ch.is_control() || matches!(ch, '/' | '\\' | ':'))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
	Render {
		code: String,
		attachments: Vec<Attachment>,
	},
	Ast {
		code: String,
	},
	Version,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Rendered {
	pub images: Vec<Vec<u8>>,
	pub more_pages: usize,
	pub warnings: String,
}

pub type RenderResponse = Result<Rendered, String>;

pub type AstResponse = String;

#[derive(Debug, Serialize, Deserialize)]
pub struct VersionResponse {
	pub version: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
	Render(RenderResponse),
	Ast(AstResponse),
	Version(VersionResponse),
	/// This can be sent at any time and is not considered a final response for a request,
	/// but can be shown to the user in the meantime as a progress update.
	Progress(String),
}

#[cfg(test)]
mod tests {
	use super::is_valid_attachment_name;

	#[test]
	fn attachment_names_are_plain_basenames() {
		for name in ["image.png", "data.csv", "lib.typ", ".hidden", "my file.typ"] {
			assert!(is_valid_attachment_name(name), "{name:?}");
		}
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
			"nul\0.typ",
			"line\n.typ",
			"tab\t.typ",
			"del\u{7f}.typ",
			"control\u{85}.typ",
		] {
			assert!(!is_valid_attachment_name(name), "{name:?}");
		}
	}
}
