//! Clipboard read for `/paste` and image staging.
//!
//! Out of process with a time limit (ADR 0023). The TUI does not link a
//! clipboard crate. A missing helper is a system line that names `/attach`.

use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// ADR 0006 part bound; same ceiling `/attach` uses.
pub(crate) const MAX_ATTACH_BYTES: u64 = 8 * 1024 * 1024;

const HELPER_TIMEOUT: Duration = Duration::from_secs(2);

/// What a clipboard helper (or a test fixture) returned.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ClipboardPayload {
    Image(Vec<u8>),
    Text(String),
    Empty,
    MissingHelper,
    Unsupported,
    Oversize,
}

/// OS helper, or a pinned payload in tests.
#[derive(Clone, Debug)]
pub(crate) enum ClipboardSource {
    Os,
    #[cfg(test)]
    Fixed(ClipboardPayload),
}

pub(crate) fn read_clipboard(source: &ClipboardSource) -> ClipboardPayload {
    match source {
        ClipboardSource::Os => read_os_clipboard(),
        #[cfg(test)]
        ClipboardSource::Fixed(payload) => payload.clone(),
    }
}

/// System line when the clipboard cannot become an attach part.
pub(crate) fn clipboard_refuse_line(payload: &ClipboardPayload) -> Option<&'static str> {
    match payload {
        ClipboardPayload::Empty => Some("clipboard is empty; use /attach for a file"),
        ClipboardPayload::MissingHelper => Some("no clipboard helper; use /attach for a file"),
        ClipboardPayload::Unsupported => Some("clipboard is not an image; use /attach for a file"),
        ClipboardPayload::Oversize => Some("clipboard exceeds 8MiB; use /attach for a file"),
        ClipboardPayload::Image(_) | ClipboardPayload::Text(_) => None,
    }
}

pub(crate) fn image_mime(bytes: &[u8]) -> Option<&'static str> {
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        Some("image/png")
    } else if bytes.len() >= 3 && bytes[0] == 0xff && bytes[1] == 0xd8 && bytes[2] == 0xff {
        Some("image/jpeg")
    } else if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        Some("image/gif")
    } else if bytes.len() >= 12 && bytes.starts_with(b"RIFF") && &bytes[8..12] == b"WEBP" {
        Some("image/webp")
    } else {
        None
    }
}

pub(crate) fn image_filename(mime: &str) -> &'static str {
    match mime {
        "image/jpeg" => "clipboard.jpg",
        "image/gif" => "clipboard.gif",
        "image/webp" => "clipboard.webp",
        _ => "clipboard.png",
    }
}

/// A one-line paste that names an existing image file, for terminals that
/// paste a path instead of bytes.
pub(crate) fn pasted_image_path(text: &str, cwd: &Path) -> Option<PathBuf> {
    let trimmed = text.trim();
    if trimmed.is_empty() || trimmed.contains('\n') || trimmed.contains('\r') {
        return None;
    }
    let unquoted = trim_matching_quotes(trimmed);
    if unquoted.is_empty() {
        return None;
    }
    let path = {
        let candidate = Path::new(unquoted);
        if candidate.is_absolute() {
            candidate.to_path_buf()
        } else {
            cwd.join(candidate)
        }
    };
    let ext = path
        .extension()
        .and_then(|ext| ext.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    matches!(ext.as_str(), "png" | "jpg" | "jpeg" | "gif" | "webp").then_some(path)
}

fn trim_matching_quotes(text: &str) -> &str {
    let bytes = text.as_bytes();
    if bytes.len() >= 2 {
        let (first, last) = (bytes[0], bytes[bytes.len() - 1]);
        if (first == b'"' && last == b'"') || (first == b'\'' && last == b'\'') {
            return &text[1..text.len() - 1];
        }
    }
    text
}

fn read_os_clipboard() -> ClipboardPayload {
    if let Some(payload) = read_os_image() {
        return payload;
    }
    read_os_text()
}

fn read_os_image() -> Option<ClipboardPayload> {
    #[cfg(target_os = "macos")]
    {
        macos_image()
    }
    #[cfg(not(target_os = "macos"))]
    {
        linux_image()
    }
}

fn read_os_text() -> ClipboardPayload {
    #[cfg(target_os = "macos")]
    {
        text_from_helper(run_helper("pbpaste", &[], MAX_ATTACH_BYTES + 1))
    }
    #[cfg(not(target_os = "macos"))]
    {
        let mut saw_helper = false;
        for (cmd, args) in [
            ("wl-paste", &["--no-newline", "--type", "text/plain"][..]),
            ("xclip", &["-selection", "clipboard", "-o"][..]),
        ] {
            match run_helper(cmd, args, MAX_ATTACH_BYTES + 1) {
                Err(HelperErr::NotFound) => {}
                other => {
                    saw_helper = true;
                    return text_from_helper(other);
                }
            }
        }
        if saw_helper {
            ClipboardPayload::Empty
        } else {
            ClipboardPayload::MissingHelper
        }
    }
}

#[cfg(target_os = "macos")]
fn macos_image() -> Option<ClipboardPayload> {
    let script = "try\nget the clipboard as «class PNGf»\non error\nreturn \"\"\nend try";
    // Hex is two characters per byte, plus the «data PNGf…» wrapper.
    let cap = MAX_ATTACH_BYTES.saturating_mul(2).saturating_add(64);
    match run_helper("osascript", &["-e", script], cap) {
        Err(HelperErr::NotFound) => None,
        Err(HelperErr::Timeout) => Some(ClipboardPayload::MissingHelper),
        Err(HelperErr::Failed) => None,
        Ok(stdout) => decode_osascript_png(&stdout).map(payload_from_image_bytes),
    }
}

#[cfg(not(target_os = "macos"))]
fn linux_image() -> Option<ClipboardPayload> {
    const TYPES: &[&str] = &["image/png", "image/jpeg", "image/gif", "image/webp"];
    for mime in TYPES {
        match run_helper("wl-paste", &["--type", mime], MAX_ATTACH_BYTES + 1) {
            Err(HelperErr::NotFound) => break,
            Err(HelperErr::Timeout) => return Some(ClipboardPayload::MissingHelper),
            Err(HelperErr::Failed) => {}
            Ok(bytes) => {
                if let Some(payload) = image_payload_or_skip(bytes) {
                    return Some(payload);
                }
            }
        }
    }
    for mime in TYPES {
        match run_helper(
            "xclip",
            &["-selection", "clipboard", "-t", mime, "-o"],
            MAX_ATTACH_BYTES + 1,
        ) {
            Err(HelperErr::NotFound) => break,
            Err(HelperErr::Timeout) => return Some(ClipboardPayload::MissingHelper),
            Err(HelperErr::Failed) => {}
            Ok(bytes) => {
                if let Some(payload) = image_payload_or_skip(bytes) {
                    return Some(payload);
                }
            }
        }
    }
    None
}

#[cfg(not(target_os = "macos"))]
fn image_payload_or_skip(bytes: Vec<u8>) -> Option<ClipboardPayload> {
    if bytes.is_empty() {
        return None;
    }
    Some(payload_from_image_bytes(bytes))
}

fn payload_from_image_bytes(bytes: Vec<u8>) -> ClipboardPayload {
    if bytes.len() as u64 > MAX_ATTACH_BYTES {
        return ClipboardPayload::Oversize;
    }
    if bytes.is_empty() {
        return ClipboardPayload::Empty;
    }
    if image_mime(&bytes).is_some() {
        ClipboardPayload::Image(bytes)
    } else {
        ClipboardPayload::Unsupported
    }
}

fn text_from_helper(result: Result<Vec<u8>, HelperErr>) -> ClipboardPayload {
    match result {
        Err(HelperErr::NotFound) | Err(HelperErr::Timeout) => ClipboardPayload::MissingHelper,
        Err(HelperErr::Failed) => ClipboardPayload::Empty,
        Ok(bytes) if bytes.is_empty() => ClipboardPayload::Empty,
        Ok(bytes) if bytes.len() as u64 > MAX_ATTACH_BYTES => ClipboardPayload::Oversize,
        Ok(bytes) => ClipboardPayload::Text(String::from_utf8_lossy(&bytes).into_owned()),
    }
}

#[derive(Debug)]
enum HelperErr {
    NotFound,
    Failed,
    Timeout,
}

fn run_helper(command: &str, args: &[&str], cap: u64) -> Result<Vec<u8>, HelperErr> {
    let mut child = match Command::new(command)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(child) => child,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Err(HelperErr::NotFound),
        Err(_) => return Err(HelperErr::Failed),
    };
    let stdout = child.stdout.take().ok_or(HelperErr::Failed)?;
    let reader = std::thread::spawn(move || {
        let mut buf = Vec::new();
        let mut limited = stdout.take(cap);
        limited.read_to_end(&mut buf).map(|_| buf)
    });
    let started = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                let buf = reader.join().ok().and_then(Result::ok).unwrap_or_default();
                if buf.len() as u64 >= cap {
                    return Ok(buf);
                }
                if status.success() || !buf.is_empty() {
                    return Ok(buf);
                }
                return Err(HelperErr::Failed);
            }
            Ok(None) if started.elapsed() >= HELPER_TIMEOUT => {
                let _ = child.kill();
                let _ = child.wait();
                let _ = reader.join();
                return Err(HelperErr::Timeout);
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(10)),
            Err(_) => return Err(HelperErr::Failed),
        }
    }
}

/// `osascript` prints `«data PNGf<hex>»` for a PNG clipboard.
pub(crate) fn decode_osascript_png(stdout: &[u8]) -> Option<Vec<u8>> {
    let text = std::str::from_utf8(stdout).ok()?.trim();
    if text.is_empty() {
        return None;
    }
    let start = text.find("PNGf")? + 4;
    let hex: String = text[start..]
        .chars()
        .take_while(|ch| ch.is_ascii_hexdigit())
        .collect();
    decode_hex(&hex)
}

fn decode_hex(hex: &str) -> Option<Vec<u8>> {
    if hex.is_empty() || !hex.len().is_multiple_of(2) {
        return None;
    }
    let mut out = Vec::with_capacity(hex.len() / 2);
    let bytes = hex.as_bytes();
    for chunk in bytes.chunks_exact(2) {
        let hi = from_hex(chunk[0])?;
        let lo = from_hex(chunk[1])?;
        out.push((hi << 4) | lo);
    }
    Some(out)
}

fn from_hex(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn png_magic_is_image_png() {
        let bytes = b"\x89PNG\r\n\x1a\n UNIQUE-TUI-702";
        assert_eq!(image_mime(bytes), Some("image/png"));
        assert_eq!(image_filename("image/png"), "clipboard.png");
    }

    #[test]
    fn jpeg_gif_webp_magic() {
        assert_eq!(image_mime(b"\xff\xd8\xff\xe0rest"), Some("image/jpeg"));
        assert_eq!(image_mime(b"GIF89a...."), Some("image/gif"));
        let mut webp = b"RIFF".to_vec();
        webp.extend_from_slice(&[0, 0, 0, 0]);
        webp.extend_from_slice(b"WEBPrest");
        assert_eq!(image_mime(&webp), Some("image/webp"));
        assert_eq!(image_mime(b"plain text"), None);
    }

    #[test]
    fn osascript_hex_roundtrip() {
        let png = b"\x89PNG\r\n\x1a\n";
        let hex: String = png.iter().map(|b| format!("{b:02X}")).collect();
        let stdout = format!("«data PNGf{hex}»");
        assert_eq!(
            decode_osascript_png(stdout.as_bytes()).as_deref(),
            Some(&png[..])
        );
        assert_eq!(decode_osascript_png(b""), None);
        assert_eq!(decode_osascript_png(b"missing"), None);
    }

    #[test]
    fn pasted_image_path_is_single_line_image() {
        let cwd = Path::new("/work");
        assert_eq!(
            pasted_image_path("shot.png\nmore", cwd),
            None,
            "multiline stays text"
        );
        assert_eq!(
            pasted_image_path("shot.png", cwd).as_deref(),
            Some(Path::new("/work/shot.png"))
        );
        assert_eq!(
            pasted_image_path("\"/tmp/x.JPEG\"", cwd).as_deref(),
            Some(Path::new("/tmp/x.JPEG"))
        );
        assert_eq!(pasted_image_path("notes.txt", cwd), None);
        assert_eq!(pasted_image_path("see shot.png later", cwd), None);
    }
}
