//! Shared bounded framing for MCP stdio adapters.
//!
//! Two framings exist in the wild: LSP-style `Content-Length` headers (the
//! shikigami MCP server and older hosts) and the MCP stdio specification's
//! newline-delimited JSON-RPC (`sekai-mcp`, the reference servers). Writers
//! pick one explicitly; [`read`] accepts either by looking at the first byte
//! of the frame, so a client never mis-parses a compliant server's reply.

use serde_json::Value;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub(crate) const MAX_FRAME_BYTES: usize = 1024 * 1024;
pub(crate) const MAX_HEADER_BYTES: usize = 8 * 1024;

/// Write one `Content-Length` framed message.
pub(crate) async fn write<W: AsyncWrite + Unpin>(
    writer: &mut W,
    message: &Value,
) -> Result<(), String> {
    let framed = encode(message)?;
    writer.write_all(&framed).await.map_err(|e| e.to_string())?;
    writer.flush().await.map_err(|e| e.to_string())
}

/// Write one newline-delimited message (MCP stdio transport).
pub(crate) async fn write_line<W: AsyncWrite + Unpin>(
    writer: &mut W,
    message: &Value,
) -> Result<(), String> {
    let framed = encode_line(message)?;
    writer.write_all(&framed).await.map_err(|e| e.to_string())?;
    writer.flush().await.map_err(|e| e.to_string())
}

/// Read one message in either framing.
///
/// A frame whose first non-whitespace byte is `{` is a newline-delimited
/// JSON-RPC message; anything else is parsed as `Content-Length` headers.
/// Leading JSON whitespace (blank separators between line frames, indentation)
/// is skipped up to [`MAX_HEADER_BYTES`]. Both paths enforce
/// [`MAX_FRAME_BYTES`] before allocating the body.
pub(crate) async fn read<R: AsyncBufRead + Unpin>(reader: &mut R) -> Result<Value, String> {
    let mut skipped = 0usize;
    loop {
        let buffered = reader.fill_buf().await.map_err(|e| e.to_string())?;
        match buffered.first() {
            None => return Err("eof".into()),
            Some(b'{') => return read_line(reader).await,
            Some(b' ' | b'\t' | b'\r' | b'\n') => {
                skipped += 1;
                if skipped > MAX_HEADER_BYTES {
                    return Err(format!(
                        "mcp frame padding exceeds {MAX_HEADER_BYTES} bytes"
                    ));
                }
                reader.consume(1);
            }
            Some(_) => return read_content_length(reader).await,
        }
    }
}

async fn read_line<R: AsyncBufRead + Unpin>(reader: &mut R) -> Result<Value, String> {
    let mut line = Vec::new();
    let n = (&mut *reader)
        .take((MAX_FRAME_BYTES + 1) as u64)
        .read_until(b'\n', &mut line)
        .await
        .map_err(|e| e.to_string())?;
    if n == 0 {
        return Err("eof".into());
    }
    if line.last() != Some(&b'\n') {
        return Err(if line.len() > MAX_FRAME_BYTES {
            format!("mcp frame exceeds {MAX_FRAME_BYTES} bytes")
        } else {
            "mcp unterminated frame".into()
        });
    }
    while matches!(line.last(), Some(b'\n' | b'\r')) {
        line.pop();
    }
    serde_json::from_slice(&line).map_err(|e| e.to_string())
}

async fn read_content_length<R: AsyncBufRead + Unpin>(reader: &mut R) -> Result<Value, String> {
    let mut content_length = None;
    let mut header_bytes = 0usize;
    loop {
        let mut line = String::new();
        let n = (&mut *reader)
            .take((MAX_HEADER_BYTES + 1) as u64)
            .read_line(&mut line)
            .await
            .map_err(|e| e.to_string())?;
        if n == 0 {
            return Err("eof".into());
        }
        header_bytes = header_bytes.saturating_add(n);
        if header_bytes > MAX_HEADER_BYTES {
            return Err(format!("mcp headers exceed {MAX_HEADER_BYTES} bytes"));
        }
        let line = line.trim_end();
        if line.is_empty() {
            break;
        }
        if let Some(rest) = line.strip_prefix("Content-Length:") {
            if content_length.is_some() {
                return Err("mcp duplicate Content-Length".into());
            }
            let length = rest
                .trim()
                .parse::<usize>()
                .map_err(|_| "mcp invalid Content-Length".to_string())?;
            if length > MAX_FRAME_BYTES {
                return Err(format!(
                    "mcp Content-Length {length} exceeds {MAX_FRAME_BYTES} bytes"
                ));
            }
            content_length = Some(length);
        }
    }

    let len = content_length.ok_or_else(|| "mcp missing Content-Length".to_string())?;
    let mut body = vec![0u8; len];
    reader
        .read_exact(&mut body)
        .await
        .map_err(|e| e.to_string())?;
    serde_json::from_slice(&body).map_err(|e| e.to_string())
}

pub(crate) fn encode(message: &Value) -> Result<Vec<u8>, String> {
    let body = serde_json::to_vec(message).map_err(|e| e.to_string())?;
    let header = format!("Content-Length: {}\r\n\r\n", body.len());
    let mut framed = header.into_bytes();
    framed.extend_from_slice(&body);
    Ok(framed)
}

/// Compact serialization escapes newlines inside strings, so the body is a
/// single line by construction; only the size bound needs checking.
pub(crate) fn encode_line(message: &Value) -> Result<Vec<u8>, String> {
    let mut body = serde_json::to_vec(message).map_err(|e| e.to_string())?;
    if body.len() > MAX_FRAME_BYTES {
        return Err(format!("mcp frame exceeds {MAX_FRAME_BYTES} bytes"));
    }
    body.push(b'\n');
    Ok(body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tokio::io::BufReader;

    #[tokio::test]
    async fn round_trips_a_message() {
        let message = json!({"jsonrpc": "2.0", "id": 1, "result": {}});
        let framed = encode(&message).unwrap();
        let mut reader = BufReader::new(framed.as_slice());
        assert_eq!(read(&mut reader).await.unwrap(), message);
    }

    #[tokio::test]
    async fn reads_newline_delimited_and_content_length_frames_from_one_stream() {
        let first = json!({"jsonrpc": "2.0", "id": 1, "result": {"framing": "newline"}});
        let second = json!({"jsonrpc": "2.0", "id": 2, "result": {"framing": "headers"}});
        let third = json!({"jsonrpc": "2.0", "id": 3, "result": {}});
        let mut stream = encode_line(&first).unwrap();
        stream.extend_from_slice(b"\r\n");
        stream.extend(encode(&second).unwrap());
        // Leading JSON whitespace must not be mistaken for header bytes.
        stream.extend_from_slice(b" \t ");
        stream.extend(encode_line(&third).unwrap());
        let mut reader = BufReader::new(stream.as_slice());
        assert_eq!(read(&mut reader).await.unwrap(), first);
        assert_eq!(read(&mut reader).await.unwrap(), second);
        assert_eq!(read(&mut reader).await.unwrap(), third);
        assert_eq!(read(&mut reader).await.unwrap_err(), "eof");

        let padded = format!("{}{{\"id\":1}}\n", " ".repeat(MAX_HEADER_BYTES + 1));
        let mut reader = BufReader::new(padded.as_bytes());
        assert!(read(&mut reader).await.unwrap_err().contains("padding"));
    }

    #[tokio::test]
    async fn newline_frames_stay_bounded_and_single_line() {
        let oversized = format!("{{\"pad\":\"{}\"}}\n", "x".repeat(MAX_FRAME_BYTES));
        let mut reader = BufReader::new(oversized.as_bytes());
        assert!(read(&mut reader).await.unwrap_err().contains("exceeds"));

        let unterminated = br#"{"jsonrpc":"2.0","id":1}"#;
        let mut reader = BufReader::new(&unterminated[..]);
        assert!(
            read(&mut reader)
                .await
                .unwrap_err()
                .contains("unterminated")
        );

        let framed = encode_line(&json!({"text": "line one\nline two"})).unwrap();
        assert_eq!(framed.iter().filter(|byte| **byte == b'\n').count(), 1);
        assert!(!framed.starts_with(b"Content-Length"));
        let mut reader = BufReader::new(framed.as_slice());
        assert_eq!(
            read(&mut reader).await.unwrap()["text"],
            "line one\nline two"
        );
    }

    #[tokio::test]
    async fn rejects_oversized_duplicate_and_invalid_lengths_before_body_read() {
        let oversized = format!("Content-Length: {}\r\n\r\n", MAX_FRAME_BYTES + 1);
        let mut reader = BufReader::new(oversized.as_bytes());
        assert!(read(&mut reader).await.unwrap_err().contains("exceeds"));

        let duplicate = b"Content-Length: 2\r\nContent-Length: 2\r\n\r\n{}";
        let mut reader = BufReader::new(&duplicate[..]);
        assert!(read(&mut reader).await.unwrap_err().contains("duplicate"));

        let invalid = b"Content-Length: nope\r\n\r\n";
        let mut reader = BufReader::new(&invalid[..]);
        assert!(read(&mut reader).await.unwrap_err().contains("invalid"));
    }

    #[tokio::test]
    async fn rejects_oversized_headers_before_body_read() {
        let header = format!("X-Fill: {}\r\n\r\n", "x".repeat(MAX_HEADER_BYTES));
        let mut reader = BufReader::new(header.as_bytes());
        assert!(
            read(&mut reader)
                .await
                .unwrap_err()
                .contains("headers exceed")
        );
    }
}
