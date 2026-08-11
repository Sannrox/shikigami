//! Shared bounded `Content-Length` framing for MCP stdio adapters.

use serde_json::Value;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub(crate) const MAX_FRAME_BYTES: usize = 1024 * 1024;
pub(crate) const MAX_HEADER_BYTES: usize = 8 * 1024;

pub(crate) async fn write<W: AsyncWrite + Unpin>(
    writer: &mut W,
    message: &Value,
) -> Result<(), String> {
    let framed = encode(message)?;
    writer.write_all(&framed).await.map_err(|e| e.to_string())?;
    writer.flush().await.map_err(|e| e.to_string())
}

pub(crate) async fn read<R: AsyncBufRead + Unpin>(reader: &mut R) -> Result<Value, String> {
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
