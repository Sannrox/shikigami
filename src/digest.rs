//! Shared SHA-256 and wall-clock helpers.
//!
//! Digest strings are either a bare 64-char lowercase hex or the `sha256:`
//! prefixed form used by content, replay, fallback, and evidence bindings.

use std::time::{SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};

/// Lowercase hex encoding of raw bytes.
pub(crate) fn hex_lower(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Bare SHA-256 hex (64 lowercase chars, no prefix).
pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    hex_lower(Sha256::digest(bytes).as_slice())
}

/// SHA-256 digest with the `sha256:` prefix used by content and evidence.
pub(crate) fn sha256_prefixed(bytes: &[u8]) -> String {
    format!("sha256:{}", sha256_hex(bytes))
}

/// True when `value` is `sha256:` plus 64 hexadecimal characters.
///
/// `lowercase_hex` requires `a-f` (content and generated artifacts). Replay
/// admits `A-F` so historically mixed-case bundles still validate.
pub(crate) fn is_sha256_prefixed(value: &str, lowercase_hex: bool) -> bool {
    value.strip_prefix("sha256:").is_some_and(|hex| {
        hex.len() == 64
            && hex.bytes().all(|byte| {
                byte.is_ascii_hexdigit() && (!lowercase_hex || !byte.is_ascii_uppercase())
            })
    })
}

/// Current Unix time in milliseconds as `u64`. Before-epoch clocks become `0`.
pub(crate) fn unix_now_ms_u64() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Current Unix time in milliseconds as `i64`. Before-epoch or overflow becomes `0`.
pub(crate) fn unix_now_ms_i64() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_millis()).ok())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefixed_digest_matches_known_empty_input() {
        assert_eq!(
            sha256_prefixed(b""),
            "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn clocks_are_non_negative() {
        assert!(unix_now_ms_u64() > 0);
        assert!(unix_now_ms_i64() > 0);
    }

    #[test]
    fn prefixed_shape_accepts_known_digest_and_rejects_garbage() {
        let empty = sha256_prefixed(b"");
        let upper_hex = format!("sha256:{}", empty[7..].to_ascii_uppercase());
        assert!(is_sha256_prefixed(&empty, true));
        assert!(is_sha256_prefixed(&upper_hex, false));
        assert!(!is_sha256_prefixed(&upper_hex, true));
        assert!(!is_sha256_prefixed("sha256:abcd", true));
        assert!(!is_sha256_prefixed("not-a-digest", false));
    }
}
