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
}
