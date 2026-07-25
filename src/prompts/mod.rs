//! Versioned prompt assets for attribution and evals.

use sha2::{Digest, Sha256};

/// A stable, versioned system prompt shipped with the binary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PromptAsset {
    /// Stable name without digest (e.g. `harness-v1`).
    pub id: &'static str,
    /// Prompt body text (LF-normalized for digests).
    pub body: &'static str,
}

/// Default harness system prompt (v1).
pub const HARNESS_V1: PromptAsset = PromptAsset {
    id: "harness-v1",
    body: include_str!("harness-v1.md"),
};

/// Currently selected default for new runs.
pub const DEFAULT_PROMPT: PromptAsset = HARNESS_V1;

/// Normalize newlines then SHA-256 hex digest of the body.
pub fn body_digest(body: &str) -> String {
    let normalized = body.replace("\r\n", "\n");
    let digest = Sha256::digest(normalized.as_bytes());
    format!("{digest:x}")
}

/// Versioned id: `{id}:{sha256hex}` — changes when content changes.
pub fn versioned_id(asset: &PromptAsset) -> String {
    format!("{}:{}", asset.id, body_digest(asset.body))
}

/// Validate that a stored prompt id matches an asset (full versioned form).
pub fn matches_asset(stored_id: &str, asset: &PromptAsset) -> bool {
    stored_id == versioned_id(asset)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn harness_v1_id_scheme() {
        let id = versioned_id(&HARNESS_V1);
        assert!(id.starts_with("harness-v1:"), "{id}");
        let digest = id.strip_prefix("harness-v1:").unwrap();
        assert_eq!(digest.len(), 64);
        assert!(digest.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn digest_stable_and_content_sensitive() {
        let a = body_digest("hello\n");
        let b = body_digest("hello\r\n");
        let c = body_digest("hello!\n");
        assert_eq!(a, b, "CRLF vs LF must normalize");
        assert_ne!(a, c);
    }

    #[test]
    fn changing_content_changes_versioned_id() {
        let base = PromptAsset {
            id: "test",
            body: "alpha",
        };
        let changed = PromptAsset {
            id: "test",
            body: "beta",
        };
        assert_ne!(versioned_id(&base), versioned_id(&changed));
        assert!(matches_asset(&versioned_id(&base), &base));
        assert!(!matches_asset(&versioned_id(&base), &changed));
    }
}
