//! Stable product identity shared by the library and CLI.

/// Product name (repo, binary family, and stack peer identity).
pub const PRODUCT: &str = "shikigami";

/// Crate / release version.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// One-line product description for CLI and docs.
pub const PRODUCT_DESCRIPTION: &str =
    "headless agent harness governed by sekai-chisei, deliverable through tenkai";
