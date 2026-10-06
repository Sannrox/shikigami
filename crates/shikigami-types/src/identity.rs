//! Stable product identity shared by the library and CLI.

/// Product name (repo, binary family, and stack peer identity).
pub const PRODUCT: &str = "shikigami";

/// Crate / release version.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// One-line product description for CLI and docs.
pub const PRODUCT_DESCRIPTION: &str =
    "open-source headless agent harness (local or governed via adapters)";

#[cfg(test)]
mod tests {
    use super::VERSION;

    #[test]
    fn changelog_dated_heading_matches_crate_version() {
        let changelog = include_str!("../../../CHANGELOG.md");
        let heading = format!("## [{VERSION}]");
        assert!(
            changelog.lines().any(|line| line.starts_with(&heading)),
            "CHANGELOG.md must ship a dated heading for crate version {VERSION}"
        );
    }

    #[test]
    fn readme_status_matches_crate_version() {
        let readme = include_str!("../../../README.md");
        let stamp = format!("`v{VERSION}`");
        assert!(
            readme.contains(&stamp),
            "README.md Status stamp must match crate version {stamp}"
        );
    }
}
