//! Shikigami application core.
//!
//! The headless harness owns run lifecycle, local workspaces, tool execution,
//! and evidence harvest. Governance stays in sekai-chisei; delivery stays in
//! tenkai. CLI and future daemon hosts are adapters around this library.

pub mod config;
pub mod identity;
pub mod state;

pub use config::Config;
pub use identity::{PRODUCT, PRODUCT_DESCRIPTION, VERSION};
pub use state::{StateError, StateRoot};

/// Library liveness probe for hosts and smoke tests.
pub fn ping() -> PingResponse {
    PingResponse {
        service: PRODUCT.to_string(),
        version: VERSION.to_string(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PingResponse {
    pub service: String,
    pub version: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ping_reports_product_identity() {
        let res = ping();
        assert_eq!(res.service, "shikigami");
        assert_eq!(res.version, env!("CARGO_PKG_VERSION"));
    }
}
