//! Shikigami — open-source headless agent harness.
//!
//! # Embed
//!
//! ```ignore
//! use shikigami::{Config, Harness, RunRequest, StateRoot};
//!
//! # async fn demo() -> Result<(), shikigami::HarnessError> {
//! let state = StateRoot::default_in(".");
//! let mut config = Config::default();
//! config.governance.adapter = "local".into();
//! let harness = Harness::from_config(config, state)?;
//! let mut req = RunRequest::new("write hello");
//! req.keep_workspace = true;
//! let result = harness.run(req).await?;
//! assert!(result.success);
//! # Ok(())
//! # }
//! ```
//!
//! Ports are selected by [Config] settings. Production governance is
//! `sekai-chisei`. Tenkai delivers the binary only — not a runtime port.

pub mod checkpoint;
pub mod config;
pub mod events;
pub mod governance;
pub mod harness;
pub mod identity;
pub mod model;
pub mod run;
pub mod state;
pub mod tools;
pub mod workspace;

pub use config::{Config, ConfigSource};
pub use harness::{DoctorReport, Harness, HarnessError};
pub use identity::{PRODUCT, PRODUCT_DESCRIPTION, VERSION};
pub use run::{ParkInfo, RunRequest, RunResult, RunTermination, SYSTEM_PROMPT};
pub use state::{StateError, StateRoot};

/// Library liveness probe.
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
    fn ping_ok() {
        assert_eq!(ping().service, "shikigami");
    }
}
