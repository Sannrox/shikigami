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
pub mod context;
pub mod events;
pub mod governance;
pub mod harness;
pub mod hooks;
pub mod identity;
pub mod mcp;
pub mod mcp_server;
pub mod metrics;
pub mod model;
pub mod plane_intake;
pub mod prompts;
pub mod run;
pub mod serve;
pub mod state;
pub mod tools;
pub mod transcript;
pub mod worker_lifecycle;
pub mod workspace;

pub use config::{
    Config, ConfigSource, EgressMode, HookSettings, McpServerSettings, PermissionMode,
};
pub use events::{ChannelSink, EventSink, FanoutSink, HarnessEvent};
pub use harness::{DoctorReport, Harness, HarnessError};
pub use identity::{PRODUCT, PRODUCT_DESCRIPTION, VERSION};
pub use mcp_server::McpRunSummary;
pub use metrics::{Metrics, MetricsSnapshot};
pub use model::{CostEstimate, TokenUsage};
pub use plane_intake::{
    CLAIMED_STATUS, ClaimedPlaneWork, ClaimedWorkMappingError, ClaimedWorkPolicy,
    DEFAULT_MAX_CLAIMED_TASK_BYTES, DEFAULT_MAX_CONTINUATION_BYTES, PlaneAck, PlaneAckOutcome,
    PlaneCheckpoint, PlaneClaim, PlaneClaimEventKind, PlaneClaimLease, PlaneIntakeError,
    PlaneIntakePort, PlaneServeOptions, PlaneWorkContinuation, RUNTIME_DISPATCH_KIND,
    map_claimed_work, run_plane_serve,
};
pub use prompts::{DEFAULT_PROMPT, HARNESS_V1, PromptAsset};
pub use run::{ParkInfo, RunRequest, RunResult, RunTermination, SYSTEM_PROMPT};
pub use serve::{QueueJob, QueueLayout, ServeOptions};
pub use state::{StateError, StateRoot};
pub use tools::{TodoItem, TodoStatus};
pub use transcript::{
    ExportOptions, TRANSCRIPT_SCHEMA_VERSION, TranscriptError, export_run_transcript,
};
pub use worker_lifecycle::{
    TerminalOutcome, WORKER_LIFECYCLE_CONCURRENCY_V1, WORKER_LIFECYCLE_PROTOCOL,
    WORKER_LIFECYCLE_SCHEMA_VERSION, WorkerLifecycle, WorkerLifecycleError,
    WorkerLifecycleIdentity, WorkerLifecycleSnapshot, WorkerLifecycleState, lifecycle_path,
    resolve_state, serve_lifecycle_http,
};

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
