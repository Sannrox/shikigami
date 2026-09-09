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

pub mod artifacts;
pub(crate) mod atomic;
pub mod checkpoint;
pub mod config;
pub mod content;
pub mod context;
pub mod eval;
pub mod events;
pub mod evidence_queue;
pub mod fallback;
pub mod governance;
pub mod harness;
pub mod hooks;
pub mod identity;
pub mod mcp;
pub mod mcp_server;
pub mod metrics;
pub mod model;
pub mod plane_host;
pub mod plane_intake;
pub mod prompts;
pub mod registry;
pub mod replay;
pub mod run;
pub mod sandbox;
pub mod serve;
pub mod state;
pub mod tools;
pub mod transcript;
pub mod worker_lifecycle;
pub mod workspace;

pub use config::{
    Config, ConfigSource, EgressMode, HookSettings, McpServerSettings, PermissionMode,
    SandboxBackend, SandboxSettings,
};
pub use content::{
    CLI_FILE_RESOLVER_ID, CONTENT_CONTRACT_VERSION, CONTENT_PROCESS_REQUEST_SCHEMA_VERSION,
    CONTENT_PROCESS_RESULT_SCHEMA_VERSION, CONTENT_SCHEMA_VERSION,
    CONTENT_TRANSCRIPT_SCHEMA_VERSION, ContentCapabilitiesV1, ContentDisclosureState, ContentError,
    ContentMessageV1, ContentModelTurnV1, ContentPartDescriptor, ContentPartKind,
    ContentProcessRequestV1, ContentProcessResultV1, ContentProvenanceV1, ContentResolver,
    ContentRunRequestV1, ContentRunResultV1, ContentToStore, FileContentResolver,
    MAX_CONTENT_AGGREGATE_BYTES, MAX_CONTENT_PART_BYTES, MAX_CONTENT_PARTS,
    MAX_CONTENT_PROCESS_REQUEST_BYTES, ResolvedContent, ResolvedContentPart,
    export_content_transcript,
};
pub use eval::{EVAL_SCHEMA_VERSION, EvalCaseResult, EvalError, EvalSuiteResult, run_fixture};
pub use events::{ChannelSink, EventSink, FanoutSink, HarnessEvent};
pub use governance::AvailableModel;
pub use harness::{
    DoctorReport, Harness, HarnessError, RECOVERY_DIAGNOSIS_SCHEMA_VERSION, RecoveryClass,
    RecoveryDiagnosis, RecoveryNextStep, diagnose_run,
};
pub use identity::{PRODUCT, PRODUCT_DESCRIPTION, VERSION};
pub use mcp_server::McpRunSummary;
pub use metrics::{Metrics, MetricsError, MetricsSnapshot};
pub use model::{CostEstimate, TokenUsage};
pub use plane_host::{PlaneHostError, PlaneHostInfo, PlaneHostOptions};
#[cfg(feature = "governance-sekai-chisei")]
pub use plane_host::{PreparedPlaneHost, prepare_plane_host};
pub use plane_intake::{
    CLAIMED_STATUS, ClaimedPlaneWork, ClaimedWorkMappingError, ClaimedWorkPolicy,
    DEFAULT_MAX_CLAIMED_TASK_BYTES, DEFAULT_MAX_CONTINUATION_BYTES, PlaneAck, PlaneAckOutcome,
    PlaneCheckpoint, PlaneClaim, PlaneClaimEventKind, PlaneClaimLease, PlaneIntakeError,
    PlaneIntakePort, PlaneServeOptions, PlaneWorkContinuation, RUNTIME_DISPATCH_KIND,
    map_claimed_work, run_plane_serve,
};
pub use prompts::{DEFAULT_PROMPT, HARNESS_V1, PromptAsset};
pub use registry::{RunEventRecord, RunRecord, RunRegistry};
pub use replay::{
    MAX_REPLAY_BUNDLE_BYTES, MAX_REPLAY_STEPS, REPLAY_EXPORT_SCHEMA_VERSION,
    REPLAY_REPORT_SCHEMA_VERSION, REPLAY_SCHEMA_VERSION, ReplayBindings, ReplayComparisonStatus,
    ReplayError, ReplayEvidenceBundle, ReplayExportReport, ReplayManifest, ReplayReport,
    ReplayRequest, ReplayResult, ReplayStepComparison, ReplayStepEvidence, ReplayStepKind,
    ReplayTerminalComparison, ReplayTerminalEvidence, digest_bytes, empty_workspace_digest,
    export_replay_inputs, steps_from_messages, text_digest, workspace_digest,
};
pub use run::{ParkInfo, RunRequest, RunResult, RunTermination, SYSTEM_PROMPT};
pub use serve::{
    ControlOptions, QueueJob, QueueLayout, ServeOptions, ServeRuntimeOptions,
    run_serve_with_options,
};
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
