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
//! Ports are selected by [Config] settings. Rusui is the plane caller
//! ([ADR 0024](https://github.com/Sannrox/shikigami/blob/main/docs/decisions/0024-local-guest-rusui-plane-caller.md)).
//! Tenkai delivers the binary only — not a runtime port.

pub use shikigami_engine::{
    artifacts, checkpoint, config, content, context, events, evidence_queue, fallback, hooks,
    identity, mcp, metrics, prompts, registry, replay, run, sandbox, security_finder,
    security_triage, state, tools, tracing_export, transcript, workspace,
};

pub mod acp;
pub mod governance;
pub mod harness;
pub mod mcp_server;
pub mod model;
pub mod serve;

#[cfg(test)]
mod content_tests;

pub use config::{
    Config, ConfigSource, EgressMode, HookSettings, McpFraming, McpServerSettings, PermissionMode,
    SandboxBackend, SandboxSettings, ToolSockets, TracingSettings,
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
pub use run::{
    AskDecision, ChildProfile, JOB_CREDENTIAL_ENV_NAMES, JOB_NAMESPACE_ENV, JOB_TENANT_ENV,
    JOB_TOKEN_ENV, JobCredential, JobCredentialHandle, ParkInfo, ParkKind, PlanDecision,
    RunRequest, RunResult, RunTermination, SYSTEM_PROMPT,
};
pub use serve::{
    ControlOptions, QueueJob, QueueLayout, ServeOptions, ServeRuntimeOptions,
    run_serve_with_options,
};
pub use state::{OlderStateRoot, StateError, StateRoot};
pub use tools::{TodoItem, TodoStatus};
pub use transcript::{
    ExportOptions, TRANSCRIPT_SCHEMA_VERSION, TranscriptError, export_run_transcript,
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
