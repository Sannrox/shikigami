//! Stable embeddable API for hosts (CLI, onmyoji, CI).

use std::path::Path;
use std::sync::Arc;

use crate::config::{Config, ConfigError, ConfigResolutionError, ConfigSource};
use crate::content::{ContentError, ContentRunRequestV1, ContentRunResultV1};
use crate::events::{self, EventError, EventSink, FanoutSink};
use crate::governance::{self, AvailableModel, GovernanceError, GovernancePort};
use crate::metrics::{Metrics, MetricsError};
use crate::model::{self, ModelError, ModelPort};
use crate::registry::{RegistryError, RunRegistry};
use crate::replay::{ReplayError, ReplayRequest, ReplayResult};
use crate::run::{Engine, RunError, RunRequest, RunResult, RunTermination};
use crate::state::{StateError, StateRoot};
use crate::workspace::{self, WorkspaceError, WorkspacePort};

mod diagnosis;
mod recovery;

pub use recovery::{
    RECOVERY_DIAGNOSIS_SCHEMA_VERSION, RecoveryClass, RecoveryDiagnosis, RecoveryNextStep,
    diagnose_run,
};

#[derive(Debug, thiserror::Error)]
pub enum HarnessError {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    State(#[from] StateError),
    #[error(transparent)]
    Governance(#[from] GovernanceError),
    #[error(transparent)]
    Workspace(#[from] WorkspaceError),
    #[error(transparent)]
    Model(#[from] ModelError),
    #[error(transparent)]
    Events(#[from] EventError),
    #[error(transparent)]
    Registry(#[from] RegistryError),
    #[error(transparent)]
    Metrics(#[from] MetricsError),
    #[error(transparent)]
    Run(#[from] RunError),
    #[error(transparent)]
    Replay(#[from] ReplayError),
    #[error(transparent)]
    Content(#[from] ContentError),
    #[error("doctor failed: {0}")]
    Doctor(String),
}

/// Wired harness ready to doctor or run.
#[derive(Clone)]
pub struct Harness {
    pub config: Config,
    pub config_source: ConfigSource,
    pub state: StateRoot,
    governance: Arc<dyn GovernancePort>,
    workspace: Arc<dyn WorkspacePort>,
    model: Arc<dyn ModelPort>,
    events: Arc<dyn EventSink>,
    /// Process-local counters (JSON / Prometheus text export).
    pub metrics: Arc<Metrics>,
    /// Durable host-local run records and redacted event journals.
    pub registry: Arc<RunRegistry>,
}

/// Stable doctor JSON contract (`schema_version` = 1).
///
/// Breaking field renames/removals require a schema_version bump and a breaking release note.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct DoctorReport {
    /// Doctor JSON schema version (currently `1`).
    pub schema_version: u32,
    pub ok: bool,
    pub profile: String,
    pub governance: String,
    pub governance_detail: String,
    pub workspace: String,
    pub workspace_detail: String,
    pub events: String,
    pub events_detail: String,
    pub model: String,
    pub lines: Vec<String>,
}

impl DoctorReport {
    pub const SCHEMA_VERSION: u32 = 1;

    pub fn to_json_value(&self) -> serde_json::Value {
        serde_json::to_value(self).expect("DoctorReport is always serializable")
    }
}

impl Harness {
    /// Build from an already-resolved config and state root.
    pub fn new(
        config: Config,
        config_source: ConfigSource,
        state: StateRoot,
    ) -> Result<Self, HarnessError> {
        let governance: Arc<dyn governance::GovernancePort> =
            Arc::from(governance::from_config(&config)?);
        governance.bind_state_root(state.path());
        let workspace = Arc::from(workspace::from_config(&config)?);
        let model = Arc::from(model::from_config(&config)?);
        state.ensure_ready_for_runs()?;
        let events: Arc<dyn EventSink> =
            Arc::from(events::from_config(&config, &state.runs_dir())?);
        let registry = Arc::new(RunRegistry::new(state.path())?);
        let metrics = Metrics::new_at(state.path())?;
        Ok(Self {
            config,
            config_source,
            state,
            governance,
            workspace,
            model,
            events,
            metrics,
            registry,
        })
    }

    /// Resolve settings from the usual search path and build a harness.
    ///
    /// A working-directory `shikigami.toml` is never loaded implicitly; pass
    /// it as `explicit_config` to opt in.
    pub fn resolve(
        explicit_config: Option<&Path>,
        state: StateRoot,
        cwd: &Path,
    ) -> Result<Self, HarnessError> {
        Self::resolve_with_model(explicit_config, state, cwd, None)
    }

    /// Resolve settings and apply an optional final model selection.
    ///
    /// Hosts use this for a CLI/operator override. It must happen before the
    /// governance and model ports are constructed so every adapter observes
    /// the same selected model.
    pub fn resolve_with_model(
        explicit_config: Option<&Path>,
        state: StateRoot,
        cwd: &Path,
        model: Option<&str>,
    ) -> Result<Self, HarnessError> {
        Self::resolve_with_preset(explicit_config, state, cwd, model, None)
    }

    /// Resolve settings, apply an optional model selection, then narrow them
    /// with an optional preset (`--preset`, see ADR 0022). The preset is the
    /// last step, so it narrows what the file, profile, and env resolved.
    pub fn resolve_with_preset(
        explicit_config: Option<&Path>,
        state: StateRoot,
        cwd: &Path,
        model: Option<&str>,
        preset: Option<&str>,
    ) -> Result<Self, HarnessError> {
        let (config, source) =
            Config::resolve_search_with_preset(explicit_config, state.path(), cwd, model, preset)
                .map_err(|error| match error {
                ConfigResolutionError::Search(error) => {
                    HarnessError::State(StateError::Config(error))
                }
                ConfigResolutionError::Override(error) => HarnessError::Config(error),
            })?;
        Self::new(config, source, state)
    }

    pub fn from_config(config: Config, state: StateRoot) -> Result<Self, HarnessError> {
        Self::new(config, ConfigSource::Defaults, state)
    }

    /// Whether the configured governance adapter currently reports healthy.
    pub fn governance_ok(&self) -> bool {
        self.governance.health_ok()
    }

    /// Redacted delayed-evidence observability from the governance adapter.
    pub fn delayed_evidence_snapshot(&self) -> Option<crate::evidence_queue::QueueSnapshot> {
        self.governance.delayed_evidence_snapshot()
    }

    /// Submit queued delayed-evidence envelopes to receipt authority.
    pub async fn reconcile_delayed_evidence(
        &self,
    ) -> Result<Option<crate::evidence_queue::QueueSnapshot>, GovernanceError> {
        self.governance.reconcile_delayed_evidence().await
    }

    /// Return the model name selected by the configured adapter.
    pub fn effective_model_name(&self) -> String {
        model::effective_model_name(&self.config)
    }

    pub(crate) fn model_port(&self) -> &dyn ModelPort {
        &*self.model
    }

    /// The model adapter, for a check that outlives a borrow of the harness
    /// (the TUI's launch reachability check).
    pub fn model_handle(&self) -> Arc<dyn ModelPort> {
        Arc::clone(&self.model)
    }

    /// Return the effective model catalog for this harness.
    ///
    /// Adapters expose their configured model, then `[model].alternates`.
    pub async fn available_models(&self) -> Result<Vec<AvailableModel>, HarnessError> {
        let mut names = vec![self.effective_model_name()];
        for name in &self.config.model.alternates {
            if !names.contains(name) {
                names.push(name.clone());
            }
        }
        Ok(names
            .into_iter()
            .map(|model| AvailableModel {
                provider: self.config.model.adapter.clone(),
                upstream_model: model.clone(),
                canonical_model: model,
                lifecycle: "configured".into(),
            })
            .collect())
    }

    pub fn doctor(&self) -> DoctorReport {
        diagnosis::doctor(self)
    }

    /// Reconstruct a replay package from retained artifacts without executing.
    pub fn export_replay_inputs(
        &self,
        run_id: &str,
    ) -> Result<crate::replay::ReplayExportReport, HarnessError> {
        Ok(crate::replay::export_replay_inputs(
            &self.state,
            run_id,
            &self.config,
        )?)
    }

    /// Inspect one retained run without executing.
    pub fn diagnose_run(&self, run_id: &str) -> Result<RecoveryDiagnosis, HarnessError> {
        recovery::diagnose_with_registry(
            &self.registry,
            &self.state.runs_dir(),
            run_id,
            &self.config,
        )
    }

    /// Async doctor that live-probes `http-callback` when configured.
    pub async fn doctor_async(&self) -> DoctorReport {
        diagnosis::doctor_async(self).await
    }

    pub async fn run(&self, request: RunRequest) -> Result<RunResult, HarnessError> {
        self.run_with_events_and_checkpoint_digest(request, None, None)
            .await
    }

    /// Run with an optional additional event sink (fan-out with the configured sink).
    /// Embedders use this for live in-process progress without scraping logs.
    pub async fn run_with_events(
        &self,
        request: RunRequest,
        extra: Option<Arc<dyn EventSink>>,
    ) -> Result<RunResult, HarnessError> {
        self.run_with_events_and_checkpoint_digest(request, extra, None)
            .await
    }

    /// Execute one additive bounded content run.
    pub async fn run_content(
        &self,
        request: ContentRunRequestV1,
    ) -> Result<ContentRunResultV1, HarnessError> {
        self.run_content_with_events(request, None).await
    }

    /// Execute a bounded content run with an optional metadata-only event sink.
    pub async fn run_content_with_events(
        &self,
        request: ContentRunRequestV1,
        extra: Option<Arc<dyn EventSink>>,
    ) -> Result<ContentRunResultV1, HarnessError> {
        let report = self.doctor_async().await;
        if !report.ok {
            return Err(HarnessError::Doctor(report.lines.join("; ")));
        }
        let events = match extra {
            Some(extra) => Arc::new(FanoutSink::new(vec![Arc::clone(&self.events), extra]))
                as Arc<dyn EventSink>,
            None => Arc::clone(&self.events),
        };
        let engine = Engine::new(
            self.config.clone(),
            Arc::clone(&self.governance),
            Arc::clone(&self.workspace),
            Arc::clone(&self.model),
            events,
            self.state.runs_dir(),
            Arc::clone(&self.registry),
        );
        match engine.run_content(request).await {
            Ok(run) => {
                self.metrics.record_run(
                    run.success,
                    run.termination == RunTermination::Parked,
                    run.turns,
                    run.usage.input_tokens,
                    run.usage.output_tokens,
                );
                let checkpoint =
                    crate::checkpoint::Checkpoint::load(&self.state.runs_dir(), &run.run_id)
                        .map_err(ContentError::from)?;
                let binding = checkpoint.content.as_ref().ok_or_else(|| {
                    HarnessError::Content(ContentError::Invalid(
                        "completed content run has no sidecar binding".into(),
                    ))
                })?;
                let sidecar =
                    crate::content::load_sidecar(&self.state.runs_dir(), &run.run_id, binding)?;
                Ok(ContentRunResultV1 {
                    run,
                    messages: sidecar.messages,
                })
            }
            Err(error) => {
                self.metrics.record_run(false, false, 0, 0, 0);
                Err(error.into())
            }
        }
    }

    /// Execute a content-bound replay as a new isolated attempt.
    pub async fn replay(&self, request: ReplayRequest) -> Result<ReplayResult, HarnessError> {
        self.replay_with_events(request, None).await
    }

    /// Replay with an optional additional event sink.
    pub async fn replay_with_events(
        &self,
        request: ReplayRequest,
        extra: Option<Arc<dyn EventSink>>,
    ) -> Result<ReplayResult, HarnessError> {
        // Reject malformed or mismatched host evidence before a live doctor
        // probe can contact a configured governance adapter.
        let execution = request.admit()?;
        if self.config.requires_governance() {
            return Err(HarnessError::Replay(
                ReplayError::GovernanceEvidenceUnsupported(self.config.governance.adapter.clone()),
            ));
        }
        crate::replay::validate_host_policy(&self.config, &execution.bindings)?;
        let report = self.doctor_async().await;
        if !report.ok {
            return Err(HarnessError::Doctor(report.lines.join("; ")));
        }
        if matches!(
            self.config.workspace.adapter.as_str(),
            "inplace" | "directory-inplace"
        ) {
            return Err(HarnessError::Replay(ReplayError::Invalid(
                "replay requires an isolated `directory` or `git-worktree` workspace".into(),
            )));
        }
        let events = match extra {
            Some(extra) => Arc::new(FanoutSink::new(vec![Arc::clone(&self.events), extra]))
                as Arc<dyn EventSink>,
            None => Arc::clone(&self.events),
        };
        let engine = Engine::new(
            crate::replay::replay_config(&self.config),
            Arc::clone(&self.governance),
            Arc::clone(&self.workspace),
            Arc::clone(&self.model),
            events,
            self.state.runs_dir(),
            Arc::clone(&self.registry),
        );
        match engine.replay(request).await {
            Ok(result) => {
                self.metrics.record_run(
                    result.run.success,
                    result.run.termination == RunTermination::Parked,
                    result.run.turns,
                    result.run.usage.input_tokens,
                    result.run.usage.output_tokens,
                );
                Ok(result)
            }
            Err(error) => {
                self.metrics.record_run(false, false, 0, 0, 0);
                Err(error.into())
            }
        }
    }

    async fn run_with_events_and_checkpoint_digest(
        &self,
        request: RunRequest,
        extra: Option<Arc<dyn EventSink>>,
        expected_checkpoint_digest: Option<&str>,
    ) -> Result<RunResult, HarnessError> {
        let report = self.doctor_async().await;
        if !report.ok {
            return Err(HarnessError::Doctor(report.lines.join("; ")));
        }
        let events = match extra {
            Some(extra) => Arc::new(FanoutSink::new(vec![Arc::clone(&self.events), extra]))
                as Arc<dyn EventSink>,
            None => Arc::clone(&self.events),
        };
        let engine = Engine::new(
            self.config.clone(),
            Arc::clone(&self.governance),
            Arc::clone(&self.workspace),
            Arc::clone(&self.model),
            events,
            self.state.runs_dir(),
            Arc::clone(&self.registry),
        );
        match engine
            .run_with_checkpoint_digest(request, expected_checkpoint_digest)
            .await
        {
            Ok(result) => {
                self.metrics.record_run(
                    result.success,
                    result.termination == RunTermination::Parked,
                    result.turns,
                    result.usage.input_tokens,
                    result.usage.output_tokens,
                );
                Ok(result)
            }
            Err(e) => {
                if matches!(
                    e,
                    RunError::Governance(crate::governance::GovernanceError::Unavailable(_))
                        | RunError::Governance(crate::governance::GovernanceError::Message(_))
                ) {
                    self.metrics.record_plane_error();
                }
                self.metrics.record_run(false, false, 0, 0, 0);
                Err(e.into())
            }
        }
    }
}

fn display_tools(tools: &[String]) -> String {
    if tools.is_empty() {
        "(none)".into()
    } else {
        format!("[{}]", tools.join(", "))
    }
}

/// Summarize credential *sources* without values (for doctor).
pub fn credential_summary(config: &Config) -> String {
    let host_authz = match &config.governance.token_env {
        Some(name) if !name.is_empty() => {
            let state = if env_nonempty(name) { "set" } else { "unset" };
            format!("host_token_env={name} ({state})")
        }
        _ => "host_token_env=(none)".into(),
    };
    let model_env = &config.model.api_key_env;
    let model_state = if env_nonempty(model_env) {
        "set"
    } else {
        "unset"
    };
    format!("{host_authz}, model_api_key_env={model_env} ({model_state})")
}

fn env_nonempty(name: &str) -> bool {
    std::env::var(name).map(|v| !v.is_empty()).unwrap_or(false)
}

pub use crate::config::redact_secrets_in_line;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use tempfile::tempdir;

    #[test]
    fn doctor_json_schema_keys_stable() {
        let dir = tempdir().unwrap();
        let state = StateRoot::new(dir.path().join("state"));
        let config = Config::default();
        let harness = Harness::from_config(config, state).unwrap();
        let report = harness.doctor();
        assert_eq!(report.schema_version, 1);
        assert!(report.ok);
        let v = report.to_json_value();
        for key in [
            "schema_version",
            "ok",
            "profile",
            "governance",
            "governance_detail",
            "workspace",
            "workspace_detail",
            "events",
            "events_detail",
            "model",
            "lines",
        ] {
            assert!(v.get(key).is_some(), "missing key {key}");
        }
    }

    #[test]
    fn invalid_model_override_remains_a_config_error() {
        let dir = tempdir().unwrap();
        let state = StateRoot::new(dir.path().join("state"));

        let error = match Harness::resolve_with_model(None, state, dir.path(), Some("  ")) {
            Ok(_) => panic!("empty model override must fail"),
            Err(error) => error,
        };

        assert!(matches!(
            error,
            HarnessError::Config(ConfigError::Invalid(_))
        ));
    }

    #[test]
    fn config_search_failure_remains_a_state_error() {
        let dir = tempdir().unwrap();
        let path = dir.path().join(Config::FILENAME);
        std::fs::write(&path, "not valid toml = [").unwrap();
        let state = StateRoot::new(dir.path().join("state"));

        let error = match Harness::resolve_with_model(Some(&path), state, dir.path(), None) {
            Ok(_) => panic!("invalid config must fail"),
            Err(error) => error,
        };

        assert!(matches!(
            error,
            HarnessError::State(StateError::Config(ConfigError::Parse { .. }))
        ));
    }

    #[tokio::test]
    async fn local_available_models_report_configured_model() {
        let dir = tempdir().unwrap();
        let state = StateRoot::new(dir.path().join("state"));
        let mut config = Config::default();
        config.model.model = "local-model".into();
        let harness = Harness::from_config(config, state).unwrap();

        assert_eq!(
            harness.available_models().await.unwrap(),
            vec![AvailableModel {
                provider: "scripted".into(),
                upstream_model: "local-model".into(),
                canonical_model: "local-model".into(),
                lifecycle: "configured".into(),
            }]
        );
    }

    #[test]
    fn doctor_warns_on_sandbox_none_and_refuses_unavailable_linux_native() {
        let dir = tempdir().unwrap();
        let state = StateRoot::new(dir.path().join("state"));
        let harness = Harness::from_config(Config::default(), state).unwrap();
        let report = harness.doctor();
        assert!(report.ok);
        assert!(
            report
                .lines
                .iter()
                .any(|line| line.contains("backend=none") && line.contains("no OS isolation")),
            "{:?}",
            report.lines
        );

        let state = StateRoot::new(dir.path().join("state-native"));
        let mut config = Config::default();
        config.governance.fail_closed = true;
        config.sandbox.backend = crate::config::SandboxBackend::LinuxNative;
        config.tools.enabled = vec!["bash".into(), "report".into()];
        if cfg!(target_os = "linux") {
            config.governance.fail_closed = false;
            config.network.egress = crate::config::EgressMode::Deny;
            let harness = Harness::from_config(config, state).unwrap();
            let report = harness.doctor();
            if report.ok {
                assert!(
                    report.lines.iter().any(|line| {
                        line.contains("backend=linux_native") && line.contains("abi=")
                    }),
                    "{:?}",
                    report.lines
                );
            } else {
                assert!(
                    report.lines.iter().any(|line| line.contains("Landlock")
                        || line.contains("unavailable")
                        || line.contains("linux_native")),
                    "{:?}",
                    report.lines
                );
            }
        } else {
            let harness = Harness::from_config(config, state).unwrap();
            let report = harness.doctor();
            assert!(!report.ok, "{:?}", report.lines);
            assert!(
                report
                    .lines
                    .iter()
                    .any(|line| line.contains("linux_native") && line.contains("Linux")),
                "{:?}",
                report.lines
            );
        }
    }

    #[test]
    fn doctor_reports_retired_plane_adapter_as_config_error() {
        let mut config = Config::default();
        config.governance.adapter = "sekai-chisei".into();
        let error = config.validate().unwrap_err().to_string();
        assert!(error.contains("ADR 0024"), "{error}");
    }

    #[tokio::test]
    async fn doctor_refuses_unreachable_http_callback_by_default() {
        let dir = tempdir().unwrap();
        let mut config = Config::default();
        config.governance.adapter = "http-callback".into();
        config.governance.endpoint = Some("http://127.0.0.1:1/authz".into());
        config.governance.namespace = "team-a".into();
        assert!(!config.governance.fail_closed);
        let harness = Harness::from_config(config, StateRoot::new(dir.path().join("s"))).unwrap();
        let report = harness.doctor_async().await;
        assert!(!report.ok, "{:?}", report.lines);
    }

    #[test]
    fn doctor_explains_intersection_and_implicit_tool_authority() {
        let dir = tempdir().unwrap();
        let state = StateRoot::new(dir.path().join("state"));
        let mut config = Config::default();
        config.tools.mode = crate::config::PermissionMode::WorkspaceExec;
        config.tools.enabled = vec!["read_file".into(), "bash".into()];
        config.governance.token_env = Some("PLANE_TOKEN".into());
        let harness = Harness::from_config(config, state).unwrap();

        let report = harness.doctor();
        assert!(
            report
                .lines
                .contains(&"tools.mode:       workspace_exec".into())
        );
        assert!(
            report
                .lines
                .contains(&"tools.configured: [read_file, bash]".into())
        );
        assert!(report.lines.iter().any(|line| {
            line.starts_with("tools.excluded:")
                && line.contains("write_file")
                && !line.contains("bash]")
        }));
        assert!(report.lines.contains(
            &"tools.implicit:   [bash_background, bash_job_status, bash_job_logs]".into()
        ));
        assert!(report.lines.iter().any(|line| {
            line == "tools.visible:    [read_file, bash, bash_background, bash_job_status, bash_job_logs]"
        }));
        assert!(report.lines.contains(
            &"tools.environment: parent minus protected/startup controls; protected=[PLANE_TOKEN, SHIKIGAMI_JOB_NAMESPACE, SHIKIGAMI_JOB_TENANT, SHIKIGAMI_JOB_TOKEN]"
                .into()
        ));
    }

    #[test]
    fn doctor_does_not_report_unknown_custom_names_as_visible() {
        let dir = tempdir().unwrap();
        let state = StateRoot::new(dir.path().join("state"));
        let mut config = Config::default();
        config.tools.enabled = vec!["obsolete_tool".into(), "report".into()];
        let harness = Harness::from_config(config, state).unwrap();

        let report = harness.doctor();
        assert!(
            report
                .lines
                .contains(&"tools.effective:  [obsolete_tool, report]".into())
        );
        assert!(report.lines.contains(&"tools.visible:    [report]".into()));
    }

    #[test]
    fn doctor_redacts_secret_values_from_lines() {
        let secret = "super-secret-plane-token-xyz";
        // SAFETY: test-only env mutation; sequential in this process.
        unsafe {
            std::env::set_var("SHIKIGAMI_TEST_TOKEN", secret);
        }
        let mut config = Config::default();
        config.governance.token_env = Some("SHIKIGAMI_TEST_TOKEN".into());
        let line = format!("oops leaked Bearer {secret} in error");
        let redacted = redact_secrets_in_line(&line, &config);
        assert!(!redacted.contains(secret), "{redacted}");
        assert!(redacted.contains("[REDACTED]"), "{redacted}");

        let dir = tempdir().unwrap();
        let state = StateRoot::new(dir.path().join("state"));
        let harness = Harness::from_config(config, state).unwrap();
        let report = harness.doctor();
        let json = serde_json::to_string(&report).unwrap();
        assert!(
            !json.contains(secret),
            "doctor JSON must not contain secret"
        );
        assert!(
            report
                .lines
                .iter()
                .any(|l| l.contains("host_token_env=SHIKIGAMI_TEST_TOKEN")),
            "expected credential summary: {:?}",
            report.lines
        );
        unsafe {
            std::env::remove_var("SHIKIGAMI_TEST_TOKEN");
        }
    }

    #[test]
    fn doctor_names_context_window_when_set() {
        let dir = tempdir().unwrap();
        let state = StateRoot::new(dir.path().join("state"));
        let mut config = Config::default();
        config.run.context_window_tokens = Some(1000);
        config.run.compact_at_usage_percent = 80;
        let harness = Harness::from_config(config, state).unwrap();
        let report = harness.doctor();
        assert!(
            report.lines.iter().any(|line| {
                line.contains("context_window") && line.contains("1000") && line.contains("80%")
            }),
            "expected context_window doctor line: {:?}",
            report.lines
        );
        assert!(
            report.to_json_value().get("context_window").is_none(),
            "context_window must stay in lines, not the JSON schema"
        );
    }

    #[test]
    fn doctor_names_plan_jail_when_selected() {
        let dir = tempdir().unwrap();
        let state = StateRoot::new(dir.path().join("state"));
        let mut config = Config::default();
        config.run.plan_jail = true;
        let harness = Harness::from_config(config, state).unwrap();
        let report = harness.doctor();
        assert!(
            report
                .lines
                .iter()
                .any(|line| line.contains("plan_jail") && line.contains(".shikigami/plan.md")),
            "expected plan_jail doctor line: {:?}",
            report.lines
        );
        assert!(
            report.to_json_value().get("plan_jail").is_none(),
            "plan_jail must stay in lines, not the JSON schema"
        );
    }

    #[test]
    fn doctor_names_nested_when_selected() {
        let dir = tempdir().unwrap();
        let state = StateRoot::new(dir.path().join("state"));
        let mut config = Config::default();
        config.run.nested = true;
        config.run.nested_max_depth = 2;
        config.run.nested_max_children = 3;
        let harness = Harness::from_config(config, state).unwrap();
        let report = harness.doctor();
        assert!(
            report.lines.iter().any(|line| {
                line.contains("nested:")
                    && line.contains("depth<=2")
                    && line.contains("children<=3")
            }),
            "expected nested doctor line: {:?}",
            report.lines
        );
        assert!(
            report.to_json_value().get("nested").is_none(),
            "nested must stay in lines, not the JSON schema"
        );
    }

    #[test]
    fn example_tomls_have_no_inline_secrets() {
        let examples = [
            include_str!("../examples/local-run.toml"),
            include_str!("../examples/cliproxy-http.toml"),
        ];
        for body in examples {
            for line in body.lines() {
                let t = line.trim();
                if t.starts_with('#') || t.is_empty() {
                    continue;
                }
                assert!(
                    !t.to_ascii_lowercase().contains("sk-"),
                    "possible key material: {t}"
                );
                assert!(!t.contains("Bearer "), "token must not be inline: {t}");
            }
        }
    }
}
