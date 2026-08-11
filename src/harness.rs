//! Stable embeddable API for hosts (CLI, onmyoji, CI).

use std::path::Path;
use std::sync::Arc;

use crate::config::{Config, ConfigError, ConfigResolutionError, ConfigSource};
use crate::events::{self, EventError, EventSink, FanoutSink};
use crate::governance::{self, AvailableModel, GovernanceError, GovernancePort};
use crate::metrics::{Metrics, MetricsError};
use crate::model::{self, ModelError, ModelPort};
use crate::registry::{RegistryError, RunRegistry};
use crate::run::{Engine, RunError, RunRequest, RunResult, RunTermination};
use crate::state::{StateError, StateRoot};
use crate::workspace::{self, WorkspaceError, WorkspacePort};

mod diagnosis;

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
/// Breaking field renames/removals require a schema_version bump and CHANGELOG.
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
        let governance = Arc::from(governance::from_config(&config)?);
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
        let (config, source) =
            Config::resolve_search_with_model(explicit_config, state.path(), cwd, model).map_err(
                |error| match error {
                    ConfigResolutionError::Search(error) => {
                        HarnessError::State(StateError::Config(error))
                    }
                    ConfigResolutionError::Override(error) => HarnessError::Config(error),
                },
            )?;
        Self::new(config, source, state)
    }

    pub fn from_config(config: Config, state: StateRoot) -> Result<Self, HarnessError> {
        Self::new(config, ConfigSource::Defaults, state)
    }

    /// Whether the configured governance adapter currently reports healthy.
    pub fn governance_ok(&self) -> bool {
        self.governance.health_ok()
    }

    /// Return the model name selected by the configured adapter.
    pub fn effective_model_name(&self) -> String {
        model::effective_model_name(&self.config)
    }

    /// Return the effective model catalog for this harness.
    ///
    /// Sekai-Chisei is authoritative for governed availability. Ungoverned
    /// adapters expose their configured model as a compact local catalog.
    pub async fn available_models(&self) -> Result<Vec<AvailableModel>, HarnessError> {
        if self.config.governance.adapter == "sekai-chisei" {
            return Ok(with_auto_route(self.governance.available_models().await?));
        }
        let model = self.effective_model_name();
        Ok(vec![AvailableModel {
            provider: self.config.model.adapter.clone(),
            upstream_model: model.clone(),
            canonical_model: model,
            lifecycle: "configured".into(),
        }])
    }

    pub fn doctor(&self) -> DoctorReport {
        diagnosis::doctor(self)
    }

    /// Async doctor that live-probes sekai-chisei when configured.
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

    pub(crate) async fn run_with_checkpoint_digest(
        &self,
        request: RunRequest,
        expected_checkpoint_digest: &str,
    ) -> Result<RunResult, HarnessError> {
        self.run_with_events_and_checkpoint_digest(request, None, Some(expected_checkpoint_digest))
            .await
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

fn with_auto_route(mut models: Vec<AvailableModel>) -> Vec<AvailableModel> {
    if !models.iter().any(|model| model.canonical_model == "auto") {
        models.insert(
            0,
            AvailableModel {
                provider: "sekai-chisei".into(),
                upstream_model: "auto".into(),
                canonical_model: "auto".into(),
                lifecycle: "routing".into(),
            },
        );
    }
    models
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
    let plane = match &config.governance.token_env {
        Some(name) if !name.is_empty() => {
            let state = if env_nonempty(name) { "set" } else { "unset" };
            format!("plane_token_env={name} ({state})")
        }
        _ => "plane_token_env=(none)".into(),
    };
    let model_env = &config.model.api_key_env;
    let model_state = if env_nonempty(model_env) {
        "set"
    } else {
        "unset"
    };
    format!("{plane}, model_api_key_env={model_env} ({model_state})")
}

fn env_nonempty(name: &str) -> bool {
    std::env::var(name).map(|v| !v.is_empty()).unwrap_or(false)
}

/// Replace known secret values (from env) with `[REDACTED]` in a text line.
pub fn redact_secrets_in_line(line: &str, config: &Config) -> String {
    let mut out = line.to_string();
    let mut secrets = Vec::new();
    if let Some(name) = &config.governance.token_env
        && let Ok(v) = std::env::var(name)
        && !v.is_empty()
    {
        if let Some(stripped) = v.strip_prefix("Bearer ") {
            secrets.push(stripped.to_string());
        }
        secrets.push(v);
    }
    if let Ok(v) = std::env::var(&config.model.api_key_env)
        && !v.is_empty()
    {
        secrets.push(v);
    }
    // Longest first so partial overlaps redact fully.
    secrets.sort_by_key(|s| std::cmp::Reverse(s.len()));
    for s in secrets {
        if s.len() >= 8 {
            out = out.replace(&s, "[REDACTED]");
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::governance::{RunHandle, RunOutcome};
    use crate::model::{ChatMessage, ModelPort, ModelTurn};
    use crate::tools::ToolDef;
    use tempfile::tempdir;

    struct CatalogGovernance;

    #[async_trait::async_trait]
    impl GovernancePort for CatalogGovernance {
        fn id(&self) -> &'static str {
            "catalog-test"
        }

        fn health_detail(&self) -> String {
            "ok".into()
        }

        fn health_ok(&self) -> bool {
            true
        }

        async fn available_models(&self) -> Result<Vec<AvailableModel>, GovernanceError> {
            Ok(vec![AvailableModel {
                provider: "openai".into(),
                upstream_model: "gpt-5.5".into(),
                canonical_model: "openai/gpt-5.5".into(),
                lifecycle: "active".into(),
            }])
        }

        async fn begin_run(
            &self,
            _run_id: &str,
            _task: &str,
            _logical_operation_id: Option<&str>,
        ) -> Result<RunHandle, GovernanceError> {
            unreachable!()
        }

        async fn plan_turn(
            &self,
            _handle: &RunHandle,
            _system: &str,
            _messages: &[ChatMessage],
            _tools: &[ToolDef],
            _local_model: &dyn ModelPort,
        ) -> Result<ModelTurn, GovernanceError> {
            unreachable!()
        }

        async fn authorize_tool(
            &self,
            _handle: &RunHandle,
            _name: &str,
            _args_json: &str,
        ) -> Result<(), GovernanceError> {
            unreachable!()
        }

        async fn report_tool(
            &self,
            _handle: &RunHandle,
            _name: &str,
            _ok: bool,
            _detail: &str,
        ) -> Result<(), GovernanceError> {
            unreachable!()
        }

        async fn complete_run(
            &self,
            _handle: &RunHandle,
            _outcome: RunOutcome,
        ) -> Result<(), GovernanceError> {
            unreachable!()
        }
    }

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

    #[tokio::test]
    async fn governed_available_models_include_auto_route() {
        let dir = tempdir().unwrap();
        let state = StateRoot::new(dir.path().join("state"));
        let mut harness = Harness::from_config(Config::default(), state).unwrap();
        harness.config.governance.adapter = "sekai-chisei".into();
        harness.governance = Arc::new(CatalogGovernance);

        let catalog = harness.available_models().await.unwrap();
        assert_eq!(catalog[0].canonical_model, "auto");
        assert_eq!(catalog[0].lifecycle, "routing");
        assert_eq!(
            catalog
                .iter()
                .filter(|model| model.canonical_model == "auto")
                .count(),
            1
        );
        assert_eq!(catalog[1].canonical_model, "openai/gpt-5.5");
    }

    #[test]
    fn doctor_fail_closed_governed_without_endpoint() {
        let dir = tempdir().unwrap();
        let state = StateRoot::new(dir.path().join("state"));
        let mut config = Config::default();
        config.profile.name = "governed".into();
        config.governance.adapter = "sekai-chisei".into();
        config.governance.fail_closed = true;
        config.governance.endpoint = None;
        let harness = Harness::from_config(config, state).unwrap();
        let report = harness.doctor();
        assert!(!report.ok);
        assert_eq!(report.schema_version, 1);
        assert_eq!(report.governance, "sekai-chisei");
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
            &"tools.environment: parent minus protected/startup controls; protected=[PLANE_TOKEN]"
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
                .any(|l| l.contains("plane_token_env=SHIKIGAMI_TEST_TOKEN")),
            "expected credential summary: {:?}",
            report.lines
        );
        unsafe {
            std::env::remove_var("SHIKIGAMI_TEST_TOKEN");
        }
    }

    #[test]
    fn example_tomls_have_no_inline_secrets() {
        let examples = [
            include_str!("../examples/local-run.toml"),
            include_str!("../examples/governed-sekai-chisei.toml"),
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
