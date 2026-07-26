//! Stable embeddable API for hosts (CLI, onmyoji, CI).

use std::path::Path;
use std::sync::Arc;

use crate::config::{Config, ConfigError, ConfigSource};
use crate::events::{self, EventError, EventSink, FanoutSink};
use crate::governance::{self, GovernanceError, GovernancePort};
use crate::metrics::Metrics;
use crate::model::{self, ModelError, ModelPort};
use crate::run::{Engine, RunError, RunRequest, RunResult, RunTermination};
use crate::state::{StateError, StateRoot};
use crate::workspace::{self, WorkspaceError, WorkspacePort};

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
    Run(#[from] RunError),
    #[error("doctor failed: {0}")]
    Doctor(String),
}

/// Wired harness ready to doctor or run.
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
        Ok(Self {
            config,
            config_source,
            state,
            governance,
            workspace,
            model,
            events,
            metrics: Metrics::new(),
        })
    }

    /// Resolve settings from the usual search path and build a harness.
    pub fn resolve(
        explicit_config: Option<&Path>,
        state: StateRoot,
        cwd: &Path,
    ) -> Result<Self, HarnessError> {
        let (config, source) = state.config_search(explicit_config, cwd)?;
        Self::new(config, source, state)
    }

    pub fn from_config(config: Config, state: StateRoot) -> Result<Self, HarnessError> {
        Self::new(config, ConfigSource::Defaults, state)
    }

    pub fn doctor(&self) -> DoctorReport {
        let mut lines = Vec::new();
        let gov_ok = self.governance.health_ok();
        let gov_detail = self.governance.health_detail();
        let ws_detail = self.workspace.health_detail();
        let ev_detail = self.events.health_detail();

        lines.push(format!("profile:   {}", self.config.profile.name));
        lines.push(format!("config:    {}", self.config_source.description()));
        lines.push(format!("state:     {}", self.state.path().display()));
        lines.push(format!(
            "gov:       {} — {}",
            self.governance.id(),
            gov_detail
        ));
        lines.push(format!(
            "workspace: {} — {}",
            self.workspace.id(),
            ws_detail
        ));
        lines.push(format!("events:    {} — {}", self.events.id(), ev_detail));
        lines.push(format!("model:     {}", self.model.id()));
        lines.push(format!(
            "tools:     mode={:?} [{}]",
            self.config.tools.mode,
            self.config.tools.effective_enabled().join(", ")
        ));
        lines.push(format!(
            "network:   egress={:?} allow_hosts={}",
            self.config.network.egress,
            if self.config.network.allow_hosts.is_empty() {
                "(none)".into()
            } else {
                self.config.network.allow_hosts.join(",")
            }
        ));
        lines.push(format!("max_turns: {}", self.config.run.max_turns));
        if self.config.hooks.is_empty() {
            lines.push("hooks:     (none)".into());
        } else {
            let names: Vec<_> = self
                .config
                .hooks
                .iter()
                .map(|h| format!("{}:{}", h.event, h.command))
                .collect();
            lines.push(format!(
                "hooks:     {} [{}]",
                self.config.hooks.len(),
                names.join(", ")
            ));
        }
        lines.push(format!("credentials: {}", credential_summary(&self.config)));

        let mut ok = true;
        if self.config.requires_governance() && !gov_ok {
            ok = false;
            lines.push("error: governance unhealthy under fail-closed profile".into());
        }
        if self.config.governance.adapter == "sekai-chisei"
            && let Err(e) = self.config.governance_endpoint_required()
        {
            ok = false;
            lines.push(format!("error: {e}"));
        }

        // Never surface secret values; only env var *names* and presence.
        let lines = lines
            .into_iter()
            .map(|l| redact_secrets_in_line(&l, &self.config))
            .collect();

        DoctorReport {
            schema_version: DoctorReport::SCHEMA_VERSION,
            ok,
            profile: self.config.profile.name.clone(),
            governance: self.governance.id().into(),
            governance_detail: redact_secrets_in_line(&gov_detail, &self.config),
            workspace: self.workspace.id().into(),
            workspace_detail: ws_detail,
            events: self.events.id().into(),
            events_detail: ev_detail,
            model: self.model.id().into(),
            lines,
        }
    }

    /// Async doctor that live-probes sekai-chisei when configured.
    pub async fn doctor_async(&self) -> DoctorReport {
        let mut report = self.doctor();
        #[cfg(feature = "governance-sekai-chisei")]
        if self.config.governance.adapter == "sekai-chisei"
            && self.config.governance.endpoint.is_some()
        {
            match crate::governance::sekai_chisei::live_probe(&self.config).await {
                Ok(msg) => report.lines.push(redact_secrets_in_line(
                    &format!("plane:     {msg}"),
                    &self.config,
                )),
                Err(e) => {
                    report.lines.push(redact_secrets_in_line(
                        &format!("plane:     unreachable ({e})"),
                        &self.config,
                    ));
                    if self.config.requires_governance() {
                        report.ok = false;
                    }
                }
            }
        }
        report
    }

    pub async fn run(&self, request: RunRequest) -> Result<RunResult, HarnessError> {
        self.run_with_events(request, None).await
    }

    /// Run with an optional additional event sink (fan-out with the configured sink).
    /// Embedders use this for live in-process progress without scraping logs.
    pub async fn run_with_events(
        &self,
        request: RunRequest,
        extra: Option<Arc<dyn EventSink>>,
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
        let engine = Engine {
            config: self.config.clone(),
            governance: Arc::clone(&self.governance),
            workspace: Arc::clone(&self.workspace),
            model: Arc::clone(&self.model),
            events,
            state_runs: self.state.runs_dir(),
        };
        match engine.run(request).await {
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
