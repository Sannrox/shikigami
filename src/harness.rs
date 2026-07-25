//! Stable embeddable API for hosts (CLI, onmyoji, CI).

use std::path::Path;
use std::sync::Arc;

use crate::config::{Config, ConfigError, ConfigSource};
use crate::events::{self, EventError, EventSink};
use crate::governance::{self, GovernanceError, GovernancePort};
use crate::model::{self, ModelError, ModelPort};
use crate::run::{Engine, RunError, RunRequest, RunResult};
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
        let events = Arc::from(events::from_config(&config, &state.runs_dir())?);
        Ok(Self {
            config,
            config_source,
            state,
            governance,
            workspace,
            model,
            events,
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
            "tools:     {}",
            self.config.tools.effective_enabled().join(", ")
        ));
        lines.push(format!("max_turns: {}", self.config.run.max_turns));

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

        DoctorReport {
            schema_version: DoctorReport::SCHEMA_VERSION,
            ok,
            profile: self.config.profile.name.clone(),
            governance: self.governance.id().into(),
            governance_detail: gov_detail,
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
                Ok(msg) => report.lines.push(format!("plane:     {msg}")),
                Err(e) => {
                    report.lines.push(format!("plane:     unreachable ({e})"));
                    if self.config.requires_governance() {
                        report.ok = false;
                    }
                }
            }
        }
        report
    }

    pub async fn run(&self, request: RunRequest) -> Result<RunResult, HarnessError> {
        let report = self.doctor_async().await;
        if !report.ok {
            return Err(HarnessError::Doctor(report.lines.join("; ")));
        }
        let engine = Engine {
            config: self.config.clone(),
            governance: Arc::clone(&self.governance),
            workspace: Arc::clone(&self.workspace),
            model: Arc::clone(&self.model),
            events: Arc::clone(&self.events),
            state_runs: self.state.runs_dir(),
        };
        Ok(engine.run(request).await?)
    }
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
}
