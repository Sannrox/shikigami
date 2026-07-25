//! Versioned harness settings.
//!
//! Resolve: defaults → optional file → environment → CLI.
//! Tenkai is not a runtime setting.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub version: u32,
    #[serde(default)]
    pub profile: ProfileSettings,
    #[serde(default)]
    pub governance: GovernanceSettings,
    #[serde(default)]
    pub workspace: WorkspaceSettings,
    #[serde(default)]
    pub tools: ToolsSettings,
    #[serde(default)]
    pub run: RunSettings,
    #[serde(default)]
    pub events: EventsSettings,
    #[serde(default)]
    pub model: ModelSettings,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileSettings {
    #[serde(default = "default_profile_name")]
    pub name: String,
}

fn default_profile_name() -> String {
    "local".into()
}

impl Default for ProfileSettings {
    fn default() -> Self {
        Self {
            name: default_profile_name(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GovernanceSettings {
    #[serde(default = "default_governance_adapter")]
    pub adapter: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    #[serde(default = "default_principal")]
    pub principal: String,
    #[serde(default)]
    pub fail_closed: bool,
    /// Namespace for plane operations (sekai-chisei).
    #[serde(default = "default_namespace")]
    pub namespace: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_env: Option<String>,
}

fn default_governance_adapter() -> String {
    "none".into()
}
fn default_principal() -> String {
    "shikigami".into()
}
fn default_namespace() -> String {
    "default".into()
}

impl Default for GovernanceSettings {
    fn default() -> Self {
        Self {
            adapter: default_governance_adapter(),
            endpoint: None,
            principal: default_principal(),
            fail_closed: false,
            namespace: default_namespace(),
            token_env: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceSettings {
    #[serde(default = "default_workspace_adapter")]
    pub adapter: String,
    #[serde(default = "default_workspace_root")]
    pub root: String,
    #[serde(default = "default_branch_prefix")]
    pub branch_prefix: String,
}

fn default_workspace_adapter() -> String {
    "directory".into()
}
fn default_workspace_root() -> String {
    ".".into()
}
fn default_branch_prefix() -> String {
    "shikigami/".into()
}

impl Default for WorkspaceSettings {
    fn default() -> Self {
        Self {
            adapter: default_workspace_adapter(),
            root: default_workspace_root(),
            branch_prefix: default_branch_prefix(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolsSettings {
    #[serde(default)]
    pub enabled: Vec<String>,
    #[serde(default = "default_bash_timeout")]
    pub bash_timeout_secs: u64,
}

fn default_bash_timeout() -> u64 {
    60
}

impl Default for ToolsSettings {
    fn default() -> Self {
        Self {
            enabled: Vec::new(),
            bash_timeout_secs: default_bash_timeout(),
        }
    }
}

impl ToolsSettings {
    pub fn effective_enabled(&self) -> Vec<String> {
        if self.enabled.is_empty() {
            // Local default: no bash unless explicitly enabled (safety).
            vec![
                "read_file".into(),
                "write_file".into(),
                "edit".into(),
                "report".into(),
                "escalate".into(),
            ]
        } else {
            self.enabled.clone()
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunSettings {
    #[serde(default = "default_max_turns")]
    pub max_turns: u32,
    /// Optional overall wall-clock limit in seconds (checked at turn boundaries).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_secs: Option<u64>,
}

fn default_max_turns() -> u32 {
    50
}

impl Default for RunSettings {
    fn default() -> Self {
        Self {
            max_turns: default_max_turns(),
            timeout_secs: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventsSettings {
    #[serde(default = "default_events_adapter")]
    pub adapter: String,
}

fn default_events_adapter() -> String {
    "stderr".into()
}

impl Default for EventsSettings {
    fn default() -> Self {
        Self {
            adapter: default_events_adapter(),
        }
    }
}

/// Model source for turns when governance does not own planning (none/local).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelSettings {
    /// `scripted` | `http` | `plane` (plane forces governance sekai-chisei path).
    #[serde(default = "default_model_adapter")]
    pub adapter: String,
    /// OpenAI-compatible base URL (http adapter).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    #[serde(default = "default_model_name")]
    pub model: String,
    /// Env var holding API key for http adapter.
    #[serde(default = "default_api_key_env")]
    pub api_key_env: String,
    /// Inline JSON array of scripted turns for tests/demos.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub script_json: Option<String>,
}

fn default_model_adapter() -> String {
    "scripted".into()
}
fn default_model_name() -> String {
    "gpt-4.1-mini".into()
}
fn default_api_key_env() -> String {
    "OPENAI_API_KEY".into()
}

impl Default for ModelSettings {
    fn default() -> Self {
        Self {
            adapter: default_model_adapter(),
            base_url: None,
            model: default_model_name(),
            api_key_env: default_api_key_env(),
            script_json: None,
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            version: Self::CURRENT_VERSION,
            profile: ProfileSettings::default(),
            governance: GovernanceSettings::default(),
            workspace: WorkspaceSettings::default(),
            tools: ToolsSettings::default(),
            run: RunSettings::default(),
            events: EventsSettings::default(),
            model: ModelSettings::default(),
        }
    }
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read config at {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to write config at {path}: {source}")]
    Write {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse config at {path}: {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },
    #[error("failed to serialize config: {0}")]
    Serialize(#[from] toml::ser::Error),
    #[error("unsupported config version {found}; expected {expected}")]
    UnsupportedVersion { found: u32, expected: u32 },
    #[error("unknown governance adapter `{0}`")]
    UnknownGovernanceAdapter(String),
    #[error("unknown workspace adapter `{0}`")]
    UnknownWorkspaceAdapter(String),
    #[error("unknown events adapter `{0}`")]
    UnknownEventsAdapter(String),
    #[error("unknown model adapter `{0}`")]
    UnknownModelAdapter(String),
    #[error("governance adapter `sekai-chisei` requires an endpoint")]
    MissingGovernanceEndpoint,
}

impl Config {
    pub const FILENAME: &'static str = "shikigami.toml";
    pub const CURRENT_VERSION: u32 = 1;
    pub const CONFIG_PATH_ENV: &'static str = "SHIKIGAMI_CONFIG";
    pub const CONTROL_PLANE_ENV: &'static str = "SHIKIGAMI_CONTROL_PLANE";
    pub const GOVERNANCE_ADAPTER_ENV: &'static str = "SHIKIGAMI_GOVERNANCE_ADAPTER";
    pub const PROFILE_ENV: &'static str = "SHIKIGAMI_PROFILE";
    pub const MODEL_ADAPTER_ENV: &'static str = "SHIKIGAMI_MODEL_ADAPTER";
    pub const MODEL_SCRIPT_ENV: &'static str = "SHIKIGAMI_MODEL_SCRIPT";

    pub fn path_in(root: impl AsRef<Path>) -> PathBuf {
        root.as_ref().join(Self::FILENAME)
    }

    pub fn resolve(path: impl AsRef<Path>) -> Result<(Self, ConfigSource), ConfigError> {
        let path = path.as_ref();
        let (mut config, source) = if path.is_file() {
            (Self::load(path)?, ConfigSource::File(path.to_path_buf()))
        } else {
            (Self::default(), ConfigSource::Defaults)
        };
        config.apply_profile_presets();
        config.apply_env();
        config.validate()?;
        Ok((config, source))
    }

    pub fn resolve_search(
        explicit: Option<&Path>,
        state_root: &Path,
        cwd: &Path,
    ) -> Result<(Self, ConfigSource), ConfigError> {
        if let Some(path) = explicit {
            return Self::resolve(path);
        }
        if let Ok(path) = env::var(Self::CONFIG_PATH_ENV)
            && !path.is_empty()
        {
            return Self::resolve(PathBuf::from(path));
        }
        let under_state = Self::path_in(state_root);
        if under_state.is_file() {
            return Self::resolve(under_state);
        }
        let under_cwd = cwd.join(Self::FILENAME);
        if under_cwd.is_file() {
            return Self::resolve(under_cwd);
        }
        Self::resolve(under_state)
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let raw = fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        let mut config: Self = toml::from_str(&raw).map_err(|source| ConfigError::Parse {
            path: path.to_path_buf(),
            source,
        })?;
        if config.version != Self::CURRENT_VERSION {
            return Err(ConfigError::UnsupportedVersion {
                found: config.version,
                expected: Self::CURRENT_VERSION,
            });
        }
        config.apply_profile_presets();
        Ok(config)
    }

    pub fn save(&self, path: impl AsRef<Path>) -> Result<(), ConfigError> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|source| ConfigError::Write {
                path: path.to_path_buf(),
                source,
            })?;
        }
        let raw = toml::to_string_pretty(self)?;
        fs::write(path, raw).map_err(|source| ConfigError::Write {
            path: path.to_path_buf(),
            source,
        })
    }

    fn apply_profile_presets(&mut self) {
        if self.profile.name == "governed" {
            if self.governance.adapter == "none" {
                self.governance.adapter = "sekai-chisei".into();
            }
            self.governance.fail_closed = true;
            if self.model.adapter == "scripted" {
                self.model.adapter = "plane".into();
            }
        }
    }

    fn apply_env(&mut self) {
        if let Ok(value) = env::var(Self::PROFILE_ENV)
            && !value.is_empty()
        {
            self.profile.name = value;
            self.apply_profile_presets();
        }
        if let Ok(value) = env::var(Self::GOVERNANCE_ADAPTER_ENV)
            && !value.is_empty()
        {
            self.governance.adapter = value;
        }
        if let Ok(value) = env::var(Self::CONTROL_PLANE_ENV)
            && !value.is_empty()
        {
            self.governance.endpoint = Some(value);
        }
        if let Ok(value) = env::var(Self::MODEL_ADAPTER_ENV)
            && !value.is_empty()
        {
            self.model.adapter = value;
        }
        if let Ok(value) = env::var(Self::MODEL_SCRIPT_ENV)
            && !value.is_empty()
        {
            self.model.script_json = Some(value);
        }
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        match self.governance.adapter.as_str() {
            "none" | "local" | "sekai-chisei" => {}
            other => return Err(ConfigError::UnknownGovernanceAdapter(other.into())),
        }
        match self.workspace.adapter.as_str() {
            "directory" | "git-worktree" => {}
            other => return Err(ConfigError::UnknownWorkspaceAdapter(other.into())),
        }
        match self.events.adapter.as_str() {
            "stderr" | "jsonl" | "none" => {}
            other => return Err(ConfigError::UnknownEventsAdapter(other.into())),
        }
        match self.model.adapter.as_str() {
            "scripted" | "http" | "plane" => {}
            other => return Err(ConfigError::UnknownModelAdapter(other.into())),
        }
        Ok(())
    }

    pub fn requires_governance(&self) -> bool {
        self.governance.fail_closed || self.profile.name == "governed"
    }

    pub fn uses_plane_model(&self) -> bool {
        self.governance.adapter == "sekai-chisei" || self.model.adapter == "plane"
    }

    pub fn governance_endpoint_required(&self) -> Result<(), ConfigError> {
        if self.governance.adapter == "sekai-chisei"
            && self
                .governance
                .endpoint
                .as_ref()
                .map(|s| s.trim().is_empty())
                .unwrap_or(true)
        {
            return Err(ConfigError::MissingGovernanceEndpoint);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigSource {
    Defaults,
    File(PathBuf),
}

impl ConfigSource {
    pub fn description(&self) -> String {
        match self {
            Self::Defaults => "defaults (optional file and env may override)".into(),
            Self::File(path) => format!("file {}", path.display()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn governed_profile_selects_sekai_and_plane_model() {
        let dir = tempdir().unwrap();
        let path = Config::path_in(dir.path());
        fs::write(
            &path,
            r#"
version = 1
[profile]
name = "governed"
"#,
        )
        .unwrap();
        let (c, _) = Config::resolve(&path).unwrap();
        assert_eq!(c.governance.adapter, "sekai-chisei");
        assert!(c.governance.fail_closed);
        assert_eq!(c.model.adapter, "plane");
    }

    #[test]
    fn loads_nested_settings() {
        let dir = tempdir().unwrap();
        let path = Config::path_in(dir.path());
        fs::write(
            &path,
            r#"
version = 1
[governance]
adapter = "local"
[model]
adapter = "scripted"
script_json = "[]"
"#,
        )
        .unwrap();
        let (c, _) = Config::resolve(&path).unwrap();
        assert_eq!(c.governance.adapter, "local");
        assert_eq!(c.model.adapter, "scripted");
    }

    #[test]
    fn rejects_unknown_top_level_key() {
        let dir = tempdir().unwrap();
        let path = Config::path_in(dir.path());
        fs::write(
            &path,
            r#"
version = 1
unknown_thing = true
"#,
        )
        .unwrap();
        let err = Config::load(&path).unwrap_err();
        assert!(matches!(err, ConfigError::Parse { .. }));
    }
}
