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
    #[serde(default)]
    pub context: ContextSettings,
    #[serde(default)]
    pub network: NetworkSettings,
    /// Operator-trusted lifecycle hooks (disabled when empty). See docs/hooks.md.
    #[serde(default)]
    pub hooks: Vec<HookSettings>,
}

/// One lifecycle hook entry (`command` is operator-trusted).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookSettings {
    /// `pre_run` | `post_run` | `pre_tool` | `post_tool` | `on_park`
    pub event: String,
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default = "default_hook_timeout_ms")]
    pub timeout_ms: u64,
    /// When true, hook failure/timeout aborts the run or tool.
    #[serde(default)]
    pub fail_closed: bool,
}

fn default_hook_timeout_ms() -> u64 {
    5_000
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
    /// Copy workspace to state after materialize for later restore.
    #[serde(default)]
    pub snapshot: bool,
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
            snapshot: false,
        }
    }
}

/// Host tool authority mode (composes with `enabled` allow-list).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum PermissionMode {
    /// Use `enabled` if set, otherwise the safe coding default (no bash).
    #[default]
    Custom,
    /// Read/search only (+ report/escalate).
    Read,
    /// Read + write/edit (no bash).
    Workspace,
    /// Workspace + bash.
    WorkspaceExec,
}

impl PermissionMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Custom => "custom",
            Self::Read => "read",
            Self::Workspace => "workspace",
            Self::WorkspaceExec => "workspace_exec",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolAuthoritySummary {
    pub configured_enabled: Vec<String>,
    pub preset_enabled: Vec<String>,
    pub excluded_by_intersection: Vec<String>,
    pub effective_enabled: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolsSettings {
    #[serde(default)]
    pub enabled: Vec<String>,
    /// Expands to a base tool set; non-empty `enabled` intersects (further restricts).
    #[serde(default)]
    pub mode: PermissionMode,
    #[serde(default = "default_bash_timeout")]
    pub bash_timeout_secs: u64,
    /// When true (default), `glob`/`grep` honor built-in defaults plus
    /// workspace `.gitignore` and `.shikigamiignore`. `read_file` of an
    /// explicit path is never blocked by ignore rules.
    #[serde(default = "default_respect_ignore")]
    pub respect_ignore: bool,
    /// MCP servers whose tools are registered as `mcp.<name>.<tool>`.
    #[serde(default)]
    pub mcp_servers: Vec<McpServerSettings>,
}

/// MCP server configuration (stdio command or HTTP URL).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpServerSettings {
    pub name: String,
    /// Executable for stdio transport. Special value `mock` registers an offline echo tool.
    /// Empty when using `url` / HTTP transport.
    #[serde(default)]
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    /// `stdio` (default) or `http` (JSON-RPC POST; SSE stream not required for v1).
    #[serde(default = "default_mcp_transport")]
    pub transport: String,
    /// Base URL for HTTP transport (e.g. `http://127.0.0.1:8080/mcp`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// Optional env var holding a Bearer token for HTTP MCP.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_env: Option<String>,
}

fn default_mcp_transport() -> String {
    "stdio".into()
}

fn default_bash_timeout() -> u64 {
    60
}

fn default_respect_ignore() -> bool {
    true
}

impl Default for ToolsSettings {
    fn default() -> Self {
        Self {
            enabled: Vec::new(),
            mode: PermissionMode::Custom,
            bash_timeout_secs: default_bash_timeout(),
            respect_ignore: default_respect_ignore(),
            mcp_servers: Vec::new(),
        }
    }
}

impl ToolsSettings {
    /// Safe coding default without bash (also used by `custom` when enabled is empty).
    pub fn default_coding_tools() -> Vec<String> {
        vec![
            "read_file".into(),
            "write_file".into(),
            "edit".into(),
            "multi_edit".into(),
            "apply_patch".into(),
            "glob".into(),
            "grep".into(),
            "todo_write".into(),
            "report".into(),
            "escalate".into(),
        ]
    }

    pub fn tools_for_mode(mode: PermissionMode) -> Vec<String> {
        match mode {
            PermissionMode::Custom => Self::default_coding_tools(),
            PermissionMode::Read => vec![
                "read_file".into(),
                "glob".into(),
                "grep".into(),
                "todo_write".into(),
                "report".into(),
                "escalate".into(),
            ],
            PermissionMode::Workspace => Self::default_coding_tools(),
            PermissionMode::WorkspaceExec => {
                let mut t = Self::default_coding_tools();
                t.push("bash".into());
                // bg job tools are auto-exposed when bash is enabled (definitions())
                t
            }
        }
    }

    pub fn effective_enabled(&self) -> Vec<String> {
        let base = match self.mode {
            PermissionMode::Custom if self.enabled.is_empty() => Self::default_coding_tools(),
            PermissionMode::Custom => self.enabled.clone(),
            other => Self::tools_for_mode(other),
        };
        if matches!(self.mode, PermissionMode::Custom) || self.enabled.is_empty() {
            return base;
        }
        // Non-custom mode + explicit enabled → intersect (operator can only remove tools).
        base.into_iter()
            .filter(|t| self.enabled.iter().any(|e| e == t))
            .collect()
    }

    pub fn authority_summary(&self) -> ToolAuthoritySummary {
        let preset_enabled = Self::tools_for_mode(self.mode);
        let effective_enabled = self.effective_enabled();
        let excluded_by_intersection =
            if matches!(self.mode, PermissionMode::Custom) || self.enabled.is_empty() {
                Vec::new()
            } else {
                preset_enabled
                    .iter()
                    .filter(|tool| !effective_enabled.contains(tool))
                    .cloned()
                    .collect()
            };
        ToolAuthoritySummary {
            configured_enabled: self.enabled.clone(),
            preset_enabled,
            excluded_by_intersection,
            effective_enabled,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunSettings {
    #[serde(default = "default_max_turns")]
    pub max_turns: u32,
    /// Max concurrent tool executions for a batch of parallel-safe tools only.
    /// `1` forces sequential execution. Writes always force a serial batch.
    #[serde(default = "default_tool_concurrency")]
    pub tool_concurrency: u32,
    /// Optional overall wall-clock limit in seconds (checked at turn boundaries).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_secs: Option<u64>,
    /// When message count exceeds this, compact middle history (None = disabled).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compact_after_messages: Option<u32>,
    /// Messages to retain after the initial user task when compacting (default 8).
    #[serde(default = "default_compact_keep_tail")]
    pub compact_keep_tail: u32,
}

fn default_max_turns() -> u32 {
    50
}

fn default_tool_concurrency() -> u32 {
    4
}

fn default_compact_keep_tail() -> u32 {
    8
}

impl Default for RunSettings {
    fn default() -> Self {
        Self {
            max_turns: default_max_turns(),
            tool_concurrency: default_tool_concurrency(),
            timeout_secs: None,
            compact_after_messages: None,
            compact_keep_tail: default_compact_keep_tail(),
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
    /// Optional cost rate: USD **microdollars** per million input tokens
    /// (1_000_000 = $1.00 / MTok). When unset with output rate, no cost estimate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_usd_micros_per_mtok: Option<u64>,
    /// Optional cost rate: USD microdollars per million output tokens.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_usd_micros_per_mtok: Option<u64>,
}

fn default_model_adapter() -> String {
    "scripted".into()
}
fn default_model_name() -> String {
    "auto".into()
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
            input_usd_micros_per_mtok: None,
            output_usd_micros_per_mtok: None,
        }
    }
}

/// Project rules / extra context attached to runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextSettings {
    /// Discover and load the first matching project rules file from the workspace.
    #[serde(default = "default_true")]
    pub load_project_rules: bool,
    /// Filenames tried in order under the workspace root.
    #[serde(default = "default_rules_filenames")]
    pub rules_filenames: Vec<String>,
    /// Max bytes of rules text injected into the system prompt.
    #[serde(default = "default_max_rules_bytes")]
    pub max_rules_bytes: usize,
    /// Root directory for skill packs (relative to workspace, or absolute).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skills_root: Option<String>,
    /// Skill directory names under `skills_root` (each contains `SKILL.md`).
    #[serde(default)]
    pub skills: Vec<String>,
    /// Max bytes per skill body.
    #[serde(default = "default_max_rules_bytes")]
    pub max_skill_bytes: usize,
}

fn default_true() -> bool {
    true
}

fn default_rules_filenames() -> Vec<String> {
    vec!["AGENTS.md".into(), "shikigami.rules.md".into()]
}

fn default_max_rules_bytes() -> usize {
    32 * 1024
}

impl Default for ContextSettings {
    fn default() -> Self {
        Self {
            load_project_rules: default_true(),
            rules_filenames: default_rules_filenames(),
            max_rules_bytes: default_max_rules_bytes(),
            skills_root: None,
            skills: Vec::new(),
            max_skill_bytes: default_max_rules_bytes(),
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
            context: ContextSettings::default(),
            network: NetworkSettings::default(),
            hooks: Vec::new(),
        }
    }
}

/// Network egress policy (honest residual risk for unrestricted bash).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum EgressMode {
    /// No harness-level network restriction (default OSS behavior).
    #[default]
    Unrestricted,
    /// Block harness HTTP client calls (model http adapter).
    Deny,
    /// Only listed hosts for harness HTTP client.
    Allowlist,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NetworkSettings {
    #[serde(default)]
    pub egress: EgressMode,
    /// Hostnames allowed when `egress = allowlist` (exact match, case-insensitive).
    #[serde(default)]
    pub allow_hosts: Vec<String>,
}

impl Default for NetworkSettings {
    fn default() -> Self {
        Self {
            egress: EgressMode::Unrestricted,
            allow_hosts: Vec::new(),
        }
    }
}

impl NetworkSettings {
    /// Validate an HTTP(S) URL against egress policy.
    pub fn check_http_url(&self, url: &str) -> Result<(), String> {
        match self.egress {
            EgressMode::Unrestricted => Ok(()),
            EgressMode::Deny => Err("network egress denied by settings (egress=deny)".into()),
            EgressMode::Allowlist => {
                let host = url::Url::parse(url)
                    .ok()
                    .and_then(|u| u.host_str().map(|h| h.to_ascii_lowercase()))
                    .ok_or_else(|| format!("cannot parse host from URL for egress check: {url}"))?;
                let ok = self
                    .allow_hosts
                    .iter()
                    .any(|h| h.eq_ignore_ascii_case(&host));
                if ok {
                    Ok(())
                } else {
                    Err(format!(
                        "host `{host}` not in network.allow_hosts (egress=allowlist)"
                    ))
                }
            }
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
    #[error("{0}")]
    Invalid(String),
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
            "none" | "local" | "sekai-chisei" | "http-callback" | "host-authz" => {}
            other => return Err(ConfigError::UnknownGovernanceAdapter(other.into())),
        }
        if matches!(
            self.governance.adapter.as_str(),
            "http-callback" | "host-authz"
        ) && self
            .governance
            .endpoint
            .as_ref()
            .map(|s| s.trim().is_empty())
            .unwrap_or(true)
        {
            return Err(ConfigError::Invalid(
                "governance adapter `http-callback` requires governance.endpoint".into(),
            ));
        }
        match self.workspace.adapter.as_str() {
            "directory" | "inplace" | "directory-inplace" | "git-worktree" => {}
            other => return Err(ConfigError::UnknownWorkspaceAdapter(other.into())),
        }
        if self.workspace.snapshot
            && matches!(
                self.workspace.adapter.as_str(),
                "inplace" | "directory-inplace"
            )
        {
            return Err(ConfigError::Invalid(
                "workspace.snapshot cannot be used with adapter `inplace`".into(),
            ));
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

    /// Credential environment names consumed by the harness and never exposed
    /// to agent-controlled Bash subprocesses.
    pub(crate) fn protected_tool_environment_names(&self) -> Vec<String> {
        let mut names = Vec::new();
        if let Some(name) = &self.governance.token_env {
            names.push(name.clone());
        }
        if self.model.adapter == "http" {
            names.push(self.model.api_key_env.clone());
        }
        names.extend(
            self.tools
                .mcp_servers
                .iter()
                .filter_map(|server| server.token_env.clone()),
        );
        names.sort_by_key(|name| name.to_ascii_uppercase());
        names.dedup_by(|a, b| a.eq_ignore_ascii_case(b));
        names
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
        assert_eq!(c.model.model, "auto");
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
    fn configured_harness_credentials_are_protected_from_tools() {
        let mut config = Config::default();
        config.governance.token_env = Some("PLANE_TOKEN".into());
        config.model.adapter = "http".into();
        config.model.api_key_env = "MODEL_KEY".into();
        config.tools.mcp_servers.push(McpServerSettings {
            name: "remote".into(),
            command: String::new(),
            args: Vec::new(),
            transport: "http".into(),
            url: Some("https://mcp.example".into()),
            token_env: Some("MCP_TOKEN".into()),
        });

        assert_eq!(
            config.protected_tool_environment_names(),
            vec!["MCP_TOKEN", "MODEL_KEY", "PLANE_TOKEN"]
        );

        config.model.adapter = "scripted".into();
        assert!(
            !config
                .protected_tool_environment_names()
                .contains(&"MODEL_KEY".into())
        );
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

    #[test]
    fn property_unknown_adapters_fail_validate() {
        use proptest::prelude::*;
        proptest!(|(name in "[a-z]{3,12}")| {
            let known = matches!(
                name.as_str(),
                "none" | "local" | "sekai-chisei"
            );
            let mut c = Config::default();
            c.governance.adapter = name.clone();
            let result = c.validate();
            if known {
                prop_assert!(result.is_ok());
            } else {
                let bad = matches!(result, Err(ConfigError::UnknownGovernanceAdapter(_)));
                prop_assert!(bad, "expected unknown adapter error for {}", name);
            }
        });
    }

    #[test]
    fn property_unknown_toml_keys_rejected() {
        use proptest::prelude::*;
        proptest!(|(key in "[a-z]{4,16}")| {
            prop_assume!(key != "version");
            let dir = tempdir().unwrap();
            let path = Config::path_in(dir.path());
            let body = format!("version = 1\n{key} = true\n");
            fs::write(&path, body).unwrap();
            let err = Config::load(&path).unwrap_err();
            let is_parse = matches!(err, ConfigError::Parse { .. });
            prop_assert!(is_parse, "expected Parse for key {}, got {}", key, err);
        });
    }

    #[test]
    fn egress_allowlist_and_deny() {
        let deny = NetworkSettings {
            egress: EgressMode::Deny,
            ..Default::default()
        };
        assert!(deny.check_http_url("https://api.openai.com/v1").is_err());
        let allow = NetworkSettings {
            egress: EgressMode::Allowlist,
            allow_hosts: vec!["api.openai.com".into()],
        };
        assert!(
            allow
                .check_http_url("https://api.openai.com/v1/chat")
                .is_ok()
        );
        assert!(allow.check_http_url("https://evil.example/v1").is_err());
        let open = NetworkSettings::default();
        assert!(open.check_http_url("https://evil.example/v1").is_ok());
    }

    #[test]
    fn permission_mode_read_excludes_write_and_bash() {
        let mut c = Config::default();
        c.tools.mode = PermissionMode::Read;
        let e = c.tools.effective_enabled();
        assert!(e.contains(&"read_file".into()));
        assert!(e.contains(&"grep".into()));
        assert!(!e.contains(&"write_file".into()));
        assert!(!e.contains(&"bash".into()));
    }

    #[test]
    fn permission_mode_workspace_exec_includes_bash() {
        let mut c = Config::default();
        c.tools.mode = PermissionMode::WorkspaceExec;
        assert!(c.tools.effective_enabled().contains(&"bash".into()));
    }

    #[test]
    fn permission_mode_intersect_with_enabled() {
        let mut c = Config::default();
        c.tools.mode = PermissionMode::Workspace;
        c.tools.enabled = vec!["read_file".into(), "bash".into()];
        let e = c.tools.effective_enabled();
        assert_eq!(e, vec!["read_file".to_string()]);
    }

    #[test]
    fn tool_authority_summary_characterizes_all_compositions() {
        let custom_default = ToolsSettings::default().authority_summary();
        assert_eq!(
            custom_default.effective_enabled,
            ToolsSettings::default_coding_tools()
        );
        assert!(custom_default.configured_enabled.is_empty());
        assert!(custom_default.excluded_by_intersection.is_empty());

        let custom_explicit = ToolsSettings {
            enabled: vec!["report".into()],
            ..Default::default()
        }
        .authority_summary();
        assert_eq!(custom_explicit.effective_enabled, vec!["report"]);
        assert!(custom_explicit.excluded_by_intersection.is_empty());

        let read = ToolsSettings {
            mode: PermissionMode::Read,
            ..Default::default()
        }
        .authority_summary();
        assert!(read.effective_enabled.contains(&"read_file".into()));
        assert!(!read.effective_enabled.contains(&"write_file".into()));

        let workspace = ToolsSettings {
            mode: PermissionMode::Workspace,
            enabled: vec!["read_file".into(), "bash".into()],
            ..Default::default()
        }
        .authority_summary();
        assert_eq!(workspace.effective_enabled, vec!["read_file"]);
        assert!(
            workspace
                .excluded_by_intersection
                .contains(&"write_file".into())
        );
        assert!(!workspace.excluded_by_intersection.contains(&"bash".into()));

        let workspace_exec = ToolsSettings {
            mode: PermissionMode::WorkspaceExec,
            ..Default::default()
        }
        .authority_summary();
        assert!(workspace_exec.effective_enabled.contains(&"bash".into()));
    }

    #[test]
    fn property_default_config_always_validates() {
        use proptest::prelude::*;
        // Max turns in a sane range must keep validate ok for defaults.
        proptest!(|(max_turns in 1u32..10_000)| {
            let mut c = Config::default();
            c.run.max_turns = max_turns;
            prop_assert!(c.validate().is_ok());
            prop_assert!(!c.requires_governance());
        });
    }
}
