//! Versioned, content-bound run replay contracts.
//!
//! Replay evidence is host-supplied comparative input. It is not a governance
//! receipt and does not make local checkpoints authoritative.

use std::collections::BTreeMap;
use std::path::Path;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::sync::watch;

use crate::checkpoint::{Checkpoint, CheckpointError};
use crate::config::{Config, EgressMode, PermissionMode};
use crate::model::{ChatMessage, CostEstimate, TokenUsage};
use crate::registry::{RegistryError, RunRegistry};
use crate::run::{RunError, RunResult, RunTermination};
use crate::tools::ToolDef;

pub const REPLAY_SCHEMA_VERSION: u32 = 1;
pub const MAX_REPLAY_BUNDLE_BYTES: usize = 1024 * 1024;
pub const MAX_REPLAY_STEPS: usize = 1024;
const MAX_REPLAY_TASK_BYTES: usize = 256 * 1024;
const MAX_REPLAY_INPUT_FILES: usize = 10_000;
const MAX_REPLAY_INPUT_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayManifest {
    pub schema_version: u32,
    pub evidence_digest: String,
    pub bindings: ReplayBindings,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayEvidenceBundle {
    pub schema_version: u32,
    pub source_run_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_logical_operation_id: Option<String>,
    pub task: String,
    pub bindings: ReplayBindings,
    pub expected_steps: Vec<ReplayStepEvidence>,
    pub expected_terminal: ReplayTerminalEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayBindings {
    pub task_digest: String,
    pub inputs_digest: String,
    pub prompt_digest: String,
    pub model_digest: String,
    pub tool_catalog_digest: String,
    pub policy_evidence_digest: String,
    pub retained_evidence_digest: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplayStepKind {
    Model,
    Tool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayStepEvidence {
    pub kind: ReplayStepKind,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    pub content_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayTerminalEvidence {
    pub success: bool,
    pub termination: String,
    pub summary_digest: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplayComparisonStatus {
    Equal,
    Changed,
    Missing,
    Unsupported,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayStepComparison {
    pub index: usize,
    pub status: ReplayComparisonStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected: Option<ReplayStepEvidence>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actual: Option<ReplayStepEvidence>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayTerminalComparison {
    pub status: ReplayComparisonStatus,
    pub expected: ReplayTerminalEvidence,
    pub actual: ReplayTerminalEvidence,
}

#[derive(Debug, Clone)]
pub struct ReplayRequest {
    pub manifest: ReplayManifest,
    pub evidence: ReplayEvidenceBundle,
    pub keep_workspace: bool,
    pub cancel: Option<watch::Receiver<bool>>,
    /// Restart a previously admitted replay attempt, never the source run.
    pub resume_run_id: Option<String>,
}

impl ReplayRequest {
    pub fn new(manifest: ReplayManifest, evidence: ReplayEvidenceBundle) -> Self {
        Self {
            manifest,
            evidence,
            // Replay workspaces are comparative evidence and remain available
            // by default. Hosts may opt into ordinary successful-run cleanup.
            keep_workspace: true,
            cancel: None,
            resume_run_id: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReplayResult {
    pub run: RunResult,
    pub manifest_digest: String,
    pub steps: Vec<ReplayStepComparison>,
    pub terminal: ReplayTerminalComparison,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayCheckpoint {
    pub manifest_digest: String,
    pub source_run_id: String,
    pub workspace: String,
    #[serde(default)]
    pub comparison_cursor: usize,
    #[serde(default)]
    pub usage: TokenUsage,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal: Option<ReplayTerminalCheckpoint>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayTerminalCheckpoint {
    pub success: bool,
    pub termination: RunTermination,
    pub summary: String,
    pub prompt_id: String,
    pub usage: TokenUsage,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost: Option<CostEstimate>,
    #[serde(default)]
    pub finalized: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_dir: Option<String>,
}

#[derive(Debug, Error)]
pub enum ReplayError {
    #[error("replay manifest: {0}")]
    Invalid(String),
    #[error("replay binding `{surface}` changed")]
    BindingMismatch { surface: &'static str },
    #[error("governed replay evidence cannot be verified by adapter `{0}`")]
    GovernanceEvidenceUnsupported(String),
    #[error("replay input I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Checkpoint(#[from] CheckpointError),
    #[error(transparent)]
    Registry(#[from] RegistryError),
    #[error(transparent)]
    Run(#[from] RunError),
}

#[derive(Debug, Clone)]
pub(crate) struct ReplayExecution {
    pub manifest_digest: String,
    pub source_run_id: String,
    pub logical_operation_id: Option<String>,
    pub bindings: ReplayBindings,
    pub expected_steps: Vec<ReplayStepEvidence>,
    pub expected_terminal: ReplayTerminalEvidence,
}

pub(crate) struct RecoveredReplay {
    pub run: RunResult,
    pub finalized: bool,
    pub keep_workspace: bool,
    pub workspace_adapter: String,
}

impl ReplayManifest {
    pub fn for_evidence(evidence: &ReplayEvidenceBundle) -> Result<Self, ReplayError> {
        evidence.validate()?;
        Ok(Self {
            schema_version: REPLAY_SCHEMA_VERSION,
            evidence_digest: canonical_digest(evidence)?,
            bindings: evidence.bindings.clone(),
        })
    }
}

impl ReplayEvidenceBundle {
    pub fn validate(&self) -> Result<(), ReplayError> {
        if self.schema_version != REPLAY_SCHEMA_VERSION {
            return Err(ReplayError::Invalid(format!(
                "unsupported evidence schema version {}; expected {}",
                self.schema_version, REPLAY_SCHEMA_VERSION
            )));
        }
        if !crate::checkpoint::is_safe_run_id(&self.source_run_id) {
            return Err(ReplayError::Invalid(
                "source_run_id must be a non-empty opaque ASCII identifier".into(),
            ));
        }
        if self.task.is_empty() || self.task.len() > MAX_REPLAY_TASK_BYTES {
            return Err(ReplayError::Invalid(format!(
                "task must be 1..={MAX_REPLAY_TASK_BYTES} bytes"
            )));
        }
        if contains_credential_material(&self.source_run_id)
            || self
                .source_logical_operation_id
                .as_deref()
                .is_some_and(contains_credential_material)
            || contains_credential_material(&self.task)
        {
            return Err(ReplayError::Invalid(
                "evidence bundle contains credential-shaped material".into(),
            ));
        }
        if self.expected_steps.len() > MAX_REPLAY_STEPS {
            return Err(ReplayError::Invalid(format!(
                "expected_steps exceeds {MAX_REPLAY_STEPS}"
            )));
        }
        validate_bindings(&self.bindings)?;
        for step in &self.expected_steps {
            validate_digest("step content", &step.content_digest)?;
            if step.name.len() > 256 {
                return Err(ReplayError::Invalid("step name exceeds 256 bytes".into()));
            }
            if contains_credential_material(&step.name) {
                return Err(ReplayError::Invalid(
                    "evidence bundle contains credential-shaped material".into(),
                ));
            }
        }
        validate_digest("terminal summary", &self.expected_terminal.summary_digest)?;
        let encoded = canonical_json(self)?;
        if encoded.len() > MAX_REPLAY_BUNDLE_BYTES {
            return Err(ReplayError::Invalid(format!(
                "evidence bundle exceeds {MAX_REPLAY_BUNDLE_BYTES} bytes"
            )));
        }
        Ok(())
    }
}

impl ReplayRequest {
    pub(crate) fn admit(&self) -> Result<ReplayExecution, ReplayError> {
        if self.manifest.schema_version != REPLAY_SCHEMA_VERSION {
            return Err(ReplayError::Invalid(format!(
                "unsupported manifest schema version {}; expected {}",
                self.manifest.schema_version, REPLAY_SCHEMA_VERSION
            )));
        }
        self.evidence.validate()?;
        validate_digest("evidence", &self.manifest.evidence_digest)?;
        validate_bindings(&self.manifest.bindings)?;
        if self.manifest.bindings != self.evidence.bindings {
            return Err(ReplayError::Invalid(
                "manifest bindings do not match the evidence bundle".into(),
            ));
        }
        let actual_evidence_digest = canonical_digest(&self.evidence)?;
        if self.manifest.evidence_digest != actual_evidence_digest {
            return Err(ReplayError::Invalid(
                "evidence bundle digest does not match the manifest".into(),
            ));
        }
        Ok(ReplayExecution {
            manifest_digest: canonical_digest(&self.manifest)?,
            source_run_id: self.evidence.source_run_id.clone(),
            logical_operation_id: self.evidence.source_logical_operation_id.clone(),
            bindings: self.manifest.bindings.clone(),
            expected_steps: self.evidence.expected_steps.clone(),
            expected_terminal: self.evidence.expected_terminal.clone(),
        })
    }
}

impl ReplayBindings {
    /// Construct bindings for the built-in observation-only replay surface.
    pub fn for_replay(
        config: &Config,
        task: &str,
        system_prompt: &str,
        inputs_digest: impl Into<String>,
        retained_evidence_digest: impl Into<String>,
    ) -> Result<Self, ReplayError> {
        let replay = replay_config(config);
        let tools = crate::tools::definitions_for_enabled(&replay.tools.effective_enabled());
        let mut bindings = Self::from_runtime(
            &replay,
            task,
            system_prompt,
            &tools,
            inputs_digest,
            retained_evidence_digest,
        )?;
        bindings.policy_evidence_digest = local_policy_digest(config)?;
        Ok(bindings)
    }

    /// Construct the bindings a host records for one concrete runtime surface.
    pub fn from_runtime(
        config: &Config,
        task: &str,
        system_prompt: &str,
        tools: &[ToolDef],
        inputs_digest: impl Into<String>,
        retained_evidence_digest: impl Into<String>,
    ) -> Result<Self, ReplayError> {
        let inputs_digest = inputs_digest.into();
        let retained_evidence_digest = retained_evidence_digest.into();
        validate_digest("inputs", &inputs_digest)?;
        validate_digest("retained evidence", &retained_evidence_digest)?;
        Ok(Self {
            task_digest: text_digest(task),
            inputs_digest,
            prompt_digest: text_digest(system_prompt),
            model_digest: model_digest(config)?,
            tool_catalog_digest: tool_catalog_digest(tools)?,
            policy_evidence_digest: local_policy_digest(config)?,
            retained_evidence_digest,
        })
    }
}

pub(crate) fn replay_config(config: &Config) -> Config {
    let mut replay = config.clone();
    replay.tools.mode = PermissionMode::Custom;
    replay.tools.enabled = vec![
        "read_file".into(),
        "glob".into(),
        "grep".into(),
        "report".into(),
    ];
    replay.tools.mcp_servers.clear();
    replay.network.egress = EgressMode::Deny;
    replay.network.allow_hosts.clear();
    replay.hooks.clear();
    // Ordered replay evidence must remain append-only in the checkpoint.
    replay.run.compact_after_messages = None;
    replay
}

pub(crate) fn validate_runtime_bindings(
    config: &Config,
    execution: &ReplayExecution,
    task: &str,
    system_prompt: &str,
    tools: &[ToolDef],
    workspace: &Path,
) -> Result<(), ReplayError> {
    if config.requires_governance() {
        return Err(ReplayError::GovernanceEvidenceUnsupported(
            config.governance.adapter.clone(),
        ));
    }
    compare_binding("task", &execution.bindings.task_digest, &text_digest(task))?;
    compare_binding(
        "prompt",
        &execution.bindings.prompt_digest,
        &text_digest(system_prompt),
    )?;
    compare_binding(
        "inputs",
        &execution.bindings.inputs_digest,
        &workspace_digest(workspace)?,
    )?;
    compare_binding(
        "model",
        &execution.bindings.model_digest,
        &model_digest(config)?,
    )?;
    compare_binding(
        "tool_catalog",
        &execution.bindings.tool_catalog_digest,
        &tool_catalog_digest(tools)?,
    )?;
    Ok(())
}

pub(crate) fn validate_host_policy(
    config: &Config,
    bindings: &ReplayBindings,
) -> Result<(), ReplayError> {
    compare_binding(
        "policy_evidence",
        &bindings.policy_evidence_digest,
        &local_policy_digest(config)?,
    )
}

pub(crate) fn replay_checkpoint(
    execution: &ReplayExecution,
    workspace: &std::path::Path,
) -> ReplayCheckpoint {
    ReplayCheckpoint {
        manifest_digest: execution.manifest_digest.clone(),
        source_run_id: execution.source_run_id.clone(),
        workspace: workspace.display().to_string(),
        comparison_cursor: 0,
        usage: TokenUsage::default(),
        terminal: None,
    }
}

pub(crate) fn recover_terminal_replay(
    state_runs: &Path,
    registry: &RunRegistry,
    run_id: &str,
    execution: &ReplayExecution,
) -> Result<Option<RecoveredReplay>, ReplayError> {
    let checkpoint = Checkpoint::load(state_runs, run_id)?;
    let Some(replay) = &checkpoint.replay else {
        return Ok(None);
    };
    if replay.manifest_digest != execution.manifest_digest {
        return Err(ReplayError::Invalid(format!(
            "replay manifest digest mismatch for run {run_id}"
        )));
    }
    if replay.source_run_id != execution.source_run_id {
        return Err(ReplayError::Invalid(format!(
            "replay source identity mismatch for run {run_id}"
        )));
    }
    if replay.workspace != checkpoint.workspace.display().to_string() {
        return Err(ReplayError::Invalid(format!(
            "replay workspace binding mismatch for run {run_id}"
        )));
    }
    let Some(terminal) = &replay.terminal else {
        return Ok(None);
    };
    let artifact_dir = terminal
        .artifact_dir
        .clone()
        .or_else(|| {
            registry
                .load(run_id)
                .ok()
                .and_then(|record| record.artifact_dir)
        })
        .map(std::path::PathBuf::from);
    Ok(Some(RecoveredReplay {
        run: RunResult {
            run_id: checkpoint.run_id,
            success: terminal.success,
            summary: terminal.summary.clone(),
            turns: checkpoint.completed_turns,
            workspace: checkpoint.workspace,
            artifact_dir,
            termination: terminal.termination,
            park: None,
            prompt_id: terminal.prompt_id.clone(),
            usage: terminal.usage,
            cost: terminal.cost.clone(),
            todos: checkpoint.todos,
        },
        finalized: terminal.finalized,
        keep_workspace: checkpoint.keep_workspace,
        workspace_adapter: checkpoint.workspace_adapter,
    }))
}

pub(crate) fn mark_replay_finalized(
    state_runs: &Path,
    run_id: &str,
    artifact_dir: Option<&Path>,
) -> Result<(), ReplayError> {
    let mut checkpoint = Checkpoint::load(state_runs, run_id)?;
    let replay = checkpoint
        .replay
        .as_mut()
        .ok_or_else(|| ReplayError::Invalid(format!("run {run_id} is not a replay attempt")))?;
    let terminal = replay.terminal.as_mut().ok_or_else(|| {
        ReplayError::Invalid(format!("replay attempt {run_id} has no terminal result"))
    })?;
    terminal.finalized = true;
    terminal.artifact_dir = artifact_dir.map(|path| path.display().to_string());
    checkpoint.save(state_runs)?;
    Ok(())
}

pub(crate) fn complete_replay(
    state_runs: &std::path::Path,
    run: RunResult,
    execution: ReplayExecution,
) -> Result<ReplayResult, ReplayError> {
    let checkpoint = Checkpoint::load(state_runs, &run.run_id)?;
    let actual_steps = steps_from_messages(&checkpoint.messages)?;
    let steps = compare_steps(&execution.expected_steps, &actual_steps);
    let actual_terminal = ReplayTerminalEvidence {
        success: run.success,
        termination: run.termination.as_str().into(),
        summary_digest: text_digest(&run.summary),
    };
    let terminal = ReplayTerminalComparison {
        status: if execution.expected_terminal == actual_terminal {
            ReplayComparisonStatus::Equal
        } else {
            ReplayComparisonStatus::Changed
        },
        expected: execution.expected_terminal,
        actual: actual_terminal,
    };
    Ok(ReplayResult {
        run,
        manifest_digest: execution.manifest_digest,
        steps,
        terminal,
    })
}

pub fn steps_from_messages(
    messages: &[ChatMessage],
) -> Result<Vec<ReplayStepEvidence>, ReplayError> {
    let mut tool_names = BTreeMap::<String, String>::new();
    let mut out = Vec::new();
    for message in messages {
        match message.role.as_str() {
            "assistant" => {
                for call in &message.tool_calls {
                    tool_names.insert(call.id.clone(), call.name.clone());
                }
                let normalized_calls: Vec<_> = message
                    .tool_calls
                    .iter()
                    .map(|call| {
                        let arguments = serde_json::from_str::<serde_json::Value>(&call.args_json)
                            .unwrap_or_else(|_| serde_json::Value::String(call.args_json.clone()));
                        (call.name.as_str(), arguments)
                    })
                    .collect();
                out.push(ReplayStepEvidence {
                    kind: ReplayStepKind::Model,
                    name: String::new(),
                    content_digest: canonical_digest(&(
                        message.content.as_str(),
                        normalized_calls,
                    ))?,
                });
            }
            "tool" => out.push(ReplayStepEvidence {
                kind: ReplayStepKind::Tool,
                name: tool_names
                    .get(&message.tool_call_id)
                    .cloned()
                    .unwrap_or_else(|| "unknown".into()),
                content_digest: text_digest(&message.content),
            }),
            _ => {}
        }
    }
    Ok(out)
}

pub fn text_digest(text: &str) -> String {
    digest_bytes(text.replace("\r\n", "\n").as_bytes())
}

pub fn digest_bytes(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    format!(
        "sha256:{}",
        digest
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    )
}

/// Digest a bounded workspace inventory without following symbolic links.
///
/// Paths are relative and sorted; `.git` administration state is excluded.
pub fn workspace_digest(root: &Path) -> Result<String, ReplayError> {
    if !root.is_dir() {
        return Err(ReplayError::Invalid(format!(
            "replay input root is not a directory: {}",
            root.display()
        )));
    }
    let mut entries = Vec::<(String, u64, String)>::new();
    let mut stack = vec![root.to_path_buf()];
    let mut total_bytes = 0u64;
    while let Some(directory) = stack.pop() {
        for entry in std::fs::read_dir(&directory)? {
            let entry = entry?;
            let path = entry.path();
            let relative = path
                .strip_prefix(root)
                .map_err(|_| ReplayError::Invalid("workspace path escaped input root".into()))?;
            if relative
                .components()
                .next()
                .is_some_and(|component| component.as_os_str() == ".git")
            {
                continue;
            }
            let file_type = entry.file_type()?;
            if file_type.is_symlink() {
                return Err(ReplayError::Invalid(format!(
                    "replay inputs contain symbolic link `{}`",
                    relative.display()
                )));
            }
            if file_type.is_dir() {
                stack.push(path);
                continue;
            }
            if !file_type.is_file() {
                return Err(ReplayError::Invalid(format!(
                    "replay inputs contain unsupported file `{}`",
                    relative.display()
                )));
            }
            let bytes = std::fs::read(&path)?;
            total_bytes = total_bytes.saturating_add(bytes.len() as u64);
            if entries.len() >= MAX_REPLAY_INPUT_FILES || total_bytes > MAX_REPLAY_INPUT_BYTES {
                return Err(ReplayError::Invalid(format!(
                    "replay inputs exceed {MAX_REPLAY_INPUT_FILES} files or {MAX_REPLAY_INPUT_BYTES} bytes"
                )));
            }
            let relative = relative
                .to_str()
                .ok_or_else(|| ReplayError::Invalid("replay input path is not UTF-8".into()))?
                .replace('\\', "/");
            entries.push((relative, bytes.len() as u64, digest_bytes(&bytes)));
        }
    }
    entries.sort_unstable();
    canonical_digest(&entries)
}

/// Digest for a replay workspace with no files.
pub fn empty_workspace_digest() -> String {
    canonical_digest(&Vec::<(String, u64, String)>::new())
        .expect("an empty workspace inventory is serializable")
}

fn canonical_json<T: Serialize>(value: &T) -> Result<Vec<u8>, ReplayError> {
    serde_json::to_vec(value)
        .map_err(|error| ReplayError::Invalid(format!("canonical JSON encoding failed: {error}")))
}

fn canonical_digest<T: Serialize>(value: &T) -> Result<String, ReplayError> {
    Ok(digest_bytes(&canonical_json(value)?))
}

fn model_digest(config: &Config) -> Result<String, ReplayError> {
    let source = match config.model.adapter.as_str() {
        "http" => format!(
            "base_url:{}",
            config
                .model
                .base_url
                .as_deref()
                .unwrap_or("https://api.openai.com/v1")
                .trim_end_matches('/')
        ),
        "scripted" => format!(
            "script:{}",
            config
                .model
                .script_json
                .as_deref()
                .map(text_digest)
                .unwrap_or_else(|| "builtin-default-script-v1".into())
        ),
        "plane" => format!("governance:{}", config.governance.adapter),
        other => format!("adapter:{other}"),
    };
    canonical_digest(&(
        config.model.adapter.as_str(),
        crate::model::effective_model_name(config),
        source,
    ))
}

fn local_policy_digest(config: &Config) -> Result<String, ReplayError> {
    let mut enabled_tools = config.tools.effective_enabled();
    enabled_tools.sort_unstable();
    canonical_digest(&(
        config.profile.name.as_str(),
        config.governance.adapter.as_str(),
        config.governance.fail_closed,
        config.run.max_turns,
        config.run.timeout_secs,
        config.run.tool_concurrency,
        config.tools.mode.as_str(),
        enabled_tools,
        config.tools.respect_ignore,
        config.workspace.adapter.as_str(),
    ))
}

fn tool_catalog_digest(tools: &[ToolDef]) -> Result<String, ReplayError> {
    let mut catalog: Vec<_> = tools
        .iter()
        .map(|tool| {
            (
                tool.name.as_str(),
                tool.description.as_str(),
                tool.schema.as_str(),
            )
        })
        .collect();
    catalog.sort_unstable();
    canonical_digest(&catalog)
}

fn validate_bindings(bindings: &ReplayBindings) -> Result<(), ReplayError> {
    for (name, digest) in [
        ("task", &bindings.task_digest),
        ("inputs", &bindings.inputs_digest),
        ("prompt", &bindings.prompt_digest),
        ("model", &bindings.model_digest),
        ("tool catalog", &bindings.tool_catalog_digest),
        ("policy evidence", &bindings.policy_evidence_digest),
        ("retained evidence", &bindings.retained_evidence_digest),
    ] {
        validate_digest(name, digest)?;
    }
    Ok(())
}

fn validate_digest(name: &str, digest: &str) -> Result<(), ReplayError> {
    let valid = digest.len() == 71
        && digest.starts_with("sha256:")
        && digest[7..].bytes().all(|byte| byte.is_ascii_hexdigit());
    if valid {
        Ok(())
    } else {
        Err(ReplayError::Invalid(format!(
            "{name} digest must be `sha256:` plus 64 hexadecimal characters"
        )))
    }
}

fn contains_credential_material(text: &str) -> bool {
    let normalized = text.to_ascii_lowercase();
    for marker in [
        "authorization: bearer ",
        "api_key=",
        "api_key:",
        "api-key=",
        "api-key:",
        "token=",
        "token:",
        "secret=",
        "secret:",
        "password=",
        "password:",
    ] {
        let mut remainder = normalized.as_str();
        while let Some(index) = remainder.find(marker) {
            let candidate = remainder[index + marker.len()..]
                .trim_start_matches([' ', '\'', '"'])
                .split(|character: char| {
                    character.is_whitespace()
                        || matches!(character, '\'' | '"' | ',' | ';' | '}' | ']')
                })
                .next()
                .unwrap_or_default();
            let placeholder = candidate.is_empty()
                || candidate.contains("redacted")
                || candidate.starts_with('<')
                || candidate.starts_with("${");
            if candidate.len() >= 8 && !placeholder {
                return true;
            }
            remainder = &remainder[index + marker.len()..];
        }
    }
    normalized.split_whitespace().any(|word| {
        let word = word.trim_matches(|character: char| {
            !character.is_ascii_alphanumeric() && character != '_' && character != '-'
        });
        (word.starts_with("ghp_") || word.starts_with("github_pat_") || word.starts_with("sk-"))
            && word.len() >= 12
    })
}

fn compare_binding(surface: &'static str, expected: &str, actual: &str) -> Result<(), ReplayError> {
    if expected == actual {
        Ok(())
    } else {
        Err(ReplayError::BindingMismatch { surface })
    }
}

fn compare_steps(
    expected: &[ReplayStepEvidence],
    actual: &[ReplayStepEvidence],
) -> Vec<ReplayStepComparison> {
    let count = expected.len().max(actual.len());
    (0..count)
        .map(|index| {
            let expected_step = expected.get(index).cloned();
            let actual_step = actual.get(index).cloned();
            let status = match (&expected_step, &actual_step) {
                (Some(expected), Some(actual)) if expected == actual => {
                    ReplayComparisonStatus::Equal
                }
                (Some(expected), Some(actual))
                    if expected.kind != actual.kind || expected.name != actual.name =>
                {
                    ReplayComparisonStatus::Unsupported
                }
                (Some(_), Some(_)) => ReplayComparisonStatus::Changed,
                (Some(_), None) | (None, Some(_)) => ReplayComparisonStatus::Missing,
                (None, None) => unreachable!("count excludes empty tail"),
            };
            ReplayStepComparison {
                index,
                status,
                expected: expected_step,
                actual: actual_step,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::ToolCall;

    fn valid_digest(seed: &str) -> String {
        text_digest(seed)
    }

    fn bundle() -> ReplayEvidenceBundle {
        let bindings = ReplayBindings {
            task_digest: valid_digest("task"),
            inputs_digest: valid_digest("inputs"),
            prompt_digest: valid_digest("prompt"),
            model_digest: valid_digest("model"),
            tool_catalog_digest: valid_digest("tools"),
            policy_evidence_digest: valid_digest("policy"),
            retained_evidence_digest: valid_digest("source"),
        };
        ReplayEvidenceBundle {
            schema_version: REPLAY_SCHEMA_VERSION,
            source_run_id: "source-run".into(),
            source_logical_operation_id: None,
            task: "task".into(),
            bindings,
            expected_steps: vec![],
            expected_terminal: ReplayTerminalEvidence {
                success: true,
                termination: "completed".into(),
                summary_digest: valid_digest("done"),
            },
        }
    }

    #[test]
    fn manifest_binds_exact_evidence_bytes() {
        let evidence = bundle();
        let manifest = ReplayManifest::for_evidence(&evidence).unwrap();
        let request = ReplayRequest::new(manifest, evidence);
        assert!(request.keep_workspace);
        assert!(request.admit().is_ok());
    }

    #[test]
    fn changed_evidence_is_rejected() {
        let evidence = bundle();
        let manifest = ReplayManifest::for_evidence(&evidence).unwrap();
        let mut changed = evidence;
        changed.task = "changed".into();
        let error = ReplayRequest::new(manifest, changed).admit().unwrap_err();
        assert!(error.to_string().contains("digest"));
    }

    #[test]
    fn unsupported_versions_and_unknown_fields_are_rejected() {
        let mut evidence = bundle();
        evidence.schema_version = 99;
        assert!(
            evidence
                .validate()
                .unwrap_err()
                .to_string()
                .contains("version")
        );

        let evidence = bundle();
        let manifest = ReplayManifest::for_evidence(&evidence).unwrap();
        let mut value = serde_json::to_value(manifest).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert("unexpected".into(), serde_json::Value::Bool(true));
        assert!(serde_json::from_value::<ReplayManifest>(value).is_err());
    }

    #[test]
    fn oversized_and_credential_bearing_evidence_is_rejected() {
        let mut oversized = bundle();
        oversized.task = "x".repeat(MAX_REPLAY_TASK_BYTES + 1);
        assert!(
            oversized
                .validate()
                .unwrap_err()
                .to_string()
                .contains("task")
        );

        let mut too_many_steps = bundle();
        too_many_steps.expected_steps = (0..=MAX_REPLAY_STEPS)
            .map(|_| ReplayStepEvidence {
                kind: ReplayStepKind::Model,
                name: String::new(),
                content_digest: valid_digest("step"),
            })
            .collect();
        assert!(
            too_many_steps
                .validate()
                .unwrap_err()
                .to_string()
                .contains("expected_steps")
        );

        let mut credential = bundle();
        credential.task = "call with authorization: bearer secret-value-123".into();
        assert!(
            credential
                .validate()
                .unwrap_err()
                .to_string()
                .contains("credential")
        );
    }

    #[test]
    fn every_manifest_binding_is_content_bound() {
        let evidence = bundle();
        let manifest = ReplayManifest::for_evidence(&evidence).unwrap();
        for mutate in 0..7 {
            let mut changed = manifest.clone();
            let replacement = valid_digest(&format!("changed-{mutate}"));
            match mutate {
                0 => changed.bindings.task_digest = replacement,
                1 => changed.bindings.inputs_digest = replacement,
                2 => changed.bindings.prompt_digest = replacement,
                3 => changed.bindings.model_digest = replacement,
                4 => changed.bindings.tool_catalog_digest = replacement,
                5 => changed.bindings.policy_evidence_digest = replacement,
                6 => changed.bindings.retained_evidence_digest = replacement,
                _ => unreachable!(),
            }
            let error = ReplayRequest::new(changed, evidence.clone())
                .admit()
                .unwrap_err();
            assert!(
                error.to_string().contains("bindings"),
                "binding {mutate}: {error}"
            );
        }
    }

    #[test]
    fn messages_become_ordered_digest_only_evidence() {
        let messages = vec![
            ChatMessage {
                role: "assistant".into(),
                content: String::new(),
                tool_call_id: String::new(),
                tool_calls: vec![ToolCall {
                    id: "call-1".into(),
                    name: "read_file".into(),
                    args_json: r#"{"path":"README.md"}"#.into(),
                }],
            },
            ChatMessage {
                role: "tool".into(),
                content: "contents".into(),
                tool_call_id: "call-1".into(),
                tool_calls: vec![],
            },
        ];
        let steps = steps_from_messages(&messages).unwrap();
        assert_eq!(steps.len(), 2);
        assert_eq!(steps[0].kind, ReplayStepKind::Model);
        assert_eq!(steps[1].name, "read_file");
        assert!(!steps[1].content_digest.contains("contents"));
    }

    #[test]
    fn model_step_digest_ignores_provider_tool_call_ids() {
        let message = |id: &str| ChatMessage {
            role: "assistant".into(),
            content: "inspect".into(),
            tool_call_id: String::new(),
            tool_calls: vec![ToolCall {
                id: id.into(),
                name: "read_file".into(),
                args_json: r#"{"path":"README.md"}"#.into(),
            }],
        };
        let first = steps_from_messages(&[message("provider-call-1")]).unwrap();
        let second = steps_from_messages(&[message("provider-call-999")]).unwrap();
        assert_eq!(first, second);
    }

    #[test]
    fn ordered_comparison_classifies_nondeterministic_text_and_structure() {
        let step = |kind, name: &str, content: &str| ReplayStepEvidence {
            kind,
            name: name.into(),
            content_digest: valid_digest(content),
        };
        let expected = vec![
            step(ReplayStepKind::Model, "", "equal"),
            step(ReplayStepKind::Model, "", "source wording"),
            step(ReplayStepKind::Tool, "grep", "structure"),
            step(ReplayStepKind::Tool, "read_file", "missing"),
        ];
        let actual = vec![
            step(ReplayStepKind::Model, "", "equal"),
            step(ReplayStepKind::Model, "", "nondeterministic wording"),
            step(ReplayStepKind::Tool, "glob", "structure"),
        ];

        let statuses: Vec<_> = compare_steps(&expected, &actual)
            .into_iter()
            .map(|comparison| comparison.status)
            .collect();

        assert_eq!(
            statuses,
            vec![
                ReplayComparisonStatus::Equal,
                ReplayComparisonStatus::Changed,
                ReplayComparisonStatus::Unsupported,
                ReplayComparisonStatus::Missing,
            ]
        );
    }

    #[test]
    fn model_binding_includes_credential_free_source_configuration() {
        let mut first = Config::default();
        first.model.adapter = "scripted".into();
        first.model.script_json = Some(r#"[{"content":"first"}]"#.into());
        let mut second = first.clone();
        second.model.script_json = Some(r#"[{"content":"second"}]"#.into());

        let first = model_digest(&first).unwrap();
        let second = model_digest(&second).unwrap();
        assert_ne!(first, second);

        let mut first = Config::default();
        first.model.adapter = "http".into();
        first.model.base_url = Some("https://one.example/v1".into());
        let mut second = first.clone();
        second.model.base_url = Some("https://two.example/v1".into());
        assert_ne!(
            model_digest(&first).unwrap(),
            model_digest(&second).unwrap()
        );
    }

    #[test]
    fn workspace_digest_is_path_independent_and_content_sensitive() {
        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(first.path().join("nested")).unwrap();
        std::fs::create_dir_all(second.path().join("nested")).unwrap();
        std::fs::write(first.path().join("nested/input.txt"), "same").unwrap();
        std::fs::write(second.path().join("nested/input.txt"), "same").unwrap();

        assert_eq!(
            workspace_digest(first.path()).unwrap(),
            workspace_digest(second.path()).unwrap()
        );
        std::fs::write(second.path().join("nested/input.txt"), "changed").unwrap();
        assert_ne!(
            workspace_digest(first.path()).unwrap(),
            workspace_digest(second.path()).unwrap()
        );
    }

    #[test]
    fn replay_runtime_disables_context_compaction() {
        let mut config = Config::default();
        config.run.compact_after_messages = Some(4);
        assert_eq!(replay_config(&config).run.compact_after_messages, None);
    }
}
