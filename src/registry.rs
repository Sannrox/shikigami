//! Durable local run registry and control markers.
//!
//! The registry is host-local operational state. Governed operation truth still
//! belongs to the configured governance plane.

use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::events::HarnessEvent;
use crate::model::{CostEstimate, TokenUsage};
use crate::run::{RunError, RunResult};

pub const RUN_REGISTRY_SCHEMA_VERSION: u32 = 1;
pub const RUN_RECORD_FILENAME: &str = "run.json";
pub const RUN_EVENTS_FILENAME: &str = "events.jsonl";
pub const RUN_CANCEL_FILENAME: &str = "cancel";
pub const RUN_OWNER_FILENAME: &str = "owner";
const ACTIVE_HEARTBEAT_TTL_MS: u64 = 120_000;

#[derive(Debug, Error)]
pub enum RegistryError {
    #[error("run registry I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("run registry parse: {0}")]
    Parse(#[from] serde_json::Error),
    #[error("invalid run id")]
    InvalidRunId,
    #[error("run not found: {0}")]
    Missing(String),
    #[error("run {0} is still active; wait for it to terminate before cleanup or resume")]
    Active(String),
    #[error("run {0} is not active")]
    NotActive(String),
    #[error("run registry lock poisoned")]
    Lock,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RunRecord {
    pub schema_version: u32,
    pub run_id: String,
    /// `starting` | `running` | terminal status.
    pub status: String,
    pub success: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub termination: Option<String>,
    pub summary: String,
    pub task_digest: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logical_operation_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artifact_dir: Option<String>,
    pub started_at_ms: u64,
    pub last_started_at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at_ms: Option<u64>,
    pub process_id: u32,
    /// Last local owner heartbeat. This is the resume/cleanup ownership
    /// lease; `process_id` remains informational because PIDs can be reused.
    #[serde(default)]
    pub last_heartbeat_at_ms: u64,
    pub turns: u32,
    pub usage: TokenUsage,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cost: Option<CostEstimate>,
    pub cancel_requested: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RunEventRecord {
    pub schema_version: u32,
    pub timestamp_ms: u64,
    pub event: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RunRegistry {
    runs_root: PathBuf,
    cancel_root: PathBuf,
    run_locks: Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,
}

impl RunRegistry {
    pub fn new(state_root: impl AsRef<Path>) -> Result<Self, RegistryError> {
        let runs_root = state_root.as_ref().join("runs");
        let cancel_root = state_root.as_ref().join("run-controls");
        fs::create_dir_all(&runs_root)?;
        fs::create_dir_all(&cancel_root)?;
        Ok(Self {
            runs_root,
            cancel_root,
            run_locks: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub fn runs_root(&self) -> &Path {
        &self.runs_root
    }

    pub fn run_dir(&self, run_id: &str) -> Result<PathBuf, RegistryError> {
        validate_run_id(run_id)?;
        Ok(self.runs_root.join(run_id))
    }

    pub fn start(
        &self,
        run_id: &str,
        task: &str,
        logical_operation_id: Option<&str>,
        workspace: Option<&Path>,
    ) -> Result<(), RegistryError> {
        let run_lock = self.lock_for(run_id)?;
        let _guard = run_lock.lock().map_err(|_| RegistryError::Lock)?;
        let dir = self.run_dir(run_id)?;
        fs::create_dir_all(&dir)?;
        let now = now_ms();
        let existing = match self.load_unlocked(run_id) {
            Ok(record) => Some(record),
            Err(RegistryError::Missing(_)) => None,
            Err(error) => return Err(error),
        };
        if existing.as_ref().is_some_and(|record| {
            matches!(record.status.as_str(), "starting" | "running")
                && now.saturating_sub(record.last_heartbeat_at_ms) < ACTIVE_HEARTBEAT_TTL_MS
        }) {
            return Err(RegistryError::Active(run_id.into()));
        }
        self.acquire_owner_unlocked(run_id, now)?;
        let record = match existing {
            Some(mut record) => {
                self.clear_cancel_unlocked(run_id)?;
                record.status = "running".into();
                record.success = None;
                record.termination = None;
                record.summary.clear();
                record.task_digest = digest(task);
                record.logical_operation_id = logical_operation_id
                    .filter(|value| !value.is_empty())
                    .map(str::to_string)
                    .or(record.logical_operation_id);
                record.workspace = workspace.map(|path| path.display().to_string());
                record.artifact_dir = None;
                record.last_started_at_ms = now;
                record.finished_at_ms = None;
                record.process_id = std::process::id();
                record.last_heartbeat_at_ms = now;
                record.turns = 0;
                record.usage = TokenUsage::default();
                record.cost = None;
                record.cancel_requested = false;
                record
            }
            None => {
                self.clear_cancel_unlocked(run_id)?;
                RunRecord {
                    schema_version: RUN_REGISTRY_SCHEMA_VERSION,
                    run_id: run_id.into(),
                    status: "running".into(),
                    success: None,
                    termination: None,
                    summary: String::new(),
                    task_digest: digest(task),
                    logical_operation_id: logical_operation_id
                        .filter(|value| !value.is_empty())
                        .map(str::to_string),
                    workspace: workspace.map(|path| path.display().to_string()),
                    artifact_dir: None,
                    started_at_ms: now,
                    last_started_at_ms: now,
                    finished_at_ms: None,
                    process_id: std::process::id(),
                    last_heartbeat_at_ms: now,
                    turns: 0,
                    usage: TokenUsage::default(),
                    cost: None,
                    cancel_requested: false,
                }
            }
        };
        if let Err(error) = self.write(&record) {
            let _ = self.remove_owner_unlocked(run_id);
            return Err(error);
        }
        Ok(())
    }

    pub fn set_workspace(&self, run_id: &str, workspace: &Path) -> Result<(), RegistryError> {
        let run_lock = self.lock_for(run_id)?;
        let _guard = run_lock.lock().map_err(|_| RegistryError::Lock)?;
        let mut record = self.load_unlocked(run_id)?;
        record.workspace = Some(workspace.display().to_string());
        self.write(&record)
    }

    pub fn update_running(
        &self,
        run_id: &str,
        task: &str,
        logical_operation_id: Option<&str>,
        workspace: &Path,
        turns: u32,
    ) -> Result<(), RegistryError> {
        let run_lock = self.lock_for(run_id)?;
        let _guard = run_lock.lock().map_err(|_| RegistryError::Lock)?;
        let mut record = self.load_unlocked(run_id)?;
        record.status = "running".into();
        record.task_digest = digest(task);
        if let Some(operation_id) = logical_operation_id.filter(|value| !value.is_empty()) {
            record.logical_operation_id = Some(operation_id.into());
        }
        record.workspace = Some(workspace.display().to_string());
        record.turns = turns;
        record.last_heartbeat_at_ms = now_ms();
        self.write(&record)?;
        self.touch_owner_unlocked(run_id)
    }

    /// Refresh the local ownership lease for an active run.
    pub fn heartbeat(&self, run_id: &str) -> Result<(), RegistryError> {
        let run_lock = self.lock_for(run_id)?;
        let _guard = run_lock.lock().map_err(|_| RegistryError::Lock)?;
        let mut record = self.load_unlocked(run_id)?;
        if !matches!(record.status.as_str(), "starting" | "running") {
            return Err(RegistryError::NotActive(run_id.into()));
        }
        record.last_heartbeat_at_ms = now_ms();
        self.write(&record)?;
        self.touch_owner_unlocked(run_id)
    }

    pub fn set_artifact_dir(&self, run_id: &str, artifact_dir: &Path) -> Result<(), RegistryError> {
        let run_lock = self.lock_for(run_id)?;
        let _guard = run_lock.lock().map_err(|_| RegistryError::Lock)?;
        let mut record = self.load_unlocked(run_id)?;
        record.artifact_dir = Some(artifact_dir.display().to_string());
        self.write(&record)
    }

    pub fn finish_result(&self, result: &RunResult) -> Result<(), RegistryError> {
        let run_lock = self.lock_for(&result.run_id)?;
        let _guard = run_lock.lock().map_err(|_| RegistryError::Lock)?;
        let mut record = self.load_unlocked(&result.run_id)?;
        record.status = result.termination.as_str().into();
        record.success = Some(result.success);
        record.termination = Some(result.termination.as_str().into());
        record.summary = result.summary.clone();
        record.workspace = Some(result.workspace.display().to_string());
        record.artifact_dir = result
            .artifact_dir
            .as_ref()
            .map(|path| path.display().to_string());
        record.finished_at_ms = Some(now_ms());
        record.turns = result.turns;
        record.usage = result.usage;
        record.cost = result.cost.clone();
        record.cancel_requested = false;
        record.last_heartbeat_at_ms = now_ms();
        self.clear_cancel_unlocked(&result.run_id)?;
        self.write(&record)?;
        self.remove_owner_unlocked(&result.run_id)
    }

    pub fn finish_error(&self, run_id: &str, error: &RunError) -> Result<(), RegistryError> {
        let run_lock = self.lock_for(run_id)?;
        let _guard = run_lock.lock().map_err(|_| RegistryError::Lock)?;
        let mut record = self.load_unlocked(run_id)?;
        record.status = error.termination().as_str().into();
        record.success = Some(false);
        record.termination = Some(error.termination().as_str().into());
        record.summary = error.to_string();
        record.finished_at_ms = Some(now_ms());
        record.cancel_requested = false;
        record.last_heartbeat_at_ms = now_ms();
        self.clear_cancel_unlocked(run_id)?;
        self.write(&record)?;
        self.remove_owner_unlocked(run_id)
    }

    pub fn load(&self, run_id: &str) -> Result<RunRecord, RegistryError> {
        let run_lock = self.lock_for(run_id)?;
        let _guard = run_lock.lock().map_err(|_| RegistryError::Lock)?;
        self.load_unlocked(run_id)
    }

    fn load_unlocked(&self, run_id: &str) -> Result<RunRecord, RegistryError> {
        let path = self.run_dir(run_id)?.join(RUN_RECORD_FILENAME);
        if !path.is_file() {
            return Err(RegistryError::Missing(run_id.into()));
        }
        Ok(serde_json::from_slice(&fs::read(path)?)?)
    }

    pub fn list(&self) -> Result<Vec<RunRecord>, RegistryError> {
        let mut records: Vec<RunRecord> = Vec::new();
        for entry in fs::read_dir(&self.runs_root)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let path = entry.path().join(RUN_RECORD_FILENAME);
            if path.is_file() {
                records.push(serde_json::from_slice(&fs::read(path)?)?);
            }
        }
        records.sort_by_key(|record| std::cmp::Reverse(record.last_started_at_ms));
        Ok(records)
    }

    pub fn cancel(&self, run_id: &str) -> Result<(), RegistryError> {
        let run_lock = self.lock_for(run_id)?;
        let _guard = run_lock.lock().map_err(|_| RegistryError::Lock)?;
        let mut record = self.load_unlocked(run_id)?;
        if !matches!(record.status.as_str(), "starting" | "running") {
            return Err(RegistryError::NotActive(run_id.into()));
        }
        fs::write(self.cancel_root.join(run_id), b"cancel\n")?;
        record.cancel_requested = true;
        self.write(&record)
    }

    pub fn cancel_requested(&self, run_id: &str) -> bool {
        self.cancel_root.join(run_id).is_file()
            || self
                .run_dir(run_id)
                .map(|dir| dir.join(RUN_CANCEL_FILENAME).is_file())
                .unwrap_or(false)
    }

    pub fn clear_cancel(&self, run_id: &str) -> Result<(), RegistryError> {
        let run_lock = self.lock_for(run_id)?;
        let _guard = run_lock.lock().map_err(|_| RegistryError::Lock)?;
        self.clear_cancel_unlocked(run_id)
    }

    fn clear_cancel_unlocked(&self, run_id: &str) -> Result<(), RegistryError> {
        remove_cancel_marker(&self.cancel_root.join(run_id))?;
        remove_cancel_marker(&self.run_dir(run_id)?.join(RUN_CANCEL_FILENAME))
    }

    pub fn event_log(&self, run_id: &str) -> Result<String, RegistryError> {
        // An absent journal is valid for a run that has not emitted events;
        // an absent record is still a 404/registry miss.
        let _ = self.load(run_id)?;
        let path = self.run_dir(run_id)?.join(RUN_EVENTS_FILENAME);
        match fs::read_to_string(path) {
            Ok(text) => Ok(text),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(String::new()),
            Err(error) => Err(error.into()),
        }
    }

    pub fn append_event(&self, run_id: &str, event: &HarnessEvent) {
        let Ok(dir) = self.run_dir(run_id) else {
            return;
        };
        if fs::create_dir_all(&dir).is_err() {
            return;
        }
        let record = RunEventRecord {
            schema_version: RUN_REGISTRY_SCHEMA_VERSION,
            timestamp_ms: now_ms(),
            event: event_name(event).into(),
            detail: event_detail(event),
        };
        let Ok(mut line) = serde_json::to_string(&record) else {
            return;
        };
        line.push('\n');
        let Ok(mut file) = OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join(RUN_EVENTS_FILENAME))
        else {
            return;
        };
        let _ = file.write_all(line.as_bytes());
    }

    pub fn clean(&self, run_id: &str, force: bool) -> Result<(), RegistryError> {
        let run_lock = self.lock_for(run_id)?;
        let _guard = run_lock.lock().map_err(|_| RegistryError::Lock)?;
        let record = self.load_unlocked(run_id)?;
        let active = matches!(record.status.as_str(), "starting" | "running");
        if active && self.owner_lease_is_live(run_id, &record) {
            if !force {
                return Err(RegistryError::Active(run_id.into()));
            }
            fs::write(self.cancel_root.join(run_id), b"cancel\n")?;
            let mut record = record;
            record.cancel_requested = true;
            self.write(&record)?;
            return Err(RegistryError::Active(run_id.into()));
        }
        // A stale owner lease means no live process can safely be using the
        // run anymore, so its crashed state is eligible for cleanup.
        self.clear_cancel_unlocked(run_id)?;
        fs::remove_dir_all(self.run_dir(run_id)?)?;
        Ok(())
    }

    fn write(&self, record: &RunRecord) -> Result<(), RegistryError> {
        let dir = self.run_dir(&record.run_id)?;
        fs::create_dir_all(&dir)?;
        let path = dir.join(RUN_RECORD_FILENAME);
        let temp = dir.join(format!(
            ".{RUN_RECORD_FILENAME}.{}.tmp",
            uuid::Uuid::new_v4()
        ));
        let raw = serde_json::to_vec_pretty(record)?;
        fs::write(&temp, raw)?;
        crate::atomic::replace_file(&temp, path)?;
        Ok(())
    }

    fn owner_path(&self, run_id: &str) -> Result<PathBuf, RegistryError> {
        Ok(self.run_dir(run_id)?.join(RUN_OWNER_FILENAME))
    }

    fn acquire_owner_unlocked(&self, run_id: &str, now: u64) -> Result<(), RegistryError> {
        let path = self.owner_path(run_id)?;
        for attempt in 0..2 {
            match OpenOptions::new().write(true).create_new(true).open(&path) {
                Ok(mut file) => {
                    file.write_all(
                        format!("pid={} heartbeat={now}\n", std::process::id()).as_bytes(),
                    )?;
                    return Ok(());
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    let stale = fs::metadata(&path)
                        .and_then(|metadata| metadata.modified())
                        .ok()
                        .and_then(|modified| modified.elapsed().ok())
                        .is_some_and(|age| age.as_millis() as u64 >= ACTIVE_HEARTBEAT_TTL_MS);
                    if attempt == 0 && stale {
                        let _ = fs::remove_file(&path);
                        continue;
                    }
                    return Err(RegistryError::Active(run_id.into()));
                }
                Err(error) => return Err(error.into()),
            }
        }
        Err(RegistryError::Active(run_id.into()))
    }

    fn touch_owner_unlocked(&self, run_id: &str) -> Result<(), RegistryError> {
        let path = self.owner_path(run_id)?;
        let mut file = OpenOptions::new().write(true).truncate(true).open(path)?;
        file.write_all(format!("pid={} heartbeat={}\n", std::process::id(), now_ms()).as_bytes())?;
        Ok(())
    }

    fn remove_owner_unlocked(&self, run_id: &str) -> Result<(), RegistryError> {
        remove_cancel_marker(&self.owner_path(run_id)?)
    }

    fn owner_lease_is_live(&self, run_id: &str, record: &RunRecord) -> bool {
        let now = now_ms();
        let record_recent =
            now.saturating_sub(record.last_heartbeat_at_ms) < ACTIVE_HEARTBEAT_TTL_MS;
        let owner_recent = self
            .owner_path(run_id)
            .ok()
            .and_then(|path| fs::metadata(path).ok())
            .and_then(|metadata| metadata.modified().ok())
            .and_then(|modified| modified.elapsed().ok())
            .is_some_and(|age| (age.as_millis() as u64) < ACTIVE_HEARTBEAT_TTL_MS);
        record_recent || owner_recent
    }

    fn lock_for(&self, run_id: &str) -> Result<Arc<Mutex<()>>, RegistryError> {
        validate_run_id(run_id)?;
        let mut locks = self.run_locks.lock().map_err(|_| RegistryError::Lock)?;
        Ok(Arc::clone(
            locks
                .entry(run_id.to_owned())
                .or_insert_with(|| Arc::new(Mutex::new(()))),
        ))
    }
}

fn validate_run_id(run_id: &str) -> Result<(), RegistryError> {
    if crate::checkpoint::is_safe_run_id(run_id) {
        Ok(())
    } else {
        Err(RegistryError::InvalidRunId)
    }
}

fn remove_cancel_marker(dir: &Path) -> Result<(), RegistryError> {
    match fs::remove_file(dir) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn digest(text: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(text.as_bytes());
    format!("sha256:{}", hex_digest(&hasher.finalize()))
}

fn hex_digest(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn event_name(event: &HarnessEvent) -> &'static str {
    match event {
        HarnessEvent::Status { .. } => "status",
        HarnessEvent::ToolStart { .. } => "tool_start",
        HarnessEvent::ToolEnd { .. } => "tool_end",
        HarnessEvent::ModelTurn { .. } => "model_turn",
        HarnessEvent::Message { .. } => "message",
        HarnessEvent::RunFinished { .. } => "run_finished",
        HarnessEvent::Prompt { .. } => "prompt",
        HarnessEvent::ContextCompacted { .. } => "context_compacted",
        HarnessEvent::TodosUpdated { .. } => "todos_updated",
    }
}

fn event_detail(event: &HarnessEvent) -> Option<String> {
    let detail = match event {
        HarnessEvent::Status { status } => status.clone(),
        HarnessEvent::ToolStart { name, .. } => name.clone(),
        HarnessEvent::ToolEnd { name, ok, .. } => format!("{name} ok={ok}"),
        HarnessEvent::ModelTurn { turn, .. } => format!("turn={turn}"),
        HarnessEvent::Message { level, .. } => format!("[{level}]"),
        HarnessEvent::RunFinished {
            run_id, success, ..
        } => format!("run={run_id} success={success}"),
        HarnessEvent::Prompt { prompt_id } => prompt_id.clone(),
        HarnessEvent::ContextCompacted { before, after } => {
            format!("before={before} after={after}")
        }
        HarnessEvent::TodosUpdated { item_count, .. } => format!("items={item_count}"),
    };
    Some(detail)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn records_runs_and_control_markers_without_task_text() {
        let dir = tempdir().unwrap();
        let registry = RunRegistry::new(dir.path()).unwrap();
        registry
            .start("run-1", "secret task", Some("op-1"), None)
            .unwrap();
        let record = registry.load("run-1").unwrap();
        assert_eq!(record.status, "running");
        assert!(
            !serde_json::to_string(&record)
                .unwrap()
                .contains("secret task")
        );
        registry.cancel("run-1").unwrap();
        assert!(registry.cancel_requested("run-1"));
        registry.clear_cancel("run-1").unwrap();
        assert!(!registry.cancel_requested("run-1"));
    }

    #[test]
    fn recent_heartbeat_prevents_concurrent_start_but_stale_lease_can_resume() {
        let dir = tempdir().unwrap();
        let registry = RunRegistry::new(dir.path()).unwrap();
        registry.start("run-1", "task", None, None).unwrap();
        assert!(matches!(
            registry.start("run-1", "task", None, None),
            Err(RegistryError::Active(_))
        ));
        let mut record = registry.load("run-1").unwrap();
        record.last_heartbeat_at_ms = 0;
        registry.write(&record).unwrap();
        std::fs::remove_file(registry.run_dir("run-1").unwrap().join(RUN_OWNER_FILENAME)).unwrap();
        registry.start("run-1", "resumed task", None, None).unwrap();
        assert_eq!(
            registry.load("run-1").unwrap().task_digest,
            digest("resumed task")
        );
    }

    #[test]
    fn independent_registry_instances_respect_filesystem_owner_lease() {
        let dir = tempdir().unwrap();
        let first = RunRegistry::new(dir.path()).unwrap();
        let second = RunRegistry::new(dir.path()).unwrap();
        first.start("run-1", "task", None, None).unwrap();
        let mut record = first.load("run-1").unwrap();
        record.last_heartbeat_at_ms = 0;
        first.write(&record).unwrap();
        assert!(matches!(
            second.start("run-1", "concurrent", None, None),
            Err(RegistryError::Active(_))
        ));
    }

    #[test]
    fn stale_owner_lease_can_be_cleaned() {
        let dir = tempdir().unwrap();
        let registry = RunRegistry::new(dir.path()).unwrap();
        registry.start("run-1", "task", None, None).unwrap();
        let mut record = registry.load("run-1").unwrap();
        record.last_heartbeat_at_ms = 0;
        registry.write(&record).unwrap();
        std::fs::remove_file(registry.run_dir("run-1").unwrap().join(RUN_OWNER_FILENAME)).unwrap();
        registry.clean("run-1", false).unwrap();
        assert!(!registry.run_dir("run-1").unwrap().exists());
    }

    #[test]
    fn missing_run_has_no_event_log() {
        let dir = tempdir().unwrap();
        let registry = RunRegistry::new(dir.path()).unwrap();
        assert!(matches!(
            registry.event_log("missing"),
            Err(RegistryError::Missing(_))
        ));
    }

    #[test]
    fn event_log_omits_tool_arguments() {
        let dir = tempdir().unwrap();
        let registry = RunRegistry::new(dir.path()).unwrap();
        registry.start("run-1", "task", None, None).unwrap();
        registry.append_event(
            "run-1",
            &HarnessEvent::ToolStart {
                name: "write_file".into(),
                args_json: "secret-content".into(),
            },
        );
        let log = registry.event_log("run-1").unwrap();
        assert!(log.contains("write_file"));
        assert!(!log.contains("secret-content"));
        registry.append_event(
            "run-1",
            &HarnessEvent::RunFinished {
                run_id: "run-1".into(),
                success: true,
                summary: "private summary that must not persist".into(),
            },
        );
        let log = registry.event_log("run-1").unwrap();
        assert!(!log.contains("private summary"));
    }
}
