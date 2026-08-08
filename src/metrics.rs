//! Optional process metrics (JSON snapshot). No Prometheus dependency by default.

use std::fs::{self, OpenOptions};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum MetricsError {
    #[error("metrics I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("metrics parse: {0}")]
    Parse(#[from] serde_json::Error),
}

/// Cumulative counters for fleet operators.
#[derive(Debug)]
pub struct Metrics {
    pub runs_total: AtomicU64,
    pub runs_success: AtomicU64,
    pub runs_failed: AtomicU64,
    pub runs_parked: AtomicU64,
    pub turns_total: AtomicU64,
    pub plane_errors: AtomicU64,
    pub tokens_input_total: AtomicU64,
    pub tokens_output_total: AtomicU64,
    persist_path: Option<PathBuf>,
    persist_lock: std::sync::Mutex<()>,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            runs_total: AtomicU64::new(0),
            runs_success: AtomicU64::new(0),
            runs_failed: AtomicU64::new(0),
            runs_parked: AtomicU64::new(0),
            turns_total: AtomicU64::new(0),
            plane_errors: AtomicU64::new(0),
            tokens_input_total: AtomicU64::new(0),
            tokens_output_total: AtomicU64::new(0),
            persist_path: None,
            persist_lock: std::sync::Mutex::new(()),
        })
    }

    pub fn new_at(state_root: impl AsRef<Path>) -> Result<Arc<Self>, MetricsError> {
        let directory = state_root.as_ref().join("metrics");
        fs::create_dir_all(&directory)?;
        write_process_identity(&directory, &process_identity())?;
        let mut metrics = Self::new_unpersisted();
        metrics.persist_path = Some(directory.join(format!(
            "process-{}-{}.json",
            std::process::id(),
            Uuid::new_v4()
        )));
        Ok(Arc::new(metrics))
    }

    fn new_unpersisted() -> Self {
        Self {
            runs_total: AtomicU64::new(0),
            runs_success: AtomicU64::new(0),
            runs_failed: AtomicU64::new(0),
            runs_parked: AtomicU64::new(0),
            turns_total: AtomicU64::new(0),
            plane_errors: AtomicU64::new(0),
            tokens_input_total: AtomicU64::new(0),
            tokens_output_total: AtomicU64::new(0),
            persist_path: None,
            persist_lock: std::sync::Mutex::new(()),
        }
    }

    pub fn aggregate(state_root: impl AsRef<Path>) -> Result<MetricsSnapshot, MetricsError> {
        let directory = state_root.as_ref().join("metrics");
        if !directory.is_dir() {
            return Ok(MetricsSnapshot::default());
        }
        let lock = acquire_aggregate_lock(&directory)?;
        let aggregate_path = directory.join("aggregate.json");
        let mut retired_total = MetricsSnapshot::default();
        if aggregate_path.is_file() {
            let persisted: PersistedMetrics = serde_json::from_slice(&fs::read(&aggregate_path)?)?;
            retired_total.add_assign(&persisted.snapshot);
        }
        let mut total = retired_total.clone();
        let mut retired = false;
        let mut retired_paths = Vec::new();
        for entry in fs::read_dir(&directory)? {
            let entry = entry?;
            let path = entry.path();
            if path.file_name().and_then(|name| name.to_str()) == Some("aggregate.json")
                || path.extension().and_then(|ext| ext.to_str()) != Some("json")
            {
                continue;
            }
            let raw = fs::read(&path)?;
            let persisted: PersistedMetrics = serde_json::from_slice(&raw)?;
            if process_is_alive(&directory, &persisted) {
                total.add_assign(&persisted.snapshot);
            } else if lock.is_some() {
                retired_total.add_assign(&persisted.snapshot);
                total.add_assign(&persisted.snapshot);
                retired = true;
                retired_paths.push(path);
            } else {
                total.add_assign(&persisted.snapshot);
            }
        }
        if retired {
            write_persisted(
                &aggregate_path,
                &PersistedMetrics::from_snapshot(&retired_total),
            )?;
            for path in retired_paths {
                let _ = fs::remove_file(path);
            }
        }
        Ok(total)
    }

    pub fn record_run(
        &self,
        success: bool,
        parked: bool,
        turns: u32,
        input_tokens: u64,
        output_tokens: u64,
    ) {
        self.runs_total.fetch_add(1, Ordering::Relaxed);
        self.turns_total.fetch_add(turns as u64, Ordering::Relaxed);
        self.tokens_input_total
            .fetch_add(input_tokens, Ordering::Relaxed);
        self.tokens_output_total
            .fetch_add(output_tokens, Ordering::Relaxed);
        if parked {
            self.runs_parked.fetch_add(1, Ordering::Relaxed);
        } else if success {
            self.runs_success.fetch_add(1, Ordering::Relaxed);
        } else {
            self.runs_failed.fetch_add(1, Ordering::Relaxed);
        }
        self.persist();
    }

    pub fn record_plane_error(&self) {
        self.plane_errors.fetch_add(1, Ordering::Relaxed);
        self.persist();
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            runs_total: self.runs_total.load(Ordering::Relaxed),
            runs_success: self.runs_success.load(Ordering::Relaxed),
            runs_failed: self.runs_failed.load(Ordering::Relaxed),
            runs_parked: self.runs_parked.load(Ordering::Relaxed),
            turns_total: self.turns_total.load(Ordering::Relaxed),
            plane_errors: self.plane_errors.load(Ordering::Relaxed),
            tokens_input_total: self.tokens_input_total.load(Ordering::Relaxed),
            tokens_output_total: self.tokens_output_total.load(Ordering::Relaxed),
        }
    }
}

/// Serializable metrics export (JSON).
#[derive(Debug, Clone, Default, Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub runs_total: u64,
    pub runs_success: u64,
    pub runs_failed: u64,
    pub runs_parked: u64,
    pub turns_total: u64,
    pub plane_errors: u64,
    pub tokens_input_total: u64,
    pub tokens_output_total: u64,
}

impl MetricsSnapshot {
    fn add_assign(&mut self, other: &Self) {
        self.runs_total = self.runs_total.saturating_add(other.runs_total);
        self.runs_success = self.runs_success.saturating_add(other.runs_success);
        self.runs_failed = self.runs_failed.saturating_add(other.runs_failed);
        self.runs_parked = self.runs_parked.saturating_add(other.runs_parked);
        self.turns_total = self.turns_total.saturating_add(other.turns_total);
        self.plane_errors = self.plane_errors.saturating_add(other.plane_errors);
        self.tokens_input_total = self
            .tokens_input_total
            .saturating_add(other.tokens_input_total);
        self.tokens_output_total = self
            .tokens_output_total
            .saturating_add(other.tokens_output_total);
    }
}

#[derive(Debug, Serialize, serde::Deserialize)]
struct PersistedMetrics {
    schema_version: u32,
    process_id: u32,
    #[serde(default)]
    process_identity: String,
    updated_at_ms: u64,
    snapshot: MetricsSnapshot,
}

impl PersistedMetrics {
    fn from_snapshot(snapshot: &MetricsSnapshot) -> Self {
        Self {
            schema_version: 1,
            process_id: 0,
            process_identity: String::new(),
            updated_at_ms: now_ms(),
            snapshot: snapshot.clone(),
        }
    }
}

impl Metrics {
    fn persist(&self) {
        let Some(path) = &self.persist_path else {
            return;
        };
        let Ok(_guard) = self.persist_lock.lock() else {
            return;
        };
        let value = PersistedMetrics {
            schema_version: 1,
            process_id: std::process::id(),
            process_identity: process_identity(),
            updated_at_ms: now_ms(),
            snapshot: self.snapshot(),
        };
        let Ok(raw) = serde_json::to_vec_pretty(&value) else {
            return;
        };
        let temp = path.with_extension(format!("json.{}.tmp", Uuid::new_v4()));
        if fs::write(&temp, raw).is_ok() {
            let _ = crate::atomic::replace_file(&temp, path);
        }
    }
}

impl Drop for Metrics {
    fn drop(&mut self) {
        let Some(path) = &self.persist_path else {
            return;
        };
        let Ok(_guard) = self.persist_lock.lock() else {
            return;
        };
        let Some(directory) = path.parent() else {
            return;
        };
        let Ok(Some(_lock)) = acquire_aggregate_lock(directory) else {
            return;
        };
        let aggregate_path = directory.join("aggregate.json");
        let mut total = fs::read(&aggregate_path)
            .ok()
            .and_then(|raw| serde_json::from_slice::<PersistedMetrics>(&raw).ok())
            .map(|persisted| persisted.snapshot)
            .unwrap_or_default();
        total.add_assign(&self.snapshot());
        if write_persisted(&aggregate_path, &PersistedMetrics::from_snapshot(&total)).is_ok() {
            let _ = fs::remove_file(path);
        }
    }
}

fn write_persisted(path: &Path, value: &PersistedMetrics) -> Result<(), MetricsError> {
    let raw = serde_json::to_vec_pretty(value)?;
    let temp = path.with_extension(format!("json.{}.tmp", Uuid::new_v4()));
    fs::write(&temp, raw)?;
    crate::atomic::replace_file(&temp, path)?;
    Ok(())
}

struct AggregateLock {
    path: PathBuf,
}

impl Drop for AggregateLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn acquire_aggregate_lock(directory: &Path) -> Result<Option<AggregateLock>, MetricsError> {
    let path = directory.join("aggregate.lock");
    for _ in 0..20 {
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(_) => return Ok(Some(AggregateLock { path })),
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                let stale = fs::metadata(&path)
                    .and_then(|metadata| metadata.modified())
                    .ok()
                    .and_then(|modified| modified.elapsed().ok())
                    .is_some_and(|age| age > Duration::from_secs(60));
                if stale {
                    let _ = fs::remove_file(&path);
                } else {
                    thread::sleep(Duration::from_millis(5));
                }
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(None)
}

fn process_is_alive(directory: &Path, persisted: &PersistedMetrics) -> bool {
    if persisted.process_id == 0 {
        return false;
    }
    if !persisted.process_identity.is_empty()
        && let Ok(current) =
            fs::read_to_string(process_identity_path(directory, persisted.process_id))
        && current.trim() != persisted.process_identity
    {
        return false;
    }
    #[cfg(unix)]
    {
        let result = unsafe { libc::kill(persisted.process_id as libc::pid_t, 0) };
        result == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
    }
    #[cfg(not(unix))]
    {
        let _ = persisted;
        // Without a portable process probe, keep the snapshot live. Retiring
        // an unproven process would double-count the cumulative file on its
        // next update, so operators can remove stale snapshots explicitly.
        true
    }
}

fn process_identity() -> String {
    static IDENTITY: OnceLock<String> = OnceLock::new();
    IDENTITY.get_or_init(|| Uuid::new_v4().to_string()).clone()
}

fn process_identity_path(directory: &Path, process_id: u32) -> PathBuf {
    directory.join(format!("process-{process_id}.identity"))
}

fn write_process_identity(directory: &Path, identity: &str) -> Result<(), MetricsError> {
    let path = process_identity_path(directory, std::process::id());
    let temp = path.with_extension(format!("identity.{}.tmp", Uuid::new_v4()));
    fs::write(&temp, identity.as_bytes())?;
    crate::atomic::replace_file(&temp, &path)?;
    Ok(())
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

impl MetricsSnapshot {
    /// Prometheus text exposition (optional operator export without extra crates).
    pub fn to_prometheus(&self) -> String {
        format!(
            concat!(
                "# HELP shikigami_runs_total Total runs attempted\n",
                "# TYPE shikigami_runs_total counter\n",
                "shikigami_runs_total {}\n",
                "# HELP shikigami_runs_success_total Successful runs\n",
                "# TYPE shikigami_runs_success_total counter\n",
                "shikigami_runs_success_total {}\n",
                "# HELP shikigami_runs_failed_total Failed runs\n",
                "# TYPE shikigami_runs_failed_total counter\n",
                "shikigami_runs_failed_total {}\n",
                "# HELP shikigami_runs_parked_total Parked runs\n",
                "# TYPE shikigami_runs_parked_total counter\n",
                "shikigami_runs_parked_total {}\n",
                "# HELP shikigami_turns_total Model turns completed\n",
                "# TYPE shikigami_turns_total counter\n",
                "shikigami_turns_total {}\n",
                "# HELP shikigami_plane_errors_total Plane/governance errors observed\n",
                "# TYPE shikigami_plane_errors_total counter\n",
                "shikigami_plane_errors_total {}\n",
                "# HELP shikigami_tokens_input_total Input tokens reported by models\n",
                "# TYPE shikigami_tokens_input_total counter\n",
                "shikigami_tokens_input_total {}\n",
                "# HELP shikigami_tokens_output_total Output tokens reported by models\n",
                "# TYPE shikigami_tokens_output_total counter\n",
                "shikigami_tokens_output_total {}\n",
            ),
            self.runs_total,
            self.runs_success,
            self.runs_failed,
            self.runs_parked,
            self.turns_total,
            self.plane_errors,
            self.tokens_input_total,
            self.tokens_output_total,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn snapshot_and_prometheus_export() {
        let m = Metrics::new();
        m.record_run(true, false, 3, 10, 5);
        m.record_run(false, true, 1, 2, 1);
        m.record_plane_error();
        let s = m.snapshot();
        assert_eq!(s.runs_total, 2);
        assert_eq!(s.runs_success, 1);
        assert_eq!(s.runs_parked, 1);
        assert_eq!(s.turns_total, 4);
        assert_eq!(s.plane_errors, 1);
        assert_eq!(s.tokens_input_total, 12);
        assert_eq!(s.tokens_output_total, 6);
        let text = s.to_prometheus();
        assert!(text.contains("shikigami_runs_total 2"));
        assert!(text.contains("shikigami_plane_errors_total 1"));
        assert!(text.contains("shikigami_tokens_input_total 12"));
        let json = serde_json::to_string(&s).unwrap();
        assert!(json.contains("\"runs_total\":2"));
    }

    #[test]
    fn aggregate_reads_unique_durable_instance_snapshot() {
        let dir = tempdir().unwrap();
        let metrics = Metrics::new_at(dir.path()).unwrap();
        metrics.record_run(true, false, 2, 3, 4);
        let aggregate = Metrics::aggregate(dir.path()).unwrap();
        assert_eq!(aggregate.runs_total, 1);
        assert_eq!(aggregate.turns_total, 2);
        assert_eq!(aggregate.tokens_input_total, 3);
        assert_eq!(aggregate.tokens_output_total, 4);
    }

    #[test]
    fn aggregate_retires_snapshot_when_pid_identity_changes() {
        let dir = tempdir().unwrap();
        let metrics_dir = dir.path().join("metrics");
        fs::create_dir_all(&metrics_dir).unwrap();
        fs::write(
            process_identity_path(&metrics_dir, std::process::id()),
            "new-process",
        )
        .unwrap();
        let stale = PersistedMetrics {
            schema_version: 1,
            process_id: std::process::id(),
            process_identity: "old-process".into(),
            updated_at_ms: now_ms(),
            snapshot: MetricsSnapshot {
                runs_total: 2,
                ..MetricsSnapshot::default()
            },
        };
        fs::write(
            metrics_dir.join("process-stale-instance.json"),
            serde_json::to_vec(&stale).unwrap(),
        )
        .unwrap();
        assert_eq!(Metrics::aggregate(dir.path()).unwrap().runs_total, 2);
        assert!(!metrics_dir.join("process-stale-instance.json").exists());
        assert_eq!(Metrics::aggregate(dir.path()).unwrap().runs_total, 2);
    }
}
