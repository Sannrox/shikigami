//! Managed production host for sekai-chisei plane intake.
//!
//! This module owns the fail-closed startup sequence. Hosts supply deployment
//! intent, inspect the prepared endpoint metadata, and then consume the
//! prepared host to run it.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use crate::governance::GovernanceError;
use crate::plane_intake::PlaneIntakeError;
use crate::worker_lifecycle::WorkerLifecycleError;
use thiserror::Error;

#[cfg(feature = "governance-sekai-chisei")]
use crate::governance::sekai_chisei::SekaiClaimClient;
#[cfg(feature = "governance-sekai-chisei")]
use crate::harness::Harness;
#[cfg(feature = "governance-sekai-chisei")]
use crate::plane_intake::{ClaimedWorkPolicy, PlaneServeOptions, run_plane_serve};
#[cfg(feature = "governance-sekai-chisei")]
use crate::worker_lifecycle::{
    WorkerLifecycle, WorkerLifecycleIdentity, lifecycle_path, serve_lifecycle_http,
};
#[cfg(feature = "governance-sekai-chisei")]
use std::path::Path;
#[cfg(feature = "governance-sekai-chisei")]
use tokio::sync::watch;

/// Operator intent for one managed plane-intake host.
#[derive(Debug, Clone)]
pub struct PlaneHostOptions {
    pub runtime_id: String,
    pub worker_id: Option<String>,
    pub poll_interval: Duration,
    pub max_jobs: Option<u64>,
    pub claim_ttl: Duration,
    pub checkpoint_store_id: Option<String>,
    pub lifecycle_listen: Option<String>,
}

impl Default for PlaneHostOptions {
    fn default() -> Self {
        Self {
            runtime_id: "shikigami".into(),
            worker_id: None,
            poll_interval: Duration::from_millis(200),
            max_jobs: None,
            claim_ttl: Duration::from_secs(60),
            checkpoint_store_id: None,
            lifecycle_listen: None,
        }
    }
}

/// Startup metadata available before the blocking intake loop begins.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlaneHostInfo {
    pub worker_id: String,
    pub runtime_id: String,
    pub namespace: String,
    pub lifecycle_path: PathBuf,
    pub lifecycle_addr: Option<SocketAddr>,
}

#[derive(Debug, Error)]
pub enum PlaneHostError {
    #[error("plane host requires governance.adapter = \"sekai-chisei\", got {actual:?}")]
    WrongGovernanceAdapter { actual: String },
    #[error("invalid plane host option {field}: {message}")]
    InvalidOption {
        field: &'static str,
        message: String,
    },
    #[error("plane host doctor failed: {0}")]
    DoctorFailed(String),
    #[error(transparent)]
    Governance(#[from] GovernanceError),
    #[error(transparent)]
    Lifecycle(#[from] WorkerLifecycleError),
    #[error(transparent)]
    Intake(#[from] PlaneIntakeError),
}

/// A plane host that passed every startup gate and can be run exactly once.
#[cfg(feature = "governance-sekai-chisei")]
pub struct PreparedPlaneHost<'a> {
    harness: &'a Harness,
    client: SekaiClaimClient,
    options: PlaneServeOptions,
    shutdown: watch::Receiver<bool>,
    listener_shutdown: watch::Sender<bool>,
    lifecycle: WorkerLifecycle,
    finished: bool,
    info: PlaneHostInfo,
}

#[cfg(feature = "governance-sekai-chisei")]
impl PreparedPlaneHost<'_> {
    pub fn info(&self) -> &PlaneHostInfo {
        &self.info
    }

    pub async fn run(mut self) -> Result<u64, PlaneHostError> {
        if let Err(error) = self.lifecycle.mark_serving() {
            let _ = std::fs::remove_file(self.lifecycle.path());
            return Err(error.into());
        }
        let result = run_plane_serve(
            self.harness,
            &self.client,
            self.options.clone(),
            self.shutdown.clone(),
        )
        .await;
        self.finished = true;
        Ok(result?)
    }
}

#[cfg(feature = "governance-sekai-chisei")]
impl Drop for PreparedPlaneHost<'_> {
    fn drop(&mut self) {
        let _ = self.listener_shutdown.send(true);
        if !self.finished {
            let _ = std::fs::remove_file(self.lifecycle.path());
        }
    }
}

/// Validate and prepare a managed plane host without admitting work.
///
/// Startup is fail closed: the lifecycle is published non-ready before client,
/// doctor, and listener setup, and is removed if any later gate fails.
/// The caller must exclusively own the configured state root for this host;
/// preparation treats any prior lifecycle snapshot there as stale restart
/// state. Multiple live workers require distinct state roots.
#[cfg(feature = "governance-sekai-chisei")]
pub async fn prepare_plane_host(
    harness: &Harness,
    options: PlaneHostOptions,
    shutdown: watch::Receiver<bool>,
) -> Result<PreparedPlaneHost<'_>, PlaneHostError> {
    remove_stale_lifecycle(harness.state.path());
    validate_options(harness, &options)?;
    let worker_id = options.worker_id.clone().unwrap_or_else(default_worker_id);
    if worker_id.trim().is_empty() {
        return Err(invalid("worker_id", "must not be empty"));
    }

    let lifecycle = WorkerLifecycle::open(
        harness.state.path(),
        WorkerLifecycleIdentity {
            worker_id: worker_id.clone(),
            namespace: harness.config.governance.namespace.clone(),
            runtime_id: options.runtime_id.clone(),
        },
    )?;

    let prepared =
        prepare_after_lifecycle(harness, &options, shutdown, worker_id, &lifecycle).await;
    if prepared.is_err() {
        let _ = lifecycle.set_unhealthy("plane_startup_failed");
        let _ = std::fs::remove_file(lifecycle.path());
    }
    prepared
}

#[cfg(feature = "governance-sekai-chisei")]
async fn prepare_after_lifecycle<'a>(
    harness: &'a Harness,
    host: &PlaneHostOptions,
    shutdown: watch::Receiver<bool>,
    worker_id: String,
    lifecycle: &WorkerLifecycle,
) -> Result<PreparedPlaneHost<'a>, PlaneHostError> {
    let client = SekaiClaimClient::from_config(&harness.config)?;
    let report = harness.doctor();
    if !report.ok {
        return Err(PlaneHostError::DoctorFailed(report.lines.join("; ")));
    }
    let (listener_shutdown, listener_rx) = watch::channel(false);
    let listener_addr = match lifecycle_bind(host)? {
        Some(bind) => Some(serve_lifecycle_http(bind, lifecycle.clone(), listener_rx).await?),
        None => None,
    };
    if listener_addr.is_some() {
        let listener_shutdown_on_host = listener_shutdown.clone();
        let mut host_shutdown = shutdown.clone();
        tokio::spawn(async move {
            if !*host_shutdown.borrow() {
                let _ = host_shutdown.changed().await;
            }
            let _ = listener_shutdown_on_host.send(true);
        });
    }
    let options = PlaneServeOptions {
        poll_interval: host.poll_interval.max(Duration::from_millis(10)),
        max_jobs: host.max_jobs,
        claim_ttl: host.claim_ttl,
        heartbeat_interval: host.claim_ttl / 3,
        ack_retry_limit: 5,
        checkpoint_store_id: host.checkpoint_store_id.clone(),
        policy: ClaimedWorkPolicy {
            expected_runtime: host.runtime_id.clone(),
            host_timeout: harness.config.run.timeout_secs.map(Duration::from_secs),
            ..Default::default()
        },
        lifecycle: Some(lifecycle.clone()),
    };
    Ok(PreparedPlaneHost {
        harness,
        client,
        options,
        shutdown,
        listener_shutdown,
        lifecycle: lifecycle.clone(),
        finished: false,
        info: PlaneHostInfo {
            worker_id,
            runtime_id: host.runtime_id.clone(),
            namespace: harness.config.governance.namespace.clone(),
            lifecycle_path: lifecycle.path(),
            lifecycle_addr: listener_addr,
        },
    })
}

#[cfg(feature = "governance-sekai-chisei")]
fn validate_options(harness: &Harness, options: &PlaneHostOptions) -> Result<(), PlaneHostError> {
    if harness.config.governance.adapter != "sekai-chisei" {
        return Err(PlaneHostError::WrongGovernanceAdapter {
            actual: harness.config.governance.adapter.clone(),
        });
    }
    if options.runtime_id.trim().is_empty() {
        return Err(invalid("runtime_id", "must not be empty"));
    }
    if options.claim_ttl < Duration::from_millis(3) {
        return Err(invalid(
            "claim_ttl",
            "must be at least 3 milliseconds so the derived heartbeat is positive",
        ));
    }
    if options
        .checkpoint_store_id
        .as_deref()
        .is_some_and(|value| value.trim().is_empty())
    {
        return Err(invalid("checkpoint_store_id", "must not be empty"));
    }
    if let Some(bind) = lifecycle_bind(options)?
        && !bind.ip().is_loopback()
        && !bind.ip().is_unspecified()
    {
        return Err(invalid(
            "lifecycle_listen",
            "must be loopback or unspecified (0.0.0.0/[::])",
        ));
    }
    Ok(())
}

#[cfg(feature = "governance-sekai-chisei")]
fn lifecycle_bind(options: &PlaneHostOptions) -> Result<Option<SocketAddr>, PlaneHostError> {
    options
        .lifecycle_listen
        .as_deref()
        .map(str::parse)
        .transpose()
        .map_err(|error| {
            invalid(
                "lifecycle_listen",
                format!("invalid socket address: {error}"),
            )
        })
}

#[cfg(feature = "governance-sekai-chisei")]
fn invalid(field: &'static str, message: impl Into<String>) -> PlaneHostError {
    PlaneHostError::InvalidOption {
        field,
        message: message.into(),
    }
}

#[cfg(feature = "governance-sekai-chisei")]
fn remove_stale_lifecycle(state_root: &Path) {
    let _ = std::fs::remove_file(lifecycle_path(state_root));
}

#[cfg(feature = "governance-sekai-chisei")]
fn default_worker_id() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("HOST"))
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "shikigami-worker".into())
}

#[cfg(all(test, feature = "governance-sekai-chisei"))]
mod tests {
    use super::*;
    use crate::{Config, StateRoot};

    fn harness() -> (tempfile::TempDir, Harness) {
        let dir = tempfile::tempdir().unwrap();
        let harness = Harness::from_config(Config::default(), StateRoot::new(dir.path())).unwrap();
        (dir, harness)
    }

    #[test]
    fn rejects_non_plane_governance_at_the_host_interface() {
        let (_dir, harness) = harness();
        let error = validate_options(&harness, &PlaneHostOptions::default()).unwrap_err();
        assert!(matches!(
            error,
            PlaneHostError::WrongGovernanceAdapter { .. }
        ));
    }

    #[tokio::test]
    async fn invalid_startup_removes_a_stale_lifecycle_snapshot() {
        let (_dir, harness) = harness();
        let path = lifecycle_path(harness.state.path());
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, r#"{"state":"ready"}"#).unwrap();
        let (_tx, rx) = watch::channel(false);

        let result = prepare_plane_host(&harness, PlaneHostOptions::default(), rx).await;

        assert!(matches!(
            result,
            Err(PlaneHostError::WrongGovernanceAdapter { .. })
        ));
        assert!(!path.exists());
    }

    #[test]
    fn rejects_public_lifecycle_bind_at_the_host_interface() {
        let (_dir, mut harness) = harness();
        harness.config.governance.adapter = "sekai-chisei".into();
        let options = PlaneHostOptions {
            lifecycle_listen: Some("192.0.2.10:8080".into()),
            ..Default::default()
        };
        let error = validate_options(&harness, &options).unwrap_err();
        assert!(matches!(
            error,
            PlaneHostError::InvalidOption {
                field: "lifecycle_listen",
                ..
            }
        ));
    }
}
