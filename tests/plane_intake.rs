use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::json;
use sha2::{Digest, Sha256};
use shikigami::{
    ClaimedPlaneWork, Config, Harness, PlaneAck, PlaneAckOutcome, PlaneClaim, PlaneClaimEventKind,
    PlaneClaimLease, PlaneIntakeError, PlaneIntakePort, PlaneServeOptions, PlaneWorkContinuation,
    StateRoot, WorkerLifecycle, WorkerLifecycleIdentity, WorkerLifecycleState, run_plane_serve,
};
use tempfile::tempdir;

struct MockPlaneIntake {
    claims: Mutex<VecDeque<PlaneClaim>>,
    acks: Mutex<Vec<(String, PlaneAckOutcome)>>,
    ack_attempts: Mutex<u32>,
    transient_ack_failures: Mutex<u32>,
}

#[async_trait]
impl PlaneIntakePort for MockPlaneIntake {
    async fn claim_next(
        &self,
        _runtime_id: &str,
        _ttl: Duration,
    ) -> Result<Option<PlaneClaim>, PlaneIntakeError> {
        Ok(self.claims.lock().unwrap().pop_front())
    }

    async fn heartbeat(
        &self,
        claim: &PlaneClaim,
        ttl: Duration,
    ) -> Result<PlaneClaimLease, PlaneIntakeError> {
        let mut lease = claim.lease.clone();
        lease.valid_until = std::time::Instant::now() + ttl;
        Ok(lease)
    }

    async fn ack(&self, claim: &PlaneClaim, ack: &PlaneAck) -> Result<(), PlaneIntakeError> {
        *self.ack_attempts.lock().unwrap() += 1;
        let mut failures = self.transient_ack_failures.lock().unwrap();
        if *failures > 0 {
            *failures -= 1;
            return Err(PlaneIntakeError::Source("temporary outage".into()));
        }
        self.acks
            .lock()
            .unwrap()
            .push((claim.work.effect_id.clone(), ack.outcome));
        Ok(())
    }

    async fn report_claim_event(
        &self,
        _claim: &PlaneClaim,
        _kind: PlaneClaimEventKind,
        _checkpoint_digest: &str,
        _reason_code: &str,
        _request_id: &str,
    ) -> Result<(), PlaneIntakeError> {
        Ok(())
    }
}

struct ParkResumeIntake {
    claims: Mutex<VecDeque<PlaneClaim>>,
    acks: Mutex<Vec<PlaneAck>>,
    events: Mutex<Vec<PlaneClaimEventKind>>,
}

#[async_trait]
impl PlaneIntakePort for ParkResumeIntake {
    async fn claim_next(
        &self,
        _runtime_id: &str,
        _ttl: Duration,
    ) -> Result<Option<PlaneClaim>, PlaneIntakeError> {
        Ok(self.claims.lock().unwrap().pop_front())
    }

    async fn heartbeat(
        &self,
        claim: &PlaneClaim,
        ttl: Duration,
    ) -> Result<PlaneClaimLease, PlaneIntakeError> {
        let mut lease = claim.lease.clone();
        lease.valid_until = std::time::Instant::now() + ttl;
        Ok(lease)
    }

    async fn ack(&self, claim: &PlaneClaim, ack: &PlaneAck) -> Result<(), PlaneIntakeError> {
        self.acks.lock().unwrap().push(ack.clone());
        if ack.outcome == PlaneAckOutcome::Parked {
            let mut resumed = claim.clone();
            resumed.lease.generation += 1;
            resumed.lease.fencing_token = "fence-2".into();
            resumed.lease.valid_until = std::time::Instant::now() + Duration::from_secs(60);
            let input_json = json!({"answer": "approved"}).to_string();
            let input_digest = Sha256::digest(input_json.as_bytes())
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            resumed.work.continuation = Some(PlaneWorkContinuation {
                resolution_id: "resolution-1".into(),
                park_id: "park-1".into(),
                effect_id: claim.work.effect_id.clone(),
                operation_id: claim.work.operation_id.clone(),
                park_generation: 1,
                input_json,
                input_digest: format!("sha256:{input_digest}"),
                checkpoint: ack.checkpoint.clone(),
            });
            self.claims.lock().unwrap().push_back(resumed);
        }
        Ok(())
    }

    async fn report_claim_event(
        &self,
        _claim: &PlaneClaim,
        kind: PlaneClaimEventKind,
        _checkpoint_digest: &str,
        _reason_code: &str,
        _request_id: &str,
    ) -> Result<(), PlaneIntakeError> {
        self.events.lock().unwrap().push(kind);
        Ok(())
    }
}

#[tokio::test]
async fn claim_run_and_ack_with_mock_plane() {
    let dir = tempdir().unwrap();
    let state = StateRoot::new(dir.path().join("state"));
    let mut config = Config::default();
    config.governance.adapter = "local".into();
    config.model.adapter = "scripted".into();
    config.events.adapter = "none".into();
    config.workspace.root = dir.path().join("ws").to_string_lossy().into();
    let harness = Harness::from_config(config, state).unwrap();

    let parameters_json = json!({"task": "complete the claimed task"}).to_string();
    let parameters_digest = Sha256::digest(parameters_json.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let work = ClaimedPlaneWork {
        effect_id: "effect-1".into(),
        instance_id: "instance-1".into(),
        operation_id: "operation-1".into(),
        kind: "runtime_dispatch".into(),
        status: "claimed".into(),
        payload_json: json!({
            "runtime": "shikigami",
            "instance_id": "instance-1",
            "operation_id": "operation-1",
            "parameters_digest": parameters_digest,
        })
        .to_string(),
        parameters_json,
        resolved_task: None,
        continuation: None,
    };
    let intake = MockPlaneIntake {
        claims: Mutex::new(VecDeque::from([PlaneClaim {
            work,
            lease: PlaneClaimLease {
                runtime_id: "shikigami".into(),
                generation: 1,
                fencing_token: "fence-1".into(),
                expires_at_ms: chrono::Utc::now().timestamp_millis() + 60_000,
                valid_until: std::time::Instant::now() + Duration::from_secs(60),
            },
        }])),
        acks: Mutex::new(Vec::new()),
        ack_attempts: Mutex::new(0),
        transient_ack_failures: Mutex::new(1),
    };
    let (_tx, rx) = tokio::sync::watch::channel(false);
    let options = PlaneServeOptions {
        // Queue polling may be much slower than the claim lifecycle; an
        // ambiguous acknowledgement must still replay promptly.
        poll_interval: Duration::from_secs(60),
        max_jobs: Some(1),
        ..Default::default()
    };

    let completed = run_plane_serve(&harness, &intake, options, rx)
        .await
        .unwrap();
    assert_eq!(completed, 1);
    assert_eq!(
        *intake.acks.lock().unwrap(),
        vec![("effect-1".into(), PlaneAckOutcome::Completed)]
    );
    assert_eq!(*intake.ack_attempts.lock().unwrap(), 2);
}

#[tokio::test]
async fn park_resolve_reclaim_resumes_same_checkpoint_and_operation() {
    let dir = tempdir().unwrap();
    let state = StateRoot::new(dir.path().join("state"));
    let mut config = Config::default();
    config.governance.adapter = "local".into();
    config.model.adapter = "scripted".into();
    config.model.script_json = Some(
        json!([
            {
                "tool_calls": [{
                    "name": "escalate",
                    "args_json": "{\"reason\":\"approval required\",\"question\":\"continue?\"}"
                }]
            },
            {
                "tool_calls": [{
                    "name": "report",
                    "args_json": "{\"summary\":\"resumed work complete\",\"success\":true}"
                }]
            }
        ])
        .to_string(),
    );
    config.events.adapter = "none".into();
    config.workspace.root = dir.path().join("ws").to_string_lossy().into();
    let harness = Harness::from_config(config, state).unwrap();

    let parameters_json = json!({"task": "complete governed work"}).to_string();
    let parameters_digest = Sha256::digest(parameters_json.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let work = ClaimedPlaneWork {
        effect_id: "effect-park".into(),
        instance_id: "instance-park".into(),
        operation_id: "operation-stable".into(),
        kind: "runtime_dispatch".into(),
        status: "claimed".into(),
        payload_json: json!({
            "runtime": "shikigami",
            "instance_id": "instance-park",
            "operation_id": "operation-stable",
            "parameters_digest": parameters_digest,
        })
        .to_string(),
        parameters_json,
        resolved_task: None,
        continuation: None,
    };
    let intake = ParkResumeIntake {
        claims: Mutex::new(VecDeque::from([PlaneClaim {
            work,
            lease: PlaneClaimLease {
                runtime_id: "shikigami".into(),
                generation: 1,
                fencing_token: "fence-1".into(),
                expires_at_ms: chrono::Utc::now().timestamp_millis() + 60_000,
                valid_until: std::time::Instant::now() + Duration::from_secs(60),
            },
        }])),
        acks: Mutex::new(Vec::new()),
        events: Mutex::new(Vec::new()),
    };
    let (_tx, rx) = tokio::sync::watch::channel(false);
    let options = PlaneServeOptions {
        max_jobs: Some(2),
        checkpoint_store_id: Some("shikigami-local".into()),
        ..Default::default()
    };

    let completed = run_plane_serve(&harness, &intake, options, rx)
        .await
        .unwrap();
    assert_eq!(completed, 2);
    let acks = intake.acks.lock().unwrap();
    assert_eq!(acks.len(), 2);
    assert_eq!(acks[0].outcome, PlaneAckOutcome::Parked);
    assert_eq!(
        acks[0]
            .checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.store_id.as_str()),
        Some("shikigami-local")
    );
    assert_eq!(acks[1].outcome, PlaneAckOutcome::Completed);
    assert_eq!(
        *intake.events.lock().unwrap(),
        vec![
            PlaneClaimEventKind::ResumeStarted,
            PlaneClaimEventKind::ResumeSucceeded
        ]
    );
}

struct CountingIntake {
    claims: Mutex<VecDeque<PlaneClaim>>,
    claim_calls: Mutex<u32>,
}

#[async_trait]
impl PlaneIntakePort for CountingIntake {
    async fn claim_next(
        &self,
        _runtime_id: &str,
        _ttl: Duration,
    ) -> Result<Option<PlaneClaim>, PlaneIntakeError> {
        *self.claim_calls.lock().unwrap() += 1;
        Ok(self.claims.lock().unwrap().pop_front())
    }

    async fn heartbeat(
        &self,
        claim: &PlaneClaim,
        ttl: Duration,
    ) -> Result<PlaneClaimLease, PlaneIntakeError> {
        let mut lease = claim.lease.clone();
        lease.valid_until = std::time::Instant::now() + ttl;
        Ok(lease)
    }

    async fn ack(&self, _claim: &PlaneClaim, _ack: &PlaneAck) -> Result<(), PlaneIntakeError> {
        Ok(())
    }

    async fn report_claim_event(
        &self,
        _claim: &PlaneClaim,
        _kind: PlaneClaimEventKind,
        _checkpoint_digest: &str,
        _reason_code: &str,
        _request_id: &str,
    ) -> Result<(), PlaneIntakeError> {
        Ok(())
    }
}

fn sample_claim(effect_id: &str) -> PlaneClaim {
    let parameters_json = json!({"task": "work"}).to_string();
    let parameters_digest = Sha256::digest(parameters_json.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    PlaneClaim {
        work: ClaimedPlaneWork {
            effect_id: effect_id.into(),
            instance_id: "instance-1".into(),
            operation_id: "operation-1".into(),
            kind: "runtime_dispatch".into(),
            status: "claimed".into(),
            payload_json: json!({
                "runtime": "shikigami",
                "instance_id": "instance-1",
                "operation_id": "operation-1",
                "parameters_digest": parameters_digest,
            })
            .to_string(),
            parameters_json,
            resolved_task: None,
            continuation: None,
        },
        lease: PlaneClaimLease {
            runtime_id: "shikigami".into(),
            generation: 1,
            fencing_token: "fence-1".into(),
            expires_at_ms: chrono::Utc::now().timestamp_millis() + 60_000,
            valid_until: std::time::Instant::now() + Duration::from_secs(60),
        },
    }
}

#[tokio::test]
async fn drain_stops_new_plane_claims() {
    let dir = tempdir().unwrap();
    let state_path = dir.path().join("state");
    let state = StateRoot::new(&state_path);
    let mut config = Config::default();
    config.governance.adapter = "local".into();
    config.model.adapter = "scripted".into();
    config.events.adapter = "none".into();
    config.workspace.root = dir.path().join("ws").to_string_lossy().into();
    let harness = Harness::from_config(config, state).unwrap();

    let lifecycle = WorkerLifecycle::open(
        &state_path,
        WorkerLifecycleIdentity {
            worker_id: "w1".into(),
            namespace: "ns".into(),
            runtime_id: "shikigami".into(),
        },
    )
    .unwrap();
    lifecycle.mark_serving().unwrap();
    lifecycle.set_draining().unwrap();
    assert_eq!(lifecycle.snapshot().state, WorkerLifecycleState::Draining);

    let intake = CountingIntake {
        claims: Mutex::new(VecDeque::from([sample_claim("effect-should-not-run")])),
        claim_calls: Mutex::new(0),
    };
    let (_tx, rx) = tokio::sync::watch::channel(false);
    let options = PlaneServeOptions {
        max_jobs: Some(1),
        poll_interval: Duration::from_millis(10),
        lifecycle: Some(lifecycle.clone()),
        ..Default::default()
    };

    let completed = run_plane_serve(&harness, &intake, options, rx)
        .await
        .unwrap();
    assert_eq!(completed, 0);
    assert_eq!(*intake.claim_calls.lock().unwrap(), 0);
    assert_eq!(lifecycle.snapshot().state, WorkerLifecycleState::Draining);
}

#[tokio::test]
async fn lifecycle_ready_active_and_terminal_counters() {
    let dir = tempdir().unwrap();
    let state_path = dir.path().join("state");
    let state = StateRoot::new(&state_path);
    let mut config = Config::default();
    config.governance.adapter = "local".into();
    config.model.adapter = "scripted".into();
    config.events.adapter = "none".into();
    config.workspace.root = dir.path().join("ws").to_string_lossy().into();
    let harness = Harness::from_config(config, state).unwrap();

    let lifecycle = WorkerLifecycle::open(
        &state_path,
        WorkerLifecycleIdentity {
            worker_id: "w1".into(),
            namespace: "ns".into(),
            runtime_id: "shikigami".into(),
        },
    )
    .unwrap();
    lifecycle.mark_serving().unwrap();
    assert_eq!(lifecycle.snapshot().state, WorkerLifecycleState::Ready);

    let intake = MockPlaneIntake {
        claims: Mutex::new(VecDeque::from([sample_claim("effect-lc")])),
        acks: Mutex::new(Vec::new()),
        ack_attempts: Mutex::new(0),
        transient_ack_failures: Mutex::new(0),
    };
    let (_tx, rx) = tokio::sync::watch::channel(false);
    let options = PlaneServeOptions {
        max_jobs: Some(1),
        lifecycle: Some(lifecycle.clone()),
        ..Default::default()
    };
    let completed = run_plane_serve(&harness, &intake, options, rx)
        .await
        .unwrap();
    assert_eq!(completed, 1);
    let snap = lifecycle.snapshot();
    // max_jobs exit publishes draining so a dead worker is not fleet-ready.
    assert_eq!(snap.state, WorkerLifecycleState::Draining);
    assert_eq!(snap.terminal_completed, 1);
    assert!(snap.active_claim_ids.is_empty());
    assert!(
        !std::fs::read_to_string(lifecycle.path())
            .unwrap()
            .contains("complete the claimed")
    );
}

struct ClaimErrorIntake;

#[async_trait]
impl PlaneIntakePort for ClaimErrorIntake {
    async fn claim_next(
        &self,
        _runtime_id: &str,
        _ttl: Duration,
    ) -> Result<Option<PlaneClaim>, PlaneIntakeError> {
        Err(PlaneIntakeError::Source("plane unreachable".into()))
    }

    async fn heartbeat(
        &self,
        _claim: &PlaneClaim,
        _ttl: Duration,
    ) -> Result<PlaneClaimLease, PlaneIntakeError> {
        unreachable!("claim_next fails first")
    }

    async fn ack(&self, _claim: &PlaneClaim, _ack: &PlaneAck) -> Result<(), PlaneIntakeError> {
        unreachable!("claim_next fails first")
    }

    async fn report_claim_event(
        &self,
        _claim: &PlaneClaim,
        _kind: PlaneClaimEventKind,
        _checkpoint_digest: &str,
        _reason_code: &str,
        _request_id: &str,
    ) -> Result<(), PlaneIntakeError> {
        unreachable!("claim_next fails first")
    }
}

struct ClaimFenceLostIntake;

#[async_trait]
impl PlaneIntakePort for ClaimFenceLostIntake {
    async fn claim_next(
        &self,
        _runtime_id: &str,
        _ttl: Duration,
    ) -> Result<Option<PlaneClaim>, PlaneIntakeError> {
        // Mirrors pre-run HeartbeatActionClaim FailedPrecondition after an
        // owned claim: must surface FenceLost, never idle Ok(None).
        Err(PlaneIntakeError::FenceLost(
            "pre-run renew lost the fence".into(),
        ))
    }

    async fn heartbeat(
        &self,
        _claim: &PlaneClaim,
        _ttl: Duration,
    ) -> Result<PlaneClaimLease, PlaneIntakeError> {
        unreachable!("claim_next fails first")
    }

    async fn ack(&self, _claim: &PlaneClaim, _ack: &PlaneAck) -> Result<(), PlaneIntakeError> {
        unreachable!("claim_next fails first")
    }

    async fn report_claim_event(
        &self,
        _claim: &PlaneClaim,
        _kind: PlaneClaimEventKind,
        _checkpoint_digest: &str,
        _reason_code: &str,
        _request_id: &str,
    ) -> Result<(), PlaneIntakeError> {
        unreachable!("claim_next fails first")
    }
}

#[tokio::test]
async fn claim_next_fence_lost_demotes_lifecycle_not_idle() {
    let dir = tempdir().unwrap();
    let state_path = dir.path().join("state");
    let state = StateRoot::new(&state_path);
    let mut config = Config::default();
    config.governance.adapter = "local".into();
    config.model.adapter = "scripted".into();
    config.events.adapter = "none".into();
    config.workspace.root = dir.path().join("ws").to_string_lossy().into();
    let harness = Harness::from_config(config, state).unwrap();
    let lifecycle = WorkerLifecycle::open(
        &state_path,
        WorkerLifecycleIdentity {
            worker_id: "w1".into(),
            namespace: "ns".into(),
            runtime_id: "shikigami".into(),
        },
    )
    .unwrap();
    lifecycle.mark_serving().unwrap();
    let (_tx, rx) = tokio::sync::watch::channel(false);
    let options = PlaneServeOptions {
        lifecycle: Some(lifecycle.clone()),
        ..Default::default()
    };
    let err = run_plane_serve(&harness, &ClaimFenceLostIntake, options, rx)
        .await
        .unwrap_err();
    assert!(matches!(err, PlaneIntakeError::FenceLost(_)), "{err}");
    assert_eq!(lifecycle.snapshot().state, WorkerLifecycleState::FenceLost);
    assert!(!lifecycle.accepting_claims());
}

#[tokio::test]
async fn lifecycle_marks_governance_unavailable_on_claim_error() {
    let dir = tempdir().unwrap();
    let state_path = dir.path().join("state");
    let state = StateRoot::new(&state_path);
    let mut config = Config::default();
    config.governance.adapter = "local".into();
    config.model.adapter = "scripted".into();
    config.events.adapter = "none".into();
    config.workspace.root = dir.path().join("ws").to_string_lossy().into();
    let harness = Harness::from_config(config, state).unwrap();
    let lifecycle = WorkerLifecycle::open(
        &state_path,
        WorkerLifecycleIdentity {
            worker_id: "w1".into(),
            namespace: "ns".into(),
            runtime_id: "shikigami".into(),
        },
    )
    .unwrap();
    lifecycle.mark_serving().unwrap();
    let (_tx, rx) = tokio::sync::watch::channel(false);
    let options = PlaneServeOptions {
        lifecycle: Some(lifecycle.clone()),
        ..Default::default()
    };
    let err = run_plane_serve(&harness, &ClaimErrorIntake, options, rx)
        .await
        .unwrap_err();
    assert!(matches!(err, PlaneIntakeError::Source(_)), "{err}");
    assert_eq!(
        lifecycle.snapshot().state,
        WorkerLifecycleState::GovernanceUnavailable
    );
    assert!(!lifecycle.accepting_claims());
}

struct FenceFailIntake {
    claims: Mutex<VecDeque<PlaneClaim>>,
}

#[async_trait]
impl PlaneIntakePort for FenceFailIntake {
    async fn claim_next(
        &self,
        _runtime_id: &str,
        _ttl: Duration,
    ) -> Result<Option<PlaneClaim>, PlaneIntakeError> {
        Ok(self.claims.lock().unwrap().pop_front())
    }

    async fn heartbeat(
        &self,
        _claim: &PlaneClaim,
        _ttl: Duration,
    ) -> Result<PlaneClaimLease, PlaneIntakeError> {
        Err(PlaneIntakeError::FenceLost("lease_fenced".into()))
    }

    async fn ack(&self, _claim: &PlaneClaim, _ack: &PlaneAck) -> Result<(), PlaneIntakeError> {
        Ok(())
    }

    async fn report_claim_event(
        &self,
        _claim: &PlaneClaim,
        _kind: PlaneClaimEventKind,
        _checkpoint_digest: &str,
        _reason_code: &str,
        _request_id: &str,
    ) -> Result<(), PlaneIntakeError> {
        Ok(())
    }
}

#[tokio::test]
async fn lifecycle_records_fence_lost() {
    let dir = tempdir().unwrap();
    let state_path = dir.path().join("state");
    let state = StateRoot::new(&state_path);
    let mut config = Config::default();
    config.governance.adapter = "local".into();
    config.model.adapter = "scripted".into();
    config.events.adapter = "none".into();
    config.workspace.root = dir.path().join("ws").to_string_lossy().into();
    let harness = Harness::from_config(config, state).unwrap();

    let lifecycle = WorkerLifecycle::open(
        &state_path,
        WorkerLifecycleIdentity {
            worker_id: "w1".into(),
            namespace: "ns".into(),
            runtime_id: "shikigami".into(),
        },
    )
    .unwrap();
    lifecycle.mark_serving().unwrap();

    // Replacement path heartbeats before the run; fail that heartbeat to hit fence_lost.
    let mut claim = sample_claim("effect-fence");
    let input_json = json!({"answer": "retry"}).to_string();
    let input_digest = Sha256::digest(input_json.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    claim.work.continuation = Some(PlaneWorkContinuation {
        resolution_id: "resolution-1".into(),
        park_id: "park-1".into(),
        effect_id: claim.work.effect_id.clone(),
        operation_id: claim.work.operation_id.clone(),
        park_generation: 1,
        input_json,
        input_digest: format!("sha256:{input_digest}"),
        checkpoint: Some(shikigami::PlaneCheckpoint {
            store_id: "missing-store".into(),
            reference: "run-missing".into(),
            digest: "sha256:deadbeef".into(),
        }),
    });

    let intake = FenceFailIntake {
        claims: Mutex::new(VecDeque::from([claim])),
    };
    let (_tx, rx) = tokio::sync::watch::channel(false);
    let options = PlaneServeOptions {
        max_jobs: Some(1),
        checkpoint_store_id: Some("shikigami-local".into()),
        lifecycle: Some(lifecycle.clone()),
        ..Default::default()
    };
    let err = run_plane_serve(&harness, &intake, options, rx)
        .await
        .unwrap_err();
    assert!(matches!(err, PlaneIntakeError::FenceLost(_)), "{err}");
    assert_eq!(lifecycle.snapshot().state, WorkerLifecycleState::FenceLost);
    assert!(!lifecycle.accepting_claims());
}
