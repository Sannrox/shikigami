use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::json;
use sha2::{Digest, Sha256};
use shikigami::{
    ClaimedPlaneWork, Config, Harness, PlaneAckOutcome, PlaneClaim, PlaneClaimLease,
    PlaneIntakeError, PlaneIntakePort, PlaneServeOptions, StateRoot, run_plane_serve,
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

    async fn ack(
        &self,
        claim: &PlaneClaim,
        outcome: PlaneAckOutcome,
        _reason: &str,
    ) -> Result<(), PlaneIntakeError> {
        *self.ack_attempts.lock().unwrap() += 1;
        let mut failures = self.transient_ack_failures.lock().unwrap();
        if *failures > 0 {
            *failures -= 1;
            return Err(PlaneIntakeError::Source("temporary outage".into()));
        }
        self.acks
            .lock()
            .unwrap()
            .push((claim.work.effect_id.clone(), outcome));
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
        // Queue polling may be much slower than the claim lifecycle; ack retry
        // must still renew and retry promptly.
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
