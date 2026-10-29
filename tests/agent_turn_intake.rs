use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::json;
use shikigami::{
    AgentTurnAck, AgentTurnContinuation, AgentTurnPort, AgentTurnServeOptions, ClaimedAgentTurn,
    Config, Harness, PlaneAckOutcome, PlaneClaimLease, PlaneIntakeError, PublishedAgentDefinition,
    RuntimeToken, SiteAgentInstanceView, StateRoot, run_agent_turn_serve,
};
use tempfile::tempdir;

struct FakeAgentTurnPlane {
    claims: Mutex<VecDeque<ClaimedAgentTurn>>,
    acks: Mutex<Vec<AgentTurnAck>>,
    shutdown_on_claim: Option<tokio::sync::watch::Sender<bool>>,
    ack_failures_remaining: Mutex<u32>,
}

#[async_trait]
impl AgentTurnPort for FakeAgentTurnPlane {
    async fn claim_next(
        &self,
        _runtime_id: &str,
        _ttl: Duration,
    ) -> Result<Option<ClaimedAgentTurn>, PlaneIntakeError> {
        let claim = self.claims.lock().unwrap().pop_front();
        if claim.is_some()
            && let Some(shutdown) = &self.shutdown_on_claim
        {
            let _ = shutdown.send(true);
        }
        Ok(claim)
    }

    async fn heartbeat(
        &self,
        claim: &ClaimedAgentTurn,
        ttl: Duration,
    ) -> Result<PlaneClaimLease, PlaneIntakeError> {
        let mut lease = claim.lease.clone();
        lease.valid_until = std::time::Instant::now() + ttl;
        Ok(lease)
    }

    async fn ack(
        &self,
        claim: &ClaimedAgentTurn,
        ack: &AgentTurnAck,
    ) -> Result<(), PlaneIntakeError> {
        {
            let mut remaining = self.ack_failures_remaining.lock().unwrap();
            if *remaining > 0 {
                *remaining -= 1;
                return Err(PlaneIntakeError::Source(
                    "acknowledgement failed before the plane recorded it".into(),
                ));
            }
        }
        self.acks.lock().unwrap().push(ack.clone());
        if ack.outcome == PlaneAckOutcome::Parked {
            let mut resumed = claim.clone();
            resumed.lease.generation += 1;
            resumed.lease.fencing_token = "fence-2".into();
            resumed.lease.valid_until = std::time::Instant::now() + Duration::from_secs(60);
            resumed.parked_action_instance_id = ack.parked_action_instance_id.clone();
            resumed.continuation = Some(AgentTurnContinuation {
                action_instance_id: ack.parked_action_instance_id.clone(),
                status: "admitted".into(),
                approval_decision: "approve".into(),
                deny_reason: String::new(),
            });
            self.claims.lock().unwrap().push_back(resumed);
        }
        Ok(())
    }
}

fn site_agent_harness(dir: &tempfile::TempDir, script: serde_json::Value) -> Harness {
    let state = StateRoot::new(dir.path().join("state"));
    let mut config = Config::default();
    config.governance.adapter = "local".into();
    config.governance.principal = "agent:line-7".into();
    config.model.adapter = "scripted".into();
    config.model.script_json = Some(script.to_string());
    config.events.adapter = "none".into();
    config.workspace.root = dir.path().join("ws").to_string_lossy().into();
    Harness::from_config(config, state).unwrap()
}

fn sample_turn(turn_id: &str) -> ClaimedAgentTurn {
    ClaimedAgentTurn {
        turn_id: turn_id.into(),
        namespace: "site".into(),
        instance_id: "inst-line-7".into(),
        principal_id: "agent:line-7".into(),
        cause: "schedule".into(),
        status: "claimed".into(),
        parked_action_instance_id: String::new(),
        park_count: 0,
        lease: PlaneClaimLease {
            runtime_id: "shikigami".into(),
            generation: 1,
            fencing_token: "fence-1".into(),
            expires_at_ms: chrono::Utc::now().timestamp_millis() + 60_000,
            valid_until: std::time::Instant::now() + Duration::from_secs(60),
            runtime_token: RuntimeToken::from_grant("rt-turn".into(), i64::MAX, 0).unwrap(),
        },
        instance: SiteAgentInstanceView {
            instance_id: "inst-line-7".into(),
            principal_id: "agent:line-7".into(),
            frozen: false,
            kill_switched: false,
            definition_revision_digest: "rev-1".into(),
            agent_member: "line-7".into(),
        },
        definition: PublishedAgentDefinition {
            name: "line-7".into(),
            task_class: "site".into(),
            instructions: "inspect the line and propose the next Action".into(),
        },
        continuation: None,
    }
}

#[tokio::test]
async fn wake_claim_propose_action_park_resume_receipt() {
    let dir = tempdir().unwrap();
    let harness = site_agent_harness(
        &dir,
        json!([
            {
                "tool_calls": [{
                    "name": "escalate",
                    "args_json": "{\"reason\":\"approval required\",\"question\":\"awaiting Action decision\",\"action_instance_id\":\"action-1\"}"
                }]
            },
            {
                "tool_calls": [{
                    "name": "report",
                    "args_json": "{\"summary\":\"action granted; receipt recorded\",\"success\":true}"
                }]
            }
        ]),
    );
    let intake = FakeAgentTurnPlane {
        claims: Mutex::new(VecDeque::from([sample_turn("turn-1")])),
        acks: Mutex::new(Vec::new()),
        shutdown_on_claim: None,
        ack_failures_remaining: Mutex::new(0),
    };
    let (_tx, rx) = tokio::sync::watch::channel(false);
    let completed = run_agent_turn_serve(
        &harness,
        &intake,
        AgentTurnServeOptions {
            max_jobs: Some(2),
            ..Default::default()
        },
        rx,
    )
    .await
    .unwrap();
    assert_eq!(completed, 2);
    let acks = intake.acks.lock().unwrap();
    assert_eq!(acks.len(), 2);
    assert_eq!(acks[0].outcome, PlaneAckOutcome::Parked);
    assert_eq!(acks[0].parked_action_instance_id, "action-1");
    assert_eq!(acks[1].outcome, PlaneAckOutcome::Completed);
    assert!(
        acks[1].reason.contains("receipt") || acks[1].reason.contains("granted"),
        "{}",
        acks[1].reason
    );
}

#[tokio::test]
async fn worker_principal_may_differ_from_instance_principal() {
    let dir = tempdir().unwrap();
    let harness = {
        let mut harness = site_agent_harness(
            &dir,
            json!([{
                "tool_calls": [{
                    "name": "report",
                    "args_json": "{\"summary\":\"ran as instance\",\"success\":true}"
                }]
            }]),
        );
        harness.config.governance.principal = "worker:runtime".into();
        harness
    };
    let intake = FakeAgentTurnPlane {
        claims: Mutex::new(VecDeque::from([sample_turn("turn-worker")])),
        acks: Mutex::new(Vec::new()),
        shutdown_on_claim: None,
        ack_failures_remaining: Mutex::new(0),
    };
    let (_tx, rx) = tokio::sync::watch::channel(false);
    let completed = run_agent_turn_serve(
        &harness,
        &intake,
        AgentTurnServeOptions {
            max_jobs: Some(1),
            ..Default::default()
        },
        rx,
    )
    .await
    .unwrap();
    assert_eq!(completed, 1);
    let acks = intake.acks.lock().unwrap();
    assert_eq!(acks[0].outcome, PlaneAckOutcome::Completed);
    assert!(
        acks[0].reason.contains("ran as instance"),
        "{}",
        acks[0].reason
    );
}

#[tokio::test]
async fn park_without_submitted_action_is_refused() {
    let dir = tempdir().unwrap();
    let harness = site_agent_harness(
        &dir,
        json!([{
            "tool_calls": [{
                "name": "escalate",
                "args_json": "{\"reason\":\"approval required\",\"question\":\"no action id\"}"
            }]
        }]),
    );
    let intake = FakeAgentTurnPlane {
        claims: Mutex::new(VecDeque::from([sample_turn("turn-no-action")])),
        acks: Mutex::new(Vec::new()),
        shutdown_on_claim: None,
        ack_failures_remaining: Mutex::new(0),
    };
    let (_tx, rx) = tokio::sync::watch::channel(false);
    let completed = run_agent_turn_serve(
        &harness,
        &intake,
        AgentTurnServeOptions {
            max_jobs: Some(1),
            ..Default::default()
        },
        rx,
    )
    .await
    .unwrap();
    assert_eq!(completed, 1);
    let acks = intake.acks.lock().unwrap();
    assert_eq!(acks[0].outcome, PlaneAckOutcome::Failed);
    assert!(
        acks[0]
            .reason
            .contains("no Action submitted under this claim"),
        "{}",
        acks[0].reason
    );
}

#[tokio::test]
async fn lost_ack_response_is_recovered_without_a_heartbeat() {
    struct LostAckPlane {
        claims: Mutex<VecDeque<ClaimedAgentTurn>>,
        acks: Mutex<Vec<AgentTurnAck>>,
        fail_next_ack: Mutex<bool>,
    }
    #[async_trait]
    impl AgentTurnPort for LostAckPlane {
        async fn claim_next(
            &self,
            _runtime_id: &str,
            _ttl: Duration,
        ) -> Result<Option<ClaimedAgentTurn>, PlaneIntakeError> {
            Ok(self.claims.lock().unwrap().pop_front())
        }

        async fn heartbeat(
            &self,
            claim: &ClaimedAgentTurn,
            ttl: Duration,
        ) -> Result<PlaneClaimLease, PlaneIntakeError> {
            if !self.acks.lock().unwrap().is_empty() {
                return Err(PlaneIntakeError::FenceLost(
                    "turn is no longer claimed".into(),
                ));
            }
            let mut lease = claim.lease.clone();
            lease.valid_until = std::time::Instant::now() + ttl;
            Ok(lease)
        }

        async fn ack(
            &self,
            _claim: &ClaimedAgentTurn,
            ack: &AgentTurnAck,
        ) -> Result<(), PlaneIntakeError> {
            self.acks.lock().unwrap().push(ack.clone());
            let mut fail = self.fail_next_ack.lock().unwrap();
            if *fail {
                *fail = false;
                return Err(PlaneIntakeError::Source(
                    "acknowledgement response was lost".into(),
                ));
            }
            Ok(())
        }
    }

    let dir = tempdir().unwrap();
    let harness = site_agent_harness(
        &dir,
        json!([{
            "tool_calls": [{
                "name": "report",
                "args_json": "{\"summary\":\"finished\",\"success\":true}"
            }]
        }]),
    );
    let intake = LostAckPlane {
        claims: Mutex::new(VecDeque::from([sample_turn("turn-lost-ack")])),
        acks: Mutex::new(Vec::new()),
        fail_next_ack: Mutex::new(true),
    };
    let (_tx, rx) = tokio::sync::watch::channel(false);
    let completed = run_agent_turn_serve(
        &harness,
        &intake,
        AgentTurnServeOptions {
            max_jobs: Some(1),
            ack_retry_limit: 2,
            ..Default::default()
        },
        rx,
    )
    .await
    .unwrap();
    assert_eq!(completed, 1);
    let acks = intake.acks.lock().unwrap();
    assert_eq!(acks.len(), 2);
    assert_eq!(acks[0].outcome, PlaneAckOutcome::Completed);
    assert_eq!(acks[1].outcome, PlaneAckOutcome::Completed);
}

#[tokio::test]
async fn static_bearer_refused_when_runtime_token_required() {
    let error = RuntimeToken::from_grant(String::new(), 0, 0).unwrap_err();
    assert!(matches!(error, PlaneIntakeError::Source(_)), "{error}");
    assert!(error.to_string().contains("no runtime token"), "{error}");

    struct MissingTokenPlane;
    #[async_trait]
    impl AgentTurnPort for MissingTokenPlane {
        async fn claim_next(
            &self,
            _runtime_id: &str,
            _ttl: Duration,
        ) -> Result<Option<ClaimedAgentTurn>, PlaneIntakeError> {
            Err(RuntimeToken::from_grant(String::new(), 0, 0).unwrap_err())
        }

        async fn heartbeat(
            &self,
            _claim: &ClaimedAgentTurn,
            _ttl: Duration,
        ) -> Result<PlaneClaimLease, PlaneIntakeError> {
            unreachable!("claim_next refuses first")
        }

        async fn ack(
            &self,
            _claim: &ClaimedAgentTurn,
            _ack: &AgentTurnAck,
        ) -> Result<(), PlaneIntakeError> {
            unreachable!("claim_next refuses first")
        }
    }

    let dir = tempdir().unwrap();
    let harness = site_agent_harness(
        &dir,
        json!([{
            "tool_calls": [{
                "name": "report",
                "args_json": "{\"summary\":\"should not run\",\"success\":true}"
            }]
        }]),
    );
    let (_tx, rx) = tokio::sync::watch::channel(false);
    let error = run_agent_turn_serve(
        &harness,
        &MissingTokenPlane,
        AgentTurnServeOptions {
            max_jobs: Some(1),
            ..Default::default()
        },
        rx,
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("no runtime token"), "{error}");
}

#[tokio::test]
async fn abrupt_stop_does_not_double_apply() {
    let dir = tempdir().unwrap();
    let harness = site_agent_harness(
        &dir,
        json!([{
            "tool_calls": [{
                "name": "report",
                "args_json": "{\"summary\":\"fresh apply\",\"success\":true}"
            }]
        }]),
    );
    let (tx, rx) = tokio::sync::watch::channel(false);
    let first = sample_turn("turn-stop");
    let intake = FakeAgentTurnPlane {
        claims: Mutex::new(VecDeque::from([first])),
        acks: Mutex::new(Vec::new()),
        shutdown_on_claim: Some(tx),
        ack_failures_remaining: Mutex::new(0),
    };
    let completed = run_agent_turn_serve(
        &harness,
        &intake,
        AgentTurnServeOptions {
            max_jobs: Some(1),
            ..Default::default()
        },
        rx,
    )
    .await
    .unwrap();
    assert_eq!(completed, 0);
    assert!(intake.acks.lock().unwrap().is_empty());

    let mut resumed = sample_turn("turn-stop");
    resumed.lease.generation = 2;
    resumed.lease.fencing_token = "fence-2".into();
    resumed.continuation = Some(AgentTurnContinuation {
        action_instance_id: "action-granted".into(),
        status: "admitted".into(),
        approval_decision: "approve".into(),
        deny_reason: String::new(),
    });
    intake.claims.lock().unwrap().push_back(resumed);

    let resume_dir = tempdir().unwrap();
    let resume_harness = site_agent_harness(
        &resume_dir,
        json!([{
            "tool_calls": [{
                "name": "report",
                "args_json": "{\"summary\":\"read granted outcome; not applied again\",\"success\":true}"
            }]
        }]),
    );
    let (_tx, rx) = tokio::sync::watch::channel(false);
    let completed = run_agent_turn_serve(
        &resume_harness,
        &intake,
        AgentTurnServeOptions {
            max_jobs: Some(1),
            ..Default::default()
        },
        rx,
    )
    .await
    .unwrap();
    assert_eq!(completed, 1);
    let acks = intake.acks.lock().unwrap();
    assert_eq!(acks.len(), 1);
    assert_eq!(acks[0].outcome, PlaneAckOutcome::Completed);
    assert!(
        acks[0].reason.contains("not applied again"),
        "{}",
        acks[0].reason
    );
}

#[tokio::test]
async fn unacked_park_is_reclaimed_without_submitting_again() {
    let dir = tempdir().unwrap();
    let harness = site_agent_harness(
        &dir,
        json!([
            {
                "tool_calls": [{
                    "name": "escalate",
                    "args_json": "{\"reason\":\"approval required\",\"question\":\"awaiting Action decision\",\"action_instance_id\":\"action-crash\"}"
                }]
            },
            {
                "tool_calls": [{
                    "name": "report",
                    "args_json": "{\"summary\":\"read granted outcome; not applied again\",\"success\":true}"
                }]
            }
        ]),
    );
    let turn = sample_turn("turn-crash");
    let intake = FakeAgentTurnPlane {
        claims: Mutex::new(VecDeque::from([turn.clone()])),
        acks: Mutex::new(Vec::new()),
        shutdown_on_claim: None,
        ack_failures_remaining: Mutex::new(1),
    };
    let (_tx, rx) = tokio::sync::watch::channel(false);
    let error = run_agent_turn_serve(
        &harness,
        &intake,
        AgentTurnServeOptions {
            max_jobs: Some(1),
            ack_retry_limit: 1,
            ..Default::default()
        },
        rx,
    )
    .await
    .unwrap_err();
    assert!(
        error.to_string().contains("acknowledgement failed"),
        "{error}"
    );
    assert!(intake.acks.lock().unwrap().is_empty());

    intake.claims.lock().unwrap().push_back(turn);
    let (_tx, rx) = tokio::sync::watch::channel(false);
    let completed = run_agent_turn_serve(
        &harness,
        &intake,
        AgentTurnServeOptions {
            max_jobs: Some(1),
            ..Default::default()
        },
        rx,
    )
    .await
    .unwrap();
    assert_eq!(completed, 1);
    let acks = intake.acks.lock().unwrap();
    assert_eq!(acks.len(), 1);
    assert_eq!(acks[0].outcome, PlaneAckOutcome::Completed);
    assert!(
        acks[0].reason.contains("not applied again"),
        "{}",
        acks[0].reason
    );
}

#[tokio::test]
async fn locally_active_run_leaves_the_claim_unacked() {
    let dir = tempdir().unwrap();
    let harness = site_agent_harness(
        &dir,
        json!([{
            "tool_calls": [{
                "name": "report",
                "args_json": "{\"summary\":\"should not start while locally active\",\"success\":true}"
            }]
        }]),
    );
    harness
        .registry
        .start("turn-active", "held", None, None)
        .unwrap();
    let intake = FakeAgentTurnPlane {
        claims: Mutex::new(VecDeque::from([sample_turn("turn-active")])),
        acks: Mutex::new(Vec::new()),
        shutdown_on_claim: None,
        ack_failures_remaining: Mutex::new(0),
    };
    let (_tx, rx) = tokio::sync::watch::channel(false);
    let completed = run_agent_turn_serve(
        &harness,
        &intake,
        AgentTurnServeOptions {
            max_jobs: Some(1),
            ..Default::default()
        },
        rx,
    )
    .await
    .unwrap();
    assert_eq!(completed, 1);
    assert!(intake.acks.lock().unwrap().is_empty());
    assert!(harness.registry.run_is_active("turn-active").unwrap());
}

#[tokio::test]
async fn frozen_or_kill_switched_instance_refuses_without_applying() {
    let dir = tempdir().unwrap();
    let harness = site_agent_harness(
        &dir,
        json!([{
            "tool_calls": [{
                "name": "report",
                "args_json": "{\"summary\":\"should not apply\",\"success\":true}"
            }]
        }]),
    );

    let mut frozen = sample_turn("turn-frozen");
    frozen.instance.frozen = true;
    let mut killed = sample_turn("turn-killed");
    killed.instance.kill_switched = true;
    let intake = FakeAgentTurnPlane {
        claims: Mutex::new(VecDeque::from([frozen, killed])),
        acks: Mutex::new(Vec::new()),
        shutdown_on_claim: None,
        ack_failures_remaining: Mutex::new(0),
    };
    let (_tx, rx) = tokio::sync::watch::channel(false);
    let completed = run_agent_turn_serve(
        &harness,
        &intake,
        AgentTurnServeOptions {
            max_jobs: Some(2),
            ..Default::default()
        },
        rx,
    )
    .await
    .unwrap();
    assert_eq!(completed, 2);
    let acks = intake.acks.lock().unwrap();
    assert_eq!(acks.len(), 2);
    assert_eq!(acks[0].outcome, PlaneAckOutcome::Failed);
    assert!(acks[0].reason.contains("frozen"), "{}", acks[0].reason);
    assert_eq!(acks[1].outcome, PlaneAckOutcome::Failed);
    assert!(
        acks[1].reason.contains("kill-switched"),
        "{}",
        acks[1].reason
    );
}

#[tokio::test]
async fn principal_mismatch_is_refused() {
    let dir = tempdir().unwrap();
    let harness = site_agent_harness(
        &dir,
        json!([{
            "tool_calls": [{
                "name": "report",
                "args_json": "{\"summary\":\"should not apply\",\"success\":true}"
            }]
        }]),
    );
    let mut claim = sample_turn("turn-mismatch");
    claim.principal_id = "agent:other".into();
    let intake = FakeAgentTurnPlane {
        claims: Mutex::new(VecDeque::from([claim])),
        acks: Mutex::new(Vec::new()),
        shutdown_on_claim: None,
        ack_failures_remaining: Mutex::new(0),
    };
    let (_tx, rx) = tokio::sync::watch::channel(false);
    let completed = run_agent_turn_serve(
        &harness,
        &intake,
        AgentTurnServeOptions {
            max_jobs: Some(1),
            ..Default::default()
        },
        rx,
    )
    .await
    .unwrap();
    assert_eq!(completed, 1);
    let acks = intake.acks.lock().unwrap();
    assert_eq!(acks[0].outcome, PlaneAckOutcome::Failed);
    assert!(
        acks[0].reason.contains("does not match instance principal"),
        "{}",
        acks[0].reason
    );
}
