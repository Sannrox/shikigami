//! The security-triage profile (`examples/security-triage.toml`, #465):
//! a site-agent overlay that emits typed exposure verdicts matching
//! `pkg:security.exposure-verdict/v1`. The model is scripted.

use std::collections::VecDeque;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::json;
use shikigami::events::{self, ChannelSink, EventSink, FanoutSink};
use shikigami::governance::LocalGovernance;
use shikigami::registry::RunRegistry;
use shikigami::run::{Engine, RunError};
use shikigami::security_triage::{PACKAGE_ID, TriageReport, severity_rank};
use shikigami::{
    AgentTurnAck, AgentTurnContinuation, AgentTurnPort, AgentTurnServeOptions, ClaimedAgentTurn,
    Config, Harness, HarnessEvent, PlaneAckOutcome, PlaneClaimLease, PlaneIntakeError,
    PublishedAgentDefinition, RunRequest, RunTermination, RuntimeToken, SiteAgentInstanceView,
    StateRoot, model, run_agent_turn_serve, workspace,
};
use tempfile::TempDir;

fn repo() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
}

fn triage_engine(
    script: serde_json::Value,
) -> (TempDir, Engine, std::sync::mpsc::Receiver<HarnessEvent>) {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::load(repo().join("examples/security-triage.toml")).unwrap();
    config.governance.adapter = "local".into();
    config.governance.fail_closed = false;
    config.model.adapter = "scripted".into();
    config.model.script_json = Some(script.to_string());
    config.events.adapter = "none".into();
    config.tools.mcp_gateway = None;
    config.workspace.root = dir.path().join("ws").to_string_lossy().into();
    config.context.skills_root = Some(
        repo()
            .join("examples/security-triage/skills")
            .to_string_lossy()
            .into(),
    );
    let state = StateRoot::new(dir.path().join("state"));
    state.ensure_ready_for_runs().unwrap();
    let (sink, rx) = ChannelSink::pair();
    let events: Arc<dyn EventSink> = Arc::new(FanoutSink::new(vec![
        Arc::from(events::from_config(&config, &state.runs_dir()).unwrap()),
        Arc::new(sink),
    ]));
    let engine = Engine::new(
        config.clone(),
        Arc::new(LocalGovernance::from_config(&config)),
        Arc::from(workspace::from_config(&config).unwrap()),
        Arc::from(model::from_config(&config).unwrap()),
        events,
        state.runs_dir(),
        Arc::new(RunRegistry::new(state.path()).unwrap()),
    );
    (dir, engine, rx)
}

async fn run(
    engine: &Engine,
    rx: &std::sync::mpsc::Receiver<HarnessEvent>,
) -> (
    Result<shikigami::run::RunResult, RunError>,
    Vec<HarnessEvent>,
) {
    let result = engine.run(RunRequest::new("Triage this finding")).await;
    let events = std::iter::from_fn(|| rx.try_recv().ok()).collect();
    (result, events)
}

fn report_turn(summary: &serde_json::Value) -> serde_json::Value {
    let args = serde_json::json!({"summary": summary.to_string(), "success": true});
    serde_json::json!({"tool_calls": [{"name": "report", "args_json": args.to_string()}]})
}

fn verdict(markings: &[&str], severity: &str) -> serde_json::Value {
    json!({
        "package": PACKAGE_ID,
        "finding_signature": "find-left-pad",
        "component": "payments-api",
        "verdict_severity": severity,
        "reachable_markings": markings,
        "environment_criticality": "prod",
        "evidence": ["lineage:payments-api"],
        "hypotheses": [{"statement": "default features expose the handler"}],
        "proposed_disposition": {
            "kind": "mitigate",
            "status": "proposed",
            "autonomy_granted": false
        }
    })
}

struct FakeAgentTurnPlane {
    claims: Mutex<VecDeque<ClaimedAgentTurn>>,
    acks: Mutex<Vec<AgentTurnAck>>,
}

#[async_trait]
impl AgentTurnPort for FakeAgentTurnPlane {
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
        let mut lease = claim.lease.clone();
        lease.valid_until = std::time::Instant::now() + ttl;
        Ok(lease)
    }

    async fn ack(
        &self,
        claim: &ClaimedAgentTurn,
        ack: &AgentTurnAck,
    ) -> Result<(), PlaneIntakeError> {
        self.acks.lock().unwrap().push(ack.clone());
        if ack.outcome == PlaneAckOutcome::Parked {
            let mut resumed = claim.clone();
            resumed.lease.generation += 1;
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

fn sample_turn(turn_id: &str, cause: &str) -> ClaimedAgentTurn {
    ClaimedAgentTurn {
        turn_id: turn_id.into(),
        namespace: "site".into(),
        instance_id: "inst-triage".into(),
        principal_id: "agent:security-triage".into(),
        cause: cause.into(),
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
            instance_id: "inst-triage".into(),
            principal_id: "agent:security-triage".into(),
            frozen: false,
            kill_switched: false,
            definition_revision_digest: "rev-1".into(),
            agent_member: "security-triage".into(),
        },
        definition: PublishedAgentDefinition {
            name: "security-triage".into(),
            task_class: "security-triage".into(),
            instructions: "triage the finding".into(),
        },
        continuation: None,
    }
}

#[tokio::test]
async fn marked_data_gets_a_higher_severity_verdict_than_an_isolated_component() {
    let isolated_summary = verdict(&[], "medium");
    let marked_summary = verdict(&["customer-pii"], "high");
    let (_dir_a, isolated_engine, isolated_rx) =
        triage_engine(json!([report_turn(&isolated_summary)]));
    let (_dir_b, marked_engine, marked_rx) = triage_engine(json!([report_turn(&marked_summary)]));

    let (isolated, isolated_events) = run(&isolated_engine, &isolated_rx).await;
    let (marked, marked_events) = run(&marked_engine, &marked_rx).await;
    let isolated = isolated.unwrap();
    let marked = marked.unwrap();
    assert_eq!(isolated.termination, RunTermination::Completed);
    assert!(isolated.success);
    assert!(marked.success);
    assert!(
        isolated_events.iter().any(|event| matches!(
            event,
            HarnessEvent::Message { text, .. } if text.starts_with("skill security-triage digest=")
        )),
        "the security-triage skill pack must load: {isolated_events:?}"
    );
    assert!(
        marked_events.iter().any(|event| matches!(
            event,
            HarnessEvent::Message { text, .. } if text == "security-triage profile"
        )),
        "the triage profile must be in trusted context: {marked_events:?}"
    );

    let isolated: TriageReport = serde_json::from_str(&isolated.summary).unwrap();
    let marked: TriageReport = serde_json::from_str(&marked.summary).unwrap();
    assert_eq!(isolated.package, PACKAGE_ID);
    assert!(isolated.reachable_markings.is_empty());
    assert_eq!(marked.reachable_markings, vec!["customer-pii".to_string()]);
    assert!(
        severity_rank(&marked.verdict_severity).unwrap()
            > severity_rank(&isolated.verdict_severity).unwrap()
    );
    assert_eq!(marked.hypotheses.len(), 1);
}

#[tokio::test]
async fn unreviewed_recorded_disposition_is_refused() {
    let summary = json!({
        "package": PACKAGE_ID,
        "finding_signature": "find-left-pad",
        "component": "payments-api",
        "verdict_severity": "high",
        "reachable_markings": ["customer-pii"],
        "evidence": ["lineage:payments-api"],
        "proposed_disposition": {
            "kind": "false-positive",
            "status": "recorded",
            "autonomy_granted": false
        }
    });
    let (_dir, engine, rx) = triage_engine(json!([report_turn(&summary)]));
    let (result, events) = run(&engine, &rx).await;
    assert!(
        events.iter().any(|event| matches!(
            event,
            HarnessEvent::ToolEnd { name, ok: false, detail, .. }
                if name == "report" && detail.contains("unreviewed disposition")
        )),
        "recorded disposition without autonomy must fail report: {events:?}"
    );
    assert!(
        result.is_err() || result.as_ref().is_ok_and(|run| !run.success),
        "an unreviewed recorded disposition must not look clean: {result:?}"
    );
}

#[tokio::test]
async fn disposition_without_autonomy_parks() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.governance.adapter = "local".into();
    config.governance.principal = "agent:security-triage".into();
    config.model.adapter = "scripted".into();
    config.model.script_json = Some(
        json!([{
            "tool_calls": [{
                "name": "escalate",
                "args_json": "{\"reason\":\"disposition requires review\",\"question\":\"awaiting Action decision\",\"action_instance_id\":\"disp-1\"}"
            }]
        }])
        .to_string(),
    );
    config.events.adapter = "none".into();
    config.workspace.root = dir.path().join("ws").to_string_lossy().into();
    let harness = Harness::from_config(config, StateRoot::new(dir.path().join("state"))).unwrap();
    let intake = FakeAgentTurnPlane {
        claims: Mutex::new(VecDeque::from([sample_turn(
            "turn-disp",
            "finding.created",
        )])),
        acks: Mutex::new(Vec::new()),
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
    assert_eq!(acks.len(), 1);
    assert_eq!(acks[0].outcome, PlaneAckOutcome::Parked);
    assert_eq!(acks[0].parked_action_instance_id, "disp-1");
}

#[tokio::test]
async fn reopened_disposition_wakes_exactly_one_triage_turn() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.governance.adapter = "local".into();
    config.governance.principal = "agent:security-triage".into();
    config.model.adapter = "scripted".into();
    config.model.script_json = Some(
        json!([{
            "tool_calls": [{
                "name": "report",
                "args_json": "{\"summary\":\"reopened finding triaged\",\"success\":true}"
            }]
        }])
        .to_string(),
    );
    config.events.adapter = "none".into();
    config.workspace.root = dir.path().join("ws").to_string_lossy().into();
    let harness = Harness::from_config(config, StateRoot::new(dir.path().join("state"))).unwrap();
    let intake = FakeAgentTurnPlane {
        claims: Mutex::new(VecDeque::from([sample_turn(
            "turn-reopen",
            "disposition.reopened",
        )])),
        acks: Mutex::new(Vec::new()),
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
    assert_eq!(acks.len(), 1);
    assert_eq!(acks[0].outcome, PlaneAckOutcome::Completed);
    assert!(intake.claims.lock().unwrap().is_empty());
}

#[tokio::test]
async fn frozen_instance_takes_no_triage_turns() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.governance.adapter = "local".into();
    config.governance.principal = "agent:security-triage".into();
    config.model.adapter = "scripted".into();
    config.model.script_json = Some(
        json!([{
            "tool_calls": [{
                "name": "report",
                "args_json": "{\"summary\":\"should not apply\",\"success\":true}"
            }]
        }])
        .to_string(),
    );
    config.events.adapter = "none".into();
    config.workspace.root = dir.path().join("ws").to_string_lossy().into();
    let harness = Harness::from_config(config, StateRoot::new(dir.path().join("state"))).unwrap();
    let mut frozen = sample_turn("turn-frozen", "finding.created");
    frozen.instance.frozen = true;
    let intake = FakeAgentTurnPlane {
        claims: Mutex::new(VecDeque::from([frozen])),
        acks: Mutex::new(Vec::new()),
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
    assert_eq!(acks.len(), 1);
    assert_eq!(acks[0].outcome, PlaneAckOutcome::Failed);
    assert!(acks[0].reason.contains("frozen"), "{}", acks[0].reason);
}
