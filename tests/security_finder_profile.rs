//! The security-finder profile (`examples/security-finder.toml`, #464):
//! specialised read-only roles emit typed findings matching
//! `pkg:security.finding-exposure/v1`. The model is scripted.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use shikigami::run::RunError;
use shikigami::security_finder::{
    FinderReport, PACKAGE_ID, ROLE_AUTHORIZATION_BOUNDARY, ROLE_DEPENDENCY_ADVISORY,
    problem_signature,
};
use shikigami::{
    ChannelSink, Config, Harness, HarnessError, HarnessEvent, RunRequest, RunTermination, StateRoot,
};
use tempfile::TempDir;

fn repo() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
}

fn copy_tree(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let dest = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &dest);
        } else {
            std::fs::copy(entry.path(), dest).unwrap();
        }
    }
}

fn finder(role: &str, inputs: &str, script: &str, model: &str) -> (TempDir, Harness, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let workspace = dir.path().join("inputs");
    copy_tree(
        &repo().join("tests/fixtures/security_finder").join(inputs),
        &workspace,
    );

    let mut config = Config::load(repo().join("examples/security-finder.toml")).unwrap();
    config.security_finder.role = Some(role.into());
    if role == ROLE_AUTHORIZATION_BOUNDARY {
        config.run.max_turns = 16;
        config.run.timeout_secs = Some(600);
    }
    config.model.adapter = "scripted".into();
    config.model.model = model.into();
    config.model.script_json = Some(script.into());
    config.events.adapter = "none".into();
    config.workspace.root = workspace.to_string_lossy().into();
    config.context.skills_root = Some(
        repo()
            .join("examples/security-finder/skills")
            .to_string_lossy()
            .into(),
    );
    config.validate().unwrap();
    let state = StateRoot::new(dir.path().join("state"));
    let harness = Harness::from_config(config, state).unwrap();
    (dir, harness, workspace)
}

async fn run(
    harness: &Harness,
) -> (
    Result<shikigami::RunResult, HarnessError>,
    Vec<HarnessEvent>,
) {
    let (sink, rx) = ChannelSink::pair();
    let result = harness
        .run_with_events(
            RunRequest::new("Review this workspace"),
            Some(Arc::new(sink)),
        )
        .await;
    let events = std::iter::from_fn(|| rx.try_recv().ok()).collect();
    (result, events)
}

fn report_turn(summary: &serde_json::Value) -> serde_json::Value {
    let args = serde_json::json!({"summary": summary.to_string(), "success": true});
    serde_json::json!({"tool_calls": [{"name": "report", "args_json": args.to_string()}]})
}

fn tool_turn(name: &str, args: serde_json::Value) -> serde_json::Value {
    serde_json::json!({"tool_calls": [{"name": name, "args_json": args.to_string()}]})
}

fn parsed_report(result: &shikigami::RunResult) -> FinderReport {
    serde_json::from_str(&result.summary).unwrap()
}

#[tokio::test]
async fn a_seeded_vulnerable_dependency_yields_one_typed_finding() {
    let summary = serde_json::json!({
        "package": PACKAGE_ID,
        "role": ROLE_DEPENDENCY_ADVISORY,
        "coverage": {"complete": true, "summary": "Reviewed Cargo.lock and Cargo.toml."},
        "findings": [{
            "source": "advisory",
            "component": "left-pad",
            "version_range": "=1.3.0",
            "reported_severity": "high",
            "evidence_refs": "Cargo.lock:left-pad@1.3.0",
            "finding_class": "vulnerable-dependency"
        }],
        "hypotheses": [{"finding_index": 0, "statement": "reachable from default features"}]
    });
    let script = serde_json::json!([
        tool_turn("read_file", serde_json::json!({"path": "Cargo.lock"})),
        report_turn(&summary),
    ]);
    let (_dir, harness, _inputs) = finder(
        ROLE_DEPENDENCY_ADVISORY,
        "vulnerable-dep",
        &script.to_string(),
        "auto",
    );
    let (result, events) = run(&harness).await;
    let result = result.unwrap();

    assert_eq!(result.termination, RunTermination::Completed);
    assert!(result.success);
    assert!(
        events.iter().any(|event| matches!(
            event,
            HarnessEvent::Message { text, .. } if text.starts_with("skill security-finder digest=")
        )),
        "the security-finder skill pack must load: {events:?}"
    );
    assert!(
        events.iter().any(|event| matches!(
            event,
            HarnessEvent::Message { text, .. } if text == "security-finder role=dependency-advisory"
        )),
        "the configured role must be in trusted context: {events:?}"
    );
    assert!(
        events.iter().any(|event| matches!(
            event,
            HarnessEvent::ToolEnd { name, ok: true, .. } if name == "read_file"
        )),
        "Cargo.lock must be read: {events:?}"
    );

    let report = parsed_report(&result);
    assert_eq!(report.package, PACKAGE_ID);
    assert_eq!(report.role, ROLE_DEPENDENCY_ADVISORY);
    assert!(report.coverage.complete);
    assert!(!report.coverage.summary.is_empty());
    assert_eq!(report.findings.len(), 1);
    let finding = &report.findings[0];
    assert_eq!(finding.source, "advisory");
    assert_eq!(finding.component, "left-pad");
    assert_eq!(finding.version_range, "=1.3.0");
    assert_eq!(finding.evidence_refs, "Cargo.lock:left-pad@1.3.0");
    assert_eq!(
        finding.problem_signature,
        problem_signature("left-pad", "=1.3.0", "vulnerable-dependency", "")
    );
    assert_eq!(report.hypotheses.len(), 1);
    assert_eq!(
        report.hypotheses[0].problem_signature,
        finding.problem_signature
    );
}

#[tokio::test]
async fn a_seeded_authorization_gap_yields_one_typed_finding() {
    let summary = serde_json::json!({
        "package": PACKAGE_ID,
        "role": ROLE_AUTHORIZATION_BOUNDARY,
        "coverage": {"complete": true, "summary": "Reviewed policy.toml."},
        "findings": [{
            "source": "review",
            "component": "http.anonymous",
            "version_range": "*",
            "reported_severity": "high",
            "evidence_refs": "policy.toml:http.anonymous",
            "finding_class": "authorization-gap"
        }]
    });
    let script = serde_json::json!([
        tool_turn("read_file", serde_json::json!({"path": "policy.toml"})),
        report_turn(&summary),
    ]);
    let (_dir, harness, _inputs) = finder(
        ROLE_AUTHORIZATION_BOUNDARY,
        "authz-gap",
        &script.to_string(),
        "auto",
    );
    let (result, _events) = run(&harness).await;
    let result = result.unwrap();
    assert!(result.success);
    let report = parsed_report(&result);
    assert_eq!(report.findings.len(), 1);
    let finding = &report.findings[0];
    assert_eq!(finding.component, "http.anonymous");
    assert_eq!(finding.evidence_refs, "policy.toml:http.anonymous");
    assert_eq!(
        finding.problem_signature,
        problem_signature("http.anonymous", "*", "authorization-gap", "")
    );
    assert!(report.hypotheses.is_empty());
}

#[tokio::test]
async fn a_clean_fixture_yields_no_findings_and_a_coverage_summary() {
    let summary = serde_json::json!({
        "package": PACKAGE_ID,
        "role": ROLE_DEPENDENCY_ADVISORY,
        "coverage": {
            "complete": true,
            "summary": "Reviewed Cargo.lock and policy.toml; no vulnerable dependency."
        },
        "findings": [],
        "hypotheses": []
    });
    let script = serde_json::json!([
        tool_turn("read_file", serde_json::json!({"path": "Cargo.lock"})),
        report_turn(&summary),
    ]);
    let (_dir, harness, _inputs) = finder(
        ROLE_DEPENDENCY_ADVISORY,
        "clean",
        &script.to_string(),
        "auto",
    );
    let (result, _events) = run(&harness).await;
    let result = result.unwrap();
    assert!(result.success);
    let report = parsed_report(&result);
    assert!(report.findings.is_empty());
    assert!(report.hypotheses.is_empty());
    assert!(report.coverage.complete);
    assert!(report.coverage.summary.contains("no vulnerable dependency"));
}

#[tokio::test]
async fn roles_refuse_tools_outside_the_allowlist_and_journal_attempts() {
    let attempts = ["web_fetch", "bash", "write_file", "child_run", "escalate"];
    let mut script = attempts
        .iter()
        .map(|tool| tool_turn(tool, serde_json::json!({})))
        .collect::<Vec<_>>();
    script.push(report_turn(&serde_json::json!({
        "package": PACKAGE_ID,
        "role": ROLE_DEPENDENCY_ADVISORY,
        "coverage": {"complete": true, "summary": "Reviewed after refusals."},
        "findings": []
    })));
    let (dir, harness, inputs) = finder(
        ROLE_DEPENDENCY_ADVISORY,
        "clean",
        &serde_json::Value::Array(script).to_string(),
        "auto",
    );

    let doctor = harness.doctor();
    let visible = doctor
        .lines
        .iter()
        .find_map(|line| line.strip_prefix("tools.visible:"))
        .expect("doctor names visible tools");
    let visible = visible
        .split(|c: char| c == ',' || c == '[' || c == ']' || c.is_whitespace())
        .filter(|tool| !tool.is_empty())
        .collect::<Vec<_>>();
    assert_eq!(visible, ["read_file", "glob", "grep", "report"]);
    assert!(
        !doctor
            .lines
            .iter()
            .any(|line| line.starts_with("tools.external:"))
    );

    let (result, events) = run(&harness).await;
    let result = result.unwrap();
    assert_eq!(result.termination, RunTermination::Completed);
    let journal = std::fs::read_to_string(
        dir.path()
            .join("state/runs")
            .join(&result.run_id)
            .join("events.jsonl"),
    )
    .unwrap();
    for tool in attempts {
        assert!(
            events.iter().any(|event| matches!(
                event,
                HarnessEvent::ToolEnd { name, ok: false, detail, .. }
                    if name == tool && detail.contains("tool not enabled")
            )),
            "{tool} attempt must be refused: {events:?}"
        );
        let refused = format!("{tool} ok=false");
        assert!(
            journal
                .lines()
                .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
                .any(|record| record["event"] == "tool_end"
                    && record["detail"]
                        .as_str()
                        .is_some_and(|detail| detail.starts_with(&refused))),
            "{tool} attempt must be in the run journal: {journal}"
        );
    }
    let mut left = std::fs::read_dir(&inputs)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect::<Vec<_>>();
    left.sort();
    assert_eq!(
        left,
        ["Cargo.lock", "policy.toml"],
        "inputs must stay untouched"
    );
}

#[tokio::test]
async fn two_configured_models_agree_on_problem_signatures() {
    let facts = serde_json::json!({
        "package": PACKAGE_ID,
        "role": ROLE_DEPENDENCY_ADVISORY,
        "coverage": {"complete": true, "summary": "Reviewed Cargo.lock."},
        "findings": [{
            "source": "review",
            "component": "left-pad",
            "version_range": "=1.3.0",
            "problem_signature": "model-a-invented",
            "reported_severity": "high",
            "evidence_refs": "Cargo.lock:left-pad@1.3.0",
            "finding_class": "vulnerable-dependency"
        }],
        "hypotheses": [{"finding_index": 0, "statement": "model a prose"}]
    });
    let script_a = serde_json::json!([
        tool_turn("read_file", serde_json::json!({"path": "Cargo.lock"})),
        report_turn(&facts),
    ]);
    let mut facts_b = facts.clone();
    facts_b["findings"][0]["problem_signature"] = serde_json::json!("model-b-invented");
    facts_b["findings"][0]["reported_severity"] = serde_json::json!("medium");
    facts_b["hypotheses"][0]["statement"] = serde_json::json!("model b prose");
    let script_b = serde_json::json!([
        tool_turn(
            "grep",
            serde_json::json!({"pattern": "left-pad", "path": "."})
        ),
        report_turn(&facts_b),
    ]);

    let mut signatures = Vec::new();
    for (index, script) in [script_a, script_b].into_iter().enumerate() {
        let (_dir, harness, _inputs) = finder(
            ROLE_DEPENDENCY_ADVISORY,
            "vulnerable-dep",
            &script.to_string(),
            &format!("finder-{index}"),
        );
        let (result, _events) = run(&harness).await;
        let report = parsed_report(&result.unwrap());
        signatures.push(report.findings[0].problem_signature.clone());
    }
    assert_eq!(signatures[0], signatures[1]);
    assert_eq!(
        signatures[0],
        problem_signature("left-pad", "=1.3.0", "vulnerable-dependency", "")
    );
}

#[tokio::test]
async fn an_exhausted_budget_ends_blocked_without_findings() {
    let script = (0..64)
        .map(|_| tool_turn("read_file", serde_json::json!({"path": "Cargo.lock"})))
        .collect::<Vec<_>>();
    let (_dir, harness, _inputs) = finder(
        ROLE_DEPENDENCY_ADVISORY,
        "vulnerable-dep",
        &serde_json::Value::Array(script).to_string(),
        "auto",
    );
    let (result, events) = run(&harness).await;
    assert!(
        matches!(result, Err(HarnessError::Run(RunError::MaxTurns(12)))),
        "{result:?}"
    );
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, HarnessEvent::ToolStart { name, .. } if name == "report")),
        "a blocked run must not report: {events:?}"
    );
}

#[tokio::test]
async fn finishing_without_report_is_blocked_not_clean() {
    let script = serde_json::json!([{"content": "looks clean, no findings"}]);
    let (_dir, harness, _inputs) = finder(
        ROLE_DEPENDENCY_ADVISORY,
        "clean",
        &script.to_string(),
        "auto",
    );
    let (result, events) = run(&harness).await;
    assert!(
        matches!(
            result,
            Err(HarnessError::Run(RunError::Message(ref message)))
                if message.contains("missing terminal `report`")
        ),
        "{result:?}"
    );
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, HarnessEvent::ToolStart { name, .. } if name == "report")),
        "a blocked run must not report: {events:?}"
    );
}

#[tokio::test]
async fn a_request_timeout_cannot_exceed_the_role_budget() {
    let script = serde_json::json!([{"content": "unused"}]);
    let (_dir, harness, _inputs) = finder(
        ROLE_DEPENDENCY_ADVISORY,
        "clean",
        &script.to_string(),
        "auto",
    );
    let (sink, _rx) = ChannelSink::pair();
    let mut request = RunRequest::new("Review this workspace");
    request.timeout = Some(Duration::from_secs(10_000));
    let result = harness.run_with_events(request, Some(Arc::new(sink))).await;
    assert!(
        matches!(
            result,
            Err(HarnessError::Run(RunError::Message(ref message)))
                if message.contains("exceeds role `dependency-advisory` budget")
        ),
        "{result:?}"
    );
}
