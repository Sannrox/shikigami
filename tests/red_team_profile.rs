//! The red-team probe author (`examples/red-team.toml`, #466): it reads an RPC
//! surface diff and past bug reports, emits `sekai.probe-spec/v1` specs only
//! through its final `report`, holds no network or RPC tool, and ends blocked
//! when its budget runs out. The model is scripted; a real-model run is
//! sekai-chisei#1622's evidence.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::Deserialize;
use shikigami::run::RunError;
use shikigami::{
    ChannelSink, Config, Harness, HarnessError, HarnessEvent, RunRequest, RunTermination, StateRoot,
};
use tempfile::TempDir;

const SURFACE_DIFF: &str = include_str!("fixtures/red_team/surface.diff");
const BUG_REPORT: &str = include_str!("fixtures/red_team/bugs/resolved-comments-leak.md");

fn repo() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
}

/// The shipped red-team settings with a scripted model, reading a private
/// copy of the fixture inputs in place.
fn red_team(script: &str) -> (TempDir, Harness, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let inputs = dir.path().join("inputs");
    std::fs::create_dir_all(inputs.join("bugs")).unwrap();
    std::fs::write(inputs.join("surface.diff"), SURFACE_DIFF).unwrap();
    std::fs::write(inputs.join("bugs/resolved-comments-leak.md"), BUG_REPORT).unwrap();

    let mut config = Config::load(repo().join("examples/red-team.toml")).unwrap();
    config.model.adapter = "scripted".into();
    config.model.script_json = Some(script.into());
    config.events.adapter = "none".into();
    config.workspace.root = inputs.to_string_lossy().into();
    config.context.skills_root = Some(
        repo()
            .join("examples/red-team/skills")
            .to_string_lossy()
            .into(),
    );
    let state = StateRoot::new(dir.path().join("state"));
    let harness = Harness::from_config(config, state).unwrap();
    (dir, harness, inputs)
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
            RunRequest::new("Propose probe specs for surface.diff"),
            Some(Arc::new(sink)),
        )
        .await;
    let events = std::iter::from_fn(|| rx.try_recv().ok()).collect();
    (result, events)
}

// A test-side mirror of `sekai.probe-spec/v1`. The guarded runner in
// sekai-chisei is the authority and re-validates every spec it loads.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProbeSpec {
    schema: String,
    id: String,
    #[serde(rename = "as")]
    principal: Principal,
    call: Call,
    expect: Expect,
}

#[derive(Debug, PartialEq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Principal {
    Tenant,
    Anonymous,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "rpc", rename_all = "PascalCase", deny_unknown_fields)]
enum Call {
    GetObject {},
    GetLinks {
        #[serde(default)]
        relation: String,
        #[serde(default)]
        direction: String,
    },
    GetLinkedObjects {
        #[serde(default)]
        relation: String,
        #[serde(default)]
        direction: String,
    },
    ListObjectComments {
        #[serde(default)]
        include_resolved: bool,
    },
    GetLineage {
        #[serde(default = "default_lineage_nodes")]
        max_nodes: i32,
    },
}

fn default_lineage_nodes() -> i32 {
    10
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Expect {
    codes: Vec<String>,
    #[serde(default)]
    reason: Option<String>,
}

const DENIAL_CODES: [&str; 5] = [
    "Unauthenticated",
    "PermissionDenied",
    "NotFound",
    "FailedPrecondition",
    "InvalidArgument",
];

fn is_name(value: &str, max: usize) -> bool {
    value.len() <= max
        && value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

impl ProbeSpec {
    fn validate(&self) -> Result<(), String> {
        let id = &self.id;
        if self.schema != "sekai.probe-spec/v1" {
            return Err(format!("{id}: schema {}", self.schema));
        }
        if id.is_empty() || !is_name(id, 64) {
            return Err(format!("bad id {id:?}"));
        }
        if self.expect.codes.is_empty()
            || !self
                .expect
                .codes
                .iter()
                .all(|code| DENIAL_CODES.contains(&code.as_str()))
        {
            return Err(format!("{id}: codes {:?}", self.expect.codes));
        }
        if self
            .expect
            .reason
            .as_ref()
            .is_some_and(|reason| reason.is_empty() || reason.len() > 200)
        {
            return Err(format!("{id}: reason length"));
        }
        match &self.call {
            Call::GetLinks {
                relation,
                direction,
            }
            | Call::GetLinkedObjects {
                relation,
                direction,
            } if !is_name(relation, 64)
                || !["", "outgoing", "incoming"].contains(&direction.as_str()) =>
            {
                Err(format!("{id}: link options"))
            }
            Call::GetLineage { max_nodes } if !(1..=100).contains(max_nodes) => {
                Err(format!("{id}: max_nodes {max_nodes}"))
            }
            _ => Ok(()),
        }
    }
}

/// The report summary the red-team skill asks for.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RedTeamReport {
    specs: Vec<serde_json::Value>,
    #[serde(default)]
    unsupported: Vec<Unsupported>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Unsupported {
    rpc: String,
    why: String,
}

/// Every spec valid and no id used twice, as the guarded runner requires.
fn validated_specs(report: &RedTeamReport) -> Vec<ProbeSpec> {
    let specs = report
        .specs
        .iter()
        .map(|value| serde_json::from_value::<ProbeSpec>(value.clone()).unwrap())
        .collect::<Vec<_>>();
    for spec in &specs {
        spec.validate().unwrap();
    }
    let mut ids = specs
        .iter()
        .map(|spec| spec.id.as_str())
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len(), specs.len(), "duplicate spec ids");
    specs
}

fn report_turn(summary: &serde_json::Value) -> serde_json::Value {
    let args = serde_json::json!({"summary": summary.to_string(), "success": true});
    serde_json::json!({"tool_calls": [{"name": "report", "args_json": args.to_string()}]})
}

fn tool_turn(name: &str, args: serde_json::Value) -> serde_json::Value {
    serde_json::json!({"tool_calls": [{"name": name, "args_json": args.to_string()}]})
}

#[tokio::test]
async fn a_surface_diff_adding_an_rpc_yields_valid_specs_for_it_in_one_run() {
    let summary = serde_json::json!({
        "specs": [
            {
                "schema": "sekai.probe-spec/v1",
                "id": "list-object-comments-resolved-cross-tenant",
                "as": "tenant",
                "call": {"rpc": "ListObjectComments", "include_resolved": true},
                "expect": {"codes": ["PermissionDenied", "NotFound"]}
            },
            {
                "schema": "sekai.probe-spec/v1",
                "id": "list-object-comments-anonymous",
                "as": "anonymous",
                "call": {"rpc": "ListObjectComments"},
                "expect": {"codes": ["Unauthenticated", "PermissionDenied"]}
            }
        ],
        "unsupported": [
            {"rpc": "SekaiService.AddObjectComment", "why": "a write; probe specs are read-only"}
        ]
    });
    let script = serde_json::json!([
        tool_turn("read_file", serde_json::json!({"path": "surface.diff"})),
        tool_turn(
            "grep",
            serde_json::json!({"pattern": "include_resolved", "path": "bugs"})
        ),
        report_turn(&summary),
    ]);
    let (_dir, harness, _inputs) = red_team(&script.to_string());
    let (result, events) = run(&harness).await;
    let result = result.unwrap();

    assert_eq!(result.termination, RunTermination::Completed);
    assert!(result.success);
    assert!(
        events.iter().any(|event| matches!(
            event,
            HarnessEvent::Message { text, .. } if text.starts_with("skill red-team digest=")
        )),
        "the red-team skill pack must load: {events:?}"
    );
    for tool in ["read_file", "grep"] {
        assert!(
            events.iter().any(|event| matches!(
                event,
                HarnessEvent::ToolEnd { name, ok: true, .. } if name == tool
            )),
            "{tool} must read the inputs: {events:?}"
        );
    }

    let report: RedTeamReport = serde_json::from_str(&result.summary).unwrap();
    let specs = validated_specs(&report);
    let comment_probes = specs
        .iter()
        .filter(|spec| matches!(spec.call, Call::ListObjectComments { .. }))
        .collect::<Vec<_>>();
    assert!(!comment_probes.is_empty(), "no spec for the added RPC");
    assert!(
        comment_probes
            .iter()
            .any(|spec| spec.principal == Principal::Anonymous)
    );
    // The bug report steers the cross-tenant probe at resolved comments.
    assert!(
        comment_probes
            .iter()
            .any(|spec| spec.principal == Principal::Tenant
                && matches!(
                    spec.call,
                    Call::ListObjectComments {
                        include_resolved: true
                    }
                ))
    );
    assert_eq!(report.unsupported.len(), 1);
    assert_eq!(report.unsupported[0].rpc, "SekaiService.AddObjectComment");
    assert!(!report.unsupported[0].why.is_empty());
}

#[tokio::test]
async fn the_profile_holds_no_network_or_rpc_tool_and_journals_attempts() {
    let attempts = ["web_fetch", "bash", "write_file", "child_run", "escalate"];
    let mut script = attempts
        .iter()
        .map(|tool| tool_turn(tool, serde_json::json!({})))
        .collect::<Vec<_>>();
    script.push(report_turn(
        &serde_json::json!({"specs": [], "unsupported": []}),
    ));
    let (dir, harness, inputs) = red_team(&serde_json::Value::Array(script).to_string());

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
        // The durable journal is redacted: the refusal shows as a failed call.
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
    assert_eq!(left, ["bugs", "surface.diff"], "inputs must stay untouched");
}

#[tokio::test]
async fn an_exhausted_budget_ends_blocked_without_specs() {
    // More reads than `run.max_turns` allows and no report.
    let script = (0..64)
        .map(|_| tool_turn("read_file", serde_json::json!({"path": "surface.diff"})))
        .collect::<Vec<_>>();
    let (_dir, harness, _inputs) = red_team(&serde_json::Value::Array(script).to_string());
    let (result, events) = run(&harness).await;

    // Blocked: an error with no result, so there is no summary to take specs from.
    assert!(
        matches!(result, Err(HarnessError::Run(RunError::MaxTurns(16)))),
        "{result:?}"
    );
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, HarnessEvent::ToolStart { name, .. } if name == "report")),
        "a blocked run must not report: {events:?}"
    );
}
