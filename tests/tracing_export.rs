use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::Path;

use shikigami::{Config, Harness, RunRequest, StateRoot};
use tempfile::tempdir;

const SECRET_TASK: &str = "SECRET_TASK_PAYLOAD_284";
const SECRET_INPUT: &str = "SECRET_TOOL_INPUT_284";
const SECRET_OUTPUT: &str = "SECRET_TOOL_OUTPUT_284";

#[tokio::test]
async fn scripted_run_exports_metadata_spans_without_payloads() {
    let dir = tempdir().unwrap();
    let spans_path = dir.path().join("spans.json");
    let mut config = offline_config(dir.path());
    config.tracing.enabled = true;
    config.tracing.exporter = "otlp".into();
    config.tracing.endpoint = Some(spans_path.to_string_lossy().into());
    config.model.script_json = Some(
        r#"[
        {"tool_calls":[{"name":"write_file","args_json":"{\"path\":\"note.txt\",\"content\":\"SECRET_TOOL_OUTPUT_284 SECRET_TOOL_INPUT_284\"}"}],
         "usage":{"input_tokens":11,"output_tokens":7}},
        {"tool_calls":[{"name":"report","args_json":"{\"summary\":\"SECRET_TASK_PAYLOAD_284\",\"success\":true}"}],
         "usage":{"input_tokens":13,"output_tokens":5}}
    ]"#
        .into(),
    );

    let harness = Harness::from_config(config, StateRoot::new(dir.path().join("state"))).unwrap();
    let mut request = RunRequest::new(SECRET_TASK);
    request.logical_operation_id = Some("parent-op-284".into());
    request.keep_workspace = true;
    let result = harness.run(request).await.unwrap();
    assert!(result.success, "{}", result.summary);

    let exported = fs::read_to_string(&spans_path).unwrap();
    assert!(exported.contains("\"name\":\"run\""), "{exported}");
    assert!(exported.contains("\"name\":\"turn\""), "{exported}");
    assert!(exported.contains("\"name\":\"tool\""), "{exported}");
    assert!(exported.contains(&result.run_id), "{exported}");
    assert!(exported.contains("parent-op-284"), "{exported}");
    assert!(exported.contains("\"key\":\"run_id\""), "{exported}");
    assert!(exported.contains("\"key\":\"attempt_id\""), "{exported}");
    assert!(
        exported.contains("\"key\":\"logical_operation_id\""),
        "{exported}"
    );
    assert!(
        exported.contains("\"key\":\"plan_operation_id\""),
        "{exported}"
    );
    assert!(exported.contains("\"key\":\"call_id\""), "{exported}");
    assert!(exported.contains("tool-"), "{exported}");

    let spans = spans_by_name(&exported);
    let [run] = spans["run"].as_slice() else {
        panic!("{exported}")
    };
    assert_eq!(run["shikigami.prompt_id"], result.prompt_id);
    assert_eq!(run["shikigami.termination"], "completed");
    assert!(
        !run.contains_key("shikigami.preset_id"),
        "no preset: {run:?}"
    );
    assert_eq!(run["parent.source"], "none");
    let turns = &spans["turn"];
    let tokens: Vec<_> = turns
        .iter()
        .map(|turn| {
            assert_eq!(turn["gen_ai.request.model"], result.model);
            (
                turn["gen_ai.usage.input_tokens"].as_str(),
                turn["gen_ai.usage.output_tokens"].as_str(),
            )
        })
        .collect();
    assert_eq!(tokens, [("11", "7"), ("13", "5")]);
    let mut tools: Vec<_> = spans["tool"]
        .iter()
        .map(|tool| tool["gen_ai.tool.name"].as_str())
        .collect();
    tools.sort_unstable();
    assert_eq!(tools, ["report", "write_file"]);
    for secret in [SECRET_TASK, SECRET_INPUT, SECRET_OUTPUT] {
        assert!(
            !exported.contains(secret),
            "payload leaked into spans: {secret}\n{exported}"
        );
    }
}

#[tokio::test]
async fn a_resumed_run_writes_into_the_same_trace() {
    let dir = tempdir().unwrap();
    let spans_path = dir.path().join("spans.jsonl");
    let attempt = |script: &str| {
        let mut config = offline_config(dir.path());
        config.tracing.enabled = true;
        config.tracing.exporter = "otlp".into();
        config.tracing.endpoint = Some(spans_path.to_string_lossy().into());
        config.model.script_json = Some(script.into());
        Harness::from_config(config, StateRoot::new(dir.path().join("state"))).unwrap()
    };
    let write = r#"{"tool_calls":[{"name":"write_file","args_json":"{\"path\":\"a.txt\",\"content\":\"a\"}"}]}"#;
    let report = r#"{"tool_calls":[{"name":"report","args_json":"{\"summary\":\"done\",\"success\":true}"}]}"#;

    // The first script runs out after the tool call, leaving a checkpoint.
    attempt(&format!("[{write}]"))
        .run(RunRequest::new("trace"))
        .await
        .unwrap_err();
    let run_id = fs::read_dir(StateRoot::new(dir.path().join("state")).runs_dir())
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .file_name()
        .into_string()
        .unwrap();
    // The scripted model picks its reply by turn, so the resumed script
    // repeats the turn already taken.
    let mut resume = RunRequest::new("");
    resume.resume_run_id = Some(run_id);
    let result = attempt(&format!("[{write},{report}]"))
        .run(resume)
        .await
        .unwrap();
    assert!(result.success, "{}", result.summary);

    let exported = fs::read_to_string(&spans_path).unwrap();
    let spans = exported_spans(&exported);
    let trace_ids: std::collections::BTreeSet<_> = spans
        .iter()
        .map(|span| span["traceId"].as_str().unwrap())
        .collect();
    assert_eq!(trace_ids.len(), 1, "{exported}");
    let runs = spans.iter().filter(|span| span["name"] == "run").count();
    assert_eq!(runs, 2, "one run span per attempt: {exported}");
}

const CALLER_TRACE: &str = "4bf92f3577b34da6a3ce929d0e0e4736";
const CALLER_SPAN: &str = "00f067aa0ba902b7";

fn caller_context(traceparent: &str) -> Option<shikigami::tracing_export::TraceContext> {
    Some(shikigami::tracing_export::TraceContext {
        traceparent: traceparent.into(),
        tracestate: Some("vendor=opaque".into()),
        source: shikigami::tracing_export::TraceSource::Request,
    })
}

#[tokio::test]
async fn a_run_joins_the_callers_trace_and_keeps_it_across_resume() {
    let dir = tempdir().unwrap();
    let spans_path = dir.path().join("spans.jsonl");
    let attempt = |script: &str| {
        let mut config = offline_config(dir.path());
        config.tracing.enabled = true;
        config.tracing.exporter = "otlp".into();
        config.tracing.endpoint = Some(spans_path.to_string_lossy().into());
        config.model.script_json = Some(script.into());
        Harness::from_config(config, StateRoot::new(dir.path().join("state"))).unwrap()
    };
    let write = r#"{"tool_calls":[{"name":"write_file","args_json":"{\"path\":\"a.txt\",\"content\":\"a\"}"}]}"#;
    let report = r#"{"tool_calls":[{"name":"report","args_json":"{\"summary\":\"done\",\"success\":true}"}]}"#;

    let mut first = RunRequest::new("trace");
    first.trace_context = caller_context(&format!("00-{CALLER_TRACE}-{CALLER_SPAN}-01"));
    attempt(&format!("[{write}]")).run(first).await.unwrap_err();
    let run_id = fs::read_dir(StateRoot::new(dir.path().join("state")).runs_dir())
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .file_name()
        .into_string()
        .unwrap();
    // The resume carries no context of its own: the checkpoint supplies it.
    let mut resume = RunRequest::new("");
    resume.resume_run_id = Some(run_id);
    let result = attempt(&format!("[{write},{report}]"))
        .run(resume)
        .await
        .unwrap();
    assert!(result.success, "{}", result.summary);

    let exported = fs::read_to_string(&spans_path).unwrap();
    let spans = exported_spans(&exported);
    assert!(
        spans.iter().all(|span| span["traceId"] == CALLER_TRACE),
        "{exported}"
    );
    let runs: Vec<_> = spans.iter().filter(|span| span["name"] == "run").collect();
    assert_eq!(runs.len(), 2, "{exported}");
    for run in runs {
        assert_eq!(run["parentSpanId"], CALLER_SPAN);
    }
    for run in &spans_by_name(&exported)["run"] {
        assert_eq!(run["parent.source"], "request");
    }
    assert!(
        !exported.contains("vendor=opaque"),
        "tracestate is never an attribute"
    );
}

#[tokio::test]
async fn a_malformed_traceparent_starts_the_runs_own_trace() {
    let dir = tempdir().unwrap();
    let spans_path = dir.path().join("spans.jsonl");
    let mut config = offline_config(dir.path());
    config.tracing.enabled = true;
    config.tracing.exporter = "otlp".into();
    config.tracing.endpoint = Some(spans_path.to_string_lossy().into());
    let harness = Harness::from_config(config, StateRoot::new(dir.path().join("state"))).unwrap();
    let mut request = RunRequest::new("demo");
    request.trace_context = caller_context(&format!("00-{CALLER_TRACE}-not-a-span-01"));
    let result = harness.run(request).await.unwrap();
    assert!(result.success, "{}", result.summary);

    let exported = fs::read_to_string(&spans_path).unwrap();
    let spans = exported_spans(&exported);
    let run = spans.iter().find(|span| span["name"] == "run").unwrap();
    assert_ne!(run["traceId"], CALLER_TRACE);
    assert_eq!(run["parentSpanId"], "");
    assert_eq!(spans_by_name(&exported)["run"][0]["parent.source"], "none");
}

/// Run a scripted `bash` call that saves the `TRACEPARENT` it sees, and
/// return it with the span export (empty when tracing is off).
async fn traceparent_seen_by_bash(tracing: bool) -> (String, String) {
    let dir = tempdir().unwrap();
    let spans_path = dir.path().join("spans.jsonl");
    let seen_path = dir.path().join("seen");
    let mut config = offline_config(dir.path());
    config.tools.enabled = vec!["bash".into(), "report".into()];
    config.tracing.enabled = tracing;
    config.tracing.exporter = "otlp".into();
    config.tracing.endpoint = Some(spans_path.to_string_lossy().into());
    let command = format!("printf %s \"$TRACEPARENT\" > '{}'", seen_path.display());
    let bash = serde_json::json!({"tool_calls": [{
        "name": "bash",
        "args_json": serde_json::json!({"command": command}).to_string(),
    }]});
    let report = r#"{"tool_calls":[{"name":"report","args_json":"{\"summary\":\"done\",\"success\":true}"}]}"#;
    config.model.script_json = Some(format!("[{bash},{report}]"));
    let harness = Harness::from_config(config, StateRoot::new(dir.path().join("state"))).unwrap();
    let result = harness.run(RunRequest::new("trace")).await.unwrap();
    assert!(result.success, "{}", result.summary);
    let exported = fs::read_to_string(&spans_path).unwrap_or_default();
    (fs::read_to_string(&seen_path).unwrap(), exported)
}

#[tokio::test]
async fn bash_sees_the_tool_spans_traceparent() {
    let (seen, exported) = traceparent_seen_by_bash(true).await;
    let spans = exported_spans(&exported);
    let tool = spans
        .iter()
        .find(|span| span["name"] == "tool")
        .unwrap_or_else(|| panic!("{exported}"));
    let expected = format!(
        "00-{}-{}-01",
        tool["traceId"].as_str().unwrap(),
        tool["spanId"].as_str().unwrap()
    );
    assert_eq!(seen, expected);
}

#[tokio::test]
async fn bash_sees_no_traceparent_while_tracing_is_off() {
    let (seen, exported) = traceparent_seen_by_bash(false).await;
    assert_eq!(seen, "");
    assert_eq!(exported, "");
}

#[tokio::test]
async fn an_unreachable_collector_does_not_fail_the_run() {
    let dir = tempdir().unwrap();
    // A port that was just free: nothing listens there.
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let mut config = offline_config(dir.path());
    config.tracing.enabled = true;
    config.tracing.exporter = "otlp".into();
    config.tracing.endpoint = Some(format!("http://127.0.0.1:{port}"));

    let harness = Harness::from_config(config, StateRoot::new(dir.path().join("state"))).unwrap();
    let result = harness.run(RunRequest::new("demo")).await.unwrap();
    assert!(result.success, "{}", result.summary);
}

#[tokio::test]
async fn disabled_exporter_leaves_offline_run_unchanged() {
    let dir = tempdir().unwrap();
    let spans_path = dir.path().join("must-not-exist.json");
    let mut config = offline_config(dir.path());
    config.tracing.enabled = false;
    config.tracing.exporter = "otlp".into();
    config.tracing.endpoint = Some(spans_path.to_string_lossy().into());

    let harness = Harness::from_config(config, StateRoot::new(dir.path().join("state"))).unwrap();
    let report = harness.doctor();
    assert!(report.ok);
    assert!(
        report
            .lines
            .iter()
            .any(|line| line.contains("tracing:    disabled")),
        "{:?}",
        report.lines
    );

    let mut request = RunRequest::new("demo");
    request.keep_workspace = true;
    let result = harness.run(request).await.unwrap();
    assert!(result.success);
    assert!(
        !spans_path.exists(),
        "disabled exporter wrote {}",
        spans_path.display()
    );
}

fn offline_config(root: &Path) -> Config {
    let mut config = Config::default();
    config.governance.adapter = "local".into();
    config.model.adapter = "scripted".into();
    config.events.adapter = "none".into();
    config.workspace.root = root.join("ws").to_string_lossy().into();
    config
}

/// Every span in an OTLP JSON-lines export, in export order.
fn exported_spans(exported: &str) -> Vec<serde_json::Value> {
    exported
        .lines()
        .flat_map(|line| {
            let payload: serde_json::Value = serde_json::from_str(line).unwrap();
            payload["resourceSpans"][0]["scopeSpans"][0]["spans"]
                .as_array()
                .unwrap()
                .clone()
        })
        .collect()
}

/// Attributes of each exported span, grouped by span name in export order.
fn spans_by_name(exported: &str) -> HashMap<String, Vec<BTreeMap<String, String>>> {
    let mut spans: HashMap<String, Vec<_>> = HashMap::new();
    for span in exported_spans(exported) {
        let attributes = span["attributes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|attribute| {
                (
                    attribute["key"].as_str().unwrap().to_owned(),
                    attribute["value"]["stringValue"]
                        .as_str()
                        .unwrap()
                        .to_owned(),
                )
            })
            .collect();
        spans
            .entry(span["name"].as_str().unwrap().to_owned())
            .or_default()
            .push(attributes);
    }
    spans
}
