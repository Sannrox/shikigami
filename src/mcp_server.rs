//! Optional MCP **server** host: expose `doctor`, `run`, and async run poll tools over stdio.
//!
//! This is a thin host for IDE agents and automation. Library embed remains
//! the preferred in-process path (ADR 0001). Not a multi-tenant control plane;
//! tenkai stays delivery-only.
//!
//! Transport: JSON-RPC 2.0 with `Content-Length` framing on **stdio only**
//! (no network bind in v1).

use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, stdin, stdout};
use tokio::sync::{Mutex, Notify};

use crate::events::{ChannelSink, HarnessEvent};
use crate::harness::Harness;
use crate::identity::{PRODUCT, VERSION};
use crate::model::TokenUsage;
use crate::run::{ParkInfo, RunRequest, RunResult};

/// Stable summary returned by the MCP `run` / async run tools.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct McpRunSummary {
    pub run_id: String,
    pub success: bool,
    pub summary: String,
    pub turns: u32,
    pub workspace: String,
    pub termination: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub park: Option<ParkInfo>,
    pub prompt_id: String,
    pub usage: TokenUsage,
}

impl From<&RunResult> for McpRunSummary {
    fn from(r: &RunResult) -> Self {
        Self {
            run_id: r.run_id.clone(),
            success: r.success,
            summary: r.summary.clone(),
            turns: r.turns,
            workspace: r.workspace.display().to_string(),
            termination: r.termination.as_str().to_string(),
            park: r.park.clone(),
            prompt_id: r.prompt_id.clone(),
            usage: r.usage,
        }
    }
}

/// Session state for stdio MCP server (single-flight async run).
pub struct McpServerState {
    harness: Arc<Harness>,
    job: Mutex<JobSlot>,
}

#[derive(Default)]
struct JobSlot {
    phase: String, // idle | running | finished
    events: Vec<String>,
    result: Option<Result<McpRunSummary, String>>,
    done: Option<Arc<Notify>>,
}

impl McpServerState {
    pub fn new(harness: Harness) -> Arc<Self> {
        Arc::new(Self {
            harness: Arc::new(harness),
            job: Mutex::new(JobSlot {
                phase: "idle".into(),
                events: Vec::new(),
                result: None,
                done: None,
            }),
        })
    }
}

/// Serve MCP on process stdio until EOF.
pub async fn run_stdio(harness: Harness) -> Result<(), String> {
    let state = McpServerState::new(harness);
    let mut reader = BufReader::new(stdin());
    let mut writer = stdout();
    loop {
        let msg = match read_message(&mut reader).await {
            Ok(m) => m,
            Err(e) if e == "eof" => break,
            Err(e) => return Err(e),
        };
        if let Some(resp) = handle_message(&state, msg).await {
            write_message(&mut writer, &resp).await?;
        }
    }
    Ok(())
}

/// Handle one JSON-RPC message. Returns `None` for notifications (no response).
pub async fn handle_message(state: &Arc<McpServerState>, msg: Value) -> Option<Value> {
    let method = msg.get("method").and_then(|m| m.as_str()).unwrap_or("");
    let id = msg.get("id").cloned()?;

    let params = msg.get("params").cloned().unwrap_or(Value::Null);
    let result = match method {
        "initialize" => initialize_result(),
        "ping" => json!({}),
        "tools/list" => tools_list_result(),
        "tools/call" => match call_tool(state, &params).await {
            Ok(v) => v,
            Err(text) => tool_error_result(&text),
        },
        other => {
            return Some(json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": {
                    "code": -32601,
                    "message": format!("Method not found: {other}"),
                }
            }));
        }
    };

    Some(json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": result,
    }))
}

fn initialize_result() -> Value {
    json!({
        "protocolVersion": "2024-11-05",
        "capabilities": {
            "tools": {}
        },
        "serverInfo": {
            "name": PRODUCT,
            "version": VERSION,
        },
        "instructions": "Shikigami MCP host: doctor, run (blocking), run_start/run_status/run_wait (async poll). Stdio only. Not a multi-tenant control plane."
    })
}

fn tools_list_result() -> Value {
    json!({
        "tools": [
            {
                "name": "doctor",
                "description": "Check effective settings, adapters, and plane reachability. Secrets are redacted as in the CLI doctor JSON contract.",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                    "additionalProperties": false
                }
            },
            {
                "name": "run",
                "description": "Blocking harness run. Prefer run_start + run_status/run_wait for long tasks.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "task": { "type": "string" },
                        "keep_workspace": { "type": "boolean" },
                        "timeout_secs": { "type": "integer", "minimum": 1 },
                        "resume_run_id": { "type": "string" },
                        "resume_answer": { "type": "string" }
                    },
                    "additionalProperties": false
                }
            },
            {
                "name": "run_start",
                "description": "Start a harness run in the background (single-flight). Poll with run_status or run_wait.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "task": { "type": "string" },
                        "keep_workspace": { "type": "boolean" },
                        "timeout_secs": { "type": "integer", "minimum": 1 },
                        "resume_run_id": { "type": "string" },
                        "resume_answer": { "type": "string" }
                    },
                    "additionalProperties": false
                }
            },
            {
                "name": "run_status",
                "description": "Status of the background run started by run_start (phase, recent events, result if finished).",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                    "additionalProperties": false
                }
            },
            {
                "name": "run_wait",
                "description": "Wait until the background run finishes. Optional timeout_secs.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "timeout_secs": { "type": "integer", "minimum": 1 }
                    },
                    "additionalProperties": false
                }
            }
        ]
    })
}

async fn call_tool(state: &Arc<McpServerState>, params: &Value) -> Result<Value, String> {
    let name = params
        .get("name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "tools/call missing name".to_string())?;
    let args = params.get("arguments").cloned().unwrap_or(json!({}));
    match name {
        "doctor" => {
            let report = state.harness.doctor_async().await;
            let text = serde_json::to_string_pretty(&report).map_err(|e| e.to_string())?;
            Ok(tool_text_result(&text, !report.ok))
        }
        "run" => {
            let summary = run_tool(&state.harness, &args).await?;
            let text = serde_json::to_string_pretty(&summary).map_err(|e| e.to_string())?;
            Ok(tool_text_result(&text, !summary.success))
        }
        "run_start" => {
            start_background_run(state, &args).await?;
            Ok(tool_text_result(
                r#"{"phase":"running","message":"run started; poll with run_status or run_wait"}"#,
                false,
            ))
        }
        "run_status" => {
            let text = status_json(state).await?;
            Ok(tool_text_result(&text, false))
        }
        "run_wait" => {
            let timeout_secs = args.get("timeout_secs").and_then(|v| v.as_u64());
            let text = wait_for_run(state, timeout_secs).await?;
            let is_err = text.contains("\"success\":false") || text.contains("error");
            Ok(tool_text_result(
                &text,
                is_err && text.contains("\"phase\":\"finished\""),
            ))
        }
        other => Err(format!("unknown tool: {other}")),
    }
}

async fn start_background_run(state: &Arc<McpServerState>, args: &Value) -> Result<(), String> {
    let mut slot = state.job.lock().await;
    if slot.phase == "running" {
        return Err("a background run is already in progress (single-flight)".into());
    }
    let request = build_request(args)?;
    let done = Arc::new(Notify::new());
    *slot = JobSlot {
        phase: "running".into(),
        events: vec!["status=starting".into()],
        result: None,
        done: Some(Arc::clone(&done)),
    };
    drop(slot);

    let harness = Arc::clone(&state.harness);
    let state_job = Arc::clone(state);
    tokio::spawn(async move {
        let (sink, rx) = ChannelSink::pair();
        let run_fut = harness.run_with_events(request, Some(Arc::new(sink)));
        let drain = tokio::task::spawn_blocking(move || {
            let mut lines = Vec::new();
            while let Ok(ev) = rx.recv() {
                lines.push(format_event(&ev));
            }
            lines
        });

        let result = run_fut.await;
        let event_lines = drain.await.unwrap_or_default();

        let mut slot = state_job.job.lock().await;
        slot.events.extend(event_lines);
        slot.phase = "finished".into();
        slot.result = Some(match result {
            Ok(r) => Ok(McpRunSummary::from(&r)),
            Err(e) => Err(e.to_string()),
        });
        if let Some(d) = slot.done.take() {
            d.notify_waiters();
        }
        // Restore done notify for waiters that race
        slot.done = Some(Arc::new(Notify::new()));
        slot.done.as_ref().unwrap().notify_waiters();
    });
    Ok(())
}

fn format_event(ev: &HarnessEvent) -> String {
    match ev {
        HarnessEvent::Status { status } => format!("status={status}"),
        HarnessEvent::ToolStart { name, .. } => format!("tool_start={name}"),
        HarnessEvent::ToolEnd { name, ok, .. } => format!("tool_end={name} ok={ok}"),
        HarnessEvent::ModelTurn { turn, .. } => format!("model_turn={turn}"),
        HarnessEvent::RunFinished {
            run_id,
            success,
            summary,
        } => format!("run_finished id={run_id} success={success} summary={summary}"),
        HarnessEvent::Prompt { prompt_id } => format!("prompt={prompt_id}"),
        HarnessEvent::ContextCompacted { before, after } => {
            format!("compacted before={before} after={after}")
        }
        HarnessEvent::TodosUpdated { item_count, .. } => {
            format!("todos item_count={item_count}")
        }
        HarnessEvent::Message { level, text } => format!("message[{level}]={text}"),
    }
}

async fn status_json(state: &Arc<McpServerState>) -> Result<String, String> {
    let slot = state.job.lock().await;
    let recent: Vec<&String> = slot.events.iter().rev().take(30).collect();
    let recent: Vec<&String> = recent.into_iter().rev().collect();
    let body = json!({
        "phase": slot.phase,
        "events": recent,
        "result": match &slot.result {
            Some(Ok(s)) => json!(s),
            Some(Err(e)) => json!({"error": e}),
            None => Value::Null,
        }
    });
    serde_json::to_string_pretty(&body).map_err(|e| e.to_string())
}

async fn wait_for_run(
    state: &Arc<McpServerState>,
    timeout_secs: Option<u64>,
) -> Result<String, String> {
    let notify = {
        let slot = state.job.lock().await;
        if slot.phase == "idle" {
            return Err("no background run (call run_start first)".into());
        }
        if slot.phase == "finished" {
            drop(slot);
            return status_json(state).await;
        }
        slot.done
            .clone()
            .ok_or_else(|| "run missing completion notify".to_string())?
    };
    if let Some(secs) = timeout_secs {
        match tokio::time::timeout(Duration::from_secs(secs), notify.notified()).await {
            Ok(()) => {}
            Err(_) => {
                return Ok(json!({
                    "phase": "running",
                    "error": format!("timed out waiting after {secs}s")
                })
                .to_string());
            }
        }
    } else {
        notify.notified().await;
    }
    status_json(state).await
}

async fn run_tool(harness: &Harness, args: &Value) -> Result<McpRunSummary, String> {
    let request = build_request(args)?;
    let result = harness.run(request).await.map_err(|e| e.to_string())?;
    Ok(McpRunSummary::from(&result))
}

fn build_request(args: &Value) -> Result<RunRequest, String> {
    let resume_run_id = args
        .get("resume_run_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let task = args
        .get("task")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    if resume_run_id.is_none() && task.is_empty() {
        return Err("task is required unless resume_run_id is set".into());
    }
    let keep_workspace = args
        .get("keep_workspace")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let timeout = args
        .get("timeout_secs")
        .and_then(|v| v.as_u64())
        .map(Duration::from_secs);
    let resume_answer = args
        .get("resume_answer")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let mut request = RunRequest::new(task);
    request.keep_workspace = keep_workspace;
    request.timeout = timeout;
    request.resume_run_id = resume_run_id;
    request.resume_answer = resume_answer;
    Ok(request)
}

fn tool_text_result(text: &str, is_error: bool) -> Value {
    json!({
        "content": [{
            "type": "text",
            "text": text
        }],
        "isError": is_error
    })
}

fn tool_error_result(message: &str) -> Value {
    tool_text_result(message, true)
}

async fn write_message<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    msg: &Value,
) -> Result<(), String> {
    let body = serde_json::to_vec(msg).map_err(|e| e.to_string())?;
    let header = format!("Content-Length: {}\r\n\r\n", body.len());
    writer
        .write_all(header.as_bytes())
        .await
        .map_err(|e| e.to_string())?;
    writer.write_all(&body).await.map_err(|e| e.to_string())?;
    writer.flush().await.map_err(|e| e.to_string())?;
    Ok(())
}

async fn read_message<R: AsyncBufReadExt + Unpin>(reader: &mut R) -> Result<Value, String> {
    let mut content_length = None;
    loop {
        let mut line = String::new();
        let n = reader
            .read_line(&mut line)
            .await
            .map_err(|e| e.to_string())?;
        if n == 0 {
            return Err("eof".into());
        }
        let line = line.trim_end();
        if line.is_empty() {
            break;
        }
        if let Some(rest) = line.strip_prefix("Content-Length:") {
            content_length = rest.trim().parse::<usize>().ok();
        }
    }
    let len = content_length.ok_or_else(|| "mcp missing Content-Length".to_string())?;
    let mut buf = vec![0u8; len];
    reader
        .read_exact(&mut buf)
        .await
        .map_err(|e| e.to_string())?;
    serde_json::from_slice(&buf).map_err(|e| e.to_string())
}

/// Encode a JSON-RPC message with Content-Length framing (for tests/clients).
pub fn frame_message(msg: &Value) -> Result<Vec<u8>, String> {
    let body = serde_json::to_vec(msg).map_err(|e| e.to_string())?;
    let header = format!("Content-Length: {}\r\n\r\n", body.len());
    let mut out = header.into_bytes();
    out.extend_from_slice(&body);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::state::StateRoot;
    use tempfile::tempdir;

    fn local_scripted_harness(dir: &std::path::Path) -> Harness {
        let state = StateRoot::new(dir.join("state"));
        let mut config = Config::default();
        config.governance.adapter = "local".into();
        config.model.adapter = "scripted".into();
        config.events.adapter = "none".into();
        config.workspace.root = dir.join("ws-root").to_string_lossy().into();
        Harness::from_config(config, state).unwrap()
    }

    #[tokio::test]
    async fn initialize_and_list_tools() {
        let dir = tempdir().unwrap();
        let state = McpServerState::new(local_scripted_harness(dir.path()));

        let list = handle_message(
            &state,
            json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/list",
                "params": {}
            }),
        )
        .await
        .unwrap();
        let tools = list["result"]["tools"].as_array().unwrap();
        let names: Vec<&str> = tools
            .iter()
            .filter_map(|t| t.get("name").and_then(|n| n.as_str()))
            .collect();
        assert!(names.contains(&"doctor"));
        assert!(names.contains(&"run"));
        assert!(names.contains(&"run_start"));
        assert!(names.contains(&"run_status"));
        assert!(names.contains(&"run_wait"));
    }

    #[tokio::test]
    async fn doctor_and_scripted_run_offline() {
        let dir = tempdir().unwrap();
        let state = McpServerState::new(local_scripted_harness(dir.path()));

        let doctor = handle_message(
            &state,
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {"name": "doctor", "arguments": {}}
            }),
        )
        .await
        .unwrap();
        let text = doctor["result"]["content"][0]["text"].as_str().unwrap();
        let report: Value = serde_json::from_str(text).unwrap();
        assert_eq!(report["ok"], true);

        let run = handle_message(
            &state,
            json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "run",
                    "arguments": {
                        "task": "demo via mcp",
                        "keep_workspace": true
                    }
                }
            }),
        )
        .await
        .unwrap();
        assert_eq!(run["result"]["isError"], false);
        let text = run["result"]["content"][0]["text"].as_str().unwrap();
        let summary: McpRunSummary = serde_json::from_str(text).unwrap();
        assert!(summary.success);
    }

    #[tokio::test]
    async fn async_run_start_status_wait() {
        let dir = tempdir().unwrap();
        let state = McpServerState::new(local_scripted_harness(dir.path()));

        let start = handle_message(
            &state,
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {
                    "name": "run_start",
                    "arguments": {"task": "async demo", "keep_workspace": true}
                }
            }),
        )
        .await
        .unwrap();
        assert_eq!(start["result"]["isError"], false);

        let wait = handle_message(
            &state,
            json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {"name": "run_wait", "arguments": {"timeout_secs": 30}}
            }),
        )
        .await
        .unwrap();
        let text = wait["result"]["content"][0]["text"].as_str().unwrap();
        let status: Value = serde_json::from_str(text).unwrap();
        assert_eq!(status["phase"], "finished");
        assert_eq!(status["result"]["success"], true);
        // Events should have been recorded for multi-turn scripted run
        let events = status["events"].as_array().unwrap();
        assert!(!events.is_empty(), "{status}");
    }

    #[tokio::test]
    async fn run_requires_task_without_resume() {
        let dir = tempdir().unwrap();
        let state = McpServerState::new(local_scripted_harness(dir.path()));
        let resp = handle_message(
            &state,
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {"name": "run", "arguments": {}}
            }),
        )
        .await
        .unwrap();
        assert_eq!(resp["result"]["isError"], true);
    }

    #[tokio::test]
    async fn notification_has_no_response() {
        let dir = tempdir().unwrap();
        let state = McpServerState::new(local_scripted_harness(dir.path()));
        let resp = handle_message(
            &state,
            json!({
                "jsonrpc": "2.0",
                "method": "notifications/initialized",
                "params": {}
            }),
        )
        .await;
        assert!(resp.is_none());
    }
}
