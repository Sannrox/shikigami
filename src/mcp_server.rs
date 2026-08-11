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
use tokio::io::{BufReader, stdin, stdout};

use crate::harness::Harness;
use crate::identity::{PRODUCT, VERSION};
use crate::mcp::framing;
use crate::model::TokenUsage;
use crate::run::{ParkInfo, RunRequest, RunResult};

mod background_run;

use background_run::BackgroundRunLifecycle;

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
    background: Arc<BackgroundRunLifecycle>,
}

impl McpServerState {
    pub fn new(harness: Harness) -> Arc<Self> {
        let harness = Arc::new(harness);
        Arc::new(Self {
            background: BackgroundRunLifecycle::new(Arc::clone(&harness)),
            harness,
        })
    }
}

/// Serve MCP on process stdio until EOF.
pub async fn run_stdio(harness: Harness) -> Result<(), String> {
    let state = McpServerState::new(harness);
    let mut reader = BufReader::new(stdin());
    let mut writer = stdout();
    loop {
        let msg = match framing::read(&mut reader).await {
            Ok(m) => m,
            Err(e) if e == "eof" => break,
            Err(e) => return Err(e),
        };
        if let Some(resp) = handle_message(&state, msg).await {
            framing::write(&mut writer, &resp).await?;
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
            state.background.start(build_request(&args)?).await?;
            Ok(tool_text_result(
                r#"{"phase":"running","message":"run started; poll with run_status or run_wait"}"#,
                false,
            ))
        }
        "run_status" => {
            let text = state.background.status_json().await?;
            Ok(tool_text_result(&text, false))
        }
        "run_wait" => {
            let timeout_secs = args.get("timeout_secs").and_then(|v| v.as_u64());
            let text = state.background.wait(timeout_secs).await?;
            let is_err = text.contains("\"success\":false") || text.contains("error");
            Ok(tool_text_result(
                &text,
                is_err && text.contains("\"phase\":\"finished\""),
            ))
        }
        other => Err(format!("unknown tool: {other}")),
    }
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
    if resume_run_id
        .as_deref()
        .is_some_and(|run_id| !crate::checkpoint::is_safe_run_id(run_id))
    {
        return Err("resume_run_id must be an opaque ASCII run id".into());
    }
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

/// Encode a JSON-RPC message with Content-Length framing (for tests/clients).
pub fn frame_message(msg: &Value) -> Result<Vec<u8>, String> {
    framing::encode(msg)
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

    #[test]
    fn mcp_resume_id_rejects_path_components() {
        let err = build_request(&json!({"resume_run_id": "../other"})).unwrap_err();
        assert!(err.contains("opaque ASCII run id"), "{err}");
        build_request(&json!({"resume_run_id": "abc-123"})).unwrap();
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
    async fn wait_observes_completion_published_before_registration() {
        let directory = tempdir().unwrap();
        let state = McpServerState::new(local_scripted_harness(directory.path()));
        state
            .background
            .start(build_request(&json!({"task": "fast run"})).unwrap())
            .await
            .unwrap();

        loop {
            let status: Value =
                serde_json::from_str(&state.background.status_json().await.unwrap()).unwrap();
            if status["phase"] == "finished" {
                break;
            }
            tokio::task::yield_now().await;
        }

        let status: Value =
            serde_json::from_str(&state.background.wait(Some(1)).await.unwrap()).unwrap();
        assert_eq!(status["phase"], "finished");
        assert_eq!(status["result"]["success"], true);
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
