//! Optional MCP **server** host: expose `doctor` and `run` over stdio.
//!
//! This is a thin host for IDE agents and automation. Library embed remains
//! the preferred in-process path (ADR 0001). Not a multi-tenant control plane;
//! tenkai stays delivery-only.
//!
//! Transport: JSON-RPC 2.0 with `Content-Length` framing on **stdio only**
//! (no network bind in v1).

use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, stdin, stdout};

use crate::harness::Harness;
use crate::identity::{PRODUCT, VERSION};
use crate::model::TokenUsage;
use crate::run::{ParkInfo, RunRequest, RunResult};

/// Stable summary returned by the MCP `run` tool.
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

/// Serve MCP on process stdio until EOF. Protocol traffic uses stdout; leave
/// diagnostics on stderr so hosts can separate streams.
pub async fn run_stdio(harness: &Harness) -> Result<(), String> {
    let mut reader = BufReader::new(stdin());
    let mut writer = stdout();
    loop {
        let msg = match read_message(&mut reader).await {
            Ok(m) => m,
            Err(e) if e == "eof" => break,
            Err(e) => return Err(e),
        };
        if let Some(resp) = handle_message(harness, msg).await {
            write_message(&mut writer, &resp).await?;
        }
    }
    Ok(())
}

/// Handle one JSON-RPC message. Returns `None` for notifications (no response).
pub async fn handle_message(harness: &Harness, msg: Value) -> Option<Value> {
    let method = msg.get("method").and_then(|m| m.as_str()).unwrap_or("");
    // Notifications have no `id`.
    let id = msg.get("id").cloned()?;

    let params = msg.get("params").cloned().unwrap_or(Value::Null);
    let result = match method {
        "initialize" => initialize_result(),
        "ping" => json!({}),
        "tools/list" => tools_list_result(),
        "tools/call" => match call_tool(harness, &params).await {
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
        "instructions": "Shikigami MCP host exposes doctor and run. Prefer library embed for in-process hosts. Not a multi-tenant control plane; tenkai is delivery-only."
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
                "description": "Execute a harness run. Returns a RunResult summary (not a multi-tenant control plane). Default local profile needs no plane.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "task": {
                            "type": "string",
                            "description": "Task specification (required unless resume_run_id is set)."
                        },
                        "keep_workspace": {
                            "type": "boolean",
                            "description": "Keep the workspace directory after a successful run."
                        },
                        "timeout_secs": {
                            "type": "integer",
                            "minimum": 1,
                            "description": "Overall wall-clock timeout in seconds."
                        },
                        "resume_run_id": {
                            "type": "string",
                            "description": "Resume a previous run from its local checkpoint."
                        },
                        "resume_answer": {
                            "type": "string",
                            "description": "Operator answer when resuming a parked (escalate) run."
                        }
                    },
                    "additionalProperties": false
                }
            }
        ]
    })
}

async fn call_tool(harness: &Harness, params: &Value) -> Result<Value, String> {
    let name = params
        .get("name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "tools/call missing name".to_string())?;
    let args = params.get("arguments").cloned().unwrap_or(json!({}));
    match name {
        "doctor" => {
            let report = harness.doctor_async().await;
            let text = serde_json::to_string_pretty(&report).map_err(|e| e.to_string())?;
            Ok(tool_text_result(&text, !report.ok))
        }
        "run" => {
            let summary = run_tool(harness, &args).await?;
            let text = serde_json::to_string_pretty(&summary).map_err(|e| e.to_string())?;
            Ok(tool_text_result(&text, !summary.success))
        }
        other => Err(format!("unknown tool: {other}")),
    }
}

async fn run_tool(harness: &Harness, args: &Value) -> Result<McpRunSummary, String> {
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

    let result = harness.run(request).await.map_err(|e| e.to_string())?;
    // Park is a valid outcome; summary still returned (success typically false).
    Ok(McpRunSummary::from(&result))
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
        let harness = local_scripted_harness(dir.path());

        let init = handle_message(
            &harness,
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {"name": "test", "version": "0"}
                }
            }),
        )
        .await
        .unwrap();
        assert_eq!(init["result"]["serverInfo"]["name"], PRODUCT);
        assert!(init["result"]["capabilities"]["tools"].is_object());

        let list = handle_message(
            &harness,
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
    }

    #[tokio::test]
    async fn doctor_and_scripted_run_offline() {
        let dir = tempdir().unwrap();
        let harness = local_scripted_harness(dir.path());

        let doctor = handle_message(
            &harness,
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
        assert_eq!(report["schema_version"], 1);
        assert_eq!(report["ok"], true);
        assert_eq!(report["governance"], "local");
        // Redaction contract: no raw secret keys expected in empty default config.
        assert!(!text.contains("api_key="));

        let run = handle_message(
            &harness,
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
        assert_eq!(summary.termination, "completed");
        assert!(summary.turns >= 2);
        let marker = std::path::Path::new(&summary.workspace).join("SHIKIGAMI_OK.txt");
        assert!(marker.is_file(), "expected {}", marker.display());
    }

    #[tokio::test]
    async fn run_requires_task_without_resume() {
        let dir = tempdir().unwrap();
        let harness = local_scripted_harness(dir.path());
        let resp = handle_message(
            &harness,
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
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("task is required"), "{text}");
    }

    #[tokio::test]
    async fn notification_has_no_response() {
        let dir = tempdir().unwrap();
        let harness = local_scripted_harness(dir.path());
        let resp = handle_message(
            &harness,
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
