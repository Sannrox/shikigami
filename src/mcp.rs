//! Minimal MCP client: register remote tools into [`ToolRegistry`].
//!
//! Protocol: JSON-RPC 2.0 over stdio (tools/list + tools/call). For offline
//! tests, a process can speak a tiny subset without a full MCP stack.

use std::process::Stdio;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::Mutex;

use crate::config::{Config, McpServerSettings};
use crate::tools::{ExternalTool, ToolDef, ToolError, ToolRegistry};

/// Attach MCP tools named `mcp.<server>.<tool>` to a registry.
pub async fn attach_mcp_servers(
    registry: &mut ToolRegistry,
    config: &Config,
) -> Result<usize, ToolError> {
    let mut n = 0;
    for server in &config.tools.mcp_servers {
        if server.command.is_empty() {
            continue;
        }
        // Special offline mock: command "mock" registers a fixed tool.
        if server.command == "mock" {
            let name = format!("mcp.{}.echo", server.name);
            registry.register_external(Arc::new(MockEchoTool {
                name,
                server: server.name.clone(),
            }));
            n += 1;
            continue;
        }
        match McpStdioClient::spawn(server).await {
            Ok(mut client) => {
                let tools = client.list_tools().await?;
                let client = Arc::new(Mutex::new(client));
                for t in tools {
                    let full = format!("mcp.{}.{}", server.name, t.name);
                    registry.register_external(Arc::new(McpRemoteTool {
                        full_name: full,
                        remote_name: t.name,
                        description: t.description,
                        schema: t.input_schema,
                        client: Arc::clone(&client),
                    }));
                    n += 1;
                }
            }
            Err(e) => {
                // Fail closed only when servers are configured and unreachable.
                return Err(ToolError::Message(format!(
                    "mcp server `{}`: {e}",
                    server.name
                )));
            }
        }
    }
    Ok(n)
}

struct MockEchoTool {
    name: String,
    server: String,
}

#[async_trait]
impl ExternalTool for MockEchoTool {
    fn definition(&self) -> ToolDef {
        ToolDef {
            name: self.name.clone(),
            description: format!("Mock MCP echo tool from server {}", self.server),
            schema:
                r#"{"type":"object","properties":{"text":{"type":"string"}},"required":["text"]}"#
                    .into(),
        }
    }

    async fn call(&self, args_json: &str) -> Result<String, ToolError> {
        let v: Value = serde_json::from_str(args_json).unwrap_or(json!({}));
        let text = v.get("text").and_then(|t| t.as_str()).unwrap_or("");
        Ok(format!("echo:{text}"))
    }
}

struct McpToolInfo {
    name: String,
    description: String,
    input_schema: String,
}

struct McpStdioClient {
    /// Kept alive for process lifetime (kill_on_drop).
    #[allow(dead_code)]
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    next_id: u64,
}

impl McpStdioClient {
    async fn spawn(server: &McpServerSettings) -> Result<Self, ToolError> {
        let mut child = Command::new(&server.command)
            .args(&server.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| ToolError::Message(format!("spawn mcp: {e}")))?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| ToolError::Message("mcp stdin missing".into()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| ToolError::Message("mcp stdout missing".into()))?;
        let mut client = Self {
            child,
            stdin,
            stdout: BufReader::new(stdout),
            next_id: 1,
        };
        // initialize (best-effort)
        let _ = client
            .request(
                "initialize",
                json!({
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {"name": "shikigami", "version": env!("CARGO_PKG_VERSION")}
                }),
            )
            .await;
        let _ = client.notify("notifications/initialized", json!({})).await;
        Ok(client)
    }

    async fn list_tools(&mut self) -> Result<Vec<McpToolInfo>, ToolError> {
        let result = self.request("tools/list", json!({})).await?;
        let tools = result
            .get("tools")
            .and_then(|t| t.as_array())
            .cloned()
            .unwrap_or_default();
        let mut out = Vec::new();
        for t in tools {
            let name = t
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if name.is_empty() {
                continue;
            }
            let description = t
                .get("description")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let schema = t
                .get("inputSchema")
                .cloned()
                .unwrap_or_else(|| json!({"type":"object"}));
            out.push(McpToolInfo {
                name,
                description,
                input_schema: schema.to_string(),
            });
        }
        Ok(out)
    }

    async fn call_tool(&mut self, name: &str, args_json: &str) -> Result<String, ToolError> {
        let args: Value = serde_json::from_str(args_json).unwrap_or(json!({}));
        let result = self
            .request(
                "tools/call",
                json!({
                    "name": name,
                    "arguments": args,
                }),
            )
            .await?;
        // Prefer content[0].text
        if let Some(arr) = result.get("content").and_then(|c| c.as_array()) {
            let mut texts = Vec::new();
            for c in arr {
                if let Some(t) = c.get("text").and_then(|v| v.as_str()) {
                    texts.push(t.to_string());
                }
            }
            if !texts.is_empty() {
                return Ok(texts.join("\n"));
            }
        }
        Ok(result.to_string())
    }

    async fn request(&mut self, method: &str, params: Value) -> Result<Value, ToolError> {
        let id = self.next_id;
        self.next_id += 1;
        let msg = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        });
        self.write_message(&msg).await?;
        loop {
            let resp = self.read_message().await?;
            if resp.get("id").and_then(|v| v.as_u64()) == Some(id) {
                if let Some(err) = resp.get("error") {
                    return Err(ToolError::Message(format!("mcp error: {err}")));
                }
                return Ok(resp.get("result").cloned().unwrap_or(Value::Null));
            }
        }
    }

    async fn notify(&mut self, method: &str, params: Value) -> Result<(), ToolError> {
        let msg = json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        });
        self.write_message(&msg).await
    }

    async fn write_message(&mut self, msg: &Value) -> Result<(), ToolError> {
        let body = serde_json::to_vec(msg).map_err(|e| ToolError::Message(e.to_string()))?;
        let header = format!("Content-Length: {}\r\n\r\n", body.len());
        self.stdin
            .write_all(header.as_bytes())
            .await
            .map_err(|e| ToolError::Message(e.to_string()))?;
        self.stdin
            .write_all(&body)
            .await
            .map_err(|e| ToolError::Message(e.to_string()))?;
        self.stdin
            .flush()
            .await
            .map_err(|e| ToolError::Message(e.to_string()))?;
        Ok(())
    }

    async fn read_message(&mut self) -> Result<Value, ToolError> {
        let mut content_length = None;
        loop {
            let mut line = String::new();
            let n = self
                .stdout
                .read_line(&mut line)
                .await
                .map_err(|e| ToolError::Message(e.to_string()))?;
            if n == 0 {
                return Err(ToolError::Message("mcp stdout closed".into()));
            }
            let line = line.trim_end();
            if line.is_empty() {
                break;
            }
            if let Some(rest) = line.strip_prefix("Content-Length:") {
                content_length = rest.trim().parse::<usize>().ok();
            }
        }
        let len = content_length
            .ok_or_else(|| ToolError::Message("mcp missing Content-Length".into()))?;
        let mut buf = vec![0u8; len];
        use tokio::io::AsyncReadExt;
        self.stdout
            .read_exact(&mut buf)
            .await
            .map_err(|e| ToolError::Message(e.to_string()))?;
        serde_json::from_slice(&buf).map_err(|e| ToolError::Message(e.to_string()))
    }
}

struct McpRemoteTool {
    full_name: String,
    remote_name: String,
    description: String,
    schema: String,
    client: Arc<Mutex<McpStdioClient>>,
}

#[async_trait]
impl ExternalTool for McpRemoteTool {
    fn definition(&self) -> ToolDef {
        ToolDef {
            name: self.full_name.clone(),
            description: self.description.clone(),
            schema: self.schema.clone(),
        }
    }

    async fn call(&self, args_json: &str) -> Result<String, ToolError> {
        let mut guard = self.client.lock().await;
        guard.call_tool(&self.remote_name, args_json).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::tools::ToolRegistry;
    use tempfile::tempdir;

    #[tokio::test]
    async fn mock_mcp_registers_and_echoes() {
        let dir = tempdir().unwrap();
        let mut config = Config::default();
        config.tools.mcp_servers = vec![McpServerSettings {
            name: "demo".into(),
            command: "mock".into(),
            args: vec![],
        }];
        let mut reg = ToolRegistry::with_builtins(
            dir.path(),
            vec!["read_file".into()],
            30,
            crate::config::NetworkSettings::default(),
        )
        .unwrap();
        let n = attach_mcp_servers(&mut reg, &config).await.unwrap();
        assert_eq!(n, 1);
        let defs = reg.definitions();
        assert!(defs.iter().any(|d| d.name == "mcp.demo.echo"));
        let out = reg
            .execute("mcp.demo.echo", r#"{"text":"hi"}"#)
            .await
            .unwrap();
        match out {
            crate::tools::ToolOutput::Text(t) => assert_eq!(t, "echo:hi"),
            _ => panic!("expected text"),
        }
    }
}
