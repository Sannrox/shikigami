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

mod protocol;

use protocol::{Client as McpClient, Transport, attach_tools};

use crate::config::{Config, McpServerSettings};
use crate::tools::{ExternalTool, ToolDef, ToolError, ToolRegistry};

pub(crate) const MAX_MCP_FRAME_BYTES: usize = 1024 * 1024;
pub(crate) const MAX_MCP_HEADER_BYTES: usize = 8 * 1024;

/// Attach MCP tools named `mcp.<server>.<tool>` to a registry.
pub async fn attach_mcp_servers(
    registry: &mut ToolRegistry,
    config: &Config,
) -> Result<usize, ToolError> {
    let mut n = 0;
    for server in &config.tools.mcp_servers {
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
        let transport = server.transport.as_str();
        let is_http = transport == "http"
            || (server.url.is_some() && (transport == "stdio" || transport.is_empty()));
        if is_http {
            let url = server.url.as_deref().ok_or_else(|| {
                ToolError::Message(format!("mcp server `{}`: http requires url", server.name))
            })?;
            config
                .network
                .check_http_url(url)
                .map_err(ToolError::Message)?;
            match McpHttpTransport::connect(server, config).await {
                Ok(client) => n += attach_tools(registry, &server.name, client).await?,
                Err(e) => {
                    return Err(ToolError::Message(format!(
                        "mcp server `{}`: {e}",
                        server.name
                    )));
                }
            }
            continue;
        }
        if server.command.is_empty() {
            continue;
        }
        match McpStdioTransport::spawn(server).await {
            Ok(transport) => {
                n += attach_tools(registry, &server.name, McpClient::new(transport)).await?;
            }
            Err(e) => {
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

struct McpStdioTransport {
    /// Kept alive for process lifetime (kill_on_drop).
    #[allow(dead_code)]
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}

impl McpStdioTransport {
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
        Ok(Self {
            child,
            stdin,
            stdout: BufReader::new(stdout),
        })
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
        let mut header_bytes = 0usize;
        loop {
            let mut line = String::new();
            let n = (&mut self.stdout)
                .take((MAX_MCP_HEADER_BYTES + 1) as u64)
                .read_line(&mut line)
                .await
                .map_err(|e| ToolError::Message(e.to_string()))?;
            if n == 0 {
                return Err(ToolError::Message("mcp stdout closed".into()));
            }
            header_bytes = header_bytes.saturating_add(n);
            if header_bytes > MAX_MCP_HEADER_BYTES {
                return Err(ToolError::Message(format!(
                    "mcp headers exceed {MAX_MCP_HEADER_BYTES} bytes"
                )));
            }
            let line = line.trim_end();
            if line.is_empty() {
                break;
            }
            if let Some(rest) = line.strip_prefix("Content-Length:") {
                if content_length.is_some() {
                    return Err(ToolError::Message("mcp duplicate Content-Length".into()));
                }
                let length = rest
                    .trim()
                    .parse::<usize>()
                    .map_err(|_| ToolError::Message("mcp invalid Content-Length".into()))?;
                if length > MAX_MCP_FRAME_BYTES {
                    return Err(ToolError::Message(format!(
                        "mcp Content-Length {length} exceeds {MAX_MCP_FRAME_BYTES} bytes"
                    )));
                }
                content_length = Some(length);
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

#[async_trait]
impl Transport for McpStdioTransport {
    async fn exchange(&mut self, request: &Value) -> Result<Value, ToolError> {
        self.write_message(request).await?;
        let id = request.get("id").and_then(Value::as_u64);
        loop {
            let response = self.read_message().await?;
            if response.get("id").and_then(Value::as_u64) == id {
                return Ok(response);
            }
        }
    }

    async fn send(&mut self, notification: &Value) -> Result<(), ToolError> {
        self.write_message(notification).await
    }
}

/// Minimal HTTP JSON-RPC MCP client (POST body; not full SSE streaming).
struct McpHttpTransport {
    url: String,
    token: Option<String>,
    #[cfg(feature = "model-http")]
    client: reqwest::Client,
}

impl McpHttpTransport {
    async fn connect(
        server: &McpServerSettings,
        config: &Config,
    ) -> Result<McpClient<Self>, ToolError> {
        let url = server
            .url
            .clone()
            .ok_or_else(|| ToolError::Message("http mcp missing url".into()))?;
        config
            .network
            .check_http_url(&url)
            .map_err(ToolError::Message)?;
        let token = server
            .token_env
            .as_ref()
            .and_then(|e| std::env::var(e).ok())
            .filter(|v| !v.is_empty());
        #[cfg(feature = "model-http")]
        {
            let client = reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .map_err(|e| ToolError::Message(format!("http mcp client: {e}")))?;
            Ok(McpClient::new(Self { url, token, client }))
        }
        #[cfg(not(feature = "model-http"))]
        {
            let _ = (url, token);
            Err(ToolError::Message(
                "http mcp requires the model-http feature".into(),
            ))
        }
    }
}

#[async_trait]
impl Transport for McpHttpTransport {
    async fn exchange(&mut self, request: &Value) -> Result<Value, ToolError> {
        #[cfg(not(feature = "model-http"))]
        {
            let _ = request;
            return Err(ToolError::Message(
                "http mcp requires the model-http feature".into(),
            ));
        }
        #[cfg(feature = "model-http")]
        {
            let mut req = self.client.post(&self.url).json(request);
            if let Some(tok) = &self.token {
                req = req.bearer_auth(tok);
            }
            let resp = req
                .send()
                .await
                .map_err(|e| ToolError::Message(format!("http mcp: {e}")))?;
            if !resp.status().is_success() {
                return Err(ToolError::Message(format!(
                    "http mcp status {}",
                    resp.status()
                )));
            }
            resp.json()
                .await
                .map_err(|e| ToolError::Message(format!("http mcp body: {e}")))
        }
    }

    async fn send(&mut self, notification: &Value) -> Result<(), ToolError> {
        #[cfg(not(feature = "model-http"))]
        {
            let _ = notification;
            return Err(ToolError::Message(
                "http mcp requires the model-http feature".into(),
            ));
        }
        #[cfg(feature = "model-http")]
        {
            let mut req = self.client.post(&self.url).json(notification);
            if let Some(tok) = &self.token {
                req = req.bearer_auth(tok);
            }
            let _ = req.send().await;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, EgressMode, NetworkSettings};
    use crate::tools::ToolRegistry;
    use tempfile::tempdir;
    #[cfg(feature = "model-http")]
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    #[cfg(feature = "model-http")]
    use tokio::net::TcpListener;

    fn mock_stdio_server(name: &str) -> McpServerSettings {
        McpServerSettings {
            name: name.into(),
            command: "mock".into(),
            args: vec![],
            transport: "stdio".into(),
            url: None,
            token_env: None,
        }
    }

    #[tokio::test]
    async fn mock_mcp_registers_and_echoes() {
        let dir = tempdir().unwrap();
        let mut config = Config::default();
        config.tools.mcp_servers = vec![mock_stdio_server("demo")];
        let mut reg = ToolRegistry::with_builtins(
            dir.path(),
            vec!["read_file".into()],
            30,
            NetworkSettings::default(),
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

    #[cfg(feature = "model-http")]
    #[tokio::test]
    async fn http_mcp_registers_with_local_server() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                let mut buf = vec![0u8; 8192];
                let n = sock.read(&mut buf).await.unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]);
                let body = if req.contains("tools/list") {
                    json!({
                        "jsonrpc": "2.0",
                        "id": 1,
                        "result": {
                            "tools": [{
                                "name": "ping",
                                "description": "ping",
                                "inputSchema": {"type":"object","properties":{}}
                            }]
                        }
                    })
                } else if req.contains("tools/call") {
                    json!({
                        "jsonrpc": "2.0",
                        "id": 2,
                        "result": {
                            "content": [{"type":"text","text":"pong"}]
                        }
                    })
                } else {
                    json!({"jsonrpc":"2.0","id":0,"result":{}})
                };
                let body_s = body.to_string();
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body_s.len(),
                    body_s
                );
                let _ = sock.write_all(resp.as_bytes()).await;
            }
        });

        let dir = tempdir().unwrap();
        let mut config = Config {
            network: NetworkSettings {
                egress: EgressMode::Allowlist,
                allow_hosts: vec!["127.0.0.1".into()],
            },
            ..Default::default()
        };
        config.tools.mcp_servers = vec![McpServerSettings {
            name: "httpdemo".into(),
            command: String::new(),
            args: vec![],
            transport: "http".into(),
            url: Some(format!("http://{addr}/mcp")),
            token_env: None,
        }];
        let mut reg = ToolRegistry::with_builtins(
            dir.path(),
            vec!["read_file".into()],
            30,
            config.network.clone(),
        )
        .unwrap();
        let n = attach_mcp_servers(&mut reg, &config).await.unwrap();
        assert_eq!(n, 1);
        let out = reg.execute("mcp.httpdemo.ping", "{}").await.unwrap();
        match out {
            crate::tools::ToolOutput::Text(t) => assert_eq!(t, "pong"),
            _ => panic!("expected text"),
        }

        // egress deny
        let mut deny_cfg = Config {
            network: NetworkSettings {
                egress: EgressMode::Deny,
                allow_hosts: vec![],
            },
            ..Default::default()
        };
        deny_cfg.tools.mcp_servers = config.tools.mcp_servers.clone();
        let mut reg2 = ToolRegistry::with_builtins(
            dir.path(),
            vec!["read_file".into()],
            30,
            deny_cfg.network.clone(),
        )
        .unwrap();
        let err = attach_mcp_servers(&mut reg2, &deny_cfg).await.unwrap_err();
        assert!(
            err.to_string().contains("denied") || err.to_string().contains("egress"),
            "{err}"
        );
    }
}
