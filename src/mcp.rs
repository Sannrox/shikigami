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
            match McpHttpClient::connect(server, config).await {
                Ok(mut client) => {
                    let tools = client.list_tools().await?;
                    let client = Arc::new(Mutex::new(client));
                    for t in tools {
                        let full = format!("mcp.{}.{}", server.name, t.name);
                        registry.register_external(Arc::new(McpHttpRemoteTool {
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

/// Minimal HTTP JSON-RPC MCP client (POST body; not full SSE streaming).
struct McpHttpClient {
    url: String,
    token: Option<String>,
    next_id: u64,
    #[cfg(feature = "model-http")]
    client: reqwest::Client,
}

impl McpHttpClient {
    async fn connect(server: &McpServerSettings, config: &Config) -> Result<Self, ToolError> {
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
            let mut c = Self {
                url,
                token,
                next_id: 1,
                client,
            };
            let _ = c
                .request(
                    "initialize",
                    json!({
                        "protocolVersion": "2024-11-05",
                        "capabilities": {},
                        "clientInfo": {"name": "shikigami", "version": env!("CARGO_PKG_VERSION")}
                    }),
                )
                .await;
            let _ = c.notify("notifications/initialized", json!({})).await;
            Ok(c)
        }
        #[cfg(not(feature = "model-http"))]
        {
            let _ = (url, token);
            Err(ToolError::Message(
                "http mcp requires the model-http feature".into(),
            ))
        }
    }

    async fn list_tools(&mut self) -> Result<Vec<McpToolInfo>, ToolError> {
        #[cfg(not(feature = "model-http"))]
        {
            return Err(ToolError::Message(
                "http mcp requires the model-http feature".into(),
            ));
        }
        #[cfg(feature = "model-http")]
        {
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
    }

    #[cfg(feature = "model-http")]
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

    #[cfg(feature = "model-http")]
    async fn request(&mut self, method: &str, params: Value) -> Result<Value, ToolError> {
        let id = self.next_id;
        self.next_id += 1;
        let msg = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        });
        let mut req = self.client.post(&self.url).json(&msg);
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
        let body: Value = resp
            .json()
            .await
            .map_err(|e| ToolError::Message(format!("http mcp body: {e}")))?;
        if let Some(err) = body.get("error") {
            return Err(ToolError::Message(format!("mcp error: {err}")));
        }
        Ok(body.get("result").cloned().unwrap_or(Value::Null))
    }

    #[cfg(feature = "model-http")]
    async fn notify(&mut self, method: &str, params: Value) -> Result<(), ToolError> {
        let msg = json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        });
        let mut req = self.client.post(&self.url).json(&msg);
        if let Some(tok) = &self.token {
            req = req.bearer_auth(tok);
        }
        let _ = req.send().await;
        Ok(())
    }
}

struct McpHttpRemoteTool {
    full_name: String,
    remote_name: String,
    description: String,
    schema: String,
    client: Arc<Mutex<McpHttpClient>>,
}

#[async_trait]
impl ExternalTool for McpHttpRemoteTool {
    fn definition(&self) -> ToolDef {
        ToolDef {
            name: self.full_name.clone(),
            description: self.description.clone(),
            schema: self.schema.clone(),
        }
    }

    async fn call(&self, args_json: &str) -> Result<String, ToolError> {
        let mut guard = self.client.lock().await;
        #[cfg(feature = "model-http")]
        {
            return guard.call_tool(&self.remote_name, args_json).await;
        }
        #[cfg(not(feature = "model-http"))]
        {
            let _ = args_json;
            Err(ToolError::Message(
                "http mcp requires the model-http feature".into(),
            ))
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
