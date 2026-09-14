//! Minimal MCP client: register remote tools into [`ToolRegistry`].
//!
//! Protocol: JSON-RPC 2.0 over stdio (tools/list + tools/call). For offline
//! tests, a process can speak a tiny subset without a full MCP stack.

use std::process::Stdio;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::io::BufReader;
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::mpsc;

pub(crate) mod framing;
mod protocol;

use protocol::{Client as McpClient, Transport, attach_tools};

use crate::config::{Config, McpFraming, McpServerSettings};
use crate::tools::{ExternalTool, ToolDef, ToolEnvironment, ToolError, ToolRegistry};

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
                Ok(client) => {
                    n += attach_tools(registry, &server.name, client)
                        .await
                        .map_err(|e| {
                            ToolError::Message(format!("mcp server `{}`: {e}", server.name))
                        })?;
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
        let protected = config.protected_tool_environment_names();
        match McpStdioTransport::spawn(server, &protected).await {
            Ok(transport) => {
                n += attach_tools(
                    registry,
                    &server.name,
                    McpClient::new(transport, server.timeout()),
                )
                .await
                .map_err(|e| ToolError::Message(format!("mcp server `{}`: {e}", server.name)))?;
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

/// Complete frames the reader task may hold ahead of the next exchange. Once
/// full, the reader stops draining stdout and the pipe applies backpressure
/// to the server, so a chatty server cannot grow harness memory without bound.
const FRAME_QUEUE_CAPACITY: usize = 64;

struct McpStdioTransport {
    /// Kept alive for process lifetime (kill_on_drop); killed explicitly once
    /// the stdin stream is known to be desynchronized.
    child: Child,
    stdin: ChildStdin,
    /// Complete frames decoded by a dedicated reader task. Awaiting a channel
    /// is cancellation-safe, so a request deadline can expire mid-response
    /// without desynchronizing the stream; the late frame is discarded by id
    /// on the next exchange.
    frames: mpsc::Receiver<Result<Value, String>>,
    framing: McpFraming,
    /// Set for the duration of every stdin write. A deadline that fires while
    /// the pipe is full drops the write future mid-frame and leaves this set,
    /// which marks the stream unusable: the next write fails closed and kills
    /// the server instead of appending a new request to a partial frame.
    write_in_flight: bool,
}

impl McpStdioTransport {
    async fn spawn(
        server: &McpServerSettings,
        protected_environment_names: &[String],
    ) -> Result<Self, ToolError> {
        // Same credential reconstruction as Bash: clear then allowlist parent
        // env minus plane/model/MCP token names and shell-startup controls.
        let environment = ToolEnvironment::resolve(protected_environment_names);
        let mut command = Command::new(&server.command);
        command
            .args(&server.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        environment.apply(&mut command);
        let mut child = command
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
        let (tx, frames) = mpsc::channel(FRAME_QUEUE_CAPACITY);
        tokio::spawn(async move {
            let mut stdout = BufReader::new(stdout);
            loop {
                let frame = framing::read(&mut stdout).await;
                let done = frame.is_err();
                if tx.send(frame).await.is_err() || done {
                    break;
                }
            }
        });
        Ok(Self {
            child,
            stdin,
            frames,
            framing: server.framing,
            write_in_flight: false,
        })
    }

    async fn write_message(&mut self, msg: &Value) -> Result<(), ToolError> {
        if self.write_in_flight {
            let _ = self.child.start_kill();
            return Err(ToolError::Message(
                "mcp stdin desynchronized: a previous request was interrupted mid-frame by its deadline; server killed, no further calls are accepted on this attachment"
                    .into(),
            ));
        }
        self.write_in_flight = true;
        let written = match self.framing {
            McpFraming::ContentLength => framing::write(&mut self.stdin, msg).await,
            McpFraming::Newline => framing::write_line(&mut self.stdin, msg).await,
        };
        self.write_in_flight = false;
        written.map_err(ToolError::Message)
    }

    async fn read_message(&mut self) -> Result<Value, ToolError> {
        match self.frames.recv().await {
            Some(Ok(frame)) => Ok(frame),
            Some(Err(error)) if error != "eof" => Err(ToolError::Message(error)),
            Some(Err(_)) | None => Err(ToolError::Message("mcp stdout closed".into())),
        }
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
                .timeout(server.timeout())
                .build()
                .map_err(|e| ToolError::Message(format!("http mcp client: {e}")))?;
            Ok(McpClient::new(
                Self { url, token, client },
                server.timeout(),
            ))
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
            framing: McpFraming::ContentLength,
            timeout_secs: 30,
        }
    }

    /// Spec-compliant newline-delimited stdio server written in `sh`: it
    /// answers `tools/list` with one tool, never answers a call whose
    /// arguments say `hang`, and answers every other call with an unrelated
    /// stray frame followed by an `isError` result.
    #[cfg(unix)]
    fn newline_sh_server(name: &str, timeout_secs: u64) -> McpServerSettings {
        let script = r#"
while IFS= read -r line; do
  id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
  case "$line" in
    *'"method":"initialize"'*) printf '{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":"2024-11-05","capabilities":{"tools":{}},"serverInfo":{"name":"sh-mcp","version":"0"}}}\n' "$id" ;;
    *'"method":"tools/list"'*) printf '{"jsonrpc":"2.0","id":%s,"result":{"tools":[{"name":"probe","description":"probe","inputSchema":{"type":"object"}}]}}\n' "$id" ;;
    *'"hang"'*) ;;
    *'"method":"tools/call"'*)
      printf '{"jsonrpc":"2.0","id":999,"result":{"stray":true}}\n'
      printf '{"jsonrpc":"2.0","id":%s,"result":{"content":[{"type":"text","text":"{\\"code\\":\\"permission_denied\\"}"}],"isError":true}}\n' "$id" ;;
  esac
done
"#;
        McpServerSettings {
            name: name.into(),
            command: "sh".into(),
            args: vec!["-c".into(), script.into()],
            transport: "stdio".into(),
            url: None,
            token_env: None,
            framing: McpFraming::Newline,
            timeout_secs,
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn newline_stdio_server_projects_is_error_and_deadline_as_failures() {
        let dir = tempdir().unwrap();
        let mut config = Config::default();
        config.tools.mcp_servers = vec![newline_sh_server("nl", 1)];
        let mut reg = ToolRegistry::with_builtins(
            dir.path(),
            vec!["read_file".into()],
            30,
            NetworkSettings::default(),
        )
        .unwrap();
        assert_eq!(attach_mcp_servers(&mut reg, &config).await.unwrap(), 1);
        assert!(reg.definitions().iter().any(|d| d.name == "mcp.nl.probe"));

        let denied = reg
            .execute("mcp.nl.probe", r#"{"mode":"deny"}"#)
            .await
            .unwrap_err()
            .to_string();
        assert!(denied.contains("permission_denied"), "{denied}");

        let timed_out = reg
            .execute("mcp.nl.probe", r#"{"mode":"hang"}"#)
            .await
            .unwrap_err()
            .to_string();
        assert!(timed_out.contains("deadline_exceeded"), "{timed_out}");

        // The stream stays usable after a deadline: the next call still gets
        // its own response instead of the stale frame.
        let denied_again = reg
            .execute("mcp.nl.probe", r#"{"mode":"deny"}"#)
            .await
            .unwrap_err()
            .to_string();
        assert!(denied_again.contains("permission_denied"), "{denied_again}");
    }

    /// Newline server that answers discovery and then stops reading stdin, so
    /// a request larger than the pipe blocks until the client deadline drops
    /// the write future mid-frame.
    #[cfg(unix)]
    fn stalled_sh_server(name: &str) -> McpServerSettings {
        let script = r#"
while IFS= read -r line; do
  id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
  case "$line" in
    *'"method":"initialize"'*) printf '{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":"2024-11-05","capabilities":{"tools":{}},"serverInfo":{"name":"sh-mcp","version":"0"}}}\n' "$id" ;;
    *'"method":"tools/list"'*)
      printf '{"jsonrpc":"2.0","id":%s,"result":{"tools":[{"name":"probe","description":"probe","inputSchema":{"type":"object"}}]}}\n' "$id"
      exec sleep 10 ;;
  esac
done
"#;
        McpServerSettings {
            name: name.into(),
            command: "sh".into(),
            args: vec!["-c".into(), script.into()],
            transport: "stdio".into(),
            url: None,
            token_env: None,
            framing: McpFraming::Newline,
            timeout_secs: 1,
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn interrupted_stdin_write_fails_closed_instead_of_desynchronizing() {
        let dir = tempdir().unwrap();
        let mut config = Config::default();
        config.tools.mcp_servers = vec![stalled_sh_server("stall")];
        let mut reg = ToolRegistry::with_builtins(
            dir.path(),
            vec!["read_file".into()],
            30,
            NetworkSettings::default(),
        )
        .unwrap();
        assert_eq!(attach_mcp_servers(&mut reg, &config).await.unwrap(), 1);

        // Larger than any default pipe capacity, smaller than MAX_FRAME_BYTES:
        // the write parks on a full pipe and the deadline cancels it mid-frame.
        let oversized = format!(r#"{{"pad":"{}"}}"#, "x".repeat(900 * 1024));
        let timed_out = reg
            .execute("mcp.stall.probe", &oversized)
            .await
            .unwrap_err()
            .to_string();
        assert!(timed_out.contains("deadline_exceeded"), "{timed_out}");

        // The partial frame must never be completed by a later request.
        let refused = reg
            .execute("mcp.stall.probe", r#"{"mode":"small"}"#)
            .await
            .unwrap_err()
            .to_string();
        assert!(refused.contains("desynchronized"), "{refused}");
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

    #[tokio::test]
    async fn mcp_stdio_spawn_scrubs_protected_credentials() {
        let dir = tempdir().unwrap();
        let dump = dir.path().join("child-env.txt");
        let dump_path = dump.to_string_lossy().into_owned();
        let safe_name = "SHIKIGAMI_TEST_MCP_SAFE_FLAG_SEC";
        let token_name = "SEKAI_TOKEN";
        let plane_name = "SHIKIGAMI_TEST_PLANE_TOKEN_SEC";
        // SAFETY: unique/synthetic test names removed immediately after spawn
        // snapshots the parent environment.
        unsafe {
            std::env::set_var(safe_name, "visible");
            std::env::set_var(token_name, "must-not-reach-mcp");
            std::env::set_var(plane_name, "must-not-reach-mcp-either");
        }
        let protected = vec![token_name.into(), plane_name.into()];
        let server = McpServerSettings {
            name: "envprobe".into(),
            command: "sh".into(),
            args: vec![
                "-c".into(),
                format!(
                    "printf '%s|%s|%s' \"${{{safe_name}-unset}}\" \"${{{token_name}-unset}}\" \"${{{plane_name}-unset}}\" > '{dump_path}'; sleep 30"
                ),
            ],
            transport: "stdio".into(),
            url: None,
            token_env: None,
            framing: McpFraming::ContentLength,
            timeout_secs: 30,
        };
        let transport = McpStdioTransport::spawn(&server, &protected)
            .await
            .expect("spawn env probe");
        // SAFETY: cleanup of the synthetic names above.
        unsafe {
            std::env::remove_var(safe_name);
            std::env::remove_var(token_name);
            std::env::remove_var(plane_name);
        }
        let mut saw = None;
        for _ in 0..50 {
            if dump.is_file() {
                saw = std::fs::read_to_string(&dump).ok();
                if saw.as_ref().is_some_and(|s| !s.is_empty()) {
                    break;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        drop(transport);
        let body = saw.expect("child wrote env probe");
        assert_eq!(
            body, "visible|unset|unset",
            "mcp child env leaked secrets: {body}"
        );
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
            framing: McpFraming::ContentLength,
            timeout_secs: 30,
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
