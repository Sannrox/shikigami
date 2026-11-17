//! Minimal MCP client: register remote tools into [`ToolRegistry`].
//!
//! Protocol: JSON-RPC 2.0 over stdio or HTTP (`tools/list` + `tools/call`).
//! Stdio framing is `content-length` or `newline`; HTTP uses the same JSON-RPC
//! body when `model-http` is enabled. For offline tests, a process can speak a
//! tiny subset without a full MCP stack.

use std::process::Stdio;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::io::BufReader;
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::mpsc;

pub mod framing;
mod protocol;

use protocol::{Client as McpClient, Transport, attach_tools};

use crate::config::{Config, MCP_GATEWAY_NAME, McpFraming, McpGatewaySettings, McpServerSettings};
use crate::tools::{
    ExternalTool, JobCredentialHandle, ToolDef, ToolEnvironment, ToolError, ToolRegistry,
};

/// Gateway mode fails closed: every remote server must go through the
/// gateway, and its name is reserved. Local stdio servers are unaffected.
pub fn check_gateway_servers(servers: &[McpServerSettings]) -> Result<(), String> {
    for server in servers {
        if server.is_http() {
            return Err(format!(
                "tools.mcp_servers `{}`: direct remote MCP server refused while tools.mcp_gateway is set",
                server.name
            ));
        }
        if server.name == MCP_GATEWAY_NAME {
            return Err(format!(
                "tools.mcp_servers `{MCP_GATEWAY_NAME}`: name is reserved for tools.mcp_gateway"
            ));
        }
    }
    Ok(())
}

/// Attach MCP tools named `mcp.<server>.<tool>` to a registry. `job` is the
/// run's live grant; the gateway, when configured, requires it.
pub async fn attach_mcp_servers(
    registry: &mut ToolRegistry,
    config: &Config,
    job: Option<&JobCredentialHandle>,
) -> Result<usize, ToolError> {
    let mut n = 0;
    if let Some(gateway) = &config.tools.mcp_gateway {
        check_gateway_servers(&config.tools.mcp_servers).map_err(ToolError::Message)?;
        let job = job.ok_or_else(|| {
            ToolError::Message("mcp gateway requires the run's job credential".into())
        })?;
        let client = McpHttpTransport::connect_gateway(gateway, job.clone(), config).await?;
        // A site-agent turn offers only the gateway tools its settings name
        // (#487); other profiles attach every tool the gateway lists.
        let allowed = crate::config::is_site_agent_profile(&config.profile.name)
            .then_some(config.tools.enabled.as_slice());
        n += attach_tools(registry, MCP_GATEWAY_NAME, client, allowed)
            .await
            .map_err(|e| ToolError::Message(format!("mcp gateway: {e}")))?;
    }
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
        if server.is_http() {
            let url = server.url.as_deref().ok_or_else(|| {
                ToolError::Message(format!("mcp server `{}`: http requires url", server.name))
            })?;
            config
                .network
                .check_http_url(url)
                .map_err(ToolError::Message)?;
            match McpHttpTransport::connect(server, config).await {
                Ok(client) => {
                    n += attach_tools(registry, &server.name, client, None)
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
                    None,
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
    /// Set when the server announced a tool-list change while we waited.
    list_changed: bool,
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
            list_changed: false,
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
            self.list_changed |= protocol::is_list_changed(&response);
        }
    }

    fn take_list_changed(&mut self) -> bool {
        std::mem::take(&mut self.list_changed)
    }

    async fn send(&mut self, notification: &Value) -> Result<(), ToolError> {
        self.write_message(notification).await
    }
}

/// Bearer credential an HTTP MCP request carries.
enum HttpAuth {
    None,
    /// Read once at connect from the server's `token_env`.
    Static(String),
    /// The run's job grant, read per request so a heartbeat refresh applies
    /// and an expired grant fails closed instead of sending a stale token.
    Job(JobCredentialHandle),
}

impl HttpAuth {
    fn bearer(&self) -> Result<Option<String>, ToolError> {
        match self {
            Self::None => Ok(None),
            Self::Static(token) => Ok(Some(token.clone())),
            Self::Job(job) => job
                .live_token()
                .map(Some)
                .ok_or_else(|| ToolError::Message("mcp gateway: job credential expired".into())),
        }
    }
}

/// Incremental event-stream decoder. Bytes carry across chunks and are
/// decoded per complete line, so a chunk boundary inside a character or a
/// CRLF never corrupts an event. Lines end in LF, CRLF, or CR. Buffered bytes
/// are bounded by the MCP frame limit, so a server cannot grow memory freely.
#[cfg_attr(not(feature = "model-http"), allow(dead_code))]
#[derive(Default)]
struct SseDecoder {
    pending: Vec<u8>,
    data: Option<String>,
    /// The last line ended in CR; an LF that follows belongs to it.
    skip_lf: bool,
}

#[cfg_attr(not(feature = "model-http"), allow(dead_code))]
impl SseDecoder {
    /// Feed bytes; returns the data of every event this completed.
    fn push(&mut self, mut bytes: &[u8]) -> Result<Vec<String>, ToolError> {
        if std::mem::take(&mut self.skip_lf) && bytes.first() == Some(&b'\n') {
            bytes = &bytes[1..];
        }
        self.pending.extend_from_slice(bytes);
        let mut events = Vec::new();
        while let Some(end) = self.pending.iter().position(|b| *b == b'\n' || *b == b'\r') {
            let mut terminator = 1;
            if self.pending[end] == b'\r' {
                match self.pending.get(end + 1) {
                    Some(b'\n') => terminator = 2,
                    Some(_) => {}
                    None => self.skip_lf = true,
                }
            }
            let raw: Vec<u8> = self.pending.drain(..end + terminator).collect();
            let line = String::from_utf8_lossy(&raw[..end]);
            // A stream may open with a byte-order mark.
            let line = line.strip_prefix('\u{feff}').unwrap_or(&line);
            if line.is_empty() {
                // A blank line ends the event.
                events.extend(self.data.take());
            } else if let Some(value) = line.strip_prefix("data:") {
                let value = value.strip_prefix(' ').unwrap_or(value);
                let data = match &mut self.data {
                    Some(data) => {
                        data.push('\n');
                        data.push_str(value);
                        data
                    }
                    None => self.data.insert(value.to_owned()),
                };
                Self::bound(data.len())?;
            }
        }
        Self::bound(self.pending.len())?;
        Ok(events)
    }

    fn bound(len: usize) -> Result<(), ToolError> {
        if len > framing::MAX_FRAME_BYTES {
            return Err(ToolError::Message(format!(
                "http mcp event exceeds {} bytes",
                framing::MAX_FRAME_BYTES
            )));
        }
        Ok(())
    }
}

/// Streamable-HTTP MCP client. Each message is one POST; the server answers
/// with a JSON body or an event stream, and may bind the client to a session.
struct McpHttpTransport {
    url: String,
    auth: HttpAuth,
    /// `Mcp-Session-Id` the server issued at initialize, sent on every later request.
    session_id: Option<String>,
    /// The initialize request, replayed once when the server forgets the session.
    initialize: Option<Value>,
    /// Version the server chose at initialize, sent as `MCP-Protocol-Version`.
    protocol_version: Option<String>,
    /// A re-initialize failed; the next request retries it first.
    reinitialize_pending: bool,
    /// Set when the server announced a tool-list change in a reply stream.
    list_changed: bool,
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
        let auth = server
            .token_env
            .as_ref()
            .and_then(|e| std::env::var(e).ok())
            .filter(|v| !v.is_empty())
            .map_or(HttpAuth::None, HttpAuth::Static);
        Self::open(url, auth, server.timeout(), config)
    }

    async fn connect_gateway(
        gateway: &McpGatewaySettings,
        job: JobCredentialHandle,
        config: &Config,
    ) -> Result<McpClient<Self>, ToolError> {
        let timeout = std::time::Duration::from_secs(gateway.timeout_secs.max(1));
        Self::open(gateway.url.clone(), HttpAuth::Job(job), timeout, config)
    }

    fn open(
        url: String,
        auth: HttpAuth,
        timeout: std::time::Duration,
        config: &Config,
    ) -> Result<McpClient<Self>, ToolError> {
        config
            .network
            .check_http_url(&url)
            .map_err(ToolError::Message)?;
        #[cfg(feature = "model-http")]
        {
            // No redirects: one would carry the session id to another host
            // and skip the egress check made on `url`.
            let client = reqwest::Client::builder()
                .timeout(timeout)
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .map_err(|e| ToolError::Message(format!("http mcp client: {e}")))?;
            Ok(McpClient::new(
                Self {
                    url,
                    auth,
                    session_id: None,
                    initialize: None,
                    protocol_version: None,
                    reinitialize_pending: false,
                    list_changed: false,
                    client,
                },
                timeout,
            ))
        }
        #[cfg(not(feature = "model-http"))]
        {
            let _ = (url, auth, timeout);
            Err(ToolError::Message(
                "http mcp requires the model-http feature".into(),
            ))
        }
    }
}

#[cfg(feature = "model-http")]
const MCP_SESSION_HEADER: &str = "mcp-session-id";

#[cfg(feature = "model-http")]
impl McpHttpTransport {
    /// POST one JSON-RPC message with the headers streamable HTTP requires.
    async fn post(&self, message: &Value) -> Result<reqwest::Response, ToolError> {
        let mut req = self
            .client
            .post(&self.url)
            .header(
                reqwest::header::ACCEPT,
                "application/json, text/event-stream",
            )
            .json(message);
        if let Some(tok) = self.auth.bearer()? {
            req = req.bearer_auth(tok);
        }
        if let Some(session) = &self.session_id {
            req = req.header(MCP_SESSION_HEADER, session);
        }
        if let Some(version) = &self.protocol_version {
            req = req.header("mcp-protocol-version", version);
        }
        req.send()
            .await
            .map_err(|e| ToolError::Message(format!("http mcp: {e}")))
    }

    /// POST a request and return its reply, adopting any session id issued.
    /// `None` means the server answered 404 to the session we sent: it lost it.
    async fn round_trip(&mut self, request: &Value) -> Result<Option<Value>, ToolError> {
        let resp = self.post(request).await?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND && self.session_id.is_some() {
            return Ok(None);
        }
        if !resp.status().is_success() {
            return Err(ToolError::Message(format!(
                "http mcp status {}",
                resp.status()
            )));
        }
        if let Some(session) = resp
            .headers()
            .get(MCP_SESSION_HEADER)
            .and_then(|v| v.to_str().ok())
        {
            self.session_id = Some(session.to_owned());
        }
        let is_stream = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.starts_with("text/event-stream"));
        let reply = if is_stream {
            self.read_event_stream(resp, request.get("id")).await?
        } else {
            resp.json()
                .await
                .map_err(|e| ToolError::Message(format!("http mcp body: {e}")))?
        };
        if request.get("method").and_then(Value::as_str) == Some("initialize")
            && let Some(version) = reply
                .pointer("/result/protocolVersion")
                .and_then(Value::as_str)
        {
            self.protocol_version = Some(version.to_owned());
        }
        Ok(Some(reply))
    }

    /// Read events until the reply to `id` arrives. Notifications on the way
    /// are kept for the protocol layer; anything else is ignored.
    async fn read_event_stream(
        &mut self,
        mut resp: reqwest::Response,
        id: Option<&Value>,
    ) -> Result<Value, ToolError> {
        let mut decoder = SseDecoder::default();
        loop {
            let chunk = resp
                .chunk()
                .await
                .map_err(|e| ToolError::Message(format!("http mcp stream: {e}")))?
                .ok_or_else(|| {
                    ToolError::Message("http mcp stream ended before the reply".into())
                })?;
            for data in decoder.push(&chunk)? {
                let Ok(message) = serde_json::from_str::<Value>(&data) else {
                    continue;
                };
                // A server request may reuse our id; only a response has no method.
                if message.get("id") == id && message.get("method").is_none() {
                    return Ok(message);
                }
                self.list_changed |= protocol::is_list_changed(&message);
            }
        }
    }

    /// Start a fresh session after the server forgot ours.
    async fn reinitialize(&mut self, initialize: &Value) -> Result<(), ToolError> {
        self.session_id = None;
        self.reinitialize_pending = true;
        // A server that restarted stateless issues no new session; later
        // requests then go without one.
        let reply = self
            .round_trip(initialize)
            .await?
            .ok_or_else(|| ToolError::Message("http mcp session lost".into()))?;
        if let Some(error) = reply.get("error") {
            return Err(ToolError::Message(format!(
                "http mcp re-initialize refused: {error}"
            )));
        }
        let initialized = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "notifications/initialized",
            "params": {},
        });
        let accepted = self.post(&initialized).await?;
        if !accepted.status().is_success() {
            return Err(ToolError::Message(format!(
                "http mcp re-initialize: initialized notice got {}",
                accepted.status()
            )));
        }
        self.reinitialize_pending = false;
        Ok(())
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
            if request.get("method").and_then(Value::as_str) == Some("initialize") {
                self.initialize = Some(request.clone());
                self.reinitialize_pending = false;
            } else if self.reinitialize_pending
                && let Some(initialize) = self.initialize.clone()
            {
                self.reinitialize(&initialize).await?;
            }
            if let Some(reply) = self.round_trip(request).await? {
                return Ok(reply);
            }
            // The server forgot the session. Re-initialize once and retry;
            // a second loss fails the call instead of looping.
            let initialize = self
                .initialize
                .clone()
                .filter(|init| init.get("id") != request.get("id"))
                .ok_or_else(|| ToolError::Message("http mcp session lost".into()))?;
            self.reinitialize(&initialize).await?;
            self.round_trip(request).await?.ok_or_else(|| {
                ToolError::Message("http mcp session lost again after re-initialize".into())
            })
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
            let _ = self.post(notification).await;
            Ok(())
        }
    }

    fn take_list_changed(&mut self) -> bool {
        std::mem::take(&mut self.list_changed)
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
        assert_eq!(
            attach_mcp_servers(&mut reg, &config, None).await.unwrap(),
            1
        );
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
        assert_eq!(
            attach_mcp_servers(&mut reg, &config, None).await.unwrap(),
            1
        );

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
        let n = attach_mcp_servers(&mut reg, &config, None).await.unwrap();
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
        let n = attach_mcp_servers(&mut reg, &config, None).await.unwrap();
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
        let err = attach_mcp_servers(&mut reg2, &deny_cfg, None)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("denied") || err.to_string().contains("egress"),
            "{err}"
        );
    }

    /// What the streamable-HTTP test server does with a request.
    #[cfg(feature = "model-http")]
    #[derive(Default)]
    struct StreamableServer {
        /// Session ids issued so far; the last one is the live session.
        sessions: Vec<String>,
        /// Answer 404 to every session id, as a server that keeps restarting would.
        forget_every_session: bool,
        /// Issue no sessions: accept requests without one, 404 any that carry one.
        stateless: bool,
        /// Answer the next initialize with 503, as a server mid-restart would.
        fail_next_initialize: bool,
        /// Tools the server currently offers.
        tools: Vec<&'static str>,
        /// (method, session header, accept header) per request.
        seen: Vec<(String, Option<String>, String)>,
        /// `mcp-protocol-version` header per request.
        versions: Vec<Option<String>>,
    }

    /// Read one HTTP request: lower-cased headers and the JSON body.
    #[cfg(feature = "model-http")]
    async fn read_http_request(
        sock: &mut tokio::net::TcpStream,
    ) -> Option<(std::collections::HashMap<String, String>, Value)> {
        let mut raw = Vec::new();
        let mut buf = [0u8; 4096];
        let head_end = loop {
            let n = sock.read(&mut buf).await.ok().filter(|n| *n > 0)?;
            raw.extend_from_slice(&buf[..n]);
            if let Some(end) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
                break end + 4;
            }
        };
        let headers: std::collections::HashMap<_, _> = String::from_utf8_lossy(&raw[..head_end])
            .lines()
            .skip(1)
            .filter_map(|line| line.split_once(':'))
            .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string()))
            .collect();
        let len: usize = headers.get("content-length")?.parse().ok()?;
        while raw.len() < head_end + len {
            let n = sock.read(&mut buf).await.ok().filter(|n| *n > 0)?;
            raw.extend_from_slice(&buf[..n]);
        }
        Some((
            headers,
            serde_json::from_slice(&raw[head_end..head_end + len]).ok()?,
        ))
    }

    /// Serve the streamable HTTP transport on an ephemeral port: replies are
    /// event streams, `initialize` issues a session, and every later request
    /// must carry it. Calling `ping` withdraws it and announces the change.
    #[cfg(feature = "model-http")]
    async fn streamable_server(state: Arc<std::sync::Mutex<StreamableServer>>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                let Some((headers, body)) = read_http_request(&mut sock).await else {
                    continue;
                };
                let method = body["method"].as_str().unwrap_or("").to_string();
                let session = headers.get("mcp-session-id").cloned();
                let response = {
                    let mut state = state.lock().unwrap();
                    state
                        .versions
                        .push(headers.get("mcp-protocol-version").cloned());
                    state.seen.push((
                        method.clone(),
                        session.clone(),
                        headers.get("accept").cloned().unwrap_or_default(),
                    ));
                    let live = if state.stateless {
                        session.is_none()
                    } else {
                        !state.forget_every_session
                            && session.is_some()
                            && session.as_ref() == state.sessions.last()
                    };
                    if method == "initialize" && std::mem::take(&mut state.fail_next_initialize) {
                        "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".into()
                    } else if method == "initialize" {
                        let header = if state.stateless {
                            String::new()
                        } else {
                            let id = format!("session-{}", state.sessions.len() + 1);
                            state.sessions.push(id.clone());
                            format!("Mcp-Session-Id: {id}\r\n")
                        };
                        let reply = json!({"jsonrpc":"2.0","id":body["id"],"result":{"protocolVersion":"2025-03-26"}});
                        format!(
                            "HTTP/1.1 200 OK\r\n{header}Content-Type: text/event-stream\r\nConnection: close\r\n\r\nevent: message\r\ndata: {reply}\r\n\r\n"
                        )
                    } else if !live {
                        "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                            .into()
                    } else if body.get("id").is_none() {
                        "HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                            .into()
                    } else {
                        let mut events = String::new();
                        let result = if method == "tools/list" {
                            let tools: Vec<_> = state
                                .tools
                                .iter()
                                .map(|name| json!({"name": name, "inputSchema": {"type":"object"}}))
                                .collect();
                            json!({"tools": tools})
                        } else {
                            let name = body["params"]["name"].as_str().unwrap_or("").to_string();
                            if name == "ping" {
                                state.tools.retain(|tool| *tool != "ping");
                                let notice = json!({"jsonrpc":"2.0","method":"notifications/tools/list_changed"});
                                events.push_str(&format!("data: {notice}\n\n"));
                            }
                            json!({"content":[{"type":"text","text": format!("{name} ok ✓")}]})
                        };
                        let reply = json!({"jsonrpc":"2.0","id":body["id"],"result":result});
                        // One data line per pretty-printed line exercises multi-line reassembly.
                        let data: String = serde_json::to_string_pretty(&reply)
                            .unwrap()
                            .lines()
                            .map(|line| format!("data: {line}\n"))
                            .collect();
                        events.push_str(&format!("event: message\n{data}\n"));
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n{events}"
                        )
                    }
                };
                // Trickle a few bytes at a time so chunks split CRLFs and the
                // multibyte check mark.
                let _ = sock.set_nodelay(true);
                for piece in response.as_bytes().chunks(3) {
                    let _ = sock.write_all(piece).await;
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                }
            }
        });
        format!("http://{addr}/mcp")
    }

    #[cfg(feature = "model-http")]
    async fn attach_streamable(
        state: &Arc<std::sync::Mutex<StreamableServer>>,
        dir: &tempfile::TempDir,
    ) -> ToolRegistry {
        let url = streamable_server(Arc::clone(state)).await;
        let mut config = Config::default();
        config.tools.mcp_servers = vec![McpServerSettings {
            name: "remote".into(),
            command: String::new(),
            args: vec![],
            transport: "http".into(),
            url: Some(url),
            token_env: None,
            framing: McpFraming::ContentLength,
            timeout_secs: 10,
        }];
        let mut reg = ToolRegistry::with_builtins(
            dir.path(),
            vec!["read_file".into()],
            30,
            config.network.clone(),
        )
        .unwrap();
        assert_eq!(
            attach_mcp_servers(&mut reg, &config, None).await.unwrap(),
            2
        );
        reg
    }

    #[cfg(feature = "model-http")]
    async fn call_text(reg: &ToolRegistry, tool: &str) -> Result<String, String> {
        match reg.execute(tool, "{}").await {
            Ok(crate::tools::ToolOutput::Text(text)) => Ok(text),
            Ok(_) => Err("expected text".into()),
            Err(error) => Err(error.to_string()),
        }
    }

    #[cfg(feature = "model-http")]
    #[tokio::test]
    async fn streamable_http_lists_calls_and_follows_list_changes_in_a_session() {
        let state = Arc::new(std::sync::Mutex::new(StreamableServer {
            tools: vec!["ping", "pong"],
            ..Default::default()
        }));
        let dir = tempdir().unwrap();
        let reg = attach_streamable(&state, &dir).await;

        assert_eq!(
            call_text(&reg, "mcp.remote.ping").await.unwrap(),
            "ping ok ✓"
        );
        // The server withdrew `ping` and said so; the client re-lists and refuses it.
        let refused = call_text(&reg, "mcp.remote.ping").await.unwrap_err();
        assert!(refused.contains("no longer offered"), "{refused}");
        assert_eq!(
            call_text(&reg, "mcp.remote.pong").await.unwrap(),
            "pong ok ✓"
        );

        let state = state.lock().unwrap();
        assert_eq!(state.sessions, ["session-1"]);
        let methods: Vec<_> = state.seen.iter().map(|(m, _, _)| m.as_str()).collect();
        assert_eq!(
            methods,
            [
                "initialize",
                "notifications/initialized",
                "tools/list",
                "tools/call",
                "tools/list",
                "tools/call"
            ]
        );
        assert_eq!(state.versions[0], None);
        assert!(
            state.versions[1..]
                .iter()
                .all(|v| v.as_deref() == Some("2025-03-26"))
        );
        for (method, session, accept) in &state.seen[1..] {
            assert_eq!(session.as_deref(), Some("session-1"), "{method}");
            assert!(accept.contains("text/event-stream") && accept.contains("application/json"));
        }
    }

    #[cfg(feature = "model-http")]
    #[tokio::test]
    async fn a_lost_session_reinitializes_once_then_fails() {
        let state = Arc::new(std::sync::Mutex::new(StreamableServer {
            tools: vec!["pong", "other"],
            ..Default::default()
        }));
        let dir = tempdir().unwrap();
        let reg = attach_streamable(&state, &dir).await;

        // The server restarts and forgets session-1: one re-initialize recovers.
        state.lock().unwrap().sessions.push("restarted".into());
        assert_eq!(
            call_text(&reg, "mcp.remote.pong").await.unwrap(),
            "pong ok ✓"
        );
        assert_eq!(state.lock().unwrap().sessions.last().unwrap(), "session-3");

        // A server that never keeps a session gets one retry, not a loop.
        state.lock().unwrap().forget_every_session = true;
        let error = call_text(&reg, "mcp.remote.pong").await.unwrap_err();
        assert!(error.contains("re-initialize"), "{error}");
        let state = state.lock().unwrap();
        let tail: Vec<_> = state
            .seen
            .iter()
            .rev()
            .take(3)
            .map(|(m, _, _)| m.as_str())
            .collect();
        assert_eq!(
            tail,
            ["notifications/initialized", "initialize", "tools/call"]
        );
    }

    #[cfg(feature = "model-http")]
    #[tokio::test]
    async fn a_failed_reinitialize_is_retried_by_the_next_call() {
        let state = Arc::new(std::sync::Mutex::new(StreamableServer {
            tools: vec!["pong", "other"],
            ..Default::default()
        }));
        let dir = tempdir().unwrap();
        let reg = attach_streamable(&state, &dir).await;

        {
            let mut state = state.lock().unwrap();
            state.sessions.push("restarted".into());
            state.fail_next_initialize = true;
        }
        assert!(call_text(&reg, "mcp.remote.pong").await.is_err());
        assert_eq!(
            call_text(&reg, "mcp.remote.pong").await.unwrap(),
            "pong ok ✓"
        );
        let state = state.lock().unwrap();
        let (_, session, _) = state.seen.last().unwrap();
        assert_eq!(session.as_deref(), Some("session-3"));
    }

    #[cfg(feature = "model-http")]
    #[tokio::test]
    async fn a_server_that_restarts_stateless_recovers_without_a_session() {
        let state = Arc::new(std::sync::Mutex::new(StreamableServer {
            tools: vec!["pong", "other"],
            ..Default::default()
        }));
        let dir = tempdir().unwrap();
        let reg = attach_streamable(&state, &dir).await;

        state.lock().unwrap().stateless = true;
        assert_eq!(
            call_text(&reg, "mcp.remote.pong").await.unwrap(),
            "pong ok ✓"
        );
        let state = state.lock().unwrap();
        let (method, session, _) = state.seen.last().unwrap();
        assert_eq!((method.as_str(), session), ("tools/call", &None));
    }

    #[test]
    fn event_stream_decoder_handles_split_bytes_and_every_line_ending() {
        let stream =
            "\u{feff}data: {\"a\":\r\ndata: \"✓\"}\r\n\r\nevent: x\rdata: 2\r\rdata: 3\n\n";
        let mut decoder = SseDecoder::default();
        let mut events = Vec::new();
        for byte in stream.as_bytes().chunks(1) {
            events.extend(decoder.push(byte).unwrap());
        }
        assert_eq!(events, ["{\"a\":\n\"✓\"}", "2", "3"]);
    }

    #[test]
    fn event_stream_decoder_finishes_an_event_ended_by_cr() {
        let mut decoder = SseDecoder::default();
        assert_eq!(decoder.push(b"data: 1\r\r").unwrap(), ["1"]);
        assert!(decoder.push(b"\ndata: 2\n\n").unwrap() == ["2"]);
    }

    #[test]
    fn event_stream_decoder_bounds_unterminated_input() {
        let mut decoder = SseDecoder::default();
        let line = vec![b'x'; 64 * 1024];
        let error = (0..)
            .find_map(|_| decoder.push(&line).err())
            .unwrap()
            .to_string();
        assert!(error.contains("exceeds"), "{error}");
        let mut decoder = SseDecoder::default();
        let error = (0..)
            .find_map(|_| decoder.push(b"data: xxxxxxxxxxxxxxxx\n").err())
            .unwrap()
            .to_string();
        assert!(error.contains("exceeds"), "{error}");
    }

    #[test]
    fn gateway_mode_refuses_direct_remote_servers() {
        let mut direct = mock_stdio_server("docs");
        direct.url = Some("https://tools.example/mcp".into());
        direct.token_env = Some("DOCS_TOKEN".into());
        assert!(
            check_gateway_servers(&[direct])
                .unwrap_err()
                .contains("refused")
        );
        assert!(check_gateway_servers(&[mock_stdio_server(MCP_GATEWAY_NAME)]).is_err());
        assert!(check_gateway_servers(&[mock_stdio_server("local")]).is_ok());
    }

    /// One-tool MCP endpoint on an ephemeral port that reports each request's
    /// `authorization` header.
    #[cfg(feature = "model-http")]
    async fn auth_echo_server() -> (String, mpsc::UnboundedReceiver<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                let mut buf = vec![0u8; 8192];
                let n = sock.read(&mut buf).await.unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]).to_string();
                let auth = req
                    .lines()
                    .find_map(|l| {
                        l.to_ascii_lowercase()
                            .strip_prefix("authorization: ")
                            .map(str::to_owned)
                    })
                    .unwrap_or_default();
                let _ = tx.send(auth);
                let body = if req.contains("tools/list") {
                    json!({"jsonrpc":"2.0","id":1,"result":{"tools":[{"name":"ping","description":"ping","inputSchema":{"type":"object"}}]}})
                } else if req.contains("tools/call") {
                    json!({"jsonrpc":"2.0","id":2,"result":{"content":[{"type":"text","text":"pong"}]}})
                } else {
                    json!({"jsonrpc":"2.0","id":0,"result":{}})
                }
                .to_string();
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = sock.write_all(resp.as_bytes()).await;
            }
        });
        (format!("http://{addr}/mcp"), rx)
    }

    #[cfg(feature = "model-http")]
    #[tokio::test]
    async fn gateway_tool_works_with_the_job_credential_only() {
        let (url, mut auth) = auth_echo_server().await;
        let dir = tempdir().unwrap();
        let mut config = Config {
            network: NetworkSettings {
                egress: EgressMode::Allowlist,
                allow_hosts: vec!["127.0.0.1".into()],
            },
            ..Default::default()
        };
        config.tools.mcp_gateway = Some(McpGatewaySettings {
            url,
            timeout_secs: 5,
        });
        let registry = || {
            ToolRegistry::with_builtins(
                dir.path(),
                vec!["read_file".into()],
                30,
                config.network.clone(),
            )
            .unwrap()
        };

        // No grant: the gateway is never contacted.
        let err = attach_mcp_servers(&mut registry(), &config, None)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("job credential"), "{err}");

        let job =
            JobCredentialHandle::from_live("job-token-458", i64::MAX, "tenant", "ns", 0).unwrap();
        let mut reg = registry();
        assert_eq!(
            attach_mcp_servers(&mut reg, &config, Some(&job))
                .await
                .unwrap(),
            1
        );
        match reg.execute("mcp.gateway.ping", "{}").await.unwrap() {
            crate::tools::ToolOutput::Text(t) => assert_eq!(t, "pong"),
            _ => panic!("expected text"),
        }
        let mut seen = Vec::new();
        while let Ok(header) = auth.try_recv() {
            seen.push(header);
        }
        assert!(
            !seen.is_empty() && seen.iter().all(|h| h == "bearer job-token-458"),
            "{seen:?}"
        );

        // An expired grant fails the call closed instead of sending it.
        job.replace(crate::tools::JobCredential::live(
            "old", 2, "tenant", "ns", 1,
        ));
        assert!(reg.execute("mcp.gateway.ping", "{}").await.is_err());
        assert!(auth.try_recv().is_err());
    }

    /// A site-agent turn offers only the gateway tools its settings name:
    /// one the gateway lists but the settings omit is neither registered
    /// nor callable (#487).
    #[cfg(feature = "model-http")]
    #[tokio::test]
    async fn site_agent_offers_only_the_gateway_tools_it_names() {
        let (url, _auth) = auth_echo_server().await;
        let dir = tempdir().unwrap();
        let mut config = Config {
            network: NetworkSettings {
                egress: EgressMode::Allowlist,
                allow_hosts: vec!["127.0.0.1".into()],
            },
            ..Default::default()
        };
        config.profile.name = crate::config::SITE_AGENT_PROFILE.into();
        config.tools.mcp_gateway = Some(McpGatewaySettings {
            url,
            timeout_secs: 5,
        });
        let job =
            JobCredentialHandle::from_live("job-token-487", i64::MAX, "tenant", "ns", 0).unwrap();
        let registry = || {
            ToolRegistry::with_builtins(
                dir.path(),
                vec!["report".into()],
                30,
                config.network.clone(),
            )
            .unwrap()
        };

        config.tools.enabled = vec!["report".into()];
        let mut unnamed = registry();
        assert_eq!(
            attach_mcp_servers(&mut unnamed, &config, Some(&job))
                .await
                .unwrap(),
            0
        );
        assert!(unnamed.execute("mcp.gateway.ping", "{}").await.is_err());

        config.tools.enabled = vec!["report".into(), "mcp.gateway.ping".into()];
        let mut named = registry();
        assert_eq!(
            attach_mcp_servers(&mut named, &config, Some(&job))
                .await
                .unwrap(),
            1
        );
        assert!(named.execute("mcp.gateway.ping", "{}").await.is_ok());
    }
}
