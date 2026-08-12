//! Host-authz HTTP callback governance adapter.
//!
//! For each tool authorization, POSTs a bounded JSON request to a host URL
//! (for example Aldunis Code PermissionBroker) and only proceeds on allow.

use std::env;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Value, json};

use crate::config::Config;
use crate::model::{ChatMessage, ModelTurn};
use crate::tools::{ToolDef, builtin_is_authorized};

use super::{GovernanceError, GovernancePort, RunHandle, RunOutcome};

const REQUEST_VERSION: &str = "host-authz.request/v1";
const MAX_ARGS_CHARS: usize = 4_096;
const DEFAULT_TIMEOUT_MS: u64 = 120_000;

/// Tools that skip the host callback (harness-internal / non-mutating defaults).
fn tool_skips_host_authz(name: &str) -> bool {
    matches!(
        name,
        "read_file"
            | "glob"
            | "grep"
            | "report"
            | "escalate"
            | "todo_write"
            | "bash_job_status"
            | "bash_job_logs"
    )
}

fn redact_value(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for (key, nested) in map.iter_mut() {
                if key_looks_secret(key) {
                    *nested = Value::String("[redacted]".into());
                } else if matches!(
                    key.as_str(),
                    "content" | "new" | "old" | "patch" | "args_json"
                ) && nested.as_str().is_some_and(|s| s.len() > 120)
                {
                    *nested = Value::String("[content redacted]".into());
                } else {
                    redact_value(nested);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                redact_value(item);
            }
        }
        _ => {}
    }
}

fn redact_args_json(args_json: &str) -> String {
    // Redact first, then bound size so truncation cannot leak secrets.
    let redacted = match serde_json::from_str::<Value>(args_json) {
        Ok(mut value) => {
            redact_value(&mut value);
            value.to_string()
        }
        Err(_) => "[unparseable args]".into(),
    };
    if redacted.chars().count() > MAX_ARGS_CHARS {
        let mut out = redacted.chars().take(MAX_ARGS_CHARS).collect::<String>();
        out.push('…');
        out
    } else {
        redacted
    }
}

fn key_looks_secret(key: &str) -> bool {
    let lower = key.to_ascii_lowercase();
    lower.contains("token")
        || lower.contains("secret")
        || lower.contains("password")
        || lower.contains("authorization")
        || lower.contains("api_key")
        || lower.contains("apikey")
}

pub struct HttpCallbackGovernance {
    endpoint: String,
    principal: String,
    fail_closed: bool,
    enabled_tools: Vec<String>,
    token: Option<String>,
    timeout: Duration,
}

impl HttpCallbackGovernance {
    pub fn from_config(config: &Config) -> Result<Self, GovernanceError> {
        #[cfg(not(feature = "model-http"))]
        {
            let _ = config;
            return Err(GovernanceError::Unavailable(
                "http-callback requires the model-http feature (reqwest)".into(),
            ));
        }
        #[cfg(feature = "model-http")]
        {
            let endpoint = config
                .governance
                .endpoint
                .clone()
                .filter(|s| !s.trim().is_empty())
                .ok_or_else(|| {
                    GovernanceError::Unavailable(
                        "governance adapter `http-callback` requires governance.endpoint".into(),
                    )
                })?;
            Self::validate_endpoint(&endpoint)?;
            let token = config
                .governance
                .token_env
                .as_ref()
                .and_then(|name| env::var(name).ok())
                .filter(|s| !s.is_empty());
            Ok(Self {
                endpoint,
                principal: config.governance.principal.clone(),
                fail_closed: config.governance.fail_closed,
                enabled_tools: config.tools.effective_enabled(),
                token,
                timeout: Duration::from_millis(DEFAULT_TIMEOUT_MS),
            })
        }
    }

    fn unavailable(&self, message: String) -> Result<(), GovernanceError> {
        // http-callback / host-authz never fail-open on transport or policy
        // errors: an unreachable broker must not permit tool execution.
        // (`fail_closed` still surfaces in doctor/health text for operators.)
        Err(GovernanceError::Unavailable(message))
    }

    fn validate_endpoint(endpoint: &str) -> Result<(), GovernanceError> {
        let parsed = url::Url::parse(endpoint)
            .map_err(|e| GovernanceError::Unavailable(format!("invalid endpoint URL: {e}")))?;
        match parsed.scheme() {
            "https" => Ok(()),
            "http" => {
                let host = parsed.host_str().unwrap_or("");
                if matches!(host, "127.0.0.1" | "localhost" | "::1") {
                    Ok(())
                } else {
                    Err(GovernanceError::Unavailable(
                        "http-callback endpoint may use plain HTTP only on loopback (127.0.0.1, localhost, ::1); use https for remote hosts".into(),
                    ))
                }
            }
            other => Err(GovernanceError::Unavailable(format!(
                "http-callback endpoint scheme must be http or https, got `{other}`"
            ))),
        }
    }

    /// Match ToolRegistry expansion: bash enables background helpers; MCP tools
    /// are external registrations authorized by the host callback.
    fn tool_is_authorized(&self, name: &str) -> bool {
        builtin_is_authorized(&self.enabled_tools, name) || name.starts_with("mcp.")
    }
}

#[async_trait]
impl GovernancePort for HttpCallbackGovernance {
    fn id(&self) -> &'static str {
        "http-callback"
    }

    fn health_detail(&self) -> String {
        format!(
            "host-authz HTTP callback (principal {}, endpoint set, fail_closed={})",
            self.principal, self.fail_closed
        )
    }

    fn health_ok(&self) -> bool {
        !self.endpoint.is_empty()
    }

    async fn begin_run(
        &self,
        run_id: &str,
        _task: &str,
        logical_operation_id: Option<&str>,
    ) -> Result<RunHandle, GovernanceError> {
        Ok(RunHandle {
            run_id: run_id.into(),
            operation_id: logical_operation_id
                .map(str::to_string)
                .unwrap_or_else(|| format!("host-{run_id}")),
            namespace: "host-authz".into(),
        })
    }

    async fn plan_turn(
        &self,
        _handle: &RunHandle,
        system: &str,
        messages: &[ChatMessage],
        tools: &[ToolDef],
        local_model: &dyn crate::model::ModelPort,
    ) -> Result<ModelTurn, GovernanceError> {
        local_model
            .next_turn(system, messages, tools)
            .await
            .map_err(|e| GovernanceError::Message(e.to_string()))
    }

    async fn authorize_tool(
        &self,
        handle: &RunHandle,
        name: &str,
        args_json: &str,
    ) -> Result<(), GovernanceError> {
        if !self.tool_is_authorized(name) {
            return Err(GovernanceError::Denied(format!(
                "http-callback policy denies tool `{name}` (not enabled)"
            )));
        }
        if tool_skips_host_authz(name) {
            return Ok(());
        }

        #[cfg(feature = "model-http")]
        {
            const MAX_RESPONSE_BYTES: usize = 16_384;
            let client = reqwest::Client::builder()
                .timeout(self.timeout)
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .map_err(|e| GovernanceError::Unavailable(e.to_string()))?;
            let body = json!({
                "version": REQUEST_VERSION,
                "run_id": handle.run_id,
                "operation_id": handle.operation_id,
                "tool": name,
                "args_json": redact_args_json(args_json),
            });
            let mut request = client
                .post(&self.endpoint)
                .header("content-type", "application/json")
                .json(&body);
            if let Some(token) = &self.token {
                let value = if token.to_ascii_lowercase().starts_with("bearer ") {
                    token.clone()
                } else {
                    format!("Bearer {token}")
                };
                request = request.header("authorization", value);
            }
            let response = match request.send().await {
                Ok(response) => response,
                Err(e) => {
                    return self.unavailable(format!("host-authz request failed: {e}"));
                }
            };
            if response.status().is_redirection() {
                return self.unavailable(
                    "host-authz refused HTTP redirect (authorization callbacks do not follow redirects)"
                        .into(),
                );
            }
            if let Some(len) = response.content_length()
                && len as usize > MAX_RESPONSE_BYTES
            {
                return self.unavailable(format!(
                    "host-authz Content-Length {len} exceeds {MAX_RESPONSE_BYTES} bytes"
                ));
            }
            if !response.status().is_success() {
                let status = response.status();
                let mut limited = Vec::new();
                let mut response = response;
                while let Ok(Some(chunk)) = response.chunk().await {
                    if limited.len() + chunk.len() > 512 {
                        break;
                    }
                    limited.extend_from_slice(&chunk);
                }
                let text = String::from_utf8_lossy(&limited);
                return self.unavailable(format!("host-authz HTTP {status}: {text}"));
            }
            let mut bytes = Vec::new();
            let mut response = response;
            loop {
                match response.chunk().await {
                    Ok(Some(chunk)) => {
                        if bytes.len() + chunk.len() > MAX_RESPONSE_BYTES {
                            return self.unavailable(format!(
                                "host-authz response exceeded {MAX_RESPONSE_BYTES} bytes"
                            ));
                        }
                        bytes.extend_from_slice(&chunk);
                    }
                    Ok(None) => break,
                    Err(e) => {
                        return self.unavailable(format!("host-authz read body failed: {e}"));
                    }
                }
            }
            let value: Value = match serde_json::from_slice(&bytes) {
                Ok(value) => value,
                Err(e) => {
                    return self.unavailable(format!("host-authz invalid JSON: {e}"));
                }
            };
            let decision = value
                .get("decision")
                .or_else(|| value.get("behavior"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            match decision {
                "allow" | "permit" | "allowed_once" => Ok(()),
                "deny" | "decline" | "denied" => {
                    let message = value
                        .get("message")
                        .and_then(|v| v.as_str())
                        .unwrap_or("host denied tool");
                    Err(GovernanceError::Denied(message.into()))
                }
                other => self.unavailable(format!("host-authz unexpected decision `{other}`")),
            }
        }
        #[cfg(not(feature = "model-http"))]
        {
            let _ = (handle, name, args_json);
            Err(GovernanceError::Unavailable(
                "http-callback requires the model-http feature (reqwest)".into(),
            ))
        }
    }

    async fn report_tool(
        &self,
        _handle: &RunHandle,
        _name: &str,
        _ok: bool,
        _detail: &str,
    ) -> Result<(), GovernanceError> {
        Ok(())
    }

    async fn complete_run(
        &self,
        _handle: &RunHandle,
        _outcome: RunOutcome,
    ) -> Result<(), GovernanceError> {
        Ok(())
    }
}

#[cfg(all(test, feature = "model-http"))]
mod tests {
    use super::*;
    use crate::config::{Config, GovernanceSettings, ToolsSettings};
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::Mutex;

    async fn spawn_decision_server(
        decision: &'static str,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 8192];
            let _ = socket.read(&mut buf).await;
            let body = format!(r#"{{"decision":"{decision}","message":"test"}}"#);
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = socket.write_all(response.as_bytes()).await;
        });
        (format!("http://{addr}/authz"), handle)
    }

    fn config_with_endpoint(endpoint: &str) -> Config {
        Config {
            governance: GovernanceSettings {
                adapter: "http-callback".into(),
                endpoint: Some(endpoint.into()),
                principal: "test".into(),
                fail_closed: true,
                namespace: "default".into(),
                token_env: None,
            },
            tools: ToolsSettings {
                enabled: vec!["read_file".into(), "write_file".into(), "bash".into()],
                ..ToolsSettings::default()
            },
            ..Config::default()
        }
    }

    #[tokio::test]
    async fn allow_and_deny_from_host() {
        let (url, task) = spawn_decision_server("allow").await;
        let gov = HttpCallbackGovernance::from_config(&config_with_endpoint(&url)).unwrap();
        let handle = gov.begin_run("run-1", "task", None).await.unwrap();
        gov.authorize_tool(&handle, "write_file", r#"{"path":"a.txt","content":"x"}"#)
            .await
            .unwrap();
        task.await.unwrap();

        let (url, task) = spawn_decision_server("deny").await;
        let gov = HttpCallbackGovernance::from_config(&config_with_endpoint(&url)).unwrap();
        let handle = gov.begin_run("run-2", "task", None).await.unwrap();
        let err = gov
            .authorize_tool(&handle, "write_file", r#"{"path":"a.txt","content":"x"}"#)
            .await
            .unwrap_err();
        assert!(matches!(err, GovernanceError::Denied(_)));
        task.await.unwrap();
    }

    #[tokio::test]
    async fn transport_errors_deny_even_when_not_fail_closed() {
        let mut config = config_with_endpoint("http://127.0.0.1:1/authz");
        config.governance.fail_closed = false;
        let gov = HttpCallbackGovernance::from_config(&config).unwrap();
        assert!(!gov.fail_closed);
        let handle = gov.begin_run("run-fo", "task", None).await.unwrap();
        let err = gov
            .authorize_tool(&handle, "write_file", r#"{"path":"a.txt","content":"x"}"#)
            .await
            .expect_err("host unreachable must deny, not Ok(())");
        assert!(
            matches!(err, GovernanceError::Unavailable(_)),
            "expected Unavailable, got {err}"
        );
    }

    #[tokio::test]
    async fn read_tools_skip_host_callback() {
        // Server that would hang if called.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let called = Arc::new(Mutex::new(false));
        let flag = Arc::clone(&called);
        let task = tokio::spawn(async move {
            if let Ok((mut socket, _)) = listener.accept().await {
                *flag.lock().await = true;
                let mut buf = vec![0u8; 1024];
                let _ = socket.read(&mut buf).await;
            }
        });
        let gov = HttpCallbackGovernance::from_config(&config_with_endpoint(&format!(
            "http://{addr}/authz"
        )))
        .unwrap();
        let handle = gov.begin_run("run-r", "task", None).await.unwrap();
        gov.authorize_tool(&handle, "read_file", r#"{"path":"a.txt"}"#)
            .await
            .unwrap();
        // Give any accidental HTTP a moment.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!*called.lock().await);
        task.abort();
    }

    #[test]
    fn redacts_long_content_and_secrets() {
        let raw = r#"{"path":"a.txt","content":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","api_key":"sk-secret"}"#;
        let redacted = redact_args_json(raw);
        assert!(redacted.contains("[content redacted]") || redacted.contains("[redacted]"));
        assert!(!redacted.contains("sk-secret"));
    }
}
