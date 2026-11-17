use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::sync::Mutex;

use crate::tools::{ExternalTool, ToolDef, ToolError, ToolRegistry};

#[async_trait]
pub(super) trait Transport: Send {
    async fn exchange(&mut self, request: &Value) -> Result<Value, ToolError>;
    async fn send(&mut self, notification: &Value) -> Result<(), ToolError>;
    /// Whether the server announced a tool-list change since the last call.
    fn take_list_changed(&mut self) -> bool {
        false
    }
}

const MAX_TOOL_PAGES: usize = 64;
const TOOLS_LIST_CHANGED: &str = "notifications/tools/list_changed";

/// True for a server notification announcing that its tool list changed.
pub(super) fn is_list_changed(message: &Value) -> bool {
    message.get("id").is_none()
        && message.get("method").and_then(Value::as_str) == Some(TOOLS_LIST_CHANGED)
}

#[derive(Debug)]
pub(super) struct ToolInfo {
    pub name: String,
    pub description: String,
    pub input_schema: String,
}

pub(super) struct Client<T> {
    transport: T,
    next_id: u64,
    deadline: Duration,
    /// Set when the server announces its tool list changed; the next call re-lists.
    list_changed: bool,
    /// Tool names from the latest re-list. `None` until the server first announces
    /// a change, so a server that never does costs no extra request.
    offered: Option<HashSet<String>>,
}

impl<T: Transport> Client<T> {
    pub fn new(transport: T, deadline: Duration) -> Self {
        Self {
            transport,
            next_id: 1,
            deadline,
            list_changed: false,
            offered: None,
        }
    }

    pub async fn initialize(&mut self) {
        let _ = self
            .request(
                "initialize",
                json!({
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {"name": "shikigami", "version": env!("CARGO_PKG_VERSION")}
                }),
            )
            .await;
        let _ = self.notify("notifications/initialized", json!({})).await;
    }

    /// Every tool the server lists, following `nextCursor` up to
    /// `MAX_TOOL_PAGES` pages so a misbehaving server cannot loop us.
    pub async fn list_tools(&mut self) -> Result<Vec<ToolInfo>, ToolError> {
        let mut tools = Vec::new();
        let mut params = json!({});
        for _ in 0..MAX_TOOL_PAGES {
            let result = self.request("tools/list", params).await?;
            tools.extend(
                result
                    .get("tools")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(|tool| {
                        let name = tool.get("name")?.as_str()?.to_string();
                        if name.is_empty() {
                            return None;
                        }
                        Some(ToolInfo {
                            name,
                            description: tool
                                .get("description")
                                .and_then(Value::as_str)
                                .unwrap_or("")
                                .to_string(),
                            input_schema: tool
                                .get("inputSchema")
                                .cloned()
                                .unwrap_or_else(|| json!({"type":"object"}))
                                .to_string(),
                        })
                    }),
            );
            match result.get("nextCursor").and_then(Value::as_str) {
                Some(cursor) => params = json!({"cursor": cursor}),
                None => return Ok(tools),
            }
        }
        Err(ToolError::Message(format!(
            "mcp tools/list returned more than {MAX_TOOL_PAGES} pages"
        )))
    }

    /// Call a remote tool. A result flagged `isError` is a failed tool call:
    /// the projected error body is returned as the failure detail so
    /// governance reports and events record `ok = false` instead of a
    /// successful call whose text happens to describe a denial.
    ///
    /// The run's tool catalog is fixed at attach, so a re-list after a
    /// list-changed notice never adds tools; it refuses a call to a tool the
    /// server stopped offering instead of sending it.
    async fn call_tool(&mut self, name: &str, args_json: &str) -> Result<String, ToolError> {
        // A tool missing from the last list is re-checked on every call, so
        // one the server offers again becomes callable without a new notice.
        let withdrawn = |offered: &Option<HashSet<String>>| {
            offered
                .as_ref()
                .is_some_and(|offered| !offered.contains(name))
        };
        // Cleared before the re-list so a notice arriving during it stays
        // pending; restored when the re-list fails so it is retried.
        if std::mem::take(&mut self.list_changed) || withdrawn(&self.offered) {
            match self.list_tools().await {
                Ok(tools) => self.offered = Some(tools.into_iter().map(|tool| tool.name).collect()),
                Err(error) => {
                    self.list_changed = true;
                    return Err(error);
                }
            }
        }
        if withdrawn(&self.offered) {
            return Err(ToolError::Message(format!(
                "mcp tool `{name}` is no longer offered by the server"
            )));
        }
        let arguments = serde_json::from_str(args_json).unwrap_or_else(|_| json!({}));
        let result = self
            .request("tools/call", json!({"name": name, "arguments": arguments}))
            .await?;
        let texts = result
            .get("content")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|content| content.get("text").and_then(Value::as_str))
            .collect::<Vec<_>>();
        let body = if texts.is_empty() {
            result.to_string()
        } else {
            texts.join("\n")
        };
        if result.get("isError").and_then(Value::as_bool) == Some(true) {
            return Err(ToolError::Message(format!("mcp tool error: {body}")));
        }
        Ok(body)
    }

    /// One JSON-RPC request bounded by the configured deadline. On expiry the
    /// call fails closed; the remote side may still complete the operation, so
    /// callers must reconcile through the server's receipt surface rather than
    /// retry blindly.
    async fn request(&mut self, method: &str, params: Value) -> Result<Value, ToolError> {
        let id = self.next_id;
        self.next_id += 1;
        let request = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        });
        let outcome = tokio::time::timeout(self.deadline, self.transport.exchange(&request)).await;
        // A notice that arrived before a failure still counts.
        if self.transport.take_list_changed() {
            self.list_changed = true;
        }
        let response = match outcome {
            Ok(response) => response?,
            Err(_) => {
                return Err(ToolError::Message(format!(
                    "mcp `{method}` deadline_exceeded after {}s; outcome unknown, reconcile through the server receipt before retrying",
                    self.deadline.as_secs()
                )));
            }
        };
        if let Some(error) = response.get("error") {
            return Err(ToolError::Message(format!("mcp error: {error}")));
        }
        Ok(response.get("result").cloned().unwrap_or(Value::Null))
    }

    async fn notify(&mut self, method: &str, params: Value) -> Result<(), ToolError> {
        self.transport
            .send(&json!({
                "jsonrpc": "2.0",
                "method": method,
                "params": params,
            }))
            .await
    }
}

/// Registers the server's listed tools. With `allowed`, only tools whose
/// full name it lists are registered, so a tool the server advertises but
/// the settings do not name is never offered or callable.
pub(super) async fn attach_tools<T: Transport + 'static>(
    registry: &mut ToolRegistry,
    server_name: &str,
    mut client: Client<T>,
    allowed: Option<&[String]>,
) -> Result<usize, ToolError> {
    client.initialize().await;
    let tools: Vec<_> = client
        .list_tools()
        .await?
        .into_iter()
        .filter(|tool| {
            allowed.is_none_or(|names| names.contains(&format!("mcp.{server_name}.{}", tool.name)))
        })
        .collect();
    let count = tools.len();
    let client = Arc::new(Mutex::new(client));
    for tool in tools {
        registry.register_external(Arc::new(RemoteTool {
            full_name: format!("mcp.{server_name}.{}", tool.name),
            remote_name: tool.name,
            description: tool.description,
            schema: tool.input_schema,
            client: Arc::clone(&client),
        }));
    }
    Ok(count)
}

struct RemoteTool<T> {
    pub full_name: String,
    pub remote_name: String,
    pub description: String,
    pub schema: String,
    pub client: Arc<Mutex<Client<T>>>,
}

#[async_trait]
impl<T: Transport + 'static> ExternalTool for RemoteTool<T> {
    fn definition(&self) -> ToolDef {
        ToolDef {
            name: self.full_name.clone(),
            description: self.description.clone(),
            schema: self.schema.clone(),
        }
    }

    async fn call(&self, args_json: &str) -> Result<String, ToolError> {
        self.client
            .lock()
            .await
            .call_tool(&self.remote_name, args_json)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    struct ScriptedTransport {
        responses: VecDeque<Value>,
        sent: Vec<Value>,
        /// Reported with the next reply, as a server would announce mid-stream.
        list_changed: bool,
    }

    #[async_trait]
    impl Transport for ScriptedTransport {
        async fn exchange(&mut self, request: &Value) -> Result<Value, ToolError> {
            self.sent.push(request.clone());
            self.responses
                .pop_front()
                .ok_or_else(|| ToolError::Message("missing scripted response".into()))
        }

        async fn send(&mut self, notification: &Value) -> Result<(), ToolError> {
            self.sent.push(notification.clone());
            Ok(())
        }

        fn take_list_changed(&mut self) -> bool {
            std::mem::take(&mut self.list_changed)
        }
    }

    #[tokio::test]
    async fn protocol_owns_initialize_discovery_and_call_projection() {
        let transport = ScriptedTransport {
            responses: VecDeque::from([
                json!({"jsonrpc":"2.0","id":1,"result":{}}),
                json!({"jsonrpc":"2.0","id":2,"result":{"tools":[{
                    "name":"ping","description":"Ping","inputSchema":{"type":"object"}
                }]}}),
                json!({"jsonrpc":"2.0","id":3,"result":{"content":[
                    {"type":"text","text":"one"},{"type":"text","text":"two"}
                ]}}),
            ]),
            sent: Vec::new(),
            list_changed: false,
        };
        let mut client = Client::new(transport, Duration::from_secs(5));
        client.initialize().await;
        let tools = client.list_tools().await.unwrap();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].name, "ping");
        assert_eq!(client.call_tool("ping", "{}").await.unwrap(), "one\ntwo");
        assert_eq!(client.transport.sent.len(), 4);
        assert_eq!(
            client.transport.sent[1]["method"],
            "notifications/initialized"
        );
    }

    #[tokio::test]
    async fn protocol_projects_json_rpc_errors() {
        let transport = ScriptedTransport {
            responses: VecDeque::from([json!({
                "jsonrpc":"2.0","id":1,"error":{"code":-1,"message":"denied"}
            })]),
            sent: Vec::new(),
            list_changed: false,
        };
        let error = Client::new(transport, Duration::from_secs(5))
            .list_tools()
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("denied"), "{error}");
    }

    #[tokio::test]
    async fn tool_results_flagged_is_error_fail_the_call() {
        let body =
            json!({"code":"permission_denied","message":"access denied","operation_id":"op-1"});
        let transport = ScriptedTransport {
            responses: VecDeque::from([json!({
                "jsonrpc":"2.0","id":1,"result":{
                    "content":[{"type":"text","text": body.to_string()}],
                    "structuredContent": body,
                    "isError": true
                }
            })]),
            sent: Vec::new(),
            list_changed: false,
        };
        let error = Client::new(transport, Duration::from_secs(5))
            .call_tool("sekai.actions.submit", "{}")
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("permission_denied"), "{error}");
        assert!(error.contains("op-1"), "{error}");
    }

    #[tokio::test]
    async fn a_failed_relist_stays_pending_until_one_succeeds() {
        let call = |id: u64| json!({"jsonrpc":"2.0","id":id,"result":{"content":[]}});
        let transport = ScriptedTransport {
            responses: VecDeque::from([
                call(1),
                json!({"jsonrpc":"2.0","id":2,"error":{"code":-1,"message":"busy"}}),
                json!({"jsonrpc":"2.0","id":3,"result":{"tools":[{"name":"other"}]}}),
                json!({"jsonrpc":"2.0","id":4,"result":{"tools":[{"name":"ping"}]}}),
                call(5),
            ]),
            sent: Vec::new(),
            list_changed: true,
        };
        let mut client = Client::new(transport, Duration::from_secs(5));
        client.call_tool("ping", "{}").await.unwrap();

        let busy = client
            .call_tool("ping", "{}")
            .await
            .unwrap_err()
            .to_string();
        assert!(busy.contains("busy"), "{busy}");
        let gone = client
            .call_tool("ping", "{}")
            .await
            .unwrap_err()
            .to_string();
        assert!(gone.contains("no longer offered"), "{gone}");
        // The server offers it again: the next call re-checks and goes through.
        client.call_tool("ping", "{}").await.unwrap();
        let methods: Vec<_> = client
            .transport
            .sent
            .iter()
            .map(|m| m["method"].clone())
            .collect();
        assert_eq!(
            methods,
            [
                "tools/call",
                "tools/list",
                "tools/list",
                "tools/list",
                "tools/call"
            ]
        );
    }

    #[tokio::test]
    async fn a_change_announced_before_a_failed_exchange_still_relists() {
        let transport = ScriptedTransport {
            responses: VecDeque::new(),
            sent: Vec::new(),
            list_changed: true,
        };
        let mut client = Client::new(transport, Duration::from_secs(5));
        assert!(client.call_tool("ping", "{}").await.is_err());
        assert!(client.call_tool("ping", "{}").await.is_err());
        assert_eq!(client.transport.sent[1]["method"], "tools/list");
    }

    #[tokio::test]
    async fn list_tools_follows_the_cursor_across_pages() {
        let transport = ScriptedTransport {
            responses: VecDeque::from([
                json!({"jsonrpc":"2.0","id":1,"result":{"tools":[{"name":"a"}],"nextCursor":"p2"}}),
                json!({"jsonrpc":"2.0","id":2,"result":{"tools":[{"name":"b"}]}}),
            ]),
            sent: Vec::new(),
            list_changed: false,
        };
        let mut client = Client::new(transport, Duration::from_secs(5));
        let names: Vec<_> = client
            .list_tools()
            .await
            .unwrap()
            .into_iter()
            .map(|t| t.name)
            .collect();
        assert_eq!(names, ["a", "b"]);
        assert_eq!(client.transport.sent[1]["params"]["cursor"], "p2");
    }

    struct HangingTransport;

    #[async_trait]
    impl Transport for HangingTransport {
        async fn exchange(&mut self, _request: &Value) -> Result<Value, ToolError> {
            std::future::pending().await
        }

        async fn send(&mut self, _notification: &Value) -> Result<(), ToolError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn requests_fail_closed_at_the_deadline() {
        let error = Client::new(HangingTransport, Duration::from_millis(20))
            .call_tool("sekai.actions.submit", "{}")
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("deadline_exceeded"), "{error}");
        assert!(error.contains("reconcile"), "{error}");
    }
}
