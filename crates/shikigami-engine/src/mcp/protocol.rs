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
}

impl<T: Transport> Client<T> {
    pub fn new(transport: T, deadline: Duration) -> Self {
        Self {
            transport,
            next_id: 1,
            deadline,
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

    pub async fn list_tools(&mut self) -> Result<Vec<ToolInfo>, ToolError> {
        let result = self.request("tools/list", json!({})).await?;
        Ok(result
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
            })
            .collect())
    }

    /// Call a remote tool. A result flagged `isError` is a failed tool call:
    /// the projected error body is returned as the failure detail so
    /// governance reports and events record `ok = false` instead of a
    /// successful call whose text happens to describe a denial.
    async fn call_tool(&mut self, name: &str, args_json: &str) -> Result<String, ToolError> {
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
        let response = match tokio::time::timeout(self.deadline, self.transport.exchange(&request))
            .await
        {
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

pub(super) async fn attach_tools<T: Transport + 'static>(
    registry: &mut ToolRegistry,
    server_name: &str,
    mut client: Client<T>,
) -> Result<usize, ToolError> {
    client.initialize().await;
    let tools = client.list_tools().await?;
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
        };
        let error = Client::new(transport, Duration::from_secs(5))
            .call_tool("sekai.actions.submit", "{}")
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("permission_denied"), "{error}");
        assert!(error.contains("op-1"), "{error}");
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
