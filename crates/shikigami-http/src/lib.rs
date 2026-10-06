//! OpenAI-compatible HTTP model adapter.

use async_trait::async_trait;

use shikigami_engine::config::Config;
use shikigami_engine::model::{
    ChatMessage, ModelError, ModelPort, ModelTurn, TokenUsage, ToolCall, effective_model_name,
};
use shikigami_engine::tools::ToolDef;

/// OpenAI-compatible chat-completions adapter for ungoverned runs.
pub struct HttpModel {
    client: reqwest::Client,
    base_url: String,
    model: String,
    api_key: String,
    content_digest: String,
}

impl HttpModel {
    pub fn from_config(config: &Config) -> Result<Self, ModelError> {
        let base_url = config
            .model
            .base_url
            .clone()
            .unwrap_or_else(|| "https://api.openai.com/v1".into());
        config
            .network
            .check_http_url(&base_url)
            .map_err(ModelError::Message)?;
        let api_key = std::env::var(&config.model.api_key_env).map_err(|_| {
            ModelError::Message(format!("missing API key env {}", config.model.api_key_env))
        })?;
        let model = effective_model_name(config);
        let mut digest_payload = format!("http\n{base_url}\n{model}").into_bytes();
        if let Some(path) = config.model.fallback.artifact_path.as_ref() {
            digest_payload.push(0xff);
            digest_payload.extend(std::fs::read(path)?);
        }
        Ok(Self {
            client: reqwest::Client::new(),
            base_url,
            // `auto` is the governed routing default. Preserve a useful
            // direct HTTP default when users switch adapters without adding a
            // model field to their local config.
            model,
            api_key,
            content_digest: shikigami_engine::fallback::sha256_hex(&digest_payload),
        })
    }
}

fn http_err(error: reqwest::Error) -> ModelError {
    ModelError::Http(error.to_string())
}

#[async_trait]
impl ModelPort for HttpModel {
    fn id(&self) -> &'static str {
        "http"
    }

    fn content_digest(&self) -> String {
        self.content_digest.clone()
    }

    fn fresh_for_child(&self, config: &Config) -> Result<Box<dyn ModelPort>, ModelError> {
        Ok(Box::new(Self::from_config(config)?))
    }

    async fn next_turn(
        &self,
        system: &str,
        messages: &[ChatMessage],
        tools: &[ToolDef],
    ) -> Result<ModelTurn, ModelError> {
        let mut api_messages = vec![serde_json::json!({"role":"system","content":system})];
        for m in messages {
            if m.role == "tool" {
                api_messages.push(serde_json::json!({
                    "role": "tool",
                    "tool_call_id": m.tool_call_id,
                    "content": m.content,
                }));
            } else if !m.tool_calls.is_empty() {
                let calls: Vec<_> = m
                    .tool_calls
                    .iter()
                    .map(|c| {
                        serde_json::json!({
                            "id": c.id,
                            "type": "function",
                            "function": {
                                "name": c.name,
                                "arguments": c.args_json,
                            }
                        })
                    })
                    .collect();
                api_messages.push(serde_json::json!({
                    "role": "assistant",
                    "content": m.content,
                    "tool_calls": calls,
                }));
            } else {
                api_messages.push(serde_json::json!({
                    "role": m.role,
                    "content": m.content,
                }));
            }
        }
        let api_tools: Vec<_> = tools
            .iter()
            .map(|t| {
                let params: serde_json::Value =
                    serde_json::from_str(&t.schema).unwrap_or(serde_json::json!({}));
                serde_json::json!({
                    "type": "function",
                    "function": {
                        "name": t.name,
                        "description": t.description,
                        "parameters": params,
                    }
                })
            })
            .collect();

        let url = format!("{}/chat/completions", self.base_url.trim_end_matches('/'));
        let body = serde_json::json!({
            "model": self.model,
            "messages": api_messages,
            "tools": api_tools,
        });
        let resp = self
            .client
            .post(url)
            .bearer_auth(&self.api_key)
            .json(&body)
            .send()
            .await
            .map_err(http_err)?
            .error_for_status()
            .map_err(http_err)?
            .json::<serde_json::Value>()
            .await
            .map_err(http_err)?;

        let choice = resp
            .pointer("/choices/0/message")
            .ok_or_else(|| ModelError::Message("missing choices".into()))?;
        let content = choice
            .get("content")
            .and_then(|c| c.as_str())
            .unwrap_or("")
            .to_string();
        let mut tool_calls = Vec::new();
        if let Some(arr) = choice.get("tool_calls").and_then(|t| t.as_array()) {
            for (i, c) in arr.iter().enumerate() {
                let id = c
                    .get("id")
                    .and_then(|v| v.as_str())
                    .unwrap_or(&format!("call_{i}"))
                    .to_string();
                let name = c
                    .pointer("/function/name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let args_json = c
                    .pointer("/function/arguments")
                    .and_then(|v| v.as_str())
                    .unwrap_or("{}")
                    .to_string();
                tool_calls.push(ToolCall {
                    id,
                    name,
                    args_json,
                });
            }
        }
        let usage = resp.get("usage").and_then(|u| {
            let input = u
                .get("prompt_tokens")
                .or_else(|| u.get("input_tokens"))
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let output = u
                .get("completion_tokens")
                .or_else(|| u.get("output_tokens"))
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            if input == 0 && output == 0 {
                None
            } else {
                Some(TokenUsage {
                    input_tokens: input,
                    output_tokens: output,
                })
            }
        });
        Ok(ModelTurn {
            content,
            tool_calls,
            usage,
        })
    }
}
