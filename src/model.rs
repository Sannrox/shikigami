//! Model turn sources for ungoverned (local) runs.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::config::Config;
use crate::tools::ToolDef;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChatMessage {
    pub role: String,
    pub content: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub tool_call_id: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<ToolCall>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    pub args_json: String,
}

/// Token counts when the provider (or script) reports them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
}

impl TokenUsage {
    pub fn total(self) -> u64 {
        self.input_tokens.saturating_add(self.output_tokens)
    }
}

/// Optional USD cost estimate when rates are configured on settings.
///
/// Amounts are integer **microdollars** (1_000_000 = $1.00). Absent rates ⇒
/// no estimate (never invent costs). Zero tokens with rates ⇒ zero cost.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CostEstimate {
    pub currency: String,
    pub input_usd_micros: u64,
    pub output_usd_micros: u64,
    pub total_usd_micros: u64,
}

impl CostEstimate {
    /// Compute cost from usage and per-million-token microdollar rates.
    /// Returns `None` unless **both** rates are provided.
    pub fn from_usage_and_rates(
        usage: TokenUsage,
        input_usd_micros_per_mtok: Option<u64>,
        output_usd_micros_per_mtok: Option<u64>,
    ) -> Option<Self> {
        let (in_rate, out_rate) = match (input_usd_micros_per_mtok, output_usd_micros_per_mtok) {
            (Some(i), Some(o)) => (i, o),
            _ => return None,
        };
        let input_usd_micros = usage.input_tokens.saturating_mul(in_rate) / 1_000_000;
        let output_usd_micros = usage.output_tokens.saturating_mul(out_rate) / 1_000_000;
        Some(Self {
            currency: "USD".into(),
            input_usd_micros,
            output_usd_micros,
            total_usd_micros: input_usd_micros.saturating_add(output_usd_micros),
        })
    }
}

#[derive(Debug, Clone)]
pub struct ModelTurn {
    pub content: String,
    pub tool_calls: Vec<ToolCall>,
    pub usage: Option<TokenUsage>,
}

#[derive(Debug, Error)]
pub enum ModelError {
    #[error("model: {0}")]
    Message(String),
    #[error("scripted turns exhausted")]
    ScriptExhausted,
    #[error("http model unavailable (enable feature model-http and set credentials)")]
    HttpUnavailable,
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[cfg(feature = "model-http")]
    #[error("http: {0}")]
    Http(#[from] reqwest::Error),
}

#[async_trait]
pub trait ModelPort: Send + Sync {
    fn id(&self) -> &'static str;
    async fn next_turn(
        &self,
        system: &str,
        messages: &[ChatMessage],
        tools: &[ToolDef],
    ) -> Result<ModelTurn, ModelError>;
}

pub fn from_config(config: &Config) -> Result<Box<dyn ModelPort>, ModelError> {
    match config.model.adapter.as_str() {
        "scripted" => Ok(Box::new(ScriptedModel::from_config(config)?)),
        "http" => {
            #[cfg(feature = "model-http")]
            {
                Ok(Box::new(HttpModel::from_config(config)?))
            }
            #[cfg(not(feature = "model-http"))]
            {
                Err(ModelError::HttpUnavailable)
            }
        }
        "plane" => Ok(Box::new(PlaneModelPlaceholder)),
        other => Err(ModelError::Message(format!(
            "unknown model adapter `{other}`"
        ))),
    }
}

/// Deterministic multi-turn script for tests and offline demos.
pub struct ScriptedModel {
    turns: std::sync::Mutex<Vec<ModelTurn>>,
}

impl ScriptedModel {
    pub fn from_config(config: &Config) -> Result<Self, ModelError> {
        let raw = config
            .model
            .script_json
            .clone()
            .unwrap_or_else(default_script_json);
        let wire: Vec<ScriptedTurn> = serde_json::from_str(&raw)?;
        let turns = wire
            .into_iter()
            .map(|t| ModelTurn {
                content: t.content.unwrap_or_default(),
                usage: t.usage,
                tool_calls: t
                    .tool_calls
                    .into_iter()
                    .enumerate()
                    .map(|(i, c)| ToolCall {
                        id: c.id.unwrap_or_else(|| format!("call_{i}")),
                        name: c.name,
                        args_json: c.args_json.unwrap_or_else(|| "{}".into()),
                    })
                    .collect(),
            })
            .collect();
        Ok(Self {
            turns: std::sync::Mutex::new(turns),
        })
    }

    pub fn from_turns(turns: Vec<ModelTurn>) -> Self {
        Self {
            turns: std::sync::Mutex::new(turns),
        }
    }
}

fn default_script_json() -> String {
    // Write a marker file then report — safe offline demo.
    r#"[
      {"tool_calls":[{"name":"write_file","args_json":"{\"path\":\"SHIKIGAMI_OK.txt\",\"content\":\"ok\\n\"}"}]},
      {"tool_calls":[{"name":"report","args_json":"{\"summary\":\"scripted run complete\",\"success\":true}"}]}
    ]"#
    .into()
}

#[derive(Deserialize)]
struct ScriptedTurn {
    content: Option<String>,
    #[serde(default)]
    tool_calls: Vec<ScriptedCall>,
    #[serde(default)]
    usage: Option<TokenUsage>,
}

#[derive(Deserialize)]
struct ScriptedCall {
    id: Option<String>,
    name: String,
    args_json: Option<String>,
}

#[async_trait]
impl ModelPort for ScriptedModel {
    fn id(&self) -> &'static str {
        "scripted"
    }

    async fn next_turn(
        &self,
        _system: &str,
        _messages: &[ChatMessage],
        _tools: &[ToolDef],
    ) -> Result<ModelTurn, ModelError> {
        let mut guard = self.turns.lock().expect("script lock");
        if guard.is_empty() {
            return Err(ModelError::ScriptExhausted);
        }
        Ok(guard.remove(0))
    }
}

struct PlaneModelPlaceholder;

#[async_trait]
impl ModelPort for PlaneModelPlaceholder {
    fn id(&self) -> &'static str {
        "plane"
    }

    async fn next_turn(
        &self,
        _system: &str,
        _messages: &[ChatMessage],
        _tools: &[ToolDef],
    ) -> Result<ModelTurn, ModelError> {
        Err(ModelError::Message(
            "model adapter `plane` is only used through sekai-chisei governance".into(),
        ))
    }
}

#[cfg(feature = "model-http")]
pub struct HttpModel {
    client: reqwest::Client,
    base_url: String,
    model: String,
    api_key: String,
}

const DEFAULT_HTTP_MODEL: &str = "gpt-4.1-mini";

/// Resolve the model name an adapter will actually use.
pub fn effective_model_name(config: &Config) -> String {
    if config.model.adapter == "http" && config.model.model == "auto" && !config.uses_plane_model()
    {
        DEFAULT_HTTP_MODEL.into()
    } else {
        config.model.model.clone()
    }
}

#[cfg(feature = "model-http")]
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
        Ok(Self {
            client: reqwest::Client::new(),
            base_url,
            // `auto` is the governed routing default. Preserve a useful
            // direct HTTP default when users switch adapters without adding a
            // model field to their local config.
            model: effective_model_name(config),
            api_key,
        })
    }
}

#[cfg(feature = "model-http")]
#[async_trait]
impl ModelPort for HttpModel {
    fn id(&self) -> &'static str {
        "http"
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
            .await?
            .error_for_status()?
            .json::<serde_json::Value>()
            .await?;

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
        let usage = body.get("usage").and_then(|u| {
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

#[cfg(test)]
mod cost_tests {
    use super::*;

    #[test]
    fn cost_none_without_rates() {
        let u = TokenUsage {
            input_tokens: 1_000_000,
            output_tokens: 500_000,
        };
        assert!(CostEstimate::from_usage_and_rates(u, None, None).is_none());
        assert!(CostEstimate::from_usage_and_rates(u, Some(1), None).is_none());
    }

    #[test]
    fn cost_from_rates() {
        // $1 / MTok input, $2 / MTok output → micros 1e6 and 2e6
        let u = TokenUsage {
            input_tokens: 1_000_000,
            output_tokens: 500_000,
        };
        let c = CostEstimate::from_usage_and_rates(u, Some(1_000_000), Some(2_000_000)).unwrap();
        assert_eq!(c.currency, "USD");
        assert_eq!(c.input_usd_micros, 1_000_000);
        assert_eq!(c.output_usd_micros, 1_000_000);
        assert_eq!(c.total_usd_micros, 2_000_000);
    }

    #[test]
    fn auto_resolves_to_http_fallback_only_for_http_adapter() {
        let mut config = Config::default();
        assert_eq!(effective_model_name(&config), "auto");

        config.model.adapter = "http".into();
        assert_eq!(effective_model_name(&config), "gpt-4.1-mini");

        config.governance.adapter = "sekai-chisei".into();
        assert_eq!(effective_model_name(&config), "auto");

        config.model.model = "openai/gpt-5.5".into();
        assert_eq!(effective_model_name(&config), "openai/gpt-5.5");
    }
}
