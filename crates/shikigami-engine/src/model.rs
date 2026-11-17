//! Model turn sources for ungoverned (local) runs.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::config::Config;
use crate::content::{
    ContentCapabilitiesV1, ContentMessageV1, ContentModelTurnV1, ContentPartKind,
    ResolvedContentPart, validate_messages,
};
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

/// Durable tool-call identity for authorize, staged execution/reports, and events.
///
/// Qualifies a (possibly missing or reused) model id with turn and batch index so
/// two same-named calls in one turn stay distinct across restart.
pub fn stable_tool_call_id(call: &ToolCall, turn: u32, index: usize) -> String {
    let synthesized = format!("tool-{turn}-{index}");
    if call.id.is_empty() || call.id == synthesized {
        synthesized
    } else {
        format!("tool-{turn}-{index}-{}", call.id)
    }
}

#[cfg(test)]
mod stable_id_tests {
    use super::*;

    #[test]
    fn same_named_calls_in_one_turn_are_distinct_and_stable() {
        let first = ToolCall {
            id: String::new(),
            name: "bash".into(),
            args_json: "{}".into(),
        };
        let second = first.clone();
        let a = stable_tool_call_id(&first, 1, 0);
        let b = stable_tool_call_id(&second, 1, 1);
        assert_eq!(a, "tool-1-0");
        assert_eq!(b, "tool-1-1");
        assert_ne!(a, b);
        assert_eq!(a, stable_tool_call_id(&first, 1, 0));
        let restored = ToolCall {
            id: "tool-1-0".into(),
            name: "bash".into(),
            args_json: "{}".into(),
        };
        assert_eq!(stable_tool_call_id(&restored, 1, 0), "tool-1-0");
    }
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
    #[error("http: {0}")]
    Http(String),
}

#[async_trait]
pub trait ModelPort: Send + Sync {
    fn id(&self) -> &'static str;
    /// Restore adapter-local replay position from durable completed turns.
    fn restore_replay_cursor(&self, _completed_turns: u32) -> Result<(), ModelError> {
        Ok(())
    }
    /// Content identity of this adapter instance. Fallback admission compares
    /// this to the authorized digest; it is not itself a grant.
    fn content_digest(&self) -> String {
        crate::fallback::sha256_hex(self.id().as_bytes())
    }
    async fn next_turn(
        &self,
        system: &str,
        messages: &[ChatMessage],
        tools: &[ToolDef],
    ) -> Result<ModelTurn, ModelError>;

    /// Summarize part of a run's history for context compaction. `transcript`
    /// is the compacted span rendered as text; the result's `content` is the
    /// summary. The default asks for one tool-free turn; a turn that still
    /// calls tools or says nothing is rejected by the caller.
    async fn summarize(&self, system: &str, transcript: &str) -> Result<ModelTurn, ModelError> {
        let request = ChatMessage {
            role: "user".into(),
            content: transcript.into(),
            tool_call_id: String::new(),
            tool_calls: vec![],
        };
        self.next_turn(system, std::slice::from_ref(&request), &[])
            .await
    }

    /// Produce one bounded content turn. Adapters deny by default so content
    /// can never be silently coerced through the text interface.
    async fn next_content_turn(
        &self,
        _system: &str,
        _messages: &[ContentMessageV1],
        _tools: &[ToolDef],
        _capabilities: &ContentCapabilitiesV1,
        _resolved_parts: &[ResolvedContentPart],
    ) -> Result<ContentModelTurnV1, ModelError> {
        Err(ModelError::Message(format!(
            "model adapter `{}` does not support bounded content",
            self.id()
        )))
    }

    /// Kinds this adapter can read on a content turn. Empty means text-only.
    fn content_kinds(&self) -> &'static [ContentPartKind] {
        &[]
    }

    /// Independent adapter for a nested child run.
    ///
    /// Scripted adapters start a new cursor so parent and child do not share
    /// replay position. HTTP adapters reuse the parent client when
    /// adapter/url/key/model match. Stateless adapters may clone themselves.
    fn fresh_for_child(&self, config: &Config) -> Result<Box<dyn ModelPort>, ModelError> {
        let _ = config;
        Err(ModelError::Message(format!(
            "model adapter `{}` cannot construct a nested child",
            self.id()
        )))
    }
}

tokio::task_local! {
    static RETRY_NOTES: std::cell::RefCell<Vec<String>>;
}

/// Record a transient-failure retry for the turn this task is planning.
/// Adapters call this; outside [`collect_retry_notes`] the note is dropped.
pub fn note_model_retry(note: String) {
    let _ = RETRY_NOTES.try_with(|notes| notes.borrow_mut().push(note));
}

/// Await one model turn and return the retries noted while planning it.
/// Notes are task-local, so concurrent runs sharing an adapter never see
/// each other's retries.
pub async fn collect_retry_notes<F: std::future::Future>(turn: F) -> (F::Output, Vec<String>) {
    RETRY_NOTES
        .scope(std::cell::RefCell::new(Vec::new()), async {
            let output = turn.await;
            (output, RETRY_NOTES.with(|notes| notes.take()))
        })
        .await
}

tokio::task_local! {
    static TEXT_DELTAS: std::sync::Arc<dyn Fn(&str) + Send + Sync>;
}

/// Report assistant text of the turn this task is planning as it arrives.
/// Adapters call this; outside [`stream_text_deltas`] the text is dropped.
pub fn emit_text_delta(text: &str) {
    if !text.is_empty() {
        let _ = TEXT_DELTAS.try_with(|sink| sink(text));
    }
}

/// True when someone observes deltas of the current turn. Adapters stream
/// only then and otherwise request a whole turn as before.
pub fn text_deltas_observed() -> bool {
    TEXT_DELTAS.try_with(|_| ()).is_ok()
}

/// Await one model turn while forwarding its text deltas to `sink`.
pub async fn stream_text_deltas<F: std::future::Future>(
    sink: std::sync::Arc<dyn Fn(&str) + Send + Sync>,
    turn: F,
) -> F::Output {
    TEXT_DELTAS.scope(sink, turn).await
}

pub fn from_config(config: &Config) -> Result<Box<dyn ModelPort>, ModelError> {
    match config.model.adapter.as_str() {
        "scripted" => Ok(Box::new(ScriptedModel::from_config(config)?)),
        "http" => Err(ModelError::HttpUnavailable),
        "plane" => {
            if config.model.fallback.enabled {
                match config.model.fallback.adapter.as_deref() {
                    Some("scripted") => Ok(Box::new(ScriptedModel::from_fallback_config(config)?)),
                    Some("http") => Err(ModelError::HttpUnavailable),
                    Some(other) => Err(ModelError::Message(format!(
                        "unknown model.fallback.adapter `{other}`"
                    ))),
                    None => Ok(Box::new(PlaneModelPlaceholder)),
                }
            } else {
                Ok(Box::new(PlaneModelPlaceholder))
            }
        }
        other => Err(ModelError::Message(format!(
            "unknown model adapter `{other}`"
        ))),
    }
}

/// Deterministic multi-turn script for tests and offline demos.
pub struct ScriptedModel {
    turns: Vec<ModelTurn>,
    /// Per turn: what happens before it returns. Empty for plain turns.
    preludes: Vec<TurnPrelude>,
    /// Answer to every [`ModelPort::summarize`] call, from a script entry
    /// `{"summary": "..."}`. Summary entries are not turns, so the replay
    /// cursor stays aligned with completed turns.
    summary: Option<String>,
    cursor: std::sync::Mutex<usize>,
    content_digest: String,
}

impl ScriptedModel {
    pub fn from_config(config: &Config) -> Result<Self, ModelError> {
        let raw = config
            .model
            .script_json
            .clone()
            .unwrap_or_else(default_script_json);
        Self::from_script_json(&raw, config)
    }

    pub fn from_fallback_config(config: &Config) -> Result<Self, ModelError> {
        let raw = config
            .model
            .fallback
            .script_json
            .clone()
            .or_else(|| config.model.script_json.clone())
            .unwrap_or_else(default_script_json);
        Self::from_script_json(&raw, config)
    }

    fn from_script_json(raw: &str, config: &Config) -> Result<Self, ModelError> {
        let mut wire: Vec<ScriptedTurn> = serde_json::from_str(raw)?;
        let summary = wire.iter().find_map(|t| t.summary.clone());
        wire.retain(|t| t.summary.is_none());
        let preludes = wire
            .iter_mut()
            .map(|t| TurnPrelude {
                chunks: t.chunks.clone(),
                chunk_delay_ms: t.chunk_delay_ms,
                requires_context: std::mem::take(&mut t.requires_context),
            })
            .collect();
        let turns = wire
            .into_iter()
            .map(|t| ModelTurn {
                content: t.content.unwrap_or_else(|| t.chunks.concat()),
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
            turns,
            preludes,
            summary,
            cursor: std::sync::Mutex::new(0),
            content_digest: artifact_or_script_digest(config, raw)?,
        })
    }

    pub fn from_turns(turns: Vec<ModelTurn>) -> Self {
        let payload = turns
            .iter()
            .map(|turn| turn.content.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        Self {
            turns,
            preludes: Vec::new(),
            summary: None,
            cursor: std::sync::Mutex::new(0),
            content_digest: crate::fallback::sha256_hex(payload.as_bytes()),
        }
    }
}

fn artifact_or_script_digest(config: &Config, script: &str) -> Result<String, ModelError> {
    let mut payload = script.as_bytes().to_vec();
    if let Some(path) = config.model.fallback.artifact_path.as_ref() {
        payload.push(0xff);
        payload.extend(std::fs::read(path)?);
    }
    Ok(crate::fallback::sha256_hex(&payload))
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
    /// Marks this entry as the scripted compaction summary instead of a turn.
    #[serde(default)]
    summary: Option<String>,
    /// Defaults to the concatenated `chunks`.
    content: Option<String>,
    #[serde(default)]
    chunks: Vec<String>,
    #[serde(default)]
    chunk_delay_ms: u64,
    #[serde(default)]
    tool_calls: Vec<ScriptedCall>,
    #[serde(default)]
    usage: Option<TokenUsage>,
    /// Text that must appear in some message the turn sees, or the turn
    /// fails. Lets a fixture prove a fact survived compaction.
    #[serde(default)]
    requires_context: Vec<String>,
}

/// Scripted behavior before a turn returns.
struct TurnPrelude {
    /// Text streamed through [`emit_text_delta`], with a pause before each chunk.
    chunks: Vec<String>,
    chunk_delay_ms: u64,
    requires_context: Vec<String>,
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

    async fn summarize(&self, _system: &str, _transcript: &str) -> Result<ModelTurn, ModelError> {
        let content = self
            .summary
            .clone()
            .ok_or_else(|| ModelError::Message("script has no summary entry".into()))?;
        Ok(ModelTurn {
            content,
            tool_calls: vec![],
            usage: None,
        })
    }

    fn restore_replay_cursor(&self, completed_turns: u32) -> Result<(), ModelError> {
        let cursor = usize::try_from(completed_turns)
            .map_err(|_| ModelError::Message("completed turn count exceeds usize".into()))?;
        if cursor > self.turns.len() {
            return Err(ModelError::ScriptExhausted);
        }
        *self
            .cursor
            .lock()
            .map_err(|_| ModelError::Message("script cursor lock poisoned".into()))? = cursor;
        Ok(())
    }

    fn content_digest(&self) -> String {
        self.content_digest.clone()
    }

    async fn next_turn(
        &self,
        _system: &str,
        messages: &[ChatMessage],
        _tools: &[ToolDef],
    ) -> Result<ModelTurn, ModelError> {
        let index = {
            let mut cursor = self.cursor.lock().expect("script cursor lock");
            let index = *cursor;
            if index >= self.turns.len() {
                return Err(ModelError::ScriptExhausted);
            }
            *cursor += 1;
            index
        };
        if let Some(prelude) = self.preludes.get(index) {
            if let Some(missing) = prelude.requires_context.iter().find(|needle| {
                !messages
                    .iter()
                    .any(|message| message.content.contains(needle.as_str()))
            }) {
                return Err(ModelError::Message(format!(
                    "scripted turn {} requires `{missing}` in context",
                    index + 1
                )));
            }
            for chunk in &prelude.chunks {
                if prelude.chunk_delay_ms > 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(prelude.chunk_delay_ms))
                        .await;
                }
                emit_text_delta(chunk);
            }
        }
        Ok(self.turns[index].clone())
    }

    async fn next_content_turn(
        &self,
        system: &str,
        messages: &[ContentMessageV1],
        tools: &[ToolDef],
        capabilities: &ContentCapabilitiesV1,
        _resolved_parts: &[ResolvedContentPart],
    ) -> Result<ContentModelTurnV1, ModelError> {
        validate_messages(messages, capabilities)
            .map_err(|error| ModelError::Message(error.to_string()))?;
        let turn = self.next_turn(system, &[], tools).await?;
        Ok(ContentModelTurnV1 {
            text: turn.content,
            output_parts: Vec::new(),
            tool_calls: turn.tool_calls,
            usage: turn.usage,
        })
    }

    fn content_kinds(&self) -> &'static [ContentPartKind] {
        &[
            ContentPartKind::Text,
            ContentPartKind::Image,
            ContentPartKind::Audio,
            ContentPartKind::Document,
        ]
    }

    fn fresh_for_child(&self, config: &Config) -> Result<Box<dyn ModelPort>, ModelError> {
        // Rebuild through the engine selector so plane+scripted fallback still
        // uses `from_fallback_config`. Nested tests that stash a child script
        // on `config.model.script_json` stay on the `scripted` adapter path.
        from_config(config)
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

    fn fresh_for_child(&self, config: &Config) -> Result<Box<dyn ModelPort>, ModelError> {
        from_config(config)
    }
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

    #[test]
    fn fresh_for_child_keeps_scripted_fallback_script() {
        let mut config = Config::default();
        config.model.adapter = "plane".into();
        config.model.fallback.enabled = true;
        config.model.fallback.adapter = Some("scripted".into());
        config.model.script_json = Some(r#"[{"content":"primary"}]"#.into());
        config.model.fallback.script_json = Some(r#"[{"content":"fallback"}]"#.into());
        let parent = ScriptedModel::from_fallback_config(&config).unwrap();
        let child = parent.fresh_for_child(&config).unwrap();
        assert_eq!(child.id(), "scripted");
        assert_eq!(child.content_digest(), parent.content_digest());
    }

    #[tokio::test]
    async fn fresh_for_child_does_not_share_script_cursor() {
        let mut config = Config::default();
        config.model.script_json = Some(r#"[{"content":"one"},{"content":"two"}]"#.into());
        let parent = ScriptedModel::from_config(&config).unwrap();
        let first = parent.next_turn("", &[], &[]).await.unwrap();
        assert_eq!(first.content, "one");
        let child = parent.fresh_for_child(&config).unwrap();
        let child_first = child.next_turn("", &[], &[]).await.unwrap();
        assert_eq!(
            child_first.content, "one",
            "child must start at the first scripted turn"
        );
        let parent_second = parent.next_turn("", &[], &[]).await.unwrap();
        assert_eq!(parent_second.content, "two");
        let child_second = child.next_turn("", &[], &[]).await.unwrap();
        assert_eq!(child_second.content, "two");
    }

    #[test]
    fn engine_from_config_leaves_http_to_the_host_crate() {
        let mut config = Config::default();
        config.model.adapter = "http".into();
        assert!(matches!(
            from_config(&config),
            Err(ModelError::HttpUnavailable)
        ));

        config.model.adapter = "plane".into();
        config.model.fallback.enabled = true;
        config.model.fallback.adapter = Some("http".into());
        assert!(matches!(
            from_config(&config),
            Err(ModelError::HttpUnavailable)
        ));
    }
}
