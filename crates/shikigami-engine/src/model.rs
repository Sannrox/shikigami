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
///
/// Cache counts are subsets of [`Self::input_tokens`], not extra tokens.
/// They stay `None` when the model did not report cache usage.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cached_input_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_creation_input_tokens: Option<u64>,
}

impl TokenUsage {
    pub fn total(self) -> u64 {
        self.input_tokens.saturating_add(self.output_tokens)
    }

    /// Sum two reports. Cache fields stay absent when neither side reported them.
    pub fn saturating_add(self, other: Self) -> Self {
        Self {
            input_tokens: self.input_tokens.saturating_add(other.input_tokens),
            output_tokens: self.output_tokens.saturating_add(other.output_tokens),
            cached_input_tokens: add_optional_tokens(
                self.cached_input_tokens,
                other.cached_input_tokens,
            ),
            cache_creation_input_tokens: add_optional_tokens(
                self.cache_creation_input_tokens,
                other.cache_creation_input_tokens,
            ),
        }
    }

    /// Prompt-delta between two cumulative session totals.
    pub fn saturating_sub(self, earlier: Self) -> Self {
        Self {
            input_tokens: self.input_tokens.saturating_sub(earlier.input_tokens),
            output_tokens: self.output_tokens.saturating_sub(earlier.output_tokens),
            cached_input_tokens: sub_optional_tokens(
                self.cached_input_tokens,
                earlier.cached_input_tokens,
            ),
            cache_creation_input_tokens: sub_optional_tokens(
                self.cache_creation_input_tokens,
                earlier.cache_creation_input_tokens,
            ),
        }
    }
}

fn add_optional_tokens(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (None, None) => None,
        (left, right) => Some(left.unwrap_or(0).saturating_add(right.unwrap_or(0))),
    }
}

fn sub_optional_tokens(after: Option<u64>, before: Option<u64>) -> Option<u64> {
    match (after, before) {
        (Some(after), Some(before)) if after == before => None,
        (Some(after), before) => Some(after.saturating_sub(before.unwrap_or(0))),
        _ => None,
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

/// Closed-set model finish reason exported on a turn span.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ModelFinishReason {
    Stop,
    Length,
    ToolCalls,
    ContentFilter,
}

impl ModelFinishReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Stop => "stop",
            Self::Length => "length",
            Self::ToolCalls => "tool_calls",
            Self::ContentFilter => "content_filter",
        }
    }

    /// Map a provider or scripted finish reason onto the closed set.
    /// Unknown values are dropped so they never become span attributes.
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim() {
            "stop" => Some(Self::Stop),
            "length" | "max_tokens" => Some(Self::Length),
            "tool_calls" | "function_call" => Some(Self::ToolCalls),
            "content_filter" => Some(Self::ContentFilter),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ModelTurn {
    pub content: String,
    pub tool_calls: Vec<ToolCall>,
    pub usage: Option<TokenUsage>,
    pub finish_reason: Option<ModelFinishReason>,
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
    /// The provider answered with a non-success status (`text` is e.g.
    /// `402 Payment Required`).
    #[error("http: model provider returned {text}")]
    ProviderStatus { code: u16, text: String },
    /// The provider refused the prompt as larger than the model context window.
    #[error("model: prompt exceeds context length")]
    ContextLength,
    /// The request never reached the provider: refused connection or timeout.
    #[error("http: {0}")]
    Unreachable(String),
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

    /// Whether the model's address answers at all, without spending a model
    /// request or sending credentials. `Unreachable` when it does not;
    /// adapters without an address are always reachable.
    async fn check_reachable(&self) -> Result<(), ModelError> {
        Ok(())
    }

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
                fail: std::mem::take(&mut t.fail),
            })
            .collect();
        let turns = wire
            .into_iter()
            .map(|t| ModelTurn {
                content: t.content.unwrap_or_else(|| t.chunks.concat()),
                usage: t.usage,
                finish_reason: t
                    .finish_reason
                    .as_deref()
                    .and_then(ModelFinishReason::parse),
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
    /// OpenAI-style finish reason (`stop`, `length`, …). Unknown values
    /// are dropped when the turn is built.
    #[serde(default)]
    finish_reason: Option<String>,
    /// Text that must appear in some message the turn sees, or the turn
    /// fails. Lets a fixture prove a fact survived compaction.
    #[serde(default)]
    requires_context: Vec<String>,
    /// Fail this entry instead of returning a turn. `context_length` is the
    /// provider-window refusal; the cursor still advances so the next entry
    /// is the retry.
    #[serde(default)]
    fail: Option<String>,
}

/// Scripted behavior before a turn returns.
struct TurnPrelude {
    /// Text streamed through [`emit_text_delta`], with a pause before each chunk.
    chunks: Vec<String>,
    chunk_delay_ms: u64,
    requires_context: Vec<String>,
    fail: Option<String>,
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
            finish_reason: None,
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
            if let Some(fail) = prelude.fail.as_deref() {
                return Err(match fail {
                    "context_length" => ModelError::ContextLength,
                    other => ModelError::Message(format!("unknown scripted fail `{other}`")),
                });
            }
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
        // Rebuild from settings so nested tests that stash a child script on
        // `config.model.script_json` stay on the `scripted` adapter path.
        from_config(config)
    }
}

const DEFAULT_HTTP_MODEL: &str = "gpt-4.1-mini";

/// Resolve the model name an adapter will actually use.
pub fn effective_model_name(config: &Config) -> String {
    if config.model.adapter == "http" && config.model.model == "auto" {
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
            ..Default::default()
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
            ..Default::default()
        };
        let c = CostEstimate::from_usage_and_rates(u, Some(1_000_000), Some(2_000_000)).unwrap();
        assert_eq!(c.currency, "USD");
        assert_eq!(c.input_usd_micros, 1_000_000);
        assert_eq!(c.output_usd_micros, 1_000_000);
        assert_eq!(c.total_usd_micros, 2_000_000);
    }

    #[test]
    fn cache_counts_sum_only_when_reported() {
        let none = TokenUsage {
            input_tokens: 10,
            output_tokens: 1,
            ..Default::default()
        };
        let cached = TokenUsage {
            input_tokens: 20,
            output_tokens: 2,
            cached_input_tokens: Some(15),
            cache_creation_input_tokens: Some(3),
        };
        assert_eq!(none.saturating_add(none).cached_input_tokens, None);
        let sum = none.saturating_add(cached);
        assert_eq!(sum.input_tokens, 30);
        assert_eq!(sum.cached_input_tokens, Some(15));
        assert_eq!(sum.cache_creation_input_tokens, Some(3));
        assert_eq!(cached.saturating_sub(none).cached_input_tokens, Some(15));
        assert_eq!(sum.saturating_sub(cached).cached_input_tokens, None);
    }

    #[test]
    fn stored_usage_without_cache_fields_round_trips() {
        let old: TokenUsage =
            serde_json::from_str(r#"{"input_tokens":1,"output_tokens":2}"#).unwrap();
        assert_eq!(old.cached_input_tokens, None);
        let dumped = serde_json::to_string(&old).unwrap();
        assert!(!dumped.contains("cached"), "{dumped}");
    }

    #[test]
    fn auto_resolves_to_http_fallback_only_for_http_adapter() {
        let mut config = Config::default();
        assert_eq!(effective_model_name(&config), "auto");

        config.model.adapter = "http".into();
        assert_eq!(effective_model_name(&config), "gpt-4.1-mini");

        config.model.model = "openai/gpt-5.5".into();
        assert_eq!(effective_model_name(&config), "openai/gpt-5.5");
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

    #[tokio::test]
    async fn scripted_context_length_fail_advances_the_cursor() {
        let mut config = Config::default();
        config.model.script_json =
            Some(r#"[{"fail":"context_length"},{"content":"after compact"}]"#.into());
        let model = ScriptedModel::from_config(&config).unwrap();
        assert!(matches!(
            model.next_turn("", &[], &[]).await,
            Err(ModelError::ContextLength)
        ));
        let turn = model.next_turn("", &[], &[]).await.unwrap();
        assert_eq!(turn.content, "after compact");
    }

    #[test]
    fn engine_from_config_leaves_http_to_the_host_crate() {
        let mut config = Config::default();
        config.model.adapter = "http".into();
        assert!(matches!(
            from_config(&config),
            Err(ModelError::HttpUnavailable)
        ));
    }
}
