//! OpenAI-compatible HTTP model adapter.

use std::sync::Arc;

use async_trait::async_trait;

use shikigami_engine::config::Config;
use shikigami_engine::content::{
    ContentCapabilitiesV1, ContentDisclosureState, ContentMessageV1, ContentModelTurnV1,
    ContentPartDescriptor, ContentPartKind, ResolvedContent, ResolvedContentPart,
    tool_arguments_part_id, validate_messages,
};
use shikigami_engine::model::{
    ChatMessage, ModelError, ModelPort, ModelTurn, TokenUsage, ToolCall, effective_model_name,
};
use shikigami_engine::tools::ToolDef;

/// OpenAI-compatible chat-completions adapter for ungoverned runs.
pub struct HttpModel {
    client: Arc<reqwest::Client>,
    adapter: String,
    base_url: String,
    model: String,
    api_key: String,
    content_digest: String,
}

impl HttpModel {
    pub fn from_config(config: &Config) -> Result<Self, ModelError> {
        Self::from_config_reusing(config, None)
    }

    fn from_config_reusing(config: &Config, parent: Option<&Self>) -> Result<Self, ModelError> {
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
        let adapter = config.model.adapter.clone();
        let model = effective_model_name(config);
        let client = match parent {
            Some(parent)
                if parent.adapter == adapter
                    && parent.base_url == base_url
                    && parent.model == model
                    && parent.api_key == api_key =>
            {
                Arc::clone(&parent.client)
            }
            _ => Arc::new(reqwest::Client::new()),
        };
        let mut digest_payload = format!("http\n{base_url}\n{model}").into_bytes();
        if let Some(path) = config.model.fallback.artifact_path.as_ref() {
            digest_payload.push(0xff);
            digest_payload.extend(std::fs::read(path)?);
        }
        Ok(Self {
            client,
            adapter,
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
        Ok(Box::new(Self::from_config_reusing(config, Some(self))?))
    }

    fn content_kinds(&self) -> &'static [ContentPartKind] {
        &[
            ContentPartKind::Text,
            ContentPartKind::Image,
            ContentPartKind::Document,
        ]
    }

    async fn next_turn(
        &self,
        system: &str,
        messages: &[ChatMessage],
        tools: &[ToolDef],
    ) -> Result<ModelTurn, ModelError> {
        let mut api_messages = vec![serde_json::json!({"role":"system","content":system})];
        for m in messages {
            api_messages.push(text_chat_message(m));
        }
        self.complete(api_messages, tools).await
    }

    async fn next_content_turn(
        &self,
        system: &str,
        messages: &[ContentMessageV1],
        tools: &[ToolDef],
        capabilities: &ContentCapabilitiesV1,
        resolved_parts: &[ResolvedContentPart],
    ) -> Result<ContentModelTurnV1, ModelError> {
        validate_messages(messages, capabilities)
            .map_err(|error| ModelError::Message(error.to_string()))?;
        let mut api_messages = vec![serde_json::json!({"role":"system","content":system})];
        api_messages.extend(encode_content_messages(messages, resolved_parts)?);
        let turn = self.complete(api_messages, tools).await?;
        Ok(ContentModelTurnV1 {
            text: turn.content,
            output_parts: Vec::new(),
            tool_calls: turn.tool_calls,
            usage: turn.usage,
        })
    }
}

impl HttpModel {
    async fn complete(
        &self,
        api_messages: Vec<serde_json::Value>,
        tools: &[ToolDef],
    ) -> Result<ModelTurn, ModelError> {
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

fn text_chat_message(message: &ChatMessage) -> serde_json::Value {
    if message.role == "tool" {
        serde_json::json!({
            "role": "tool",
            "tool_call_id": message.tool_call_id,
            "content": message.content,
        })
    } else if !message.tool_calls.is_empty() {
        let calls: Vec<_> = message
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
        serde_json::json!({
            "role": "assistant",
            "content": message.content,
            "tool_calls": calls,
        })
    } else {
        serde_json::json!({
            "role": message.role,
            "content": message.content,
        })
    }
}

fn encode_content_messages(
    messages: &[ContentMessageV1],
    resolved: &[ResolvedContentPart],
) -> Result<Vec<serde_json::Value>, ModelError> {
    messages
        .iter()
        .map(|message| encode_content_message(message, resolved))
        .collect()
}

fn encode_content_message(
    message: &ContentMessageV1,
    resolved: &[ResolvedContentPart],
) -> Result<serde_json::Value, ModelError> {
    if message.role == "tool" {
        return Ok(serde_json::json!({
            "role": "tool",
            "tool_call_id": message.tool_call_id,
            "content": text_payload(message, resolved)?,
        }));
    }
    if !message.tool_calls.is_empty() {
        let mut calls = Vec::with_capacity(message.tool_calls.len());
        for call in &message.tool_calls {
            calls.push(serde_json::json!({
                "id": call.id,
                "type": "function",
                "function": {
                    "name": call.name,
                    "arguments": resolve_tool_arguments(call, resolved)?,
                }
            }));
        }
        return Ok(serde_json::json!({
            "role": "assistant",
            "content": visible_text_payload(message, resolved)?,
            "tool_calls": calls,
        }));
    }
    Ok(serde_json::json!({
        "role": message.role,
        "content": encode_parts(&message.parts, resolved)?,
    }))
}

fn text_payload(
    message: &ContentMessageV1,
    resolved: &[ResolvedContentPart],
) -> Result<String, ModelError> {
    let mut text = String::new();
    for descriptor in &message.parts {
        if !is_accepted(descriptor) {
            continue;
        }
        if descriptor.kind != ContentPartKind::Text {
            return Err(ModelError::Message(format!(
                "http adapter cannot place {} on a tool or tool-call message",
                content_kind_name(descriptor.kind)
            )));
        }
        text.push_str(&resolved_text(descriptor, resolved)?);
    }
    Ok(text)
}

fn visible_text_payload(
    message: &ContentMessageV1,
    resolved: &[ResolvedContentPart],
) -> Result<String, ModelError> {
    let mut text = String::new();
    for descriptor in message.parts.iter().filter(|part| is_visible_text(part)) {
        text.push_str(&resolved_text(descriptor, resolved)?);
    }
    Ok(text)
}

fn resolve_tool_arguments(
    call: &ToolCall,
    resolved: &[ResolvedContentPart],
) -> Result<String, ModelError> {
    let part_id = tool_arguments_part_id(&call.args_json)
        .ok_or_else(|| ModelError::Message("content tool arguments are not externalized".into()))?;
    let part = resolved
        .iter()
        .find(|part| part.descriptor.part_id == part_id)
        .ok_or_else(|| {
            ModelError::Message(format!("missing resolved tool arguments `{part_id}`"))
        })?;
    if part.descriptor.kind != ContentPartKind::Text
        || part.descriptor.provenance.source != "model-tool-arguments"
        || !is_accepted(&part.descriptor)
    {
        return Err(ModelError::Message(
            "content tool argument descriptor is missing".into(),
        ));
    }
    match &part.payload {
        ResolvedContent::Text(text) => Ok(text.clone()),
        ResolvedContent::Bytes(_) => Err(ModelError::Message(
            "content tool arguments resolved to bytes".into(),
        )),
    }
}

fn is_accepted(descriptor: &ContentPartDescriptor) -> bool {
    descriptor.disclosure_state == ContentDisclosureState::Accepted
}

fn is_visible_text(descriptor: &ContentPartDescriptor) -> bool {
    is_accepted(descriptor)
        && descriptor.kind == ContentPartKind::Text
        && descriptor.provenance.source != "model-tool-arguments"
}

fn encode_parts(
    parts: &[ContentPartDescriptor],
    resolved: &[ResolvedContentPart],
) -> Result<serde_json::Value, ModelError> {
    let mut encoded = Vec::new();
    for descriptor in parts.iter().filter(|part| is_accepted(part)) {
        encoded.push(encode_part(descriptor, resolved)?);
    }
    Ok(serde_json::Value::Array(encoded))
}

fn encode_part(
    descriptor: &ContentPartDescriptor,
    resolved: &[ResolvedContentPart],
) -> Result<serde_json::Value, ModelError> {
    match descriptor.kind {
        ContentPartKind::Text => Ok(serde_json::json!({
            "type": "text",
            "text": resolved_text(descriptor, resolved)?,
        })),
        ContentPartKind::Image => {
            let bytes = resolved_bytes(descriptor, resolved)?;
            let url = data_url(&descriptor.media_type, bytes);
            Ok(serde_json::json!({
                "type": "image_url",
                "image_url": { "url": url },
            }))
        }
        ContentPartKind::Document => match descriptor.media_type.as_str() {
            "application/pdf" => {
                let bytes = resolved_bytes(descriptor, resolved)?;
                Ok(serde_json::json!({
                    "type": "file",
                    "file": {
                        "filename": format!("{}.pdf", descriptor.part_id),
                        "file_data": data_url(&descriptor.media_type, bytes),
                    },
                }))
            }
            "text/plain" | "text/markdown" => Ok(serde_json::json!({
                "type": "text",
                "text": resolved_text(descriptor, resolved)?,
            })),
            other => Err(ModelError::Message(format!(
                "http adapter cannot read document media type `{other}`"
            ))),
        },
        other => Err(ModelError::Message(format!(
            "http adapter cannot read {} attachments",
            content_kind_name(other)
        ))),
    }
}

fn resolved_part<'a>(
    descriptor: &ContentPartDescriptor,
    resolved: &'a [ResolvedContentPart],
) -> Result<&'a ResolvedContentPart, ModelError> {
    resolved
        .iter()
        .find(|part| part.descriptor.part_id == descriptor.part_id)
        .ok_or_else(|| {
            ModelError::Message(format!(
                "missing resolved content part `{}`",
                descriptor.part_id
            ))
        })
}

fn resolved_text(
    descriptor: &ContentPartDescriptor,
    resolved: &[ResolvedContentPart],
) -> Result<String, ModelError> {
    match &resolved_part(descriptor, resolved)?.payload {
        ResolvedContent::Text(text) => Ok(text.clone()),
        ResolvedContent::Bytes(bytes) => Ok(String::from_utf8_lossy(bytes).into_owned()),
    }
}

fn resolved_bytes<'a>(
    descriptor: &ContentPartDescriptor,
    resolved: &'a [ResolvedContentPart],
) -> Result<&'a [u8], ModelError> {
    match &resolved_part(descriptor, resolved)?.payload {
        ResolvedContent::Bytes(bytes) => Ok(bytes),
        ResolvedContent::Text(text) => Ok(text.as_bytes()),
    }
}

fn data_url(media_type: &str, bytes: &[u8]) -> String {
    format!("data:{media_type};base64,{}", encode_base64(bytes))
}

fn content_kind_name(kind: ContentPartKind) -> &'static str {
    match kind {
        ContentPartKind::Text => "text",
        ContentPartKind::Image => "image",
        ContentPartKind::Audio => "audio",
        ContentPartKind::Document => "document",
    }
}

fn encode_base64(input: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let a = chunk[0];
        let b = chunk.get(1).copied().unwrap_or(0);
        let c = chunk.get(2).copied().unwrap_or(0);
        out.push(TABLE[(a >> 2) as usize] as char);
        out.push(TABLE[(((a & 0x03) << 4) | (b >> 4)) as usize] as char);
        if chunk.len() > 1 {
            out.push(TABLE[(((b & 0x0f) << 2) | (c >> 6)) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(TABLE[(c & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use shikigami_engine::config::Config;

    fn http_config(key_env: &str, key: &str) -> Config {
        // SAFETY: unique env name per test; this process does not unset it.
        unsafe {
            std::env::set_var(key_env, key);
        }
        let mut config = Config::default();
        config.model.adapter = "http".into();
        config.model.api_key_env = key_env.into();
        config.model.base_url = Some("https://api.openai.com/v1".into());
        config
    }

    #[test]
    fn fresh_for_child_reuses_http_client_when_credentials_match() {
        let config = http_config("SHIKIGAMI_HTTP_TEST_KEY_SHARE", "share-key");
        let parent = HttpModel::from_config(&config).unwrap();
        let child = HttpModel::from_config_reusing(&config, Some(&parent)).unwrap();
        assert!(
            Arc::ptr_eq(&parent.client, &child.client),
            "matching adapter/url/key/model must clone the parent HTTP client"
        );
        let boxed = parent.fresh_for_child(&config).unwrap();
        assert_eq!(boxed.id(), "http");
        assert_eq!(boxed.content_digest(), parent.content_digest);
    }

    #[test]
    fn fresh_for_child_rebuilds_http_client_when_endpoint_changes() {
        let config = http_config("SHIKIGAMI_HTTP_TEST_KEY_URL", "url-key");
        let parent = HttpModel::from_config(&config).unwrap();
        let mut other = config.clone();
        other.model.base_url = Some("https://example.invalid/v1".into());
        let child = HttpModel::from_config_reusing(&other, Some(&parent)).unwrap();
        assert!(
            !Arc::ptr_eq(&parent.client, &child.client),
            "a different base_url must construct a new HTTP client"
        );
    }

    #[test]
    fn fresh_for_child_rebuilds_http_client_when_model_changes() {
        let config = http_config("SHIKIGAMI_HTTP_TEST_KEY_MODEL", "model-key");
        let parent = HttpModel::from_config(&config).unwrap();
        let mut other = config.clone();
        other.model.model = "gpt-4.1".into();
        let child = HttpModel::from_config_reusing(&other, Some(&parent)).unwrap();
        assert!(!Arc::ptr_eq(&parent.client, &child.client));
    }

    #[test]
    fn fresh_for_child_rebuilds_http_client_when_api_key_changes() {
        let env = "SHIKIGAMI_HTTP_TEST_KEY_CRED";
        let config = http_config(env, "first-key");
        let parent = HttpModel::from_config(&config).unwrap();
        // SAFETY: this test owns `env` and immediately rebuilds from it.
        unsafe {
            std::env::set_var(env, "second-key");
        }
        let child = HttpModel::from_config_reusing(&config, Some(&parent)).unwrap();
        assert!(!Arc::ptr_eq(&parent.client, &child.client));
    }

    #[test]
    fn fresh_for_child_rebuilds_http_client_when_adapter_changes() {
        let config = http_config("SHIKIGAMI_HTTP_TEST_KEY_ADAPTER", "adapter-key");
        let parent = HttpModel::from_config(&config).unwrap();
        let mut other = config.clone();
        other.model.adapter = "plane".into();
        other.model.fallback.enabled = true;
        other.model.fallback.adapter = Some("http".into());
        let child = HttpModel::from_config_reusing(&other, Some(&parent)).unwrap();
        assert!(!Arc::ptr_eq(&parent.client, &child.client));
    }

    #[test]
    fn nested_profile_overlay_still_reuses_http_client() {
        let mut config = http_config("SHIKIGAMI_HTTP_TEST_KEY_NESTED", "nested-key");
        config
            .tools
            .mcp_servers
            .push(shikigami_engine::config::McpServerSettings::stdio(
                "demo",
                "mock",
                Vec::new(),
            ));
        let parent = HttpModel::from_config(&config).unwrap();
        config.tools.mode = shikigami_engine::config::PermissionMode::Read;
        config.tools.enabled.clear();
        config.tools.mcp_servers.clear();
        config.run.nested = false;
        config.events.adapter = "none".into();
        let child = HttpModel::from_config_reusing(&config, Some(&parent)).unwrap();
        assert!(
            Arc::ptr_eq(&parent.client, &child.client),
            "explore/plan overlays must not allocate a new HTTP client"
        );
    }

    #[test]
    fn http_adapter_reads_image_and_document() {
        let config = http_config("SHIKIGAMI_HTTP_TEST_KEY_KINDS", "kinds-key");
        let model = HttpModel::from_config(&config).unwrap();
        assert_eq!(
            model.content_kinds(),
            &[
                ContentPartKind::Text,
                ContentPartKind::Image,
                ContentPartKind::Document
            ]
        );
    }

    #[test]
    fn encode_base64_matches_rfc4648() {
        assert_eq!(encode_base64(b""), "");
        assert_eq!(encode_base64(b"Man"), "TWFu");
        assert_eq!(encode_base64(b"Ma"), "TWE=");
        assert_eq!(encode_base64(b"M"), "TQ==");
    }

    #[test]
    fn content_messages_encode_image_url_and_pdf_file() {
        let text = resolved("text-0", ContentPartKind::Text, "text/plain", b"look");
        let image = resolved("part-1", ContentPartKind::Image, "image/png", b"\x89PNG");
        let pdf = resolved(
            "part-2",
            ContentPartKind::Document,
            "application/pdf",
            b"%PDF",
        );
        let messages = vec![ContentMessageV1 {
            role: "user".into(),
            parts: vec![
                text.descriptor.clone(),
                image.descriptor.clone(),
                pdf.descriptor.clone(),
            ],
            tool_call_id: String::new(),
            tool_calls: Vec::new(),
        }];
        let encoded = encode_content_messages(&messages, &[text, image, pdf]).unwrap();
        assert_eq!(encoded.len(), 1);
        let content = encoded[0]["content"].as_array().expect("content array");
        assert_eq!(content[0]["type"], "text");
        assert_eq!(content[0]["text"], "look");
        assert_eq!(content[1]["type"], "image_url");
        assert_eq!(
            content[1]["image_url"]["url"],
            format!("data:image/png;base64,{}", encode_base64(b"\x89PNG"))
        );
        assert_eq!(content[2]["type"], "file");
        assert_eq!(content[2]["file"]["filename"], "part-2.pdf");
        assert_eq!(
            content[2]["file"]["file_data"],
            format!("data:application/pdf;base64,{}", encode_base64(b"%PDF"))
        );
    }

    #[test]
    fn content_messages_encode_markdown_document_as_text() {
        let markdown = resolved(
            "part-1",
            ContentPartKind::Document,
            "text/markdown",
            b"# notes",
        );
        let messages = vec![ContentMessageV1 {
            role: "user".into(),
            parts: vec![markdown.descriptor.clone()],
            tool_call_id: String::new(),
            tool_calls: Vec::new(),
        }];
        let encoded = encode_content_messages(&messages, &[markdown]).unwrap();
        let content = encoded[0]["content"].as_array().expect("content array");
        assert_eq!(content.len(), 1);
        assert_eq!(content[0]["type"], "text");
        assert_eq!(content[0]["text"], "# notes");
        assert!(content[0].get("file").is_none());
    }

    #[test]
    fn content_messages_refuse_audio() {
        let audio = resolved("part-1", ContentPartKind::Audio, "audio/wav", b"RIFF");
        let messages = vec![ContentMessageV1 {
            role: "user".into(),
            parts: vec![audio.descriptor.clone()],
            tool_call_id: String::new(),
            tool_calls: Vec::new(),
        }];
        let err = encode_content_messages(&messages, &[audio]).unwrap_err();
        assert!(err.to_string().contains("cannot read audio"), "{err}");
    }

    #[test]
    fn content_messages_resolve_externalized_tool_arguments() {
        let text = resolved("text-0", ContentPartKind::Text, "text/plain", b"hello");
        let mut args = resolved(
            "args-0",
            ContentPartKind::Text,
            "text/plain",
            br#"{"path":"a.txt"}"#,
        );
        args.descriptor.provenance.source = "model-tool-arguments".into();
        let messages = vec![ContentMessageV1 {
            role: "assistant".into(),
            parts: vec![text.descriptor.clone(), args.descriptor.clone()],
            tool_call_id: String::new(),
            tool_calls: vec![ToolCall {
                id: "call-1".into(),
                name: "write_file".into(),
                args_json: r#"{"shikigami_content_arguments_part_id":"args-0"}"#.into(),
            }],
        }];
        let encoded = encode_content_messages(&messages, &[text, args]).unwrap();
        assert_eq!(encoded[0]["content"], "hello");
        assert_eq!(
            encoded[0]["tool_calls"][0]["function"]["name"],
            "write_file"
        );
        assert_eq!(
            encoded[0]["tool_calls"][0]["function"]["arguments"],
            r#"{"path":"a.txt"}"#
        );
        let content = encoded[0]["content"].as_str().unwrap();
        assert!(
            !content.contains("a.txt"),
            "tool arguments must not leak into assistant prose: {content}"
        );
    }

    #[test]
    fn content_messages_skip_undisclosed_parts() {
        let text = resolved("text-0", ContentPartKind::Text, "text/plain", b"look");
        let mut redacted = resolved("part-1", ContentPartKind::Image, "image/png", b"\x89PNG");
        redacted.descriptor.disclosure_state = ContentDisclosureState::Redacted;
        redacted.descriptor.disclosure_reason = "policy".into();
        let mut omitted = resolved(
            "part-2",
            ContentPartKind::Document,
            "application/pdf",
            b"%PDF",
        );
        omitted.descriptor.disclosure_state = ContentDisclosureState::Omitted;
        omitted.descriptor.disclosure_reason = "too large".into();
        let messages = vec![ContentMessageV1 {
            role: "user".into(),
            parts: vec![
                text.descriptor.clone(),
                redacted.descriptor.clone(),
                omitted.descriptor.clone(),
            ],
            tool_call_id: String::new(),
            tool_calls: Vec::new(),
        }];
        let encoded = encode_content_messages(&messages, &[text]).unwrap();
        let content = encoded[0]["content"].as_array().expect("content array");
        assert_eq!(content.len(), 1);
        assert_eq!(content[0]["type"], "text");
        assert_eq!(content[0]["text"], "look");
    }

    fn resolved(
        part_id: &str,
        kind: ContentPartKind,
        media_type: &str,
        payload: &[u8],
    ) -> ResolvedContentPart {
        let descriptor = ContentPartDescriptor {
            part_id: part_id.into(),
            kind,
            media_type: media_type.into(),
            byte_length: payload.len() as u64,
            sha256_digest: shikigami_engine::content::sha256_digest(payload),
            reference: format!("mem:{part_id}"),
            provenance: shikigami_engine::content::ContentProvenanceV1 {
                source: "test".into(),
                source_id: part_id.into(),
                source_version: "1".into(),
                observed_at_ms: 0,
            },
            disclosure_state: ContentDisclosureState::Accepted,
            disclosure_reason: String::new(),
        };
        ResolvedContentPart {
            descriptor,
            payload: if kind == ContentPartKind::Text {
                ResolvedContent::Text(String::from_utf8(payload.to_vec()).unwrap())
            } else {
                ResolvedContent::Bytes(payload.to_vec().into())
            },
        }
    }
}
