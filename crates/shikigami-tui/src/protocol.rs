//! ACP session/update application onto TUI state.

use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::sync::oneshot;

use shikigami::acp::AcpClient;

use crate::render::{compact_value, turn_count};
use crate::session::{
    NestedChild, PendingPermission, Plan, PlanEntry, SessionModels, Shared, ToolLine,
    TranscriptLine, TuiClient,
};

/// Replayed where compaction cut earlier messages from the run's history.
pub(crate) const COMPACTED: &str = "Earlier messages were compacted.";

/// Replayed under a prompt that got no answer (failed or cancelled first).
pub(crate) const NO_ANSWER: &str = "This prompt got no answer.";

/// Leads a failed prompt whose host marked the failure as the model's, by the
/// host's `data.shikigami.cause`. The upstream detail follows on its own line.
pub(crate) fn model_failure_lead(cause: Option<&str>) -> &'static str {
    match cause {
        Some("scriptEnded") => {
            "The scripted model has no more answers. To use a real model, set [model] in a settings file and start with --config <file>."
        }
        Some("unreachable") => {
            "Could not reach the model. Check the model address in your settings and that the server is running. Your next prompt tries again."
        }
        Some("credentials") => {
            "The model provider rejected the key. Check the key variable named in your settings. Your next prompt tries again."
        }
        Some("account") => {
            "The model provider refused for billing, quota, or rate limits. Check your provider account, or wait and try again."
        }
        _ => {
            "The model did not answer. Run shikigami doctor to check your settings. Your next prompt tries again."
        }
    }
}

#[async_trait]
impl AcpClient for TuiClient {
    fn clone_arc(&self) -> Option<std::sync::Arc<dyn AcpClient>> {
        Some(std::sync::Arc::new(self.clone()))
    }

    async fn notify(&self, method: &str, params: Value) -> Result<(), String> {
        if method != "session/update" {
            return Ok(());
        }
        if let Some(id) = params.get("sessionId").and_then(|v| v.as_str()) {
            let current = self
                .session_id
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone();
            if !current.is_empty() && current != id {
                return Ok(());
            }
        }
        let mut shared = self.shared.lock().unwrap_or_else(|e| e.into_inner());
        apply_update(&mut shared, &params);
        Ok(())
    }

    async fn request(&self, method: &str, params: Value) -> Result<Value, String> {
        if method != "session/request_permission" {
            return Err(format!("unexpected agent request {method}"));
        }
        let (tx, rx) = oneshot::channel();
        let title = params
            .pointer("/toolCall/title")
            .and_then(|v| v.as_str())
            .unwrap_or("tool")
            .to_string();
        let raw_input = params
            .pointer("/toolCall/rawInput")
            .cloned()
            .unwrap_or(json!({}));
        {
            let mut shared = self.shared.lock().unwrap_or_else(|e| e.into_inner());
            shared.begin_permission(PendingPermission {
                title,
                raw_input: raw_input.to_string(),
                tx,
            });
        }
        rx.await
            .map_err(|_| "permission response dropped".to_string())
    }
}

pub(crate) fn apply_update(shared: &mut Shared, params: &Value) {
    let update = params.get("update").unwrap_or(params);
    let children = apply_children_meta(shared, update);
    let kind = update
        .get("sessionUpdate")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    match kind {
        "user_message_chunk" => {
            if update
                .pointer("/_meta/shikigami/compacted")
                .is_some_and(|v| v == true)
            {
                shared
                    .transcript
                    .push(TranscriptLine::System(COMPACTED.into()));
            } else if let Some(text) = chunk_text(update) {
                // A replayed prompt is one chunk with its own id: one line each,
                // so a prompt that got no answer stays apart from the next.
                if update.get("messageId").is_some() {
                    shared.transcript.push(TranscriptLine::User(text));
                } else {
                    merge_text(
                        &mut shared.transcript,
                        TranscriptLine::User(String::new()),
                        text,
                    );
                }
            }
        }
        "agent_message_chunk" => {
            // A turn opens with an empty chunk; it carries no text to show.
            if let Some(text) = chunk_text(update).filter(|text| !text.is_empty()) {
                merge_text(
                    &mut shared.transcript,
                    TranscriptLine::Assistant(String::new()),
                    text,
                );
            }
        }
        "tool_call" => {
            let id = update
                .get("toolCallId")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let title = update
                .get("title")
                .and_then(|v| v.as_str())
                .unwrap_or("tool")
                .to_string();
            let status = update
                .get("status")
                .and_then(|v| v.as_str())
                .unwrap_or("pending")
                .to_string();
            let detail = update
                .get("rawInput")
                .map(compact_value)
                .filter(|s| !s.is_empty())
                .unwrap_or_default();
            // A call shown at its permission ask starts again once allowed: same line.
            if let Some(TranscriptLine::Tool(tool)) = shared
                .transcript
                .iter_mut()
                .rev()
                .find(|line| matches!(line, TranscriptLine::Tool(t) if t.id == id))
            {
                tool.title = title;
                tool.status = status;
                if !detail.is_empty() {
                    tool.detail = detail;
                }
            } else {
                shared.transcript.push(TranscriptLine::Tool(ToolLine {
                    id,
                    title,
                    status,
                    detail,
                    expanded: false,
                }));
            }
        }
        "tool_call_update" => {
            let id = update
                .get("toolCallId")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let status = update
                .get("status")
                .and_then(|v| v.as_str())
                .map(str::to_string);
            let detail = tool_update_detail(update);
            if let Some(TranscriptLine::Tool(tool)) = shared
                .transcript
                .iter_mut()
                .rev()
                .find(|line| matches!(line, TranscriptLine::Tool(t) if t.id == id))
            {
                // The host replays a call cancelled at its ask as failed with
                // the text `cancelled`; keep its path and say cancelled, as live.
                if status.as_deref() == Some("failed") && detail.as_deref() == Some("cancelled") {
                    tool.status = TOOL_CANCELLED.into();
                } else {
                    if let Some(status) = status {
                        tool.status = status;
                    }
                    if let Some(detail) = detail {
                        tool.detail = detail;
                    }
                }
            }
        }
        "plan" => {
            let entries = update
                .get("entries")
                .and_then(|v| v.as_array())
                .into_iter()
                .flatten()
                .filter_map(|entry| {
                    let content = entry.get("content").and_then(|v| v.as_str())?;
                    Some(PlanEntry {
                        content: content.to_string(),
                        status: entry
                            .get("status")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                    })
                })
                .collect::<Vec<_>>();
            if !entries.is_empty() {
                shared.plan = Some(Plan { entries });
                shared.show_plan = true;
                shared.dock_scroll = 0;
            }
        }
        _ if children => {}
        _ => return,
    }
    shared.dirty = true;
}

pub(crate) fn chunk_text(update: &Value) -> Option<String> {
    update
        .pointer("/content/text")
        .or_else(|| update.get("text"))
        .and_then(|v| v.as_str())
        .map(str::to_string)
}

/// Mark the tool calls a dead host left unfinished and say so once, from the
/// `session/load` result's `_meta.shikigami.interrupted`.
/// Applies a `session/load` result: the session's run and any interrupted step.
pub(crate) fn apply_load_result(shared: &mut Shared, result: &Value) {
    shared.run_id = run_id_of(result);
    shared.models = SessionModels::of(result);
    mark_unanswered_prompts(&mut shared.transcript);
    apply_interruption(shared, result);
    apply_children_meta(shared, result);
}

/// After a replay, a prompt followed by another prompt or by nothing failed or
/// was cancelled before any answer; the history keeps no error text, so say so.
pub(crate) fn mark_unanswered_prompts(transcript: &mut Vec<TranscriptLine>) {
    let mut index = 0;
    while index < transcript.len() {
        let unanswered = matches!(transcript[index], TranscriptLine::User(_))
            && matches!(
                transcript.get(index + 1),
                None | Some(TranscriptLine::User(_))
            );
        if unanswered {
            transcript.insert(index + 1, TranscriptLine::System(NO_ANSWER.into()));
            index += 1;
        }
        index += 1;
    }
}

/// The run id the host reports in a `session/prompt` or `session/load` result.
pub(crate) fn run_id_of(result: &Value) -> Option<String> {
    result
        .pointer("/_meta/shikigami/runId")
        .and_then(Value::as_str)
        .map(str::to_string)
}

pub(crate) fn apply_interruption(shared: &mut Shared, result: &Value) {
    let Some(calls) = result
        .pointer("/_meta/shikigami/interrupted/toolCalls")
        .and_then(Value::as_array)
    else {
        return;
    };
    let mut in_doubt = false;
    for call in calls {
        let id = call["toolCallId"].as_str().unwrap_or("");
        let status = if call["outcome"] == "in_doubt" {
            in_doubt = true;
            TOOL_IN_DOUBT
        } else {
            TOOL_NOT_RUN
        };
        if let Some(TranscriptLine::Tool(tool)) = shared
            .transcript
            .iter_mut()
            .rev()
            .find(|line| matches!(line, TranscriptLine::Tool(t) if t.id == id))
        {
            tool.status = status.into();
        }
    }
    shared.transcript.push(TranscriptLine::System(
        if in_doubt {
            "Last session stopped while a tool was running. Its result is unknown; check the workspace before you continue."
        } else {
            "Last session stopped while waiting for permission. Nothing ran; your next prompt asks again."
        }
        .into(),
    ));
    shared.dirty = true;
}

/// Tool line statuses set by `apply_interruption`, shown like `failed`.
pub(crate) const TOOL_NOT_RUN: &str = "not run";
pub(crate) const TOOL_CANCELLED: &str = "cancelled";
pub(crate) const TOOL_IN_DOUBT: &str = "result unknown";

pub(crate) fn tool_update_detail(update: &Value) -> Option<String> {
    update
        .pointer("/content/0/content/text")
        .or_else(|| update.pointer("/content/0/text"))
        .and_then(|v| v.as_str())
        .map(str::to_string)
}

/// Replace the nested-child list from `_meta.shikigami.children`.
pub(crate) fn apply_children_meta(shared: &mut Shared, value: &Value) -> bool {
    let Some(children) = value
        .pointer("/_meta/shikigami/children")
        .and_then(Value::as_array)
    else {
        return false;
    };
    shared.nested_children = children.iter().filter_map(nested_child_from_meta).collect();
    true
}

fn nested_child_from_meta(value: &Value) -> Option<NestedChild> {
    let run_id = value.get("runId").and_then(Value::as_str)?.to_string();
    if run_id.is_empty() {
        return None;
    }
    let profile = value.get("profile").and_then(Value::as_str).unwrap_or("");
    if !matches!(profile, "explore" | "plan" | "full") {
        return None;
    }
    Some(NestedChild {
        run_id,
        profile: profile.to_string(),
        status: overlay_child_status(value.get("status").and_then(Value::as_str).unwrap_or("")),
    })
}

fn overlay_child_status(raw: &str) -> String {
    match raw {
        "starting" | "running" => "running".into(),
        "parked" => "parked".into(),
        "completed" | "done" => "done".into(),
        "cancelled" | "timed_out" | "failed" | "max_turns" => "failed".into(),
        _ => "running".into(),
    }
}

pub(crate) fn merge_text(lines: &mut Vec<TranscriptLine>, kind: TranscriptLine, text: String) {
    match (lines.last_mut(), kind) {
        (Some(TranscriptLine::User(existing)), TranscriptLine::User(_)) => existing.push_str(&text),
        (Some(TranscriptLine::Assistant(existing)), TranscriptLine::Assistant(_)) => {
            existing.push_str(&text)
        }
        (_, TranscriptLine::User(_)) => lines.push(TranscriptLine::User(text)),
        (_, TranscriptLine::Assistant(_)) => lines.push(TranscriptLine::Assistant(text)),
        _ => lines.push(TranscriptLine::System(text)),
    }
}

pub(crate) fn prompt_cancelled(resp: Option<&Value>) -> bool {
    resp.and_then(|v| v.pointer("/result/stopReason"))
        .and_then(|v| v.as_str())
        == Some("cancelled")
}

/// The footer word for an ACP stop reason; wire values never reach the screen.
pub(crate) fn stop_word(reason: &str) -> &'static str {
    match reason {
        "end_turn" => "done",
        "cancelled" => "cancelled",
        "max_turn_requests" => "turn limit",
        "max_tokens" => "length limit",
        "refusal" => "refused",
        _ => "ready",
    }
}

pub(crate) fn apply_prompt_result(shared: &mut Shared, resp: Option<&Value>) {
    if let Some(result) = resp.and_then(|v| v.get("result")) {
        shared.run_id = run_id_of(result);
        let turn = turn_count(shared);
        shared.usage.record(turn, result);
        apply_children_meta(shared, result);
    }
    if let Some(reason) = resp
        .and_then(|v| v.pointer("/result/stopReason"))
        .and_then(|v| v.as_str())
    {
        if reason == "cancelled" {
            for line in &mut shared.transcript {
                if let TranscriptLine::Tool(tool) = line
                    && tool.status == "pending"
                {
                    tool.status = TOOL_CANCELLED.into();
                }
            }
        }
        shared.status = stop_word(reason).into();
        return;
    }
    if let Some(err) = resp
        .and_then(|v| v.pointer("/error/message"))
        .and_then(|v| v.as_str())
    {
        let data = resp.and_then(|v| v.pointer("/error/data/shikigami"));
        let model_failed = data
            .and_then(|d| d.get("kind"))
            .is_some_and(|k| k == "model");
        let line = if model_failed {
            let cause = data.and_then(|d| d.get("cause")).and_then(Value::as_str);
            format!("{}\n{err}", model_failure_lead(cause))
        } else {
            err.to_string()
        };
        shared.transcript.push(TranscriptLine::System(line));
        shared.status = "error".into();
        return;
    }
    shared.status = "ready".into();
}
