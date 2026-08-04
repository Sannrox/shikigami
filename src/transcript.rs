//! Offline export of a run transcript from local checkpoint state.
//!
//! Not plane harvest truth. Schema version is part of each JSONL line.

use std::path::Path;

use serde::Serialize;
use thiserror::Error;

use crate::checkpoint::{self, Checkpoint};
use crate::config::Config;
use crate::harness::redact_secrets_in_line;

/// Transcript JSONL schema version (bump on breaking field renames/removals).
pub const TRANSCRIPT_SCHEMA_VERSION: u32 = 1;

const DEFAULT_TRUNCATE: usize = 2_000;

#[derive(Debug, Error)]
pub enum TranscriptError {
    #[error(transparent)]
    Checkpoint(#[from] checkpoint::CheckpointError),
    #[error("transcript: {0}")]
    Message(String),
}

/// Options for [`export_run_transcript`].
#[derive(Debug, Clone)]
pub struct ExportOptions {
    /// Max characters per content/args field before truncation (default 2000).
    pub max_field_chars: usize,
    /// Optional config for secret redaction (doctor rules).
    pub config: Option<Config>,
}

impl Default for ExportOptions {
    fn default() -> Self {
        Self {
            max_field_chars: DEFAULT_TRUNCATE,
            config: None,
        }
    }
}

/// Export a run as JSONL text (one object per line).
///
/// Reads `state_runs/<run_id>/checkpoint.json`. Works offline without a plane.
pub fn export_run_transcript(
    state_runs: &Path,
    run_id: &str,
    options: &ExportOptions,
) -> Result<String, TranscriptError> {
    let cp = Checkpoint::load(state_runs, run_id)?;
    export_checkpoint(&cp, options)
}

fn export_checkpoint(cp: &Checkpoint, options: &ExportOptions) -> Result<String, TranscriptError> {
    let mut lines = Vec::new();
    lines.push(line(TranscriptLine::Meta {
        schema_version: TRANSCRIPT_SCHEMA_VERSION,
        run_id: cp.run_id.clone(),
        task: sanitize(&cp.task, options),
        prompt_id: cp.prompt_id.clone(),
        completed_turns: cp.completed_turns,
        workspace: cp.workspace.display().to_string(),
        keep_workspace: cp.keep_workspace,
        parked: cp.park.is_some(),
        todo_count: cp.todos.len(),
    })?);

    for m in &cp.messages {
        let tool_calls: Vec<TranscriptToolCall> = m
            .tool_calls
            .iter()
            .map(|c| TranscriptToolCall {
                id: c.id.clone(),
                name: c.name.clone(),
                args_json: sanitize(&c.args_json, options),
            })
            .collect();
        lines.push(line(TranscriptLine::Message {
            schema_version: TRANSCRIPT_SCHEMA_VERSION,
            role: m.role.clone(),
            content: sanitize(&m.content, options),
            tool_call_id: if m.tool_call_id.is_empty() {
                None
            } else {
                Some(m.tool_call_id.clone())
            },
            tool_calls: if tool_calls.is_empty() {
                None
            } else {
                Some(tool_calls)
            },
        })?);
    }

    if !cp.todos.is_empty() {
        lines.push(line(TranscriptLine::Todos {
            schema_version: TRANSCRIPT_SCHEMA_VERSION,
            items: cp
                .todos
                .iter()
                .map(|t| TranscriptTodo {
                    id: t.id.clone(),
                    content: sanitize(&t.content, options),
                    status: t.status.as_str().to_string(),
                })
                .collect(),
        })?);
    }

    if let Some(park) = &cp.park {
        lines.push(line(TranscriptLine::Park {
            schema_version: TRANSCRIPT_SCHEMA_VERSION,
            reason: sanitize(&park.reason, options),
            question: sanitize(&park.question, options),
            tool_call_id: park.tool_call_id.clone(),
        })?);
    }

    lines.push(line(TranscriptLine::End {
        schema_version: TRANSCRIPT_SCHEMA_VERSION,
        run_id: cp.run_id.clone(),
        message_count: cp.messages.len(),
    })?);

    Ok(lines.join("\n") + "\n")
}

fn sanitize(text: &str, options: &ExportOptions) -> String {
    let mut s = if text.chars().count() > options.max_field_chars {
        let truncated: String = text.chars().take(options.max_field_chars).collect();
        format!("{truncated}…[truncated]")
    } else {
        text.to_string()
    };
    if let Some(cfg) = &options.config {
        s = redact_secrets_in_line(&s, cfg);
    }
    s
}

fn line(v: TranscriptLine) -> Result<String, TranscriptError> {
    serde_json::to_string(&v).map_err(|e| TranscriptError::Message(e.to_string()))
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum TranscriptLine {
    Meta {
        schema_version: u32,
        run_id: String,
        task: String,
        prompt_id: String,
        completed_turns: u32,
        workspace: String,
        keep_workspace: bool,
        parked: bool,
        todo_count: usize,
    },
    Message {
        schema_version: u32,
        role: String,
        content: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        tool_call_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        tool_calls: Option<Vec<TranscriptToolCall>>,
    },
    Todos {
        schema_version: u32,
        items: Vec<TranscriptTodo>,
    },
    Park {
        schema_version: u32,
        reason: String,
        question: String,
        tool_call_id: String,
    },
    End {
        schema_version: u32,
        run_id: String,
        message_count: usize,
    },
}

#[derive(Debug, Serialize)]
struct TranscriptToolCall {
    id: String,
    name: String,
    args_json: String,
}

#[derive(Debug, Serialize)]
struct TranscriptTodo {
    id: String,
    content: String,
    status: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::{CHECKPOINT_VERSION, ParkedState};
    use crate::config::Config;
    use crate::model::ChatMessage;
    use crate::tools::{TodoItem, TodoStatus};
    use tempfile::tempdir;

    #[test]
    fn export_jsonl_has_schema_and_messages() {
        let dir = tempdir().unwrap();
        let runs = dir.path().join("runs");
        let cp = Checkpoint {
            version: CHECKPOINT_VERSION,
            run_id: "run-1".into(),
            task: "do work".into(),
            prompt_id: "harness-v1:deadbeef".into(),
            messages: vec![
                ChatMessage {
                    role: "user".into(),
                    content: "do work".into(),
                    tool_call_id: String::new(),
                    tool_calls: vec![],
                },
                ChatMessage {
                    role: "assistant".into(),
                    content: String::new(),
                    tool_call_id: String::new(),
                    tool_calls: vec![crate::model::ToolCall {
                        id: "c1".into(),
                        name: "write_file".into(),
                        args_json: r#"{"path":"a.txt","content":"hi"}"#.into(),
                    }],
                },
                ChatMessage {
                    role: "tool".into(),
                    content: "file written".into(),
                    tool_call_id: "c1".into(),
                    tool_calls: vec![],
                },
            ],
            completed_turns: 1,
            workspace: runs.join("run-1/ws"),
            keep_workspace: true,
            workspace_adapter: "directory".into(),
            park: None,
            todos: vec![TodoItem {
                id: "1".into(),
                content: "step".into(),
                status: TodoStatus::Completed,
            }],
            governance: None,
        };
        cp.save(&runs).unwrap();

        let jsonl = export_run_transcript(&runs, "run-1", &ExportOptions::default()).unwrap();
        assert!(jsonl.contains(r#""schema_version":1"#));
        assert!(jsonl.contains(r#""type":"meta""#));
        assert!(jsonl.contains(r#""type":"message""#));
        assert!(jsonl.contains(r#""type":"todos""#));
        assert!(jsonl.contains(r#""type":"end""#));
        assert!(jsonl.contains("write_file"));
        // One object per line
        let n = jsonl.lines().filter(|l| !l.is_empty()).count();
        assert!(n >= 5, "{jsonl}");
    }

    #[test]
    fn export_redacts_secrets_and_truncates() {
        let dir = tempdir().unwrap();
        let runs = dir.path().join("runs");
        // SAFETY: test process only; restore not required for short unit test.
        // Use a long secret so redaction threshold (>=8) applies.
        let secret = "supersecretvalue999";
        unsafe { std::env::set_var("SHIKIGAMI_TEST_KEY", secret) };

        let mut config = Config::default();
        config.model.api_key_env = "SHIKIGAMI_TEST_KEY".into();

        let long = "x".repeat(5_000);
        let cp = Checkpoint {
            version: CHECKPOINT_VERSION,
            run_id: "run-2".into(),
            task: format!("leak {secret}"),
            prompt_id: "p".into(),
            messages: vec![ChatMessage {
                role: "user".into(),
                content: long,
                tool_call_id: String::new(),
                tool_calls: vec![],
            }],
            completed_turns: 0,
            workspace: runs.join("ws"),
            keep_workspace: true,
            workspace_adapter: "directory".into(),
            park: Some(ParkedState {
                reason: "need help".into(),
                question: "ok?".into(),
                tool_call_id: "t".into(),
            }),
            todos: vec![],
            governance: None,
        };
        cp.save(&runs).unwrap();

        let opts = ExportOptions {
            max_field_chars: 100,
            config: Some(config),
        };
        let jsonl = export_run_transcript(&runs, "run-2", &opts).unwrap();
        assert!(!jsonl.contains(secret), "{jsonl}");
        assert!(jsonl.contains("[REDACTED]"), "{jsonl}");
        assert!(jsonl.contains("…[truncated]"), "{jsonl}");
        assert!(jsonl.contains(r#""type":"park""#));

        unsafe { std::env::remove_var("SHIKIGAMI_TEST_KEY") };
    }
}
