//! Handoff brief types and `handoff` application.
//!
//! Writes a brief a host may pass as the first prompt of a fresh session.
//! Does not start a session, child run, or plane session, and does not
//! mutate the workspace.

use std::path::Path;

use serde::Deserialize;

use super::path::is_unsafe_relative_path;
use super::{ToolError, parse};

/// Hard caps for untrusted model text on a handoff brief.
pub const MAX_HANDOFF_TEXT_CHARS: usize = 4096;
pub const MAX_HANDOFF_LIST_ITEMS: usize = 32;
pub const MAX_HANDOFF_FILES: usize = 32;
pub const MAX_HANDOFF_PATH_CHARS: usize = 512;

/// Brief recorded by `handoff` (event payload; not a session start).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandoffBrief {
    pub task: String,
    pub decisions: Vec<String>,
    pub files: Vec<String>,
    pub ignore: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct HandoffArgs {
    task: String,
    #[serde(default)]
    decisions: Vec<String>,
    #[serde(default)]
    files: Vec<String>,
    #[serde(default)]
    ignore: Vec<String>,
}

pub(crate) fn apply_handoff(args_json: &str) -> Result<HandoffBrief, ToolError> {
    let args: HandoffArgs = parse("handoff", args_json)?;
    let task = bounded_text("task", args.task)?;
    let decisions = bounded_text_list("decisions", args.decisions)?;
    let ignore = bounded_text_list("ignore", args.ignore)?;
    let files = bounded_files(args.files)?;
    Ok(HandoffBrief {
        task,
        decisions,
        files,
        ignore,
    })
}

pub(crate) fn format_handoff_summary(brief: &HandoffBrief) -> String {
    let mut lines = vec![
        "handoff recorded (does not start a session)".into(),
        format!("task: {}", brief.task),
    ];
    if !brief.decisions.is_empty() {
        lines.push(format!("decisions: {}", brief.decisions.join("; ")));
    }
    if !brief.files.is_empty() {
        lines.push(format!("files: {}", brief.files.join(", ")));
    }
    if !brief.ignore.is_empty() {
        lines.push(format!("ignore: {}", brief.ignore.join("; ")));
    }
    lines.join("\n")
}

fn bounded_text(field: &str, raw: String) -> Result<String, ToolError> {
    let text = raw.trim().to_string();
    if text.is_empty() || text.chars().count() > MAX_HANDOFF_TEXT_CHARS {
        return Err(ToolError::Message(format!(
            "handoff: {field} must be 1..{MAX_HANDOFF_TEXT_CHARS} characters"
        )));
    }
    Ok(text)
}

fn bounded_text_list(field: &str, items: Vec<String>) -> Result<Vec<String>, ToolError> {
    if items.len() > MAX_HANDOFF_LIST_ITEMS {
        return Err(ToolError::Message(format!(
            "handoff: at most {MAX_HANDOFF_LIST_ITEMS} {field} items"
        )));
    }
    let mut out = Vec::with_capacity(items.len());
    for raw in items {
        let text = bounded_text(field, raw)?;
        out.push(text);
    }
    Ok(out)
}

fn bounded_files(items: Vec<String>) -> Result<Vec<String>, ToolError> {
    if items.len() > MAX_HANDOFF_FILES {
        return Err(ToolError::Message(format!(
            "handoff: at most {MAX_HANDOFF_FILES} files"
        )));
    }
    let mut out = Vec::with_capacity(items.len());
    for raw in items {
        let path = raw.trim().to_string();
        if path.is_empty() || path.chars().count() > MAX_HANDOFF_PATH_CHARS {
            return Err(ToolError::Message(format!(
                "handoff: file path must be 1..{MAX_HANDOFF_PATH_CHARS} characters"
            )));
        }
        if is_unsafe_relative_path(Path::new(&path)) {
            return Err(ToolError::Message(format!(
                "handoff: file path must be workspace-relative: {path}"
            )));
        }
        out.push(path);
    }
    Ok(out)
}
