//! Run-scoped todo checklist types and `todo_write` application.

use serde::{Deserialize, Serialize};

use super::{ToolError, parse};

/// Hard caps for run-scoped todo lists (untrusted model text).
pub const MAX_TODO_ITEMS: usize = 32;
pub const MAX_TODO_CONTENT_CHARS: usize = 512;
pub const MAX_TODO_ID_CHARS: usize = 64;

/// Status of one run-scoped todo item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TodoStatus {
    Pending,
    InProgress,
    Completed,
    Cancelled,
}

impl TodoStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::InProgress => "in_progress",
            Self::Completed => "completed",
            Self::Cancelled => "cancelled",
        }
    }
}

/// One checklist item for a run (not a plane work-unit).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TodoItem {
    pub id: String,
    pub content: String,
    pub status: TodoStatus,
}

#[derive(Debug, Deserialize)]
struct TodoWriteArgs {
    items: Vec<TodoItemWire>,
}

#[derive(Debug, Deserialize)]
struct TodoItemWire {
    id: String,
    content: String,
    status: String,
}

pub(crate) fn apply_todo_write(args_json: &str) -> Result<Vec<TodoItem>, ToolError> {
    let args: TodoWriteArgs = parse("todo_write", args_json)?;
    if args.items.len() > MAX_TODO_ITEMS {
        return Err(ToolError::Message(format!(
            "todo_write: at most {MAX_TODO_ITEMS} items"
        )));
    }
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::with_capacity(args.items.len());
    for raw in args.items {
        let id = raw.id.trim().to_string();
        if id.is_empty() || id.chars().count() > MAX_TODO_ID_CHARS {
            return Err(ToolError::Message(format!(
                "todo_write: id must be 1..{MAX_TODO_ID_CHARS} characters"
            )));
        }
        if !seen.insert(id.clone()) {
            return Err(ToolError::Message(format!(
                "todo_write: duplicate id `{id}`"
            )));
        }
        let content = raw.content.trim().to_string();
        if content.is_empty() || content.chars().count() > MAX_TODO_CONTENT_CHARS {
            return Err(ToolError::Message(format!(
                "todo_write: content must be 1..{MAX_TODO_CONTENT_CHARS} characters"
            )));
        }
        let status = match raw.status.as_str() {
            "pending" => TodoStatus::Pending,
            "in_progress" => TodoStatus::InProgress,
            "completed" => TodoStatus::Completed,
            "cancelled" => TodoStatus::Cancelled,
            other => {
                return Err(ToolError::Message(format!(
                    "todo_write: invalid status `{other}`"
                )));
            }
        };
        out.push(TodoItem {
            id,
            content,
            status,
        });
    }
    Ok(out)
}

pub(crate) fn format_todo_summary(items: &[TodoItem]) -> String {
    if items.is_empty() {
        return "todos: (empty)".into();
    }
    let mut lines = vec![format!("todos: {} item(s)", items.len())];
    for t in items {
        lines.push(format!("- [{}] {}: {}", t.status.as_str(), t.id, t.content));
    }
    lines.join("\n")
}
