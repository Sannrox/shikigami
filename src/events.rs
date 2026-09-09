//! Harness-local event sinks (not control-plane truth).

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::config::Config;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HarnessEvent {
    Status {
        status: String,
    },
    ToolStart {
        name: String,
        args_json: String,
        #[serde(default)]
        run_id: String,
        #[serde(default)]
        turn: u32,
        #[serde(default)]
        call_id: String,
    },
    ToolEnd {
        name: String,
        ok: bool,
        detail: String,
        #[serde(default)]
        run_id: String,
        #[serde(default)]
        turn: u32,
        #[serde(default)]
        call_id: String,
    },
    ModelTurn {
        turn: u32,
        content_preview: String,
    },
    /// Metadata-only projection for a bounded content model turn.
    ContentTurn {
        turn: u32,
        part_count: usize,
        kinds: Vec<String>,
        digests: Vec<String>,
    },
    Message {
        level: String,
        text: String,
    },
    RunFinished {
        run_id: String,
        success: bool,
        summary: String,
    },
    /// Prompt attribution for the active run.
    Prompt {
        prompt_id: String,
    },
    /// Older messages were compacted for context size.
    ContextCompacted {
        before: usize,
        after: usize,
    },
    /// Run-scoped todo checklist was replaced via `todo_write`.
    TodosUpdated {
        /// Compact summary (counts + lines); full items live on checkpoint / RunResult.
        summary: String,
        item_count: usize,
    },
}

pub trait EventSink: Send + Sync {
    fn id(&self) -> &'static str;
    fn emit(&self, event: HarnessEvent);
    fn health_detail(&self) -> String;
}

/// Fan-out to multiple sinks (config sink + embedder subscription).
pub struct FanoutSink {
    sinks: Vec<std::sync::Arc<dyn EventSink>>,
}

impl FanoutSink {
    pub fn new(sinks: Vec<std::sync::Arc<dyn EventSink>>) -> Self {
        Self { sinks }
    }
}

impl EventSink for FanoutSink {
    fn id(&self) -> &'static str {
        "fanout"
    }
    fn emit(&self, event: HarnessEvent) {
        for s in &self.sinks {
            s.emit(event.clone());
        }
    }
    fn health_detail(&self) -> String {
        let ids: Vec<_> = self.sinks.iter().map(|s| s.id()).collect();
        format!("fanout({})", ids.join("+"))
    }
}

/// In-process channel sink for embedders (lossy if receiver lags: drops).
pub struct ChannelSink {
    tx: std::sync::mpsc::Sender<HarnessEvent>,
}

impl ChannelSink {
    pub fn pair() -> (Self, std::sync::mpsc::Receiver<HarnessEvent>) {
        let (tx, rx) = std::sync::mpsc::channel();
        (Self { tx }, rx)
    }
}

impl EventSink for ChannelSink {
    fn id(&self) -> &'static str {
        "channel"
    }
    fn emit(&self, event: HarnessEvent) {
        let _ = self.tx.send(event);
    }
    fn health_detail(&self) -> String {
        "in-process channel".into()
    }
}

pub fn from_config(config: &Config, state_runs: &Path) -> Result<Box<dyn EventSink>, EventError> {
    match config.events.adapter.as_str() {
        "none" => Ok(Box::new(NoneSink)),
        "stderr" => Ok(Box::new(StderrSink)),
        "jsonl" => Ok(Box::new(JsonlSink::open(state_runs.join("events.jsonl"))?)),
        other => Err(EventError::Unknown(other.into())),
    }
}

#[derive(Debug, Error)]
pub enum EventError {
    #[error("unknown events adapter `{0}`")]
    Unknown(String),
    #[error("events I/O: {0}")]
    Io(#[from] std::io::Error),
}

struct NoneSink;
impl EventSink for NoneSink {
    fn id(&self) -> &'static str {
        "none"
    }
    fn emit(&self, _event: HarnessEvent) {}
    fn health_detail(&self) -> String {
        "events discarded".into()
    }
}

struct StderrSink;
impl EventSink for StderrSink {
    fn id(&self) -> &'static str {
        "stderr"
    }
    fn emit(&self, event: HarnessEvent) {
        if let Ok(line) = serde_json::to_string(&event) {
            eprintln!("[shikigami] {line}");
        }
    }
    fn health_detail(&self) -> String {
        "write progress to stderr".into()
    }
}

struct JsonlSink {
    path: PathBuf,
    file: Mutex<File>,
}

impl JsonlSink {
    fn open(path: PathBuf) -> Result<Self, EventError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok(Self {
            path,
            file: Mutex::new(file),
        })
    }
}

impl EventSink for JsonlSink {
    fn id(&self) -> &'static str {
        "jsonl"
    }
    fn emit(&self, event: HarnessEvent) {
        if let Ok(mut line) = serde_json::to_string(&event) {
            line.push('\n');
            if let Ok(mut f) = self.file.lock() {
                let _ = f.write_all(line.as_bytes());
                let _ = f.flush();
            }
        }
    }
    fn health_detail(&self) -> String {
        format!("append {}", self.path.display())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tool_start_json_is_additive_and_unknown_type_fails() {
        let event = HarnessEvent::ToolStart {
            name: "bash".into(),
            args_json: "{}".into(),
            run_id: "run-1".into(),
            turn: 2,
            call_id: "tool-2-0".into(),
        };
        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["type"], "tool_start");
        assert_eq!(json["call_id"], "tool-2-0");
        assert_eq!(json["turn"], 2);
        assert_eq!(json["run_id"], "run-1");

        let legacy = serde_json::from_str::<HarnessEvent>(
            r#"{"type":"tool_start","name":"bash","args_json":"{}"}"#,
        )
        .unwrap();
        match legacy {
            HarnessEvent::ToolStart {
                name,
                call_id,
                turn,
                run_id,
                ..
            } => {
                assert_eq!(name, "bash");
                assert!(call_id.is_empty());
                assert_eq!(turn, 0);
                assert!(run_id.is_empty());
            }
            other => panic!("{other:?}"),
        }

        let err = serde_json::from_str::<HarnessEvent>(r#"{"type":"tool_start_v2"}"#);
        assert!(err.is_err(), "{err:?}");
    }
}
