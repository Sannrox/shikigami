//! Harness-local event sinks (not control-plane truth).

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use serde::Serialize;
use thiserror::Error;

use crate::config::Config;

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HarnessEvent {
    Status {
        status: String,
    },
    ToolStart {
        name: String,
        args_json: String,
    },
    ToolEnd {
        name: String,
        ok: bool,
        detail: String,
    },
    ModelTurn {
        turn: u32,
        content_preview: String,
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
