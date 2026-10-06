//! Harness-local event sinks (not control-plane truth).

use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

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

/// Bound for ACP forwarding. `emit` never blocks. Extra model/content
/// previews coalesce at this size; tool lifecycle events and the latest
/// preview are not dropped.
pub const ASYNC_CHANNEL_BOUND: usize = 256;

struct AsyncChannelInner {
    queue: Mutex<QueueState>,
    notify: tokio::sync::Notify,
}

struct QueueState {
    events: VecDeque<HarnessEvent>,
    capacity: usize,
    closed: bool,
}

/// Bounded async sink. `EventSink::emit` is sync and must not `blocking_send`
/// on the runtime that also runs the ACP forwarder.
pub struct AsyncChannelSink {
    inner: Arc<AsyncChannelInner>,
    capacity: usize,
}

/// Receiver half of [`AsyncChannelSink`].
pub struct AsyncChannelRx {
    inner: Arc<AsyncChannelInner>,
}

impl AsyncChannelSink {
    pub fn pair() -> (Self, AsyncChannelRx) {
        Self::bounded(ASYNC_CHANNEL_BOUND)
    }

    pub fn bounded(capacity: usize) -> (Self, AsyncChannelRx) {
        let capacity = capacity.max(1);
        let inner = Arc::new(AsyncChannelInner {
            queue: Mutex::new(QueueState {
                events: VecDeque::with_capacity(capacity),
                capacity,
                closed: false,
            }),
            notify: tokio::sync::Notify::new(),
        });
        (
            Self {
                inner: Arc::clone(&inner),
                capacity,
            },
            AsyncChannelRx { inner },
        )
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

impl Drop for AsyncChannelSink {
    fn drop(&mut self) {
        self.inner
            .queue
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .closed = true;
        self.inner.notify.notify_waiters();
    }
}

impl AsyncChannelRx {
    pub async fn recv(&mut self) -> Option<HarnessEvent> {
        loop {
            let notified = self.inner.notify.notified();
            match self.pop_or_closed() {
                Ok(event) => return Some(event),
                Err(true) => return None,
                Err(false) => notified.await,
            }
        }
    }

    pub fn try_recv(&self) -> Option<HarnessEvent> {
        self.inner
            .queue
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .events
            .pop_front()
    }

    fn pop_or_closed(&self) -> Result<HarnessEvent, bool> {
        let mut state = self.inner.queue.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(event) = state.events.pop_front() {
            return Ok(event);
        }
        Err(state.closed)
    }
}

fn is_model_preview(event: &HarnessEvent) -> bool {
    matches!(
        event,
        HarnessEvent::ModelTurn { .. } | HarnessEvent::ContentTurn { .. }
    )
}

fn enqueue_bounded(state: &mut QueueState, event: HarnessEvent) {
    if state.closed {
        return;
    }
    if is_model_preview(&event) {
        if state.events.len() >= state.capacity {
            state.events.retain(|queued| !is_model_preview(queued));
        }
        state.events.push_back(event);
        return;
    }
    state.events.push_back(event);
}

impl EventSink for AsyncChannelSink {
    fn id(&self) -> &'static str {
        "async-channel"
    }
    fn emit(&self, event: HarnessEvent) {
        {
            let mut state = self.inner.queue.lock().unwrap_or_else(|e| e.into_inner());
            enqueue_bounded(&mut state, event);
        }
        self.inner.notify.notify_one();
    }
    fn health_detail(&self) -> String {
        format!("bounded async channel ({})", self.capacity)
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

    fn tool_start(call_id: &str) -> HarnessEvent {
        HarnessEvent::ToolStart {
            name: "bash".into(),
            args_json: "{}".into(),
            run_id: "run-1".into(),
            turn: 1,
            call_id: call_id.into(),
        }
    }

    fn model_turn(turn: u32, preview: &str) -> HarnessEvent {
        HarnessEvent::ModelTurn {
            turn,
            content_preview: preview.into(),
        }
    }

    fn drain_ready(rx: &mut AsyncChannelRx) -> Vec<HarnessEvent> {
        let mut events = Vec::new();
        while let Some(event) = rx.try_recv() {
            events.push(event);
        }
        events
    }

    #[test]
    fn async_channel_stays_bounded_and_coalesces_model_previews() {
        let capacity = 4;
        let (sink, mut rx) = AsyncChannelSink::bounded(capacity);
        assert_eq!(sink.capacity(), capacity);
        for turn in 0..20 {
            sink.emit(model_turn(turn, &format!("t{turn}")));
        }
        let drained = drain_ready(&mut rx);
        assert!(
            drained.len() <= capacity,
            "bounded sink grew to {}",
            drained.len()
        );
        assert!(
            drained
                .iter()
                .all(|event| matches!(event, HarnessEvent::ModelTurn { .. })),
            "{drained:?}"
        );
        let last = drained.last().and_then(|event| match event {
            HarnessEvent::ModelTurn {
                content_preview, ..
            } => Some(content_preview.as_str()),
            _ => None,
        });
        assert_eq!(last, Some("t19"), "{drained:?}");
    }

    #[test]
    fn async_channel_keeps_tool_then_model_order_under_the_bound() {
        let (sink, mut rx) = AsyncChannelSink::bounded(8);
        sink.emit(tool_start("tool-1-0"));
        sink.emit(HarnessEvent::ToolEnd {
            name: "bash".into(),
            ok: true,
            detail: "ok".into(),
            run_id: "run-1".into(),
            turn: 1,
            call_id: "tool-1-0".into(),
        });
        sink.emit(tool_start("tool-1-1"));
        sink.emit(HarnessEvent::ToolEnd {
            name: "bash".into(),
            ok: true,
            detail: "ok".into(),
            run_id: "run-1".into(),
            turn: 1,
            call_id: "tool-1-1".into(),
        });
        sink.emit(model_turn(2, "hello"));
        let kinds: Vec<&str> = drain_ready(&mut rx)
            .iter()
            .map(|event| match event {
                HarnessEvent::ToolStart { .. } => "tool_start",
                HarnessEvent::ToolEnd { .. } => "tool_end",
                HarnessEvent::ModelTurn { .. } => "model_turn",
                other => panic!("{other:?}"),
            })
            .collect();
        assert_eq!(
            kinds,
            [
                "tool_start",
                "tool_end",
                "tool_start",
                "tool_end",
                "model_turn"
            ]
        );
    }

    #[test]
    fn async_channel_coalesces_previews_without_dropping_tools() {
        let (sink, mut rx) = AsyncChannelSink::bounded(1);
        sink.emit(model_turn(1, "preview"));
        sink.emit(model_turn(2, "later"));
        sink.emit(tool_start("tool-1-0"));
        let drained = drain_ready(&mut rx);
        let kinds: Vec<String> = drained
            .iter()
            .map(|event| match event {
                HarnessEvent::ModelTurn {
                    content_preview, ..
                } => content_preview.clone(),
                HarnessEvent::ToolStart { call_id, .. } => call_id.clone(),
                other => panic!("{other:?}"),
            })
            .collect();
        assert_eq!(kinds, vec!["later", "tool-1-0"]);
    }

    #[test]
    fn async_channel_preserves_discrete_events_and_the_latest_preview() {
        let (sink, mut rx) = AsyncChannelSink::bounded(2);
        sink.emit(model_turn(1, "preview"));
        sink.emit(tool_start("tool-1-0"));
        sink.emit(tool_start("tool-1-1"));
        let drained = drain_ready(&mut rx);
        let kinds: Vec<&str> = drained
            .iter()
            .map(|event| match event {
                HarnessEvent::ModelTurn { .. } => "model_turn",
                HarnessEvent::ToolStart { call_id, .. } => call_id.as_str(),
                other => panic!("{other:?}"),
            })
            .collect();
        assert_eq!(kinds, ["model_turn", "tool-1-0", "tool-1-1"]);
    }

    #[test]
    fn async_channel_appends_coalesced_preview_after_intervening_tools() {
        let (sink, mut rx) = AsyncChannelSink::bounded(3);
        sink.emit(model_turn(1, "first"));
        sink.emit(tool_start("tool-1-0"));
        sink.emit(HarnessEvent::ToolEnd {
            name: "bash".into(),
            ok: true,
            detail: "ok".into(),
            run_id: "run-1".into(),
            turn: 1,
            call_id: "tool-1-0".into(),
        });
        sink.emit(model_turn(2, "second"));
        let drained = drain_ready(&mut rx);
        let kinds: Vec<&str> = drained
            .iter()
            .map(|event| match event {
                HarnessEvent::ToolStart { .. } => "tool_start",
                HarnessEvent::ToolEnd { .. } => "tool_end",
                HarnessEvent::ModelTurn {
                    content_preview, ..
                } => content_preview.as_str(),
                other => panic!("{other:?}"),
            })
            .collect();
        assert_eq!(kinds, ["tool_start", "tool_end", "second"]);
    }

    #[test]
    fn async_channel_keeps_latest_preview_when_full_of_tools() {
        let (sink, mut rx) = AsyncChannelSink::bounded(2);
        sink.emit(tool_start("tool-1-0"));
        sink.emit(tool_start("tool-1-1"));
        sink.emit(model_turn(1, "final"));
        sink.emit(HarnessEvent::RunFinished {
            run_id: "run-1".into(),
            success: true,
            summary: "done".into(),
        });
        let drained = drain_ready(&mut rx);
        assert!(
            drained.iter().any(|event| matches!(
                event,
                HarnessEvent::ModelTurn { content_preview, .. } if content_preview == "final"
            )),
            "{drained:?}"
        );
        assert!(
            drained
                .iter()
                .any(|event| matches!(event, HarnessEvent::RunFinished { .. })),
            "{drained:?}"
        );
    }

    #[tokio::test]
    async fn recv_delivers_the_last_event_after_the_sink_drops() {
        let (sink, mut rx) = AsyncChannelSink::bounded(4);
        sink.emit(tool_start("last"));
        drop(sink);
        match rx.recv().await {
            Some(HarnessEvent::ToolStart { call_id, .. }) => assert_eq!(call_id, "last"),
            other => panic!("{other:?}"),
        }
        assert!(rx.recv().await.is_none());
    }

    #[test]
    fn async_channel_does_not_drop_discrete_events_when_full() {
        let (sink, mut rx) = AsyncChannelSink::bounded(2);
        for index in 0..10 {
            sink.emit(tool_start(&format!("tool-{index}")));
        }
        let ids: Vec<String> = drain_ready(&mut rx)
            .into_iter()
            .map(|event| match event {
                HarnessEvent::ToolStart { call_id, .. } => call_id,
                other => panic!("{other:?}"),
            })
            .collect();
        assert_eq!(
            ids,
            (0..10)
                .map(|index| format!("tool-{index}"))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn async_channel_keeps_queued_events_ahead_of_new_emits() {
        let (sink, mut rx) = AsyncChannelSink::bounded(2);
        sink.emit(tool_start("tool-1-0"));
        sink.emit(model_turn(1, "preview"));
        match rx.try_recv() {
            Some(HarnessEvent::ToolStart { call_id, .. }) => assert_eq!(call_id, "tool-1-0"),
            other => panic!("{other:?}"),
        }
        sink.emit(tool_start("tool-1-1"));
        let kinds: Vec<&str> = drain_ready(&mut rx)
            .iter()
            .map(|event| match event {
                HarnessEvent::ModelTurn { .. } => "model_turn",
                HarnessEvent::ToolStart { .. } => "tool_start",
                other => panic!("{other:?}"),
            })
            .collect();
        assert_eq!(kinds, ["model_turn", "tool_start"]);
    }
}
