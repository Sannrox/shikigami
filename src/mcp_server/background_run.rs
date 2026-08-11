//! MCP background Run lifecycle behind one private interface.
//!
//! This module owns single-flight admission, event collection, terminal result
//! publication, status snapshots, and retained state-change signaling for
//! waits. JSON-RPC routing and result projection remain in the MCP host.

use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};
use tokio::sync::{Mutex, watch};

use crate::events::{ChannelSink, HarnessEvent};
use crate::harness::Harness;
use crate::run::RunRequest;

use super::McpRunSummary;

#[derive(Default)]
struct JobState {
    phase: JobPhase,
    events: Vec<String>,
    result: Option<Result<McpRunSummary, String>>,
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
enum JobPhase {
    #[default]
    Idle,
    Running,
    Finished,
}

impl JobPhase {
    fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Running => "running",
            Self::Finished => "finished",
        }
    }
}

pub(super) struct BackgroundRunLifecycle {
    harness: Arc<Harness>,
    state: Mutex<JobState>,
    changed: watch::Sender<u64>,
}

impl BackgroundRunLifecycle {
    pub(super) fn new(harness: Arc<Harness>) -> Arc<Self> {
        let (changed, _) = watch::channel(0);
        Arc::new(Self {
            harness,
            state: Mutex::new(JobState::default()),
            changed,
        })
    }

    pub(super) async fn start(self: &Arc<Self>, request: RunRequest) -> Result<(), String> {
        let mut state = self.state.lock().await;
        if state.phase == JobPhase::Running {
            return Err("a background run is already in progress (single-flight)".into());
        }
        *state = JobState {
            phase: JobPhase::Running,
            events: vec!["status=starting".into()],
            result: None,
        };
        self.publish_change();
        drop(state);

        let lifecycle = Arc::clone(self);
        tokio::spawn(async move {
            lifecycle.execute(request).await;
        });
        Ok(())
    }

    pub(super) async fn status_json(&self) -> Result<String, String> {
        let state = self.state.lock().await;
        let recent = state
            .events
            .iter()
            .rev()
            .take(30)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect::<Vec<_>>();
        serde_json::to_string_pretty(&json!({
            "phase": state.phase.as_str(),
            "events": recent,
            "result": match &state.result {
                Some(Ok(summary)) => json!(summary),
                Some(Err(error)) => json!({"error": error}),
                None => Value::Null,
            }
        }))
        .map_err(|error| error.to_string())
    }

    pub(super) async fn wait(&self, timeout_secs: Option<u64>) -> Result<String, String> {
        // Subscribe before observing state. A terminal change between the
        // observation and `changed()` remains retained by the watch receiver.
        let mut changed = self.changed.subscribe();
        let phase = { self.state.lock().await.phase };
        match phase {
            JobPhase::Idle => return Err("no background run (call run_start first)".into()),
            JobPhase::Finished => return self.status_json().await,
            JobPhase::Running => {}
        }
        let wait = async {
            loop {
                changed
                    .changed()
                    .await
                    .map_err(|_| "background run lifecycle closed".to_string())?;
                if self.state.lock().await.phase == JobPhase::Finished {
                    return Ok::<(), String>(());
                }
            }
        };
        if let Some(seconds) = timeout_secs {
            match tokio::time::timeout(Duration::from_secs(seconds), wait).await {
                Ok(result) => result?,
                Err(_) => {
                    return Ok(json!({
                        "phase": "running",
                        "error": format!("timed out waiting after {seconds}s")
                    })
                    .to_string());
                }
            }
        } else {
            wait.await?;
        }
        self.status_json().await
    }

    async fn execute(&self, request: RunRequest) {
        let (sink, receiver) = ChannelSink::pair();
        let run = self.harness.run_with_events(request, Some(Arc::new(sink)));
        let drain = tokio::task::spawn_blocking(move || {
            let mut lines = Vec::new();
            while let Ok(event) = receiver.recv() {
                lines.push(format_event(&event));
            }
            lines
        });
        let result = run.await;
        let events = drain.await.unwrap_or_default();
        let mut state = self.state.lock().await;
        state.events.extend(events);
        state.phase = JobPhase::Finished;
        state.result = Some(match result {
            Ok(result) => Ok(McpRunSummary::from(&result)),
            Err(error) => Err(error.to_string()),
        });
        self.publish_change();
    }

    fn publish_change(&self) {
        self.changed
            .send_modify(|version| *version = version.wrapping_add(1));
    }
}

fn format_event(event: &HarnessEvent) -> String {
    match event {
        HarnessEvent::Status { status } => format!("status={status}"),
        HarnessEvent::ToolStart { name, .. } => format!("tool_start={name}"),
        HarnessEvent::ToolEnd { name, ok, .. } => format!("tool_end={name} ok={ok}"),
        HarnessEvent::ModelTurn { turn, .. } => format!("model_turn={turn}"),
        HarnessEvent::RunFinished {
            run_id,
            success,
            summary,
        } => format!("run_finished id={run_id} success={success} summary={summary}"),
        HarnessEvent::Prompt { prompt_id } => format!("prompt={prompt_id}"),
        HarnessEvent::ContextCompacted { before, after } => {
            format!("compacted before={before} after={after}")
        }
        HarnessEvent::TodosUpdated { item_count, .. } => {
            format!("todos item_count={item_count}")
        }
        HarnessEvent::Message { level, text } => format!("message[{level}]={text}"),
    }
}
