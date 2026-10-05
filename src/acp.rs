//! ACP process host: newline-delimited JSON-RPC session over [`crate::Harness`].
//!
//! Evolving surface, same rank as `shikigami mcp`. Not freeze-core.
//! See [ADR 0014](../docs/decisions/0014-usable-guest-hosts.md).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::io::{BufReader, stdin, stdout};
use tokio::sync::{Mutex, mpsc, oneshot, watch};

use crate::checkpoint::{Checkpoint, ParkedState, is_safe_run_id};
use crate::events::{ChannelSink, EventSink, HarnessEvent};
use crate::harness::{Harness, HarnessError};
use crate::identity::{PRODUCT, VERSION};
use crate::mcp::framing;
use crate::model::ChatMessage;
use crate::run::{
    AskDecision, ParkInfo, ParkKind, PlanDecision, RunError, RunRequest, RunTermination,
};

const PROTOCOL_VERSION: u32 = 1;

type PendingPermissions = HashMap<u64, (String, oneshot::Sender<Value>)>;

/// JSON-RPC client callbacks the agent issues during a prompt (updates + permission).
#[async_trait]
pub trait AcpClient: Send + Sync {
    async fn notify(&self, method: &str, params: Value) -> Result<(), String>;
    async fn request(&self, method: &str, params: Value) -> Result<Value, String>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedSession {
    session_id: String,
    cwd: String,
    run_id: Option<String>,
}

#[derive(Clone)]
struct LiveSession {
    cwd: PathBuf,
    run_id: Option<String>,
    cancel: Option<watch::Sender<bool>>,
}

/// In-process ACP agent. Stdio and tests share this.
pub struct AcpHost {
    harness: Harness,
    initialized: AtomicBool,
    sessions: Mutex<HashMap<String, LiveSession>>,
}

impl AcpHost {
    pub fn new(harness: Harness) -> Self {
        Self {
            harness,
            initialized: AtomicBool::new(false),
            sessions: Mutex::new(HashMap::new()),
        }
    }

    /// Handle one client JSON-RPC message. Notifications return `None`.
    pub async fn handle(&self, msg: Value, client: &dyn AcpClient) -> Option<Value> {
        let method = msg.get("method").and_then(|m| m.as_str()).unwrap_or("");
        if method == "session/cancel" {
            let params = msg.get("params").cloned().unwrap_or(Value::Null);
            self.cancel_session(&params).await;
            return None;
        }
        let id = msg.get("id").cloned()?;
        let params = msg.get("params").cloned().unwrap_or(Value::Null);
        let result = match method {
            "initialize" => self.initialize(&params),
            "session/new" => self.session_new(&params).await,
            "session/load" => self.session_load(&params, client).await,
            "session/prompt" => self.session_prompt(&params, client).await,
            other => Err(rpc_error(-32601, format!("Method not found: {other}"))),
        };
        Some(match result {
            Ok(value) => json!({"jsonrpc": "2.0", "id": id, "result": value}),
            Err(error) => json!({"jsonrpc": "2.0", "id": id, "error": error}),
        })
    }

    fn initialize(&self, params: &Value) -> Result<Value, Value> {
        let version = params
            .get("protocolVersion")
            .and_then(|v| {
                v.as_u64()
                    .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
            })
            .unwrap_or(0);
        if version != 1 && version != 2 {
            return Err(rpc_error(
                -32602,
                format!("unsupported protocolVersion {version}"),
            ));
        }
        self.initialized.store(true, Ordering::SeqCst);
        Ok(json!({
            "protocolVersion": PROTOCOL_VERSION,
            "agentCapabilities": {
                "loadSession": true,
                "promptCapabilities": {
                    "image": false,
                    "audio": false,
                    "embeddedContext": false
                }
            },
            "agentInfo": {
                "name": PRODUCT,
                "version": VERSION
            },
            "authMethods": []
        }))
    }

    async fn session_new(&self, params: &Value) -> Result<Value, Value> {
        self.require_init()?;
        let cwd = params
            .get("cwd")
            .and_then(|v| v.as_str())
            .map(PathBuf::from)
            .filter(|p| p.is_absolute())
            .ok_or_else(|| rpc_error(-32602, "session/new requires absolute cwd"))?;
        let session_id = format!("sess-{}", uuid::Uuid::new_v4());
        let live = LiveSession {
            cwd: cwd.clone(),
            run_id: None,
            cancel: None,
        };
        self.persist(&session_id, &live)
            .map_err(|e| rpc_error(-32603, e))?;
        self.sessions.lock().await.insert(session_id.clone(), live);
        Ok(json!({ "sessionId": session_id }))
    }

    async fn session_load(&self, params: &Value, client: &dyn AcpClient) -> Result<Value, Value> {
        self.require_init()?;
        let session_id = params
            .get("sessionId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| rpc_error(-32602, "session/load requires sessionId"))?;
        if !is_safe_run_id(session_id) {
            return Err(rpc_error(-32002, "unknown session"));
        }
        let live = if let Some(live) = self.sessions.lock().await.get(session_id) {
            LiveSession {
                cwd: live.cwd.clone(),
                run_id: live.run_id.clone(),
                cancel: None,
            }
        } else {
            let persisted = self
                .load_persisted(session_id)
                .ok_or_else(|| rpc_error(-32002, "unknown session"))?;
            let live = LiveSession {
                cwd: PathBuf::from(persisted.cwd),
                run_id: persisted.run_id,
                cancel: None,
            };
            self.sessions
                .lock()
                .await
                .insert(session_id.to_string(), live.clone());
            live
        };
        self.replay_session_history(session_id, &live, client)
            .await?;
        Ok(json!({
            "sessionId": session_id,
            "cwd": live.cwd.display().to_string(),
        }))
    }

    async fn replay_session_history(
        &self,
        session_id: &str,
        live: &LiveSession,
        client: &dyn AcpClient,
    ) -> Result<(), Value> {
        let Some(run_id) = live.run_id.as_deref() else {
            return Ok(());
        };
        let harness = self
            .harness_for_cwd(&live.cwd)
            .map_err(|e| rpc_error(-32603, e))?;
        let checkpoint = Checkpoint::load(&harness.state.runs_dir(), run_id)
            .map_err(|_| rpc_error(-32603, "session run checkpoint is unreadable"))?;
        send_updates(
            client,
            conversation_updates(session_id, &checkpoint.messages),
        )
        .await
    }

    async fn session_prompt(&self, params: &Value, client: &dyn AcpClient) -> Result<Value, Value> {
        self.require_init()?;
        let session_id = params
            .get("sessionId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| rpc_error(-32602, "session/prompt requires sessionId"))?
            .to_string();
        let prompt_text = prompt_text(params.get("prompt").unwrap_or(&Value::Null))?;
        let (cwd, resume_run_id, cancel_rx) = {
            let mut sessions = self.sessions.lock().await;
            let live = sessions
                .get_mut(&session_id)
                .ok_or_else(|| rpc_error(-32002, "unknown session"))?;
            let rx = match live.cancel.as_ref() {
                Some(tx) => tx.subscribe(),
                None => {
                    let (tx, rx) = watch::channel(false);
                    live.cancel = Some(tx);
                    rx
                }
            };
            (live.cwd.clone(), live.run_id.clone(), rx)
        };

        let outcome = async {
            let harness = self
                .harness_for_cwd(&cwd)
                .map_err(|e| rpc_error(-32603, e))?;
            let (sink, events) = ChannelSink::pair();
            let sink: Arc<dyn EventSink> = Arc::new(sink);
            let events = Arc::new(std::sync::Mutex::new(events));

            let mut request = if let Some(run_id) = resume_run_id.clone() {
                match Checkpoint::load(&harness.state.runs_dir(), &run_id) {
                    Ok(checkpoint) if checkpoint.is_prompt_wait() => {
                        let mut request = RunRequest::new("");
                        request.resume_run_id = Some(run_id);
                        request.resume_prompt = Some(prompt_text.clone());
                        request
                    }
                    Ok(checkpoint)
                        if checkpoint.is_ask_park()
                            || checkpoint.is_escalate_park()
                            || checkpoint.is_plan_park() =>
                    {
                        let info = park_info_from_checkpoint(&checkpoint)
                            .map_err(|e| rpc_error(-32603, e))?;
                        let (outcome, answer) =
                            request_permission(&session_id, &info, client, Some(&cancel_rx))
                                .await?;
                        if outcome == PermissionOutcome::Cancelled {
                            persist_prompt_wait(&harness.state.runs_dir(), &run_id)
                                .map_err(|e| rpc_error(-32603, e))?;
                            return Ok(json!({ "stopReason": "cancelled" }));
                        }
                        let mut request = RunRequest::new("");
                        request.resume_run_id = Some(run_id);
                        if checkpoint.is_ask_park() {
                            request.resume_ask = Some(match outcome {
                                PermissionOutcome::Allow => AskDecision::Allow,
                                PermissionOutcome::Deny => AskDecision::Deny,
                                PermissionOutcome::Cancelled => unreachable!("cancelled returned"),
                            });
                        } else if checkpoint.is_plan_park() {
                            request.resume_plan = Some(match outcome {
                                PermissionOutcome::Allow => PlanDecision::Accept,
                                PermissionOutcome::Deny => PlanDecision::Reject,
                                PermissionOutcome::Cancelled => unreachable!("cancelled returned"),
                            });
                        } else {
                            request.resume_answer = Some(answer);
                        }
                        request
                    }
                    Ok(checkpoint)
                        if checkpoint
                            .park
                            .as_ref()
                            .is_some_and(|park| park.kind == ParkKind::Approval) =>
                    {
                        return Err(rpc_error(
                            -32603,
                            "governed approval parks are not mapped on ACP yet",
                        ));
                    }
                    Ok(_) => {
                        return Err(rpc_error(-32603, "session run is not waiting for a prompt"));
                    }
                    Err(_) => {
                        return Err(rpc_error(-32603, "session run checkpoint is unreadable"));
                    }
                }
            } else {
                RunRequest::new(prompt_text.clone())
            };
            request.keep_workspace = true;
            request.session_wait = true;
            request.cancel = Some(cancel_rx);

            let stop = self
                .drive_prompt(&session_id, harness, request, client, sink, events)
                .await?;
            Ok(json!({ "stopReason": stop }))
        }
        .await;
        self.clear_cancel(&session_id).await;
        outcome
    }

    async fn drive_prompt(
        &self,
        session_id: &str,
        harness: Harness,
        mut request: RunRequest,
        client: &dyn AcpClient,
        sink: Arc<dyn EventSink>,
        events: Arc<std::sync::Mutex<std::sync::mpsc::Receiver<HarnessEvent>>>,
    ) -> Result<&'static str, Value> {
        loop {
            if let Some(run_id) = request.resume_run_id.clone() {
                let _ = self.set_run_id(session_id, Some(run_id)).await;
            }
            let (result, forwarded_run_id) = run_and_forward(
                self,
                &harness,
                request.clone(),
                Arc::clone(&sink),
                &events,
                session_id,
                client,
            )
            .await;
            let result = match result {
                Ok(result) => result,
                Err(HarnessError::Run(RunError::Cancelled)) => {
                    self.retain_cancelled_run(
                        session_id,
                        &harness,
                        request.resume_run_id.clone().or(forwarded_run_id),
                    )
                    .await;
                    return Ok("cancelled");
                }
                Err(e) => return Err(rpc_error(-32603, e.to_string())),
            };
            self.set_run_id(session_id, Some(result.run_id.clone()))
                .await
                .map_err(|e| rpc_error(-32603, e))?;

            if result.termination == RunTermination::Cancelled {
                self.retain_cancelled_run(session_id, &harness, Some(result.run_id.clone()))
                    .await;
                return Ok("cancelled");
            }
            if result.termination == RunTermination::Failed && result.summary == "plan rejected" {
                self.set_run_id(session_id, None)
                    .await
                    .map_err(|e| rpc_error(-32603, e))?;
                return Ok("end_turn");
            }
            let park = result.park.as_ref();
            match park.map(|p| p.kind) {
                Some(ParkKind::PromptWait) => {
                    return Ok(if park.expect("park").reason == "max_turns" {
                        "max_turn_requests"
                    } else {
                        "end_turn"
                    });
                }
                Some(ParkKind::Ask) => {
                    let park = park.expect("park");
                    let (outcome, _) =
                        request_permission(session_id, park, client, request.cancel.as_ref())
                            .await?;
                    let mut next = RunRequest::new("");
                    next.keep_workspace = true;
                    next.session_wait = true;
                    next.resume_run_id = Some(result.run_id.clone());
                    next.cancel = request.cancel.clone();
                    match outcome {
                        PermissionOutcome::Cancelled => {
                            persist_prompt_wait(&harness.state.runs_dir(), &result.run_id)
                                .map_err(|e| rpc_error(-32603, e))?;
                            return Ok("cancelled");
                        }
                        PermissionOutcome::Allow => {
                            next.resume_ask = Some(AskDecision::Allow);
                        }
                        PermissionOutcome::Deny => {
                            next.resume_ask = Some(AskDecision::Deny);
                        }
                    }
                    request = next;
                }
                Some(ParkKind::Plan) => {
                    let park = park.expect("park");
                    let (outcome, _) =
                        request_permission(session_id, park, client, request.cancel.as_ref())
                            .await?;
                    let mut next = RunRequest::new("");
                    next.keep_workspace = true;
                    next.session_wait = true;
                    next.resume_run_id = Some(result.run_id.clone());
                    next.cancel = request.cancel.clone();
                    match outcome {
                        PermissionOutcome::Cancelled => {
                            persist_prompt_wait(&harness.state.runs_dir(), &result.run_id)
                                .map_err(|e| rpc_error(-32603, e))?;
                            return Ok("cancelled");
                        }
                        PermissionOutcome::Allow => {
                            next.resume_plan = Some(PlanDecision::Accept);
                        }
                        PermissionOutcome::Deny => {
                            next.resume_plan = Some(PlanDecision::Reject);
                        }
                    }
                    request = next;
                }
                Some(ParkKind::Escalate) => {
                    let park = park.expect("park");
                    let (outcome, answer) =
                        request_permission(session_id, park, client, request.cancel.as_ref())
                            .await?;
                    let mut next = RunRequest::new("");
                    next.keep_workspace = true;
                    next.session_wait = true;
                    next.resume_run_id = Some(result.run_id.clone());
                    next.cancel = request.cancel.clone();
                    match outcome {
                        PermissionOutcome::Cancelled => {
                            persist_prompt_wait(&harness.state.runs_dir(), &result.run_id)
                                .map_err(|e| rpc_error(-32603, e))?;
                            return Ok("cancelled");
                        }
                        PermissionOutcome::Allow | PermissionOutcome::Deny => {
                            next.resume_answer = Some(answer);
                        }
                    }
                    request = next;
                }
                Some(ParkKind::Approval) => {
                    return Err(rpc_error(
                        -32603,
                        "governed approval parks are not mapped on ACP yet",
                    ));
                }
                None => {
                    if result.termination == RunTermination::Completed {
                        return Ok("end_turn");
                    }
                    return Err(rpc_error(
                        -32603,
                        format!("unexpected termination {}", result.termination.as_str()),
                    ));
                }
            }
        }
    }

    pub(crate) async fn arm_cancel(&self, session_id: &str) {
        let mut sessions = self.sessions.lock().await;
        let Some(live) = sessions.get_mut(session_id) else {
            return;
        };
        if live.cancel.is_none() {
            let (tx, _) = watch::channel(false);
            live.cancel = Some(tx);
        }
    }

    async fn signal_cancel(&self, session_id: &str) {
        if let Some(live) = self.sessions.lock().await.get(session_id)
            && let Some(tx) = live.cancel.as_ref()
        {
            tx.send_replace(true);
        }
    }

    async fn clear_cancel(&self, session_id: &str) {
        if let Some(live) = self.sessions.lock().await.get_mut(session_id) {
            live.cancel = None;
        }
    }

    async fn cancel_session(&self, params: &Value) {
        let Some(session_id) = params.get("sessionId").and_then(|v| v.as_str()) else {
            return;
        };
        self.signal_cancel(session_id).await;
    }

    async fn cancel_all(&self) {
        let sessions = self.sessions.lock().await;
        for live in sessions.values() {
            if let Some(tx) = live.cancel.as_ref() {
                tx.send_replace(true);
            }
        }
    }

    fn require_init(&self) -> Result<(), Value> {
        if self.initialized.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err(rpc_error(-32600, "initialize required"))
        }
    }

    fn harness_for_cwd(&self, cwd: &Path) -> Result<Harness, String> {
        let mut config = self.harness.config.clone();
        config.workspace.adapter = "inplace".into();
        config.workspace.root = cwd.display().to_string();
        Harness::from_config(config, self.harness.state.clone()).map_err(|e| e.to_string())
    }

    fn sessions_dir(&self) -> PathBuf {
        self.harness.state.path().join("acp-sessions")
    }

    fn persist(&self, session_id: &str, live: &LiveSession) -> Result<(), String> {
        let dir = self.sessions_dir();
        std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
        let path = dir.join(format!("{session_id}.json"));
        let body = PersistedSession {
            session_id: session_id.to_string(),
            cwd: live.cwd.display().to_string(),
            run_id: live.run_id.clone(),
        };
        std::fs::write(
            path,
            serde_json::to_vec_pretty(&body).map_err(|e| e.to_string())?,
        )
        .map_err(|e| e.to_string())
    }

    fn load_persisted(&self, session_id: &str) -> Option<PersistedSession> {
        let path = self.sessions_dir().join(format!("{session_id}.json"));
        let bytes = std::fs::read(path).ok()?;
        serde_json::from_slice(&bytes).ok()
    }

    /// Most recently persisted session for `cwd`, if any. Used by the TUI
    /// continue-last-in-cwd path (`session/load`, fail closed → `session/new`).
    pub(crate) fn last_session_id_for_cwd(&self, cwd: &Path) -> Option<String> {
        let dir = self.sessions_dir();
        let mut best: Option<(std::time::SystemTime, String)> = None;
        for entry in std::fs::read_dir(dir).ok()? {
            let Ok(entry) = entry else {
                continue;
            };
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
                continue;
            }
            let Ok(bytes) = std::fs::read(&path) else {
                continue;
            };
            let Ok(persisted) = serde_json::from_slice::<PersistedSession>(&bytes) else {
                continue;
            };
            if !same_cwd(Path::new(&persisted.cwd), cwd) {
                continue;
            }
            let Ok(mtime) = entry.metadata().and_then(|meta| meta.modified()) else {
                continue;
            };
            if best.as_ref().is_none_or(|(known, _)| mtime >= *known) {
                best = Some((mtime, persisted.session_id));
            }
        }
        best.map(|(_, id)| id)
    }

    async fn set_run_id(&self, session_id: &str, run_id: Option<String>) -> Result<(), String> {
        let mut sessions = self.sessions.lock().await;
        let live = sessions
            .get_mut(session_id)
            .ok_or_else(|| "unknown session".to_string())?;
        live.run_id = run_id;
        self.persist(session_id, live)
    }

    async fn retain_cancelled_run(
        &self,
        session_id: &str,
        harness: &Harness,
        run_id: Option<String>,
    ) {
        let Some(run_id) = run_id else {
            return;
        };
        if persist_prompt_wait(&harness.state.runs_dir(), &run_id).is_ok() {
            let _ = self.set_run_id(session_id, Some(run_id)).await;
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PermissionOutcome {
    Allow,
    Deny,
    Cancelled,
}

fn park_info_from_checkpoint(checkpoint: &Checkpoint) -> Result<ParkInfo, String> {
    let park = checkpoint
        .park
        .as_ref()
        .ok_or_else(|| "session run is not parked".to_string())?;
    let call = matching_parked_call(checkpoint, park)?;
    Ok(ParkInfo {
        reason: park.reason.clone(),
        question: park.question.clone(),
        tool_call_id: park.tool_call_id.clone(),
        kind: park.kind,
        approval_id: None,
        display_call_id: (!park.allow_call_id.is_empty()).then(|| park.allow_call_id.clone()),
        args_json: Some(call.args_json.clone()),
        plan_digest: park.plan_digest.clone(),
    })
}

fn matching_parked_call<'a>(
    checkpoint: &'a Checkpoint,
    park: &ParkedState,
) -> Result<&'a crate::model::ToolCall, String> {
    let assistant = checkpoint
        .messages
        .iter()
        .rev()
        .find(|message| message.role == "assistant")
        .ok_or_else(|| "parked session is missing the assistant tool call".to_string())?;
    let turn = checkpoint.completed_turns;
    assistant
        .tool_calls
        .iter()
        .enumerate()
        .find(|(index, call)| {
            let conversation_id = conversation_call_id(call, turn, *index);
            conversation_id == park.tool_call_id
                || (!park.allow_call_id.is_empty()
                    && park.allow_call_id == stable_call_id(call, turn, *index))
        })
        .map(|(_, call)| call)
        .ok_or_else(|| "parked tool call is missing from the checkpoint".to_string())
}

fn json_raw_input(args_json: Option<&str>) -> Value {
    args_json
        .and_then(|raw| serde_json::from_str(raw).ok())
        .unwrap_or_else(|| json!({}))
}

fn conversation_call_id(call: &crate::model::ToolCall, turn: u32, index: usize) -> String {
    if call.id.is_empty() {
        format!("tool-{turn}-{index}")
    } else {
        call.id.clone()
    }
}

fn stable_call_id(call: &crate::model::ToolCall, turn: u32, index: usize) -> String {
    crate::model::stable_tool_call_id(call, turn, index)
}

fn close_outstanding_tool_calls(checkpoint: &mut Checkpoint) {
    let Some(assistant_idx) = checkpoint
        .messages
        .iter()
        .rposition(|message| message.role == "assistant")
    else {
        return;
    };
    let turn = checkpoint.completed_turns;
    let batch = checkpoint.messages[assistant_idx].tool_calls.clone();
    let answered: std::collections::HashSet<String> = checkpoint.messages[assistant_idx + 1..]
        .iter()
        .filter(|message| message.role == "tool")
        .map(|message| message.tool_call_id.clone())
        .collect();
    for (index, call) in batch.iter().enumerate() {
        let conversation_id = conversation_call_id(call, turn, index);
        if answered.contains(&conversation_id)
            || (!call.id.is_empty() && answered.contains(&call.id))
        {
            continue;
        }
        checkpoint.messages.push(ChatMessage {
            role: "tool".into(),
            content: "cancelled".into(),
            tool_call_id: conversation_id,
            tool_calls: vec![],
        });
    }
}

fn persist_prompt_wait(runs_dir: &Path, run_id: &str) -> Result<(), String> {
    let mut checkpoint = Checkpoint::load(runs_dir, run_id).map_err(|e| e.to_string())?;
    close_outstanding_tool_calls(&mut checkpoint);
    checkpoint.park = Some(ParkedState {
        reason: "end_turn".into(),
        question: String::new(),
        tool_call_id: String::new(),
        kind: ParkKind::PromptWait,
        allow_call_id: String::new(),
        plan_digest: String::new(),
    });
    checkpoint.save(runs_dir).map_err(|e| e.to_string())?;
    Ok(())
}

fn cancel_requested(cancel: Option<&watch::Receiver<bool>>) -> bool {
    cancel.is_some_and(|rx| *rx.borrow())
}

async fn request_permission(
    session_id: &str,
    park: &crate::run::ParkInfo,
    client: &dyn AcpClient,
    cancel: Option<&watch::Receiver<bool>>,
) -> Result<(PermissionOutcome, String), Value> {
    let tool_call_id = park
        .display_call_id
        .as_deref()
        .unwrap_or(park.tool_call_id.as_str());
    let title = if park.question.is_empty() {
        park.reason.as_str()
    } else {
        park.question.as_str()
    };
    let params = json!({
        "sessionId": session_id,
        "toolCall": {
            "toolCallId": tool_call_id,
            "title": title,
            "kind": "other",
            "status": "pending",
            "rawInput": json_raw_input(park.args_json.as_deref())
        },
        "options": if park.kind == ParkKind::Plan {
            json!([
                {"optionId": "allow", "name": "Accept plan", "kind": "allow_once"},
                {"optionId": "deny", "name": "Reject plan", "kind": "reject_once"}
            ])
        } else {
            json!([
                {"optionId": "allow", "name": "Allow", "kind": "allow_once"},
                {"optionId": "deny", "name": "Deny", "kind": "reject_once"}
            ])
        }
    });
    if cancel_requested(cancel) {
        return Ok((PermissionOutcome::Cancelled, String::new()));
    }
    let request = client.request("session/request_permission", params);
    tokio::pin!(request);
    let result = if let Some(rx) = cancel {
        let mut rx = rx.clone();
        loop {
            tokio::select! {
                result = &mut request => break result,
                changed = rx.changed() => {
                    if changed.is_err() || *rx.borrow() {
                        return Ok((PermissionOutcome::Cancelled, String::new()));
                    }
                }
            }
        }
    } else {
        request.await
    }
    .map_err(|e| rpc_error(-32603, e))?;
    let outcome = parse_permission(&result)?;
    Ok((outcome, permission_answer(&result, outcome)))
}

fn permission_answer(result: &Value, outcome: PermissionOutcome) -> String {
    if let Some(text) = result
        .pointer("/outcome/answer")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|text| !text.is_empty())
    {
        return text.to_string();
    }
    match outcome {
        PermissionOutcome::Allow => "approved".into(),
        PermissionOutcome::Deny => "denied".into(),
        PermissionOutcome::Cancelled => String::new(),
    }
}

fn parse_permission(result: &Value) -> Result<PermissionOutcome, Value> {
    let outcome = result
        .pointer("/outcome/outcome")
        .or_else(|| result.get("outcome"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let option = result
        .pointer("/outcome/optionId")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    match (outcome, option) {
        ("cancelled", _) => Ok(PermissionOutcome::Cancelled),
        ("selected" | "allow" | "allow_once", "allow") => Ok(PermissionOutcome::Allow),
        ("deny" | "rejected" | "reject_once", _) | ("selected", "deny") | (_, "deny") => {
            Ok(PermissionOutcome::Deny)
        }
        _ => Err(rpc_error(
            -32602,
            "session/request_permission requires an explicit allow or deny option",
        )),
    }
}

async fn persist_session_run(host: &AcpHost, session_id: &str, run_id: &str) {
    let _ = host.set_run_id(session_id, Some(run_id.to_string())).await;
}

async fn run_and_forward(
    host: &AcpHost,
    harness: &Harness,
    request: RunRequest,
    sink: Arc<dyn EventSink>,
    events: &Arc<std::sync::Mutex<std::sync::mpsc::Receiver<HarnessEvent>>>,
    session_id: &str,
    client: &dyn AcpClient,
) -> (Result<crate::run::RunResult, HarnessError>, Option<String>) {
    let known_run_id = request.resume_run_id.clone();
    let mut seen_run_id = known_run_id.clone();
    if let Some(id) = known_run_id.as_deref() {
        persist_session_run(host, session_id, id).await;
    }
    let mut held = Vec::new();
    let run_fut = harness.run_with_events(request, Some(sink));
    tokio::pin!(run_fut);
    loop {
        tokio::select! {
            result = &mut run_fut => {
                let (mut updates, event_run_id) = drain_updates(session_id, events);
                held.append(&mut updates);
                if let Some(id) = event_run_id.clone() {
                    seen_run_id = Some(id.clone());
                    persist_session_run(host, session_id, &id).await;
                }
                let run_id = result
                    .as_ref()
                    .ok()
                    .map(|run| run.run_id.clone())
                    .or(event_run_id)
                    .or(seen_run_id);
                if let Some(run_id) = run_id.as_deref() {
                    persist_session_run(host, session_id, run_id).await;
                    fill_assistant_content(&mut held, harness, run_id);
                }
                let send = send_updates(client, held).await.map_err(update_error);
                if let Err(error) = send {
                    return (Err(error), run_id);
                }
                return (result, run_id);
            }
            _ = tokio::time::sleep(std::time::Duration::from_millis(20)) => {
                let (mut updates, event_run_id) = drain_updates(session_id, events);
                held.append(&mut updates);
                if let Some(id) = event_run_id.clone() {
                    seen_run_id = Some(id.clone());
                    persist_session_run(host, session_id, &id).await;
                }
                let run_id = event_run_id.as_deref().or(known_run_id.as_deref());
                if let Some(run_id) = run_id {
                    fill_assistant_content(&mut held, harness, run_id);
                    if let Err(error) = send_updates(client, std::mem::take(&mut held))
                        .await
                        .map_err(update_error)
                    {
                        host.signal_cancel(session_id).await;
                        let abandoned = (&mut run_fut).await;
                        let run_id = abandoned
                            .as_ref()
                            .ok()
                            .map(|run| run.run_id.clone())
                            .or(seen_run_id);
                        if let Some(id) = run_id.as_deref() {
                            let _ = persist_prompt_wait(&harness.state.runs_dir(), id);
                            persist_session_run(host, session_id, id).await;
                        }
                        return (Err(error), run_id);
                    }
                }
            }
        }
    }
}

fn update_error(error: Value) -> HarnessError {
    HarnessError::Run(RunError::Message(
        error
            .get("message")
            .and_then(|message| message.as_str())
            .unwrap_or("update failed")
            .into(),
    ))
}

fn fill_assistant_content(updates: &mut [Value], harness: &Harness, run_id: &str) {
    let Ok(checkpoint) = Checkpoint::load(&harness.state.runs_dir(), run_id) else {
        return;
    };
    let chunk_count = updates
        .iter()
        .filter(|update| update["update"]["sessionUpdate"] == "agent_message_chunk")
        .count();
    if chunk_count == 0 {
        return;
    }
    let mut assistants: Vec<&str> = checkpoint
        .messages
        .iter()
        .rev()
        .filter(|message| message.role == "assistant")
        .map(|message| message.content.as_str())
        .take(chunk_count)
        .collect();
    assistants.reverse();
    let mut index = 0usize;
    for update in updates.iter_mut() {
        if update["update"]["sessionUpdate"] == "agent_message_chunk" {
            if let Some(text) = assistants.get(index) {
                update["update"]["content"]["text"] = json!(*text);
            }
            index += 1;
        }
    }
}

fn conversation_updates(session_id: &str, messages: &[ChatMessage]) -> Vec<Value> {
    let mut updates = Vec::new();
    let mut turn = 0u32;
    let mut call_ids = HashMap::new();
    for message in messages {
        match message.role.as_str() {
            "user" if !message.content.is_empty() => {
                updates.push(json!({
                    "sessionId": session_id,
                    "update": {
                        "sessionUpdate": "user_message_chunk",
                        "content": { "type": "text", "text": message.content }
                    }
                }));
            }
            "assistant" => {
                turn = turn.saturating_add(1);
                if !message.content.is_empty() {
                    updates.push(json!({
                        "sessionId": session_id,
                        "update": {
                            "sessionUpdate": "agent_message_chunk",
                            "content": { "type": "text", "text": message.content },
                            "messageId": format!("turn-{turn}")
                        }
                    }));
                }
                for (index, call) in message.tool_calls.iter().enumerate() {
                    let stable = stable_call_id(call, turn, index);
                    let conversation = conversation_call_id(call, turn, index);
                    call_ids.insert(conversation, stable.clone());
                    if !call.id.is_empty() {
                        call_ids.insert(call.id.clone(), stable.clone());
                    }
                    updates.push(json!({
                        "sessionId": session_id,
                        "update": {
                            "sessionUpdate": "tool_call",
                            "toolCallId": stable,
                            "title": call.name,
                            "kind": "other",
                            "status": "pending",
                            "rawInput": json_raw_input(Some(&call.args_json))
                        }
                    }));
                }
            }
            "tool" => {
                let call_id = call_ids
                    .get(&message.tool_call_id)
                    .cloned()
                    .unwrap_or_else(|| message.tool_call_id.clone());
                let failed =
                    message.content == "permission denied" || message.content == "cancelled";
                updates.push(json!({
                    "sessionId": session_id,
                    "update": {
                        "sessionUpdate": "tool_call_update",
                        "toolCallId": call_id,
                        "status": if failed { "failed" } else { "completed" },
                        "content": [{
                            "type": "content",
                            "content": { "type": "text", "text": message.content }
                        }]
                    }
                }));
            }
            _ => {}
        }
    }
    updates
}

fn drain_updates(
    session_id: &str,
    events: &Arc<std::sync::Mutex<std::sync::mpsc::Receiver<HarnessEvent>>>,
) -> (Vec<Value>, Option<String>) {
    let mut updates = Vec::new();
    let mut run_id = None;
    let events = events.lock().unwrap_or_else(|e| e.into_inner());
    while let Ok(event) = events.try_recv() {
        match &event {
            HarnessEvent::ToolStart { run_id: id, .. }
            | HarnessEvent::ToolEnd { run_id: id, .. }
            | HarnessEvent::RunFinished { run_id: id, .. }
                if !id.is_empty() =>
            {
                run_id = Some(id.clone());
            }
            _ => {}
        }
        let update = match event {
            HarnessEvent::ModelTurn {
                turn,
                content_preview,
            } => json!({
                "sessionId": session_id,
                "update": {
                    "sessionUpdate": "agent_message_chunk",
                    "content": { "type": "text", "text": content_preview },
                    "messageId": format!("turn-{turn}")
                }
            }),
            HarnessEvent::ToolStart {
                name,
                call_id,
                args_json,
                ..
            } => json!({
                "sessionId": session_id,
                "update": {
                    "sessionUpdate": "tool_call",
                    "toolCallId": call_id,
                    "title": name,
                    "kind": "other",
                    "status": "pending",
                    "rawInput": json_raw_input(Some(&args_json))
                }
            }),
            HarnessEvent::ToolEnd {
                name: _,
                ok,
                detail,
                call_id,
                ..
            } => json!({
                "sessionId": session_id,
                "update": {
                    "sessionUpdate": "tool_call_update",
                    "toolCallId": call_id,
                    "status": if ok { "completed" } else { "failed" },
                    "content": [{ "type": "content", "content": { "type": "text", "text": detail } }]
                }
            }),
            _ => continue,
        };
        updates.push(update);
    }
    (updates, run_id)
}

async fn send_updates(client: &dyn AcpClient, updates: Vec<Value>) -> Result<(), Value> {
    for update in updates {
        client
            .notify("session/update", update)
            .await
            .map_err(|e| rpc_error(-32603, e))?;
    }
    Ok(())
}

fn prompt_text(prompt: &Value) -> Result<String, Value> {
    let Some(parts) = prompt.as_array() else {
        return Err(rpc_error(-32602, "session/prompt requires prompt parts"));
    };
    let mut text = String::new();
    for part in parts {
        if part.get("type").and_then(|t| t.as_str()) == Some("text")
            && let Some(chunk) = part.get("text").and_then(|t| t.as_str())
        {
            text.push_str(chunk);
        }
    }
    if text.is_empty() {
        return Err(rpc_error(-32602, "session/prompt requires a text part"));
    }
    Ok(text)
}

fn rpc_error(code: i64, message: impl Into<String>) -> Value {
    json!({ "code": code, "message": message.into() })
}

fn same_cwd(left: &Path, right: &Path) -> bool {
    if left == right {
        return true;
    }
    match (left.canonicalize(), right.canonicalize()) {
        (Ok(a), Ok(b)) => a == b,
        _ => {
            let a = left.to_string_lossy();
            let b = right.to_string_lossy();
            a.trim_end_matches('/') == b.trim_end_matches('/')
        }
    }
}

/// Serve ACP on process stdio until EOF. Newline-delimited JSON-RPC.
pub async fn run_stdio(harness: Harness) -> Result<(), String> {
    let host = Arc::new(AcpHost::new(harness));
    let writer = Arc::new(Mutex::new(stdout()));
    let pending: Arc<Mutex<PendingPermissions>> = Arc::new(Mutex::new(HashMap::new()));
    let next_id = Arc::new(AtomicU64::new(1));
    let (inbound_tx, inbound_rx) = mpsc::channel::<Value>(32);

    let reader_pending = Arc::clone(&pending);
    let reader_inbound = inbound_tx;
    tokio::spawn(async move {
        let mut reader = BufReader::new(stdin());
        loop {
            let msg = match framing::read(&mut reader).await {
                Ok(m) => m,
                Err(e) if e == "eof" => break,
                Err(_) => break,
            };
            let is_response = msg.get("id").is_some()
                && (msg.get("result").is_some() || msg.get("error").is_some())
                && msg.get("method").is_none();
            if is_response {
                if let Some(id) = msg.get("id").and_then(|v| v.as_u64())
                    && let Some((_, tx)) = reader_pending.lock().await.remove(&id)
                {
                    let _ = tx.send(msg);
                }
            } else {
                let _ = reader_inbound.send(msg).await;
            }
        }
    });

    let client = StdioClient {
        writer: Arc::clone(&writer),
        pending,
        next_id,
    };

    serve_inbound(host, client, inbound_rx, writer).await
}

async fn serve_inbound(
    host: Arc<AcpHost>,
    client: StdioClient,
    mut inbound_rx: mpsc::Receiver<Value>,
    writer: Arc<Mutex<tokio::io::Stdout>>,
) -> Result<(), String> {
    let mut prompt: Option<tokio::task::JoinHandle<Option<Value>>> = None;
    loop {
        tokio::select! {
            maybe_msg = inbound_rx.recv() => {
                let Some(msg) = maybe_msg else {
                    host.cancel_all().await;
                    client.cancel_all_pending().await;
                    if let Some(task) = prompt.take() {
                        let _ = task.await;
                    }
                    break;
                };
                let method = msg.get("method").and_then(|m| m.as_str()).unwrap_or("");
                if method == "session/cancel" {
                    let sid = msg
                        .pointer("/params/sessionId")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    host.handle(msg, &client).await;
                    client.cancel_pending(&sid).await;
                    continue;
                }
                if method == "session/prompt" {
                    if prompt.is_some() {
                        if let Some(id) = msg.get("id").cloned() {
                            let resp = json!({
                                "jsonrpc": "2.0",
                                "id": id,
                                "error": rpc_error(-32000, "prompt already in flight")
                            });
                            let mut out = writer.lock().await;
                            framing::write_line(&mut *out, &resp).await?;
                        }
                        continue;
                    }
                    if let Some(sid) = msg
                        .pointer("/params/sessionId")
                        .and_then(|v| v.as_str())
                    {
                        host.arm_cancel(sid).await;
                    }
                    let host = Arc::clone(&host);
                    let client = client.clone();
                    prompt = Some(tokio::spawn(async move { host.handle(msg, &client).await }));
                    continue;
                }
                if let Some(resp) = host.handle(msg, &client).await {
                    let mut out = writer.lock().await;
                    framing::write_line(&mut *out, &resp).await?;
                }
            }
            join = async {
                prompt.as_mut().expect("prompt task").await
            }, if prompt.is_some() => {
                prompt = None;
                if let Ok(Some(resp)) = join {
                    let mut out = writer.lock().await;
                    framing::write_line(&mut *out, &resp).await?;
                }
            }
        }
    }
    Ok(())
}

#[derive(Clone)]
struct StdioClient {
    writer: Arc<Mutex<tokio::io::Stdout>>,
    pending: Arc<Mutex<PendingPermissions>>,
    next_id: Arc<AtomicU64>,
}

impl StdioClient {
    async fn cancel_pending(&self, session_id: &str) {
        if session_id.is_empty() {
            return;
        }
        let mut pending = self.pending.lock().await;
        let ids: Vec<u64> = pending
            .iter()
            .filter(|(_, (sid, _))| sid == session_id)
            .map(|(id, _)| *id)
            .collect();
        for id in ids {
            if let Some((_, tx)) = pending.remove(&id) {
                let _ = tx.send(json!({
                    "jsonrpc": "2.0",
                    "result": { "outcome": { "outcome": "cancelled" } }
                }));
            }
        }
    }

    async fn cancel_all_pending(&self) {
        let mut pending = self.pending.lock().await;
        for (_, (_, tx)) in pending.drain() {
            let _ = tx.send(json!({
                "jsonrpc": "2.0",
                "result": { "outcome": { "outcome": "cancelled" } }
            }));
        }
    }
}

#[async_trait]
impl AcpClient for StdioClient {
    async fn notify(&self, method: &str, params: Value) -> Result<(), String> {
        let msg = json!({"jsonrpc": "2.0", "method": method, "params": params});
        let mut out = self.writer.lock().await;
        framing::write_line(&mut *out, &msg).await
    }

    async fn request(&self, method: &str, params: Value) -> Result<Value, String> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = oneshot::channel();
        let session_id = params
            .get("sessionId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        self.pending.lock().await.insert(id, (session_id, tx));
        let msg = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
        {
            let mut out = self.writer.lock().await;
            framing::write_line(&mut *out, &msg).await?;
        }
        let resp = rx
            .await
            .map_err(|_| "permission response dropped".to_string())?;
        if let Some(err) = resp.get("error") {
            return Err(err.to_string());
        }
        Ok(resp.get("result").cloned().unwrap_or(Value::Null))
    }
}

#[cfg(test)]
struct RecordingClient {
    updates: Mutex<Vec<Value>>,
    permission: PermissionOutcome,
}

#[cfg(test)]
#[async_trait]
impl AcpClient for RecordingClient {
    async fn notify(&self, _method: &str, params: Value) -> Result<(), String> {
        self.updates.lock().await.push(params);
        Ok(())
    }

    async fn request(&self, method: &str, _params: Value) -> Result<Value, String> {
        if method != "session/request_permission" {
            return Err(format!("unexpected agent request {method}"));
        }
        let outcome = match self.permission {
            PermissionOutcome::Allow => "allow",
            PermissionOutcome::Deny => "deny",
            PermissionOutcome::Cancelled => "cancelled",
        };
        Ok(json!({ "outcome": { "outcome": outcome, "optionId": outcome } }))
    }
}

#[cfg(test)]
struct FailAfterFirstUpdate {
    sent: AtomicU64,
}

#[cfg(test)]
#[async_trait]
impl AcpClient for FailAfterFirstUpdate {
    async fn notify(&self, _method: &str, _params: Value) -> Result<(), String> {
        if self.sent.fetch_add(1, Ordering::SeqCst) >= 1 {
            return Err("broken pipe".into());
        }
        Ok(())
    }

    async fn request(&self, method: &str, _params: Value) -> Result<Value, String> {
        if method != "session/request_permission" {
            return Err(format!("unexpected agent request {method}"));
        }
        Ok(json!({ "outcome": { "outcome": "allow", "optionId": "allow" } }))
    }
}

#[cfg(test)]
struct CancelOnAskClient {
    host: Arc<AcpHost>,
    session_id: String,
    updates: Mutex<Vec<Value>>,
}

#[cfg(test)]
#[async_trait]
impl AcpClient for CancelOnAskClient {
    async fn notify(&self, _method: &str, params: Value) -> Result<(), String> {
        self.updates.lock().await.push(params);
        Ok(())
    }

    async fn request(&self, method: &str, _params: Value) -> Result<Value, String> {
        if method != "session/request_permission" {
            return Err(format!("unexpected agent request {method}"));
        }
        self.host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "method": "session/cancel",
                    "params": { "sessionId": self.session_id }
                }),
                self,
            )
            .await;
        Ok(json!({ "outcome": { "outcome": "cancelled" } }))
    }
}

#[cfg(test)]
fn rpc_ok(resp: &Value) -> &Value {
    resp.get("result").unwrap_or(resp)
}

#[cfg(test)]
struct MalformedPermissionClient {
    updates: Mutex<Vec<Value>>,
}

#[cfg(test)]
#[async_trait]
impl AcpClient for MalformedPermissionClient {
    async fn notify(&self, _method: &str, params: Value) -> Result<(), String> {
        self.updates.lock().await.push(params);
        Ok(())
    }

    async fn request(&self, method: &str, _params: Value) -> Result<Value, String> {
        if method != "session/request_permission" {
            return Err(format!("unexpected agent request {method}"));
        }
        Ok(json!({}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::state::StateRoot;
    use tempfile::tempdir;

    fn scripted_host(dir: &Path, script: &str) -> AcpHost {
        let state = StateRoot::new(dir.join("state"));
        let mut config = Config::default();
        config.governance.adapter = "local".into();
        config.model.adapter = "scripted".into();
        config.model.script_json = Some(script.into());
        config.events.adapter = "none".into();
        config.workspace.root = dir.join("ws").to_string_lossy().into();
        let harness = Harness::from_config(config, state).unwrap();
        AcpHost::new(harness)
    }

    async fn init_and_new(host: &AcpHost, client: &dyn AcpClient, cwd: &Path) -> String {
        let init = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "initialize",
                    "params": { "protocolVersion": 1, "capabilities": {} }
                }),
                client,
            )
            .await
            .unwrap();
        assert_eq!(rpc_ok(&init)["protocolVersion"], 1);
        assert_eq!(rpc_ok(&init)["agentCapabilities"]["loadSession"], true);
        let created = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "session/new",
                    "params": { "cwd": cwd, "mcpServers": [] }
                }),
                client,
            )
            .await
            .unwrap();
        rpc_ok(&created)["sessionId"].as_str().unwrap().to_string()
    }

    #[tokio::test]
    async fn initialize_session_prompt_end_turn_and_follow_up() {
        let dir = tempdir().unwrap();
        let host = scripted_host(dir.path(), r#"[{"content":"hello"},{"content":"again"}]"#);
        let client = RecordingClient {
            updates: Mutex::new(Vec::new()),
            permission: PermissionOutcome::Allow,
        };
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let session_id = init_and_new(&host, &client, &cwd).await;

        let unknown = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 9,
                    "method": "session/load",
                    "params": { "sessionId": "sess_missing" }
                }),
                &client,
            )
            .await
            .unwrap();
        assert_eq!(unknown["error"]["code"], -32002);

        let loaded = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 10,
                    "method": "session/load",
                    "params": { "sessionId": session_id }
                }),
                &client,
            )
            .await
            .unwrap();
        assert_eq!(rpc_ok(&loaded)["sessionId"], session_id);

        let prompt = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "session/prompt",
                    "params": {
                        "sessionId": session_id,
                        "prompt": [{"type":"text","text":"hi"}]
                    }
                }),
                &client,
            )
            .await
            .unwrap();
        assert_eq!(rpc_ok(&prompt)["stopReason"], "end_turn");
        let updates = client.updates.lock().await;
        let chunks: Vec<_> = updates
            .iter()
            .filter(|u| u["update"]["sessionUpdate"] == "agent_message_chunk")
            .collect();
        assert_eq!(chunks.len(), 1, "{updates:?}");
        drop(updates);

        let follow = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 4,
                    "method": "session/prompt",
                    "params": {
                        "sessionId": session_id,
                        "prompt": [{"type":"text","text":"more"}]
                    }
                }),
                &client,
            )
            .await
            .unwrap();
        assert_eq!(rpc_ok(&follow)["stopReason"], "end_turn");
        let updates = client.updates.lock().await;
        let chunks: Vec<_> = updates
            .iter()
            .filter(|u| u["update"]["sessionUpdate"] == "agent_message_chunk")
            .collect();
        assert_eq!(chunks.len(), 2, "{updates:?}");
    }

    #[tokio::test]
    async fn session_load_replays_conversation() {
        let dir = tempdir().unwrap();
        let host = scripted_host(
            dir.path(),
            r#"[
              {"tool_calls":[{"name":"write_file","args_json":"{\"path\":\"ok.txt\",\"content\":\"hi\\n\"}"}]},
              {"content":"wrote it"}
            ]"#,
        );
        let client = RecordingClient {
            updates: Mutex::new(Vec::new()),
            permission: PermissionOutcome::Allow,
        };
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let session_id = init_and_new(&host, &client, &cwd).await;
        let prompt = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "session/prompt",
                    "params": {
                        "sessionId": session_id,
                        "prompt": [{"type":"text","text":"write"}]
                    }
                }),
                &client,
            )
            .await
            .unwrap();
        assert_eq!(rpc_ok(&prompt)["stopReason"], "end_turn");

        let replay = RecordingClient {
            updates: Mutex::new(Vec::new()),
            permission: PermissionOutcome::Allow,
        };
        let loaded = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 10,
                    "method": "session/load",
                    "params": { "sessionId": session_id, "cwd": cwd, "mcpServers": [] }
                }),
                &replay,
            )
            .await
            .unwrap();
        assert_eq!(rpc_ok(&loaded)["sessionId"], session_id);
        let updates = replay.updates.lock().await;
        let kinds: Vec<&str> = updates
            .iter()
            .map(|update| update["update"]["sessionUpdate"].as_str().unwrap_or(""))
            .collect();
        assert!(
            kinds.contains(&"user_message_chunk"),
            "load must replay user text: {updates:?}"
        );
        assert!(
            kinds.contains(&"tool_call"),
            "load must replay tool calls: {updates:?}"
        );
        assert!(
            kinds.contains(&"tool_call_update"),
            "load must replay tool results: {updates:?}"
        );
        assert!(
            kinds.contains(&"agent_message_chunk"),
            "load must replay assistant text: {updates:?}"
        );
        assert_eq!(
            updates
                .iter()
                .find(|update| update["update"]["sessionUpdate"] == "user_message_chunk")
                .and_then(|update| update["update"]["content"]["text"].as_str()),
            Some("write")
        );
        assert_eq!(
            updates
                .iter()
                .find(|update| update["update"]["sessionUpdate"] == "agent_message_chunk")
                .and_then(|update| update["update"]["content"]["text"].as_str()),
            Some("wrote it")
        );
    }

    #[tokio::test]
    async fn ask_park_allow_resumes_mutating_tool() {
        let dir = tempdir().unwrap();
        let host = scripted_host(
            dir.path(),
            r#"[
              {"tool_calls":[{"name":"write_file","args_json":"{\"path\":\"ok.txt\",\"content\":\"hi\\n\"}"}]},
              {"content":"wrote it"}
            ]"#,
        );
        let client = RecordingClient {
            updates: Mutex::new(Vec::new()),
            permission: PermissionOutcome::Allow,
        };
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let session_id = init_and_new(&host, &client, &cwd).await;
        let prompt = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "session/prompt",
                    "params": {
                        "sessionId": session_id,
                        "prompt": [{"type":"text","text":"write"}]
                    }
                }),
                &client,
            )
            .await
            .unwrap();
        assert_eq!(rpc_ok(&prompt)["stopReason"], "end_turn");
        let written = cwd.join("ok.txt");
        assert_eq!(std::fs::read_to_string(written).unwrap(), "hi\n");
        let updates = client.updates.lock().await;
        let raw = updates
            .iter()
            .find(|update| update["update"]["sessionUpdate"] == "tool_call")
            .map(|update| &update["update"]["rawInput"]);
        assert_eq!(
            raw.and_then(|value| value.get("path")),
            Some(&json!("ok.txt"))
        );
    }

    #[tokio::test]
    async fn session_prompt_fails_closed_when_run_is_not_waiting() {
        let dir = tempdir().unwrap();
        let host = scripted_host(dir.path(), r#"[{"content":"hello"}]"#);
        let client = RecordingClient {
            updates: Mutex::new(Vec::new()),
            permission: PermissionOutcome::Allow,
        };
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let session_id = init_and_new(&host, &client, &cwd).await;
        let prompt = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "session/prompt",
                    "params": {
                        "sessionId": session_id,
                        "prompt": [{"type":"text","text":"go"}]
                    }
                }),
                &client,
            )
            .await
            .unwrap();
        assert_eq!(rpc_ok(&prompt)["stopReason"], "end_turn");
        let runs = dir.path().join("state").join("runs");
        let run_id = std::fs::read_dir(&runs)
            .unwrap()
            .flatten()
            .find(|entry| entry.path().join("checkpoint.json").exists())
            .unwrap()
            .file_name();
        let run_id = run_id.to_string_lossy().into_owned();
        let mut checkpoint = Checkpoint::load(&runs, &run_id).unwrap();
        checkpoint.park = None;
        checkpoint.save(&runs).unwrap();
        let follow = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 4,
                    "method": "session/prompt",
                    "params": {
                        "sessionId": session_id,
                        "prompt": [{"type":"text","text":"more"}]
                    }
                }),
                &client,
            )
            .await
            .unwrap();
        assert_eq!(follow["error"]["code"], -32603);
        assert!(
            follow["error"]["message"]
                .as_str()
                .unwrap_or("")
                .contains("not waiting"),
            "{follow:?}"
        );
    }

    #[tokio::test]
    async fn ask_park_deny_does_not_write() {
        let dir = tempdir().unwrap();
        let host = scripted_host(
            dir.path(),
            r#"[
              {"tool_calls":[{"name":"write_file","args_json":"{\"path\":\"ok.txt\",\"content\":\"hi\\n\"}"}]},
              {"content":"denied"}
            ]"#,
        );
        let client = RecordingClient {
            updates: Mutex::new(Vec::new()),
            permission: PermissionOutcome::Deny,
        };
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let session_id = init_and_new(&host, &client, &cwd).await;
        let prompt = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "session/prompt",
                    "params": {
                        "sessionId": session_id,
                        "prompt": [{"type":"text","text":"write"}]
                    }
                }),
                &client,
            )
            .await
            .unwrap();
        assert_eq!(rpc_ok(&prompt)["stopReason"], "end_turn");
        assert!(!cwd.join("ok.txt").exists());
        let updates = client.updates.lock().await;
        assert!(
            updates.iter().any(|update| {
                update["update"]["sessionUpdate"] == "tool_call_update"
                    && update["update"]["status"] == "failed"
            }),
            "denied ask must emit a terminal tool_call_update: {updates:?}"
        );
    }

    #[tokio::test]
    async fn session_cancel_honors_cancel_marker() {
        let dir = tempdir().unwrap();
        let host = Arc::new(scripted_host(
            dir.path(),
            r#"[
              {"tool_calls":[{"name":"write_file","args_json":"{\"path\":\"ok.txt\",\"content\":\"hi\\n\"}"}]},
              {"content":"after-cancel"}
            ]"#,
        ));
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let setup = RecordingClient {
            updates: Mutex::new(Vec::new()),
            permission: PermissionOutcome::Allow,
        };
        let session_id = init_and_new(&host, &setup, &cwd).await;
        let client = CancelOnAskClient {
            host: Arc::clone(&host),
            session_id: session_id.clone(),
            updates: Mutex::new(Vec::new()),
        };
        let prompt = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "session/prompt",
                    "params": {
                        "sessionId": session_id,
                        "prompt": [{"type":"text","text":"sleep"}]
                    }
                }),
                &client,
            )
            .await
            .unwrap();
        assert_eq!(rpc_ok(&prompt)["stopReason"], "cancelled");
        let follow = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 4,
                    "method": "session/prompt",
                    "params": {
                        "sessionId": session_id,
                        "prompt": [{"type":"text","text":"more"}]
                    }
                }),
                &setup,
            )
            .await
            .unwrap();
        assert_eq!(rpc_ok(&follow)["stopReason"], "end_turn");
        assert!(!cwd.join("ok.txt").exists());
    }

    #[tokio::test]
    async fn cancel_armed_before_prompt_is_observed() {
        let dir = tempdir().unwrap();
        let host = scripted_host(
            dir.path(),
            r#"[
              {"tool_calls":[{"name":"write_file","args_json":"{\"path\":\"ok.txt\",\"content\":\"hi\\n\"}"}]},
              {"content":"should-not-run"}
            ]"#,
        );
        let client = RecordingClient {
            updates: Mutex::new(Vec::new()),
            permission: PermissionOutcome::Allow,
        };
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let session_id = init_and_new(&host, &client, &cwd).await;
        host.arm_cancel(&session_id).await;
        host.signal_cancel(&session_id).await;
        let prompt = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "session/prompt",
                    "params": {
                        "sessionId": session_id,
                        "prompt": [{"type":"text","text":"write"}]
                    }
                }),
                &client,
            )
            .await
            .unwrap();
        assert_eq!(rpc_ok(&prompt)["stopReason"], "cancelled");
        assert!(!cwd.join("ok.txt").exists());
    }

    #[tokio::test]
    async fn update_failure_finishes_the_run() {
        let dir = tempdir().unwrap();
        let host = scripted_host(
            dir.path(),
            r#"[
              {"tool_calls":[{"name":"write_file","args_json":"{\"path\":\"ok.txt\",\"content\":\"hi\\n\"}"}]},
              {"content":"wrote it"}
            ]"#,
        );
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let setup = RecordingClient {
            updates: Mutex::new(Vec::new()),
            permission: PermissionOutcome::Allow,
        };
        let session_id = init_and_new(&host, &setup, &cwd).await;
        let failed = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "session/prompt",
                    "params": {
                        "sessionId": session_id,
                        "prompt": [{"type":"text","text":"write"}]
                    }
                }),
                &FailAfterFirstUpdate {
                    sent: AtomicU64::new(0),
                },
            )
            .await
            .unwrap();
        assert!(failed.get("error").is_some(), "{failed}");
        let follow = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 4,
                    "method": "session/prompt",
                    "params": {
                        "sessionId": session_id,
                        "prompt": [{"type":"text","text":"again"}]
                    }
                }),
                &setup,
            )
            .await
            .unwrap();
        assert_eq!(rpc_ok(&follow)["stopReason"], "end_turn");
    }

    #[tokio::test]
    async fn assistant_update_uses_full_content() {
        let dir = tempdir().unwrap();
        let long = "abcdefghij".repeat(25);
        let script = format!(r#"[{{"content":"{long}"}}]"#);
        let host = scripted_host(dir.path(), &script);
        let client = RecordingClient {
            updates: Mutex::new(Vec::new()),
            permission: PermissionOutcome::Allow,
        };
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let session_id = init_and_new(&host, &client, &cwd).await;
        let prompt = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "session/prompt",
                    "params": {
                        "sessionId": session_id,
                        "prompt": [{"type":"text","text":"hi"}]
                    }
                }),
                &client,
            )
            .await
            .unwrap();
        assert_eq!(rpc_ok(&prompt)["stopReason"], "end_turn");
        let updates = client.updates.lock().await;
        let text = updates
            .iter()
            .find(|u| u["update"]["sessionUpdate"] == "agent_message_chunk")
            .and_then(|u| u["update"]["content"]["text"].as_str())
            .unwrap();
        assert_eq!(text, long);
        assert!(text.len() > 200);
    }

    #[tokio::test]
    async fn follow_up_after_report_keeps_the_conversation() {
        let dir = tempdir().unwrap();
        let host = scripted_host(
            dir.path(),
            r#"[
              {"tool_calls":[{"name":"report","args_json":"{\"summary\":\"done\",\"success\":true}"}]},
              {"content":"again"}
            ]"#,
        );
        let client = RecordingClient {
            updates: Mutex::new(Vec::new()),
            permission: PermissionOutcome::Allow,
        };
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let session_id = init_and_new(&host, &client, &cwd).await;
        let first = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "session/prompt",
                    "params": {
                        "sessionId": session_id,
                        "prompt": [{"type":"text","text":"go"}]
                    }
                }),
                &client,
            )
            .await
            .unwrap();
        assert_eq!(rpc_ok(&first)["stopReason"], "end_turn");
        let follow = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 4,
                    "method": "session/prompt",
                    "params": {
                        "sessionId": session_id,
                        "prompt": [{"type":"text","text":"more"}]
                    }
                }),
                &client,
            )
            .await
            .unwrap();
        assert_eq!(rpc_ok(&follow)["stopReason"], "end_turn");
        let replay = RecordingClient {
            updates: Mutex::new(Vec::new()),
            permission: PermissionOutcome::Allow,
        };
        let loaded = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 10,
                    "method": "session/load",
                    "params": { "sessionId": session_id }
                }),
                &replay,
            )
            .await
            .unwrap();
        assert_eq!(rpc_ok(&loaded)["sessionId"], session_id);
        let updates = replay.updates.lock().await;
        let users: Vec<&str> = updates
            .iter()
            .filter(|update| update["update"]["sessionUpdate"] == "user_message_chunk")
            .filter_map(|update| update["update"]["content"]["text"].as_str())
            .collect();
        assert_eq!(users, vec!["go", "more"]);
    }

    #[tokio::test]
    async fn malformed_permission_does_not_write() {
        let dir = tempdir().unwrap();
        let host = scripted_host(
            dir.path(),
            r#"[
              {"tool_calls":[{"name":"write_file","args_json":"{\"path\":\"ok.txt\",\"content\":\"hi\\n\"}"}]},
              {"content":"wrote it"}
            ]"#,
        );
        let client = MalformedPermissionClient {
            updates: Mutex::new(Vec::new()),
        };
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let session_id = init_and_new(&host, &client, &cwd).await;
        let prompt = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "session/prompt",
                    "params": {
                        "sessionId": session_id,
                        "prompt": [{"type":"text","text":"write"}]
                    }
                }),
                &client,
            )
            .await
            .unwrap();
        assert_eq!(prompt["error"]["code"], -32602);
        assert!(!cwd.join("ok.txt").exists());
        let allow = RecordingClient {
            updates: Mutex::new(Vec::new()),
            permission: PermissionOutcome::Allow,
        };
        let retry = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 4,
                    "method": "session/prompt",
                    "params": {
                        "sessionId": session_id,
                        "prompt": [{"type":"text","text":"retry"}]
                    }
                }),
                &allow,
            )
            .await
            .unwrap();
        assert_eq!(rpc_ok(&retry)["stopReason"], "end_turn");
        assert_eq!(std::fs::read_to_string(cwd.join("ok.txt")).unwrap(), "hi\n");
    }

    #[tokio::test]
    async fn restored_permission_cancel_clears_for_follow_up() {
        let dir = tempdir().unwrap();
        let host = Arc::new(scripted_host(
            dir.path(),
            r#"[
              {"tool_calls":[{"name":"write_file","args_json":"{\"path\":\"ok.txt\",\"content\":\"hi\\n\"}"}]},
              {"content":"wrote it"}
            ]"#,
        ));
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let setup = RecordingClient {
            updates: Mutex::new(Vec::new()),
            permission: PermissionOutcome::Allow,
        };
        let session_id = init_and_new(&host, &setup, &cwd).await;
        let malformed = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "session/prompt",
                    "params": {
                        "sessionId": session_id,
                        "prompt": [{"type":"text","text":"write"}]
                    }
                }),
                &MalformedPermissionClient {
                    updates: Mutex::new(Vec::new()),
                },
            )
            .await
            .unwrap();
        assert_eq!(malformed["error"]["code"], -32602);
        let cancel_client = CancelOnAskClient {
            host: Arc::clone(&host),
            session_id: session_id.clone(),
            updates: Mutex::new(Vec::new()),
        };
        let cancelled = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 4,
                    "method": "session/prompt",
                    "params": {
                        "sessionId": session_id,
                        "prompt": [{"type":"text","text":"retry"}]
                    }
                }),
                &cancel_client,
            )
            .await
            .unwrap();
        assert_eq!(rpc_ok(&cancelled)["stopReason"], "cancelled");
        let follow = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 5,
                    "method": "session/prompt",
                    "params": {
                        "sessionId": session_id,
                        "prompt": [{"type":"text","text":"continue"}]
                    }
                }),
                &setup,
            )
            .await
            .unwrap();
        assert_eq!(rpc_ok(&follow)["stopReason"], "end_turn");
        assert!(!cwd.join("ok.txt").exists());
    }

    #[tokio::test]
    async fn escalate_permission_resumes_with_answer() {
        let dir = tempdir().unwrap();
        let host = scripted_host(
            dir.path(),
            r#"[
              {"tool_calls":[{"name":"escalate","args_json":"{\"reason\":\"need-decision\",\"question\":\"continue?\"}"}]},
              {"content":"resumed"}
            ]"#,
        );
        let client = RecordingClient {
            updates: Mutex::new(Vec::new()),
            permission: PermissionOutcome::Allow,
        };
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let session_id = init_and_new(&host, &client, &cwd).await;
        let prompt = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "session/prompt",
                    "params": {
                        "sessionId": session_id,
                        "prompt": [{"type":"text","text":"decide"}]
                    }
                }),
                &client,
            )
            .await
            .unwrap();
        assert_eq!(rpc_ok(&prompt)["stopReason"], "end_turn");
        let updates = client.updates.lock().await;
        let texts: Vec<&str> = updates
            .iter()
            .filter(|u| u["update"]["sessionUpdate"] == "agent_message_chunk")
            .filter_map(|u| u["update"]["content"]["text"].as_str())
            .collect();
        assert!(
            texts.contains(&"resumed"),
            "expected resumed assistant text, got {texts:?}"
        );
    }

    #[tokio::test]
    async fn plan_jail_permission_accepts_execute_authority() {
        let dir = tempdir().unwrap();
        let state = StateRoot::new(dir.path().join("state"));
        let mut config = Config::default();
        config.governance.adapter = "local".into();
        config.model.adapter = "scripted".into();
        config.events.adapter = "none".into();
        config.run.plan_jail = true;
        let write_plan = serde_json::json!({
            "path": crate::tools::PLAN_JAIL_PATH,
            "content": "# do it\n"
        })
        .to_string();
        let write_ok = serde_json::json!({"path": "ok.txt", "content": "yes\n"}).to_string();
        let report = serde_json::json!({"summary": "planned", "success": true}).to_string();
        config.model.script_json = Some(
            serde_json::json!([
                {"tool_calls":[{"name":"write_file","args_json": write_plan}]},
                {"tool_calls":[{"name":"report","args_json": report}]},
                {"tool_calls":[{"name":"write_file","args_json": write_ok}]},
                {"content":"executed"}
            ])
            .to_string(),
        );
        let host = AcpHost::new(Harness::from_config(config, state).unwrap());
        let client = RecordingClient {
            updates: Mutex::new(Vec::new()),
            permission: PermissionOutcome::Allow,
        };
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let session_id = init_and_new(&host, &client, &cwd).await;
        let prompt = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "session/prompt",
                    "params": {
                        "sessionId": session_id,
                        "prompt": [{"type":"text","text":"plan"}]
                    }
                }),
                &client,
            )
            .await
            .unwrap();
        assert_eq!(rpc_ok(&prompt)["stopReason"], "end_turn");
        assert_eq!(
            std::fs::read_to_string(cwd.join("ok.txt")).unwrap(),
            "yes\n"
        );
    }

    struct HangIfRequestedClient;

    #[async_trait]
    impl AcpClient for HangIfRequestedClient {
        async fn notify(&self, _method: &str, _params: Value) -> Result<(), String> {
            Ok(())
        }

        async fn request(&self, method: &str, _params: Value) -> Result<Value, String> {
            panic!("permission RPC should not be sent when already cancelled: {method}");
        }
    }

    #[tokio::test]
    async fn request_permission_sees_existing_cancel() {
        let (_tx, rx) = watch::channel(true);
        let park = ParkInfo {
            reason: "ask:write_file".into(),
            question: "Allow `write_file`?".into(),
            tool_call_id: "call-1".into(),
            kind: ParkKind::Ask,
            approval_id: None,
            display_call_id: None,
            args_json: Some("{}".into()),
            plan_digest: String::new(),
        };
        let (outcome, _) = request_permission("sess-1", &park, &HangIfRequestedClient, Some(&rx))
            .await
            .unwrap();
        assert_eq!(outcome, PermissionOutcome::Cancelled);
    }

    #[test]
    fn json_raw_input_parses_object_arguments() {
        assert_eq!(
            json_raw_input(Some(r#"{"path":"ok.txt"}"#)),
            json!({"path": "ok.txt"})
        );
        assert_eq!(json_raw_input(Some("not-json")), json!({}));
        assert_eq!(json_raw_input(None), json!({}));
    }

    #[test]
    fn parse_permission_fails_closed_without_explicit_choice() {
        assert!(parse_permission(&json!({})).is_err());
        assert!(parse_permission(&json!({"outcome":{}})).is_err());
        assert!(parse_permission(&json!({"outcome":{"outcome":"maybe"}})).is_err());
        assert!(matches!(
            parse_permission(&json!({"outcome":{"outcome":"allow","optionId":"allow"}})).unwrap(),
            PermissionOutcome::Allow
        ));
        assert!(matches!(
            parse_permission(&json!({"outcome":{"outcome":"selected","optionId":"allow"}}))
                .unwrap(),
            PermissionOutcome::Allow
        ));
        assert_eq!(
            permission_answer(
                &json!({"outcome":{"outcome":"allow","optionId":"allow","answer":"keep going"}}),
                PermissionOutcome::Allow
            ),
            "keep going"
        );
        assert_eq!(
            permission_answer(
                &json!({"outcome":{"outcome":"allow","optionId":"allow"}}),
                PermissionOutcome::Allow
            ),
            "approved"
        );
        assert!(matches!(
            parse_permission(&json!({"outcome":{"outcome":"deny","optionId":"deny"}})).unwrap(),
            PermissionOutcome::Deny
        ));
    }

    fn parked_ask_checkpoint(runs: &Path, run_id: &str) -> Checkpoint {
        Checkpoint {
            version: crate::checkpoint::CHECKPOINT_VERSION,
            run_id: run_id.into(),
            task: "t".into(),
            prompt_id: crate::checkpoint::prompt_id(crate::run::SYSTEM_PROMPT),
            messages: vec![
                ChatMessage {
                    role: "user".into(),
                    content: "write".into(),
                    tool_call_id: String::new(),
                    tool_calls: vec![],
                },
                ChatMessage {
                    role: "assistant".into(),
                    content: String::new(),
                    tool_call_id: String::new(),
                    tool_calls: vec![
                        crate::model::ToolCall {
                            id: "call-a".into(),
                            name: "write_file".into(),
                            args_json: r#"{"path":"a.txt","content":"a"}"#.into(),
                        },
                        crate::model::ToolCall {
                            id: "call-b".into(),
                            name: "write_file".into(),
                            args_json: r#"{"path":"b.txt","content":"b"}"#.into(),
                        },
                    ],
                },
            ],
            completed_turns: 1,
            workspace: runs.join(run_id).join("workspace"),
            keep_workspace: true,
            workspace_adapter: "inplace".into(),
            park: Some(ParkedState {
                reason: "ask:write_file".into(),
                question: "Allow `write_file`?".into(),
                tool_call_id: "call-a".into(),
                kind: ParkKind::Ask,
                allow_call_id: "tool-1-0-call-a".into(),
                plan_digest: String::new(),
            }),
            todos: vec![],
            governance: None,
            replay: None,
            content: None,
            prompt_start_turns: None,
            plan_jail: false,
            nested: false,
            children: vec![],
            nested_depth: 0,
            parent_run_id: String::new(),
            nested_profile: String::new(),
        }
    }

    #[test]
    fn persist_prompt_wait_closes_every_outstanding_call() {
        let dir = tempdir().unwrap();
        let runs = dir.path().join("runs");
        let checkpoint = parked_ask_checkpoint(&runs, "run-1");
        std::fs::create_dir_all(&checkpoint.workspace).unwrap();
        checkpoint.save(&runs).unwrap();
        persist_prompt_wait(&runs, "run-1").unwrap();
        let loaded = Checkpoint::load(&runs, "run-1").unwrap();
        assert_eq!(loaded.park.as_ref().unwrap().kind, ParkKind::PromptWait);
        let results: Vec<&str> = loaded
            .messages
            .iter()
            .filter(|message| message.role == "tool")
            .map(|message| message.tool_call_id.as_str())
            .collect();
        assert_eq!(results, vec!["call-a", "call-b"]);
    }

    #[test]
    fn restored_permission_includes_tool_arguments() {
        let dir = tempdir().unwrap();
        let runs = dir.path().join("runs");
        let checkpoint = parked_ask_checkpoint(&runs, "run-1");
        std::fs::create_dir_all(&checkpoint.workspace).unwrap();
        checkpoint.save(&runs).unwrap();
        let loaded = Checkpoint::load(&runs, "run-1").unwrap();
        let info = park_info_from_checkpoint(&loaded).unwrap();
        assert_eq!(
            info.args_json.as_deref(),
            Some(r#"{"path":"a.txt","content":"a"}"#)
        );
        assert_eq!(info.display_call_id.as_deref(), Some("tool-1-0-call-a"));
    }
}
