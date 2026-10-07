//! ACP process host: newline-delimited JSON-RPC session over [`crate::Harness`].
//!
//! Evolving surface, same rank as `shikigami mcp`. Not freeze-core.
//! See [ADR 0014](../docs/decisions/0014-usable-guest-hosts.md).

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::io::{BufReader, stdin, stdout};
use tokio::sync::{Mutex, mpsc, oneshot, watch};

use crate::checkpoint::{Checkpoint, ParkedState, is_safe_run_id};
use crate::content::{
    ContentDisclosureState, ContentMessageV1, ContentPartDescriptor, ContentPartKind,
    ContentProvenanceV1, ContentResolver, ContentRunRequestV1, ContentToStore,
    MAX_CONTENT_AGGREGATE_BYTES, MAX_CONTENT_PART_BYTES, MAX_CONTENT_PARTS, ResolvedContent,
    sha256_digest,
};
use crate::events::{AsyncChannelRx, AsyncChannelSink, EventSink, HarnessEvent};
use crate::harness::{Harness, HarnessError};
use crate::identity::{PRODUCT, VERSION};
use crate::mcp::framing;
use crate::model::{ChatMessage, ModelPort};
use crate::run::{
    AskDecision, ParkInfo, ParkKind, PlanDecision, RunError, RunRequest, RunTermination,
    compact_messages,
};

/// ACP protocol version this host speaks. `initialize` accepts client offers
/// of 1 or 2 so v2-capable clients can connect, and always replies with this
/// value. Success is not an agreement to speak v2.
const PROTOCOL_VERSION: u32 = 1;

/// Stdio inbound frame bound. Content parts cap at 16MiB aggregate; inline
/// base64 expands 4/3, plus the JSON-RPC envelope. MCP stdio stays at 1MiB.
const MAX_ACP_FRAME_BYTES: usize = 32 * 1024 * 1024;

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
    /// Frozen catalog name. Mapping overlay is current settings (ADR 0016).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    mode: Option<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    mode_frozen: bool,
}

impl PersistedSession {
    fn mode_is_frozen(&self) -> bool {
        self.mode_frozen || self.mode.is_some() || self.run_id.is_some()
    }
}

#[derive(Clone)]
struct LiveSession {
    cwd: PathBuf,
    run_id: Option<String>,
    cancel: Option<watch::Sender<bool>>,
    mode: Option<String>,
    mode_frozen: bool,
    /// In-memory payload custody for a content session. Not persisted.
    content: Option<SessionContent>,
}

#[derive(Clone)]
struct SessionContent {
    task: String,
    messages: Vec<ContentMessageV1>,
    resolver: Arc<SessionContentStore>,
}

struct SessionContentStore {
    values: std::sync::Mutex<HashMap<String, (ContentPartKind, Vec<u8>)>>,
}

impl SessionContentStore {
    fn new() -> Self {
        Self {
            values: std::sync::Mutex::new(HashMap::new()),
        }
    }

    fn insert(
        &self,
        part_id: &str,
        kind: ContentPartKind,
        media_type: &str,
        payload: &[u8],
        session_id: &str,
    ) -> Result<ContentPartDescriptor, Value> {
        if payload.len() as u64 > MAX_CONTENT_PART_BYTES {
            return Err(rpc_error(
                -32602,
                "attachment exceeds the content part size bound",
            ));
        }
        let reference = format!("acp-{part_id}");
        self.values
            .lock()
            .map_err(|_| rpc_error(-32603, "content store lock poisoned"))?
            .insert(reference.clone(), (kind, payload.to_vec()));
        Ok(ContentPartDescriptor {
            part_id: part_id.into(),
            kind,
            media_type: media_type.into(),
            byte_length: payload.len() as u64,
            sha256_digest: sha256_digest(payload),
            reference,
            provenance: ContentProvenanceV1 {
                source: "acp".into(),
                source_id: session_id.into(),
                source_version: "v1".into(),
                observed_at_ms: now_ms(),
            },
            disclosure_state: ContentDisclosureState::Accepted,
            disclosure_reason: String::new(),
        })
    }

    fn text(&self, reference: &str) -> Option<String> {
        let (kind, bytes) = self.values.lock().ok()?.get(reference).cloned()?;
        if kind == ContentPartKind::Text {
            String::from_utf8(bytes).ok()
        } else {
            None
        }
    }
}

#[async_trait]
impl ContentResolver for SessionContentStore {
    fn id(&self) -> &str {
        "acp-session-v1"
    }

    async fn resolve(
        &self,
        descriptor: &ContentPartDescriptor,
    ) -> Result<ResolvedContent, crate::content::ContentError> {
        let (kind, bytes) = self
            .values
            .lock()
            .map_err(|_| {
                crate::content::ContentError::Resolver("content store lock poisoned".into())
            })?
            .get(&descriptor.reference)
            .cloned()
            .ok_or_else(|| {
                crate::content::ContentError::Resolver(
                    "attachment payload is no longer in session memory".into(),
                )
            })?;
        if kind == ContentPartKind::Text {
            String::from_utf8(bytes)
                .map(ResolvedContent::Text)
                .map_err(|_| {
                    crate::content::ContentError::Resolver(
                        "text attachment is not valid utf-8".into(),
                    )
                })
        } else {
            Ok(ResolvedContent::Bytes(bytes))
        }
    }

    async fn store(
        &self,
        content: ContentToStore,
    ) -> Result<ContentPartDescriptor, crate::content::ContentError> {
        let bytes = content.payload.as_bytes().to_vec();
        let reference = format!("acp-{}", content.part_id);
        self.values
            .lock()
            .map_err(|_| {
                crate::content::ContentError::Resolver("content store lock poisoned".into())
            })?
            .insert(reference.clone(), (content.kind, bytes.clone()));
        Ok(ContentPartDescriptor {
            part_id: content.part_id,
            kind: content.kind,
            media_type: content.media_type,
            byte_length: bytes.len() as u64,
            sha256_digest: sha256_digest(&bytes),
            reference,
            provenance: content.provenance,
            disclosure_state: ContentDisclosureState::Accepted,
            disclosure_reason: String::new(),
        })
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
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
            "session/compact" => self.session_compact(&params).await,
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
                "promptCapabilities": prompt_capabilities(self.harness.model_port())
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
        let mode = optional_mode(params)?;
        if let Some(named) = mode.as_deref() {
            self.harness
                .config
                .clone()
                .apply_session_mode(named)
                .map_err(|e| rpc_error(-32602, e.to_string()))?;
        }
        let session_id = format!("sess-{}", uuid::Uuid::new_v4());
        let live = LiveSession {
            cwd: cwd.clone(),
            run_id: None,
            cancel: None,
            mode: mode.clone(),
            mode_frozen: mode.is_some(),
            content: None,
        };
        self.persist(&session_id, &live)
            .map_err(|e| rpc_error(-32603, e))?;
        self.sessions.lock().await.insert(session_id.clone(), live);
        let mut result = json!({ "sessionId": session_id });
        if let Some(mode) = mode {
            result["mode"] = json!(mode);
        }
        Ok(result)
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
                mode: live.mode.clone(),
                mode_frozen: live.mode_frozen,
                content: live.content.clone(),
            }
        } else {
            let persisted = self
                .load_persisted(session_id)
                .ok_or_else(|| rpc_error(-32002, "unknown session"))?;
            let live = LiveSession {
                cwd: PathBuf::from(&persisted.cwd),
                run_id: persisted.run_id.clone(),
                cancel: None,
                mode: persisted.mode.clone(),
                mode_frozen: persisted.mode_is_frozen(),
                content: None,
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

    async fn session_compact(&self, params: &Value) -> Result<Value, Value> {
        self.require_init()?;
        let session_id = params
            .get("sessionId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| rpc_error(-32602, "session/compact requires sessionId"))?;
        let (cwd, run_id, mode) = {
            let sessions = self.sessions.lock().await;
            let live = sessions
                .get(session_id)
                .ok_or_else(|| rpc_error(-32002, "unknown session"))?;
            if live.cancel.is_some() {
                return Err(rpc_error(-32000, "prompt already in flight"));
            }
            (live.cwd.clone(), live.run_id.clone(), live.mode.clone())
        };
        let Some(run_id) = run_id else {
            return Ok(json!({ "before": 0, "after": 0 }));
        };
        let harness = self
            .harness_for_cwd(&cwd, mode.as_deref())
            .map_err(|e| rpc_error(-32603, e))?;
        let mut checkpoint = Checkpoint::load(&harness.state.runs_dir(), &run_id)
            .map_err(|_| rpc_error(-32603, "session run checkpoint is unreadable"))?;
        let keep = harness.config.run.compact_keep_tail.max(2) as usize;
        // Content sidecar stays on the 32-part contract; this cut is ChatMessage history.
        let before = checkpoint.messages.len();
        let after = if let Some((_, after)) = compact_messages(&mut checkpoint.messages, 0, keep) {
            checkpoint
                .save(&harness.state.runs_dir())
                .map_err(|e| rpc_error(-32603, e.to_string()))?;
            after
        } else {
            before
        };
        Ok(json!({ "before": before, "after": after }))
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
            .harness_for_cwd(&live.cwd, live.mode.as_deref())
            .map_err(|e| rpc_error(-32603, e))?;
        let checkpoint = Checkpoint::load(&harness.state.runs_dir(), run_id)
            .map_err(|_| rpc_error(-32603, "session run checkpoint is unreadable"))?;
        if checkpoint.content.is_some() && live.content.is_none() {
            return Err(rpc_error(
                -32603,
                "content payload is no longer in session memory",
            ));
        }
        let updates = if let (Some(binding), Some(content)) =
            (checkpoint.content.as_ref(), live.content.as_ref())
        {
            let sidecar = crate::content::load_sidecar(&harness.state.runs_dir(), run_id, binding)
                .map_err(|_| rpc_error(-32603, "session content sidecar is unreadable"))?;
            content_conversation_updates(session_id, &sidecar.messages, content.resolver.as_ref())
        } else {
            conversation_updates(session_id, &checkpoint.messages)
        };
        send_updates(client, updates).await
    }

    async fn session_prompt(&self, params: &Value, client: &dyn AcpClient) -> Result<Value, Value> {
        self.require_init()?;
        let session_id = params
            .get("sessionId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| rpc_error(-32602, "session/prompt requires sessionId"))?
            .to_string();
        let parsed = parse_prompt(params.get("prompt").unwrap_or(&Value::Null))?;
        let requested_mode = optional_mode(params)?;
        let (cwd, resume_run_id, cancel_rx, mode, existing_content) = {
            let mut sessions = self.sessions.lock().await;
            let live = sessions
                .get_mut(&session_id)
                .ok_or_else(|| rpc_error(-32002, "unknown session"))?;
            if live.mode_frozen {
                match (live.mode.as_deref(), requested_mode.as_deref()) {
                    (Some(frozen), Some(named)) if frozen != named => {
                        return Err(rpc_error(
                            -32602,
                            format!(
                                "session mode `{frozen}` is frozen; start a new session to use `{named}`"
                            ),
                        ));
                    }
                    (None, Some(named)) => {
                        return Err(rpc_error(
                            -32602,
                            format!(
                                "session mode is frozen unset; start a new session to use `{named}`"
                            ),
                        ));
                    }
                    _ => {}
                }
            } else {
                if let Some(named) = requested_mode.as_deref() {
                    self.harness
                        .config
                        .clone()
                        .apply_session_mode(named)
                        .map_err(|e| rpc_error(-32602, e.to_string()))?;
                    live.mode = Some(named.to_string());
                }
                live.mode_frozen = true;
                self.persist(&session_id, live)
                    .map_err(|e| rpc_error(-32603, e))?;
            }
            let rx = match live.cancel.as_ref() {
                Some(tx) => tx.subscribe(),
                None => {
                    let (tx, rx) = watch::channel(false);
                    live.cancel = Some(tx);
                    rx
                }
            };
            (
                live.cwd.clone(),
                live.run_id.clone(),
                rx,
                live.mode.clone(),
                live.content.clone(),
            )
        };

        let outcome = async {
            let harness = self
                .harness_for_cwd(&cwd, mode.as_deref())
                .map_err(|e| rpc_error(-32603, e))?;
            let (sink, mut events) = AsyncChannelSink::pair();
            let sink: Arc<dyn EventSink> = Arc::new(sink);
            // Attachments ride the first session/prompt of a session. Later
            // prompts are text follow-ups on the same run (Issue #401).
            if parsed.has_attachments() && resume_run_id.is_some() {
                return Err(rpc_error(
                    -32602,
                    "attachments cannot be added to an existing session run",
                ));
            }
            let mut drive = if parsed.has_attachments() {
                let kinds = harness.model_port().content_kinds();
                parsed.require_supported(kinds)?;
                let store = Arc::new(SessionContentStore::new());
                let messages = parsed.messages(&store, &session_id)?;
                let content = SessionContent {
                    task: parsed.text.clone(),
                    messages: messages.clone(),
                    resolver: Arc::clone(&store),
                };
                {
                    let mut sessions = self.sessions.lock().await;
                    if let Some(live) = sessions.get_mut(&session_id) {
                        live.content = Some(content.clone());
                    }
                }
                // Content v1 denies escalate parking and skips ChatMessage
                // auto-compact. Attachment sessions inherit that 32-part sidecar bound.
                let mut request = ContentRunRequestV1::new(parsed.text.clone(), messages, store);
                request.keep_workspace = true;
                request.session_wait = true;
                request.cancel = Some(cancel_rx);
                PromptDrive::Content(request)
            } else if let Some(run_id) = resume_run_id.clone() {
                match resume_prompt_drive(
                    &harness,
                    &session_id,
                    run_id,
                    &parsed.text,
                    existing_content,
                    cancel_rx,
                    client,
                )
                .await?
                {
                    ResumeOutcome::Drive(drive) => *drive,
                    ResumeOutcome::Done(value) => return Ok(value),
                }
            } else {
                let mut request = RunRequest::new(parsed.text.clone());
                request.keep_workspace = true;
                request.session_wait = true;
                request.cancel = Some(cancel_rx);
                PromptDrive::Text(request)
            };

            let stop = self
                .drive_prompt(&session_id, harness, &mut drive, client, sink, &mut events)
                .await?;
            Ok(json!({ "stopReason": stop }))
        }
        .await;
        if outcome.is_err() {
            let mut sessions = self.sessions.lock().await;
            if let Some(live) = sessions.get_mut(&session_id)
                && live.run_id.is_none()
            {
                live.content = None;
            }
        }
        self.clear_cancel(&session_id).await;
        outcome
    }

    async fn drive_prompt(
        &self,
        session_id: &str,
        harness: Harness,
        drive: &mut PromptDrive,
        client: &dyn AcpClient,
        sink: Arc<dyn EventSink>,
        events: &mut AsyncChannelRx,
    ) -> Result<&'static str, Value> {
        loop {
            if let Some(run_id) = drive.resume_run_id() {
                let _ = self.set_run_id(session_id, Some(run_id)).await;
            }
            let (result, forwarded_run_id) = run_and_forward(
                self,
                &harness,
                drive.clone(),
                Arc::clone(&sink),
                events,
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
                        drive.resume_run_id().or(forwarded_run_id),
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
                        request_permission(session_id, park, client, drive.cancel()).await?;
                    match outcome {
                        PermissionOutcome::Cancelled => {
                            persist_prompt_wait_for_drive(
                                &harness.state.runs_dir(),
                                &result.run_id,
                                drive,
                            )
                            .await
                            .map_err(|e| rpc_error(-32603, e))?;
                            return Ok("cancelled");
                        }
                        PermissionOutcome::Allow => {
                            drive.resume_ask(result.run_id.clone(), AskDecision::Allow);
                        }
                        PermissionOutcome::Deny => {
                            drive.resume_ask(result.run_id.clone(), AskDecision::Deny);
                        }
                    }
                }
                Some(ParkKind::Plan) => {
                    let park = park.expect("park");
                    let (outcome, _) =
                        request_permission(session_id, park, client, drive.cancel()).await?;
                    match outcome {
                        PermissionOutcome::Cancelled => {
                            persist_prompt_wait_for_drive(
                                &harness.state.runs_dir(),
                                &result.run_id,
                                drive,
                            )
                            .await
                            .map_err(|e| rpc_error(-32603, e))?;
                            return Ok("cancelled");
                        }
                        PermissionOutcome::Allow => {
                            drive.resume_plan(result.run_id.clone(), PlanDecision::Accept);
                        }
                        PermissionOutcome::Deny => {
                            drive.resume_plan(result.run_id.clone(), PlanDecision::Reject);
                        }
                    }
                }
                Some(ParkKind::Escalate) => {
                    let park = park.expect("park");
                    let (outcome, answer) =
                        request_permission(session_id, park, client, drive.cancel()).await?;
                    match outcome {
                        PermissionOutcome::Cancelled => {
                            persist_prompt_wait_for_drive(
                                &harness.state.runs_dir(),
                                &result.run_id,
                                drive,
                            )
                            .await
                            .map_err(|e| rpc_error(-32603, e))?;
                            return Ok("cancelled");
                        }
                        PermissionOutcome::Allow | PermissionOutcome::Deny => {
                            drive.resume_answer(result.run_id.clone(), answer);
                        }
                    }
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

    pub async fn arm_cancel(&self, session_id: &str) {
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

    pub fn context_settings(&self) -> &crate::config::ContextSettings {
        &self.harness.config.context
    }

    fn harness_for_cwd(&self, cwd: &Path, mode: Option<&str>) -> Result<Harness, String> {
        let mut config = self.harness.config.clone();
        config.workspace.adapter = "inplace".into();
        config.workspace.root = cwd.display().to_string();
        if let Some(mode) = mode {
            config.apply_session_mode(mode).map_err(|e| e.to_string())?;
        }
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
            mode: live.mode.clone(),
            mode_frozen: live.mode_frozen,
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

    /// Persisted ACP session ids for `cwd`, newest mtime first.
    pub fn session_ids_for_cwd(&self, cwd: &Path) -> Vec<String> {
        let dir = self.sessions_dir();
        let Ok(entries) = std::fs::read_dir(dir) else {
            return Vec::new();
        };
        let mut found: Vec<(std::time::SystemTime, String)> = Vec::new();
        for entry in entries {
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
            found.push((mtime, persisted.session_id));
        }
        found.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
        found.into_iter().map(|(_, id)| id).collect()
    }

    /// Most recently persisted session for `cwd`, if any. Used by the TUI
    /// continue-last-in-cwd path (`session/load`, fail closed → `session/new`).
    pub fn last_session_id_for_cwd(&self, cwd: &Path) -> Option<String> {
        self.session_ids_for_cwd(cwd).into_iter().next()
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
        let store = session_content_store(self, session_id).await;
        if persist_prompt_wait_with_resolver(
            &harness.state.runs_dir(),
            &run_id,
            store.as_deref().map(|store| store as &dyn ContentResolver),
        )
        .await
        .is_ok()
        {
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

fn resolve_content_park_args(info: &mut crate::run::ParkInfo, store: &SessionContentStore) {
    let Some(raw) = info.args_json.as_deref() else {
        return;
    };
    let Some(part_id) = crate::content::tool_arguments_part_id(raw) else {
        return;
    };
    if let Some(text) = store.text(&format!("acp-{part_id}")) {
        info.args_json = Some(text);
    }
}

fn resolved_content_tool_args(
    call: &crate::model::ToolCall,
    message: &ContentMessageV1,
    store: &SessionContentStore,
) -> Option<String> {
    crate::content::tool_arguments_part_id(&call.args_json).and_then(|part_id| {
        message
            .parts
            .iter()
            .find(|part| part.part_id == part_id)
            .and_then(|part| store.text(&part.reference))
    })
}

fn content_tool_raw_input(
    call: &crate::model::ToolCall,
    message: &ContentMessageV1,
    store: &SessionContentStore,
) -> Value {
    json_raw_input(
        resolved_content_tool_args(call, message, store)
            .as_deref()
            .or(Some(call.args_json.as_str())),
    )
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

async fn persist_prompt_wait_for_drive(
    runs_dir: &Path,
    run_id: &str,
    drive: &PromptDrive,
) -> Result<(), String> {
    persist_prompt_wait_with_resolver(runs_dir, run_id, drive.content_resolver()).await
}

async fn persist_prompt_wait_with_resolver(
    runs_dir: &Path,
    run_id: &str,
    resolver: Option<&dyn ContentResolver>,
) -> Result<(), String> {
    persist_prompt_wait(runs_dir, run_id)?;
    let Some(resolver) = resolver else {
        return Ok(());
    };
    close_content_outstanding_calls(runs_dir, run_id, resolver).await
}

async fn close_content_outstanding_calls(
    runs_dir: &Path,
    run_id: &str,
    resolver: &dyn ContentResolver,
) -> Result<(), String> {
    let mut checkpoint = Checkpoint::load(runs_dir, run_id).map_err(|e| e.to_string())?;
    let Some(binding) = checkpoint.content.clone() else {
        return Ok(());
    };
    let mut sidecar =
        crate::content::load_sidecar(runs_dir, run_id, &binding).map_err(|e| e.to_string())?;
    let Some(assistant_idx) = sidecar
        .messages
        .iter()
        .rposition(|message| message.role == "assistant")
    else {
        return Ok(());
    };
    let turn = sidecar.completed_turns;
    let batch = sidecar.messages[assistant_idx].tool_calls.clone();
    let answered: HashSet<String> = sidecar.messages[assistant_idx + 1..]
        .iter()
        .filter(|message| message.role == "tool")
        .map(|message| message.tool_call_id.clone())
        .collect();
    let mut used_ids: HashSet<String> = sidecar
        .messages
        .iter()
        .flat_map(|message| message.parts.iter())
        .map(|part| part.part_id.clone())
        .collect();
    let mut added = false;
    for (index, call) in batch.iter().enumerate() {
        let conversation_id = conversation_call_id(call, turn, index);
        if answered.contains(&conversation_id)
            || (!call.id.is_empty() && answered.contains(&call.id))
        {
            continue;
        }
        let mut part_id = format!("shikigami-cancel-{turn}-{index}");
        let mut suffix = 1u32;
        while !used_ids.insert(part_id.clone()) {
            part_id = format!("shikigami-cancel-{turn}-{index}-{suffix}");
            suffix = suffix.saturating_add(1);
        }
        let descriptor = resolver
            .store(ContentToStore {
                part_id,
                kind: ContentPartKind::Text,
                media_type: "text/plain".into(),
                payload: ResolvedContent::Text("cancelled".into()),
                provenance: ContentProvenanceV1 {
                    source: "session".into(),
                    source_id: conversation_id.clone(),
                    source_version: "v1".into(),
                    observed_at_ms: now_ms(),
                },
            })
            .await
            .map_err(|e| e.to_string())?;
        sidecar.messages.push(ContentMessageV1 {
            role: "tool".into(),
            parts: vec![descriptor],
            tool_call_id: conversation_id,
            tool_calls: Vec::new(),
        });
        added = true;
    }
    if !added {
        return Ok(());
    }
    let binding = crate::content::save_sidecar(runs_dir, &sidecar, Some(&binding))
        .map_err(|e| e.to_string())?;
    checkpoint.content = Some(binding);
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

#[derive(Clone)]
enum PromptDrive {
    Text(RunRequest),
    Content(ContentRunRequestV1),
}

impl PromptDrive {
    fn resume_run_id(&self) -> Option<String> {
        match self {
            Self::Text(request) => request.resume_run_id.clone(),
            Self::Content(request) => request.resume_run_id.clone(),
        }
    }

    fn cancel(&self) -> Option<&watch::Receiver<bool>> {
        match self {
            Self::Text(request) => request.cancel.as_ref(),
            Self::Content(request) => request.cancel.as_ref(),
        }
    }

    fn content_resolver(&self) -> Option<&dyn ContentResolver> {
        match self {
            Self::Text(_) => None,
            Self::Content(request) => Some(request.resolver.as_ref()),
        }
    }

    fn apply_resume(&mut self, run_id: String) {
        match self {
            Self::Text(request) => {
                request.keep_workspace = true;
                request.session_wait = true;
                request.resume_run_id = Some(run_id);
                request.resume_prompt = None;
                request.resume_ask = None;
                request.resume_plan = None;
                request.resume_answer = None;
            }
            Self::Content(request) => {
                request.keep_workspace = true;
                request.session_wait = true;
                request.resume_run_id = Some(run_id);
                request.resume_prompt = None;
                request.resume_ask = None;
                request.resume_plan = None;
                request.resume_answer = None;
            }
        }
    }

    fn resume_ask(&mut self, run_id: String, decision: AskDecision) {
        self.apply_resume(run_id);
        match self {
            Self::Text(request) => request.resume_ask = Some(decision),
            Self::Content(request) => request.resume_ask = Some(decision),
        }
    }

    fn resume_plan(&mut self, run_id: String, decision: PlanDecision) {
        self.apply_resume(run_id);
        match self {
            Self::Text(request) => request.resume_plan = Some(decision),
            Self::Content(request) => request.resume_plan = Some(decision),
        }
    }

    fn resume_answer(&mut self, run_id: String, answer: String) {
        self.apply_resume(run_id);
        match self {
            Self::Text(request) => request.resume_answer = Some(answer),
            Self::Content(request) => request.resume_answer = Some(answer),
        }
    }
}

enum ResumeOutcome {
    Drive(Box<PromptDrive>),
    Done(Value),
}

async fn resume_prompt_drive(
    harness: &Harness,
    session_id: &str,
    run_id: String,
    prompt_text: &str,
    existing_content: Option<SessionContent>,
    cancel_rx: watch::Receiver<bool>,
    client: &dyn AcpClient,
) -> Result<ResumeOutcome, Value> {
    let checkpoint = Checkpoint::load(&harness.state.runs_dir(), &run_id)
        .map_err(|_| rpc_error(-32603, "session run checkpoint is unreadable"))?;
    let content_run = checkpoint.content.is_some();
    if content_run && existing_content.is_none() {
        return Err(rpc_error(
            -32603,
            "content payload is no longer in session memory",
        ));
    }
    let content_store = existing_content
        .as_ref()
        .map(|content| Arc::clone(&content.resolver));
    let mut drive = if let Some(content) = existing_content {
        let mut request = ContentRunRequestV1::new(
            content.task.clone(),
            content.messages.clone(),
            content.resolver,
        );
        request.keep_workspace = true;
        request.session_wait = true;
        request.resume_run_id = Some(run_id.clone());
        request.cancel = Some(cancel_rx.clone());
        PromptDrive::Content(request)
    } else {
        let mut request = RunRequest::new("");
        request.keep_workspace = true;
        request.session_wait = true;
        request.resume_run_id = Some(run_id.clone());
        request.cancel = Some(cancel_rx.clone());
        PromptDrive::Text(request)
    };
    if checkpoint.is_prompt_wait() {
        match &mut drive {
            PromptDrive::Text(request) => request.resume_prompt = Some(prompt_text.to_string()),
            PromptDrive::Content(request) => request.resume_prompt = Some(prompt_text.to_string()),
        }
        return Ok(ResumeOutcome::Drive(Box::new(drive)));
    }
    if checkpoint.is_ask_park() || checkpoint.is_escalate_park() || checkpoint.is_plan_park() {
        let mut info = park_info_from_checkpoint(&checkpoint).map_err(|e| rpc_error(-32603, e))?;
        if let Some(store) = content_store.as_deref() {
            resolve_content_park_args(&mut info, store);
        }
        let (outcome, answer) =
            request_permission(session_id, &info, client, Some(&cancel_rx)).await?;
        if outcome == PermissionOutcome::Cancelled {
            persist_prompt_wait_for_drive(&harness.state.runs_dir(), &run_id, &drive)
                .await
                .map_err(|e| rpc_error(-32603, e))?;
            return Ok(ResumeOutcome::Done(json!({ "stopReason": "cancelled" })));
        }
        if checkpoint.is_ask_park() {
            drive.resume_ask(
                run_id,
                match outcome {
                    PermissionOutcome::Allow => AskDecision::Allow,
                    PermissionOutcome::Deny => AskDecision::Deny,
                    PermissionOutcome::Cancelled => unreachable!("cancelled returned"),
                },
            );
        } else if checkpoint.is_plan_park() {
            drive.resume_plan(
                run_id,
                match outcome {
                    PermissionOutcome::Allow => PlanDecision::Accept,
                    PermissionOutcome::Deny => PlanDecision::Reject,
                    PermissionOutcome::Cancelled => unreachable!("cancelled returned"),
                },
            );
        } else {
            drive.resume_answer(run_id, answer);
        }
        return Ok(ResumeOutcome::Drive(Box::new(drive)));
    }
    if checkpoint
        .park
        .as_ref()
        .is_some_and(|park| park.kind == ParkKind::Approval)
    {
        return Err(rpc_error(
            -32603,
            "governed approval parks are not mapped on ACP yet",
        ));
    }
    Err(rpc_error(-32603, "session run is not waiting for a prompt"))
}

async fn run_and_forward(
    host: &AcpHost,
    harness: &Harness,
    drive: PromptDrive,
    sink: Arc<dyn EventSink>,
    events: &mut AsyncChannelRx,
    session_id: &str,
    client: &dyn AcpClient,
) -> (Result<crate::run::RunResult, HarnessError>, Option<String>) {
    let known_run_id = drive.resume_run_id();
    let mut seen_run_id = known_run_id.clone();
    if let Some(id) = known_run_id.as_deref() {
        persist_session_run(host, session_id, id).await;
    }
    let mut held = Vec::new();
    let run_fut = async {
        match drive {
            PromptDrive::Text(request) => harness.run_with_events(request, Some(sink)).await,
            PromptDrive::Content(request) => harness
                .run_content_with_events(request, Some(sink))
                .await
                .map(|result| result.run),
        }
    };
    tokio::pin!(run_fut);
    let result = loop {
        tokio::select! {
            result = &mut run_fut => break result,
            event = events.recv() => {
                let Some(first) = event else {
                    break (&mut run_fut).await;
                };
                let (mut updates, event_run_id) =
                    drain_event_batch(session_id, Some(first), events);
                held.append(&mut updates);
                if let Some(id) = event_run_id.clone() {
                    seen_run_id = Some(id.clone());
                    persist_session_run(host, session_id, &id).await;
                }
                let run_id = event_run_id.as_deref().or(known_run_id.as_deref());
                if let Some(run_id) = run_id {
                    let store = session_content_store(host, session_id).await;
                    fill_content_live_updates(&mut held, harness, run_id, store.as_deref());
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
                            let store = session_content_store(host, session_id).await;
                            let _ = persist_prompt_wait_with_resolver(
                                &harness.state.runs_dir(),
                                id,
                                store.as_deref().map(|store| store as &dyn ContentResolver),
                            )
                            .await;
                            persist_session_run(host, session_id, id).await;
                        }
                        return (Err(error), run_id);
                    }
                }
            }
        }
    };
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
        let store = session_content_store(host, session_id).await;
        fill_content_live_updates(&mut held, harness, run_id, store.as_deref());
    }
    let send = send_updates(client, held).await.map_err(update_error);
    if let Err(error) = send {
        return (Err(error), run_id);
    }
    (result, run_id)
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

async fn session_content_store(
    host: &AcpHost,
    session_id: &str,
) -> Option<Arc<SessionContentStore>> {
    host.sessions.lock().await.get(session_id).and_then(|live| {
        live.content
            .as_ref()
            .map(|content| Arc::clone(&content.resolver))
    })
}

fn fill_content_live_updates(
    updates: &mut [Value],
    harness: &Harness,
    run_id: &str,
    store: Option<&SessionContentStore>,
) {
    fill_assistant_content(updates, harness, run_id, store);
    fill_content_tool_updates(updates, harness, run_id, store);
}

fn fill_assistant_content(
    updates: &mut [Value],
    harness: &Harness,
    run_id: &str,
    store: Option<&SessionContentStore>,
) {
    apply_assistant_content(
        updates,
        || Checkpoint::load(&harness.state.runs_dir(), run_id),
        |checkpoint| content_assistant_texts(harness, run_id, checkpoint, store),
    );
}

fn content_assistant_texts(
    harness: &Harness,
    run_id: &str,
    checkpoint: &Checkpoint,
    store: Option<&SessionContentStore>,
) -> Option<Vec<String>> {
    let binding = checkpoint.content.as_ref()?;
    let sidecar = crate::content::load_sidecar(&harness.state.runs_dir(), run_id, binding).ok()?;
    let store = store?;
    Some(
        sidecar
            .messages
            .iter()
            .filter(|message| message.role == "assistant")
            .map(|message| {
                assistant_visible_text(&message.parts, |reference| store.text(reference))
            })
            .collect(),
    )
}

fn assistant_visible_text(
    parts: &[ContentPartDescriptor],
    text: impl Fn(&str) -> Option<String>,
) -> String {
    parts
        .iter()
        .filter(|part| {
            part.kind == ContentPartKind::Text && part.provenance.source != "model-tool-arguments"
        })
        .filter_map(|part| text(&part.reference))
        .collect::<Vec<_>>()
        .join("")
}

fn apply_assistant_content<E>(
    updates: &mut [Value],
    load: impl FnOnce() -> Result<Checkpoint, E>,
    content_texts: impl FnOnce(&Checkpoint) -> Option<Vec<String>>,
) {
    let chunk_count = updates
        .iter()
        .filter(|update| update["update"]["sessionUpdate"] == "agent_message_chunk")
        .count();
    if chunk_count == 0 {
        return;
    }
    let Ok(checkpoint) = load() else {
        return;
    };
    let owned = content_texts(&checkpoint);
    let fallback: Vec<&str> = checkpoint
        .messages
        .iter()
        .rev()
        .filter(|message| message.role == "assistant")
        .map(|message| message.content.as_str())
        .take(chunk_count)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    let content_refs: Vec<&str> = owned
        .as_ref()
        .map(|texts| texts.iter().map(String::as_str).collect())
        .unwrap_or_else(|| fallback.to_vec());
    let assistants = if owned.is_some() {
        let start = content_refs.len().saturating_sub(chunk_count);
        content_refs[start..].to_vec()
    } else {
        fallback
    };
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

/// Harness events stay projected (ADR 0006). The ACP client that owns the
/// session store gets the same resolved tool args and results as session/load.
fn fill_content_tool_updates(
    updates: &mut [Value],
    harness: &Harness,
    run_id: &str,
    store: Option<&SessionContentStore>,
) {
    let Some(store) = store else {
        return;
    };
    let has_tools = updates.iter().any(|update| {
        matches!(
            update["update"]["sessionUpdate"].as_str(),
            Some("tool_call" | "tool_call_update")
        )
    });
    if !has_tools {
        return;
    }
    let Ok(checkpoint) = Checkpoint::load(&harness.state.runs_dir(), run_id) else {
        return;
    };
    let Some(binding) = checkpoint.content.as_ref() else {
        return;
    };
    let Ok(sidecar) = crate::content::load_sidecar(&harness.state.runs_dir(), run_id, binding)
    else {
        return;
    };
    let mut raw_by_id = HashMap::new();
    let mut result_by_id = HashMap::new();
    let mut turn = 0u32;
    let mut call_ids = HashMap::new();
    for message in &sidecar.messages {
        match message.role.as_str() {
            "assistant" => {
                turn = turn.saturating_add(1);
                for (index, call) in message.tool_calls.iter().enumerate() {
                    let stable = stable_call_id(call, turn, index);
                    let conversation = conversation_call_id(call, turn, index);
                    call_ids.insert(conversation, stable.clone());
                    if !call.id.is_empty() {
                        call_ids.insert(call.id.clone(), stable.clone());
                    }
                    if let Some(raw) = resolved_content_tool_args(call, message, store) {
                        raw_by_id.insert(stable, json_raw_input(Some(&raw)));
                    }
                }
            }
            "tool" => {
                let call_id = call_ids
                    .get(&message.tool_call_id)
                    .cloned()
                    .unwrap_or_else(|| message.tool_call_id.clone());
                let text =
                    assistant_visible_text(&message.parts, |reference| store.text(reference));
                result_by_id.insert(call_id, text);
            }
            _ => {}
        }
    }
    for update in updates {
        let kind = update["update"]["sessionUpdate"]
            .as_str()
            .map(str::to_owned);
        let id = update["update"]["toolCallId"].as_str().map(str::to_owned);
        let Some(id) = id else {
            continue;
        };
        match kind.as_deref() {
            Some("tool_call") => {
                if let Some(raw) = raw_by_id.get(&id) {
                    update["update"]["rawInput"] = raw.clone();
                }
            }
            Some("tool_call_update") => {
                if let Some(text) = result_by_id.get(&id) {
                    update["update"]["content"] = json!([{
                        "type": "content",
                        "content": { "type": "text", "text": text }
                    }]);
                }
            }
            _ => {}
        }
    }
}

fn content_conversation_updates(
    session_id: &str,
    messages: &[ContentMessageV1],
    store: &SessionContentStore,
) -> Vec<Value> {
    let mut updates = Vec::new();
    let mut turn = 0u32;
    let mut call_ids = HashMap::new();
    for message in messages {
        match message.role.as_str() {
            "user" => {
                let text =
                    assistant_visible_text(&message.parts, |reference| store.text(reference));
                if !text.is_empty() {
                    updates.push(json!({
                        "sessionId": session_id,
                        "update": {
                            "sessionUpdate": "user_message_chunk",
                            "content": { "type": "text", "text": text }
                        }
                    }));
                }
            }
            "assistant" => {
                turn = turn.saturating_add(1);
                let text =
                    assistant_visible_text(&message.parts, |reference| store.text(reference));
                if !text.is_empty() {
                    updates.push(json!({
                        "sessionId": session_id,
                        "update": {
                            "sessionUpdate": "agent_message_chunk",
                            "content": { "type": "text", "text": text },
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
                            "rawInput": content_tool_raw_input(call, message, store)
                        }
                    }));
                }
            }
            "tool" => {
                let call_id = call_ids
                    .get(&message.tool_call_id)
                    .cloned()
                    .unwrap_or_else(|| message.tool_call_id.clone());
                let text =
                    assistant_visible_text(&message.parts, |reference| store.text(reference));
                let failed = text == "permission denied" || text == "cancelled";
                updates.push(json!({
                    "sessionId": session_id,
                    "update": {
                        "sessionUpdate": "tool_call_update",
                        "toolCallId": call_id,
                        "status": if failed { "failed" } else { "completed" },
                        "content": [{
                            "type": "content",
                            "content": { "type": "text", "text": text }
                        }]
                    }
                }));
            }
            _ => {}
        }
    }
    updates
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

fn drain_updates(session_id: &str, events: &mut AsyncChannelRx) -> (Vec<Value>, Option<String>) {
    drain_event_batch(session_id, None, events)
}

fn drain_event_batch(
    session_id: &str,
    first: Option<HarnessEvent>,
    events: &mut AsyncChannelRx,
) -> (Vec<Value>, Option<String>) {
    let mut updates = Vec::new();
    let mut run_id = None;
    if let Some(event) = first {
        consume_event(session_id, event, &mut updates, &mut run_id);
    }
    while let Some(event) = events.try_recv() {
        consume_event(session_id, event, &mut updates, &mut run_id);
    }
    (updates, run_id)
}

fn consume_event(
    session_id: &str,
    event: HarnessEvent,
    updates: &mut Vec<Value>,
    run_id: &mut Option<String>,
) {
    match &event {
        HarnessEvent::ToolStart { run_id: id, .. }
        | HarnessEvent::ToolEnd { run_id: id, .. }
        | HarnessEvent::RunFinished { run_id: id, .. }
            if !id.is_empty() =>
        {
            *run_id = Some(id.clone());
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
        HarnessEvent::ContentTurn { turn, .. } => json!({
            "sessionId": session_id,
            "update": {
                "sessionUpdate": "agent_message_chunk",
                "content": { "type": "text", "text": "" },
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
        HarnessEvent::SessionMode {
            mode,
            model,
            effort,
            tools,
        } => json!({
            "sessionId": session_id,
            "update": {
                "sessionUpdate": "session_mode",
                "mode": mode,
                "model": model,
                "effort": effort,
                "tools": tools
            }
        }),
        _ => return,
    };
    updates.push(update);
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

fn optional_mode(params: &Value) -> Result<Option<String>, Value> {
    match params.get("mode") {
        None | Some(Value::Null) => Ok(None),
        Some(value) => {
            let name = value
                .as_str()
                .ok_or_else(|| rpc_error(-32602, "mode must be a string"))?;
            if name.is_empty() {
                return Ok(None);
            }
            if !crate::config::SessionSettings::is_catalog_name(name) {
                return Err(rpc_error(-32602, format!("unknown session mode `{name}`")));
            }
            Ok(Some(name.to_string()))
        }
    }
}

struct ParsedPrompt {
    text: String,
    attachments: Vec<StagedAttachment>,
}

struct StagedAttachment {
    kind: ContentPartKind,
    media_type: String,
    payload: Vec<u8>,
}

impl ParsedPrompt {
    fn has_attachments(&self) -> bool {
        !self.attachments.is_empty()
    }

    fn require_supported(&self, kinds: &[ContentPartKind]) -> Result<(), Value> {
        for attachment in &self.attachments {
            if !kinds.contains(&attachment.kind) {
                return Err(rpc_error(
                    -32602,
                    format!(
                        "selected adapter cannot read {} attachments",
                        content_kind_name(attachment.kind)
                    ),
                ));
            }
        }
        Ok(())
    }

    fn messages(
        &self,
        store: &SessionContentStore,
        session_id: &str,
    ) -> Result<Vec<ContentMessageV1>, Value> {
        let mut parts = Vec::new();
        let mut index = 0u32;
        if !self.text.is_empty() {
            parts.push(store.insert(
                &format!("text-{index}"),
                ContentPartKind::Text,
                "text/plain",
                self.text.as_bytes(),
                session_id,
            )?);
            index += 1;
        }
        for attachment in &self.attachments {
            parts.push(store.insert(
                &format!("part-{index}"),
                attachment.kind,
                &attachment.media_type,
                &attachment.payload,
                session_id,
            )?);
            index += 1;
        }
        if parts.is_empty() {
            return Err(rpc_error(-32602, "session/prompt requires a text part"));
        }
        Ok(vec![ContentMessageV1 {
            role: "user".into(),
            parts,
            tool_call_id: String::new(),
            tool_calls: Vec::new(),
        }])
    }
}

fn parse_prompt(prompt: &Value) -> Result<ParsedPrompt, Value> {
    let Some(parts) = prompt.as_array() else {
        return Err(rpc_error(-32602, "session/prompt requires prompt parts"));
    };
    if parts.len() > MAX_CONTENT_PARTS {
        return Err(rpc_error(
            -32602,
            "session/prompt exceeds the content part count bound",
        ));
    }
    let mut text = String::new();
    let mut attachments = Vec::new();
    let mut aggregate = 0u64;
    for part in parts {
        let part_type = part
            .get("type")
            .and_then(|t| t.as_str())
            .ok_or_else(|| rpc_error(-32602, "prompt part requires type"))?;
        match part_type {
            "text" => {
                let chunk = part
                    .get("text")
                    .and_then(|t| t.as_str())
                    .ok_or_else(|| rpc_error(-32602, "text prompt part requires text"))?;
                text.push_str(chunk);
                add_prompt_bytes(&mut aggregate, chunk.len() as u64)?;
            }
            "image" | "audio" => {
                let staged = staged_inline_part(part, part_type)?;
                add_prompt_bytes(&mut aggregate, staged.payload.len() as u64)?;
                attachments.push(staged);
            }
            "resource" => {
                let staged = staged_resource_part(part)?;
                add_prompt_bytes(&mut aggregate, staged.payload.len() as u64)?;
                attachments.push(staged);
            }
            "video" => {
                return Err(rpc_error(-32602, "unsupported prompt part type `video`"));
            }
            other => {
                return Err(rpc_error(
                    -32602,
                    format!("unsupported prompt part type `{other}`"),
                ));
            }
        }
    }
    if text.is_empty() {
        return Err(rpc_error(-32602, "session/prompt requires a text part"));
    }
    Ok(ParsedPrompt { text, attachments })
}

fn add_prompt_bytes(aggregate: &mut u64, bytes: u64) -> Result<(), Value> {
    *aggregate = aggregate.saturating_add(bytes);
    if *aggregate > MAX_CONTENT_AGGREGATE_BYTES {
        return Err(rpc_error(
            -32602,
            "attachments exceed the content aggregate bound",
        ));
    }
    Ok(())
}

fn staged_inline_part(part: &Value, part_type: &str) -> Result<StagedAttachment, Value> {
    let mime = part
        .get("mimeType")
        .and_then(|v| v.as_str())
        .ok_or_else(|| rpc_error(-32602, format!("{part_type} prompt part requires mimeType")))?;
    let kind = match part_type {
        "image" => ContentPartKind::Image,
        "audio" => ContentPartKind::Audio,
        _ => unreachable!("inline part type"),
    };
    if !content_media_type_ok(kind, mime) {
        return Err(rpc_error(
            -32602,
            format!("unsupported {part_type} media type `{mime}`"),
        ));
    }
    let payload = inline_payload(part, part_type)?;
    Ok(StagedAttachment {
        kind,
        media_type: mime.to_ascii_lowercase(),
        payload,
    })
}

fn staged_resource_part(part: &Value) -> Result<StagedAttachment, Value> {
    let resource = part
        .get("resource")
        .ok_or_else(|| rpc_error(-32602, "resource prompt part requires resource"))?;
    let mime = resource
        .get("mimeType")
        .and_then(|v| v.as_str())
        .ok_or_else(|| rpc_error(-32602, "resource prompt part requires mimeType"))?;
    let kind = resource_kind(mime)?;
    let payload = if let Some(blob) = resource.get("blob").and_then(|v| v.as_str()) {
        decode_base64(blob)?
    } else if let Some(text) = resource.get("text").and_then(|v| v.as_str()) {
        text.as_bytes().to_vec()
    } else if let Some(uri) = resource.get("uri").and_then(|v| v.as_str()) {
        read_host_file(uri)?
    } else {
        return Err(rpc_error(
            -32602,
            "resource prompt part requires blob, text, or a host path uri",
        ));
    };
    if payload.len() as u64 > MAX_CONTENT_PART_BYTES {
        return Err(rpc_error(
            -32602,
            "attachment exceeds the content part size bound",
        ));
    }
    Ok(StagedAttachment {
        kind,
        media_type: mime.to_ascii_lowercase(),
        payload,
    })
}

fn inline_payload(part: &Value, part_type: &str) -> Result<Vec<u8>, Value> {
    if let Some(data) = part.get("data").and_then(|v| v.as_str()) {
        let bytes = decode_base64(data)?;
        if bytes.len() as u64 > MAX_CONTENT_PART_BYTES {
            return Err(rpc_error(
                -32602,
                "attachment exceeds the content part size bound",
            ));
        }
        return Ok(bytes);
    }
    if let Some(uri) = part.get("uri").and_then(|v| v.as_str()) {
        return read_host_file(uri);
    }
    Err(rpc_error(
        -32602,
        format!("{part_type} prompt part requires data or a host path uri"),
    ))
}

fn resource_kind(mime: &str) -> Result<ContentPartKind, Value> {
    let mime = mime.to_ascii_lowercase();
    if content_media_type_ok(ContentPartKind::Document, &mime) {
        Ok(ContentPartKind::Document)
    } else if content_media_type_ok(ContentPartKind::Image, &mime) {
        Ok(ContentPartKind::Image)
    } else if content_media_type_ok(ContentPartKind::Audio, &mime) {
        Ok(ContentPartKind::Audio)
    } else {
        Err(rpc_error(
            -32602,
            format!("unsupported prompt attachment media type `{mime}`"),
        ))
    }
}

fn content_media_type_ok(kind: ContentPartKind, mime: &str) -> bool {
    let mime = mime.trim().to_ascii_lowercase();
    match kind {
        ContentPartKind::Text => matches!(
            mime.as_str(),
            "text/plain" | "text/markdown" | "application/json"
        ),
        ContentPartKind::Image => {
            matches!(
                mime.as_str(),
                "image/png" | "image/jpeg" | "image/gif" | "image/webp"
            )
        }
        ContentPartKind::Audio => {
            matches!(
                mime.as_str(),
                "audio/wav" | "audio/mpeg" | "audio/mp4" | "audio/ogg"
            )
        }
        ContentPartKind::Document => {
            matches!(
                mime.as_str(),
                "application/pdf" | "text/plain" | "text/markdown"
            )
        }
    }
}

fn content_kind_name(kind: ContentPartKind) -> &'static str {
    match kind {
        ContentPartKind::Text => "text",
        ContentPartKind::Image => "image",
        ContentPartKind::Audio => "audio",
        ContentPartKind::Document => "document",
    }
}

fn read_host_file(uri: &str) -> Result<Vec<u8>, Value> {
    let path = Path::new(uri);
    if !path.is_absolute() {
        return Err(rpc_error(
            -32602,
            "attachment uri must be an absolute host path",
        ));
    }
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| rpc_error(-32602, format!("attachment path cannot be read: {error}")))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(rpc_error(-32602, "attachment path must be a regular file"));
    }
    if metadata.len() > MAX_CONTENT_PART_BYTES {
        return Err(rpc_error(
            -32602,
            "attachment exceeds the content part size bound",
        ));
    }
    let bytes = fs::read(path)
        .map_err(|error| rpc_error(-32602, format!("attachment path cannot be read: {error}")))?;
    if bytes.len() as u64 > MAX_CONTENT_PART_BYTES {
        return Err(rpc_error(
            -32602,
            "attachment exceeds the content part size bound",
        ));
    }
    Ok(bytes)
}

fn decode_base64(input: &str) -> Result<Vec<u8>, Value> {
    let compact: String = input.chars().filter(|c| !c.is_whitespace()).collect();
    if compact.is_empty() {
        return Err(rpc_error(-32602, "attachment data is empty"));
    }
    decode_base64_compact(&compact)
        .ok_or_else(|| rpc_error(-32602, "attachment data is not valid base64"))
}

fn decode_base64_compact(input: &str) -> Option<Vec<u8>> {
    fn value(byte: u8) -> Option<u8> {
        match byte {
            b'A'..=b'Z' => Some(byte - b'A'),
            b'a'..=b'z' => Some(byte - b'a' + 26),
            b'0'..=b'9' => Some(byte - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }
    if !input.len().is_multiple_of(4) {
        return None;
    }
    if let Some(pad_at) = input.bytes().position(|b| b == b'=') {
        if !input.as_bytes()[pad_at..].iter().all(|&b| b == b'=') {
            return None;
        }
        let pad = input.len() - pad_at;
        if pad > 2 || pad_at % 4 < 2 {
            return None;
        }
    }
    let mut out = Vec::with_capacity(input.len() / 4 * 3);
    for chunk in input.as_bytes().chunks(4) {
        let pad = chunk.iter().filter(|b| **b == b'=').count();
        if pad > 2 {
            return None;
        }
        let mut sextets = [0u8; 4];
        for (i, byte) in chunk.iter().enumerate() {
            if *byte == b'=' {
                if i < 2 {
                    return None;
                }
                continue;
            }
            sextets[i] = value(*byte)?;
        }
        out.push((sextets[0] << 2) | (sextets[1] >> 4));
        if pad < 2 {
            out.push((sextets[1] << 4) | (sextets[2] >> 2));
        }
        if pad < 1 {
            out.push((sextets[2] << 6) | sextets[3]);
        }
    }
    Some(out)
}

fn prompt_capabilities(model: &dyn ModelPort) -> Value {
    let kinds = model.content_kinds();
    json!({
        "image": kinds.contains(&ContentPartKind::Image),
        "audio": kinds.contains(&ContentPartKind::Audio),
        "embeddedContext": kinds.contains(&ContentPartKind::Document),
    })
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
            let msg = match framing::read_limited(&mut reader, MAX_ACP_FRAME_BYTES).await {
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
struct CapturePermissionClient {
    updates: Mutex<Vec<Value>>,
    permissions: Mutex<Vec<Value>>,
    permission: PermissionOutcome,
}

#[cfg(test)]
#[async_trait]
impl AcpClient for CapturePermissionClient {
    async fn notify(&self, _method: &str, params: Value) -> Result<(), String> {
        self.updates.lock().await.push(params);
        Ok(())
    }

    async fn request(&self, method: &str, params: Value) -> Result<Value, String> {
        if method != "session/request_permission" {
            return Err(format!("unexpected agent request {method}"));
        }
        self.permissions.lock().await.push(params);
        let outcome = match self.permission {
            PermissionOutcome::Allow => "allow",
            PermissionOutcome::Deny => "deny",
            PermissionOutcome::Cancelled => "cancelled",
        };
        Ok(json!({ "outcome": { "outcome": outcome, "optionId": outcome } }))
    }
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

    fn encode_base64(input: &[u8]) -> String {
        const TABLE: &[u8; 64] =
            b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
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

    #[test]
    fn decode_base64_rejects_misplaced_padding() {
        assert_eq!(
            decode_base64_compact(&encode_base64(b"a")).as_deref(),
            Some(b"a".as_slice())
        );
        assert_eq!(
            decode_base64_compact(&encode_base64(b"ab")).as_deref(),
            Some(b"ab".as_slice())
        );
        assert_eq!(
            decode_base64_compact(&encode_base64(b"abc")).as_deref(),
            Some(b"abc".as_slice())
        );
        assert_eq!(decode_base64_compact("YQ=A"), None);
        assert_eq!(decode_base64_compact("YQ==Yg=="), None);
        assert_eq!(decode_base64_compact("===="), None);
    }

    #[test]
    fn acp_stdio_frame_fits_content_aggregate_as_base64() {
        let encoded = (crate::content::MAX_CONTENT_AGGREGATE_BYTES as usize)
            .saturating_mul(4)
            .div_ceil(3);
        assert!(
            MAX_ACP_FRAME_BYTES >= encoded + 64 * 1024,
            "ACP stdio bound {MAX_ACP_FRAME_BYTES} cannot carry {encoded} base64 bytes plus envelope"
        );
        const {
            assert!(MAX_ACP_FRAME_BYTES > framing::MAX_FRAME_BYTES);
        }
    }

    fn scripted_host(dir: &Path, script: &str) -> AcpHost {
        scripted_host_with(dir, script, |_| {})
    }

    fn scripted_host_with(dir: &Path, script: &str, tweak: impl FnOnce(&mut Config)) -> AcpHost {
        let state = StateRoot::new(dir.join("state"));
        let mut config = Config::default();
        config.governance.adapter = "local".into();
        config.model.adapter = "scripted".into();
        config.model.script_json = Some(script.into());
        config.events.adapter = "none".into();
        config.workspace.root = dir.join("ws").to_string_lossy().into();
        tweak(&mut config);
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

    async fn init_and_new_mode(
        host: &AcpHost,
        client: &dyn AcpClient,
        cwd: &Path,
        mode: &str,
        id: u64,
    ) -> Value {
        if id == 1 {
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
        }
        host.handle(
            json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": "session/new",
                "params": { "cwd": cwd, "mcpServers": [], "mode": mode }
            }),
            client,
        )
        .await
        .unwrap()
    }

    fn assert_initialize_speaks_v1(result: &Value) {
        assert_initialize_speaks_v1_with_prompt(result, true, true, true);
    }

    fn assert_initialize_speaks_v1_with_prompt(
        result: &Value,
        image: bool,
        audio: bool,
        embedded: bool,
    ) {
        assert_eq!(result["protocolVersion"], 1);
        assert_eq!(result["agentCapabilities"]["loadSession"], true);
        assert_eq!(
            result["agentCapabilities"]["promptCapabilities"]["image"],
            image
        );
        assert_eq!(
            result["agentCapabilities"]["promptCapabilities"]["audio"],
            audio
        );
        assert_eq!(
            result["agentCapabilities"]["promptCapabilities"]["embeddedContext"],
            embedded
        );
        assert_eq!(result["authMethods"], json!([]));
        assert_eq!(result["agentInfo"]["name"], crate::identity::PRODUCT);
        assert_eq!(result["agentInfo"]["version"], crate::identity::VERSION);
    }

    #[tokio::test]
    async fn initialize_accepts_v1_and_v2_and_always_returns_v1() {
        let dir = tempdir().unwrap();
        let host = scripted_host(dir.path(), r#"[{"content":"hello"}]"#);
        let client = RecordingClient {
            updates: Mutex::new(Vec::new()),
            permission: PermissionOutcome::Allow,
        };
        for (id, offered) in [
            (1, json!(1)),
            (2, json!(2)),
            (3, json!("1")),
            (4, json!("2")),
        ] {
            let resp = host
                .handle(
                    json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "method": "initialize",
                        "params": { "protocolVersion": offered, "capabilities": {} }
                    }),
                    &client,
                )
                .await
                .unwrap();
            assert_initialize_speaks_v1(rpc_ok(&resp));
        }
    }

    #[tokio::test]
    async fn initialize_rejects_unsupported_protocol_version() {
        let dir = tempdir().unwrap();
        let host = scripted_host(dir.path(), r#"[{"content":"hello"}]"#);
        let client = RecordingClient {
            updates: Mutex::new(Vec::new()),
            permission: PermissionOutcome::Allow,
        };
        let resp = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "initialize",
                    "params": { "protocolVersion": 3, "capabilities": {} }
                }),
                &client,
            )
            .await
            .unwrap();
        assert_eq!(resp["error"]["code"], -32602);
        assert!(
            resp["error"]["message"]
                .as_str()
                .unwrap()
                .contains("unsupported protocolVersion 3"),
            "{}",
            resp["error"]["message"]
        );
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
    async fn invalid_mode_mapping_does_not_create_session() {
        let dir = tempdir().unwrap();
        let host = scripted_host_with(dir.path(), r#"[{"content":"hello"}]"#, |config| {
            config.session.modes.insert(
                "low".into(),
                crate::config::SessionModeMapping {
                    tools: Some(vec!["not_a_host_tool".into()]),
                    ..Default::default()
                },
            );
        });
        let client = RecordingClient {
            updates: Mutex::new(Vec::new()),
            permission: PermissionOutcome::Allow,
        };
        let result = init_and_new_mode(&host, &client, dir.path(), "low", 1).await;
        assert_eq!(result["error"]["code"], -32602, "{result}");
        assert!(
            result["error"]["message"]
                .as_str()
                .unwrap()
                .contains("intersect")
        );
        assert!(host.sessions.lock().await.is_empty());
        assert!(host.session_ids_for_cwd(dir.path()).is_empty());
    }

    #[tokio::test]
    async fn invalid_first_prompt_mode_leaves_session_unfrozen_for_retry() {
        let dir = tempdir().unwrap();
        let host = scripted_host_with(dir.path(), r#"[{"content":"hello"}]"#, |config| {
            config.session.modes.insert(
                "low".into(),
                crate::config::SessionModeMapping {
                    tools: Some(vec!["not_a_host_tool".into()]),
                    ..Default::default()
                },
            );
        });
        let client = RecordingClient {
            updates: Mutex::new(Vec::new()),
            permission: PermissionOutcome::Allow,
        };
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let session_id = init_and_new(&host, &client, &cwd).await;
        let prompt = json!({
            "sessionId": session_id,
            "mode": "low",
            "prompt": [{"type": "text", "text": "hi"}]
        });
        let result = host.session_prompt(&prompt, &client).await.unwrap_err();
        assert_eq!(result["code"], -32602, "{result}");
        {
            let sessions = host.sessions.lock().await;
            let live = sessions.get(&session_id).unwrap();
            assert!(!live.mode_frozen);
            assert!(live.mode.is_none());
            assert!(live.run_id.is_none());
        }
        let persisted = host.load_persisted(&session_id).unwrap();
        assert!(!persisted.mode_is_frozen());
        assert!(persisted.mode.is_none());
        let result = host
            .session_prompt(
                &json!({
                    "sessionId": session_id,
                    "mode": "high",
                    "prompt": [{"type": "text", "text": "hi"}]
                }),
                &client,
            )
            .await
            .unwrap();
        assert_eq!(result["stopReason"], "end_turn");
        assert_eq!(
            host.load_persisted(&session_id).unwrap().mode.as_deref(),
            Some("high")
        );
    }

    #[tokio::test]
    async fn session_new_modes_select_different_models_and_freeze() {
        let dir = tempdir().unwrap();
        let host = scripted_host_with(dir.path(), r#"[{"content":"hello"}]"#, |config| {
            config.model.model = "base-model".into();
            config.session.modes.insert(
                "low".into(),
                crate::config::SessionModeMapping {
                    model: Some("model-low".into()),
                    effort: Some("low".into()),
                    tools: Some(vec!["read_file".into(), "report".into()]),
                    prompt: None,
                },
            );
            config.session.modes.insert(
                "high".into(),
                crate::config::SessionModeMapping {
                    model: Some("model-high".into()),
                    effort: Some("high".into()),
                    tools: Some(vec!["read_file".into(), "report".into()]),
                    prompt: Some("be thorough".into()),
                },
            );
        });
        let client = RecordingClient {
            updates: Mutex::new(Vec::new()),
            permission: PermissionOutcome::Allow,
        };
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();

        let low = init_and_new_mode(&host, &client, &cwd, "low", 1).await;
        let low_id = rpc_ok(&low)["sessionId"].as_str().unwrap().to_string();
        assert_eq!(rpc_ok(&low)["mode"], "low");
        let high = init_and_new_mode(&host, &client, &cwd, "high", 2).await;
        let high_id = rpc_ok(&high)["sessionId"].as_str().unwrap().to_string();
        assert_eq!(rpc_ok(&high)["mode"], "high");

        let low_prompt = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "session/prompt",
                    "params": {
                        "sessionId": low_id,
                        "prompt": [{"type":"text","text":"hi"}]
                    }
                }),
                &client,
            )
            .await
            .unwrap();
        assert_eq!(rpc_ok(&low_prompt)["stopReason"], "end_turn");
        let high_prompt = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 4,
                    "method": "session/prompt",
                    "params": {
                        "sessionId": high_id,
                        "prompt": [{"type":"text","text":"hi"}]
                    }
                }),
                &client,
            )
            .await
            .unwrap();
        assert_eq!(rpc_ok(&high_prompt)["stopReason"], "end_turn");

        let updates = client.updates.lock().await;
        let modes: Vec<_> = updates
            .iter()
            .filter(|u| u["update"]["sessionUpdate"] == "session_mode")
            .cloned()
            .collect();
        assert_eq!(modes.len(), 2, "{updates:?}");
        assert_eq!(modes[0]["update"]["mode"], "low");
        assert_eq!(modes[0]["update"]["model"], "model-low");
        assert_eq!(modes[0]["update"]["effort"], "low");
        assert_eq!(modes[1]["update"]["mode"], "high");
        assert_eq!(modes[1]["update"]["model"], "model-high");
        assert_ne!(modes[0]["update"]["model"], modes[1]["update"]["model"]);
        drop(updates);

        let switched = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 5,
                    "method": "session/prompt",
                    "params": {
                        "sessionId": low_id,
                        "mode": "high",
                        "prompt": [{"type":"text","text":"nope"}]
                    }
                }),
                &client,
            )
            .await
            .unwrap();
        assert_eq!(switched["error"]["code"], -32602);
        assert!(
            switched["error"]["message"]
                .as_str()
                .unwrap()
                .contains("frozen"),
            "{}",
            switched["error"]["message"]
        );
    }

    #[tokio::test]
    async fn omitted_session_mode_matches_current_spawn() {
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
                        "prompt": [{"type":"text","text":"hi"}]
                    }
                }),
                &client,
            )
            .await
            .unwrap();
        assert_eq!(rpc_ok(&prompt)["stopReason"], "end_turn");
        let updates = client.updates.lock().await;
        assert!(
            updates
                .iter()
                .all(|u| u["update"]["sessionUpdate"] != "session_mode"),
            "{updates:?}"
        );
        let unknown = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 4,
                    "method": "session/new",
                    "params": { "cwd": cwd, "mcpServers": [], "mode": "rush" }
                }),
                &client,
            )
            .await
            .unwrap();
        assert_eq!(unknown["error"]["code"], -32602);
        assert!(
            unknown["error"]["message"]
                .as_str()
                .unwrap()
                .contains("unknown session mode"),
            "{}",
            unknown["error"]["message"]
        );
        let late_mode = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 5,
                    "method": "session/prompt",
                    "params": {
                        "sessionId": session_id,
                        "mode": "high",
                        "prompt": [{"type":"text","text":"nope"}]
                    }
                }),
                &client,
            )
            .await
            .unwrap();
        assert_eq!(late_mode["error"]["code"], -32602);
        assert!(
            late_mode["error"]["message"]
                .as_str()
                .unwrap()
                .contains("frozen"),
            "{}",
            late_mode["error"]["message"]
        );
    }

    #[tokio::test]
    async fn session_prompt_passes_supported_image_and_pdf() {
        let dir = tempdir().unwrap();
        let host = scripted_host_with(dir.path(), r#"[{"content":"saw-image"}]"#, |config| {
            config.events.adapter = "jsonl".into();
        });
        let client = RecordingClient {
            updates: Mutex::new(Vec::new()),
            permission: PermissionOutcome::Allow,
        };
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let session_id = init_and_new(&host, &client, &cwd).await;
        let png = b"\x89PNG UNIQUE-ACP-401-IMAGE";
        let pdf = b"%PDF UNIQUE-ACP-401-DOCUMENT";
        let prompt = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "session/prompt",
                    "params": {
                        "sessionId": session_id,
                        "prompt": [
                            {"type":"text","text":"inspect"},
                            {"type":"image","mimeType":"image/png","data": encode_base64(png)},
                            {
                                "type":"resource",
                                "resource": {
                                    "uri": "attachment://doc.pdf",
                                    "mimeType": "application/pdf",
                                    "blob": encode_base64(pdf)
                                }
                            }
                        ]
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
            texts.contains(&"saw-image"),
            "expected content turn text, got {texts:?}"
        );
        drop(updates);

        let state = dir.path().join("state");
        assert_no_payload_bytes(&state, png);
        assert_no_payload_bytes(&cwd, png);
        assert_no_payload_bytes(&state, pdf);
        assert_no_payload_bytes(&cwd, pdf);

        let runs = host.harness.state.runs_dir();
        let run_id = std::fs::read_dir(&runs)
            .unwrap()
            .filter_map(|entry| entry.ok())
            .find(|entry| entry.path().is_dir())
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .expect("content run directory");
        let checkpoint = Checkpoint::load(&runs, &run_id).unwrap();
        assert!(
            checkpoint.content.is_some(),
            "attachment prompt must use a content run"
        );
    }

    #[tokio::test]
    async fn session_load_replays_content_assistant_text() {
        let dir = tempdir().unwrap();
        let host = scripted_host(dir.path(), r#"[{"content":"saw-image"}]"#);
        let client = RecordingClient {
            updates: Mutex::new(Vec::new()),
            permission: PermissionOutcome::Allow,
        };
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let session_id = init_and_new(&host, &client, &cwd).await;
        let png = b"\x89PNG UNIQUE-ACP-401-LOAD";
        let prompt = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "session/prompt",
                    "params": {
                        "sessionId": session_id,
                        "prompt": [
                            {"type":"text","text":"inspect"},
                            {"type":"image","mimeType":"image/png","data": encode_base64(png)}
                        ]
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
        let texts: Vec<&str> = updates
            .iter()
            .filter(|update| update["update"]["sessionUpdate"] == "agent_message_chunk")
            .filter_map(|update| update["update"]["content"]["text"].as_str())
            .collect();
        assert!(
            texts.contains(&"saw-image"),
            "load must replay content assistant text, got {texts:?}"
        );
        assert!(
            updates.iter().any(|update| {
                update["update"]["sessionUpdate"] == "user_message_chunk"
                    && update["update"]["content"]["text"] == "inspect"
            }),
            "load must replay the user text: {updates:?}"
        );
    }

    #[tokio::test]
    async fn session_load_replays_content_tool_arguments() {
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
        let png = b"\x89PNG UNIQUE-ACP-401-TOOL-ARGS";
        let prompt = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "session/prompt",
                    "params": {
                        "sessionId": session_id,
                        "prompt": [
                            {"type":"text","text":"write"},
                            {"type":"image","mimeType":"image/png","data": encode_base64(png)}
                        ]
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
        let raw = updates
            .iter()
            .find(|update| {
                update["update"]["sessionUpdate"] == "tool_call"
                    && update["update"]["title"] == "write_file"
            })
            .map(|update| &update["update"]["rawInput"]);
        assert_eq!(
            raw.and_then(|value| value.get("path"))
                .and_then(|v| v.as_str()),
            Some("ok.txt"),
            "load must replay resolved tool arguments, got {updates:?}"
        );
        assert!(
            raw.is_some_and(|value| value.get("shikigami_content_arguments_part_id").is_none()),
            "load must not replay the content argument pointer: {raw:?}"
        );
    }

    #[tokio::test]
    async fn session_prompt_live_content_tool_arguments() {
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
        let png = b"\x89PNG UNIQUE-ACP-401-LIVE-TOOL";
        let prompt = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "session/prompt",
                    "params": {
                        "sessionId": session_id,
                        "prompt": [
                            {"type":"text","text":"write"},
                            {"type":"image","mimeType":"image/png","data": encode_base64(png)}
                        ]
                    }
                }),
                &client,
            )
            .await
            .unwrap();
        assert_eq!(rpc_ok(&prompt)["stopReason"], "end_turn", "{prompt}");
        let updates = client.updates.lock().await;
        let raw = updates
            .iter()
            .find(|update| {
                update["update"]["sessionUpdate"] == "tool_call"
                    && update["update"]["title"] == "write_file"
            })
            .map(|update| &update["update"]["rawInput"]);
        assert_eq!(
            raw.and_then(|value| value.get("path"))
                .and_then(|v| v.as_str()),
            Some("ok.txt"),
            "live tool_call must show resolved arguments, got {updates:?}"
        );
        assert!(
            raw.is_some_and(|value| value.get("shikigami_content_arguments_part_id").is_none()),
            "live tool_call must not show the content argument pointer: {raw:?}"
        );
        let ended = updates
            .iter()
            .find(|update| update["update"]["sessionUpdate"] == "tool_call_update");
        let detail = ended
            .and_then(|update| update["update"]["content"][0]["content"]["text"].as_str())
            .unwrap_or("");
        assert!(
            ended.is_some(),
            "live tool_call_update missing, got {updates:?}"
        );
        assert!(
            !detail.starts_with("bounded_content "),
            "live tool result must not stay projected, got {ended:?}"
        );
        assert!(
            !detail.is_empty(),
            "live tool result must restore sidecar text, got {ended:?}"
        );
    }

    #[tokio::test]
    async fn session_prompt_rejects_misplaced_base64_padding() {
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
                        "prompt": [
                            {"type":"text","text":"inspect"},
                            {"type":"image","mimeType":"image/png","data": "YQ=A"}
                        ]
                    }
                }),
                &client,
            )
            .await
            .unwrap();
        assert_eq!(prompt["error"]["code"], -32602, "{prompt}");
        assert!(
            prompt["error"]["message"]
                .as_str()
                .unwrap()
                .contains("valid base64"),
            "{}",
            prompt["error"]["message"]
        );
    }

    #[tokio::test]
    async fn content_ask_resume_resolves_tool_arguments() {
        let dir = tempdir().unwrap();
        let host = scripted_host(
            dir.path(),
            r#"[
              {"tool_calls":[{"name":"write_file","args_json":"{\"path\":\"ok.txt\",\"content\":\"hi\\n\"}"}]},
              {"content":"wrote it"}
            ]"#,
        );
        let malformed = MalformedPermissionClient {
            updates: Mutex::new(Vec::new()),
        };
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let session_id = init_and_new(&host, &malformed, &cwd).await;
        let png = b"\x89PNG UNIQUE-ACP-401-ASK-ARGS";
        let first = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "session/prompt",
                    "params": {
                        "sessionId": session_id,
                        "prompt": [
                            {"type":"text","text":"write"},
                            {"type":"image","mimeType":"image/png","data": encode_base64(png)}
                        ]
                    }
                }),
                &malformed,
            )
            .await
            .unwrap();
        assert_eq!(first["error"]["code"], -32602, "{first}");
        let retry = CapturePermissionClient {
            updates: Mutex::new(Vec::new()),
            permissions: Mutex::new(Vec::new()),
            permission: PermissionOutcome::Allow,
        };
        let second = host
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
                &retry,
            )
            .await
            .unwrap();
        assert_eq!(rpc_ok(&second)["stopReason"], "end_turn", "{second}");
        let permissions = retry.permissions.lock().await;
        let raw = permissions
            .first()
            .map(|params| &params["toolCall"]["rawInput"]);
        assert_eq!(
            raw.and_then(|value| value.get("path"))
                .and_then(|v| v.as_str()),
            Some("ok.txt"),
            "resumed ask must show resolved arguments, got {permissions:?}"
        );
        assert!(
            raw.is_some_and(|value| value.get("shikigami_content_arguments_part_id").is_none()),
            "resumed ask must not show the content argument pointer: {raw:?}"
        );
    }

    #[tokio::test]
    async fn failed_attachment_prompt_does_not_keep_content_state() {
        let dir = tempdir().unwrap();
        let host = scripted_host(dir.path(), r#"[{"content":"hello"}]"#);
        let client = RecordingClient {
            updates: Mutex::new(Vec::new()),
            permission: PermissionOutcome::Allow,
        };
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let session_id = init_and_new(&host, &client, &cwd).await;
        let blob = encode_base64(&vec![0u8; 8 * 1024 * 1024]);
        let failed = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "session/prompt",
                    "params": {
                        "sessionId": session_id,
                        "prompt": [
                            {"type":"text","text":"inspect"},
                            {"type":"image","mimeType":"image/png","data": blob},
                            {"type":"image","mimeType":"image/png","data": blob}
                        ]
                    }
                }),
                &client,
            )
            .await
            .unwrap();
        assert_eq!(failed["error"]["code"], -32602, "{failed}");
        assert!(
            failed["error"]["message"]
                .as_str()
                .is_some_and(|message| message.contains("aggregate")),
            "{failed}"
        );
        let sessions = host.sessions.lock().await;
        let live = sessions.get(&session_id).expect("session");
        assert!(live.run_id.is_none(), "{:?}", live.run_id);
        assert!(
            live.content.is_none(),
            "failed attach must drop content state"
        );
        drop(sessions);
        let follow = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 4,
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
        assert_eq!(rpc_ok(&follow)["stopReason"], "end_turn");
    }

    #[tokio::test]
    async fn session_load_fails_closed_without_content_store() {
        let dir = tempdir().unwrap();
        let host = scripted_host(dir.path(), r#"[{"content":"saw-image"}]"#);
        let client = RecordingClient {
            updates: Mutex::new(Vec::new()),
            permission: PermissionOutcome::Allow,
        };
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let session_id = init_and_new(&host, &client, &cwd).await;
        let png = b"\x89PNG UNIQUE-ACP-401-LOST";
        let prompt = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "session/prompt",
                    "params": {
                        "sessionId": session_id,
                        "prompt": [
                            {"type":"text","text":"inspect"},
                            {"type":"image","mimeType":"image/png","data": encode_base64(png)}
                        ]
                    }
                }),
                &client,
            )
            .await
            .unwrap();
        assert_eq!(rpc_ok(&prompt)["stopReason"], "end_turn");
        host.sessions.lock().await.remove(&session_id);
        let loaded = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 10,
                    "method": "session/load",
                    "params": { "sessionId": session_id, "cwd": cwd, "mcpServers": [] }
                }),
                &client,
            )
            .await
            .unwrap();
        assert_eq!(loaded["error"]["code"], -32603);
        assert!(
            loaded["error"]["message"]
                .as_str()
                .unwrap()
                .contains("no longer in session memory"),
            "{}",
            loaded["error"]["message"]
        );
    }

    #[tokio::test]
    async fn session_prompt_content_follow_up_reaches_the_model() {
        let dir = tempdir().unwrap();
        let host = scripted_host(
            dir.path(),
            r#"[{"content":"saw-image"},{"content":"saw-follow-up"}]"#,
        );
        let client = RecordingClient {
            updates: Mutex::new(Vec::new()),
            permission: PermissionOutcome::Allow,
        };
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let session_id = init_and_new(&host, &client, &cwd).await;
        let png = b"\x89PNG UNIQUE-ACP-401-FOLLOW";
        let first = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "session/prompt",
                    "params": {
                        "sessionId": session_id,
                        "prompt": [
                            {"type":"text","text":"inspect"},
                            {"type":"image","mimeType":"image/png","data": encode_base64(png)}
                        ]
                    }
                }),
                &client,
            )
            .await
            .unwrap();
        assert_eq!(rpc_ok(&first)["stopReason"], "end_turn");
        client.updates.lock().await.clear();
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
        let texts: Vec<&str> = updates
            .iter()
            .filter(|u| u["update"]["sessionUpdate"] == "agent_message_chunk")
            .filter_map(|u| u["update"]["content"]["text"].as_str())
            .collect();
        assert!(
            texts.contains(&"saw-follow-up"),
            "expected follow-up content turn, got {texts:?}"
        );
        assert!(
            !texts.contains(&"saw-image"),
            "staged assistant must not replay, got {texts:?}"
        );
    }

    #[tokio::test]
    async fn session_prompt_content_follow_up_after_report() {
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
        let png = b"\x89PNG UNIQUE-ACP-401-REPORT";
        let first = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "session/prompt",
                    "params": {
                        "sessionId": session_id,
                        "prompt": [
                            {"type":"text","text":"go"},
                            {"type":"image","mimeType":"image/png","data": encode_base64(png)}
                        ]
                    }
                }),
                &client,
            )
            .await
            .unwrap();
        assert_eq!(rpc_ok(&first)["stopReason"], "end_turn", "{first}");
        client.updates.lock().await.clear();
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
        assert_eq!(rpc_ok(&follow)["stopReason"], "end_turn", "{follow}");
        let updates = client.updates.lock().await;
        let texts: Vec<&str> = updates
            .iter()
            .filter(|update| update["update"]["sessionUpdate"] == "agent_message_chunk")
            .filter_map(|update| update["update"]["content"]["text"].as_str())
            .collect();
        assert!(
            texts.contains(&"again"),
            "report must not finalize an attachment session, got {texts:?} follow={follow}"
        );
    }

    #[tokio::test]
    async fn session_prompt_fails_closed_on_unsupported_attachment() {
        let dir = tempdir().unwrap();
        let host = scripted_host(dir.path(), r#"[{"content":"hello"}]"#);
        let client = RecordingClient {
            updates: Mutex::new(Vec::new()),
            permission: PermissionOutcome::Allow,
        };
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let session_id = init_and_new(&host, &client, &cwd).await;
        let video = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "session/prompt",
                    "params": {
                        "sessionId": session_id,
                        "prompt": [
                            {"type":"text","text":"see this"},
                            {"type":"video","mimeType":"video/mp4","data": encode_base64(b"ftyp")}
                        ]
                    }
                }),
                &client,
            )
            .await
            .unwrap();
        assert_eq!(video["error"]["code"], -32602);
        assert!(
            video["error"]["message"]
                .as_str()
                .unwrap()
                .contains("unsupported prompt part type `video`"),
            "{}",
            video["error"]["message"]
        );

        let unknown = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 4,
                    "method": "session/prompt",
                    "params": {
                        "sessionId": session_id,
                        "prompt": [
                            {"type":"text","text":"see this"},
                            {"type":"widget","data":"nope"}
                        ]
                    }
                }),
                &client,
            )
            .await
            .unwrap();
        assert_eq!(unknown["error"]["code"], -32602);
        assert!(
            unknown["error"]["message"]
                .as_str()
                .unwrap()
                .contains("unsupported prompt part type `widget`"),
            "{}",
            unknown["error"]["message"]
        );
    }

    #[tokio::test]
    async fn omitted_attachments_match_text_host() {
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
                        "prompt": [{"type":"text","text":"hi"}]
                    }
                }),
                &client,
            )
            .await
            .unwrap();
        assert_eq!(rpc_ok(&prompt)["stopReason"], "end_turn");
        let runs = host.harness.state.runs_dir();
        let run_id = std::fs::read_dir(&runs)
            .unwrap()
            .filter_map(|entry| entry.ok())
            .find(|entry| entry.path().is_dir())
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .expect("text run directory");
        let checkpoint = Checkpoint::load(&runs, &run_id).unwrap();
        assert!(
            checkpoint.content.is_none(),
            "text-only prompt must keep the ordinary run path"
        );
    }

    #[tokio::test]
    async fn initialize_plane_adapter_keeps_prompt_capabilities_false() {
        let dir = tempdir().unwrap();
        let state = StateRoot::new(dir.path().join("state"));
        let mut config = Config::default();
        config.governance.adapter = "local".into();
        config.model.adapter = "plane".into();
        config.events.adapter = "none".into();
        let host = AcpHost::new(Harness::from_config(config, state).unwrap());
        let client = RecordingClient {
            updates: Mutex::new(Vec::new()),
            permission: PermissionOutcome::Allow,
        };
        let init = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "initialize",
                    "params": { "protocolVersion": 1, "capabilities": {} }
                }),
                &client,
            )
            .await
            .unwrap();
        assert_initialize_speaks_v1_with_prompt(rpc_ok(&init), false, false, false);

        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let created = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "session/new",
                    "params": { "cwd": cwd, "mcpServers": [] }
                }),
                &client,
            )
            .await
            .unwrap();
        let session_id = rpc_ok(&created)["sessionId"].as_str().unwrap().to_string();
        let denied = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "session/prompt",
                    "params": {
                        "sessionId": session_id,
                        "prompt": [
                            {"type":"text","text":"inspect"},
                            {"type":"image","mimeType":"image/png","data": encode_base64(b"\x89PNG")}
                        ]
                    }
                }),
                &client,
            )
            .await
            .unwrap();
        assert_eq!(denied["error"]["code"], -32602);
        assert!(
            denied["error"]["message"]
                .as_str()
                .unwrap()
                .contains("cannot read image"),
            "{}",
            denied["error"]["message"]
        );
    }

    fn http_acp_host(dir: &std::path::Path) -> AcpHost {
        let env = "SHIKIGAMI_ACP_HTTP_TEST_KEY";
        // SAFETY: unique env name for this process; tests do not unset it.
        unsafe {
            std::env::set_var(env, "test-key");
        }
        let state = StateRoot::new(dir.join("state"));
        let mut config = Config::default();
        config.governance.adapter = "local".into();
        config.model.adapter = "http".into();
        config.model.api_key_env = env.into();
        config.model.base_url = Some("https://example.invalid/v1".into());
        config.events.adapter = "none".into();
        config.workspace.root = dir.join("ws").to_string_lossy().into();
        AcpHost::new(Harness::from_config(config, state).unwrap())
    }

    #[tokio::test]
    async fn initialize_http_adapter_advertises_image_and_document() {
        let dir = tempdir().unwrap();
        let host = http_acp_host(dir.path());
        let client = RecordingClient {
            updates: Mutex::new(Vec::new()),
            permission: PermissionOutcome::Allow,
        };
        let init = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "initialize",
                    "params": { "protocolVersion": 1, "capabilities": {} }
                }),
                &client,
            )
            .await
            .unwrap();
        assert_initialize_speaks_v1_with_prompt(rpc_ok(&init), true, false, true);

        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let created = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "session/new",
                    "params": { "cwd": cwd, "mcpServers": [] }
                }),
                &client,
            )
            .await
            .unwrap();
        let session_id = rpc_ok(&created)["sessionId"].as_str().unwrap().to_string();
        let denied = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "session/prompt",
                    "params": {
                        "sessionId": session_id,
                        "prompt": [
                            {"type":"text","text":"inspect"},
                            {"type":"audio","mimeType":"audio/wav","data": encode_base64(b"RIFF")}
                        ]
                    }
                }),
                &client,
            )
            .await
            .unwrap();
        assert_eq!(denied["error"]["code"], -32602);
        assert!(
            denied["error"]["message"]
                .as_str()
                .unwrap()
                .contains("cannot read audio"),
            "{}",
            denied["error"]["message"]
        );
    }

    fn assert_no_payload_bytes(root: &Path, needle: &[u8]) {
        fn walk(path: &Path, needle: &[u8]) {
            if path.is_dir() {
                let Ok(entries) = std::fs::read_dir(path) else {
                    return;
                };
                for entry in entries.filter_map(|entry| entry.ok()) {
                    walk(&entry.path(), needle);
                }
                return;
            }
            let Ok(bytes) = std::fs::read(path) else {
                return;
            };
            assert!(
                !bytes.windows(needle.len()).any(|window| window == needle),
                "payload bytes leaked into {}",
                path.display()
            );
        }
        walk(root, needle);
    }

    #[tokio::test]
    async fn session_compact_shrinks_checkpoint_messages() {
        let dir = tempdir().unwrap();
        let host = scripted_host(dir.path(), r#"[{"content":"hello"}]"#);
        let client = RecordingClient {
            updates: Mutex::new(Vec::new()),
            permission: PermissionOutcome::Allow,
        };
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let session_id = init_and_new(&host, &client, &cwd).await;
        let empty = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 8,
                    "method": "session/compact",
                    "params": { "sessionId": session_id }
                }),
                &client,
            )
            .await
            .unwrap();
        assert_eq!(rpc_ok(&empty)["before"], 0);
        assert_eq!(rpc_ok(&empty)["after"], 0);

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

        let persisted: PersistedSession = serde_json::from_slice(
            &std::fs::read(
                dir.path()
                    .join("state/acp-sessions")
                    .join(format!("{session_id}.json")),
            )
            .unwrap(),
        )
        .unwrap();
        let run_id = persisted.run_id.expect("run after prompt");
        let runs = dir.path().join("state/runs");
        let mut checkpoint = Checkpoint::load(&runs, &run_id).unwrap();
        checkpoint.messages = (0..20)
            .map(|i| ChatMessage {
                role: if i == 0 { "user" } else { "assistant" }.into(),
                content: format!("m{i}"),
                tool_call_id: String::new(),
                tool_calls: vec![],
            })
            .collect();
        checkpoint.save(&runs).unwrap();

        let compacted = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 9,
                    "method": "session/compact",
                    "params": { "sessionId": session_id }
                }),
                &client,
            )
            .await
            .unwrap();
        let before = rpc_ok(&compacted)["before"].as_u64().unwrap();
        let after = rpc_ok(&compacted)["after"].as_u64().unwrap();
        assert_eq!(before, 20);
        assert!(after < before, "before={before} after={after}");
        let loaded = Checkpoint::load(&runs, &run_id).unwrap();
        assert_eq!(loaded.messages.len() as u64, after);
        assert!(loaded.messages[1].content.contains("compacted"));
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
            tools_mode: String::new(),
            tools_enabled: Vec::new(),
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

    #[test]
    fn run_and_forward_does_not_poll_on_a_timer() {
        let src = include_str!("acp.rs");
        let start = src
            .find("async fn run_and_forward(")
            .expect("run_and_forward");
        let body = src[start..]
            .split("\nfn ")
            .next()
            .expect("run_and_forward body");
        assert!(
            !body.contains("from_millis(20)"),
            "run_and_forward must wait on events, not a 20ms poll"
        );
        assert!(
            !body.contains("time::sleep"),
            "run_and_forward must not sleep-poll idle gaps"
        );
    }

    #[test]
    fn assistant_visible_text_skips_tool_argument_descriptors() {
        let parts = vec![
            ContentPartDescriptor {
                part_id: "t".into(),
                kind: ContentPartKind::Text,
                media_type: "text/plain".into(),
                byte_length: 5,
                sha256_digest: "sha256:x".into(),
                reference: "acp-t".into(),
                provenance: ContentProvenanceV1 {
                    source: "model".into(),
                    source_id: "turn-1".into(),
                    source_version: "v1".into(),
                    observed_at_ms: 1,
                },
                disclosure_state: ContentDisclosureState::Accepted,
                disclosure_reason: String::new(),
            },
            ContentPartDescriptor {
                part_id: "args".into(),
                kind: ContentPartKind::Text,
                media_type: "text/plain".into(),
                byte_length: 16,
                sha256_digest: "sha256:y".into(),
                reference: "acp-args".into(),
                provenance: ContentProvenanceV1 {
                    source: "model-tool-arguments".into(),
                    source_id: "turn-1-call-0".into(),
                    source_version: "v1".into(),
                    observed_at_ms: 1,
                },
                disclosure_state: ContentDisclosureState::Accepted,
                disclosure_reason: String::new(),
            },
        ];
        let text = assistant_visible_text(&parts, |reference| match reference {
            "acp-t" => Some("hello".into()),
            "acp-args" => Some(r#"{"path":"secret"}"#.into()),
            _ => None,
        });
        assert_eq!(text, "hello");
    }

    #[test]
    fn fill_assistant_content_skips_checkpoint_when_batch_has_no_chunks() {
        apply_assistant_content(
            &mut [],
            || -> Result<Checkpoint, ()> {
                panic!("empty batch must not load a checkpoint");
            },
            |_| None,
        );
        let mut tool_only = vec![json!({
            "sessionId": "sess",
            "update": {
                "sessionUpdate": "tool_call",
                "toolCallId": "tool-1-0",
                "title": "bash",
                "kind": "other",
                "status": "pending"
            }
        })];
        apply_assistant_content(
            &mut tool_only,
            || -> Result<Checkpoint, ()> {
                panic!("tool-only batch must not load a checkpoint");
            },
            |_| None,
        );
        assert_eq!(tool_only[0]["update"]["sessionUpdate"], "tool_call");

        let dir = tempdir().unwrap();
        let host = scripted_host(dir.path(), r#"[{"content":"hello"}]"#);
        let runs = host.harness.state.runs_dir();
        let mut checkpoint = parked_ask_checkpoint(&runs, "run-1");
        checkpoint.messages[1].content = "from-disk".into();
        checkpoint.messages[1].tool_calls.clear();
        std::fs::create_dir_all(&checkpoint.workspace).unwrap();
        checkpoint.save(&runs).unwrap();

        let mut empty: [Value; 0] = [];
        fill_assistant_content(&mut empty, &host.harness, "run-1", None);

        let mut chunk = vec![json!({
            "sessionId": "sess",
            "update": {
                "sessionUpdate": "agent_message_chunk",
                "content": { "type": "text", "text": "preview" },
                "messageId": "turn-1"
            }
        })];
        fill_assistant_content(&mut chunk, &host.harness, "run-1", None);
        assert_eq!(chunk[0]["update"]["content"]["text"], "from-disk");
    }

    #[test]
    fn burst_tool_events_then_model_chunk_stay_ordered_and_bounded() {
        let (sink, mut rx) = AsyncChannelSink::bounded(8);
        sink.emit(HarnessEvent::ToolStart {
            name: "write_file".into(),
            args_json: r#"{"path":"a.txt"}"#.into(),
            run_id: "run-1".into(),
            turn: 1,
            call_id: "tool-1-0".into(),
        });
        sink.emit(HarnessEvent::ToolEnd {
            name: "write_file".into(),
            ok: true,
            detail: "wrote a".into(),
            run_id: "run-1".into(),
            turn: 1,
            call_id: "tool-1-0".into(),
        });
        sink.emit(HarnessEvent::ToolStart {
            name: "write_file".into(),
            args_json: r#"{"path":"b.txt"}"#.into(),
            run_id: "run-1".into(),
            turn: 1,
            call_id: "tool-1-1".into(),
        });
        sink.emit(HarnessEvent::ToolEnd {
            name: "write_file".into(),
            ok: true,
            detail: "wrote b".into(),
            run_id: "run-1".into(),
            turn: 1,
            call_id: "tool-1-1".into(),
        });
        sink.emit(HarnessEvent::ModelTurn {
            turn: 2,
            content_preview: "done".into(),
        });
        let (updates, run_id) = drain_updates("sess", &mut rx);
        assert_eq!(run_id.as_deref(), Some("run-1"));
        let kinds: Vec<&str> = updates
            .iter()
            .map(|update| update["update"]["sessionUpdate"].as_str().unwrap_or(""))
            .collect();
        assert_eq!(
            kinds,
            [
                "tool_call",
                "tool_call_update",
                "tool_call",
                "tool_call_update",
                "agent_message_chunk"
            ]
        );
        assert_eq!(updates[0]["update"]["toolCallId"], "tool-1-0");
        assert_eq!(updates[2]["update"]["toolCallId"], "tool-1-1");
        assert_eq!(updates[4]["update"]["content"]["text"], "done");

        let capacity = 4;
        let (sink, mut rx) = AsyncChannelSink::bounded(capacity);
        for turn in 0..20u32 {
            sink.emit(HarnessEvent::ModelTurn {
                turn,
                content_preview: format!("t{turn}"),
            });
        }
        let (overflowed, _) = drain_updates("sess", &mut rx);
        assert!(
            overflowed.len() <= capacity,
            "forwarder channel grew to {}",
            overflowed.len()
        );
        assert!(
            overflowed
                .iter()
                .all(|update| update["update"]["sessionUpdate"] == "agent_message_chunk"),
            "{overflowed:?}"
        );
        assert_eq!(
            overflowed
                .last()
                .and_then(|u| u["update"]["content"]["text"].as_str()),
            Some("t19"),
            "{overflowed:?}"
        );
    }
}
