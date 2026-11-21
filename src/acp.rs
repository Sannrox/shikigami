//! ACP process host: newline-delimited JSON-RPC session over [`crate::Harness`].
//!
//! Evolving surface, same rank as `shikigami mcp`. Not freeze-core.
//! See [ADR 0014](../docs/decisions/0014-usable-guest-hosts.md).

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
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
use crate::governance::{AvailableModel, GovernanceError};
use crate::harness::{Harness, HarnessError};
use crate::identity::{PRODUCT, VERSION};
use crate::mcp::framing;
use crate::model::{ChatMessage, ModelError, ModelPort, TokenUsage};
use crate::run::{
    AskDecision, ParkInfo, ParkKind, PlanDecision, RunError, RunRequest, RunTermination,
    compact_messages_bounded,
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
    /// Clone for host tasks that outlive a request (nested child status).
    fn clone_arc(&self) -> Option<Arc<dyn AcpClient>> {
        None
    }
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
    /// Model chosen with `session/set_model`; absent keeps the configured one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    model: Option<String>,
}

impl PersistedSession {
    fn mode_is_frozen(&self) -> bool {
        self.mode_frozen || self.mode.is_some() || self.run_id.is_some()
    }
}

/// A persisted session as `--continue` and `/resume` show it.
#[derive(Debug, Clone)]
pub struct SessionSummary {
    pub session_id: String,
    /// Last checkpoint write: when the session last did work.
    pub last_active: std::time::SystemTime,
    /// User prompts left in the run's history (compaction drops older ones).
    pub prompts: usize,
    pub first_prompt: Option<String>,
    /// Another process holds the session (see [`AcpHost::hold_session`]).
    pub in_use: bool,
}

struct SessionListRow {
    cwd: String,
    summary: SessionSummary,
}

impl SessionSummary {
    fn to_list_item(&self, cwd: &str) -> Value {
        let mut item = json!({
            "sessionId": self.session_id,
            "cwd": cwd,
            "updatedAt": rfc3339_utc(self.last_active),
            "_meta": {
                "shikigami": {
                    "inUse": self.in_use,
                    "prompts": self.prompts,
                }
            }
        });
        if let Some(title) = self.first_prompt.as_deref().filter(|text| !text.is_empty()) {
            item["title"] = json!(title);
        }
        item
    }

    /// Parse a `session/list` result into summaries (most recently active first).
    pub fn from_list(result: &Value) -> Result<Vec<Self>, String> {
        let items = result
            .get("sessions")
            .and_then(Value::as_array)
            .ok_or_else(|| "session/list did not return sessions".to_string())?;
        items
            .iter()
            .map(|item| {
                Self::from_list_item(item)
                    .ok_or_else(|| "session/list item missing fields".to_string())
            })
            .collect()
    }

    fn from_list_item(item: &Value) -> Option<Self> {
        let session_id = item.get("sessionId")?.as_str()?.to_string();
        let last_active = item
            .get("updatedAt")
            .and_then(Value::as_str)
            .and_then(parse_rfc3339_utc)?;
        let first_prompt = item
            .get("title")
            .and_then(Value::as_str)
            .filter(|text| !text.is_empty())
            .map(str::to_string);
        let meta = item.pointer("/_meta/shikigami");
        let in_use = meta
            .and_then(|value| value.get("inUse"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let prompts = meta
            .and_then(|value| value.get("prompts"))
            .and_then(Value::as_u64)
            .map(|n| n as usize)
            .unwrap_or(usize::from(first_prompt.is_some()));
        Some(Self {
            session_id,
            last_active,
            prompts,
            first_prompt,
            in_use,
        })
    }
}

#[derive(Clone)]
struct LiveSession {
    cwd: PathBuf,
    run_id: Option<String>,
    cancel: Option<watch::Sender<bool>>,
    mode: Option<String>,
    mode_frozen: bool,
    /// Model chosen with `session/set_model`, from the next prompt on.
    model: Option<String>,
    /// In-memory payload custody for a content session. Not persisted.
    content: Option<SessionContent>,
    /// Tokens the session's runs reported while this host held it (each run
    /// segment reports its own). In memory only; prompt results report the
    /// difference a prompt made.
    usage: TokenUsage,
    /// MCP servers hidden for later prompts of this host process. Not persisted;
    /// `--continue` / a new process attaches from settings again.
    disabled_mcp: HashSet<String>,
    /// Child run ids this session already watches for idle status.
    watched_children: HashSet<String>,
    /// Ungoverned mutating tool names allowed for the rest of this host session.
    /// In memory only; session/load and a new process start empty.
    ask_grants: HashSet<String>,
    /// Standing attachments the live run composed (no file bodies). In memory;
    /// a new process reports them after the next prompt.
    context: Option<Value>,
    /// Next first prompt of this host session uses plan write-jail. In memory
    /// only; a started run keeps the flag on its checkpoint.
    plan_jail: bool,
}

#[derive(Clone)]
struct SessionContent {
    task: String,
    messages: Vec<ContentMessageV1>,
    resolver: Arc<SessionContentStore>,
}

/// Payloads of one ACP content session (ADR 0021). Memory is a cache; with a
/// directory every payload is also written there, private to the session, so
/// a restarted host can resolve the same references.
struct SessionContentStore {
    values: std::sync::Mutex<HashMap<String, (ContentPartKind, Bytes)>>,
    dir: Option<PathBuf>,
}

impl SessionContentStore {
    fn new(dir: Option<PathBuf>) -> Self {
        Self {
            values: std::sync::Mutex::new(HashMap::new()),
            dir,
        }
    }

    /// File for `reference` of `kind`. The name hashes the reference, so no
    /// part id reaches the path.
    fn payload_path(&self, reference: &str, kind: ContentPartKind) -> Option<PathBuf> {
        let digest = sha256_digest(reference.as_bytes());
        let hex = digest.strip_prefix("sha256:").unwrap_or(&digest);
        Some(
            self.dir
                .as_ref()?
                .join(format!("{hex}-{}", kind.proto_value())),
        )
    }

    fn put(&self, reference: &str, kind: ContentPartKind, bytes: &Bytes) -> Result<(), String> {
        if let Some(path) = self.payload_path(reference, kind) {
            write_private(&path, bytes).map_err(|e| format!("attachment store: {e}"))?;
        }
        self.values
            .lock()
            .map_err(|_| "content store lock poisoned".to_string())?
            .insert(reference.to_string(), (kind, bytes.clone()));
        Ok(())
    }

    fn get(&self, reference: &str, kind: ContentPartKind) -> Option<Bytes> {
        if let Some((stored, bytes)) = self.values.lock().ok()?.get(reference).cloned() {
            return (stored == kind).then_some(bytes);
        }
        let bytes = Bytes::from(fs::read(self.payload_path(reference, kind)?).ok()?);
        self.values
            .lock()
            .ok()?
            .insert(reference.to_string(), (kind, bytes.clone()));
        Some(bytes)
    }

    fn insert(
        &self,
        part_id: &str,
        kind: ContentPartKind,
        media_type: &str,
        payload: Bytes,
        session_id: &str,
    ) -> Result<ContentPartDescriptor, Value> {
        if payload.len() as u64 > MAX_CONTENT_PART_BYTES {
            return Err(rpc_error(
                -32602,
                "attachment exceeds the content part size bound",
            ));
        }
        let reference = format!("acp-{part_id}");
        self.put(&reference, kind, &payload)
            .map_err(|e| rpc_error(-32603, e))?;
        Ok(ContentPartDescriptor {
            part_id: part_id.into(),
            kind,
            media_type: media_type.into(),
            byte_length: payload.len() as u64,
            sha256_digest: sha256_digest(&payload),
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
        let bytes = self.get(reference, ContentPartKind::Text)?;
        std::str::from_utf8(&bytes).ok().map(str::to_owned)
    }
}

/// Write `bytes` to `path` owner-only, replacing any file there atomically.
fn write_private(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    let dir = path.parent().unwrap_or(Path::new("."));
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    std::os::unix::fs::DirBuilderExt::mode(&mut builder, 0o700);
    builder.create(dir)?;
    let tmp = dir.join(format!(
        ".{}.tmp-{}",
        path.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("payload"),
        uuid::Uuid::new_v4().simple()
    ));
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    let result = options.open(&tmp).and_then(|mut file| {
        file.write_all(bytes)?;
        file.sync_all()
    });
    match result.and_then(|()| fs::rename(&tmp, path)) {
        Ok(()) => Ok(()),
        Err(error) => {
            let _ = fs::remove_file(&tmp);
            Err(error)
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
        let bytes = self
            .get(&descriptor.reference, descriptor.kind)
            .ok_or_else(|| {
                crate::content::ContentError::Resolver(
                    "attachment payload is no longer available to the session".into(),
                )
            })?;
        if descriptor.kind == ContentPartKind::Text {
            String::from_utf8(bytes.to_vec())
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
        let bytes = match content.payload {
            ResolvedContent::Text(text) => Bytes::from(text),
            ResolvedContent::Bytes(bytes) => bytes,
        };
        let reference = format!("acp-{}", content.part_id);
        self.put(&reference, content.kind, &bytes)
            .map_err(crate::content::ContentError::Resolver)?;
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

/// Servers this host would attach at run start, with known mock tool names.
fn mcp_inventory(config: &crate::config::Config) -> Vec<(String, Vec<String>)> {
    let mut out = Vec::new();
    if config.tools.mcp_gateway.is_some() {
        out.push((crate::config::MCP_GATEWAY_NAME.to_string(), Vec::new()));
    }
    for server in &config.tools.mcp_servers {
        let tools = if server.command == "mock" {
            vec![
                format!("mcp.{}.echo", server.name),
                format!("mcp.{}.wide", server.name),
            ]
        } else {
            Vec::new()
        };
        out.push((server.name.clone(), tools));
    }
    out
}

/// In-process ACP agent. Stdio and tests share this.
pub struct AcpHost {
    harness: Harness,
    initialized: AtomicBool,
    sessions: Mutex<HashMap<String, LiveSession>>,
    /// Open lock files of the sessions this host created or loaded. The OS
    /// lock lasts as long as the file stays open, so it ends with the process.
    held: std::sync::Mutex<HashMap<String, fs::File>>,
}

fn session_usage_json(usage: TokenUsage) -> Value {
    let mut body = json!({
        "inputTokens": usage.input_tokens,
        "outputTokens": usage.output_tokens,
    });
    if let Some(cached) = usage.cached_input_tokens {
        body["cachedInputTokens"] = json!(cached);
    }
    if let Some(created) = usage.cache_creation_input_tokens {
        body["cacheCreationInputTokens"] = json!(created);
    }
    body
}

impl AcpHost {
    pub fn new(harness: Harness) -> Self {
        let host = Self {
            harness,
            initialized: AtomicBool::new(false),
            sessions: Mutex::new(HashMap::new()),
            held: std::sync::Mutex::new(HashMap::new()),
        };
        host.sweep_orphan_payloads();
        host
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
        // Method futures include an engine run. Poll them from the heap so a
        // Linux debug worker (2MiB) can host session/prompt with MCP tools.
        let result = match method {
            "initialize" => self.initialize(&params),
            "session/new" => Box::pin(self.session_new(&params, client)).await,
            "session/list" => self.session_list(&params),
            "session/load" => Box::pin(self.session_load(&params, client)).await,
            "session/prompt" => Box::pin(self.session_prompt(&params, client)).await,
            "session/compact" => Box::pin(self.session_compact(&params)).await,
            "session/set_model" => Box::pin(self.session_set_model(&params)).await,
            "session/mcp" => Box::pin(self.session_mcp(&params)).await,
            "session/plan" => Box::pin(self.session_plan(&params)).await,
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
                "sessionCapabilities": { "list": {} },
                "promptCapabilities": prompt_capabilities(self.harness.model_port())
            },
            "agentInfo": {
                "name": PRODUCT,
                "version": VERSION
            },
            "authMethods": []
        }))
    }

    async fn session_new(&self, params: &Value, client: &dyn AcpClient) -> Result<Value, Value> {
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
        self.hold_session(&session_id)
            .map_err(|e| rpc_error(-32603, e.to_string()))?;
        let live = LiveSession {
            cwd: cwd.clone(),
            run_id: None,
            cancel: None,
            mode: mode.clone(),
            mode_frozen: mode.is_some(),
            model: None,
            content: None,
            usage: TokenUsage::default(),
            disabled_mcp: HashSet::new(),
            watched_children: HashSet::new(),
            ask_grants: HashSet::new(),
            context: None,
            plan_jail: false,
        };
        self.persist(&session_id, &live)
            .map_err(|e| rpc_error(-32603, e))?;
        let models = self.session_models(&live).await;
        self.sessions.lock().await.insert(session_id.clone(), live);
        let commands = self
            .advertise_available_commands(&session_id, &cwd, client)
            .await?;
        let mut result = json!({
            "sessionId": session_id,
            "availableCommands": commands,
        });
        if let Some(mode) = mode {
            result["mode"] = json!(mode);
        }
        if let Some(models) = models {
            result["models"] = models;
        }
        Ok(result)
    }

    /// `session/list`: persisted sessions this host knows, most recently
    /// active first. Optional `cwd` filters to that workspace. One page.
    fn session_list(&self, params: &Value) -> Result<Value, Value> {
        self.require_init()?;
        if let Some(cursor) = params.get("cursor")
            && !cursor.is_null()
        {
            return Err(rpc_error(-32602, "session/list cursor is not valid"));
        }
        let cwd = match params.get("cwd") {
            None | Some(Value::Null) => None,
            Some(value) => {
                let path = value
                    .as_str()
                    .map(PathBuf::from)
                    .filter(|p| p.is_absolute())
                    .ok_or_else(|| {
                        rpc_error(-32602, "session/list cwd must be an absolute path")
                    })?;
                Some(path)
            }
        };
        let sessions: Vec<Value> = self
            .collect_sessions(cwd.as_deref())
            .into_iter()
            .map(|row| row.summary.to_list_item(&row.cwd))
            .collect();
        Ok(json!({ "sessions": sessions }))
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
        if self.load_persisted(session_id).is_none() {
            return Err(rpc_error(-32002, "unknown session"));
        }
        self.hold_session(session_id).map_err(session_hold_error)?;
        let live_session = self.sessions.lock().await.get(session_id).cloned();
        let restored = live_session.is_none();
        let live = if let Some(live) = live_session {
            if let Some(stored) = self.sessions.lock().await.get_mut(session_id) {
                stored.ask_grants.clear();
            }
            LiveSession {
                cwd: live.cwd.clone(),
                run_id: live.run_id.clone(),
                cancel: None,
                mode: live.mode.clone(),
                mode_frozen: live.mode_frozen,
                model: live.model.clone(),
                content: live.content.clone(),
                usage: live.usage,
                disabled_mcp: live.disabled_mcp.clone(),
                watched_children: live.watched_children.clone(),
                ask_grants: HashSet::new(),
                context: live.context.clone(),
                plan_jail: live.plan_jail,
            }
        } else {
            let persisted = self
                .load_persisted(session_id)
                .ok_or_else(|| rpc_error(-32002, "unknown session"))?;
            let content = persisted
                .run_id
                .as_deref()
                .and_then(|run_id| self.restore_session_content(session_id, run_id));
            let live = LiveSession {
                cwd: PathBuf::from(&persisted.cwd),
                run_id: persisted.run_id.clone(),
                cancel: None,
                mode: persisted.mode.clone(),
                mode_frozen: persisted.mode_is_frozen(),
                model: persisted.model.clone(),
                content,
                usage: TokenUsage::default(),
                disabled_mcp: HashSet::new(),
                watched_children: HashSet::new(),
                ask_grants: HashSet::new(),
                context: None,
                plan_jail: false,
            };
            self.sessions
                .lock()
                .await
                .insert(session_id.to_string(), live.clone());
            live
        };
        let interrupted = self
            .replay_session_history(session_id, &live, restored, client)
            .await?;
        let mut result = json!({
            "sessionId": session_id,
            "cwd": live.cwd.display().to_string(),
        });
        if let Some(interrupted) = interrupted {
            result["_meta"]["shikigami"]["interrupted"] = interrupted;
        }
        if let Some(models) = self.session_models(&live).await {
            result["models"] = models;
        }
        if let Some(run_id) = live.run_id {
            result["_meta"]["shikigami"]["runId"] = json!(run_id);
            if let Ok(harness) =
                self.harness_for_cwd(&live.cwd, live.mode.as_deref(), live.model.as_deref())
            {
                let children = children_snapshot(&harness, &run_id);
                if !children.is_empty() {
                    result["_meta"]["shikigami"]["children"] = json!(children);
                }
                watch_loaded_children(self, &harness, client, session_id, &run_id, &children).await;
            }
        }
        if let Some(context) = live.context {
            result["_meta"]["shikigami"]["context"] = context;
        }
        let commands = self
            .advertise_available_commands(session_id, &live.cwd, client)
            .await?;
        result["availableCommands"] = json!(commands);
        Ok(result)
    }

    /// Advertise invocable slash names as a replacement `available_commands_update`.
    async fn advertise_available_commands(
        &self,
        session_id: &str,
        cwd: &Path,
        client: &dyn AcpClient,
    ) -> Result<Vec<Value>, Value> {
        let commands = available_commands_json(cwd, &self.harness.config.context);
        send_updates(
            client,
            vec![json!({
                "sessionId": session_id,
                "update": {
                    "sessionUpdate": "available_commands_update",
                    "availableCommands": commands
                }
            })],
        )
        .await?;
        Ok(commands)
    }

    /// `/compact` on `session/prompt`: same cut as `session/compact`, no model turn.
    async fn session_prompt_compact(&self, session_id: &str) -> Result<Value, Value> {
        let compact = self
            .session_compact(&json!({ "sessionId": session_id }))
            .await?;
        let mut result = json!({ "stopReason": "end_turn" });
        if let Some(before) = compact.get("before") {
            result["before"] = before.clone();
        }
        if let Some(after) = compact.get("after") {
            result["after"] = after.clone();
        }
        if let Some(run_id) = self.session_run_id(session_id).await {
            result["_meta"]["shikigami"]["runId"] = json!(run_id);
        }
        Ok(result)
    }

    /// `/model` on `session/prompt`: empty lists the session catalog; a name is
    /// `session/set_model`.
    async fn session_prompt_model(&self, session_id: &str, args: &str) -> Result<Value, Value> {
        if args.is_empty() {
            let live = self
                .sessions
                .lock()
                .await
                .get(session_id)
                .cloned()
                .ok_or_else(|| rpc_error(-32002, "unknown session"))?;
            let mut result = json!({ "stopReason": "end_turn" });
            if let Some(models) = self.session_models(&live).await {
                result["models"] = models;
            }
            return Ok(result);
        }
        self.session_set_model(&json!({
            "sessionId": session_id,
            "modelId": args
        }))
        .await?;
        Ok(json!({ "stopReason": "end_turn" }))
    }

    /// The session's model catalog in ACP's (experimental) session model
    /// shape: `currentModelId` and `availableModels`. `None` when the catalog
    /// cannot be read, so the client offers no switch.
    async fn session_models(&self, live: &LiveSession) -> Option<Value> {
        let available = self.session_catalog(live).await.ok()?;
        let harness = self
            .harness_for_cwd(&live.cwd, live.mode.as_deref(), live.model.as_deref())
            .ok()?;
        let models: Vec<Value> = available
            .iter()
            .map(|m| json!({ "modelId": m.canonical_model, "name": m.canonical_model }))
            .collect();
        Some(json!({
            "currentModelId": harness.effective_model_name(),
            "availableModels": models,
        }))
    }

    /// Models `live` may switch to. Built without the session's own choice,
    /// so the configured model stays in the list after a switch.
    async fn session_catalog(&self, live: &LiveSession) -> Result<Vec<AvailableModel>, String> {
        let harness = self.harness_for_cwd(&live.cwd, live.mode.as_deref(), None)?;
        harness.available_models().await.map_err(|e| e.to_string())
    }

    /// `session/set_model`: later prompts of the session use `modelId`. The
    /// id must be in the session's catalog. A prompt already running keeps
    /// its model.
    async fn session_set_model(&self, params: &Value) -> Result<Value, Value> {
        self.require_init()?;
        let session_id = params
            .get("sessionId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| rpc_error(-32602, "session/set_model requires sessionId"))?;
        let model_id = params
            .get("modelId")
            .and_then(|v| v.as_str())
            .filter(|id| !id.trim().is_empty())
            .ok_or_else(|| rpc_error(-32602, "session/set_model requires modelId"))?;
        let live = self
            .sessions
            .lock()
            .await
            .get(session_id)
            .cloned()
            .ok_or_else(|| rpc_error(-32002, "unknown session"))?;
        let available = self.session_catalog(&live).await.map_err(|detail| {
            let mut value = rpc_error(-32000, "The model list is unavailable.");
            value["data"] = json!({ "shikigami": { "kind": "modelCatalog", "detail": detail } });
            value
        })?;
        if !available.iter().any(|m| m.canonical_model == model_id) {
            return Err(rpc_error(
                -32602,
                format!("model `{model_id}` is not offered for this session"),
            ));
        }
        let mut sessions = self.sessions.lock().await;
        let live = sessions
            .get_mut(session_id)
            .ok_or_else(|| rpc_error(-32002, "unknown session"))?;
        let previous = live.model.replace(model_id.to_string());
        if let Err(e) = self.persist(session_id, live) {
            live.model = previous;
            return Err(rpc_error(-32603, e));
        }
        Ok(json!({}))
    }

    /// `session/mcp`: list attached MCP servers, or disable/enable one for later
    /// prompts of this process. Disable does not rewrite settings.
    async fn session_mcp(&self, params: &Value) -> Result<Value, Value> {
        self.require_init()?;
        let session_id = params
            .get("sessionId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| rpc_error(-32602, "session/mcp requires sessionId"))?;
        let action = params
            .get("action")
            .and_then(|v| v.as_str())
            .unwrap_or("list");
        let inventory = mcp_inventory(&self.harness.config);
        match action {
            "list" => {
                let disabled = {
                    let sessions = self.sessions.lock().await;
                    let live = sessions
                        .get(session_id)
                        .ok_or_else(|| rpc_error(-32002, "unknown session"))?;
                    live.disabled_mcp.clone()
                };
                let servers: Vec<Value> = inventory
                    .iter()
                    .map(|(name, tools)| {
                        json!({
                            "name": name,
                            "tools": tools,
                            "disabled": disabled.contains(name),
                        })
                    })
                    .collect();
                Ok(json!({ "servers": servers }))
            }
            "disable" | "enable" => {
                let name = params
                    .get("name")
                    .and_then(|v| v.as_str())
                    .map(str::trim)
                    .filter(|name| !name.is_empty())
                    .ok_or_else(|| {
                        rpc_error(-32602, format!("session/mcp {action} requires name"))
                    })?;
                if name == crate::config::MCP_GATEWAY_NAME {
                    return Err(rpc_error(-32602, "MCP gateway cannot be disabled"));
                }
                if !inventory.iter().any(|(server, _)| server == name) {
                    return Err(rpc_error(
                        -32602,
                        format!("MCP server `{name}` is not attached"),
                    ));
                }
                let mut sessions = self.sessions.lock().await;
                let live = sessions
                    .get_mut(session_id)
                    .ok_or_else(|| rpc_error(-32002, "unknown session"))?;
                if action == "disable" {
                    live.disabled_mcp.insert(name.to_string());
                } else {
                    live.disabled_mcp.remove(name);
                }
                Ok(json!({}))
            }
            other => Err(rpc_error(
                -32602,
                format!("session/mcp action `{other}` is not offered"),
            )),
        }
    }

    /// `session/plan`: the next first prompt of this session uses plan
    /// write-jail. A run that already started without the jail is refused.
    /// The flag is in memory until that prompt; the run's checkpoint carries
    /// it after. Settings `run.plan_jail` stay unchanged.
    async fn session_plan(&self, params: &Value) -> Result<Value, Value> {
        self.require_init()?;
        let session_id = params
            .get("sessionId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| rpc_error(-32602, "session/plan requires sessionId"))?;
        let (cwd, run_id, mode, model, already, in_flight) = {
            let sessions = self.sessions.lock().await;
            let live = sessions
                .get(session_id)
                .ok_or_else(|| rpc_error(-32002, "unknown session"))?;
            (
                live.cwd.clone(),
                live.run_id.clone(),
                live.mode.clone(),
                live.model.clone(),
                live.plan_jail,
                live.cancel.is_some(),
            )
        };
        if in_flight {
            return Err(rpc_error(-32000, "prompt already in flight"));
        }
        if let Some(run_id) = run_id {
            let harness = self
                .harness_for_cwd(&cwd, mode.as_deref(), model.as_deref())
                .map_err(|e| rpc_error(-32603, e))?;
            let checkpoint = Checkpoint::load(&harness.state.runs_dir(), &run_id)
                .map_err(|_| rpc_error(-32603, "session run checkpoint is unreadable"))?;
            if checkpoint.plan_jail {
                return Ok(json!({ "planJail": true, "already": true }));
            }
            return Err(rpc_error(
                -32602,
                "This session already ran without the jail. Start a new session to plan.",
            ));
        }
        if already {
            return Ok(json!({ "planJail": true, "already": true }));
        }
        let mut sessions = self.sessions.lock().await;
        let live = sessions
            .get_mut(session_id)
            .ok_or_else(|| rpc_error(-32002, "unknown session"))?;
        live.plan_jail = true;
        Ok(json!({ "planJail": true }))
    }

    async fn session_compact(&self, params: &Value) -> Result<Value, Value> {
        self.require_init()?;
        let session_id = params
            .get("sessionId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| rpc_error(-32602, "session/compact requires sessionId"))?;
        let (cwd, run_id, mode, model) = {
            let sessions = self.sessions.lock().await;
            let live = sessions
                .get(session_id)
                .ok_or_else(|| rpc_error(-32002, "unknown session"))?;
            if live.cancel.is_some() {
                return Err(rpc_error(-32000, "prompt already in flight"));
            }
            (
                live.cwd.clone(),
                live.run_id.clone(),
                live.mode.clone(),
                live.model.clone(),
            )
        };
        let Some(run_id) = run_id else {
            return Ok(json!({ "before": 0, "after": 0 }));
        };
        let harness = self
            .harness_for_cwd(&cwd, mode.as_deref(), model.as_deref())
            .map_err(|e| rpc_error(-32603, e))?;
        let mut checkpoint = Checkpoint::load(&harness.state.runs_dir(), &run_id)
            .map_err(|_| rpc_error(-32603, "session run checkpoint is unreadable"))?;
        let keep = harness.config.run.compact_keep_tail.max(2) as usize;
        let keep_bytes = harness.config.run.compact_keep_tail_bytes;
        // Content sidecar stays on the 32-part contract; this cut is ChatMessage history.
        let before = checkpoint.messages.len();
        let after = if let Some((_, after)) =
            compact_messages_bounded(&mut checkpoint.messages, 0, keep, keep_bytes)
        {
            checkpoint
                .save(&harness.state.runs_dir())
                .map_err(|e| rpc_error(-32603, e.to_string()))?;
            after
        } else {
            before
        };
        Ok(json!({ "before": before, "after": after }))
    }

    /// Replay the session's conversation as `session/update`s. For a session
    /// `restored` from disk, also returns its `interruption`, if any.
    async fn replay_session_history(
        &self,
        session_id: &str,
        live: &LiveSession,
        restored: bool,
        client: &dyn AcpClient,
    ) -> Result<Option<Value>, Value> {
        let Some(run_id) = live.run_id.as_deref() else {
            return Ok(None);
        };
        let harness = self
            .harness_for_cwd(&live.cwd, live.mode.as_deref(), live.model.as_deref())
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
        let interrupted = restored
            .then(|| {
                interruption(
                    &harness,
                    run_id,
                    &checkpoint,
                    unresolved_tool_calls(&updates),
                )
            })
            .flatten();
        send_updates(client, updates).await?;
        Ok(interrupted)
    }

    async fn session_prompt(&self, params: &Value, client: &dyn AcpClient) -> Result<Value, Value> {
        self.require_init()?;
        let session_id = params
            .get("sessionId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| rpc_error(-32602, "session/prompt requires sessionId"))?
            .to_string();
        let mut parsed = parse_prompt(params.get("prompt").unwrap_or(&Value::Null))?;
        let mut load_skill = prompt_load_skill(params)?;
        if let Some((name, args)) = {
            let sessions = self.sessions.lock().await;
            let live = sessions
                .get(&session_id)
                .ok_or_else(|| rpc_error(-32002, "unknown session"))?;
            let advertised = available_command_names(&live.cwd, &self.harness.config.context);
            let first = first_prompt_text(params.get("prompt").unwrap_or(&Value::Null));
            first.and_then(|text| slash_invocation(text, &advertised))
        } {
            match name.as_str() {
                "compact" => return self.session_prompt_compact(&session_id).await,
                "model" => return self.session_prompt_model(&session_id, &args).await,
                other => {
                    let id = other
                        .strip_prefix(SKILL_SLASH_PREFIX)
                        .unwrap_or(other)
                        .to_string();
                    let first = first_prompt_text(params.get("prompt").unwrap_or(&Value::Null))
                        .unwrap_or("");
                    let send = skill_prompt_text(&id, &args);
                    if parsed.text.starts_with(first) {
                        parsed.text = format!("{}{}", send, &parsed.text[first.len()..]);
                    } else {
                        parsed.text = send;
                    }
                    load_skill = Some(id);
                }
            }
        }
        let requested_mode = optional_mode(params)?;
        let (
            cwd,
            resume_run_id,
            cancel_rx,
            mode,
            model,
            existing_content,
            disabled_mcp,
            ask_grants,
            plan_jail,
        ) = {
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
                live.model.clone(),
                live.content.clone(),
                live.disabled_mcp.clone(),
                live.ask_grants.clone(),
                live.plan_jail,
            )
        };

        let before = self.session_usage(&session_id).await;
        let outcome = async {
            let harness = self
                .harness_for_session(&cwd, mode.as_deref(), model.as_deref(), &disabled_mcp)
                .map_err(|e| rpc_error(-32603, e))?;
            let (sink, mut events) = AsyncChannelSink::pair();
            let sink: Arc<dyn EventSink> = Arc::new(sink);
            // A session that starts with attachments is a content run and
            // takes attachments on any later prompt (ADR 0021). A text run
            // stays text.
            if parsed.has_attachments() {
                if resume_run_id.is_some() && existing_content.is_none() {
                    return Err(rpc_error(
                        -32602,
                        "attachments need a session that started with attachments; start a new session to attach",
                    ));
                }
                parsed.require_supported(harness.model_port().content_kinds())?;
            }
            let mut drive = if parsed.has_attachments() && resume_run_id.is_none() {
                let _ = fs::remove_dir_all(self.content_dir(&session_id));
                let store = Arc::new(SessionContentStore::new(Some(
                    self.content_dir(&session_id),
                )));
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
                request.load_skill = load_skill.clone();
                PromptDrive::Content(request)
            } else if let Some(run_id) = resume_run_id.clone() {
                match resume_prompt_drive(
                    &harness,
                    &session_id,
                    run_id,
                    &parsed,
                    existing_content,
                    cancel_rx,
                    client,
                )
                .await?
                {
                    ResumeOutcome::Drive(mut drive) => {
                        drive.set_load_skill(load_skill.clone());
                        *drive
                    }
                    ResumeOutcome::Done(value) => return Ok(value),
                }
            } else {
                let mut request = RunRequest::new(parsed.text.clone());
                request.keep_workspace = true;
                request.session_wait = true;
                request.cancel = Some(cancel_rx);
                request.plan_jail = plan_jail;
                request.set_load_skill(load_skill.clone());
                PromptDrive::Text(request)
            };
            drive.grant_session_asks(ask_grants.iter().cloned());
            self.persist_ask_grants(&session_id, &drive).await;

            let stop = self
                .drive_prompt(&session_id, harness, &mut drive, client, sink, &mut events)
                .await?;
            let mut result = json!({ "stopReason": stop });
            // The run that `shikigami runs` and `shikigami export` accept for this session.
            if let Some(run_id) = self.session_run_id(&session_id).await {
                result["_meta"]["shikigami"]["runId"] = json!(run_id);
                let children = children_snapshot(&self.harness, &run_id);
                if !children.is_empty() {
                    result["_meta"]["shikigami"]["children"] = json!(children);
                }
            }
            // Tokens this prompt used; absent while unknown (zero).
            let after = self.session_usage(&session_id).await;
            let usage = after.saturating_sub(before);
            if usage.total() > 0 {
                result["_meta"]["shikigami"]["usage"] = session_usage_json(usage);
            }
            if let Some(context) = self.session_context(&session_id).await {
                result["_meta"]["shikigami"]["context"] = context;
            }
            Ok(result)
        }
        .await;
        if outcome.is_err() {
            let no_run = self
                .sessions
                .lock()
                .await
                .get(&session_id)
                .is_some_and(|live| live.run_id.is_none());
            if no_run {
                self.end_session_content(&session_id).await;
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
                    self.retain_run_for_next_prompt(
                        session_id,
                        &harness,
                        drive.resume_run_id().or(forwarded_run_id),
                    )
                    .await;
                    return Ok("cancelled");
                }
                Err(e) => {
                    let error = prompt_error(&e);
                    if error.pointer("/data/shikigami/kind").is_some() {
                        // A model failure ends the turn, not the conversation.
                        self.retain_run_for_next_prompt(
                            session_id,
                            &harness,
                            drive.resume_run_id().or(forwarded_run_id),
                        )
                        .await;
                    }
                    return Err(error);
                }
            };
            self.set_run_id(session_id, Some(result.run_id.clone()))
                .await
                .map_err(|e| rpc_error(-32603, e))?;
            if let Some(live) = self.sessions.lock().await.get_mut(session_id) {
                live.usage = live.usage.saturating_add(result.usage);
            }

            if result.termination == RunTermination::Cancelled {
                self.retain_run_for_next_prompt(session_id, &harness, Some(result.run_id.clone()))
                    .await;
                return Ok("cancelled");
            }
            if result.park.is_none() {
                self.end_session_content(session_id).await;
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
                            persist_prompt_wait_for_drive(&harness, &result.run_id, drive)
                                .await
                                .map_err(|e| rpc_error(-32603, e))?;
                            return Ok("cancelled");
                        }
                        PermissionOutcome::Allow | PermissionOutcome::AllowSession => {
                            if outcome == PermissionOutcome::AllowSession {
                                self.remember_session_ask(session_id, park, drive).await;
                            }
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
                            persist_prompt_wait_for_drive(&harness, &result.run_id, drive)
                                .await
                                .map_err(|e| rpc_error(-32603, e))?;
                            return Ok("cancelled");
                        }
                        PermissionOutcome::Allow | PermissionOutcome::AllowSession => {
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
                            persist_prompt_wait_for_drive(&harness, &result.run_id, drive)
                                .await
                                .map_err(|e| rpc_error(-32603, e))?;
                            return Ok("cancelled");
                        }
                        PermissionOutcome::Allow
                        | PermissionOutcome::AllowSession
                        | PermissionOutcome::Deny => {
                            drive.resume_answer(result.run_id.clone(), answer);
                        }
                    }
                }
                Some(ParkKind::Approval) => {
                    return Err(rpc_error(
                        -32603,
                        "approval parks are not mapped on ACP yet",
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

    /// Where the built-in demo works, when no model is configured: a scratch
    /// directory under the state root, so the demo's permissioned write
    /// never lands in the user's project. `None` with a configured model.
    /// The configured model id, before any session switches it
    /// (`session/set_model`). The TUI shows another id only after a switch.
    pub fn configured_model(&self) -> String {
        self.harness.effective_model_name()
    }

    /// `[run] context_window_tokens`, when the operator set one.
    pub fn context_window_tokens(&self) -> Option<u64> {
        self.harness.config.run.context_window_tokens
    }

    pub fn demo_workspace(&self) -> Option<PathBuf> {
        let model = &self.harness.config.model;
        (model.adapter == "scripted" && model.script_json.is_none())
            .then(|| self.harness.state.path().join("demo-workspace"))
    }

    /// The harness one session prompt runs on: `cwd` in place (or the demo
    /// workspace), the session mode, then the session's chosen `model`.
    fn harness_for_cwd(
        &self,
        cwd: &Path,
        mode: Option<&str>,
        model: Option<&str>,
    ) -> Result<Harness, String> {
        self.harness_for_session(cwd, mode, model, &HashSet::new())
    }

    fn harness_for_session(
        &self,
        cwd: &Path,
        mode: Option<&str>,
        model: Option<&str>,
        disabled_mcp: &HashSet<String>,
    ) -> Result<Harness, String> {
        let mut config = self.harness.config.clone();
        config.workspace.adapter = "inplace".into();
        config.workspace.root = match self.demo_workspace() {
            Some(demo) => {
                fs::create_dir_all(&demo).map_err(|e| e.to_string())?;
                demo.display().to_string()
            }
            None => cwd.display().to_string(),
        };
        if let Some(mode) = mode {
            config.apply_session_mode(mode).map_err(|e| e.to_string())?;
        }
        if let Some(model) = model {
            config.model.model = model.to_string();
        }
        config
            .tools
            .mcp_servers
            .retain(|server| !disabled_mcp.contains(&server.name));
        Harness::from_config(config, self.harness.state.clone()).map_err(|e| e.to_string())
    }

    fn sessions_dir(&self) -> PathBuf {
        self.harness.state.path().join("acp-sessions")
    }

    fn session_lock_path(&self, session_id: &str) -> PathBuf {
        self.sessions_dir().join(format!("{session_id}.lock"))
    }

    /// Take the exclusive OS lock on a session, so a second process (another
    /// TUI, an editor) cannot drive it at the same time. Held until this host
    /// drops; a crashed holder releases it at once. `WouldBlock` means another
    /// process holds it.
    fn hold_session(&self, session_id: &str) -> std::io::Result<()> {
        let mut held = self.held.lock().unwrap_or_else(|e| e.into_inner());
        if held.contains_key(session_id) {
            return Ok(());
        }
        fs::create_dir_all(self.sessions_dir())?;
        let file = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(self.session_lock_path(session_id))?;
        file.try_lock().map_err(|e| match e {
            fs::TryLockError::WouldBlock => std::io::ErrorKind::WouldBlock.into(),
            fs::TryLockError::Error(e) => e,
        })?;
        held.insert(session_id.to_string(), file);
        Ok(())
    }

    /// True when another process holds the session's lock. Probes with a
    /// lock that is released again at once.
    fn held_elsewhere(&self, session_id: &str) -> bool {
        if self
            .held
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains_key(session_id)
        {
            return false;
        }
        let Ok(file) = fs::File::open(self.session_lock_path(session_id)) else {
            return false;
        };
        matches!(file.try_lock_shared(), Err(fs::TryLockError::WouldBlock))
    }

    /// Private payload directory of one content session (ADR 0021).
    fn content_dir(&self, session_id: &str) -> PathBuf {
        self.sessions_dir().join(format!("{session_id}.content"))
    }

    /// Drop a session's payloads once its content run has ended.
    async fn end_session_content(&self, session_id: &str) {
        if let Some(live) = self.sessions.lock().await.get_mut(session_id) {
            live.content = None;
        }
        let _ = fs::remove_dir_all(self.content_dir(session_id));
    }

    /// Remove payload directories whose session record is gone.
    fn sweep_orphan_payloads(&self) {
        let Ok(entries) = fs::read_dir(self.sessions_dir()) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(session_id) = path
                .file_name()
                .and_then(|name| name.to_str())
                .and_then(|name| name.strip_suffix(".content"))
                && !self
                    .sessions_dir()
                    .join(format!("{session_id}.json"))
                    .exists()
            {
                let _ = fs::remove_dir_all(&path);
            }
        }
    }

    /// Rebuild the payload custody of a persisted content session from its
    /// sidecar and payload directory. `None` for text runs or lost payloads.
    fn restore_session_content(&self, session_id: &str, run_id: &str) -> Option<SessionContent> {
        let dir = self.content_dir(session_id);
        if !dir.is_dir() {
            return None;
        }
        let runs = self.harness.state.runs_dir();
        let checkpoint = Checkpoint::load(&runs, run_id).ok()?;
        let binding = checkpoint.content.as_ref()?;
        let sidecar = crate::content::load_sidecar(&runs, run_id, binding).ok()?;
        let initial = sidecar
            .messages
            .get(..sidecar.initial_message_count as usize)?
            .to_vec();
        Some(SessionContent {
            task: checkpoint.task,
            messages: initial,
            resolver: Arc::new(SessionContentStore::new(Some(dir))),
        })
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
            model: live.model.clone(),
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

    /// Persisted sessions for `cwd` that have a run, most recently active
    /// first. Omits content runs whose payloads are no longer available.
    pub fn sessions_for_cwd(&self, cwd: &Path) -> Vec<SessionSummary> {
        self.collect_sessions(Some(cwd))
            .into_iter()
            .map(|row| row.summary)
            .collect()
    }

    /// Sessions that have a run, most recently active first. `cwd` filters
    /// to that workspace; omit it for every session in this state root.
    fn collect_sessions(&self, cwd: Option<&Path>) -> Vec<SessionListRow> {
        let dir = self.sessions_dir();
        let Ok(entries) = std::fs::read_dir(dir) else {
            return Vec::new();
        };
        // Listing is synchronous; do not wait on the async session driver.
        let live_content_ids: HashSet<_> = self
            .sessions
            .try_lock()
            .map(|sessions| {
                sessions
                    .iter()
                    .filter_map(|(id, live)| live.content.as_ref().map(|_| id.clone()))
                    .collect()
            })
            .unwrap_or_default();
        let runs_dir = self.harness.state.runs_dir();
        let mut found = Vec::new();
        for entry in entries.flatten() {
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
            if let Some(cwd) = cwd
                && !same_cwd(Path::new(&persisted.cwd), cwd)
            {
                continue;
            }
            let Some(run_id) = persisted.run_id.as_deref() else {
                continue;
            };
            let Ok(checkpoint) = Checkpoint::load(&runs_dir, run_id) else {
                continue;
            };
            if checkpoint.content.is_some()
                && !live_content_ids.contains(&persisted.session_id)
                && !self.content_dir(&persisted.session_id).is_dir()
            {
                continue;
            }
            // The checkpoint is rewritten every turn; the session record is not.
            let Ok(last_active) = std::fs::metadata(crate::checkpoint::path_for(&runs_dir, run_id))
                .and_then(|meta| meta.modified())
            else {
                continue;
            };
            let mut prompts = checkpoint.messages.iter().filter(|message| {
                message.role == "user"
                    && !message.content.is_empty()
                    && !message
                        .content
                        .starts_with(crate::run::COMPACTION_MARKER_PREFIX)
            });
            let first_prompt = prompts.next().map(|message| message.content.clone());
            found.push(SessionListRow {
                cwd: persisted.cwd,
                summary: SessionSummary {
                    in_use: self.held_elsewhere(&persisted.session_id),
                    session_id: persisted.session_id,
                    last_active,
                    prompts: usize::from(first_prompt.is_some()) + prompts.count(),
                    first_prompt,
                },
            });
        }
        found.sort_by(|a, b| {
            b.summary
                .last_active
                .cmp(&a.summary.last_active)
                .then_with(|| a.summary.session_id.cmp(&b.summary.session_id))
        });
        found
    }

    /// Ids from [`Self::sessions_for_cwd`]; prefer that, it carries the summary.
    pub fn session_ids_for_cwd(&self, cwd: &Path) -> Vec<String> {
        self.sessions_for_cwd(cwd)
            .into_iter()
            .map(|session| session.session_id)
            .collect()
    }

    /// First id from [`Self::sessions_for_cwd`].
    pub fn last_session_id_for_cwd(&self, cwd: &Path) -> Option<String> {
        self.sessions_for_cwd(cwd)
            .into_iter()
            .next()
            .map(|session| session.session_id)
    }

    async fn session_usage(&self, session_id: &str) -> TokenUsage {
        self.sessions
            .lock()
            .await
            .get(session_id)
            .map(|live| live.usage)
            .unwrap_or_default()
    }

    async fn session_run_id(&self, session_id: &str) -> Option<String> {
        self.sessions.lock().await.get(session_id)?.run_id.clone()
    }

    async fn session_context(&self, session_id: &str) -> Option<Value> {
        self.sessions.lock().await.get(session_id)?.context.clone()
    }

    async fn remember_context(&self, session_id: &str, updates: &[Value]) {
        let Some(context) = updates.iter().rev().find_map(attached_context_json) else {
            return;
        };
        if let Some(live) = self.sessions.lock().await.get_mut(session_id) {
            live.context = Some(context);
        }
    }

    async fn set_run_id(&self, session_id: &str, run_id: Option<String>) -> Result<(), String> {
        let mut sessions = self.sessions.lock().await;
        let live = sessions
            .get_mut(session_id)
            .ok_or_else(|| "unknown session".to_string())?;
        if live.run_id.is_some() && live.run_id != run_id {
            live.content = None;
            let _ = fs::remove_dir_all(self.content_dir(session_id));
        }
        live.run_id = run_id;
        self.persist(session_id, live)
    }

    /// Parks a run whose turn ended early (cancelled, or the model failed) so
    /// the session's next prompt continues it with its history.
    async fn retain_run_for_next_prompt(
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
    AllowSession,
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

/// A permission wait was cancelled: the turn ends like a mid-turn cancel, so
/// its parked background jobs end too, and the run waits for the next prompt.
async fn persist_prompt_wait_for_drive(
    harness: &Harness,
    run_id: &str,
    drive: &PromptDrive,
) -> Result<(), String> {
    harness.registry.reap_parked_background_jobs_of(run_id);
    persist_prompt_wait_with_resolver(&harness.state.runs_dir(), run_id, drive.content_resolver())
        .await
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
        "options": permission_options(park)
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
        PermissionOutcome::Allow | PermissionOutcome::AllowSession => "approved".into(),
        PermissionOutcome::Deny => "denied".into(),
        PermissionOutcome::Cancelled => String::new(),
    }
}

fn permission_options(park: &crate::run::ParkInfo) -> Value {
    if park.kind == ParkKind::Plan {
        return json!([
            {"optionId": "allow", "name": "Accept plan", "kind": "allow_once"},
            {"optionId": "deny", "name": "Reject plan", "kind": "reject_once"}
        ]);
    }
    let mut options = vec![json!({"optionId": "allow", "name": "Allow", "kind": "allow_once"})];
    if park.kind == ParkKind::Ask
        && ask_tool_name(park).is_some_and(crate::tools::mutates_workspace)
    {
        options.push(json!({
            "optionId": "allow_session",
            "name": "Allow for session",
            "kind": "allow_always"
        }));
    }
    options.push(json!({"optionId": "deny", "name": "Deny", "kind": "reject_once"}));
    json!(options)
}

fn ask_tool_name(park: &crate::run::ParkInfo) -> Option<&str> {
    park.reason.strip_prefix("ask:")
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
        ("selected", "allow_session") | ("allow_always", _) | (_, "allow_session") => {
            Ok(PermissionOutcome::AllowSession)
        }
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

impl AcpHost {
    async fn remember_session_ask(
        &self,
        session_id: &str,
        park: &crate::run::ParkInfo,
        drive: &mut PromptDrive,
    ) {
        let Some(name) = ask_tool_name(park).filter(|name| crate::tools::mutates_workspace(name))
        else {
            return;
        };
        if let Some(live) = self.sessions.lock().await.get_mut(session_id) {
            live.ask_grants.insert(name.to_string());
        }
        drive.grant_session_asks(std::iter::once(name.to_string()));
    }

    async fn persist_ask_grants(&self, session_id: &str, drive: &PromptDrive) {
        let names: Vec<String> = match drive {
            PromptDrive::Text(request) => request.ask_session_grants.iter().cloned().collect(),
            PromptDrive::Content(request) => request.ask_session_grants.iter().cloned().collect(),
        };
        if names.is_empty() {
            return;
        }
        if let Some(live) = self.sessions.lock().await.get_mut(session_id) {
            live.ask_grants.extend(names);
        }
    }
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
                request.set_load_skill(None);
            }
            Self::Content(request) => {
                request.keep_workspace = true;
                request.session_wait = true;
                request.resume_run_id = Some(run_id);
                request.resume_prompt = None;
                request.resume_ask = None;
                request.resume_plan = None;
                request.resume_answer = None;
                request.load_skill = None;
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

    fn grant_session_asks(&mut self, names: impl IntoIterator<Item = String>) {
        for name in names {
            match self {
                Self::Text(request) => request.grant_session_ask(name),
                Self::Content(request) => request.grant_session_ask(name),
            }
        }
    }

    fn set_load_skill(&mut self, id: Option<String>) {
        match self {
            Self::Text(request) => {
                if request.resume_prompt.is_some() {
                    request.set_load_skill(id);
                }
            }
            Self::Content(request) => {
                if request.resume_prompt.is_some() {
                    request.load_skill = id;
                }
            }
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
    prompt: &ParsedPrompt,
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
    if prompt.has_attachments() && !checkpoint.is_prompt_wait() {
        return Err(rpc_error(
            -32602,
            "attachments are accepted only when the session waits for a prompt",
        ));
    }
    if checkpoint.is_prompt_wait() {
        match &mut drive {
            PromptDrive::Text(request) => request.resume_prompt = Some(prompt.text.clone()),
            PromptDrive::Content(request) => {
                if let (Some(store), Some(binding)) =
                    (content_store.as_deref(), checkpoint.content.as_ref())
                {
                    let sidecar = crate::content::load_sidecar(
                        &harness.state.runs_dir(),
                        &run_id,
                        binding,
                    )
                    .map_err(|_| rpc_error(-32603, "session content sidecar is unreadable"))?;
                    prompt.admit_follow_up(&sidecar.messages)?;
                    request.resume_parts = prompt.follow_up_parts(store, session_id)?;
                }
                request.resume_prompt = Some(prompt.text.clone());
            }
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
            persist_prompt_wait_for_drive(harness, &run_id, &drive)
                .await
                .map_err(|e| rpc_error(-32603, e))?;
            return Ok(ResumeOutcome::Done(json!({ "stopReason": "cancelled" })));
        }
        if checkpoint.is_ask_park() {
            if outcome == PermissionOutcome::AllowSession {
                // Grants for a restored park land on the drive; the live
                // session table is filled after this returns.
                drive.grant_session_asks(
                    ask_tool_name(&info)
                        .map(str::to_string)
                        .into_iter()
                        .filter(|name| crate::tools::mutates_workspace(name)),
                );
            }
            drive.resume_ask(
                run_id,
                match outcome {
                    PermissionOutcome::Allow | PermissionOutcome::AllowSession => {
                        AskDecision::Allow
                    }
                    PermissionOutcome::Deny => AskDecision::Deny,
                    PermissionOutcome::Cancelled => unreachable!("cancelled returned"),
                },
            );
        } else if checkpoint.is_plan_park() {
            drive.resume_plan(
                run_id,
                match outcome {
                    PermissionOutcome::Allow | PermissionOutcome::AllowSession => {
                        PlanDecision::Accept
                    }
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
            "approval parks are not mapped on ACP yet",
        ));
    }
    Err(rpc_error(-32603, "session run is not waiting for a prompt"))
}

/// Aborts the spawned engine run if the ACP forwarder is dropped first.
struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn join_run(
    joined: Result<Result<crate::run::RunResult, HarnessError>, tokio::task::JoinError>,
) -> Result<crate::run::RunResult, HarnessError> {
    match joined {
        Ok(result) => result,
        Err(error) if error.is_cancelled() => Err(HarnessError::Run(RunError::Cancelled)),
        Err(error) => Err(HarnessError::Run(RunError::Message(format!(
            "run task failed: {error}"
        )))),
    }
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
    let mut streamed = HashSet::new();
    // Engine run poll frames are large (MCP attach + tool batch). Spawn so they
    // do not nest under the ACP forwarder on a 2MiB Linux debug worker.
    let harness_for_run = harness.clone();
    let mut run_task = AbortOnDrop(tokio::spawn(async move {
        match drive {
            PromptDrive::Text(request) => {
                harness_for_run.run_with_events(request, Some(sink)).await
            }
            PromptDrive::Content(request) => harness_for_run
                .run_content_with_events(request, Some(sink))
                .await
                .map(|result| result.run),
        }
    }));
    let result = loop {
        tokio::select! {
            joined = &mut run_task.0 => break join_run(joined),
            event = events.recv() => {
                let Some(first) = event else {
                    break join_run((&mut run_task.0).await);
                };
                let (mut updates, event_run_id) =
                    drain_event_batch(session_id, Some(first), events, &mut streamed);
                host.remember_context(session_id, &updates).await;
                held.append(&mut updates);
                if let Some(id) = event_run_id.clone() {
                    seen_run_id = Some(id.clone());
                    persist_session_run(host, session_id, &id).await;
                }
                let run_id = event_run_id.as_deref().or(known_run_id.as_deref());
                if let Some(run_id) = run_id {
                    let store = session_content_store(host, session_id).await;
                    fill_content_live_updates(&mut held, harness, run_id, store.as_deref());
                    attach_nested_children_meta(&mut held, harness, run_id);
                    watch_background_children(host, harness, client, session_id, run_id, &held)
                        .await;
                    if let Err(error) = send_updates(client, std::mem::take(&mut held))
                        .await
                        .map_err(update_error)
                    {
                        host.signal_cancel(session_id).await;
                        let abandoned = join_run((&mut run_task.0).await);
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
    let (mut updates, event_run_id) = drain_updates(session_id, events, &mut streamed);
    host.remember_context(session_id, &updates).await;
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
        attach_nested_children_meta(&mut held, harness, run_id);
        watch_background_children(host, harness, client, session_id, run_id, &held).await;
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
        .filter(|update| {
            update["update"]["sessionUpdate"] == "agent_message_chunk" && !is_delta(update)
        })
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
        if update["update"]["sessionUpdate"] == "agent_message_chunk" && !is_delta(update) {
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
    for (index, message) in messages.iter().enumerate() {
        match message.role.as_str() {
            "user" => {
                let text =
                    assistant_visible_text(&message.parts, |reference| store.text(reference));
                if !text.is_empty() {
                    updates.push(json!({
                        "sessionId": session_id,
                        "update": {
                            "sessionUpdate": "user_message_chunk",
                            "content": { "type": "text", "text": text },
                            "messageId": format!("user-{index}")
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

/// Replayed `tool_call` ids that no `tool_call_update` resolves, in order.
fn unresolved_tool_calls(updates: &[Value]) -> Vec<String> {
    let mut open: Vec<String> = Vec::new();
    for update in updates {
        let inner = &update["update"];
        let Some(id) = inner["toolCallId"].as_str() else {
            continue;
        };
        match inner["sessionUpdate"].as_str() {
            Some("tool_call") => open.push(id.to_string()),
            Some("tool_call_update") => open.retain(|open_id| open_id != id),
            _ => {}
        }
    }
    open
}

/// `session/load` `_meta.shikigami.interrupted` for a restored session whose
/// last host went away mid-step, or `None`. Read-only: the run keeps its park,
/// so the next prompt asks the pending question again. Outcomes come from the
/// recovery diagnosis (`in_doubt` for an uncertain tool) and the durable park
/// (`not_run` while a permission, plan, or question was pending).
fn interruption(
    harness: &Harness,
    run_id: &str,
    checkpoint: &Checkpoint,
    unresolved: Vec<String>,
) -> Option<Value> {
    let in_doubt = harness
        .diagnose_run(run_id)
        .is_ok_and(|diagnosis| diagnosis.class == crate::harness::RecoveryClass::UncertainTool);
    let outcome = if in_doubt {
        "in_doubt"
    } else if checkpoint.is_ask_park() || checkpoint.is_plan_park() || checkpoint.is_escalate_park()
    {
        "not_run"
    } else {
        return None;
    };
    let tool_calls: Vec<Value> = unresolved
        .into_iter()
        .map(|id| json!({ "toolCallId": id, "outcome": outcome }))
        .collect();
    Some(json!({ "toolCalls": tool_calls }))
}

fn conversation_updates(session_id: &str, messages: &[ChatMessage]) -> Vec<Value> {
    let mut updates = Vec::new();
    let mut turn = 0u32;
    let mut call_ids = HashMap::new();
    for (index, message) in messages.iter().enumerate() {
        match message.role.as_str() {
            "user"
                if message
                    .content
                    .starts_with(crate::run::COMPACTION_MARKER_PREFIX) =>
            {
                updates.push(json!({
                    "sessionId": session_id,
                    "update": {
                        "sessionUpdate": "user_message_chunk",
                        "content": { "type": "text", "text": message.content },
                        "_meta": { "shikigami": { "compacted": true } }
                    }
                }));
            }
            "user" if !message.content.is_empty() => {
                updates.push(json!({
                    "sessionId": session_id,
                    "update": {
                        "sessionUpdate": "user_message_chunk",
                        "content": { "type": "text", "text": message.content },
                        "messageId": format!("user-{index}")
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
                    call_ids.insert(conversation, (stable.clone(), call.name.clone()));
                    if !call.id.is_empty() {
                        call_ids.insert(call.id.clone(), (stable.clone(), call.name.clone()));
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
                let (call_id, name) = call_ids
                    .get(&message.tool_call_id)
                    .cloned()
                    .unwrap_or_else(|| (message.tool_call_id.clone(), String::new()));
                let failed =
                    message.content == "permission denied" || message.content == "cancelled";
                // A report result is stored as `report: <summary>` for resume;
                // the live update showed the summary alone.
                let text = match name.as_str() {
                    "report" => message
                        .content
                        .strip_prefix("report: ")
                        .unwrap_or(&message.content),
                    _ => message.content.as_str(),
                };
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

fn drain_updates(
    session_id: &str,
    events: &mut AsyncChannelRx,
    streamed: &mut HashSet<u32>,
) -> (Vec<Value>, Option<String>) {
    drain_event_batch(session_id, None, events, streamed)
}

/// `streamed` remembers turns whose text already went out as deltas, across
/// batches of one prompt drive.
fn drain_event_batch(
    session_id: &str,
    first: Option<HarnessEvent>,
    events: &mut AsyncChannelRx,
    streamed: &mut HashSet<u32>,
) -> (Vec<Value>, Option<String>) {
    let mut updates = Vec::new();
    let mut run_id = None;
    if let Some(event) = first {
        consume_event(session_id, event, &mut updates, &mut run_id, streamed);
    }
    while let Some(event) = events.try_recv() {
        consume_event(session_id, event, &mut updates, &mut run_id, streamed);
    }
    (updates, run_id)
}

fn consume_event(
    session_id: &str,
    event: HarnessEvent,
    updates: &mut Vec<Value>,
    run_id: &mut Option<String>,
    streamed: &mut HashSet<u32>,
) {
    match &event {
        HarnessEvent::ToolStart { run_id: id, .. }
        | HarnessEvent::ToolEnd { run_id: id, .. }
        | HarnessEvent::AssistantDelta { run_id: id, .. }
        | HarnessEvent::RunFinished { run_id: id, .. }
            if !id.is_empty() =>
        {
            *run_id = Some(id.clone());
        }
        _ => {}
    }
    let update = match event {
        HarnessEvent::AssistantDelta { turn, text, .. } => {
            streamed.insert(turn);
            json!({
                "sessionId": session_id,
                "update": {
                    "sessionUpdate": "agent_message_chunk",
                    "content": { "type": "text", "text": text },
                    "messageId": format!("turn-{turn}"),
                    DELTA_MARK: true
                }
            })
        }
        // A streamed turn's chunk still takes its place in the assistant
        // text fill, then is dropped before sending: its text went out.
        HarnessEvent::ModelTurn {
            turn,
            content_preview,
        } => json!({
            "sessionId": session_id,
            "update": {
                "sessionUpdate": "agent_message_chunk",
                "content": { "type": "text", "text": content_preview },
                "messageId": format!("turn-{turn}"),
                STREAMED_MARK: streamed.remove(&turn)
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
        HarnessEvent::ContextAttached { context } => json!({
            "sessionId": session_id,
            "update": {
                "_meta": { "shikigami": { "context": context } }
            }
        }),
        _ => return,
    };
    updates.push(update);
}

/// Host-internal markers on `agent_message_chunk` updates, removed before
/// sending: a live delta, and the whole-turn chunk of a streamed turn.
const DELTA_MARK: &str = "_shikigamiDelta";
const STREAMED_MARK: &str = "_shikigamiStreamed";

fn is_delta(update: &Value) -> bool {
    update["update"][DELTA_MARK] == true
}

fn attached_context_json(value: &Value) -> Option<Value> {
    value
        .pointer("/update/_meta/shikigami/context")
        .or_else(|| value.pointer("/_meta/shikigami/context"))
        .cloned()
}

fn children_snapshot(harness: &Harness, parent_run_id: &str) -> Vec<Value> {
    let Ok(checkpoint) = Checkpoint::load(&harness.state.runs_dir(), parent_run_id) else {
        return Vec::new();
    };
    checkpoint
        .children
        .iter()
        .map(|child| {
            json!({
                "runId": child.run_id,
                "profile": child.profile,
                "status": child_overlay_status(harness, &child.run_id),
            })
        })
        .collect()
}

fn child_overlay_status(harness: &Harness, child_id: &str) -> &'static str {
    if let Ok(checkpoint) = Checkpoint::load(&harness.state.runs_dir(), child_id)
        && checkpoint.park.is_some()
    {
        return "parked";
    }
    let active = harness.registry.run_is_active(child_id).unwrap_or(false);
    match harness.registry.load(child_id) {
        Ok(record) => match record.status.as_str() {
            "starting" | "running" if active => "running",
            "starting" | "running" => "failed",
            "parked" => "parked",
            "completed" => "done",
            "cancelled" | "timed_out" | "failed" | "max_turns" => "failed",
            _ if record.success == Some(true) => "done",
            _ if record.success == Some(false) => "failed",
            _ if active => "running",
            _ => "failed",
        },
        Err(_) if active => "running",
        Err(_) => "failed",
    }
}

fn child_run_output(update: &Value) -> Option<Value> {
    let inner = update.get("update").unwrap_or(update);
    let detail = inner
        .pointer("/content/0/content/text")
        .or_else(|| inner.pointer("/content/0/text"))
        .and_then(Value::as_str)?;
    serde_json::from_str::<Value>(detail).ok().filter(|value| {
        value.get("run_id").and_then(Value::as_str).is_some()
            && value.get("profile").and_then(Value::as_str).is_some()
    })
}

fn update_carries_child_run(update: &Value) -> bool {
    let inner = update.get("update").unwrap_or(update);
    (inner.get("sessionUpdate").and_then(Value::as_str) == Some("tool_call")
        && inner.get("title").and_then(Value::as_str) == Some("child_run"))
        || child_run_output(update).is_some()
}

fn attach_nested_children_meta(updates: &mut [Value], harness: &Harness, parent_run_id: &str) {
    let children = children_snapshot(harness, parent_run_id);
    for update in updates {
        if update_carries_child_run(update) {
            update["update"]["_meta"]["shikigami"]["children"] = json!(children);
        }
    }
}

async fn watch_background_children(
    host: &AcpHost,
    harness: &Harness,
    client: &dyn AcpClient,
    session_id: &str,
    parent_run_id: &str,
    updates: &[Value],
) {
    let Some(client) = client.clone_arc() else {
        return;
    };
    let mut to_watch = Vec::new();
    {
        let mut sessions = host.sessions.lock().await;
        let Some(live) = sessions.get_mut(session_id) else {
            return;
        };
        for update in updates {
            let Some(output) = child_run_output(update) else {
                continue;
            };
            if output.get("status").and_then(Value::as_str) != Some("running") {
                continue;
            }
            let Some(child_id) = output.get("run_id").and_then(Value::as_str) else {
                continue;
            };
            if live.watched_children.insert(child_id.to_string()) {
                to_watch.push(child_id.to_string());
            }
        }
    }
    for child_id in to_watch {
        spawn_child_idle_watch(
            harness.clone(),
            Arc::clone(&client),
            session_id.to_string(),
            parent_run_id.to_string(),
            child_id,
        );
    }
}

async fn watch_loaded_children(
    host: &AcpHost,
    harness: &Harness,
    client: &dyn AcpClient,
    session_id: &str,
    parent_run_id: &str,
    children: &[Value],
) {
    let Some(client) = client.clone_arc() else {
        return;
    };
    let mut to_watch = Vec::new();
    {
        let mut sessions = host.sessions.lock().await;
        let Some(live) = sessions.get_mut(session_id) else {
            return;
        };
        for child in children {
            if child.get("status").and_then(Value::as_str) != Some("running") {
                continue;
            }
            let Some(child_id) = child.get("runId").and_then(Value::as_str) else {
                continue;
            };
            if live.watched_children.insert(child_id.to_string()) {
                to_watch.push(child_id.to_string());
            }
        }
    }
    for child_id in to_watch {
        spawn_child_idle_watch(
            harness.clone(),
            Arc::clone(&client),
            session_id.to_string(),
            parent_run_id.to_string(),
            child_id,
        );
    }
}

fn spawn_child_idle_watch(
    harness: Harness,
    client: Arc<dyn AcpClient>,
    session_id: String,
    parent_run_id: String,
    child_id: String,
) {
    tokio::spawn(async move {
        let Ok((mut rx, local)) = harness.registry.watch_idle(&child_id) else {
            return;
        };
        if !*rx.borrow() {
            if local {
                let _ = rx.changed().await;
            } else {
                loop {
                    if *rx.borrow() || !harness.registry.run_is_active(&child_id).unwrap_or(false) {
                        break;
                    }
                    tokio::select! {
                        changed = rx.changed() => {
                            if changed.is_err() {
                                break;
                            }
                        }
                        _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                    }
                }
            }
        }
        let children = children_snapshot(&harness, &parent_run_id);
        let update = json!({
            "sessionId": session_id,
            "update": {
                "sessionUpdate": "tool_call_update",
                "_meta": { "shikigami": { "children": children } }
            }
        });
        let _ = client.notify("session/update", update).await;
    });
}

async fn send_updates(client: &dyn AcpClient, updates: Vec<Value>) -> Result<(), Value> {
    for mut update in updates {
        if update["update"][STREAMED_MARK] == true {
            continue;
        }
        if let Some(inner) = update.get_mut("update").and_then(Value::as_object_mut) {
            inner.remove(DELTA_MARK);
            inner.remove(STREAMED_MARK);
        }
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
    payload: Bytes,
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

    /// Fail closed before admission when this follow-up would take the
    /// session past the ADR 0006 bounds, which hold over the whole
    /// conversation because every model call resolves all of it.
    fn admit_follow_up(&self, history: &[ContentMessageV1]) -> Result<(), Value> {
        let held = history.iter().flat_map(|message| &message.parts);
        let text = (!self.text.is_empty()).then_some(self.text.len() as u64);
        let added = self
            .attachments
            .iter()
            .map(|attachment| attachment.payload.len() as u64)
            .chain(text);
        let parts = held.clone().count() + added.clone().count();
        let bytes = held
            .map(|part| part.byte_length)
            .chain(added)
            .fold(0_u64, u64::saturating_add);
        if parts > MAX_CONTENT_PARTS || bytes > MAX_CONTENT_AGGREGATE_BYTES {
            return Err(rpc_error(
                -32602,
                "attachments exceed the session content bound (32 parts, 16 MiB)",
            ));
        }
        Ok(())
    }

    /// Store this follow-up's attachments under fresh part ids. The engine
    /// adds the prompt text as its own part.
    fn follow_up_parts(
        &self,
        store: &SessionContentStore,
        session_id: &str,
    ) -> Result<Vec<ContentPartDescriptor>, Value> {
        let prompt = uuid::Uuid::new_v4().simple().to_string();
        self.attachments
            .iter()
            .enumerate()
            .map(|(index, attachment)| {
                store.insert(
                    &format!("f{}-part-{index}", &prompt[..12]),
                    attachment.kind,
                    &attachment.media_type,
                    attachment.payload.clone(),
                    session_id,
                )
            })
            .collect()
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
                Bytes::copy_from_slice(self.text.as_bytes()),
                session_id,
            )?);
            index += 1;
        }
        for attachment in &self.attachments {
            parts.push(store.insert(
                &format!("part-{index}"),
                attachment.kind,
                &attachment.media_type,
                attachment.payload.clone(),
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

/// Host-advertised slash names. TUI-only chrome (`help`, `copy`, `attach`,
/// `exit`, `quit`, `new`, `resume`, `run`, `usage`, plus `context` / `plan` /
/// `mcp`) stays off the wire.
const ADVERTISED_SLASH: &[(&str, &str, Option<&str>)] = &[
    ("compact", "shrink conversation history", None),
    ("model", "list or switch models", Some("model id")),
];

/// TUI built-in names: a skill pack with one of these ids is advertised as
/// `skill:<id>`, matching TUI `BUILTIN_COMMANDS`.
const SLASH_COLLISION_NAMES: &[&str] = &[
    "help", "compact", "context", "copy", "attach", "exit", "quit", "new", "plan", "run", "usage",
    "model", "mcp", "resume",
];

const SKILL_SLASH_PREFIX: &str = "skill:";

fn skill_slash_name(id: &str) -> String {
    if SLASH_COLLISION_NAMES.contains(&id) || id.starts_with(SKILL_SLASH_PREFIX) {
        format!("{SKILL_SLASH_PREFIX}{id}")
    } else {
        id.to_string()
    }
}

fn available_commands_json(cwd: &Path, settings: &crate::config::ContextSettings) -> Vec<Value> {
    let mut commands: Vec<Value> = ADVERTISED_SLASH
        .iter()
        .map(|(name, description, hint)| {
            let mut command = json!({ "name": name, "description": description });
            if let Some(hint) = hint {
                command["input"] = json!({ "hint": hint });
            }
            command
        })
        .collect();
    for id in crate::context::list_skill_ids(cwd, settings) {
        commands.push(json!({
            "name": skill_slash_name(&id),
            "description": "load skill pack",
            "input": { "hint": "args" }
        }));
    }
    commands
}

fn available_command_names(
    cwd: &Path,
    settings: &crate::config::ContextSettings,
) -> HashSet<String> {
    available_commands_json(cwd, settings)
        .iter()
        .filter_map(|command| {
            command
                .get("name")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .collect()
}

fn first_prompt_text(prompt: &Value) -> Option<&str> {
    prompt.as_array()?.iter().find_map(|part| {
        if part.get("type").and_then(Value::as_str) != Some("text") {
            return None;
        }
        part.get("text").and_then(Value::as_str)
    })
}

/// First text part `/name` then whitespace or end, when `name` was advertised.
/// `//text` is not a command.
fn slash_invocation(first_text: &str, advertised: &HashSet<String>) -> Option<(String, String)> {
    let trimmed = first_text.trim();
    if !trimmed.starts_with('/') || trimmed.starts_with("//") {
        return None;
    }
    let rest = trimmed[1..].trim();
    let (name, args) = match rest.split_once(char::is_whitespace) {
        Some((name, args)) => (name, args.trim()),
        None => (rest, ""),
    };
    advertised
        .contains(name)
        .then(|| (name.to_string(), args.to_string()))
}

fn skill_prompt_text(id: &str, args: &str) -> String {
    if args.is_empty() {
        format!("follow skill `{id}`")
    } else {
        args.to_string()
    }
}

/// Evolving ACP `_meta.shikigami.loadSkill`: a skill id the host loads as
/// `load_skill` before the model turn. Absent or JSON null is no load.
fn prompt_load_skill(params: &Value) -> Result<Option<String>, Value> {
    let Some(value) = params.pointer("/_meta/shikigami/loadSkill") else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    value
        .as_str()
        .map(str::trim)
        .filter(|id| !id.is_empty())
        .map(|id| Ok(Some(id.to_string())))
        .unwrap_or_else(|| {
            Err(rpc_error(
                -32602,
                "session/prompt _meta.shikigami.loadSkill must be a skill id",
            ))
        })
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
    if payload.is_empty() {
        return Err(rpc_error(-32602, "attachment data is empty"));
    }
    Ok(StagedAttachment {
        kind,
        media_type: mime.trim().to_ascii_lowercase(),
        payload: payload.into(),
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
    if payload.is_empty() {
        return Err(rpc_error(-32602, "attachment data is empty"));
    }
    if payload.len() as u64 > MAX_CONTENT_PART_BYTES {
        return Err(rpc_error(
            -32602,
            "attachment exceeds the content part size bound",
        ));
    }
    Ok(StagedAttachment {
        kind,
        media_type: mime.trim().to_ascii_lowercase(),
        payload: payload.into(),
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
    let encoded = input.chars().filter(|ch| !ch.is_whitespace());
    let encoded_len: usize = encoded.clone().map(char::len_utf8).sum();
    let padding = encoded.rev().take_while(|ch| *ch == '=').take(2).count();
    // Count before allocating either the compact input or decoded payload.
    // The decoder below still validates alphabet and padding placement.
    let decoded_len = (encoded_len / 4 * 3).saturating_sub(padding);
    if decoded_len as u64 > MAX_CONTENT_PART_BYTES {
        return Err(rpc_error(
            -32602,
            "attachment exceeds the content part size bound",
        ));
    }
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

/// A failed `session/prompt` run. Model failures carry
/// `session/load` error message for a session another process holds.
pub const SESSION_IN_USE: &str = "session is open in another process";

/// A session another process holds: `data.shikigami.kind = "sessionInUse"`,
/// so a client can start a new session and say why.
fn session_hold_error(error: std::io::Error) -> Value {
    if error.kind() != std::io::ErrorKind::WouldBlock {
        return rpc_error(-32603, error.to_string());
    }
    let mut value = rpc_error(-32000, SESSION_IN_USE);
    value["data"] = json!({ "shikigami": { "kind": "sessionInUse" } });
    value
}

/// `data.shikigami.kind = "model"` so a client can say what failed in its own words.
fn prompt_error(error: &HarnessError) -> Value {
    let mut value = rpc_error(-32603, error.to_string());
    let model_error = match error {
        HarnessError::Model(e)
        | HarnessError::Run(RunError::Model(e))
        | HarnessError::Run(RunError::Governance(GovernanceError::Model(e))) => e,
        _ => return value,
    };
    value["data"] = json!({ "shikigami": { "kind": "model" } });
    if let Some(cause) = model_failure_cause(model_error) {
        value["data"]["shikigami"]["cause"] = json!(cause);
    }
    value
}

/// The `data.shikigami.cause` of a model failure, when it is known:
/// `scriptEnded` (a scripted model has no more turns), `unreachable` (no
/// connection or timeout), `credentials` (401/403), `account` (402/429:
/// billing, quota, or rate).
fn model_failure_cause(error: &ModelError) -> Option<&'static str> {
    match error {
        ModelError::ScriptExhausted => Some("scriptEnded"),
        ModelError::Unreachable(_) => Some("unreachable"),
        ModelError::ProviderStatus {
            code: 401 | 403, ..
        } => Some("credentials"),
        ModelError::ProviderStatus {
            code: 402 | 429, ..
        } => Some("account"),
        _ => None,
    }
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

/// UTC `YYYY-MM-DDTHH:MM:SSZ` for `session/list` `updatedAt`.
fn rfc3339_utc(when: SystemTime) -> String {
    let secs = when
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let mut days = secs / 86_400;
    let tod = secs % 86_400;
    let hour = tod / 3600;
    let min = (tod % 3600) / 60;
    let sec = tod % 60;
    let mut year = 1970i32;
    loop {
        let length = u64::from(days_in_year(year));
        if days < length {
            break;
        }
        days -= length;
        year += 1;
    }
    let months = days_in_months(year);
    let mut month = 1u32;
    for &length in &months {
        if days < length {
            break;
        }
        days -= length;
        month += 1;
    }
    format!(
        "{year:04}-{month:02}-{:02}T{hour:02}:{min:02}:{sec:02}Z",
        days + 1
    )
}

fn parse_rfc3339_utc(value: &str) -> Option<SystemTime> {
    let value = value
        .strip_suffix('Z')
        .or_else(|| value.strip_suffix("+00:00"))?;
    let (date, time) = value.split_once('T')?;
    let mut date = date.split('-');
    let year: i32 = date.next()?.parse().ok()?;
    let month: u32 = date.next()?.parse().ok()?;
    let day: u32 = date.next()?.parse().ok()?;
    if date.next().is_some() || year < 1970 || !(1..=12).contains(&month) {
        return None;
    }
    let time = time.split(['.', '+']).next()?;
    let mut time = time.split(':');
    let hour: u32 = time.next()?.parse().ok()?;
    let min: u32 = time.next()?.parse().ok()?;
    let sec: u32 = time.next()?.parse().ok()?;
    if time.next().is_some() || hour > 23 || min > 59 || sec > 60 {
        return None;
    }
    let months = days_in_months(year);
    let dim = months[(month as usize) - 1];
    if day == 0 || u64::from(day) > dim {
        return None;
    }
    let mut days = 0u64;
    for y in 1970..year {
        days += u64::from(days_in_year(y));
    }
    days += months.iter().take((month as usize) - 1).sum::<u64>();
    days += u64::from(day - 1);
    Some(UNIX_EPOCH + Duration::from_secs(days * 86_400 + u64::from(hour * 3600 + min * 60 + sec)))
}

fn days_in_year(year: i32) -> u32 {
    if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) {
        366
    } else {
        365
    }
}

fn days_in_months(year: i32) -> [u64; 12] {
    let feb = if days_in_year(year) == 366 { 29 } else { 28 };
    [31, feb, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
}

/// Serve ACP on process stdio until EOF. Newline-delimited JSON-RPC.
pub async fn run_stdio(harness: Harness) -> Result<(), String> {
    run_stdio_until(harness, std::future::pending()).await
}

/// [`run_stdio`] that also stops when `shutdown` resolves (for example on
/// SIGTERM). Either way the in-flight prompt is cancelled and awaited, so
/// its run reaps its background jobs, and parked session jobs are reaped.
pub async fn run_stdio_until(
    harness: Harness,
    shutdown: impl std::future::Future<Output = ()>,
) -> Result<(), String> {
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

    // Sessions end with the host: no parked background job outlives it, also
    // when this future is dropped on a signal or unwinds from a panic.
    let _reap = ReapParkedJobsOnDrop(Arc::clone(&host.harness.registry));
    serve_inbound(host, client, inbound_rx, writer, shutdown).await
}

struct ReapParkedJobsOnDrop(Arc<crate::registry::RunRegistry>);

impl Drop for ReapParkedJobsOnDrop {
    fn drop(&mut self) {
        self.0.reap_parked_background_jobs();
    }
}

async fn serve_inbound(
    host: Arc<AcpHost>,
    client: StdioClient,
    mut inbound_rx: mpsc::Receiver<Value>,
    writer: Arc<Mutex<tokio::io::Stdout>>,
    shutdown: impl std::future::Future<Output = ()>,
) -> Result<(), String> {
    let mut prompt: Option<tokio::task::JoinHandle<Option<Value>>> = None;
    let mut shutdown = std::pin::pin!(shutdown);
    loop {
        tokio::select! {
            () = &mut shutdown => {
                host.cancel_all().await;
                client.cancel_all_pending().await;
                if let Some(task) = prompt.take() {
                    let _ = task.await;
                }
                break;
            }
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
    fn clone_arc(&self) -> Option<Arc<dyn AcpClient>> {
        Some(Arc::new(self.clone()))
    }

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
fn permission_result(permission: PermissionOutcome) -> Value {
    match permission {
        PermissionOutcome::Allow => {
            json!({ "outcome": { "outcome": "allow", "optionId": "allow" } })
        }
        PermissionOutcome::AllowSession => {
            json!({ "outcome": { "outcome": "selected", "optionId": "allow_session" } })
        }
        PermissionOutcome::Deny => json!({ "outcome": { "outcome": "deny", "optionId": "deny" } }),
        PermissionOutcome::Cancelled => json!({ "outcome": { "outcome": "cancelled" } }),
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
        Ok(permission_result(self.permission))
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

/// Cancels its session when the first streamed text chunk arrives.
#[cfg(test)]
struct CancelOnChunkClient {
    host: Arc<AcpHost>,
    session_id: String,
    updates: Mutex<Vec<Value>>,
}

#[cfg(test)]
#[async_trait]
impl AcpClient for CancelOnChunkClient {
    async fn notify(&self, _method: &str, params: Value) -> Result<(), String> {
        let chunk = params["update"]["sessionUpdate"] == "agent_message_chunk";
        self.updates.lock().await.push(params);
        if chunk {
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
        }
        Ok(())
    }

    async fn request(&self, method: &str, _params: Value) -> Result<Value, String> {
        Err(format!("unexpected agent request {method}"))
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
        Ok(permission_result(self.permission))
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
    #[tokio::test]
    async fn attachment_storage_and_resolution_share_the_decoded_buffer() {
        let parsed = parse_prompt(&serde_json::json!([
            {"type": "text", "text": "inspect"},
            {"type": "image", "mimeType": "image/png", "data": "aGVsbG8="}
        ]))
        .unwrap();
        let pointer = parsed.attachments[0].payload.as_ptr();
        let store = SessionContentStore::new(None);
        let messages = parsed.messages(&store, "session").unwrap();
        let descriptor = &messages[0].parts[1];
        let first = store.resolve(descriptor).await.unwrap();
        let second = store.resolve(descriptor).await.unwrap();
        assert_eq!(first.as_bytes(), b"hello");
        assert_eq!(first.as_bytes().as_ptr(), pointer);
        assert_eq!(second.as_bytes().as_ptr(), pointer);
        drop(parsed);
        drop(first);
        assert_eq!(second.as_bytes(), b"hello");
    }

    use super::*;
    use crate::config::{
        Config, MCP_GATEWAY_NAME, McpGatewaySettings, McpServerSettings, ToolsSettings,
    };
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
    fn base64_part_bound_accepts_padding_and_whitespace_at_limit() {
        let cap = MAX_CONTENT_PART_BYTES as usize;
        for size in [cap - 1, cap, cap + 1] {
            let mut encoded = "AAAA".repeat(size / 3);
            encoded.push_str(match size % 3 {
                1 => "AA==",
                2 => "AAA=",
                _ => "",
            });
            encoded.push_str("\n \t");
            let decoded = decode_base64(&encoded);
            if size <= cap {
                assert_eq!(decoded.unwrap().len(), size);
            } else {
                let error = decoded.unwrap_err();
                assert_eq!(error["code"], -32602);
                assert!(
                    error["message"]
                        .as_str()
                        .unwrap()
                        .contains("part size bound")
                );
            }
        }
    }

    #[test]
    fn oversized_inline_and_resource_payloads_fail_before_base64_decoding() {
        let mut data = "AAAA".repeat(MAX_CONTENT_PART_BYTES as usize / 3 + 2);
        data.replace_range(..1, "!");
        for part in [
            json!({"type":"image", "mimeType":"image/png", "data":data}),
            json!({"type":"audio", "mimeType":"audio/wav", "data":data}),
            json!({"type":"resource", "resource": {"mimeType":"application/pdf", "blob":data}}),
        ] {
            let error = match parse_prompt(&json!([
                {"type":"text", "text":"inspect"}, part
            ])) {
                Ok(_) => panic!("oversized attachment admitted"),
                Err(error) => error,
            };
            assert_eq!(error["code"], -32602);
            assert!(
                error["message"]
                    .as_str()
                    .unwrap()
                    .contains("part size bound"),
                "{error}"
            );
        }
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
            result["agentCapabilities"]["sessionCapabilities"]["list"],
            json!({})
        );
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
    async fn nested_child_run_tool_updates_carry_children_meta() {
        let dir = tempdir().unwrap();
        let script = serde_json::json!([
            {
                "tool_calls": [
                    {
                        "name": "child_run",
                        "args_json": serde_json::json!({
                            "profile": "explore",
                            "task": "scout-a",
                            "wait": false
                        }).to_string()
                    },
                    {
                        "name": "child_run",
                        "args_json": serde_json::json!({
                            "profile": "explore",
                            "task": "scout-b",
                            "wait": false
                        }).to_string()
                    }
                ]
            },
            {
                "tool_calls": [{
                    "name": "report",
                    "args_json": serde_json::json!({
                        "summary": "parent done",
                        "success": true
                    }).to_string()
                }]
            }
        ])
        .to_string();
        let host = scripted_host_with(dir.path(), &script, |config| {
            config.run.nested = true;
        });
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
                        "prompt": [{"type":"text","text":"delegate"}]
                    }
                }),
                &client,
            )
            .await
            .unwrap();
        assert_eq!(rpc_ok(&prompt)["stopReason"], "end_turn", "{prompt}");
        let updates = client.updates.lock().await;
        let children_meta: Vec<&Value> = updates
            .iter()
            .filter_map(|update| update.pointer("/update/_meta/shikigami/children"))
            .collect();
        assert!(
            !children_meta.is_empty(),
            "child_run updates must carry _meta.shikigami.children: {updates:?}"
        );
        let listed = children_meta
            .iter()
            .flat_map(|value| value.as_array().into_iter().flatten())
            .filter_map(|child| {
                Some((
                    child.get("runId")?.as_str()?,
                    child.get("profile")?.as_str()?,
                    child.get("status")?.as_str()?,
                ))
            })
            .collect::<Vec<_>>();
        assert!(
            listed
                .iter()
                .filter(|(_, profile, _)| *profile == "explore")
                .count()
                >= 2,
            "expected two explore children, got {listed:?}"
        );
        for (_, _, status) in &listed {
            assert!(
                matches!(*status, "running" | "parked" | "done" | "failed"),
                "unexpected child status in {listed:?}"
            );
        }
        let result_children = rpc_ok(&prompt)
            .pointer("/_meta/shikigami/children")
            .and_then(Value::as_array);
        assert!(
            result_children.is_some(),
            "prompt result should list children: {prompt}"
        );
        drop(updates);
        let load = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 4,
                    "method": "session/load",
                    "params": { "sessionId": session_id }
                }),
                &client,
            )
            .await
            .unwrap();
        assert!(
            rpc_ok(&load)
                .pointer("/_meta/shikigami/children")
                .and_then(Value::as_array)
                .is_some(),
            "session/load should list children: {load}"
        );
    }

    #[tokio::test]
    async fn invalid_mode_mapping_does_not_create_session() {
        let dir = tempdir().unwrap();
        let host = scripted_host_with(dir.path(), r#"[{"content":"hello"}]"#, |config| {
            config.session.modes.insert("low".into(), {
                let mut mode = crate::config::SessionModeMapping::default();
                mode.tools = Some(vec!["not_a_host_tool".into()]);
                mode
            });
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
        assert!(host.sessions_for_cwd(dir.path()).is_empty());
    }

    #[tokio::test]
    async fn invalid_first_prompt_mode_leaves_session_unfrozen_for_retry() {
        let dir = tempdir().unwrap();
        let host = scripted_host_with(dir.path(), r#"[{"content":"hello"}]"#, |config| {
            config.session.modes.insert("low".into(), {
                let mut mode = crate::config::SessionModeMapping::default();
                mode.tools = Some(vec!["not_a_host_tool".into()]);
                mode
            });
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
            config.session.modes.insert("low".into(), {
                let mut mode = crate::config::SessionModeMapping::default();
                mode.model = Some("model-low".into());
                mode.effort = Some("low".into());
                mode.tools = Some(vec!["read_file".into(), "report".into()]);
                mode.prompt = None;
                mode
            });
            config.session.modes.insert("high".into(), {
                let mut mode = crate::config::SessionModeMapping::default();
                mode.model = Some("model-high".into());
                mode.effort = Some("high".into());
                mode.tools = Some(vec!["read_file".into(), "report".into()]);
                mode.prompt = Some("be thorough".into());
                mode
            });
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
                            {"type":"image","mimeType":"  IMAGE/PNG \t","data": encode_base64(png)},
                            {
                                "type":"resource",
                                "resource": {
                                    "uri": "attachment://doc.pdf",
                                    "mimeType": "\t APPLICATION/PDF ",
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

    /// #616: replayed prompts carry their own message id, and compaction markers
    /// are flagged so a client shows them as a note.
    #[test]
    fn replay_marks_compaction_and_separates_prompts() {
        let user = |content: &str| ChatMessage {
            role: "user".into(),
            content: content.into(),
            tool_call_id: String::new(),
            tool_calls: vec![],
        };
        let marker = format!(
            "{} 2 earlier messages omitted]",
            crate::run::COMPACTION_MARKER_PREFIX
        );
        let updates = conversation_updates("s", &[user("first"), user(&marker), user("second")]);
        let ids: Vec<&Value> = updates.iter().map(|u| &u["update"]["messageId"]).collect();
        assert_eq!(ids, [&json!("user-0"), &Value::Null, &json!("user-2")]);
        assert_eq!(
            updates[1]["update"]["_meta"]["shikigami"]["compacted"],
            true
        );
        assert!(updates[0]["update"].get("_meta").is_none());
    }

    /// #615: a model failure on `session/prompt` says it is a model failure.
    #[tokio::test]
    async fn prompt_model_failure_is_marked_as_model() {
        let dir = tempdir().unwrap();
        let host = scripted_host(dir.path(), "[]");
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
                        "prompt": [{"type":"text","text":"hello"}]
                    }
                }),
                &client,
            )
            .await
            .unwrap();
        assert_eq!(prompt["error"]["code"], -32603, "{prompt}");
        assert_eq!(
            prompt["error"]["data"]["shikigami"]["kind"], "model",
            "{prompt}"
        );
    }

    /// #615: after a model failure the next prompt continues the same run,
    /// with the failed prompt still in its history.
    #[tokio::test]
    async fn prompt_after_model_failure_continues_the_run() {
        let dir = tempdir().unwrap();
        let host = scripted_host(
            dir.path(),
            // Fails until both prompts are in the run's history.
            r#"[{"requires_context":["first-ask","again"],"content":"recovered"}]"#,
        );
        let client = RecordingClient {
            updates: Mutex::new(Vec::new()),
            permission: PermissionOutcome::Allow,
        };
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let session_id = init_and_new(&host, &client, &cwd).await;
        let prompt = |id: u64, text: &str| {
            json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": "session/prompt",
                "params": { "sessionId": session_id, "prompt": [{"type":"text","text":text}] }
            })
        };
        let failed = host.handle(prompt(3, "first-ask"), &client).await.unwrap();
        assert_eq!(
            failed["error"]["data"]["shikigami"]["kind"], "model",
            "{failed}"
        );
        let retried = host.handle(prompt(4, "again"), &client).await.unwrap();
        assert_eq!(rpc_ok(&retried)["stopReason"], "end_turn", "{retried}");
        let updates = client.updates.lock().await;
        assert!(
            updates
                .iter()
                .any(|u| u["update"]["content"]["text"] == "recovered"),
            "{updates:?}"
        );
    }

    /// #601: prompt and load results name the run `shikigami runs` and
    /// `export` accept, and a restarted host reports the same run.
    #[tokio::test]
    async fn prompt_and_load_report_the_session_run() {
        let dir = tempdir().unwrap();
        let host = scripted_host(dir.path(), r#"[{"content":"ok"}]"#);
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
                        "prompt": [{"type":"text","text":"hello"}]
                    }
                }),
                &client,
            )
            .await
            .unwrap();
        let run_id = rpc_ok(&prompt)["_meta"]["shikigami"]["runId"]
            .as_str()
            .expect("prompt result names the run")
            .to_string();
        assert!(Checkpoint::load(&dir.path().join("state/runs"), &run_id).is_ok());

        drop(host);
        let restarted = scripted_host(dir.path(), r#"[{"content":"ok"}]"#);
        let loaded = load_after_restart(&restarted, &client, &session_id, &cwd).await;
        assert_eq!(rpc_ok(&loaded)["_meta"]["shikigami"]["runId"], run_id);
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
        let empty_file = dir.path().join("empty");
        std::fs::write(&empty_file, []).unwrap();
        for attachment in [
            json!({"type":"image", "mimeType":"image/png", "uri":empty_file}),
            json!({"type":"resource", "resource":{"mimeType":"application/pdf", "uri":empty_file}}),
            json!({"type":"resource", "resource":{"mimeType":"text/plain", "text":""}}),
        ] {
            let rejected = host
                .handle(
                    json!({
                        "jsonrpc":"2.0", "id":2, "method":"session/prompt",
                        "params":{"sessionId":session_id, "prompt":[
                            {"type":"text", "text":"inspect"}, attachment
                        ]}
                    }),
                    &client,
                )
                .await
                .unwrap();
            assert_eq!(rejected["error"]["code"], -32602, "{rejected}");
            assert!(
                rejected["error"]["message"]
                    .as_str()
                    .unwrap()
                    .contains("empty")
            );
            let sessions = host.sessions.lock().await;
            let live = sessions.get(&session_id).unwrap();
            assert!(live.run_id.is_none());
            assert!(live.content.is_none());
        }
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

    /// #609: each prompt result carries the tokens that prompt used, summed
    /// over its run segments, and leaves them out while none were reported.
    #[tokio::test]
    async fn prompt_result_carries_the_prompts_token_use() {
        let dir = tempdir().unwrap();
        let host = scripted_host(
            dir.path(),
            r#"[{"content":"a"},
                {"content":"b","usage":{"input_tokens":10,"output_tokens":2,"cached_input_tokens":8,"cache_creation_input_tokens":1}},
                {"content":"c","usage":{"input_tokens":5,"output_tokens":1}},
                {"tool_calls":[{"name":"write_file","args_json":"{\"path\":\"x.txt\",\"content\":\"x\"}"}],"usage":{"input_tokens":3,"output_tokens":1}},
                {"content":"d","usage":{"input_tokens":2,"output_tokens":1}}]"#,
        );
        let client = RecordingClient {
            updates: Mutex::new(Vec::new()),
            permission: PermissionOutcome::Allow,
        };
        let project = dir.path().join("project");
        fs::create_dir_all(&project).unwrap();
        let session_id = init_and_new(&host, &client, &project).await;
        let text = |t: &str| json!([{ "type": "text", "text": t }]);
        let usage = |resp: &Value| rpc_ok(resp)["_meta"]["shikigami"]["usage"].clone();

        let first = prompt(&host, &client, &session_id, text("one")).await;
        assert_eq!(rpc_ok(&first)["stopReason"], "end_turn", "{first}");
        assert_eq!(usage(&first), Value::Null, "{first}");
        let second = prompt(&host, &client, &session_id, text("two")).await;
        assert_eq!(
            usage(&second),
            json!({
                "inputTokens": 10,
                "outputTokens": 2,
                "cachedInputTokens": 8,
                "cacheCreationInputTokens": 1
            }),
            "{second}"
        );
        let third = prompt(&host, &client, &session_id, text("three")).await;
        assert_eq!(usage(&third), json!({"inputTokens": 5, "outputTokens": 1}));
        // Parks for the write, resumes after the allow: both segments count.
        let fourth = prompt(&host, &client, &session_id, text("write")).await;
        assert_eq!(usage(&fourth), json!({"inputTokens": 5, "outputTokens": 2}));
    }

    #[tokio::test]
    async fn set_model_switches_later_turns_and_records_each_turns_model() {
        let dir = tempdir().unwrap();
        let script = r#"[{"content":"a"},{"content":"b"},{"content":"c"}]"#;
        let with_alternate = |config: &mut Config| {
            config.model.model = "model-a".into();
            config.model.alternates = vec!["model-b".into()];
        };
        let host = scripted_host_with(dir.path(), script, with_alternate);
        let client = RecordingClient {
            updates: Mutex::new(Vec::new()),
            permission: PermissionOutcome::Allow,
        };
        let project = dir.path().join("project");
        fs::create_dir_all(&project).unwrap();
        host.handle(
            json!({"jsonrpc": "2.0", "id": 1, "method": "initialize",
                   "params": {"protocolVersion": 1, "capabilities": {}}}),
            &client,
        )
        .await
        .unwrap();
        let created = host
            .handle(
                json!({"jsonrpc": "2.0", "id": 2, "method": "session/new",
                       "params": {"cwd": project, "mcpServers": []}}),
                &client,
            )
            .await
            .unwrap();
        let models = &rpc_ok(&created)["models"];
        assert_eq!(models["currentModelId"], "model-a", "{created}");
        assert_eq!(
            models["availableModels"],
            json!([{"modelId": "model-a", "name": "model-a"},
                   {"modelId": "model-b", "name": "model-b"}])
        );
        let session_id = rpc_ok(&created)["sessionId"].as_str().unwrap().to_string();
        let set_model = |model: &str| {
            json!({"jsonrpc": "2.0", "id": 4, "method": "session/set_model",
                   "params": {"sessionId": session_id, "modelId": model}})
        };
        let text = |t: &str| json!([{ "type": "text", "text": t }]);

        let first = prompt(&host, &client, &session_id, text("one")).await;
        let refused = host.handle(set_model("model-z"), &client).await.unwrap();
        assert_eq!(refused["error"]["code"], -32602, "{refused}");
        let switched = host.handle(set_model("model-b"), &client).await.unwrap();
        assert_eq!(rpc_ok(&switched), &json!({}));
        let second = prompt(&host, &client, &session_id, text("two")).await;
        let run_id = rpc_ok(&second)["_meta"]["shikigami"]["runId"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(rpc_ok(&first)["_meta"]["shikigami"]["runId"], run_id);

        let log = host.harness.registry.event_log(&run_id).unwrap();
        let turns: Vec<String> = log
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .filter(|event| event["event"] == "model_turn")
            .map(|event| event["detail"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(turns, ["turn=1 model=model-a", "turn=2 model=model-b"]);

        // A restarted host keeps the session's choice.
        drop(host);
        let host = scripted_host_with(dir.path(), script, with_alternate);
        let loaded = load_after_restart(&host, &client, &session_id, &project).await;
        let models = &rpc_ok(&loaded)["models"];
        assert_eq!(models["currentModelId"], "model-b", "{loaded}");
        assert_eq!(
            models["availableModels"].as_array().unwrap().len(),
            2,
            "{loaded}"
        );
        let back = host.handle(set_model("model-a"), &client).await.unwrap();
        assert_eq!(
            rpc_ok(&back),
            &json!({}),
            "the configured model stays offered"
        );
    }

    fn with_demo_mcp(config: &mut Config) {
        config
            .tools
            .mcp_servers
            .push(McpServerSettings::stdio("demo", "mock", Vec::new()));
        let mut enabled = ToolsSettings::default_coding_tools();
        enabled.push("mcp.demo.echo".into());
        config.tools.enabled = enabled;
    }

    fn session_mcp(session_id: &str, action: &str, name: Option<&str>) -> Value {
        let mut params = json!({ "sessionId": session_id, "action": action });
        if let Some(name) = name {
            params["name"] = json!(name);
        }
        json!({
            "jsonrpc": "2.0",
            "id": 4,
            "method": "session/mcp",
            "params": params
        })
    }

    fn tool_result_texts(updates: &[Value]) -> Vec<String> {
        updates
            .iter()
            .filter(|update| update["update"]["sessionUpdate"] == "tool_call_update")
            .map(|update| {
                update["update"]["content"][0]["content"]["text"]
                    .as_str()
                    .unwrap_or("")
                    .to_string()
            })
            .collect()
    }

    #[tokio::test]
    async fn session_mcp_lists_disables_and_refuses_the_gateway() {
        let dir = tempdir().unwrap();
        let host = scripted_host_with(dir.path(), r#"[{"content":"ok"}]"#, |config| {
            with_demo_mcp(config);
            config.tools.mcp_gateway = Some(McpGatewaySettings::new("http://127.0.0.1:1/mcp"));
        });
        let client = RecordingClient {
            updates: Mutex::new(Vec::new()),
            permission: PermissionOutcome::Allow,
        };
        let project = dir.path().join("project");
        fs::create_dir_all(&project).unwrap();
        let session_id = init_and_new(&host, &client, &project).await;

        let listed = host
            .handle(session_mcp(&session_id, "list", None), &client)
            .await
            .unwrap();
        assert_eq!(
            rpc_ok(&listed)["servers"],
            json!([
                {"name": MCP_GATEWAY_NAME, "tools": [], "disabled": false},
                {"name": "demo", "tools": ["mcp.demo.echo", "mcp.demo.wide"], "disabled": false}
            ]),
            "{listed}"
        );

        let gateway = host
            .handle(
                session_mcp(&session_id, "disable", Some(MCP_GATEWAY_NAME)),
                &client,
            )
            .await
            .unwrap();
        assert_eq!(gateway["error"]["code"], -32602, "{gateway}");
        assert!(
            gateway["error"]["message"]
                .as_str()
                .unwrap()
                .contains("cannot be disabled"),
            "{gateway}"
        );

        let missing = host
            .handle(session_mcp(&session_id, "disable", Some("other")), &client)
            .await
            .unwrap();
        assert_eq!(missing["error"]["code"], -32602, "{missing}");
        assert!(
            missing["error"]["message"]
                .as_str()
                .unwrap()
                .contains("is not attached"),
            "{missing}"
        );

        let disabled = host
            .handle(session_mcp(&session_id, "disable", Some("demo")), &client)
            .await
            .unwrap();
        assert_eq!(rpc_ok(&disabled), &json!({}));
        let listed = host
            .handle(session_mcp(&session_id, "list", None), &client)
            .await
            .unwrap();
        assert_eq!(rpc_ok(&listed)["servers"][1]["disabled"], true, "{listed}");

        let enabled = host
            .handle(session_mcp(&session_id, "enable", Some("demo")), &client)
            .await
            .unwrap();
        assert_eq!(rpc_ok(&enabled), &json!({}));
        let listed = host
            .handle(session_mcp(&session_id, "list", None), &client)
            .await
            .unwrap();
        assert_eq!(rpc_ok(&listed)["servers"][1]["disabled"], false, "{listed}");
    }

    #[tokio::test]
    async fn session_mcp_disable_hides_tools_on_later_prompts_and_does_not_persist() {
        let dir = tempdir().unwrap();
        let script = r#"[{"tool_calls":[{"name":"mcp.demo.echo","args_json":"{\"text\":\"hi\"}"}]},{"content":"first"},{"tool_calls":[{"name":"mcp.demo.echo","args_json":"{\"text\":\"hi\"}"}]},{"content":"second"},{"tool_calls":[{"name":"mcp.demo.echo","args_json":"{\"text\":\"hi\"}"}]},{"content":"third"}]"#;
        let host = scripted_host_with(dir.path(), script, with_demo_mcp);
        let client = RecordingClient {
            updates: Mutex::new(Vec::new()),
            permission: PermissionOutcome::Allow,
        };
        let project = dir.path().join("project");
        fs::create_dir_all(&project).unwrap();
        let session_id = init_and_new(&host, &client, &project).await;
        let text = |t: &str| json!([{ "type": "text", "text": t }]);

        let first = prompt(&host, &client, &session_id, text("one")).await;
        assert_eq!(rpc_ok(&first)["stopReason"], "end_turn", "{first}");
        {
            let updates = client.updates.lock().await;
            let details = tool_result_texts(&updates);
            assert!(
                details.iter().any(|detail| detail == "echo:hi"),
                "attached mock must run, got {details:?}"
            );
        }

        let disabled = host
            .handle(session_mcp(&session_id, "disable", Some("demo")), &client)
            .await
            .unwrap();
        assert_eq!(rpc_ok(&disabled), &json!({}));
        client.updates.lock().await.clear();
        let second = prompt(&host, &client, &session_id, text("two")).await;
        assert_eq!(rpc_ok(&second)["stopReason"], "end_turn", "{second}");
        {
            let updates = client.updates.lock().await;
            let details = tool_result_texts(&updates);
            assert!(
                details.iter().any(|detail| {
                    detail.contains("mcp.demo.echo")
                        && (detail.contains("not enabled")
                            || detail.contains("unknown")
                            || detail.contains("denies"))
                }),
                "disabled mock must not run, got {details:?}"
            );
        }

        drop(host);
        let host = scripted_host_with(dir.path(), script, with_demo_mcp);
        let loaded = load_after_restart(&host, &client, &session_id, &project).await;
        assert_eq!(rpc_ok(&loaded)["sessionId"], session_id, "{loaded}");
        let listed = host
            .handle(session_mcp(&session_id, "list", None), &client)
            .await
            .unwrap();
        assert_eq!(
            rpc_ok(&listed)["servers"],
            json!([{"name": "demo", "tools": ["mcp.demo.echo", "mcp.demo.wide"], "disabled": false}]),
            "{listed}"
        );
        client.updates.lock().await.clear();
        let third = prompt(&host, &client, &session_id, text("three")).await;
        assert_eq!(rpc_ok(&third)["stopReason"], "end_turn", "{third}");
        {
            let updates = client.updates.lock().await;
            let details = tool_result_texts(&updates);
            assert!(
                details.iter().any(|detail| detail == "echo:hi"),
                "a restarted host follows settings again, got {details:?}"
            );
        }
    }

    async fn prompt(
        host: &AcpHost,
        client: &dyn AcpClient,
        session_id: &str,
        parts: Value,
    ) -> Value {
        host.handle(
            json!({
                "jsonrpc": "2.0",
                "id": 3,
                "method": "session/prompt",
                "params": { "sessionId": session_id, "prompt": parts }
            }),
            client,
        )
        .await
        .unwrap()
    }

    /// [`load`] on a host started after the session's previous host was
    /// dropped. Retries `sessionInUse` for a moment: a process another test
    /// forks in parallel keeps the old lock's descriptor until it execs.
    async fn load_after_restart(
        host: &AcpHost,
        client: &dyn AcpClient,
        session_id: &str,
        cwd: &Path,
    ) -> Value {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let loaded = load(host, client, session_id, cwd).await;
            if loaded["error"]["data"]["shikigami"]["kind"] != "sessionInUse"
                || std::time::Instant::now() > deadline
            {
                return loaded;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }

    async fn list_sessions(host: &AcpHost, client: &dyn AcpClient, cwd: Option<&Path>) -> Value {
        let mut params = json!({});
        if let Some(cwd) = cwd {
            params["cwd"] = json!(cwd);
        }
        host.handle(
            json!({
                "jsonrpc": "2.0",
                "id": 20,
                "method": "session/list",
                "params": params
            }),
            client,
        )
        .await
        .unwrap()
    }

    /// `initialize` a fresh (restarted) host, then `session/load`.
    async fn load(host: &AcpHost, client: &dyn AcpClient, session_id: &str, cwd: &Path) -> Value {
        host.handle(
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
        host.handle(
            json!({
                "jsonrpc": "2.0",
                "id": 10,
                "method": "session/load",
                "params": { "sessionId": session_id, "cwd": cwd, "mcpServers": [] }
            }),
            client,
        )
        .await
        .unwrap()
    }

    /// ADR 0021: attachments on later prompts, kept across a host restart,
    /// gone with the session's run; a text session stays text.
    #[tokio::test]
    async fn content_session_takes_later_attachments_and_survives_restart() {
        let dir = tempdir().unwrap();
        let host = scripted_host(
            dir.path(),
            r#"[{"content":"one"},{"content":"two"},{"content":"three"},{"content":"text"}]"#,
        );
        let client = RecordingClient {
            updates: Mutex::new(Vec::new()),
            permission: PermissionOutcome::Allow,
        };
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let session_id = init_and_new(&host, &client, &cwd).await;
        let png = b"\x89PNG UNIQUE-ACP-537-FIRST";
        let pdf = b"%PDF UNIQUE-ACP-537-THIRD";
        let image = json!([
            {"type":"text","text":"inspect"},
            {"type":"image","mimeType":"image/png","data": encode_base64(png)}
        ]);
        let document = json!([
            {"type":"text","text":"and this"},
            {"type":"resource","resource":{
                "uri":"attachment://doc.pdf","mimeType":"application/pdf","blob": encode_base64(pdf)
            }}
        ]);
        let text = json!([{"type":"text","text":"go on"}]);
        for parts in [&image, &text, &document] {
            let reply = prompt(&host, &client, &session_id, parts.clone()).await;
            assert_eq!(rpc_ok(&reply)["stopReason"], "end_turn", "{reply}");
        }
        assert_no_payload_bytes(&cwd, pdf);
        let payloads = host.content_dir(&session_id);
        assert!(std::fs::read_dir(&payloads).unwrap().count() >= 2);

        let text_session = init_and_new(&host, &client, &cwd).await;
        let reply = prompt(&host, &client, &text_session, text.clone()).await;
        assert_eq!(rpc_ok(&reply)["stopReason"], "end_turn");
        let refused = prompt(&host, &client, &text_session, image.clone()).await;
        assert_eq!(refused["error"]["code"], -32602, "{refused}");

        // A restarted host loads the session and resolves every part again.
        // The scripted model indexes its script by the run's turn number.
        drop(host);
        let restarted = scripted_host(
            dir.path(),
            r#"[{"content":"1"},{"content":"2"},{"content":"3"},{"content":"after-restart"}]"#,
        );
        assert!(
            restarted
                .sessions_for_cwd(&cwd)
                .iter()
                .any(|s| s.session_id == session_id)
        );
        let loaded = load_after_restart(&restarted, &client, &session_id, &cwd).await;
        assert_eq!(rpc_ok(&loaded)["sessionId"], session_id.as_str());
        let reply = prompt(&restarted, &client, &session_id, text.clone()).await;
        assert_eq!(rpc_ok(&reply)["stopReason"], "end_turn", "{reply}");

        // Ending the session's run drops its payloads.
        restarted.set_run_id(&session_id, None).await.unwrap();
        assert!(!payloads.exists());
    }

    /// #595: a session another host holds is listed as in use and refused
    /// with `sessionInUse`; once that host is gone it loads.
    #[tokio::test]
    async fn a_held_session_is_refused_until_its_host_is_gone() {
        let dir = tempdir().unwrap();
        let client = RecordingClient {
            updates: Mutex::new(Vec::new()),
            permission: PermissionOutcome::Allow,
        };
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let holder = scripted_host(dir.path(), r#"[{"content":"ok"}]"#);
        let session_id = init_and_new(&holder, &client, &cwd).await;
        let reply = prompt(
            &holder,
            &client,
            &session_id,
            json!([{"type":"text","text":"hi"}]),
        )
        .await;
        assert_eq!(rpc_ok(&reply)["stopReason"], "end_turn", "{reply}");

        let second = scripted_host(dir.path(), r#"[{"content":"ok"}]"#);
        let listed = second.sessions_for_cwd(&cwd);
        assert!(
            listed
                .iter()
                .all(|s| s.session_id == session_id && s.in_use),
            "{}",
            listed.len()
        );
        let refused = load(&second, &client, &session_id, &cwd).await;
        assert_eq!(
            refused["error"]["data"]["shikigami"]["kind"], "sessionInUse",
            "{refused}"
        );
        assert!(
            !holder.sessions_for_cwd(&cwd)[0].in_use,
            "the holder's own session is free to it"
        );

        drop(holder);
        let loaded = load_after_restart(&second, &client, &session_id, &cwd).await;
        assert_eq!(rpc_ok(&loaded)["sessionId"], session_id.as_str());
    }

    /// #696: `session/list` is the discovery method; it does not take the lock.
    #[tokio::test]
    async fn session_list_filters_by_cwd_and_reports_in_use() {
        let dir = tempdir().unwrap();
        let client = RecordingClient {
            updates: Mutex::new(Vec::new()),
            permission: PermissionOutcome::Allow,
        };
        let cwd = dir.path().join("project");
        let other = dir.path().join("other");
        std::fs::create_dir_all(&cwd).unwrap();
        std::fs::create_dir_all(&other).unwrap();
        let holder = scripted_host(dir.path(), r#"[{"content":"ok"}]"#);
        let session_id = init_and_new(&holder, &client, &cwd).await;
        let reply = prompt(
            &holder,
            &client,
            &session_id,
            json!([{"type":"text","text":"fix the login test"}]),
        )
        .await;
        assert_eq!(rpc_ok(&reply)["stopReason"], "end_turn", "{reply}");
        let elsewhere = init_and_new(&holder, &client, &other).await;
        let reply = prompt(
            &holder,
            &client,
            &elsewhere,
            json!([{"type":"text","text":"elsewhere"}]),
        )
        .await;
        assert_eq!(rpc_ok(&reply)["stopReason"], "end_turn", "{reply}");

        let listed = list_sessions(&holder, &client, Some(&cwd)).await;
        let sessions = rpc_ok(&listed)["sessions"].as_array().unwrap();
        assert_eq!(sessions.len(), 1, "{listed}");
        assert_eq!(sessions[0]["sessionId"], session_id.as_str());
        assert_eq!(sessions[0]["cwd"], cwd.to_string_lossy().as_ref());
        assert_eq!(sessions[0]["title"], "fix the login test");
        assert_eq!(sessions[0]["_meta"]["shikigami"]["inUse"], false);
        assert!(sessions[0]["updatedAt"].as_str().unwrap().ends_with('Z'));
        assert!(rpc_ok(&listed).get("nextCursor").is_none());

        let all = list_sessions(&holder, &client, None).await;
        let ids: Vec<_> = rpc_ok(&all)["sessions"]
            .as_array()
            .unwrap()
            .iter()
            .map(|item| item["sessionId"].as_str().unwrap().to_string())
            .collect();
        assert!(ids.contains(&session_id), "{all}");
        assert!(ids.contains(&elsewhere), "{all}");

        let second = scripted_host(dir.path(), r#"[{"content":"ok"}]"#);
        second
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
        let from_second = list_sessions(&second, &client, Some(&cwd)).await;
        assert_eq!(
            rpc_ok(&from_second)["sessions"][0]["_meta"]["shikigami"]["inUse"],
            true,
            "{from_second}"
        );
        let refused = load(&second, &client, &session_id, &cwd).await;
        assert_eq!(
            refused["error"]["data"]["shikigami"]["kind"], "sessionInUse",
            "{refused}"
        );

        let bad_cursor = holder
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 21,
                    "method": "session/list",
                    "params": { "cursor": "not-issued" }
                }),
                &client,
            )
            .await
            .unwrap();
        assert_eq!(bad_cursor["error"]["code"], -32602, "{bad_cursor}");
        let relative = holder
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 22,
                    "method": "session/list",
                    "params": { "cwd": "relative" }
                }),
                &client,
            )
            .await
            .unwrap();
        assert_eq!(relative["error"]["code"], -32602, "{relative}");
    }

    #[test]
    fn rfc3339_utc_round_trips_known_instants() {
        let cases = [
            (0, "1970-01-01T00:00:00Z"),
            (86_400, "1970-01-02T00:00:00Z"),
            (1_000_000_000, "2001-09-09T01:46:40Z"),
            (1_704_067_200, "2024-01-01T00:00:00Z"),
        ];
        for (secs, expected) in cases {
            let when = UNIX_EPOCH + Duration::from_secs(secs);
            assert_eq!(rfc3339_utc(when), expected, "{secs}");
            assert_eq!(parse_rfc3339_utc(expected), Some(when), "{expected}");
        }
    }

    #[tokio::test]
    async fn session_load_fails_closed_when_payloads_are_lost() {
        let dir = tempdir().unwrap();
        let host = scripted_host(dir.path(), r#"[{"content":"saw-image"}]"#);
        let client = RecordingClient {
            updates: Mutex::new(Vec::new()),
            permission: PermissionOutcome::Allow,
        };
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let session_id = init_and_new(&host, &client, &cwd).await;
        let reply = prompt(
            &host,
            &client,
            &session_id,
            json!([
                {"type":"text","text":"inspect"},
                {"type":"image","mimeType":"image/png","data": encode_base64(b"\x89PNG LOST")}
            ]),
        )
        .await;
        assert_eq!(rpc_ok(&reply)["stopReason"], "end_turn");
        std::fs::remove_dir_all(host.content_dir(&session_id)).unwrap();
        drop(host);
        let restarted = scripted_host(dir.path(), r#"[{"content":"hello"}]"#);
        assert!(
            !restarted
                .sessions_for_cwd(&cwd)
                .iter()
                .any(|s| s.session_id == session_id)
        );
        let loaded = load_after_restart(&restarted, &client, &session_id, &cwd).await;
        assert_eq!(loaded["error"]["code"], -32603, "{loaded}");
    }

    #[tokio::test]
    async fn host_start_sweeps_payloads_without_a_session_record() {
        let dir = tempdir().unwrap();
        let host = scripted_host(dir.path(), "[]");
        let orphan = host.content_dir("sess-gone");
        write_private(&orphan.join("payload"), b"x").unwrap();
        drop(host);
        let _restarted = scripted_host(dir.path(), "[]");
        assert!(!orphan.exists());
    }

    #[test]
    fn follow_up_admission_holds_the_session_bounds() {
        let part = |bytes: u64| ContentPartDescriptor {
            part_id: "p".into(),
            kind: ContentPartKind::Image,
            media_type: "image/png".into(),
            byte_length: bytes,
            sha256_digest: String::new(),
            reference: String::new(),
            provenance: ContentProvenanceV1 {
                source: "acp".into(),
                source_id: "s".into(),
                source_version: "v1".into(),
                observed_at_ms: 0,
            },
            disclosure_state: ContentDisclosureState::Accepted,
            disclosure_reason: String::new(),
        };
        let history = |parts: Vec<ContentPartDescriptor>| {
            vec![ContentMessageV1 {
                role: "user".into(),
                parts,
                tool_call_id: String::new(),
                tool_calls: Vec::new(),
            }]
        };
        let follow_up = ParsedPrompt {
            text: "more".into(),
            attachments: vec![StagedAttachment {
                kind: ContentPartKind::Image,
                media_type: "image/png".into(),
                payload: Bytes::from(vec![0; 4 * 1024 * 1024]),
            }],
        };
        let mib = 1024 * 1024;
        assert!(
            follow_up
                .admit_follow_up(&history(vec![part(8 * mib)]))
                .is_ok()
        );
        let full = follow_up.admit_follow_up(&history(vec![part(8 * mib), part(5 * mib)]));
        assert_eq!(full.unwrap_err()["code"], -32602);
        let many = follow_up.admit_follow_up(&history((0..31).map(|_| part(1)).collect()));
        assert_eq!(many.unwrap_err()["code"], -32602);
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
    async fn initialize_scripted_adapter_advertises_content_kinds() {
        let dir = tempdir().unwrap();
        let state = StateRoot::new(dir.path().join("state"));
        let mut config = Config::default();
        config.governance.adapter = "local".into();
        config.model.adapter = "scripted".into();
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
        assert_initialize_speaks_v1_with_prompt(rpc_ok(&init), true, true, true);
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
            // Session payload custody (ADR 0021) is the one allowed copy.
            if path.extension().is_some_and(|ext| ext == "content") {
                return;
            }
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

    fn advertised_names(update: &Value) -> Vec<String> {
        update["update"]["availableCommands"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|command| command["name"].as_str().map(str::to_string))
            .collect()
    }

    fn command_update(updates: &[Value]) -> Option<&Value> {
        updates
            .iter()
            .find(|update| update["update"]["sessionUpdate"] == "available_commands_update")
    }

    #[test]
    fn slash_invocation_matches_advertised_name_then_args() {
        let advertised = HashSet::from(["compact".into(), "model".into(), "demo".into()]);
        assert_eq!(
            slash_invocation("/compact", &advertised),
            Some(("compact".into(), String::new()))
        );
        assert_eq!(
            slash_invocation("  /model gpt-4  ", &advertised),
            Some(("model".into(), "gpt-4".into()))
        );
        assert_eq!(
            slash_invocation("/demo extra words", &advertised),
            Some(("demo".into(), "extra words".into()))
        );
        assert!(slash_invocation("//compact", &advertised).is_none());
        assert!(slash_invocation("/not-a-command", &advertised).is_none());
        assert!(slash_invocation("compact", &advertised).is_none());
    }

    #[tokio::test]
    async fn session_prompt_handles_advertised_slash_commands() {
        let dir = tempdir().unwrap();
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(cwd.join(".agents/skills/demo")).unwrap();
        std::fs::write(
            cwd.join(".agents/skills/demo/SKILL.md"),
            "UNIQUE-ACP-SKILL-BODY\n",
        )
        .unwrap();
        let host = scripted_host(
            dir.path(),
            r#"[{"content":"ordinary"},{"requires_context":["UNIQUE-ACP-SKILL-BODY","extra"],"content":"loaded"}]"#,
        );
        let client = RecordingClient {
            updates: Mutex::new(Vec::new()),
            permission: PermissionOutcome::Allow,
        };
        let session_id = init_and_new(&host, &client, &cwd).await;
        let updates = client.updates.lock().await.clone();
        let advertised = command_update(&updates).expect("available_commands_update after new");
        let names = advertised_names(advertised);
        assert!(names.contains(&"compact".into()), "{names:?}");
        assert!(names.contains(&"model".into()), "{names:?}");
        assert!(names.contains(&"demo".into()), "{names:?}");
        for tui_only in [
            "help", "copy", "attach", "exit", "quit", "new", "resume", "run", "usage",
        ] {
            assert!(!names.iter().any(|name| name == tui_only), "{names:?}");
        }

        client.updates.lock().await.clear();
        let seed = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "session/prompt",
                    "params": {
                        "sessionId": session_id,
                        "prompt": [{"type":"text","text":"/not-a-command"}]
                    }
                }),
                &client,
            )
            .await
            .unwrap();
        assert_eq!(rpc_ok(&seed)["stopReason"], "end_turn", "{seed}");
        let run_id = rpc_ok(&seed)["_meta"]["shikigami"]["runId"]
            .as_str()
            .unwrap()
            .to_string();
        let runs = dir.path().join("state/runs");
        let mut checkpoint = Checkpoint::load(&runs, &run_id).unwrap();
        let user = checkpoint
            .messages
            .iter()
            .find(|message| message.role == "user")
            .expect("user message");
        assert_eq!(user.content, "/not-a-command");
        checkpoint.messages = (0..20)
            .map(|i| ChatMessage {
                role: if i == 0 { "user" } else { "assistant" }.into(),
                content: format!("m{i}"),
                tool_call_id: String::new(),
                tool_calls: vec![],
            })
            .collect();
        checkpoint.save(&runs).unwrap();

        client.updates.lock().await.clear();
        let compacted = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 4,
                    "method": "session/prompt",
                    "params": {
                        "sessionId": session_id,
                        "prompt": [{"type":"text","text":"/compact"}]
                    }
                }),
                &client,
            )
            .await
            .unwrap();
        let before = rpc_ok(&compacted)["before"].as_u64().unwrap();
        let after = rpc_ok(&compacted)["after"].as_u64().unwrap();
        assert_eq!(rpc_ok(&compacted)["stopReason"], "end_turn", "{compacted}");
        assert_eq!(before, 20);
        assert!(after < before, "before={before} after={after}");
        let compact_updates = client.updates.lock().await.clone();
        assert!(
            compact_updates
                .iter()
                .all(|update| update["update"]["sessionUpdate"] != "agent_message_chunk"),
            "{compact_updates:?}"
        );
        let loaded = Checkpoint::load(&runs, &run_id).unwrap();
        assert_eq!(loaded.messages.len() as u64, after);

        client.updates.lock().await.clear();
        let skill = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 5,
                    "method": "session/prompt",
                    "params": {
                        "sessionId": session_id,
                        "prompt": [{"type":"text","text":"/demo extra"}]
                    }
                }),
                &client,
            )
            .await
            .unwrap();
        assert_eq!(rpc_ok(&skill)["stopReason"], "end_turn", "{skill}");
        let skill_run = rpc_ok(&skill)["_meta"]["shikigami"]["runId"]
            .as_str()
            .unwrap()
            .to_string();
        let after_skill = Checkpoint::load(&runs, &skill_run).unwrap();
        let skill_user = after_skill
            .messages
            .iter()
            .rev()
            .find(|message| message.role == "user")
            .expect("skill user text");
        assert_eq!(skill_user.content, "extra");
        assert!(!skill_user.content.contains("UNIQUE-ACP-SKILL-BODY"));
        assert!(!skill_user.content.contains("/demo"));
        let tool = after_skill
            .messages
            .iter()
            .rev()
            .find(|message| message.role == "tool")
            .expect("load_skill result");
        assert!(tool.content.contains("UNIQUE-ACP-SKILL-BODY"), "{tool:?}");
        assert!(tool.content.contains("# Skill `demo`"), "{tool:?}");
        let skill_updates = client.updates.lock().await.clone();
        assert!(
            skill_updates.iter().any(|update| {
                update.pointer("/update/title").and_then(Value::as_str) == Some("load_skill")
            }),
            "{skill_updates:?}"
        );

        let typed = host
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
        assert!(rpc_ok(&typed).get("before").is_some(), "{typed}");
        assert!(rpc_ok(&typed).get("after").is_some(), "{typed}");
    }

    #[tokio::test]
    async fn session_load_advertises_available_commands() {
        let dir = tempdir().unwrap();
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(cwd.join(".agents/skills/demo")).unwrap();
        std::fs::write(
            cwd.join(".agents/skills/demo/SKILL.md"),
            "---\ndescription: demo pack\n---\nbody\n",
        )
        .unwrap();
        let host = scripted_host(dir.path(), r#"[{"content":"hello"}]"#);
        let client = RecordingClient {
            updates: Mutex::new(Vec::new()),
            permission: PermissionOutcome::Allow,
        };
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
        let names = rpc_ok(&loaded)["availableCommands"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|command| command["name"].as_str())
            .collect::<Vec<_>>();
        assert!(names.contains(&"compact"), "{loaded}");
        assert!(names.contains(&"demo"), "{loaded}");
        let updates = replay.updates.lock().await.clone();
        let advertised = command_update(&updates).expect("available_commands_update after load");
        assert!(advertised_names(advertised).contains(&"compact".into()));
    }

    #[tokio::test]
    async fn session_prompt_meta_lists_attached_context() {
        let dir = tempdir().unwrap();
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(cwd.join(".shikigami/skills/demo")).unwrap();
        std::fs::write(cwd.join("AGENTS.md"), "prefer tests\n").unwrap();
        std::fs::write(
            cwd.join(".shikigami/skills/demo/SKILL.md"),
            "---\ndescription: demo pack\n---\nbody\n",
        )
        .unwrap();
        let host = scripted_host_with(dir.path(), r#"[{"content":"hello"}]"#, |config| {
            config.context.skills = vec!["demo".into()];
        });
        let client = RecordingClient {
            updates: Mutex::new(Vec::new()),
            permission: PermissionOutcome::Allow,
        };
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
        let context = &rpc_ok(&prompt)["_meta"]["shikigami"]["context"];
        assert_eq!(context["rules"][0]["filename"], "AGENTS.md");
        assert_eq!(context["rules"][0]["bytes"], "prefer tests\n".len() as u64);
        assert!(context["eager"].as_array().is_none_or(|v| v.is_empty()));
        assert_eq!(context["catalog"], json!(["demo"]));
        assert_ne!(context["nested"], true);
        assert!(
            context.get("truncated").is_none(),
            "bodies stay out of _meta: {context}"
        );
        let dumped = context.to_string();
        assert!(!dumped.contains("prefer tests"), "{dumped}");
        assert!(!dumped.contains("demo pack"), "{dumped}");
    }

    #[tokio::test]
    async fn session_prompt_load_skill_meta_injects_tool_result() {
        let dir = tempdir().unwrap();
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(cwd.join(".agents/skills/demo")).unwrap();
        std::fs::write(
            cwd.join(".agents/skills/demo/SKILL.md"),
            "UNIQUE-ACP-SKILL-BODY\n",
        )
        .unwrap();
        let host = scripted_host(
            dir.path(),
            r#"[{"requires_context":["UNIQUE-ACP-SKILL-BODY","extra"],"content":"loaded"}]"#,
        );
        let client = RecordingClient {
            updates: Mutex::new(Vec::new()),
            permission: PermissionOutcome::Allow,
        };
        let session_id = init_and_new(&host, &client, &cwd).await;
        let prompt = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "session/prompt",
                    "params": {
                        "sessionId": session_id,
                        "prompt": [{"type":"text","text":"extra"}],
                        "_meta": { "shikigami": { "loadSkill": "demo" } }
                    }
                }),
                &client,
            )
            .await
            .unwrap();
        assert_eq!(rpc_ok(&prompt)["stopReason"], "end_turn", "{prompt}");
        let run_id = rpc_ok(&prompt)["_meta"]["shikigami"]["runId"]
            .as_str()
            .unwrap()
            .to_string();
        let loaded = Checkpoint::load(&dir.path().join("state/runs"), &run_id).unwrap();
        let user = loaded
            .messages
            .iter()
            .find(|message| message.role == "user")
            .expect("user message");
        assert_eq!(user.content, "extra");
        assert!(!user.content.contains("UNIQUE-ACP-SKILL-BODY"));
        let tool = loaded
            .messages
            .iter()
            .find(|message| message.role == "tool")
            .expect("load_skill result");
        assert!(tool.content.contains("UNIQUE-ACP-SKILL-BODY"), "{tool:?}");
        assert!(tool.content.contains("# Skill `demo`"), "{tool:?}");
        let updates = client.updates.lock().await.clone();
        assert!(
            updates.iter().any(|update| {
                update.pointer("/update/title").and_then(Value::as_str) == Some("load_skill")
            }),
            "{updates:?}"
        );
    }

    #[tokio::test]
    async fn session_prompt_load_skill_unknown_fails_closed() {
        let dir = tempdir().unwrap();
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let host = scripted_host(dir.path(), r#"[{"content":"unused"}]"#);
        let client = RecordingClient {
            updates: Mutex::new(Vec::new()),
            permission: PermissionOutcome::Allow,
        };
        let session_id = init_and_new(&host, &client, &cwd).await;
        let prompt = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "session/prompt",
                    "params": {
                        "sessionId": session_id,
                        "prompt": [{"type":"text","text":"extra"}],
                        "_meta": { "shikigami": { "loadSkill": "missing" } }
                    }
                }),
                &client,
            )
            .await
            .unwrap();
        assert_eq!(prompt["error"]["code"], -32603, "{prompt}");
        assert!(
            prompt["error"]["message"]
                .as_str()
                .unwrap_or("")
                .contains("no skill `missing`"),
            "{prompt}"
        );
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
    async fn ask_park_session_allow_skips_later_same_tool() {
        let dir = tempdir().unwrap();
        let host = scripted_host(
            dir.path(),
            r#"[
              {"tool_calls":[
                {"name":"write_file","args_json":"{\"path\":\"a.txt\",\"content\":\"a\\n\"}"},
                {"name":"write_file","args_json":"{\"path\":\"b.txt\",\"content\":\"b\\n\"}"},
                {"name":"edit","args_json":"{\"path\":\"a.txt\",\"old\":\"a\\n\",\"new\":\"aa\\n\"}"}
              ]},
              {"content":"wrote them"}
            ]"#,
        );
        let client = CapturePermissionClient {
            updates: Mutex::new(Vec::new()),
            permissions: Mutex::new(Vec::new()),
            permission: PermissionOutcome::AllowSession,
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
        let asked: Vec<String> = client
            .permissions
            .lock()
            .await
            .iter()
            .map(|params| {
                params["toolCall"]["title"]
                    .as_str()
                    .unwrap_or("")
                    .to_string()
            })
            .collect();
        assert_eq!(asked.len(), 2, "{asked:?}");
        assert!(
            asked[0].contains("write_file"),
            "first ask is write_file: {asked:?}"
        );
        assert!(
            asked[1].contains("edit"),
            "session grant does not cover edit: {asked:?}"
        );
        let options = &client.permissions.lock().await[0]["options"];
        assert!(
            options
                .as_array()
                .is_some_and(|opts| opts.iter().any(|opt| opt["optionId"] == "allow_session")),
            "mutating ask offers session allow: {options}"
        );
        assert_eq!(std::fs::read_to_string(cwd.join("a.txt")).unwrap(), "aa\n");
        assert_eq!(std::fs::read_to_string(cwd.join("b.txt")).unwrap(), "b\n");
    }

    #[tokio::test]
    async fn session_load_does_not_revive_ask_grants() {
        let dir = tempdir().unwrap();
        let script = r#"[
              {"tool_calls":[{"name":"write_file","args_json":"{\"path\":\"a.txt\",\"content\":\"a\\n\"}"}]},
              {"content":"first"},
              {"tool_calls":[{"name":"write_file","args_json":"{\"path\":\"b.txt\",\"content\":\"b\\n\"}"}]},
              {"content":"second"}
            ]"#;
        let host = scripted_host(dir.path(), script);
        let client = CapturePermissionClient {
            updates: Mutex::new(Vec::new()),
            permissions: Mutex::new(Vec::new()),
            permission: PermissionOutcome::AllowSession,
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
                        "prompt": [{"type":"text","text":"write"}]
                    }
                }),
                &client,
            )
            .await
            .unwrap();
        assert_eq!(rpc_ok(&first)["stopReason"], "end_turn");
        assert_eq!(client.permissions.lock().await.len(), 1);

        host.handle(
            json!({
                "jsonrpc": "2.0",
                "id": 4,
                "method": "session/load",
                "params": { "sessionId": session_id }
            }),
            &client,
        )
        .await
        .unwrap();
        client.permissions.lock().await.clear();
        let second = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 5,
                    "method": "session/prompt",
                    "params": {
                        "sessionId": session_id,
                        "prompt": [{"type":"text","text":"write again"}]
                    }
                }),
                &client,
            )
            .await
            .unwrap();
        assert_eq!(rpc_ok(&second)["stopReason"], "end_turn");
        assert_eq!(
            client.permissions.lock().await.len(),
            1,
            "load must drop session grants"
        );
        assert_eq!(std::fs::read_to_string(cwd.join("b.txt")).unwrap(), "b\n");
    }

    /// A process that is gone or a zombie awaiting its reaper.
    fn process_alive(pid: &str) -> bool {
        let out = std::process::Command::new("ps")
            .args(["-o", "stat=", "-p", pid])
            .output()
            .unwrap();
        let stat = String::from_utf8_lossy(&out.stdout);
        let stat = stat.trim();
        !stat.is_empty() && !stat.starts_with('Z')
    }

    /// Pid that `echo $$ > pid.txt; exec sleep 30` wrote in the workspace.
    async fn first_job_pid(cwd: &Path) -> String {
        let pid_file = cwd.join("pid.txt");
        for _ in 0..250 {
            let pid = std::fs::read_to_string(&pid_file).unwrap_or_default();
            if !pid.trim().is_empty() {
                return pid.trim().to_string();
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        panic!("job never wrote its pid");
    }

    #[tokio::test]
    async fn background_job_outlives_session_parks_and_dies_with_the_host() {
        let dir = tempdir().unwrap();
        let host = scripted_host_with(
            dir.path(),
            r#"[
              {"tool_calls":[{"name":"bash_background","args_json":"{\"command\":\"echo $$ > pid.txt; exec sleep 30\"}"}]},
              {"content":"started"},
              {"tool_calls":[{"name":"write_file","args_json":"{\"path\":\"note.txt\",\"content\":\"x\"}"}]},
              {"tool_calls":[{"name":"bash_job_status","args_json":"{\"job_id\":\"job-1\"}"}]},
              {"content":"checked"}
            ]"#,
            |config| config.tools.mode = crate::config::PermissionMode::WorkspaceExec,
        );
        let client = RecordingClient {
            updates: Mutex::new(Vec::new()),
            permission: PermissionOutcome::Allow,
        };
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let session_id = init_and_new(&host, &client, &cwd).await;
        for (id, text) in [(3, "start a job"), (4, "is it running")] {
            let prompt = host
                .handle(
                    json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "method": "session/prompt",
                        "params": {
                            "sessionId": session_id,
                            "prompt": [{"type":"text","text":text}]
                        }
                    }),
                    &client,
                )
                .await
                .unwrap();
            assert_eq!(rpc_ok(&prompt)["stopReason"], "end_turn", "{prompt:?}");
        }
        let updates = format!("{:?}", client.updates.lock().await);
        assert!(
            updates.contains("job_id=job-1 status=running"),
            "job must survive the prompt wait and the approval park: {updates}"
        );
        let pid = first_job_pid(&cwd).await;
        assert!(process_alive(&pid));

        host.harness.registry.reap_parked_background_jobs();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while process_alive(&pid) {
            assert!(
                std::time::Instant::now() < deadline,
                "job {pid} outlived the host"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }

    #[tokio::test]
    async fn cancelled_permission_wait_reaps_parked_background_jobs() {
        let dir = tempdir().unwrap();
        let host = scripted_host_with(
            dir.path(),
            r#"[
              {"tool_calls":[{"name":"bash_background","args_json":"{\"command\":\"echo $$ > pid.txt; exec sleep 30\"}"}]},
              {"content":"started"},
              {"tool_calls":[{"name":"write_file","args_json":"{\"path\":\"note.txt\",\"content\":\"x\"}"}]}
            ]"#,
            |config| config.tools.mode = crate::config::PermissionMode::WorkspaceExec,
        );
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let allow = RecordingClient {
            updates: Mutex::new(Vec::new()),
            permission: PermissionOutcome::Allow,
        };
        let session_id = init_and_new(&host, &allow, &cwd).await;
        let mut stops = Vec::new();
        for (id, permission) in [
            (3, PermissionOutcome::Allow),
            (4, PermissionOutcome::Cancelled),
        ] {
            let client = RecordingClient {
                updates: Mutex::new(Vec::new()),
                permission,
            };
            let prompt = host
                .handle(
                    json!({
                        "jsonrpc": "2.0",
                        "id": id,
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
            stops.push(rpc_ok(&prompt)["stopReason"].clone());
            if id == 3 {
                let pid = first_job_pid(&cwd).await;
                assert!(process_alive(&pid), "job must survive the prompt wait");
            }
        }
        assert_eq!(stops, [json!("end_turn"), json!("cancelled")]);
        let pid = first_job_pid(&cwd).await;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while process_alive(&pid) {
            assert!(
                std::time::Instant::now() < deadline,
                "cancel left job {pid} running"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
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

    fn chunk_texts(updates: &[Value]) -> Vec<String> {
        updates
            .iter()
            .filter(|u| u["update"]["sessionUpdate"] == "agent_message_chunk")
            .map(|u| u["update"]["content"]["text"].as_str().unwrap().to_owned())
            .collect()
    }

    #[tokio::test]
    async fn streamed_turn_reaches_the_client_in_order_once() {
        let dir = tempdir().unwrap();
        let host = scripted_host(
            dir.path(),
            r#"[{"chunks":["Hel","lo ","world"],"chunk_delay_ms":50}]"#,
        );
        let client = RecordingClient {
            updates: Mutex::new(Vec::new()),
            permission: PermissionOutcome::Allow,
        };
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let session_id = init_and_new(&host, &client, &cwd).await;
        let reply = host
            .handle(
                json!({
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "session/prompt",
                    "params": { "sessionId": session_id, "prompt": [{"type":"text","text":"hi"}] }
                }),
                &client,
            )
            .await
            .unwrap();
        assert_eq!(rpc_ok(&reply)["stopReason"], "end_turn");
        let updates = client.updates.lock().await;
        let chunks = chunk_texts(&updates);
        assert!(
            chunks.len() >= 2,
            "text arrived before the turn ended: {chunks:?}"
        );
        assert_eq!(chunks.concat(), "Hello world");
        assert!(
            updates.iter().all(|u| u["update"]
                .as_object()
                .is_some_and(|o| o.keys().all(|k| !k.starts_with("_shikigami")))),
            "host markers leaked: {updates:?}"
        );
        let run_id = host.sessions.lock().await[&session_id]
            .run_id
            .clone()
            .unwrap();
        let checkpoint = Checkpoint::load(&host.harness.state.runs_dir(), &run_id).unwrap();
        let assistant = checkpoint
            .messages
            .iter()
            .rfind(|m| m.role == "assistant")
            .unwrap();
        assert_eq!(assistant.content, "Hello world");
    }

    #[tokio::test]
    async fn cancel_during_a_streamed_turn_keeps_the_session_usable() {
        let dir = tempdir().unwrap();
        let host = Arc::new(scripted_host(
            dir.path(),
            r#"[{"chunks":["a","b","c","d","e","f","g","h"],"chunk_delay_ms":50}]"#,
        ));
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let setup = RecordingClient {
            updates: Mutex::new(Vec::new()),
            permission: PermissionOutcome::Allow,
        };
        let session_id = init_and_new(&host, &setup, &cwd).await;
        let client = CancelOnChunkClient {
            host: Arc::clone(&host),
            session_id: session_id.clone(),
            updates: Mutex::new(Vec::new()),
        };
        let prompt = |id: u64| {
            json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": "session/prompt",
                "params": { "sessionId": session_id, "prompt": [{"type":"text","text":"go"}] }
            })
        };
        let reply = host.handle(prompt(3), &client).await.unwrap();
        assert_eq!(rpc_ok(&reply)["stopReason"], "cancelled");
        let partial = chunk_texts(&client.updates.lock().await).concat();
        assert!(partial.len() < 8, "cancel stopped the stream: {partial}");

        // The cancelled turn is not durable; the follow-up plans it again.
        let reply = host.handle(prompt(4), &setup).await.unwrap();
        assert_eq!(rpc_ok(&reply)["stopReason"], "end_turn", "{reply}");
        assert_eq!(
            chunk_texts(&setup.updates.lock().await).concat(),
            "abcdefgh"
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

    fn session_plan_rpc(session_id: &str) -> Value {
        json!({
            "jsonrpc": "2.0",
            "id": 4,
            "method": "session/plan",
            "params": { "sessionId": session_id }
        })
    }

    fn plan_jail_turns() -> String {
        let write = |path: &str, content: &str| {
            json!({
                "tool_calls": [{
                    "name": "write_file",
                    "args_json": json!({"path": path, "content": content}).to_string()
                }]
            })
        };
        let report = json!({
            "tool_calls": [{
                "name": "report",
                "args_json": json!({"summary": "planned", "success": true}).to_string()
            }]
        });
        json!([
            write("other.txt", "no\n"),
            write(crate::tools::PLAN_JAIL_PATH, "# do it\n"),
            report,
            write("ok.txt", "yes\n"),
            {"content": "executed"}
        ])
        .to_string()
    }

    /// #686: `session/plan` jails the next prompt without flipping settings.
    #[tokio::test]
    async fn session_plan_jails_the_next_prompt_without_settings() {
        let dir = tempdir().unwrap();
        let host = scripted_host(dir.path(), &plan_jail_turns());
        assert!(
            !host.harness.config.run.plan_jail,
            "default sessions stay unjailed"
        );
        let client = RecordingClient {
            updates: Mutex::new(Vec::new()),
            permission: PermissionOutcome::Allow,
        };
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let session_id = init_and_new(&host, &client, &cwd).await;

        let flagged = host
            .handle(session_plan_rpc(&session_id), &client)
            .await
            .unwrap();
        assert_eq!(rpc_ok(&flagged), &json!({ "planJail": true }), "{flagged}");
        let again = host
            .handle(session_plan_rpc(&session_id), &client)
            .await
            .unwrap();
        assert_eq!(
            rpc_ok(&again),
            &json!({ "planJail": true, "already": true }),
            "{again}"
        );
        assert!(!host.harness.config.run.plan_jail);

        let prompt = prompt(
            &host,
            &client,
            &session_id,
            json!([{"type":"text","text":"draft"}]),
        )
        .await;
        assert_eq!(rpc_ok(&prompt)["stopReason"], "end_turn", "{prompt}");
        assert!(!cwd.join("other.txt").exists());
        assert_eq!(
            std::fs::read_to_string(cwd.join(crate::tools::PLAN_JAIL_PATH)).unwrap(),
            "# do it\n"
        );
        assert_eq!(
            std::fs::read_to_string(cwd.join("ok.txt")).unwrap(),
            "yes\n"
        );
        assert!(!host.harness.config.run.plan_jail);
    }

    /// #686: a session that already ran without the jail cannot enter it.
    #[tokio::test]
    async fn session_plan_refuses_a_run_that_already_started() {
        let dir = tempdir().unwrap();
        let host = scripted_host(dir.path(), r#"[{"content":"ok"}]"#);
        let client = RecordingClient {
            updates: Mutex::new(Vec::new()),
            permission: PermissionOutcome::Allow,
        };
        let cwd = dir.path().join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let session_id = init_and_new(&host, &client, &cwd).await;
        let first = prompt(
            &host,
            &client,
            &session_id,
            json!([{"type":"text","text":"hello"}]),
        )
        .await;
        assert_eq!(rpc_ok(&first)["stopReason"], "end_turn", "{first}");

        let refused = host
            .handle(session_plan_rpc(&session_id), &client)
            .await
            .unwrap();
        assert_eq!(refused["error"]["code"], -32602, "{refused}");
        let message = refused["error"]["message"].as_str().unwrap();
        assert!(
            message.contains("already ran without the jail"),
            "{refused}"
        );
        assert!(!message.contains("plan_jail"), "{refused}");
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
        assert!(matches!(
            parse_permission(&json!({
                "outcome": {"outcome": "selected", "optionId": "allow_session"}
            }))
            .unwrap(),
            PermissionOutcome::AllowSession
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
            stop_repairs: 0,
            preset: String::new(),
            artifacts: Vec::new(),
            trace_context: None,
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
        let (updates, run_id) = drain_updates("sess", &mut rx, &mut HashSet::new());
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
        let (overflowed, _) = drain_updates("sess", &mut rx, &mut HashSet::new());
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
