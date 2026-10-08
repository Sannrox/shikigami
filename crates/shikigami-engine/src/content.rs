//! Bounded, metadata-only content contracts for library embedders.
//!
//! Payloads are transient values owned by a host-supplied resolver. Durable
//! harness state stores only validated descriptors and resolver bindings.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
pub use bytes::Bytes;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::watch;

use crate::checkpoint::{self, Checkpoint};
use crate::model::{TokenUsage, ToolCall};
use crate::run::{AskDecision, PlanDecision, RunResult, RunTermination};

pub const CONTENT_SCHEMA_VERSION: u32 = 1;
pub const CONTENT_TRANSCRIPT_SCHEMA_VERSION: u32 = 1;
pub const CONTENT_PROCESS_REQUEST_SCHEMA_VERSION: u32 = 1;
pub const CONTENT_PROCESS_RESULT_SCHEMA_VERSION: u32 = 1;
pub const CONTENT_CONTRACT_VERSION: &str = "chisei.content-execution/v1";
pub const CLI_FILE_RESOLVER_ID: &str = "cli-file-v1";
pub const MAX_CONTENT_PARTS: usize = 32;
pub const MAX_CONTENT_PART_BYTES: u64 = 8 * 1024 * 1024;
pub const MAX_CONTENT_AGGREGATE_BYTES: u64 = 16 * 1024 * 1024;
pub const MAX_CONTENT_PROCESS_REQUEST_BYTES: usize = 1024 * 1024;

const SIDECAR_SLOT_A: &str = "content-checkpoint-a.json";
const SIDECAR_SLOT_B: &str = "content-checkpoint-b.json";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContentPartKind {
    Text,
    Image,
    Audio,
    Document,
}

impl ContentPartKind {
    pub fn proto_value(self) -> i32 {
        match self {
            Self::Text => 1,
            Self::Image => 2,
            Self::Audio => 3,
            Self::Document => 4,
        }
    }

    pub fn from_proto(value: i32) -> Result<Self, ContentError> {
        match value {
            1 => Ok(Self::Text),
            2 => Ok(Self::Image),
            3 => Ok(Self::Audio),
            4 => Ok(Self::Document),
            _ => Err(ContentError::Invalid("unknown content part kind".into())),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContentDisclosureState {
    Accepted,
    Redacted,
    Omitted,
}

impl ContentDisclosureState {
    pub fn proto_value(self) -> i32 {
        match self {
            Self::Accepted => 1,
            Self::Redacted => 2,
            Self::Omitted => 3,
        }
    }

    pub fn from_proto(value: i32) -> Result<Self, ContentError> {
        match value {
            1 => Ok(Self::Accepted),
            2 => Ok(Self::Redacted),
            3 => Ok(Self::Omitted),
            _ => Err(ContentError::Invalid(
                "unknown content disclosure state".into(),
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContentProvenanceV1 {
    pub source: String,
    pub source_id: String,
    pub source_version: String,
    pub observed_at_ms: i64,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContentPartDescriptor {
    pub part_id: String,
    pub kind: ContentPartKind,
    pub media_type: String,
    pub byte_length: u64,
    pub sha256_digest: String,
    pub reference: String,
    pub provenance: ContentProvenanceV1,
    pub disclosure_state: ContentDisclosureState,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub disclosure_reason: String,
}

impl fmt::Debug for ContentPartDescriptor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ContentPartDescriptor")
            .field("part_id", &self.part_id)
            .field("kind", &self.kind)
            .field("media_type", &self.media_type)
            .field("byte_length", &self.byte_length)
            .field("sha256_digest", &self.sha256_digest)
            .field("reference", &"[REDACTED]")
            .field("provenance", &self.provenance)
            .field("disclosure_state", &self.disclosure_state)
            .field("disclosure_reason", &self.disclosure_reason)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContentMessageV1 {
    pub role: String,
    pub parts: Vec<ContentPartDescriptor>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub tool_call_id: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<ToolCall>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContentCapabilitiesV1 {
    pub contract_version: String,
    pub input_kinds: Vec<ContentPartKind>,
    pub output_kinds: Vec<ContentPartKind>,
    pub media_types: Vec<String>,
    pub reference_modes: Vec<String>,
    pub max_parts: u32,
    pub max_part_bytes: u64,
    pub max_aggregate_bytes: u64,
    pub streaming: bool,
}

impl ContentCapabilitiesV1 {
    pub fn bounded_for(messages: &[ContentMessageV1]) -> Self {
        let mut input_kinds = Vec::new();
        let mut media_types = Vec::new();
        for descriptor in messages.iter().flat_map(|message| &message.parts) {
            if !input_kinds.contains(&descriptor.kind) {
                input_kinds.push(descriptor.kind);
            }
            if !media_types.contains(&descriptor.media_type) {
                media_types.push(descriptor.media_type.clone());
            }
        }
        if !input_kinds.contains(&ContentPartKind::Text) {
            input_kinds.push(ContentPartKind::Text);
        }
        if !media_types
            .iter()
            .any(|media_type| media_type == "text/plain")
        {
            media_types.push("text/plain".into());
        }
        Self {
            contract_version: CONTENT_CONTRACT_VERSION.into(),
            input_kinds,
            output_kinds: vec![ContentPartKind::Text],
            media_types,
            reference_modes: vec!["opaque".into()],
            max_parts: MAX_CONTENT_PARTS as u32,
            max_part_bytes: MAX_CONTENT_PART_BYTES,
            max_aggregate_bytes: MAX_CONTENT_AGGREGATE_BYTES,
            streaming: true,
        }
    }

    pub fn validate(&self) -> Result<(), ContentError> {
        if self.contract_version != CONTENT_CONTRACT_VERSION
            || self.output_kinds != [ContentPartKind::Text]
            || self.reference_modes != ["opaque"]
            || !self.streaming
            || self.max_parts == 0
            || self.max_parts as usize > MAX_CONTENT_PARTS
            || self.max_part_bytes == 0
            || self.max_part_bytes > MAX_CONTENT_PART_BYTES
            || self.max_aggregate_bytes < self.max_part_bytes
            || self.max_aggregate_bytes > MAX_CONTENT_AGGREGATE_BYTES
        {
            return Err(ContentError::Invalid(
                "content capabilities exceed or contradict hard bounds".into(),
            ));
        }
        if self.input_kinds.is_empty()
            || self.input_kinds.len() > 4
            || self.input_kinds.iter().collect::<HashSet<_>>().len() != self.input_kinds.len()
        {
            return Err(ContentError::Invalid(
                "content capability input kinds are invalid".into(),
            ));
        }
        if self.media_types.is_empty()
            || self.media_types.len() > 16
            || self.media_types.iter().collect::<HashSet<_>>().len() != self.media_types.len()
            || self
                .media_types
                .iter()
                .any(|media_type| media_type.len() > 64 || !supported_media_type(media_type))
        {
            return Err(ContentError::Invalid(
                "content capability media types are unsupported or unbounded".into(),
            ));
        }
        Ok(())
    }
}

pub enum ResolvedContent {
    Text(String),
    /// Shared immutable payload; cloning a handle does not copy decoded bytes.
    Bytes(Bytes),
}

pub struct ResolvedContentPart {
    pub descriptor: ContentPartDescriptor,
    pub payload: ResolvedContent,
}

impl fmt::Debug for ResolvedContentPart {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResolvedContentPart")
            .field("descriptor", &self.descriptor)
            .field("payload", &self.payload)
            .finish()
    }
}

impl fmt::Debug for ResolvedContent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (kind, byte_length) = match self {
            Self::Text(text) => ("text", text.len()),
            Self::Bytes(bytes) => ("bytes", bytes.len()),
        };
        formatter
            .debug_struct("ResolvedContent")
            .field("kind", &kind)
            .field("byte_length", &byte_length)
            .field("payload", &"[REDACTED]")
            .finish()
    }
}

impl ResolvedContent {
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Text(text) => text.as_bytes(),
            Self::Bytes(bytes) => bytes,
        }
    }
}

pub struct ContentToStore {
    pub part_id: String,
    pub kind: ContentPartKind,
    pub media_type: String,
    pub payload: ResolvedContent,
    pub provenance: ContentProvenanceV1,
}

#[async_trait]
pub trait ContentResolver: Send + Sync {
    /// Stable identity for the host-owned payload store/resolver binding.
    fn id(&self) -> &str;

    /// Resolve one accepted descriptor for a single authorized call.
    async fn resolve(
        &self,
        descriptor: &ContentPartDescriptor,
    ) -> Result<ResolvedContent, ContentError>;

    /// Externalize transient model/tool output and return durable metadata.
    async fn store(&self, content: ContentToStore) -> Result<ContentPartDescriptor, ContentError>;
}

#[derive(Clone)]
pub struct ContentRunRequestV1 {
    pub task: String,
    pub messages: Vec<ContentMessageV1>,
    pub capabilities: ContentCapabilitiesV1,
    pub resolver: Arc<dyn ContentResolver>,
    pub keep_workspace: bool,
    pub timeout: Option<Duration>,
    pub cancel: Option<watch::Receiver<bool>>,
    pub resume_run_id: Option<String>,
    pub logical_operation_id: Option<String>,
    pub restore_snapshot: Option<String>,
    /// ACP/TUI session host: no-tool assistant waits; mutating tools ask=park.
    /// Unattended `run_content` leaves this false.
    pub session_wait: bool,
    /// Follow-up user prompt when resuming a `ParkKind::PromptWait` content run.
    pub resume_prompt: Option<String>,
    /// Allow or deny a `ParkKind::Ask` mutating tool on resume.
    pub resume_ask: Option<AskDecision>,
    /// Accept or reject a `ParkKind::Plan` park on resume.
    pub resume_plan: Option<PlanDecision>,
    /// Operator answer when resuming a parked run (from `escalate`).
    pub resume_answer: Option<String>,
}

impl ContentRunRequestV1 {
    pub fn new(
        task: impl Into<String>,
        messages: Vec<ContentMessageV1>,
        resolver: Arc<dyn ContentResolver>,
    ) -> Self {
        let capabilities = ContentCapabilitiesV1::bounded_for(&messages);
        Self {
            task: task.into(),
            messages,
            capabilities,
            resolver,
            keep_workspace: false,
            timeout: None,
            cancel: None,
            resume_run_id: None,
            logical_operation_id: None,
            restore_snapshot: None,
            session_wait: false,
            resume_prompt: None,
            resume_ask: None,
            resume_plan: None,
            resume_answer: None,
        }
    }
}

#[derive(Clone)]
pub struct ContentModelTurnV1 {
    /// Transient text returned by an adapter. The run externalizes it before
    /// writing a checkpoint.
    pub text: String,
    /// Already externalized provider output descriptors, if any.
    pub output_parts: Vec<ContentPartDescriptor>,
    pub tool_calls: Vec<ToolCall>,
    pub usage: Option<TokenUsage>,
}

impl fmt::Debug for ContentModelTurnV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ContentModelTurnV1")
            .field("text", &"[REDACTED]")
            .field("text_bytes", &self.text.len())
            .field("output_parts", &self.output_parts)
            .field(
                "tool_calls",
                &self
                    .tool_calls
                    .iter()
                    .map(|call| call.name.as_str())
                    .collect::<Vec<_>>(),
            )
            .field("usage", &self.usage)
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct ContentRunResultV1 {
    pub run: RunResult,
    pub messages: Vec<ContentMessageV1>,
}

impl ContentRunResultV1 {
    pub fn report(&self) -> ContentProcessResultV1 {
        ContentProcessResultV1 {
            schema_version: CONTENT_PROCESS_RESULT_SCHEMA_VERSION,
            run_id: self.run.run_id.clone(),
            success: self.run.success,
            summary: self.run.summary.clone(),
            turns: self.run.turns,
            workspace: self.run.workspace.display().to_string(),
            termination: self.run.termination.as_str().into(),
            messages: self.messages.clone(),
        }
    }
}

/// Versioned CLI/process-host content request. Payloads stay in a host directory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContentProcessRequestV1 {
    pub schema_version: u32,
    pub task: String,
    pub messages: Vec<ContentMessageV1>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capabilities: Option<ContentCapabilitiesV1>,
    #[serde(default)]
    pub keep_workspace: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume_run_id: Option<String>,
    /// Opaque descriptor reference → relative filename under the payload directory.
    pub payloads: BTreeMap<String, String>,
}

impl ContentProcessRequestV1 {
    pub fn into_run_request(
        self,
        payloads_root: impl AsRef<Path>,
    ) -> Result<ContentRunRequestV1, ContentError> {
        if self.schema_version != CONTENT_PROCESS_REQUEST_SCHEMA_VERSION {
            return Err(ContentError::Invalid(format!(
                "unsupported content process request schema version {}; expected {}",
                self.schema_version, CONTENT_PROCESS_REQUEST_SCHEMA_VERSION
            )));
        }
        let resolver = Arc::new(FileContentResolver::new(payloads_root, &self.payloads)?);
        let capabilities = self
            .capabilities
            .unwrap_or_else(|| ContentCapabilitiesV1::bounded_for(&self.messages));
        Ok(ContentRunRequestV1 {
            task: self.task,
            messages: self.messages,
            capabilities,
            resolver,
            keep_workspace: self.keep_workspace,
            timeout: None,
            cancel: None,
            resume_run_id: self.resume_run_id,
            logical_operation_id: None,
            restore_snapshot: None,
            session_wait: false,
            resume_prompt: None,
            resume_ask: None,
            resume_plan: None,
            resume_answer: None,
        })
    }
}

/// Credential-free JSON projection for one completed content process run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentProcessResultV1 {
    pub schema_version: u32,
    pub run_id: String,
    pub success: bool,
    pub summary: String,
    pub turns: u32,
    pub workspace: String,
    pub termination: String,
    pub messages: Vec<ContentMessageV1>,
}

/// Host-owned directory store for CLI content intake. References are opaque ids.
#[derive(Debug)]
pub struct FileContentResolver {
    root: PathBuf,
    files: Mutex<HashMap<String, PathBuf>>,
}

impl FileContentResolver {
    pub fn new(
        root: impl AsRef<Path>,
        payloads: &BTreeMap<String, String>,
    ) -> Result<Self, ContentError> {
        let root = root.as_ref();
        if !root.is_dir() {
            return Err(ContentError::Resolver(format!(
                "payload directory is not a directory: {}",
                root.display()
            )));
        }
        let mut files = HashMap::new();
        for (reference, filename) in payloads {
            if !valid_opaque_reference(reference) {
                return Err(ContentError::Invalid(
                    "content reference must be credential-free and opaque".into(),
                ));
            }
            let relative = relative_payload_filename(filename)?;
            let path = root.join(&relative);
            let metadata = fs::symlink_metadata(&path).map_err(|error| {
                ContentError::Resolver(format!("payload `{reference}` cannot be read: {error}"))
            })?;
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                return Err(ContentError::Resolver(format!(
                    "payload `{reference}` must be a regular file"
                )));
            }
            files.insert(reference.clone(), relative);
        }
        Ok(Self {
            root: root.to_path_buf(),
            files: Mutex::new(files),
        })
    }

    fn open_payload(&self, relative: &Path) -> Result<File, ContentError> {
        let candidate = self.root.join(relative);
        let mut options = OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
        }
        #[cfg(not(unix))]
        {
            let metadata = fs::symlink_metadata(&candidate).map_err(|error| {
                ContentError::Resolver(format!(
                    "payload {} cannot be inspected: {error}",
                    candidate.display()
                ))
            })?;
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                return Err(ContentError::Resolver(
                    "payload path must be a regular file".into(),
                ));
            }
        }
        let file = options.open(&candidate).map_err(|error| {
            ContentError::Resolver(format!(
                "payload {} cannot be opened: {error}",
                candidate.display()
            ))
        })?;
        let metadata = file.metadata().map_err(|error| {
            ContentError::Resolver(format!(
                "payload {} cannot be inspected: {error}",
                candidate.display()
            ))
        })?;
        if !metadata.is_file() {
            return Err(ContentError::Resolver(
                "payload path must be a regular file".into(),
            ));
        }
        Ok(file)
    }
}

fn relative_payload_filename(name: &str) -> Result<PathBuf, ContentError> {
    let path = Path::new(name);
    if name.is_empty()
        || name == "."
        || name == ".."
        || path.is_absolute()
        || path.components().count() != 1
        || name.contains(['/', '\\', '\0'])
    {
        return Err(ContentError::Invalid(
            "payload filename must be a single relative path component".into(),
        ));
    }
    Ok(path.to_path_buf())
}

#[async_trait]
impl ContentResolver for FileContentResolver {
    fn id(&self) -> &str {
        CLI_FILE_RESOLVER_ID
    }

    async fn resolve(
        &self,
        descriptor: &ContentPartDescriptor,
    ) -> Result<ResolvedContent, ContentError> {
        let relative = {
            let files = self
                .files
                .lock()
                .map_err(|_| ContentError::Resolver("payload map lock poisoned".into()))?;
            files.get(&descriptor.reference).cloned()
        };
        let Some(relative) = relative else {
            return Err(ContentError::Resolver(
                "payload reference is not mapped".into(),
            ));
        };
        let file = self.open_payload(&relative)?;
        let mut limited = file.take(MAX_CONTENT_PART_BYTES.saturating_add(1));
        let mut bytes = Vec::new();
        limited
            .read_to_end(&mut bytes)
            .map_err(|error| ContentError::Resolver(error.to_string()))?;
        if bytes.len() as u64 > MAX_CONTENT_PART_BYTES {
            return Err(ContentError::Invalid(
                "payload exceeds the content part byte limit".into(),
            ));
        }
        if descriptor.kind == ContentPartKind::Text {
            let text = String::from_utf8(bytes)
                .map_err(|_| ContentError::Resolver("text payload is not valid UTF-8".into()))?;
            Ok(ResolvedContent::Text(text))
        } else {
            Ok(ResolvedContent::Bytes(bytes.into()))
        }
    }

    async fn store(&self, content: ContentToStore) -> Result<ContentPartDescriptor, ContentError> {
        let bytes = content.payload.as_bytes();
        if bytes.len() as u64 > MAX_CONTENT_PART_BYTES {
            return Err(ContentError::Invalid(
                "stored payload exceeds the content part byte limit".into(),
            ));
        }
        let mut last_exists = None;
        for _ in 0..8 {
            let reference = format!(
                "cliout-{}-{}",
                content.part_id,
                uuid::Uuid::new_v4().simple()
            );
            if !valid_opaque_reference(&reference) {
                return Err(ContentError::Invalid(
                    "stored content reference must be credential-free and opaque".into(),
                ));
            }
            let relative = relative_payload_filename(&reference)?;
            let path = self.root.join(&relative);
            match OpenOptions::new().write(true).create_new(true).open(&path) {
                Ok(mut file) => {
                    file.write_all(bytes)?;
                    file.sync_all()?;
                    self.files
                        .lock()
                        .map_err(|_| ContentError::Resolver("payload map lock poisoned".into()))?
                        .insert(reference.clone(), relative);
                    return Ok(ContentPartDescriptor {
                        part_id: content.part_id,
                        kind: content.kind,
                        media_type: content.media_type,
                        byte_length: bytes.len() as u64,
                        sha256_digest: sha256_digest(bytes),
                        reference,
                        provenance: content.provenance,
                        disclosure_state: ContentDisclosureState::Accepted,
                        disclosure_reason: String::new(),
                    });
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    last_exists = Some(error);
                    continue;
                }
                Err(error) => return Err(ContentError::Io(error)),
            }
        }
        Err(ContentError::Resolver(format!(
            "could not allocate a unique payload file: {}",
            last_exists
                .map(|error| error.to_string())
                .unwrap_or_else(|| "already exists".into())
        )))
    }
}

#[derive(Debug, Error)]
pub enum ContentError {
    #[error("content contract: {0}")]
    Invalid(String),
    #[error("content capability denied: {0}")]
    Unsupported(String),
    #[error("content resolver: {0}")]
    Resolver(String),
    #[error("content checkpoint I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("content checkpoint parse: {0}")]
    Parse(#[from] serde_json::Error),
    #[error(transparent)]
    Checkpoint(#[from] checkpoint::CheckpointError),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContentCheckpointBinding {
    pub schema_version: u32,
    pub generation: u64,
    pub slot: String,
    pub sha256_digest: String,
    pub resolver_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContentCheckpointV1 {
    pub schema_version: u32,
    pub run_id: String,
    pub generation: u64,
    pub resolver_id: String,
    pub capabilities: ContentCapabilitiesV1,
    pub messages: Vec<ContentMessageV1>,
    pub initial_message_count: u32,
    pub completed_turns: u32,
    #[serde(default)]
    pub usage: TokenUsage,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal: Option<ContentTerminalCheckpoint>,
}

pub(crate) fn initial_messages_match(
    sidecar: &ContentCheckpointV1,
    requested: &[ContentMessageV1],
) -> bool {
    let Ok(initial_message_count) = usize::try_from(sidecar.initial_message_count) else {
        return false;
    };
    initial_message_count == requested.len()
        && sidecar.messages.get(..initial_message_count) == Some(requested)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContentTerminalCheckpoint {
    pub success: bool,
    pub termination: RunTermination,
    pub summary_part_id: String,
    pub summary_digest: String,
    #[serde(default)]
    pub usage: TokenUsage,
    #[serde(default)]
    pub finalized: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_dir: Option<String>,
}

pub fn validate_messages(
    messages: &[ContentMessageV1],
    capabilities: &ContentCapabilitiesV1,
) -> Result<(), ContentError> {
    capabilities.validate()?;
    if messages.is_empty() {
        return Err(ContentError::Invalid(
            "at least one content message is required".into(),
        ));
    }
    let mut ids = HashSet::new();
    let mut count = 0_usize;
    let mut aggregate = 0_u64;
    let mut accepted = 0_usize;
    let mut tool_call_ids = HashSet::new();
    for message in messages {
        if !matches!(message.role.as_str(), "user" | "assistant" | "tool") {
            return Err(ContentError::Invalid("unsupported content role".into()));
        }
        if message.parts.is_empty() && message.tool_calls.is_empty() {
            return Err(ContentError::Invalid(
                "content messages cannot be empty".into(),
            ));
        }
        if !message.tool_call_id.is_empty() {
            required_text(&message.tool_call_id, 128, "tool call id")?;
        }
        match message.role.as_str() {
            "user" if !message.tool_call_id.is_empty() || !message.tool_calls.is_empty() => {
                return Err(ContentError::Invalid(
                    "user content messages cannot carry tool metadata".into(),
                ));
            }
            "tool" if message.tool_call_id.is_empty() || !message.tool_calls.is_empty() => {
                return Err(ContentError::Invalid(
                    "tool content messages require exactly one call binding".into(),
                ));
            }
            _ => {}
        }
        for descriptor in &message.parts {
            validate_descriptor(descriptor)?;
            count = count.saturating_add(1);
            if count > capabilities.max_parts as usize
                || !ids.insert(descriptor.part_id.as_str())
                || !capabilities.input_kinds.contains(&descriptor.kind)
                || !capabilities.media_types.contains(&descriptor.media_type)
                || descriptor.byte_length > capabilities.max_part_bytes
            {
                return Err(ContentError::Invalid(
                    "content identity or capability mismatch".into(),
                ));
            }
            aggregate = aggregate
                .checked_add(descriptor.byte_length)
                .ok_or_else(|| ContentError::Invalid("content size overflow".into()))?;
            if aggregate > capabilities.max_aggregate_bytes {
                return Err(ContentError::Invalid(
                    "content aggregate exceeds configured bound".into(),
                ));
            }
            if descriptor.disclosure_state == ContentDisclosureState::Accepted {
                accepted += 1;
            }
        }
        for call in &message.tool_calls {
            if !call.id.is_empty() {
                required_text(&call.id, 128, "tool call id")?;
                if !tool_call_ids.insert(call.id.as_str()) {
                    return Err(ContentError::Invalid("duplicate tool call id".into()));
                }
            }
            required_text(&call.name, 128, "tool name")?;
            let arguments_part_id = tool_arguments_part_id(&call.args_json).ok_or_else(|| {
                ContentError::Invalid(
                    "content tool arguments must use an externalized part pointer".into(),
                )
            })?;
            if !message.parts.iter().any(|descriptor| {
                descriptor.part_id == arguments_part_id
                    && descriptor.kind == ContentPartKind::Text
                    && descriptor.disclosure_state == ContentDisclosureState::Accepted
                    && descriptor.provenance.source == "model-tool-arguments"
            }) {
                return Err(ContentError::Invalid(
                    "content tool argument pointer is not bound to accepted text".into(),
                ));
            }
        }
    }
    if accepted == 0 {
        return Err(ContentError::Invalid(
            "at least one accepted content part is required".into(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_descriptor(descriptor: &ContentPartDescriptor) -> Result<(), ContentError> {
    required_text(&descriptor.part_id, 128, "part id")?;
    if descriptor.byte_length > MAX_CONTENT_PART_BYTES
        || (descriptor.byte_length == 0 && descriptor.kind != ContentPartKind::Text)
    {
        return Err(ContentError::Invalid(
            "content part byte length is out of bounds".into(),
        ));
    }
    if !valid_sha256_digest(&descriptor.sha256_digest) {
        return Err(ContentError::Invalid(
            "content digest must be canonical sha256".into(),
        ));
    }
    if !valid_opaque_reference(&descriptor.reference) {
        return Err(ContentError::Invalid(
            "content reference must be credential-free and opaque".into(),
        ));
    }
    if !valid_media_type(descriptor.kind, &descriptor.media_type) {
        return Err(ContentError::Invalid(
            "content media type is unsupported for its kind".into(),
        ));
    }
    required_text(&descriptor.provenance.source, 256, "provenance source")?;
    required_text(
        &descriptor.provenance.source_id,
        256,
        "provenance source id",
    )?;
    required_text(
        &descriptor.provenance.source_version,
        256,
        "provenance source version",
    )?;
    match descriptor.disclosure_state {
        ContentDisclosureState::Accepted if !descriptor.disclosure_reason.is_empty() => {
            return Err(ContentError::Invalid(
                "accepted content cannot carry a disclosure reason".into(),
            ));
        }
        ContentDisclosureState::Redacted | ContentDisclosureState::Omitted => {
            required_text(&descriptor.disclosure_reason, 512, "disclosure reason")?;
        }
        ContentDisclosureState::Accepted => {}
    }
    Ok(())
}

pub async fn resolve_accepted(
    resolver: &dyn ContentResolver,
    messages: &[ContentMessageV1],
) -> Result<Vec<ResolvedContentPart>, ContentError> {
    let mut resolved = Vec::new();
    for descriptor in messages
        .iter()
        .flat_map(|message| &message.parts)
        .filter(|descriptor| descriptor.disclosure_state == ContentDisclosureState::Accepted)
    {
        let payload = resolver.resolve(descriptor).await.map_err(|_| {
            ContentError::Resolver(format!("failed to resolve part `{}`", descriptor.part_id))
        })?;
        validate_resolved(descriptor, &payload)?;
        resolved.push(ResolvedContentPart {
            descriptor: descriptor.clone(),
            payload,
        });
    }
    Ok(resolved)
}

pub(crate) fn validate_resolved(
    descriptor: &ContentPartDescriptor,
    payload: &ResolvedContent,
) -> Result<(), ContentError> {
    let bytes = payload.as_bytes();
    if bytes.len() as u64 != descriptor.byte_length
        || sha256_digest(bytes) != descriptor.sha256_digest
        || matches!(
            (&descriptor.kind, payload),
            (ContentPartKind::Text, ResolvedContent::Bytes(_))
                | (
                    ContentPartKind::Image | ContentPartKind::Audio | ContentPartKind::Document,
                    ResolvedContent::Text(_)
                )
        )
    {
        return Err(ContentError::Resolver(format!(
            "resolved payload does not match descriptor `{}`",
            descriptor.part_id
        )));
    }
    Ok(())
}

pub(crate) async fn store_text(
    resolver: &dyn ContentResolver,
    part_id: String,
    text: String,
    source: &str,
    source_id: String,
) -> Result<ContentPartDescriptor, ContentError> {
    if text.len() as u64 > MAX_CONTENT_PART_BYTES {
        return Err(ContentError::Invalid(
            "stored text byte length is out of bounds".into(),
        ));
    }
    let descriptor = resolver
        .store(ContentToStore {
            part_id: part_id.clone(),
            kind: ContentPartKind::Text,
            media_type: "text/plain".into(),
            payload: ResolvedContent::Text(text),
            provenance: ContentProvenanceV1 {
                source: source.into(),
                source_id,
                source_version: "v1".into(),
                observed_at_ms: crate::digest::unix_now_ms_i64(),
            },
        })
        .await
        .map_err(|_| ContentError::Resolver(format!("failed to store part `{part_id}`")))?;
    validate_descriptor(&descriptor)?;
    if descriptor.part_id != part_id
        || descriptor.kind != ContentPartKind::Text
        || descriptor.disclosure_state != ContentDisclosureState::Accepted
    {
        return Err(ContentError::Resolver(
            "stored text descriptor changed required identity".into(),
        ));
    }
    let payload = resolver
        .resolve(&descriptor)
        .await
        .map_err(|_| ContentError::Resolver(format!("failed to verify stored part `{part_id}`")))?;
    validate_resolved(&descriptor, &payload)?;
    Ok(descriptor)
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ToolArgumentsPointer {
    shikigami_content_arguments_part_id: String,
}

pub(crate) fn tool_arguments_pointer(part_id: &str) -> String {
    serde_json::to_string(&ToolArgumentsPointer {
        shikigami_content_arguments_part_id: part_id.into(),
    })
    .expect("tool argument pointer is serializable")
}

/// Part id stored in content-run `ToolCall.args_json` instead of the JSON arguments.
pub fn tool_arguments_part_id(args_json: &str) -> Option<String> {
    serde_json::from_str::<ToolArgumentsPointer>(args_json)
        .ok()
        .map(|pointer| pointer.shikigami_content_arguments_part_id)
}

pub fn save_sidecar(
    state_runs: &Path,
    sidecar: &ContentCheckpointV1,
    current: Option<&ContentCheckpointBinding>,
) -> Result<ContentCheckpointBinding, ContentError> {
    let generation = current.map_or(1, |binding| binding.generation.saturating_add(1));
    let slot = match current.map(|binding| binding.slot.as_str()) {
        Some(SIDECAR_SLOT_A) => SIDECAR_SLOT_B,
        _ => SIDECAR_SLOT_A,
    };
    let mut sidecar = sidecar.clone();
    sidecar.generation = generation;
    let raw = serde_json::to_vec_pretty(&sidecar)?;
    let digest = sha256_digest(&raw);
    let run_dir = state_runs.join(&sidecar.run_id);
    fs::create_dir_all(&run_dir)?;
    let path = run_dir.join(slot);
    let temporary = path.with_extension("json.tmp");
    fs::write(&temporary, raw)?;
    if let Err(error) = crate::atomic::replace_file(&temporary, &path) {
        let _ = fs::remove_file(&temporary);
        return Err(error.into());
    }
    Ok(ContentCheckpointBinding {
        schema_version: CONTENT_SCHEMA_VERSION,
        generation,
        slot: slot.into(),
        sha256_digest: digest,
        resolver_id: sidecar.resolver_id,
    })
}

pub fn load_sidecar(
    state_runs: &Path,
    run_id: &str,
    binding: &ContentCheckpointBinding,
) -> Result<ContentCheckpointV1, ContentError> {
    if binding.schema_version != CONTENT_SCHEMA_VERSION
        || !matches!(binding.slot.as_str(), SIDECAR_SLOT_A | SIDECAR_SLOT_B)
        || !checkpoint::is_safe_run_id(run_id)
    {
        return Err(ContentError::Invalid(
            "invalid content checkpoint binding".into(),
        ));
    }
    let raw = fs::read(state_runs.join(run_id).join(&binding.slot))?;
    if sha256_digest(&raw) != binding.sha256_digest {
        return Err(ContentError::Invalid(
            "content checkpoint digest mismatch".into(),
        ));
    }
    let sidecar: ContentCheckpointV1 = serde_json::from_slice(&raw)?;
    if sidecar.schema_version != CONTENT_SCHEMA_VERSION
        || sidecar.run_id != run_id
        || sidecar.generation != binding.generation
        || sidecar.resolver_id != binding.resolver_id
    {
        return Err(ContentError::Invalid(
            "content checkpoint identity mismatch".into(),
        ));
    }
    validate_messages(&sidecar.messages, &sidecar.capabilities)?;
    Ok(sidecar)
}

pub(crate) async fn resolve_terminal_summary(
    resolver: &dyn ContentResolver,
    sidecar: &ContentCheckpointV1,
) -> Result<Option<String>, ContentError> {
    let Some(terminal) = &sidecar.terminal else {
        return Ok(None);
    };
    let descriptor = sidecar
        .messages
        .iter()
        .flat_map(|message| &message.parts)
        .find(|descriptor| descriptor.part_id == terminal.summary_part_id)
        .ok_or_else(|| ContentError::Invalid("terminal summary descriptor is missing".into()))?;
    if descriptor.kind != ContentPartKind::Text
        || descriptor.disclosure_state != ContentDisclosureState::Accepted
    {
        return Err(ContentError::Invalid(
            "terminal summary descriptor must be accepted text".into(),
        ));
    }
    let payload = resolver.resolve(descriptor).await.map_err(|_| {
        ContentError::Resolver(format!(
            "failed to resolve terminal part `{}`",
            descriptor.part_id
        ))
    })?;
    validate_resolved(descriptor, &payload)?;
    let ResolvedContent::Text(summary) = payload else {
        return Err(ContentError::Invalid(
            "terminal summary descriptor resolved to bytes".into(),
        ));
    };
    if sha256_digest(summary.as_bytes()) != terminal.summary_digest {
        return Err(ContentError::Invalid(
            "terminal summary digest mismatch".into(),
        ));
    }
    Ok(Some(summary))
}

pub fn export_content_transcript(state_runs: &Path, run_id: &str) -> Result<String, ContentError> {
    let checkpoint = Checkpoint::load(state_runs, run_id)?;
    let binding = checkpoint.content.as_ref().ok_or_else(|| {
        ContentError::Invalid(format!("run {run_id} is not a bounded content run"))
    })?;
    let sidecar = load_sidecar(state_runs, run_id, binding)?;
    let mut lines = Vec::new();
    lines.push(serde_json::to_string(&serde_json::json!({
        "type": "meta",
        "schema_version": CONTENT_TRANSCRIPT_SCHEMA_VERSION,
        "run_id": run_id,
        "completed_turns": sidecar.completed_turns,
        "message_count": sidecar.messages.len(),
    }))?);
    for message in sidecar.messages {
        let tool_calls = message
            .tool_calls
            .iter()
            .map(|call| {
                serde_json::json!({
                    "id": call.id,
                    "name": call.name,
                    "arguments_part_id": tool_arguments_part_id(&call.args_json),
                })
            })
            .collect::<Vec<_>>();
        let parts = message
            .parts
            .into_iter()
            .map(|descriptor| {
                serde_json::json!({
                    "part_id": descriptor.part_id,
                    "kind": descriptor.kind,
                    "media_type": descriptor.media_type,
                    "byte_length": descriptor.byte_length,
                    "sha256_digest": descriptor.sha256_digest,
                    "reference_digest": sha256_digest(descriptor.reference.as_bytes()),
                    "provenance": descriptor.provenance,
                    "disclosure_state": descriptor.disclosure_state,
                    "disclosure_reason": descriptor.disclosure_reason,
                })
            })
            .collect::<Vec<_>>();
        lines.push(serde_json::to_string(&serde_json::json!({
            "type": "message",
            "schema_version": CONTENT_TRANSCRIPT_SCHEMA_VERSION,
            "role": message.role,
            "parts": parts,
            "tool_call_id": message.tool_call_id,
            "tool_calls": tool_calls,
        }))?);
    }
    lines.push(serde_json::to_string(&serde_json::json!({
        "type": "end",
        "schema_version": CONTENT_TRANSCRIPT_SCHEMA_VERSION,
        "run_id": run_id,
    }))?);
    Ok(lines.join("\n") + "\n")
}

pub fn sha256_digest(bytes: &[u8]) -> String {
    crate::digest::sha256_prefixed(bytes)
}

/// Project bounded-content payloads to identity-only `bytes=` / `digest=` text.
pub fn project_bounded_text(content_run: bool, label: &str, text: &str) -> String {
    if content_run {
        format!(
            "{label} bytes={} digest={}",
            text.len(),
            sha256_digest(text.as_bytes())
        )
    } else {
        text.to_string()
    }
}

fn required_text(value: &str, max: usize, field: &str) -> Result<(), ContentError> {
    if value.is_empty()
        || value != value.trim()
        || value.len() > max
        || value.contains(['\0', '\r', '\n'])
    {
        return Err(ContentError::Invalid(format!(
            "{field} must be bounded single-line text"
        )));
    }
    Ok(())
}

fn valid_sha256_digest(value: &str) -> bool {
    crate::digest::is_sha256_prefixed(value, true)
}

fn valid_opaque_reference(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    value.len() >= 8
        && value.len() <= 256
        && value == value.trim()
        && !value.contains(char::is_whitespace)
        && !value.contains(char::is_control)
        && !value.contains('\\')
        && !value.contains('?')
        && !value.contains('#')
        && !value.contains('@')
        && !value.starts_with('/')
        && !value.contains("://")
        && !lower.starts_with("data:")
        && !lower.starts_with("file:")
        && !lower.contains("bearer")
        && !lower.contains("token=")
        && !lower.contains("key=")
        && !lower.contains("secret=")
        && value
            .split('/')
            .all(|component| !matches!(component, "" | "." | ".."))
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b':' | b'/')
        })
}

fn supported_media_type(value: &str) -> bool {
    matches!(
        value,
        "text/plain"
            | "text/markdown"
            | "application/json"
            | "image/png"
            | "image/jpeg"
            | "image/gif"
            | "image/webp"
            | "audio/wav"
            | "audio/mpeg"
            | "audio/mp4"
            | "audio/ogg"
            | "application/pdf"
    )
}

fn valid_media_type(kind: ContentPartKind, value: &str) -> bool {
    if value != value.trim().to_ascii_lowercase()
        || value.contains(';')
        || value.contains(char::is_whitespace)
    {
        return false;
    }
    match kind {
        ContentPartKind::Text => {
            matches!(value, "text/plain" | "text/markdown" | "application/json")
        }
        ContentPartKind::Image => matches!(
            value,
            "image/png" | "image/jpeg" | "image/gif" | "image/webp"
        ),
        ContentPartKind::Audio => matches!(
            value,
            "audio/wav" | "audio/mpeg" | "audio/mp4" | "audio/ogg"
        ),
        ContentPartKind::Document => {
            matches!(value, "application/pdf" | "text/plain" | "text/markdown")
        }
    }
}
