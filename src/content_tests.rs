//! Bounded-content composition tests over `Harness`.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::sync::{Arc, Mutex};

use crate::checkpoint::{self, Checkpoint};
use crate::content::*;
use crate::model::{TokenUsage, ToolCall};
use crate::{Config, Harness, RunRequest, StateRoot};
use async_trait::async_trait;
use tempfile::tempdir;

struct MemoryResolver {
    values: Mutex<HashMap<String, (ContentPartKind, Vec<u8>)>>,
    fail_store_source: Mutex<Option<String>>,
}

impl MemoryResolver {
    fn new() -> Self {
        Self {
            values: Mutex::new(HashMap::new()),
            fail_store_source: Mutex::new(None),
        }
    }

    fn fail_store_source(&self, source: Option<&str>) {
        *self.fail_store_source.lock().unwrap() = source.map(str::to_string);
    }

    fn insert(
        &self,
        part_id: &str,
        kind: ContentPartKind,
        media_type: &str,
        payload: &[u8],
    ) -> ContentPartDescriptor {
        let reference = format!("memory:{part_id}");
        self.values
            .lock()
            .unwrap()
            .insert(reference.clone(), (kind, payload.to_vec()));
        ContentPartDescriptor {
            part_id: part_id.into(),
            kind,
            media_type: media_type.into(),
            byte_length: payload.len() as u64,
            sha256_digest: sha256_digest(payload),
            reference,
            provenance: ContentProvenanceV1 {
                source: "fixture".into(),
                source_id: "mixed".into(),
                source_version: "v1".into(),
                observed_at_ms: 1,
            },
            disclosure_state: ContentDisclosureState::Accepted,
            disclosure_reason: String::new(),
        }
    }
}

#[async_trait]
impl ContentResolver for MemoryResolver {
    fn id(&self) -> &str {
        "memory-fixture-v1"
    }

    async fn resolve(
        &self,
        descriptor: &ContentPartDescriptor,
    ) -> Result<ResolvedContent, ContentError> {
        let (kind, bytes) = self
            .values
            .lock()
            .unwrap()
            .get(&descriptor.reference)
            .cloned()
            .ok_or_else(|| ContentError::Resolver("fixture reference is missing".into()))?;
        if kind == ContentPartKind::Text {
            Ok(ResolvedContent::Text(String::from_utf8(bytes).map_err(
                |_| ContentError::Resolver("fixture text is invalid".into()),
            )?))
        } else {
            Ok(ResolvedContent::Bytes(bytes))
        }
    }

    async fn store(&self, content: ContentToStore) -> Result<ContentPartDescriptor, ContentError> {
        if self
            .fail_store_source
            .lock()
            .unwrap()
            .as_deref()
            .is_some_and(|source| source == content.provenance.source)
        {
            return Err(ContentError::Resolver("fixture store failure".into()));
        }
        let bytes = content.payload.as_bytes().to_vec();
        let reference = format!("memory:{}", content.part_id);
        self.values
            .lock()
            .unwrap()
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

fn mixed_messages(resolver: &MemoryResolver) -> Vec<ContentMessageV1> {
    vec![ContentMessageV1 {
        role: "user".into(),
        parts: vec![
            resolver.insert(
                "text-1",
                ContentPartKind::Text,
                "text/plain",
                b"secret-text-payload",
            ),
            resolver.insert("image-1", ContentPartKind::Image, "image/png", b"\x89PNG"),
            resolver.insert("audio-1", ContentPartKind::Audio, "audio/wav", b"RIFF"),
            resolver.insert(
                "document-1",
                ContentPartKind::Document,
                "application/pdf",
                b"%PDF",
            ),
        ],
        tool_call_id: String::new(),
        tool_calls: Vec::new(),
    }]
}

fn local_config(directory: &tempfile::TempDir, script: &str) -> Config {
    let mut config = Config::default();
    config.governance.adapter = "local".into();
    config.model.adapter = "scripted".into();
    config.model.script_json = Some(script.into());
    config.events.adapter = "jsonl".into();
    config.workspace.root = directory.path().join("workspaces").display().to_string();
    config
}

#[tokio::test]
async fn mixed_content_and_tool_text_round_trip_without_payload_projections() {
    let directory = tempdir().unwrap();
    let state = StateRoot::new(directory.path().join("state"));
    let resolver = Arc::new(MemoryResolver::new());
    let messages = mixed_messages(resolver.as_ref());
    let config = local_config(
        &directory,
        r#"[
            {"tool_calls":[{"id":"write-1","name":"write_file","args_json":"{\"path\":\"result.txt\",\"content\":\"ok\"}"}]},
            {"tool_calls":[{"id":"report-1","name":"report","args_json":"{\"summary\":\"finished-secret\",\"success\":true}"}]}
        ]"#,
    );
    let harness = Harness::from_config(config, state.clone()).unwrap();
    let mut request = ContentRunRequestV1::new("inspect bounded content", messages, resolver);
    request.keep_workspace = true;

    let result = harness.run_content(request).await.unwrap();

    assert!(result.run.success);
    assert_eq!(result.run.summary, "finished-secret");
    assert_eq!(
        result.messages[0]
            .parts
            .iter()
            .map(|part| part.kind)
            .collect::<Vec<_>>(),
        [
            ContentPartKind::Text,
            ContentPartKind::Image,
            ContentPartKind::Audio,
            ContentPartKind::Document
        ]
    );
    assert!(result.messages.iter().any(|message| message.role == "tool"));

    let checkpoint = Checkpoint::load(&state.runs_dir(), &result.run.run_id).unwrap();
    assert!(checkpoint.content.is_some());
    assert!(
        checkpoint
            .messages
            .iter()
            .skip(1)
            .all(|message| message.content.is_empty())
    );
    let checkpoint_json = serde_json::to_string(&checkpoint).unwrap();
    assert!(!checkpoint_json.contains(r#""content":"ok""#));
    assert!(!checkpoint_json.contains("finished-secret"));
    let sidecar = load_sidecar(
        &state.runs_dir(),
        &result.run.run_id,
        checkpoint.content.as_ref().unwrap(),
    )
    .unwrap();
    let sidecar_json = serde_json::to_string(&sidecar).unwrap();
    assert!(!sidecar_json.contains(r#""content":"ok""#));
    assert!(!sidecar_json.contains("finished-secret"));
    assert!(
        crate::transcript::export_run_transcript(
            &state.runs_dir(),
            &result.run.run_id,
            &crate::transcript::ExportOptions::default(),
        )
        .unwrap_err()
        .to_string()
        .contains("export_content_transcript")
    );
    let transcript = export_content_transcript(&state.runs_dir(), &result.run.run_id).unwrap();
    assert!(!transcript.contains("secret-text-payload"));
    assert!(!transcript.contains("memory:text-1"));
    assert!(transcript.contains("reference_digest"));

    let events = fs::read_to_string(state.runs_dir().join("events.jsonl")).unwrap();
    assert!(!events.contains("secret-text-payload"), "{events}");
    assert!(!events.contains("finished-secret"), "{events}");
    assert!(events.contains("content_turn"), "{events}");
    let record = harness.registry.load(&result.run.run_id).unwrap();
    assert!(!record.summary.contains("finished-secret"));
    assert!(record.summary.contains("bounded_content_result"));

    let mut ordinary_resume = RunRequest::new("");
    ordinary_resume.resume_run_id = Some(result.run.run_id);
    let error = harness.run(ordinary_resume).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("resumed through Harness::run_content"),
        "{error}"
    );
}

#[tokio::test]
async fn invalid_reference_and_digest_drift_fail_before_model_execution() {
    let directory = tempdir().unwrap();
    let state = StateRoot::new(directory.path().join("state"));
    let resolver = Arc::new(MemoryResolver::new());
    let mut messages = mixed_messages(resolver.as_ref());
    messages[0].parts[0].reference = "https://user:token@example.test/payload".into();
    let harness = Harness::from_config(
        local_config(&directory, r#"[{"content":"must-not-run"}]"#),
        state.clone(),
    )
    .unwrap();
    let error = harness
        .run_content(ContentRunRequestV1::new(
            "invalid",
            messages,
            resolver.clone(),
        ))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("reference"), "{error}");

    let mut messages = mixed_messages(resolver.as_ref());
    messages[0].parts[0].sha256_digest = format!("sha256:{}", "0".repeat(64));
    let error = harness
        .run_content(ContentRunRequestV1::new("drift", messages, resolver))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("does not match"), "{error}");
}

#[tokio::test]
async fn unsupported_model_adapter_denies_content_without_text_fallback() {
    let directory = tempdir().unwrap();
    let state = StateRoot::new(directory.path().join("state"));
    let resolver = Arc::new(MemoryResolver::new());
    let messages = mixed_messages(resolver.as_ref());
    let mut config = local_config(&directory, r#"[{"content":"must-not-run"}]"#);
    config.model.adapter = "plane".into();
    let harness = Harness::from_config(config, state).unwrap();

    let error = harness
        .run_content(ContentRunRequestV1::new("unsupported", messages, resolver))
        .await
        .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("does not support bounded content"),
        "{error}"
    );
}

#[tokio::test]
async fn park_tool_is_explicitly_unsupported_for_content_v1() {
    let directory = tempdir().unwrap();
    let state = StateRoot::new(directory.path().join("state"));
    let resolver = Arc::new(MemoryResolver::new());
    let messages = mixed_messages(resolver.as_ref());
    let harness = Harness::from_config(
        local_config(
            &directory,
            r#"[{"tool_calls":[{"id":"park-1","name":"escalate","args_json":"{\"reason\":\"review\",\"question\":\"continue?\"}"}]}]"#,
        ),
        state,
    )
    .unwrap();

    let error = harness
        .run_content(ContentRunRequestV1::new("park", messages, resolver))
        .await
        .unwrap_err();

    assert!(
        error.to_string().contains("do not support parking"),
        "{error}"
    );
}

#[tokio::test]
async fn empty_report_summary_remains_a_valid_text_result() {
    let directory = tempdir().unwrap();
    let state = StateRoot::new(directory.path().join("state"));
    let resolver = Arc::new(MemoryResolver::new());
    let messages = mixed_messages(resolver.as_ref());
    let harness = Harness::from_config(
        local_config(
            &directory,
            r#"[{"tool_calls":[{"id":"","name":"report","args_json":"{\"summary\":\"\",\"success\":true}"}]}]"#,
        ),
        state,
    )
    .unwrap();

    let result = harness
        .run_content(ContentRunRequestV1::new("empty report", messages, resolver))
        .await
        .unwrap();

    assert!(result.run.success);
    assert!(result.run.summary.is_empty());
}

#[tokio::test]
async fn plan_jail_settings_do_not_park_bounded_content_runs() {
    let directory = tempdir().unwrap();
    let state = StateRoot::new(directory.path().join("state"));
    let resolver = Arc::new(MemoryResolver::new());
    let messages = mixed_messages(resolver.as_ref());
    let mut config = local_config(
        &directory,
        r#"[{"tool_calls":[{"id":"report-1","name":"report","args_json":"{\"summary\":\"bounded\",\"success\":true}"}]}]"#,
    );
    config.run.plan_jail = true;
    let harness = Harness::from_config(config, state).unwrap();
    let result = harness
        .run_content(ContentRunRequestV1::new("content", messages, resolver))
        .await
        .unwrap();
    assert!(result.run.success);
    assert_eq!(result.run.summary, "bounded");
    assert_eq!(
        result.run.termination,
        crate::run::RunTermination::Completed
    );
    assert!(result.run.park.is_none());
}

#[tokio::test]
async fn generated_output_ids_do_not_collide_with_caller_owned_ids() {
    let directory = tempdir().unwrap();
    let state = StateRoot::new(directory.path().join("state"));
    let resolver = Arc::new(MemoryResolver::new());
    let mut messages = mixed_messages(resolver.as_ref());
    messages[0].parts[0].part_id = "shikigami-model-1-text".into();
    let harness =
        Harness::from_config(local_config(&directory, r#"[{"content":"done"}]"#), state).unwrap();

    let result = harness
        .run_content(ContentRunRequestV1::new("collision", messages, resolver))
        .await
        .unwrap();

    let ids = result
        .messages
        .iter()
        .flat_map(|message| &message.parts)
        .map(|descriptor| descriptor.part_id.as_str())
        .collect::<HashSet<_>>();
    let count = result
        .messages
        .iter()
        .map(|message| message.parts.len())
        .sum::<usize>();
    assert_eq!(ids.len(), count);
    assert!(ids.contains("shikigami-model-1-text-1"));
}

#[tokio::test]
async fn content_resume_restores_cursor_without_repeating_tool_result() {
    let directory = tempdir().unwrap();
    let state = StateRoot::new(directory.path().join("state"));
    let resolver = Arc::new(MemoryResolver::new());
    let mut messages = mixed_messages(resolver.as_ref());
    messages.push(ContentMessageV1 {
        role: "user".into(),
        parts: vec![resolver.insert(
            "text-2",
            ContentPartKind::Text,
            "text/plain",
            b"second-message",
        )],
        tool_call_id: String::new(),
        tool_calls: Vec::new(),
    });
    let mut first_config = local_config(
        &directory,
        r#"[{"tool_calls":[{"id":"write-1","name":"write_file","args_json":"{\"path\":\"once.txt\",\"content\":\"once\"}"}],"usage":{"input_tokens":10,"output_tokens":2}}]"#,
    );
    first_config.run.max_turns = 1;
    let first = Harness::from_config(first_config, state.clone()).unwrap();
    let mut request =
        ContentRunRequestV1::new("resume bounded content", messages.clone(), resolver.clone());
    request.keep_workspace = true;
    let error = first.run_content(request).await.unwrap_err();
    assert!(error.to_string().contains("max turns"), "{error}");
    let run_id = fs::read_dir(state.runs_dir())
        .unwrap()
        .filter_map(Result::ok)
        .find(|entry| entry.path().join(checkpoint::CHECKPOINT_FILENAME).is_file())
        .unwrap()
        .file_name()
        .to_string_lossy()
        .into_owned();

    let mismatch = Harness::from_config(
        local_config(
            &directory,
            r#"[
                {"tool_calls":[{"id":"write-1","name":"write_file","args_json":"{\"path\":\"once.txt\",\"content\":\"once\"}"}]},
                {"tool_calls":[{"id":"report-1","name":"report","args_json":"{\"summary\":\"resumed\",\"success\":true}"}]}
            ]"#,
        ),
        state.clone(),
    )
    .unwrap();
    let mut truncated = ContentRunRequestV1::new(
        "resume bounded content",
        messages[..1].to_vec(),
        resolver.clone(),
    );
    truncated.keep_workspace = true;
    truncated.resume_run_id = Some(run_id.clone());
    let error = mismatch.run_content(truncated).await.unwrap_err();
    assert!(
        error.to_string().contains("changed across resume"),
        "{error}"
    );

    let second_config = local_config(
        &directory,
        r#"[
            {"tool_calls":[{"id":"write-1","name":"write_file","args_json":"{\"path\":\"once.txt\",\"content\":\"once\"}"}],"usage":{"input_tokens":10,"output_tokens":2}},
            {"tool_calls":[{"id":"report-1","name":"report","args_json":"{\"summary\":\"resumed\",\"success\":true}"}],"usage":{"input_tokens":20,"output_tokens":4}}
        ]"#,
    );
    let second = Harness::from_config(second_config, state.clone()).unwrap();
    let mut resume =
        ContentRunRequestV1::new("resume bounded content", messages.clone(), resolver.clone());
    resume.keep_workspace = true;
    resume.resume_run_id = Some(run_id.clone());
    let result = second.run_content(resume).await.unwrap();

    assert_eq!(result.run.turns, 2);
    assert_eq!(result.run.summary, "resumed");
    assert_eq!(result.run.usage.input_tokens, 30);
    assert_eq!(result.run.usage.output_tokens, 6);
    assert_eq!(
        result
            .messages
            .iter()
            .filter(|message| message.role == "tool")
            .count(),
        2
    );

    let recovered = Harness::from_config(
        local_config(&directory, r#"[{"content":"must-not-run"}]"#),
        state,
    )
    .unwrap();
    let mut recover = ContentRunRequestV1::new("resume bounded content", messages, resolver);
    recover.keep_workspace = true;
    recover.resume_run_id = Some(run_id);
    let recovered = recovered.run_content(recover).await.unwrap();
    assert_eq!(recovered.run.turns, 2);
    assert_eq!(recovered.run.summary, "resumed");
    assert_eq!(recovered.run.usage.input_tokens, 30);
    assert_eq!(recovered.run.usage.output_tokens, 6);
    assert_eq!(
        recovered
            .messages
            .iter()
            .filter(|message| message.role == "tool")
            .count(),
        2
    );
}

#[tokio::test]
async fn failed_result_storage_leaves_local_effect_in_doubt_on_resume() {
    let directory = tempdir().unwrap();
    let state = StateRoot::new(directory.path().join("state"));
    let resolver = Arc::new(MemoryResolver::new());
    let messages = mixed_messages(resolver.as_ref());
    resolver.fail_store_source(Some("tool"));
    let script = r#"[
        {"tool_calls":[{"id":"write-1","name":"write_file","args_json":"{\"path\":\"once.txt\",\"content\":\"once\"}"}]},
        {"tool_calls":[{"id":"report-1","name":"report","args_json":"{\"summary\":\"done\",\"success\":true}"}]}
    ]"#;
    let first = Harness::from_config(local_config(&directory, script), state.clone()).unwrap();
    let mut request = ContentRunRequestV1::new("store failure", messages.clone(), resolver.clone());
    request.keep_workspace = true;

    let error = first.run_content(request).await.unwrap_err();
    assert!(error.to_string().contains("failed to store"), "{error}");
    let run_id = fs::read_dir(state.runs_dir())
        .unwrap()
        .filter_map(Result::ok)
        .find(|entry| entry.path().join(checkpoint::CHECKPOINT_FILENAME).is_file())
        .unwrap()
        .file_name()
        .to_string_lossy()
        .into_owned();
    let checkpoint = Checkpoint::load(&state.runs_dir(), &run_id).unwrap();
    assert_eq!(
        fs::read_to_string(checkpoint.workspace.join("once.txt")).unwrap(),
        "once"
    );
    resolver.fail_store_source(None);
    let second = Harness::from_config(local_config(&directory, script), state).unwrap();
    let mut resume = ContentRunRequestV1::new("store failure", messages, resolver);
    resume.keep_workspace = true;
    resume.resume_run_id = Some(run_id);

    let error = second.run_content(resume).await.unwrap_err();

    assert!(error.to_string().contains("in-doubt"), "{error}");
    assert_eq!(
        fs::read_to_string(checkpoint.workspace.join("once.txt")).unwrap(),
        "once"
    );
}

#[test]
fn two_slot_sidecar_keeps_last_committed_binding_readable() {
    let directory = tempdir().unwrap();
    let runs = directory.path().join("runs");
    let resolver = MemoryResolver::new();
    let messages = mixed_messages(&resolver);
    let sidecar = ContentCheckpointV1 {
        schema_version: CONTENT_SCHEMA_VERSION,
        run_id: "run-1".into(),
        generation: 0,
        resolver_id: resolver.id().into(),
        capabilities: ContentCapabilitiesV1::bounded_for(&messages),
        initial_message_count: messages.len() as u32,
        messages,
        completed_turns: 0,
        usage: TokenUsage::default(),
        terminal: None,
    };
    let committed = save_sidecar(&runs, &sidecar, None).unwrap();
    let uncommitted = save_sidecar(&runs, &sidecar, Some(&committed)).unwrap();

    assert_ne!(committed.slot, uncommitted.slot);
    assert_eq!(
        load_sidecar(&runs, "run-1", &committed).unwrap().generation,
        committed.generation
    );
    assert_eq!(
        load_sidecar(&runs, "run-1", &uncommitted)
            .unwrap()
            .generation,
        uncommitted.generation
    );
}

#[test]
fn validation_rejects_duplicate_ids_unknown_media_and_hard_limit_overflow() {
    let resolver = MemoryResolver::new();
    let mut messages = mixed_messages(&resolver);
    messages[0].parts[1].part_id = messages[0].parts[0].part_id.clone();
    let capabilities = ContentCapabilitiesV1::bounded_for(&messages);
    assert!(
        validate_messages(&messages, &capabilities)
            .unwrap_err()
            .to_string()
            .contains("identity")
    );

    let mut messages = mixed_messages(&resolver);
    messages[0].parts[1].media_type = "image/svg+xml".into();
    let capabilities = ContentCapabilitiesV1::bounded_for(&messages);
    assert!(
        validate_messages(&messages, &capabilities)
            .unwrap_err()
            .to_string()
            .contains("media type")
    );

    let mut messages = mixed_messages(&resolver);
    messages[0].parts[0].reference = "short".into();
    let capabilities = ContentCapabilitiesV1::bounded_for(&messages);
    assert!(
        validate_messages(&messages, &capabilities)
            .unwrap_err()
            .to_string()
            .contains("reference")
    );

    let mut messages = mixed_messages(&resolver);
    messages[0].parts[0].reference = "store/../../secret".into();
    let capabilities = ContentCapabilitiesV1::bounded_for(&messages);
    assert!(
        validate_messages(&messages, &capabilities)
            .unwrap_err()
            .to_string()
            .contains("reference")
    );

    let messages = mixed_messages(&resolver);
    let mut capabilities = ContentCapabilitiesV1::bounded_for(&messages);
    capabilities.media_types = (0..17).map(|_| "text/plain".into()).collect();
    assert!(
        validate_messages(&messages, &capabilities)
            .unwrap_err()
            .to_string()
            .contains("media types")
    );

    let mut messages = mixed_messages(&resolver);
    messages[0].parts[0].byte_length = MAX_CONTENT_PART_BYTES + 1;
    let capabilities = ContentCapabilitiesV1::bounded_for(&messages);
    assert!(
        validate_messages(&messages, &capabilities)
            .unwrap_err()
            .to_string()
            .contains("byte length")
    );

    let mut messages = mixed_messages(&resolver);
    messages[0].role = "assistant".into();
    messages[0].tool_calls.push(ToolCall {
        id: "call-1".into(),
        name: "write_file".into(),
        args_json: r#"{"content":"payload"}"#.into(),
    });
    let capabilities = ContentCapabilitiesV1::bounded_for(&messages);
    assert!(
        validate_messages(&messages, &capabilities)
            .unwrap_err()
            .to_string()
            .contains("externalized part pointer")
    );
}

#[test]
fn file_resolver_rejects_path_escape_and_missing_payload() {
    let directory = tempdir().unwrap();
    let payloads = directory.path().join("payloads");
    fs::create_dir_all(&payloads).unwrap();
    fs::write(payloads.join("payload-text-1"), b"hello").unwrap();
    let mut map = BTreeMap::new();
    map.insert("payload-text-1".into(), "../secret".into());
    assert!(
        FileContentResolver::new(&payloads, &map)
            .unwrap_err()
            .to_string()
            .contains("filename")
    );
    map.insert("payload-text-1".into(), "payload-text-1".into());
    map.insert("missing-payload-ref".into(), "missing-payload-ref".into());
    assert!(
        FileContentResolver::new(&payloads, &map)
            .unwrap_err()
            .to_string()
            .contains("cannot be read")
    );
}

#[tokio::test]
async fn file_resolver_runs_content_without_embedding_payloads_in_result() {
    let directory = tempdir().unwrap();
    let payloads = directory.path().join("payloads");
    fs::create_dir_all(&payloads).unwrap();
    fs::write(payloads.join("payload-text-1"), b"hello from file").unwrap();
    let mut files = BTreeMap::new();
    files.insert("payload-text-1".into(), "payload-text-1".into());
    let messages = vec![ContentMessageV1 {
        role: "user".into(),
        parts: vec![ContentPartDescriptor {
            part_id: "text-1".into(),
            kind: ContentPartKind::Text,
            media_type: "text/plain".into(),
            byte_length: 15,
            sha256_digest: sha256_digest(b"hello from file"),
            reference: "payload-text-1".into(),
            provenance: ContentProvenanceV1 {
                source: "cli".into(),
                source_id: "fixture".into(),
                source_version: "v1".into(),
                observed_at_ms: 1,
            },
            disclosure_state: ContentDisclosureState::Accepted,
            disclosure_reason: String::new(),
        }],
        tool_call_id: String::new(),
        tool_calls: Vec::new(),
    }];
    let request = ContentProcessRequestV1 {
        schema_version: 1,
        task: "inspect bounded content".into(),
        messages,
        capabilities: None,
        keep_workspace: true,
        resume_run_id: None,
        payloads: files,
    };
    let run_request = request.into_run_request(&payloads).unwrap();
    let config = local_config(
        &directory,
        r#"[{"tool_calls":[{"id":"report-1","name":"report","args_json":"{\"summary\":\"ok\",\"success\":true}"}]}]"#,
    );
    let harness =
        Harness::from_config(config, StateRoot::new(directory.path().join("state"))).unwrap();
    let result = harness.run_content(run_request).await.unwrap();
    assert!(result.run.success);
    let report = result.report();
    assert_eq!(report.schema_version, 1);
    assert!(
        !serde_json::to_string(&report)
            .unwrap()
            .contains("hello from file")
    );
    assert_eq!(report.messages[0].parts[0].reference, "payload-text-1");
}

#[tokio::test]
async fn file_resolver_store_uses_unique_exclusive_filenames() {
    let directory = tempdir().unwrap();
    let payloads = directory.path().join("payloads");
    fs::create_dir_all(&payloads).unwrap();
    let resolver = FileContentResolver::new(&payloads, &BTreeMap::new()).unwrap();
    let first = resolver
        .store(ContentToStore {
            part_id: "shikigami-model-1-text".into(),
            kind: ContentPartKind::Text,
            media_type: "text/plain".into(),
            payload: ResolvedContent::Text("one".into()),
            provenance: ContentProvenanceV1 {
                source: "model".into(),
                source_id: "fixture".into(),
                source_version: "v1".into(),
                observed_at_ms: 1,
            },
        })
        .await
        .unwrap();
    let second = resolver
        .store(ContentToStore {
            part_id: "shikigami-model-1-text".into(),
            kind: ContentPartKind::Text,
            media_type: "text/plain".into(),
            payload: ResolvedContent::Text("two".into()),
            provenance: ContentProvenanceV1 {
                source: "model".into(),
                source_id: "fixture".into(),
                source_version: "v1".into(),
                observed_at_ms: 2,
            },
        })
        .await
        .unwrap();
    assert_ne!(first.reference, second.reference);
    assert_eq!(
        fs::read_to_string(payloads.join(&first.reference)).unwrap(),
        "one"
    );
    assert_eq!(
        fs::read_to_string(payloads.join(&second.reference)).unwrap(),
        "two"
    );
}

#[tokio::test]
async fn file_resolver_rejects_unmapped_existing_payloads() {
    let directory = tempdir().unwrap();
    let payloads = directory.path().join("payloads");
    fs::create_dir_all(&payloads).unwrap();
    fs::write(payloads.join("secret-file"), b"not allowlisted").unwrap();
    let resolver = FileContentResolver::new(&payloads, &BTreeMap::new()).unwrap();
    let error = resolver
        .resolve(&ContentPartDescriptor {
            part_id: "text-1".into(),
            kind: ContentPartKind::Text,
            media_type: "text/plain".into(),
            byte_length: 15,
            sha256_digest: sha256_digest(b"not allowlisted"),
            reference: "secret-file".into(),
            provenance: ContentProvenanceV1 {
                source: "cli".into(),
                source_id: "fixture".into(),
                source_version: "v1".into(),
                observed_at_ms: 1,
            },
            disclosure_state: ContentDisclosureState::Accepted,
            disclosure_reason: String::new(),
        })
        .await
        .unwrap_err();
    assert!(error.to_string().contains("not mapped"), "{error}");
}

#[cfg(unix)]
#[tokio::test]
async fn file_resolver_does_not_follow_a_symlink_swapped_for_a_mapped_file() {
    let directory = tempdir().unwrap();
    let payloads = directory.path().join("payloads");
    fs::create_dir_all(&payloads).unwrap();
    fs::write(payloads.join("payload-text-1"), b"inside").unwrap();
    let outside = directory.path().join("outside.txt");
    fs::write(&outside, b"secret-outside").unwrap();
    let mut map = BTreeMap::new();
    map.insert("payload-text-1".into(), "payload-text-1".into());
    let resolver = FileContentResolver::new(&payloads, &map).unwrap();
    fs::remove_file(payloads.join("payload-text-1")).unwrap();
    std::os::unix::fs::symlink(&outside, payloads.join("payload-text-1")).unwrap();
    let error = resolver
        .resolve(&ContentPartDescriptor {
            part_id: "text-1".into(),
            kind: ContentPartKind::Text,
            media_type: "text/plain".into(),
            byte_length: 6,
            sha256_digest: sha256_digest(b"inside"),
            reference: "payload-text-1".into(),
            provenance: ContentProvenanceV1 {
                source: "cli".into(),
                source_id: "fixture".into(),
                source_version: "v1".into(),
                observed_at_ms: 1,
            },
            disclosure_state: ContentDisclosureState::Accepted,
            disclosure_reason: String::new(),
        })
        .await
        .unwrap_err();
    let message = error.to_string();
    assert!(
        message.contains("cannot be opened") || message.contains("regular file"),
        "{error}"
    );
    assert!(!message.contains("secret-outside"), "{error}");
}

#[cfg(unix)]
#[tokio::test]
async fn file_resolver_rejects_a_fifo_swapped_for_a_mapped_file() {
    let directory = tempdir().unwrap();
    let payloads = directory.path().join("payloads");
    fs::create_dir_all(&payloads).unwrap();
    let mapped = payloads.join("payload-text-1");
    fs::write(&mapped, b"inside").unwrap();
    let mut map = BTreeMap::new();
    map.insert("payload-text-1".into(), "payload-text-1".into());
    let resolver = FileContentResolver::new(&payloads, &map).unwrap();
    fs::remove_file(&mapped).unwrap();
    let c_path = std::ffi::CString::new(mapped.to_str().unwrap()).unwrap();
    // SAFETY: `c_path` is a unique temp path we just removed.
    assert_eq!(unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) }, 0);
    let error = resolver
        .resolve(&ContentPartDescriptor {
            part_id: "text-1".into(),
            kind: ContentPartKind::Text,
            media_type: "text/plain".into(),
            byte_length: 6,
            sha256_digest: sha256_digest(b"inside"),
            reference: "payload-text-1".into(),
            provenance: ContentProvenanceV1 {
                source: "cli".into(),
                source_id: "fixture".into(),
                source_version: "v1".into(),
                observed_at_ms: 1,
            },
            disclosure_state: ContentDisclosureState::Accepted,
            disclosure_reason: String::new(),
        })
        .await
        .unwrap_err();
    let message = error.to_string();
    assert!(
        message.contains("cannot be opened") || message.contains("regular file"),
        "{error}"
    );
}

#[test]
fn project_bounded_text_is_identity_unless_content_run() {
    assert_eq!(
        project_bounded_text(false, "bounded_content", "hello"),
        "hello"
    );
    assert_eq!(
        project_bounded_text(true, "bounded_content", "hello"),
        format!("bounded_content bytes=5 digest={}", sha256_digest(b"hello"))
    );
}
