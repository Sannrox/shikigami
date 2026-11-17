//! Optional OpenTelemetry span export. Off by default; metadata only.
//!
//! This is not a governance receipt and not a tracing backend. When enabled,
//! one run emits a root span plus one span per turn and tool call. Each span
//! is exported as soon as it ends, so a process that dies mid-run loses only
//! its open spans. The trace id derives from the run id, so every process
//! that works on the run (resume, takeover, park and wake) writes into the
//! same trace. Attributes
//! are restricted to a fixed allowlist of ids, names, and counts; task text,
//! prompts, tool arguments, tool outputs, and summaries are never exported.

use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};
use thiserror::Error;
use uuid::Uuid;

use crate::config::{Config, NetworkSettings, TracingSettings};
use crate::governance::RunHandle;
use crate::identity::{PRODUCT, VERSION};
use crate::model::TokenUsage;

/// Span attributes: correlation ids, plus names and counts that follow the
/// OpenTelemetry generative-AI conventions where one exists. Anything else is
/// dropped at the emission boundary, including keys a future caller might
/// attempt to add.
pub const ALLOWED_SPAN_ATTRIBUTES: &[&str] = &[
    "run_id",
    "attempt_id",
    "logical_operation_id",
    "plan_operation_id",
    "call_id",
    // Tool spans.
    "gen_ai.tool.name",
    // Turn spans.
    "gen_ai.request.model",
    "gen_ai.usage.input_tokens",
    "gen_ai.usage.output_tokens",
    // Run span.
    "parent.source",
    "shikigami.prompt_id",
    "shikigami.preset_id",
    "shikigami.termination",
];

/// Where a run's parent trace context came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TraceSource {
    /// The library caller set it on the request.
    Request,
    /// `shikigami run` read the `TRACEPARENT` environment variable.
    Env,
    /// `POST /runs` read the `traceparent` header.
    Header,
}

impl TraceSource {
    fn as_str(self) -> &'static str {
        match self {
            Self::Request => "request",
            Self::Env => "env",
            Self::Header => "header",
        }
    }
}

/// A caller's W3C trace context. The run's `run` span becomes a child of it.
/// Kept in the checkpoint so every attempt at the run keeps the same parent.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TraceContext {
    pub traceparent: String,
    /// Passed through unchanged; never a span attribute.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tracestate: Option<String>,
    pub source: TraceSource,
}

impl TraceContext {
    /// Whether `traceparent` is a well-formed W3C value the run can join.
    pub fn is_valid(&self) -> bool {
        parse_traceparent(&self.traceparent).is_some()
    }
}

/// Trace id and parent span id of a W3C `traceparent`
/// (`version-traceid-parentid-flags`). A future version may append fields;
/// version `ff`, uppercase hex, and all-zero ids are invalid.
fn parse_traceparent(value: &str) -> Option<(&str, &str)> {
    let hex = |part: &str, len: usize| {
        part.len() == len
            && part
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    };
    let mut parts = value.trim().split('-');
    let (version, trace_id, span_id, flags) =
        (parts.next()?, parts.next()?, parts.next()?, parts.next()?);
    let extra = parts.next().is_some();
    let valid = hex(version, 2)
        && version != "ff"
        && !(version == "00" && extra)
        && hex(trace_id, 32)
        && trace_id.bytes().any(|b| b != b'0')
        && hex(span_id, 16)
        && span_id.bytes().any(|b| b != b'0')
        && hex(flags, 2);
    valid.then_some((trace_id, span_id))
}

#[derive(Debug, Error)]
pub enum TracingExportError {
    #[error("span export I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("span export: {0}")]
    Message(String),
}

/// In-run span recorder. Disabled unless `[tracing].enabled` is true.
pub struct RunSpanTrace {
    inner: Option<ActiveTrace>,
}

struct ActiveTrace {
    destination: Destination,
    network: NetworkSettings,
    trace_id: String,
    run_span: OpenSpan,
    current_turn: Option<OpenSpan>,
    current_tools: Vec<OpenSpan>,
    /// Ended spans not yet handed to the exporter.
    finished_spans: Vec<FinishedSpan>,
    /// HTTP exports in flight; `finish` awaits them.
    in_flight: Vec<tokio::task::JoinHandle<Result<(), TracingExportError>>>,
    /// First export failure, reported by `finish`. Export never fails a run.
    first_error: Option<String>,
    identities: Identities,
    finished: bool,
}

#[derive(Clone)]
struct Identities {
    run_id: String,
    logical_operation_id: String,
    plan_operation_id: String,
}

enum Destination {
    File(PathBuf),
    Http(String),
}

struct OpenSpan {
    span_id: String,
    parent_span_id: String,
    name: String,
    start_unix_nano: u128,
    attributes: Vec<(String, String)>,
}

struct FinishedSpan {
    span_id: String,
    parent_span_id: String,
    name: String,
    start_unix_nano: u128,
    end_unix_nano: u128,
    attributes: Vec<(String, String)>,
    status_ok: bool,
}

impl RunSpanTrace {
    pub fn disabled() -> Self {
        Self { inner: None }
    }

    /// Opens the run span, as a child of `parent` when it is a valid W3C
    /// context; otherwise in the run's own trace. `preset_id` is empty when
    /// the run has no preset.
    pub fn begin(
        config: &Config,
        handle: &RunHandle,
        plan_operation_id: &str,
        prompt_id: &str,
        preset_id: &str,
        parent: Option<&TraceContext>,
    ) -> Result<Self, TracingExportError> {
        let settings = &config.tracing;
        if !settings.enabled {
            return Ok(Self::disabled());
        }
        let destination = destination_from_settings(settings)?;
        let mut run_attributes = identity_attributes(
            &handle.run_id,
            &handle.operation_id,
            plan_operation_id,
            None,
        );
        let joined = parent.and_then(|context| {
            parse_traceparent(&context.traceparent).map(|ids| (ids, context.source))
        });
        let (trace_id, parent_span_id, source) = match joined {
            Some(((trace_id, span_id), source)) => {
                (trace_id.to_owned(), span_id.to_owned(), source.as_str())
            }
            None => (trace_id_for(&handle.run_id), String::new(), "none"),
        };
        run_attributes.extend(allow_attribute("parent.source", source));
        run_attributes.extend(allow_attribute("shikigami.prompt_id", prompt_id));
        if !preset_id.is_empty() {
            run_attributes.extend(allow_attribute("shikigami.preset_id", preset_id));
        }
        Ok(Self {
            inner: Some(ActiveTrace {
                destination,
                network: config.network.clone(),
                trace_id,
                run_span: OpenSpan {
                    span_id: random_hex(16),
                    parent_span_id,
                    name: "run".into(),
                    start_unix_nano: unix_nano(),
                    attributes: run_attributes,
                },
                current_turn: None,
                current_tools: Vec::new(),
                finished_spans: Vec::new(),
                in_flight: Vec::new(),
                first_error: None,
                identities: Identities {
                    run_id: handle.run_id.clone(),
                    logical_operation_id: handle.operation_id.clone(),
                    plan_operation_id: plan_operation_id.to_string(),
                },
                finished: false,
            }),
        })
    }

    pub fn start_turn(&mut self, _turn: u32) {
        let Some(inner) = self.inner.as_mut() else {
            return;
        };
        inner.close_turn(false);
        let parent = inner.run_span.span_id.clone();
        inner.current_turn = Some(OpenSpan {
            span_id: random_hex(16),
            parent_span_id: parent,
            name: "turn".into(),
            start_unix_nano: unix_nano(),
            attributes: identity_attributes(
                &inner.identities.run_id,
                &inner.identities.logical_operation_id,
                &inner.identities.plan_operation_id,
                None,
            ),
        });
    }

    /// Closes the turn span with the model it used and, when the model
    /// reported it, the turn's token usage.
    pub fn end_turn(&mut self, model: &str, usage: Option<TokenUsage>) {
        let Some(inner) = self.inner.as_mut() else {
            return;
        };
        if let Some(turn) = inner.current_turn.as_mut() {
            turn.attributes
                .extend(allow_attribute("gen_ai.request.model", model));
            if let Some(usage) = usage {
                turn.attributes.extend(allow_attribute(
                    "gen_ai.usage.input_tokens",
                    &usage.input_tokens.to_string(),
                ));
                turn.attributes.extend(allow_attribute(
                    "gen_ai.usage.output_tokens",
                    &usage.output_tokens.to_string(),
                ));
            }
        }
        inner.close_turn(true);
    }

    pub fn start_tool(&mut self, call_id: &str, tool_name: &str) {
        let Some(inner) = self.inner.as_mut() else {
            return;
        };
        let parent = inner
            .current_turn
            .as_ref()
            .map(|span| span.span_id.clone())
            .unwrap_or_else(|| inner.run_span.span_id.clone());
        let mut attributes = identity_attributes(
            &inner.identities.run_id,
            &inner.identities.logical_operation_id,
            &inner.identities.plan_operation_id,
            Some(call_id),
        );
        attributes.extend(allow_attribute("gen_ai.tool.name", tool_name));
        inner.current_tools.push(OpenSpan {
            span_id: random_hex(16),
            parent_span_id: parent,
            name: "tool".into(),
            start_unix_nano: unix_nano(),
            attributes,
        });
    }

    pub fn end_tool(&mut self, call_id: &str, ok: bool) {
        let Some(inner) = self.inner.as_mut() else {
            return;
        };
        if let Some(index) = inner
            .current_tools
            .iter()
            .rposition(|span| span.attribute("call_id") == Some(call_id))
        {
            let open = inner.current_tools.remove(index);
            inner.finished_spans.push(open.finish(ok));
            inner.flush();
        }
    }

    pub async fn finish(
        &mut self,
        success: bool,
        termination: &str,
    ) -> Result<(), TracingExportError> {
        let Some(inner) = self.inner.as_mut() else {
            return Ok(());
        };
        if inner.finished {
            return Ok(());
        }
        inner.close_turn(success);
        inner
            .run_span
            .attributes
            .extend(allow_attribute("shikigami.termination", termination));
        let run = std::mem::replace(
            &mut inner.run_span,
            OpenSpan {
                span_id: String::new(),
                parent_span_id: String::new(),
                name: String::new(),
                start_unix_nano: 0,
                attributes: Vec::new(),
            },
        );
        inner.finished_spans.push(run.finish(success));
        inner.flush();
        inner.finished = true;
        for export in std::mem::take(&mut inner.in_flight) {
            let outcome = export
                .await
                .unwrap_or_else(|error| Err(TracingExportError::Message(error.to_string())));
            if let Err(error) = outcome {
                inner.first_error.get_or_insert(error.to_string());
            }
        }
        match inner.first_error.take() {
            Some(error) => Err(TracingExportError::Message(error)),
            None => Ok(()),
        }
    }
}

impl ActiveTrace {
    fn close_turn(&mut self, ok: bool) {
        while let Some(open) = self.current_tools.pop() {
            self.finished_spans.push(open.finish(false));
        }
        if let Some(open) = self.current_turn.take() {
            self.finished_spans.push(open.finish(ok));
        }
        self.flush();
    }

    /// Hands ended spans to the exporter. A file gets one OTLP JSON line
    /// appended now; an HTTP collector gets a background POST that `finish`
    /// awaits, so the run never waits on the collector mid-run.
    fn flush(&mut self) {
        if self.finished_spans.is_empty() {
            return;
        }
        let payload = encode_otlp(&self.trace_id, &std::mem::take(&mut self.finished_spans));
        match &self.destination {
            Destination::File(path) => {
                if let Err(error) = append_file(path, &payload) {
                    self.first_error.get_or_insert(error.to_string());
                }
            }
            Destination::Http(url) => match tokio::runtime::Handle::try_current() {
                Ok(runtime) => {
                    let (url, network) = (url.clone(), self.network.clone());
                    self.in_flight.push(
                        runtime.spawn(async move { post_otlp(&url, &network, &payload).await }),
                    );
                }
                Err(error) => {
                    self.first_error.get_or_insert(error.to_string());
                }
            },
        }
    }
}

impl OpenSpan {
    fn attribute(&self, key: &str) -> Option<&str> {
        self.attributes
            .iter()
            .find(|(name, _)| name == key)
            .map(|(_, value)| value.as_str())
    }

    fn finish(self, ok: bool) -> FinishedSpan {
        FinishedSpan {
            span_id: self.span_id,
            parent_span_id: self.parent_span_id,
            name: self.name,
            start_unix_nano: self.start_unix_nano,
            end_unix_nano: unix_nano().max(self.start_unix_nano),
            attributes: self.attributes,
            status_ok: ok,
        }
    }
}

fn identity_attributes(
    run_id: &str,
    logical_operation_id: &str,
    plan_operation_id: &str,
    call_id: Option<&str>,
) -> Vec<(String, String)> {
    let mut attributes = vec![
        allow_attribute("run_id", run_id),
        allow_attribute("attempt_id", run_id),
        allow_attribute("logical_operation_id", logical_operation_id),
        allow_attribute("plan_operation_id", plan_operation_id),
    ];
    if let Some(call_id) = call_id {
        attributes.push(allow_attribute("call_id", call_id));
    }
    attributes.into_iter().flatten().collect()
}

fn allow_attribute(key: &str, value: &str) -> Option<(String, String)> {
    ALLOWED_SPAN_ATTRIBUTES
        .contains(&key)
        .then(|| (key.to_string(), value.to_string()))
}

fn destination_from_settings(
    settings: &TracingSettings,
) -> Result<Destination, TracingExportError> {
    let endpoint = settings
        .endpoint
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| TracingExportError::Message("tracing.endpoint is required".into()))?;
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        Ok(Destination::Http(otlp_http_url(endpoint)))
    } else if let Some(path) = endpoint.strip_prefix("file://") {
        Ok(Destination::File(PathBuf::from(path)))
    } else {
        Ok(Destination::File(PathBuf::from(endpoint)))
    }
}

fn otlp_http_url(endpoint: &str) -> String {
    if endpoint.contains("/v1/traces") {
        endpoint.to_string()
    } else {
        format!("{}/v1/traces", endpoint.trim_end_matches('/'))
    }
}

/// Appends one OTLP JSON document as a line. Each line is one complete
/// write, so a crash can cut only the last line.
fn append_file(path: &Path, payload: &Value) -> Result<(), TracingExportError> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)?;
    }
    let mut line = serde_json::to_vec(payload).expect("otlp json");
    line.push(b'\n');
    fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?
        .write_all(&line)?;
    Ok(())
}

async fn post_otlp(
    url: &str,
    network: &NetworkSettings,
    payload: &Value,
) -> Result<(), TracingExportError> {
    network
        .check_http_url(url)
        .map_err(TracingExportError::Message)?;
    post_otlp_http(url, payload).await
}

#[cfg(feature = "model-http")]
async fn post_otlp_http(url: &str, payload: &Value) -> Result<(), TracingExportError> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .map_err(|error| TracingExportError::Message(error.to_string()))?;
    let response = client
        .post(url)
        .header("Content-Type", "application/json")
        .body(serde_json::to_vec(payload).expect("otlp json"))
        .send()
        .await
        .map_err(|error| TracingExportError::Message(error.to_string()))?;
    if !response.status().is_success() {
        return Err(TracingExportError::Message(format!(
            "otlp export HTTP {}",
            response.status()
        )));
    }
    Ok(())
}

#[cfg(not(feature = "model-http"))]
async fn post_otlp_http(_url: &str, _payload: &Value) -> Result<(), TracingExportError> {
    Err(TracingExportError::Message(
        "otlp HTTP export requires the model-http feature".into(),
    ))
}

fn encode_otlp(trace_id: &str, spans: &[FinishedSpan]) -> Value {
    let encoded: Vec<Value> = spans
        .iter()
        .map(|span| {
            json!({
                "traceId": trace_id,
                "spanId": span.span_id,
                "parentSpanId": span.parent_span_id,
                "name": span.name,
                "kind": 1,
                "startTimeUnixNano": span.start_unix_nano.to_string(),
                "endTimeUnixNano": span.end_unix_nano.to_string(),
                "attributes": span
                    .attributes
                    .iter()
                    .map(|(key, value)| json!({
                        "key": key,
                        "value": { "stringValue": value }
                    }))
                    .collect::<Vec<_>>(),
                "status": {
                    "code": if span.status_ok { 1 } else { 2 }
                }
            })
        })
        .collect();
    json!({
        "resourceSpans": [{
            "resource": {
                "attributes": [
                    {
                        "key": "service.name",
                        "value": { "stringValue": PRODUCT }
                    },
                    {
                        "key": "service.version",
                        "value": { "stringValue": VERSION }
                    }
                ]
            },
            "scopeSpans": [{
                "scope": {
                    "name": PRODUCT,
                    "version": VERSION
                },
                "spans": encoded
            }]
        }]
    })
}

fn unix_nano() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_nanos()
}

/// 32 hex chars derived from the run id: every attempt at one run shares it.
fn trace_id_for(run_id: &str) -> String {
    crate::evidence_queue::sha256_hex(run_id.as_bytes())[..32].to_owned()
}

fn random_hex(chars: usize) -> String {
    Uuid::new_v4().simple().to_string()[..chars].to_string()
}

/// Defense-in-depth filter used by tests and any future attribute builder.
pub fn filter_span_attributes(
    attributes: impl IntoIterator<Item = (String, String)>,
) -> Vec<(String, String)> {
    attributes
        .into_iter()
        .filter(|(key, _)| ALLOWED_SPAN_ATTRIBUTES.contains(&key.as_str()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TracingSettings;

    #[test]
    fn traceparent_parses_only_well_formed_w3c_values() {
        let trace = "4bf92f3577b34da6a3ce929d0e0e4736";
        let span = "00f067aa0ba902b7";
        assert_eq!(
            parse_traceparent(&format!("00-{trace}-{span}-01")),
            Some((trace, span))
        );
        // A later version may append fields.
        assert!(parse_traceparent(&format!("01-{trace}-{span}-01-extra")).is_some());
        for bad in [
            String::new(),
            "garbage".into(),
            format!("00-{trace}-{span}-01-extra"),
            format!("ff-{trace}-{span}-01"),
            format!("00-{}-{span}-01", "0".repeat(32)),
            format!("00-{trace}-{}-01", "0".repeat(16)),
            format!("00-{}-{span}-01", trace.to_uppercase()),
            format!("00-{trace}-{span}"),
        ] {
            assert_eq!(parse_traceparent(&bad), None, "{bad}");
        }
    }

    #[test]
    fn ended_spans_reach_the_file_before_the_run_finishes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("spans.jsonl");
        let mut config = Config::default();
        config.tracing.enabled = true;
        config.tracing.exporter = "otlp".into();
        config.tracing.endpoint = Some(path.to_string_lossy().into());
        let handle = RunHandle {
            run_id: "run-1".into(),
            operation_id: "run-1".into(),
            namespace: String::new(),
        };
        let mut trace = RunSpanTrace::begin(&config, &handle, "", "prompt", "", None).unwrap();
        trace.start_turn(1);
        trace.end_turn("model", None);
        trace.start_tool("tool-1-0", "read_file");
        trace.end_tool("tool-1-0", true);
        // No `finish`: the process dies here.
        drop(trace);

        let exported = std::fs::read_to_string(&path).unwrap();
        let names: Vec<String> = exported
            .lines()
            .flat_map(|line| {
                let payload: Value = serde_json::from_str(line).unwrap();
                payload["resourceSpans"][0]["scopeSpans"][0]["spans"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|span| {
                        assert_eq!(span["traceId"], trace_id_for("run-1"));
                        span["name"].as_str().unwrap().to_owned()
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(names, ["turn", "tool"]);
    }

    #[test]
    fn allowlist_drops_prompt_and_tool_payload_keys() {
        let filtered = filter_span_attributes([
            ("run_id".into(), "r1".into()),
            ("prompt".into(), "SECRET_PROMPT".into()),
            ("args_json".into(), "SECRET_ARGS".into()),
            ("detail".into(), "SECRET_OUTPUT".into()),
            ("call_id".into(), "tool-1-0".into()),
        ]);
        let keys: Vec<_> = filtered.iter().map(|(key, _)| key.as_str()).collect();
        assert_eq!(keys, ["run_id", "call_id"]);
        let blob = format!("{filtered:?}");
        assert!(!blob.contains("SECRET_"));
    }

    #[test]
    fn identity_attributes_never_include_task_or_summary() {
        let attributes = identity_attributes("run-1", "op-1", "plan-1", Some("tool-1-0"));
        let encoded = format!("{attributes:?}");
        for forbidden in ["task", "summary", "prompt", "args", "content"] {
            assert!(
                !attributes.iter().any(|(key, _)| key.contains(forbidden)),
                "{encoded}"
            );
        }
        assert!(
            attributes
                .iter()
                .any(|(key, value)| key == "run_id" && value == "run-1")
        );
        assert!(
            attributes
                .iter()
                .any(|(key, value)| key == "logical_operation_id" && value == "op-1")
        );
        assert!(
            attributes
                .iter()
                .any(|(key, value)| key == "plan_operation_id" && value == "plan-1")
        );
        assert!(
            attributes
                .iter()
                .any(|(key, value)| key == "call_id" && value == "tool-1-0")
        );
    }

    #[test]
    fn otlp_payload_uses_allowlisted_keys_only() {
        let payload = encode_otlp(
            "aa".repeat(16).as_str(),
            &[FinishedSpan {
                span_id: "bb".repeat(8),
                parent_span_id: String::new(),
                name: "run".into(),
                start_unix_nano: 1,
                end_unix_nano: 2,
                attributes: identity_attributes("run-1", "op-1", "", None),
                status_ok: true,
            }],
        );
        let text = payload.to_string();
        assert!(text.contains("\"name\":\"run\""));
        assert!(text.contains("run_id"));
        assert!(text.contains("logical_operation_id"));
        assert!(!text.contains("SECRET"));
        assert!(!text.contains("prompt"));
    }

    #[test]
    fn http_destination_appends_traces_path() {
        let dest = destination_from_settings(&TracingSettings {
            enabled: true,
            exporter: "otlp".into(),
            endpoint: Some("http://127.0.0.1:4318".into()),
        })
        .unwrap();
        match dest {
            Destination::Http(url) => assert_eq!(url, "http://127.0.0.1:4318/v1/traces"),
            Destination::File(_) => panic!("expected http"),
        }
    }
}
