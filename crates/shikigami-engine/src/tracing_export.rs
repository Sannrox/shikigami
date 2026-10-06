//! Optional OpenTelemetry span export. Off by default; identity-only.
//!
//! This is not a governance receipt and not a tracing backend. When enabled,
//! one run emits a root span plus one span per turn and tool call. Attributes
//! are restricted to the harvest/identity allowlist; prompts, tool arguments,
//! and tool outputs are never exported.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};
use thiserror::Error;
use uuid::Uuid;

use crate::config::{Config, NetworkSettings, TracingSettings};
use crate::governance::RunHandle;
use crate::identity::{PRODUCT, VERSION};

/// Harvest-aligned span attributes. Anything else is dropped at the emission
/// boundary, including keys a future caller might attempt to add.
pub const ALLOWED_SPAN_ATTRIBUTES: &[&str] = &[
    "run_id",
    "attempt_id",
    "logical_operation_id",
    "plan_operation_id",
    "call_id",
];

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
    finished_spans: Vec<FinishedSpan>,
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

    pub fn begin(
        config: &Config,
        handle: &RunHandle,
        plan_operation_id: &str,
    ) -> Result<Self, TracingExportError> {
        let settings = &config.tracing;
        if !settings.enabled {
            return Ok(Self::disabled());
        }
        let destination = destination_from_settings(settings)?;
        Ok(Self {
            inner: Some(ActiveTrace {
                destination,
                network: config.network.clone(),
                trace_id: random_hex(32),
                run_span: OpenSpan {
                    span_id: random_hex(16),
                    parent_span_id: String::new(),
                    name: "run".into(),
                    start_unix_nano: unix_nano(),
                    attributes: identity_attributes(
                        &handle.run_id,
                        &handle.operation_id,
                        plan_operation_id,
                        None,
                    ),
                },
                current_turn: None,
                current_tools: Vec::new(),
                finished_spans: Vec::new(),
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

    pub fn end_turn(&mut self) {
        if let Some(inner) = self.inner.as_mut() {
            inner.close_turn(true);
        }
    }

    pub fn start_tool(&mut self, call_id: &str) {
        let Some(inner) = self.inner.as_mut() else {
            return;
        };
        let parent = inner
            .current_turn
            .as_ref()
            .map(|span| span.span_id.clone())
            .unwrap_or_else(|| inner.run_span.span_id.clone());
        inner.current_tools.push(OpenSpan {
            span_id: random_hex(16),
            parent_span_id: parent,
            name: "tool".into(),
            start_unix_nano: unix_nano(),
            attributes: identity_attributes(
                &inner.identities.run_id,
                &inner.identities.logical_operation_id,
                &inner.identities.plan_operation_id,
                Some(call_id),
            ),
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
        }
    }

    pub async fn finish(&mut self, success: bool) -> Result<(), TracingExportError> {
        let Some(inner) = self.inner.as_mut() else {
            return Ok(());
        };
        if inner.finished {
            return Ok(());
        }
        inner.close_turn(success);
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
        let payload = encode_otlp(&inner.trace_id, &inner.finished_spans);
        export_payload(&inner.destination, &inner.network, &payload).await?;
        inner.finished = true;
        Ok(())
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

async fn export_payload(
    destination: &Destination,
    network: &NetworkSettings,
    payload: &Value,
) -> Result<(), TracingExportError> {
    match destination {
        Destination::File(path) => write_file(path, payload),
        Destination::Http(url) => post_otlp(url, network, payload).await,
    }
}

fn write_file(path: &Path, payload: &Value) -> Result<(), TracingExportError> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, serde_json::to_vec_pretty(payload).expect("otlp json"))?;
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
