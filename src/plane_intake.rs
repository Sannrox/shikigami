//! Validated mapping from plane-claimed `runtime_dispatch` work to [`RunRequest`].
//!
//! This module does not call plane RPCs or admit work. A thin host intake adapter
//! supplies a claimed effect plus its ActionInstance parameters, then invokes the
//! shared harness with the mapped request.

use std::time::Duration;

use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::run::RunRequest;

pub const RUNTIME_DISPATCH_KIND: &str = "runtime_dispatch";
pub const CLAIMED_STATUS: &str = "claimed";
pub const DEFAULT_MAX_CLAIMED_TASK_BYTES: usize = 64 * 1024;

/// Plane data required to map one claimed Action effect into a harness run.
///
/// `parameters_json` comes from the effect's parent ActionInstance. When those
/// parameters contain `artifact_refs` instead of inline `task`, the intake
/// adapter must resolve them under its own authorization and supply the
/// resulting task in `resolved_task`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimedPlaneWork {
    /// Top-level `ActionEffect.effect_id` used for heartbeat/ack. The plane's
    /// v1 `payload_json` does not duplicate this field.
    pub effect_id: String,
    pub instance_id: String,
    pub operation_id: String,
    pub kind: String,
    pub status: String,
    pub payload_json: String,
    pub parameters_json: String,
    pub resolved_task: Option<String>,
}

/// Host-owned constraints applied after plane admission.
///
/// A claimed timeout may narrow `host_timeout`, never expand it. Workspace
/// retention remains disabled unless the host explicitly permits it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimedWorkPolicy {
    pub expected_runtime: String,
    pub max_task_bytes: usize,
    pub host_timeout: Option<Duration>,
    pub allow_keep_workspace: bool,
}

impl Default for ClaimedWorkPolicy {
    fn default() -> Self {
        Self {
            expected_runtime: "shikigami".into(),
            max_task_bytes: DEFAULT_MAX_CLAIMED_TASK_BYTES,
            host_timeout: None,
            allow_keep_workspace: false,
        }
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ClaimedWorkMappingError {
    #[error("claimed work {field} is required")]
    MissingField { field: &'static str },
    #[error("claimed effect kind must be runtime_dispatch, got {0:?}")]
    InvalidKind(String),
    #[error("claimed effect status must be claimed, got {0:?}")]
    InvalidStatus(String),
    #[error("claimed effect payload must be a JSON object: {0}")]
    InvalidPayload(String),
    #[error("claimed Action parameters must be a JSON object: {0}")]
    InvalidParameters(String),
    #[error("claimed effect payload {field} does not match the claim envelope")]
    CorrelationMismatch { field: &'static str },
    #[error("claimed effect runtime {actual:?} does not match host runtime {expected:?}")]
    RuntimeMismatch { expected: String, actual: String },
    #[error("claimed Action parameters digest does not match the effect payload")]
    ParametersDigestMismatch,
    #[error("claimed Action task is required")]
    MissingTask,
    #[error("artifact_refs require an authorized host resolution result")]
    ArtifactResolutionRequired,
    #[error("claimed Action task is {actual} bytes; maximum is {maximum}")]
    TaskTooLarge { actual: usize, maximum: usize },
    #[error("claimed Action timeout_secs must be a positive integer")]
    InvalidTimeout,
    #[error("claimed Action keep_workspace must be a boolean")]
    InvalidKeepWorkspace,
    #[error("claimed Action artifact_refs must be an array of non-empty strings")]
    InvalidArtifactRefs,
}

/// Map a fenced, already-claimed plane effect into the stable harness request.
///
/// Unknown payload and Action parameter fields are ignored: they cannot alter
/// host configuration or grant authority. Required correlation fields, runtime,
/// duplicated instance/operation ids, digest, task source, and typed execution
/// hints are validated fail closed. `effect_id` is the claim object's
/// top-level identity; the v1 payload has no second effect id to compare.
pub fn map_claimed_work(
    work: &ClaimedPlaneWork,
    policy: &ClaimedWorkPolicy,
) -> Result<RunRequest, ClaimedWorkMappingError> {
    require("effect_id", &work.effect_id)?;
    require("instance_id", &work.instance_id)?;
    require("operation_id", &work.operation_id)?;

    if work.kind != RUNTIME_DISPATCH_KIND {
        return Err(ClaimedWorkMappingError::InvalidKind(work.kind.clone()));
    }
    if work.status != CLAIMED_STATUS {
        return Err(ClaimedWorkMappingError::InvalidStatus(work.status.clone()));
    }
    if policy.expected_runtime.trim().is_empty() {
        return Err(ClaimedWorkMappingError::MissingField {
            field: "expected_runtime",
        });
    }

    let payload: Value = serde_json::from_str(&work.payload_json)
        .map_err(|error| ClaimedWorkMappingError::InvalidPayload(error.to_string()))?;
    let payload = payload
        .as_object()
        .ok_or_else(|| ClaimedWorkMappingError::InvalidPayload("expected object".into()))?;

    matching_payload_string(payload, "instance_id", &work.instance_id)?;
    matching_payload_string(payload, "operation_id", &work.operation_id)?;
    let runtime = required_payload_string(payload, "runtime")?;
    if runtime != policy.expected_runtime {
        return Err(ClaimedWorkMappingError::RuntimeMismatch {
            expected: policy.expected_runtime.clone(),
            actual: runtime.into(),
        });
    }
    let expected_digest = required_payload_string(payload, "parameters_digest")?;
    if expected_digest != sha256_hex(work.parameters_json.as_bytes()) {
        return Err(ClaimedWorkMappingError::ParametersDigestMismatch);
    }

    let parameters: Value = serde_json::from_str(&work.parameters_json)
        .map_err(|error| ClaimedWorkMappingError::InvalidParameters(error.to_string()))?;
    let parameters = parameters
        .as_object()
        .ok_or_else(|| ClaimedWorkMappingError::InvalidParameters("expected object".into()))?;

    let task = match parameters.get("task") {
        Some(Value::String(task)) if !task.trim().is_empty() => task.clone(),
        Some(Value::String(_)) | Some(Value::Null) | None => {
            let refs = artifact_refs(parameters.get("artifact_refs"))?;
            if refs.is_empty() {
                return Err(ClaimedWorkMappingError::MissingTask);
            }
            work.resolved_task
                .as_ref()
                .filter(|task| !task.trim().is_empty())
                .cloned()
                .ok_or(ClaimedWorkMappingError::ArtifactResolutionRequired)?
        }
        Some(_) => return Err(ClaimedWorkMappingError::MissingTask),
    };

    let task_bytes = task.len();
    if task_bytes > policy.max_task_bytes {
        return Err(ClaimedWorkMappingError::TaskTooLarge {
            actual: task_bytes,
            maximum: policy.max_task_bytes,
        });
    }

    let requested_timeout = match parameters.get("timeout_secs") {
        None | Some(Value::Null) => None,
        Some(value) => {
            let seconds = value
                .as_u64()
                .filter(|seconds| *seconds > 0)
                .ok_or(ClaimedWorkMappingError::InvalidTimeout)?;
            Some(Duration::from_secs(seconds))
        }
    };
    let timeout = match (policy.host_timeout, requested_timeout) {
        (Some(host), Some(requested)) => Some(host.min(requested)),
        (Some(host), None) => Some(host),
        (None, Some(requested)) => Some(requested),
        (None, None) => None,
    };

    let requested_keep_workspace = match parameters.get("keep_workspace") {
        None | Some(Value::Null) => false,
        Some(Value::Bool(value)) => *value,
        Some(_) => return Err(ClaimedWorkMappingError::InvalidKeepWorkspace),
    };

    let mut request = RunRequest::new(task);
    request.logical_operation_id = Some(work.operation_id.clone());
    request.timeout = timeout;
    request.keep_workspace = policy.allow_keep_workspace && requested_keep_workspace;
    Ok(request)
}

fn require(field: &'static str, value: &str) -> Result<(), ClaimedWorkMappingError> {
    if value.trim().is_empty() {
        return Err(ClaimedWorkMappingError::MissingField { field });
    }
    Ok(())
}

fn required_payload_string<'a>(
    payload: &'a serde_json::Map<String, Value>,
    field: &'static str,
) -> Result<&'a str, ClaimedWorkMappingError> {
    payload
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or(ClaimedWorkMappingError::MissingField { field })
}

fn matching_payload_string(
    payload: &serde_json::Map<String, Value>,
    field: &'static str,
    expected: &str,
) -> Result<(), ClaimedWorkMappingError> {
    if required_payload_string(payload, field)? != expected {
        return Err(ClaimedWorkMappingError::CorrelationMismatch { field });
    }
    Ok(())
}

fn artifact_refs(value: Option<&Value>) -> Result<Vec<&str>, ClaimedWorkMappingError> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let Some(values) = value.as_array() else {
        return Err(ClaimedWorkMappingError::InvalidArtifactRefs);
    };
    values
        .iter()
        .map(|value| {
            value
                .as_str()
                .filter(|value| !value.trim().is_empty())
                .ok_or(ClaimedWorkMappingError::InvalidArtifactRefs)
        })
        .collect()
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn work(parameters: Value) -> ClaimedPlaneWork {
        let parameters_json = parameters.to_string();
        ClaimedPlaneWork {
            effect_id: "effect-1".into(),
            instance_id: "instance-1".into(),
            operation_id: "operation-1".into(),
            kind: RUNTIME_DISPATCH_KIND.into(),
            status: CLAIMED_STATUS.into(),
            payload_json: json!({
                "runtime": "shikigami",
                "instance_id": "instance-1",
                "operation_id": "operation-1",
                "parameters_digest": sha256_hex(parameters_json.as_bytes()),
            })
            .to_string(),
            parameters_json,
            resolved_task: None,
        }
    }

    #[test]
    fn maps_claim_with_host_caps_and_ignores_non_authoritative_fields() {
        let claimed = work(json!({
            "task": "run the admitted task",
            "timeout_secs": 120,
            "keep_workspace": true,
            "unknown_future_field": {"cannot": "configure host"},
        }));
        let policy = ClaimedWorkPolicy {
            host_timeout: Some(Duration::from_secs(60)),
            allow_keep_workspace: false,
            ..Default::default()
        };

        let request = map_claimed_work(&claimed, &policy).unwrap();
        assert_eq!(request.task, "run the admitted task");
        assert_eq!(request.logical_operation_id.as_deref(), Some("operation-1"));
        assert_eq!(request.timeout, Some(Duration::from_secs(60)));
        assert!(!request.keep_workspace);
    }

    #[test]
    fn uses_authorized_artifact_resolution_result() {
        let mut claimed = work(json!({
            "artifact_refs": ["artifact:task/1"],
            "timeout_secs": 30,
        }));
        claimed.resolved_task = Some("resolved task text".into());

        let request = map_claimed_work(&claimed, &ClaimedWorkPolicy::default()).unwrap();
        assert_eq!(request.task, "resolved task text");
        assert_eq!(request.timeout, Some(Duration::from_secs(30)));
    }

    #[test]
    fn rejects_missing_operation_id() {
        let mut claimed = work(json!({"task": "demo"}));
        claimed.operation_id.clear();
        assert_eq!(
            map_claimed_work(&claimed, &ClaimedWorkPolicy::default()).unwrap_err(),
            ClaimedWorkMappingError::MissingField {
                field: "operation_id"
            }
        );
    }

    #[test]
    fn rejects_oversized_task() {
        let claimed = work(json!({"task": "12345"}));
        let policy = ClaimedWorkPolicy {
            max_task_bytes: 4,
            ..Default::default()
        };
        assert_eq!(
            map_claimed_work(&claimed, &policy).unwrap_err(),
            ClaimedWorkMappingError::TaskTooLarge {
                actual: 5,
                maximum: 4
            }
        );
    }

    #[test]
    fn rejects_mismatched_correlation_and_digest() {
        let mut mismatched = work(json!({"task": "demo"}));
        mismatched.instance_id = "other-instance".into();
        assert_eq!(
            map_claimed_work(&mismatched, &ClaimedWorkPolicy::default()).unwrap_err(),
            ClaimedWorkMappingError::CorrelationMismatch {
                field: "instance_id"
            }
        );

        let mut changed = work(json!({"task": "demo"}));
        changed.parameters_json = json!({"task": "changed"}).to_string();
        assert_eq!(
            map_claimed_work(&changed, &ClaimedWorkPolicy::default()).unwrap_err(),
            ClaimedWorkMappingError::ParametersDigestMismatch
        );
    }

    #[test]
    fn rejects_unresolved_artifact_refs_and_wrong_runtime() {
        let unresolved = work(json!({"artifact_refs": ["artifact:task/1"]}));
        assert_eq!(
            map_claimed_work(&unresolved, &ClaimedWorkPolicy::default()).unwrap_err(),
            ClaimedWorkMappingError::ArtifactResolutionRequired
        );

        let mut wrong_runtime = work(json!({"task": "demo"}));
        let mut payload: Value = serde_json::from_str(&wrong_runtime.payload_json).unwrap();
        payload["runtime"] = Value::String("other".into());
        wrong_runtime.payload_json = payload.to_string();
        assert_eq!(
            map_claimed_work(&wrong_runtime, &ClaimedWorkPolicy::default()).unwrap_err(),
            ClaimedWorkMappingError::RuntimeMismatch {
                expected: "shikigami".into(),
                actual: "other".into()
            }
        );
    }
}
