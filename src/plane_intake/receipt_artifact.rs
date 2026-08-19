//! Credential-free retained-artifact JSON for completed plane acknowledgements.
//!
//! The host does not invent projection files. It only classifies captured
//! workspace files under the documented path prefixes and omits `artifact_json`
//! when the four required kinds are not all present, the inventory is
//! truncated or bound to another run, or the compact JSON exceeds 64 KiB.

use std::fs;
use std::path::Path;

use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::artifacts::ArtifactManifest;

const REQUIRED_KINDS: [&str; 4] = ["application", "typed_sdk", "tests", "delivery_inputs"];
/// Same wire limit as sekai-chisei #646 `ACK_ARTIFACT_JSON_MAX_BYTES`.
/// Oversized JSON is omitted so the completed ack still lands.
const ACK_ARTIFACT_JSON_MAX_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct ReceiptArtifactFile {
    path: String,
    kind: String,
    digest: String,
    mode: String,
    immutable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct ReceiptArtifact {
    artifact_id: String,
    digest: String,
    tree_digest: String,
    files: Vec<ReceiptArtifactFile>,
}

/// Build compact receipt `artifact_json` from a retained run manifest.
///
/// Returns `None` when the inventory is missing, truncated, bound to a
/// different run, over the Chisei 64 KiB ack limit, or does not cover every
/// required projection kind. Callers then ack without an artifact.
pub(super) fn from_retained_manifest(artifact_dir: Option<&Path>, run_id: &str) -> Option<String> {
    let artifact_dir = artifact_dir?;
    let raw = fs::read(artifact_dir.join("manifest.json")).ok()?;
    let manifest: ArtifactManifest = serde_json::from_slice(&raw).ok()?;
    from_captured_manifest(&manifest, run_id)
}

fn from_captured_manifest(manifest: &ArtifactManifest, run_id: &str) -> Option<String> {
    if manifest.files_truncated || run_id.trim().is_empty() || manifest.run_id != run_id {
        return None;
    }
    let mut files = Vec::new();
    for file in &manifest.files {
        let Some(kind) = projection_kind(&file.path) else {
            continue;
        };
        if !valid_generated_path(&file.path) || !file.sha256.starts_with("sha256:") {
            return None;
        }
        files.push(ReceiptArtifactFile {
            path: file.path.clone(),
            kind: kind.into(),
            digest: file.sha256.clone(),
            mode: "100644".into(),
            immutable: true,
        });
    }
    files.sort_by(|left, right| left.path.cmp(&right.path));
    let covered = files
        .iter()
        .map(|file| file.kind.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    if REQUIRED_KINDS.iter().any(|kind| !covered.contains(kind)) || files.is_empty() {
        return None;
    }
    let files_json = serde_json::to_vec(&files).ok()?;
    let tree_digest = content_digest(&files_json);
    let artifact_id = format!("artifact:{run_id}");
    let digest = content_digest(format!("{artifact_id}\n{tree_digest}").as_bytes());
    let json = serde_json::to_string(&ReceiptArtifact {
        artifact_id,
        digest,
        tree_digest,
        files,
    })
    .ok()?;
    if json.len() > ACK_ARTIFACT_JSON_MAX_BYTES {
        return None;
    }
    Some(json)
}

fn projection_kind(path: &str) -> Option<&'static str> {
    const RULES: &[(&str, &str)] = &[
        ("app/", "application"),
        ("application/", "application"),
        ("sdk/", "typed_sdk"),
        ("typed_sdk/", "typed_sdk"),
        ("tests/", "tests"),
        ("test/", "tests"),
        ("deploy/", "delivery_inputs"),
        ("delivery/", "delivery_inputs"),
        ("delivery_inputs/", "delivery_inputs"),
    ];
    RULES
        .iter()
        .find(|(prefix, _)| path.starts_with(prefix))
        .map(|(_, kind)| *kind)
}

fn valid_generated_path(path: &str) -> bool {
    !path.is_empty()
        && !path.starts_with('/')
        && !path.contains('\\')
        && !path.contains("//")
        && !path.split('/').any(|segment| {
            segment.is_empty() || segment == "." || segment == ".." || segment.ends_with('.')
        })
}

fn content_digest(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    format!(
        "sha256:{}",
        digest
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifacts::{ArtifactFile, ArtifactManifest};

    fn file(path: &str) -> ArtifactFile {
        ArtifactFile {
            path: path.into(),
            bytes: 1,
            sha256: content_digest(path.as_bytes()),
        }
    }

    fn manifest(files: Vec<ArtifactFile>) -> ArtifactManifest {
        ArtifactManifest {
            schema_version: 1,
            run_id: "run-1".into(),
            captured_at_ms: 1,
            workspace: "/tmp/ws".into(),
            workspace_present: true,
            files_truncated: false,
            files,
            changes: Vec::new(),
            diff_path: None,
        }
    }

    #[test]
    fn omits_artifact_when_a_projection_kind_is_missing() {
        let captured = manifest(vec![
            file("app/index.html"),
            file("sdk/client.ts"),
            file("tests/acceptance.test.ts"),
        ]);
        assert!(from_captured_manifest(&captured, "run-1").is_none());
    }

    #[test]
    fn builds_sorted_credential_free_manifest_when_all_kinds_exist() {
        let captured = manifest(vec![
            file("tests/acceptance.test.ts"),
            file("deploy/compose.yaml"),
            file("sdk/client.ts"),
            file("app/index.html"),
            file("README.md"),
        ]);
        let json = from_captured_manifest(&captured, "run-1").expect("artifact");
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["artifact_id"], "artifact:run-1");
        let files = value["files"].as_array().unwrap();
        assert_eq!(
            files
                .iter()
                .map(|file| file["path"].as_str().unwrap())
                .collect::<Vec<_>>(),
            [
                "app/index.html",
                "deploy/compose.yaml",
                "sdk/client.ts",
                "tests/acceptance.test.ts"
            ]
        );
        assert_eq!(files[0]["kind"], "application");
        assert_eq!(files[1]["kind"], "delivery_inputs");
        assert_eq!(files[2]["kind"], "typed_sdk");
        assert_eq!(files[3]["kind"], "tests");
        assert!(files.iter().all(|file| file["immutable"] == true));
        assert!(files.iter().all(|file| file.get("content").is_none()));
        assert!(
            value["tree_digest"]
                .as_str()
                .is_some_and(|digest| digest.starts_with("sha256:") && digest.len() == 71)
        );
    }

    #[test]
    fn omits_truncated_inventory() {
        let mut captured = manifest(vec![
            file("app/index.html"),
            file("sdk/client.ts"),
            file("tests/acceptance.test.ts"),
            file("deploy/compose.yaml"),
        ]);
        captured.files_truncated = true;
        assert!(from_captured_manifest(&captured, "run-1").is_none());
    }

    #[test]
    fn omits_manifest_bound_to_a_different_run() {
        let captured = manifest(vec![
            file("app/index.html"),
            file("sdk/client.ts"),
            file("tests/acceptance.test.ts"),
            file("deploy/compose.yaml"),
        ]);
        assert!(from_captured_manifest(&captured, "run-other").is_none());
    }

    #[test]
    fn omits_artifact_that_exceeds_chisei_ack_json_limit() {
        let mut files: Vec<ArtifactFile> = (0..900)
            .map(|index| file(&format!("app/generated-{index:04}.ts")))
            .collect();
        files.extend([
            file("app/index.html"),
            file("sdk/client.ts"),
            file("tests/acceptance.test.ts"),
            file("deploy/compose.yaml"),
        ]);
        let captured = manifest(files);
        assert!(
            from_captured_manifest(&captured, "run-1").is_none(),
            "expected omit when classified JSON exceeds {ACK_ARTIFACT_JSON_MAX_BYTES} bytes"
        );
    }
}
