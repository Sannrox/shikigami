//! Deterministic offline golden-fixture evaluation.
//!
//! The evaluator intentionally drives the public Harness API. It does not
//! add a second model or governance path; each case supplies a scripted model
//! turn sequence and asserts the observable run result.

use std::fs;
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::config::Config;
use crate::harness::{Harness, HarnessError};
use crate::run::RunRequest;
use crate::state::StateRoot;

pub const EVAL_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Error)]
pub enum EvalError {
    #[error("eval I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("eval fixture parse: {0}")]
    Parse(#[from] serde_json::Error),
    #[error(transparent)]
    Harness(#[from] HarnessError),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvalSuite {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    #[serde(default = "default_suite_name")]
    pub name: String,
    pub cases: Vec<EvalCase>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvalCase {
    pub name: String,
    pub task: String,
    /// JSON array accepted by the scripted model adapter.
    #[serde(default)]
    pub script: serde_json::Value,
    #[serde(default = "default_expect_success")]
    pub expect_success: bool,
    #[serde(default)]
    pub summary_contains: Vec<String>,
    #[serde(default)]
    pub expect_files: Vec<EvalFileExpectation>,
    #[serde(default)]
    pub timeout_secs: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvalFileExpectation {
    pub path: String,
    #[serde(default)]
    pub contains: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct EvalSuiteResult {
    pub schema_version: u32,
    pub suite: String,
    pub passed: bool,
    pub passed_cases: u32,
    pub failed_cases: u32,
    pub cases: Vec<EvalCaseResult>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct EvalCaseResult {
    pub name: String,
    pub passed: bool,
    pub run_id: Option<String>,
    pub success: Option<bool>,
    pub summary: Option<String>,
    pub failure: Option<String>,
}

pub async fn run_fixture(path: impl AsRef<Path>) -> Result<EvalSuiteResult, EvalError> {
    let suite: EvalSuite = serde_json::from_slice(&fs::read(path)?)?;
    if suite.schema_version != EVAL_SCHEMA_VERSION {
        return Err(EvalError::Harness(HarnessError::Doctor(format!(
            "unsupported eval schema {}; expected {}",
            suite.schema_version, EVAL_SCHEMA_VERSION
        ))));
    }
    let root = std::env::temp_dir().join(format!(
        "shikigami-eval-{}-{}",
        std::process::id(),
        Uuid::new_v4()
    ));
    fs::create_dir_all(&root)?;
    let mut results = Vec::with_capacity(suite.cases.len());
    for (index, case) in suite.cases.iter().enumerate() {
        results.push(run_case(&root, index, case).await?);
    }
    let _ = fs::remove_dir_all(&root);
    let passed_cases = results.iter().filter(|case| case.passed).count() as u32;
    let failed_cases = results.len() as u32 - passed_cases;
    Ok(EvalSuiteResult {
        schema_version: EVAL_SCHEMA_VERSION,
        suite: suite.name,
        passed: failed_cases == 0,
        passed_cases,
        failed_cases,
        cases: results,
    })
}

async fn run_case(root: &Path, index: usize, case: &EvalCase) -> Result<EvalCaseResult, EvalError> {
    let case_root = root.join(format!("case-{index}"));
    let state = StateRoot::new(case_root.join("state"));
    let workspace_root = case_root.join("workspace");
    let mut config = Config::default();
    config.governance.adapter = "local".into();
    config.model.adapter = "scripted".into();
    config.events.adapter = "none".into();
    config.workspace.root = workspace_root.display().to_string();
    if !case.script.is_null() {
        config.model.script_json = Some(serde_json::to_string(&case.script)?);
    }
    let harness = Harness::from_config(config, state)?;
    let mut request = RunRequest::new(case.task.clone());
    request.keep_workspace = true;
    request.timeout = case.timeout_secs.map(std::time::Duration::from_secs);
    let result = match harness.run(request).await {
        Ok(result) => result,
        Err(error) => {
            return Ok(EvalCaseResult {
                name: case.name.clone(),
                passed: false,
                run_id: None,
                success: None,
                summary: None,
                failure: Some(error.to_string()),
            });
        }
    };
    let mut failures = Vec::new();
    if result.success != case.expect_success {
        failures.push(format!(
            "success was {}, expected {}",
            result.success, case.expect_success
        ));
    }
    for needle in &case.summary_contains {
        if !result.summary.contains(needle) {
            failures.push(format!("summary does not contain {needle}"));
        }
    }
    for expected in &case.expect_files {
        let path = safe_child(&result.workspace, &expected.path).map_err(|error| {
            EvalError::Harness(HarnessError::Doctor(format!(
                "case {} has invalid expected path: {error}",
                case.name
            )))
        })?;
        if !path.is_file() {
            failures.push(format!("expected file is missing: {}", expected.path));
        } else if let Some(needle) = &expected.contains {
            let contents = fs::read_to_string(&path)?;
            if !contents.contains(needle) {
                failures.push(format!("file {} does not contain {needle}", expected.path));
            }
        }
    }
    Ok(EvalCaseResult {
        name: case.name.clone(),
        passed: failures.is_empty(),
        run_id: Some(result.run_id),
        success: Some(result.success),
        summary: Some(result.summary),
        failure: (!failures.is_empty()).then(|| failures.join("; ")),
    })
}

fn safe_child(root: &Path, relative: &str) -> Result<PathBuf, &'static str> {
    let path = Path::new(relative);
    if path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, Component::ParentDir | Component::RootDir))
    {
        return Err("path must be relative and must not contain parent traversal");
    }
    Ok(root.join(path))
}

fn default_schema_version() -> u32 {
    EVAL_SCHEMA_VERSION
}

fn default_suite_name() -> String {
    "shikigami-eval".into()
}

fn default_expect_success() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn scripted_golden_fixture_passes() {
        let dir = tempdir().unwrap();
        let fixture = dir.path().join("fixture.json");
        fs::write(
            &fixture,
            r#"{
              "name":"smoke",
              "cases":[{
                "name":"writes marker",
                "task":"write marker",
                "script":[
                  {"tool_calls":[{"name":"write_file","args_json":"{\"path\":\"ok.txt\",\"content\":\"hello\"}"}]},
                  {"tool_calls":[{"name":"report","args_json":"{\"summary\":\"done\",\"success\":true}"}]}
                ],
                "summary_contains":["done"],
                "expect_files":[{"path":"ok.txt","contains":"hello"}]
              }]
            }"#,
        )
        .unwrap();
        let result = run_fixture(&fixture).await.unwrap();
        assert!(result.passed, "{result:?}");
        assert_eq!(result.passed_cases, 1);
    }
}
