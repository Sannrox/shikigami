//! Workspace-jailed tool executor for builtin tools.

use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use serde::Deserialize;
use tokio::process::Command;
use tokio::time::timeout;

use super::bash::MAX_BASH_OUTPUT_BYTES;
use super::catalog::builtin_catalog;
use super::environment::ToolEnvironment;
use super::fs::{
    ApplyPatchArgs, EditArgs, GlobArgs, GrepArgs, MAX_APPLY_PATCH_BYTES, MAX_SEARCH_MATCHES,
    MultiEditArgs, PathArgs, WriteArgs,
};
use super::path::load_ignore_patterns;
use super::{ParkRequest, Report, ToolError, ToolOutput, parse};
use crate::config::SandboxSettings;
use crate::sandbox::Sandbox;

/// Private workspace-jailed builtin dispatch used by [`super::ToolRegistry`].
#[derive(Debug, Clone)]
pub(crate) struct ToolExecutor {
    pub(crate) workspace: PathBuf,
    pub(crate) enabled: Vec<String>,
    pub(crate) bash_timeout: Duration,
    pub(crate) environment: ToolEnvironment,
    /// Patterns for glob/grep filtering (empty when respect_ignore is false).
    pub(crate) ignore_patterns: Vec<String>,
    pub(crate) sandbox: Sandbox,
}

#[derive(Deserialize)]
pub(crate) struct BashArgs {
    pub(crate) command: String,
    pub(crate) timeout_ms: Option<u64>,
}

impl ToolExecutor {
    pub(crate) fn new(
        workspace: impl Into<PathBuf>,
        enabled: Vec<String>,
        bash_timeout_secs: u64,
        respect_ignore: bool,
        protected_environment_names: &[String],
        sandbox_settings: SandboxSettings,
    ) -> Result<Self, ToolError> {
        let workspace = std::fs::canonicalize(workspace.into())?;
        let ignore_patterns = if respect_ignore {
            load_ignore_patterns(&workspace)
        } else {
            Vec::new()
        };
        let sandbox = Sandbox::new(sandbox_settings)
            .map_err(|error| ToolError::Message(error.to_string()))?;
        Ok(Self {
            workspace,
            enabled,
            bash_timeout: Duration::from_secs(bash_timeout_secs.max(1)),
            environment: ToolEnvironment::resolve(protected_environment_names),
            ignore_patterns,
            sandbox,
        })
    }

    pub(crate) fn bash_command(&self, script: &str) -> Result<Command, ToolError> {
        let mut command = Command::new("bash");
        command
            .arg("--noprofile")
            .arg("--norc")
            .arg("-c")
            .arg(script)
            .current_dir(&self.workspace)
            .kill_on_drop(true);
        self.environment.apply(&mut command);
        self.sandbox
            .apply(&mut command)
            .map_err(|error| ToolError::Message(error.to_string()))?;
        Ok(command)
    }

    pub(crate) async fn execute(
        &self,
        name: &str,
        args_json: &str,
    ) -> Result<ToolOutput, ToolError> {
        if !self.enabled.iter().any(|e| e == name) {
            return Err(ToolError::Disabled(name.into()));
        }
        // Unknown names that are enabled still fail closed.
        if !builtin_catalog().iter().any(|d| d.name == name) {
            return Err(ToolError::UnknownTool(name.into()));
        }
        match name {
            "read_file" => {
                let args: PathArgs = parse(name, args_json)?;
                Ok(ToolOutput::Text(self.read_file(&args.path)?))
            }
            "write_file" => {
                let args: WriteArgs = parse(name, args_json)?;
                self.write_file(&args.path, &args.content)?;
                Ok(ToolOutput::Text("file written".into()))
            }
            "edit" => {
                let args: EditArgs = parse(name, args_json)?;
                self.edit(&args.path, &args.old, &args.new)?;
                Ok(ToolOutput::Text("file edited".into()))
            }
            "multi_edit" => {
                let args: MultiEditArgs = parse(name, args_json)?;
                let n = self.multi_edit(&args.path, &args.edits)?;
                Ok(ToolOutput::Text(format!("{n} edits applied")))
            }
            "apply_patch" => {
                if args_json.len() > MAX_APPLY_PATCH_BYTES {
                    return Err(ToolError::ApplyPatch(format!(
                        "payload exceeds {MAX_APPLY_PATCH_BYTES} bytes"
                    )));
                }
                let args: ApplyPatchArgs = parse(name, args_json)?;
                let n = self.apply_patch(&args.patches)?;
                Ok(ToolOutput::Text(format!(
                    "{n} hunk(s) applied across {} file(s)",
                    args.patches.len()
                )))
            }
            "glob" => {
                let args: GlobArgs = parse(name, args_json)?;
                Ok(ToolOutput::Text(
                    self.glob_files(&args.pattern, args.path.as_deref())?,
                ))
            }
            "grep" => {
                let args: GrepArgs = parse(name, args_json)?;
                let max = args
                    .max_matches
                    .unwrap_or(MAX_SEARCH_MATCHES)
                    .clamp(1, MAX_SEARCH_MATCHES);
                Ok(ToolOutput::Text(self.grep_files(
                    &args.pattern,
                    args.path.as_deref(),
                    max,
                )?))
            }
            "bash" => {
                let args: BashArgs = parse(name, args_json)?;
                let t = args
                    .timeout_ms
                    .map(Duration::from_millis)
                    .unwrap_or(self.bash_timeout)
                    .min(Duration::from_secs(120));
                Ok(ToolOutput::Text(self.bash(&args.command, t).await?))
            }
            "report" => {
                let mut report: Report = parse(name, args_json)?;
                if !args_json.contains("success") {
                    report.success = true;
                }
                Ok(ToolOutput::Report(report))
            }
            "escalate" => {
                let mut park: ParkRequest = parse(name, args_json)?;
                if park.question.is_empty() {
                    park.question = park.reason.clone();
                }
                Ok(ToolOutput::Park(park))
            }
            other => Err(ToolError::UnknownTool(other.into())),
        }
    }

    async fn bash(&self, script: &str, limit: Duration) -> Result<String, ToolError> {
        let mut command = self.bash_command(script)?;
        command.stdout(Stdio::piped()).stderr(Stdio::piped());
        let child = command.spawn()?;
        let pid = child.id();
        // kill_on_drop SIGKILLs only the bash parent. rlimit children live in a
        // new process group; dropping this future (fence-loss / cancel) must
        // still reap the group or descendants keep mutating the workspace.
        let _guard = ProcessGroupGuard {
            sandbox: &self.sandbox,
            pid,
        };

        let output = match timeout(limit, child.wait_with_output()).await {
            Ok(Ok(o)) => o,
            Ok(Err(e)) => return Err(ToolError::Io(e)),
            Err(_) => {
                return Err(ToolError::BashTimeout(limit));
            }
        };
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        let combined = match (stdout.trim(), stderr.trim()) {
            (o, "") => o.to_string(),
            ("", e) => e.to_string(),
            (o, e) => format!("{o}\n{e}"),
        };
        if combined.len() > MAX_BASH_OUTPUT_BYTES {
            return Err(ToolError::BashOutputLimit);
        }
        if !output.status.success() {
            return Err(ToolError::BashFailed {
                status: output.status.to_string(),
                output: combined,
            });
        }
        Ok(combined)
    }
}

struct ProcessGroupGuard<'a> {
    sandbox: &'a crate::sandbox::Sandbox,
    pid: Option<u32>,
}

impl Drop for ProcessGroupGuard<'_> {
    fn drop(&mut self) {
        self.sandbox.kill_process_group(self.pid);
    }
}
