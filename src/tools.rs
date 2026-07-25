//! Workspace-jailed tools for the agent loop.

use std::path::{Component, Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use serde::Deserialize;
use thiserror::Error;
use tokio::process::Command;
use tokio::time::timeout;

const MAX_FILE_BYTES: u64 = 2 * 1024 * 1024;
const MAX_BASH_OUTPUT_BYTES: usize = 2 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolOutput {
    Text(String),
    Report(Report),
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Report {
    pub summary: String,
    #[serde(default)]
    pub success: bool,
}

#[derive(Debug, Error)]
pub enum ToolError {
    #[error("invalid arguments for {tool}: {source}")]
    InvalidArguments {
        tool: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("workspace path must be relative and must not traverse parents: {0}")]
    UnsafePath(PathBuf),
    #[error("path escapes workspace: {0}")]
    PathEscape(PathBuf),
    #[error("not a regular file: {0}")]
    NotRegular(PathBuf),
    #[error("file exceeds {MAX_FILE_BYTES} bytes: {0}")]
    FileTooLarge(PathBuf),
    #[error("edit target must occur exactly once, found {count}")]
    EditMatch { count: usize },
    #[error("bash timed out after {0:?}")]
    BashTimeout(Duration),
    #[error("bash output exceeded limit")]
    BashOutputLimit,
    #[error("bash failed with status {status}: {output}")]
    BashFailed { status: String, output: String },
    #[error("tool not enabled: {0}")]
    Disabled(String),
    #[error("unknown tool: {0}")]
    UnknownTool(String),
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Clone)]
pub struct ToolExecutor {
    workspace: PathBuf,
    enabled: Vec<String>,
    bash_timeout: Duration,
}

impl ToolExecutor {
    pub fn new(
        workspace: impl Into<PathBuf>,
        enabled: Vec<String>,
        bash_timeout_secs: u64,
    ) -> Result<Self, ToolError> {
        let workspace = std::fs::canonicalize(workspace.into())?;
        Ok(Self {
            workspace,
            enabled,
            bash_timeout: Duration::from_secs(bash_timeout_secs.max(1)),
        })
    }

    pub fn definitions_json(enabled: &[String]) -> Vec<ToolDef> {
        let all = [
            ToolDef {
                name: "read_file",
                description: "Read a UTF-8 text file relative to the workspace root.",
                schema: r#"{"type":"object","properties":{"path":{"type":"string"}},"required":["path"]}"#,
            },
            ToolDef {
                name: "write_file",
                description: "Write a UTF-8 text file relative to the workspace root.",
                schema: r#"{"type":"object","properties":{"path":{"type":"string"},"content":{"type":"string"}},"required":["path","content"]}"#,
            },
            ToolDef {
                name: "edit",
                description: "Replace exactly one occurrence of old with new in a file.",
                schema: r#"{"type":"object","properties":{"path":{"type":"string"},"old":{"type":"string"},"new":{"type":"string"}},"required":["path","old","new"]}"#,
            },
            ToolDef {
                name: "bash",
                description: "Run a shell command inside the workspace (timeout-bounded).",
                schema: r#"{"type":"object","properties":{"command":{"type":"string"},"timeout_ms":{"type":"integer"}},"required":["command"]}"#,
            },
            ToolDef {
                name: "report",
                description: "Finish the run with a structured summary. Must be the only call in the batch.",
                schema: r#"{"type":"object","properties":{"summary":{"type":"string"},"success":{"type":"boolean"}},"required":["summary"]}"#,
            },
        ];
        all.into_iter()
            .filter(|d| enabled.iter().any(|e| e == d.name))
            .collect()
    }

    pub async fn execute(&self, name: &str, args_json: &str) -> Result<ToolOutput, ToolError> {
        if !self.enabled.iter().any(|e| e == name) {
            return Err(ToolError::Disabled(name.into()));
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
            other => Err(ToolError::UnknownTool(other.into())),
        }
    }

    fn resolve(&self, relative: &Path) -> Result<PathBuf, ToolError> {
        if relative.is_absolute()
            || relative.components().any(|c| {
                matches!(
                    c,
                    Component::ParentDir | Component::RootDir | Component::Prefix(_)
                )
            })
        {
            return Err(ToolError::UnsafePath(relative.to_path_buf()));
        }
        let joined = self.workspace.join(relative);
        if let Some(parent) = joined.parent() {
            std::fs::create_dir_all(parent)?;
        }
        // For existing paths, canonicalize and ensure under workspace.
        if joined.exists() {
            let canon = std::fs::canonicalize(&joined)?;
            if !canon.starts_with(&self.workspace) {
                return Err(ToolError::PathEscape(relative.to_path_buf()));
            }
            return Ok(canon);
        }
        // New file: canonicalize parent.
        if let Some(parent) = joined.parent() {
            let parent_canon =
                std::fs::canonicalize(parent).unwrap_or_else(|_| parent.to_path_buf());
            if !parent_canon.starts_with(&self.workspace)
                && parent_canon != self.workspace
                && !self.workspace.starts_with(&parent_canon)
            {
                // parent may be workspace itself after create_dir_all
                let ws = &self.workspace;
                if !parent_canon.starts_with(ws) && parent_canon != *ws {
                    return Err(ToolError::PathEscape(relative.to_path_buf()));
                }
            }
        }
        Ok(joined)
    }

    fn read_file(&self, path: &Path) -> Result<String, ToolError> {
        let path = self.resolve(path)?;
        let meta = std::fs::metadata(&path)?;
        if !meta.is_file() {
            return Err(ToolError::NotRegular(path));
        }
        if meta.len() > MAX_FILE_BYTES {
            return Err(ToolError::FileTooLarge(path));
        }
        Ok(std::fs::read_to_string(path)?)
    }

    fn write_file(&self, path: &Path, content: &str) -> Result<(), ToolError> {
        if content.len() as u64 > MAX_FILE_BYTES {
            return Err(ToolError::FileTooLarge(path.to_path_buf()));
        }
        let path = self.resolve(path)?;
        std::fs::write(path, content)?;
        Ok(())
    }

    fn edit(&self, path: &Path, old: &str, new: &str) -> Result<(), ToolError> {
        let text = self.read_file(path)?;
        let count = text.matches(old).count();
        if count != 1 {
            return Err(ToolError::EditMatch { count });
        }
        let updated = text.replacen(old, new, 1);
        self.write_file(path, &updated)
    }

    async fn bash(&self, script: &str, limit: Duration) -> Result<String, ToolError> {
        let child = Command::new("bash")
            .arg("-lc")
            .arg(script)
            .current_dir(&self.workspace)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;

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

#[derive(Debug, Clone)]
pub struct ToolDef {
    pub name: &'static str,
    pub description: &'static str,
    pub schema: &'static str,
}

#[derive(Deserialize)]
struct PathArgs {
    path: PathBuf,
}

#[derive(Deserialize)]
struct WriteArgs {
    path: PathBuf,
    content: String,
}

#[derive(Deserialize)]
struct EditArgs {
    path: PathBuf,
    old: String,
    new: String,
}

#[derive(Deserialize)]
struct BashArgs {
    command: String,
    timeout_ms: Option<u64>,
}

fn parse<T: for<'de> Deserialize<'de>>(tool: &str, raw: &str) -> Result<T, ToolError> {
    serde_json::from_str(raw).map_err(|source| ToolError::InvalidArguments {
        tool: tool.into(),
        source,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn write_read_edit_report() {
        let dir = tempdir().unwrap();
        let tools = ToolExecutor::new(
            dir.path(),
            vec![
                "read_file".into(),
                "write_file".into(),
                "edit".into(),
                "report".into(),
            ],
            30,
        )
        .unwrap();
        tools
            .execute("write_file", r#"{"path":"a.txt","content":"hello"}"#)
            .await
            .unwrap();
        let out = tools
            .execute("read_file", r#"{"path":"a.txt"}"#)
            .await
            .unwrap();
        assert_eq!(out, ToolOutput::Text("hello".into()));
        tools
            .execute("edit", r#"{"path":"a.txt","old":"hello","new":"world"}"#)
            .await
            .unwrap();
        let out = tools
            .execute("report", r#"{"summary":"done","success":true}"#)
            .await
            .unwrap();
        assert!(matches!(out, ToolOutput::Report(r) if r.success));
    }

    #[tokio::test]
    async fn rejects_absolute_path() {
        let dir = tempdir().unwrap();
        let tools = ToolExecutor::new(dir.path(), vec!["read_file".into()], 30).unwrap();
        let err = tools
            .execute("read_file", r#"{"path":"/etc/passwd"}"#)
            .await
            .unwrap_err();
        assert!(matches!(err, ToolError::UnsafePath(_)));
    }

    #[tokio::test]
    async fn rejects_parent_path() {
        let dir = tempdir().unwrap();
        let tools = ToolExecutor::new(dir.path(), vec!["read_file".into()], 30).unwrap();
        let err = tools
            .execute("read_file", r#"{"path":"../secret"}"#)
            .await
            .unwrap_err();
        assert!(matches!(err, ToolError::UnsafePath(_)));
    }
}
