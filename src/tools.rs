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
const MAX_SEARCH_MATCHES: usize = 200;
const MAX_SEARCH_OUTPUT_BYTES: usize = 256 * 1024;
const MAX_WALK_FILES: usize = 5_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolOutput {
    Text(String),
    Report(Report),
    /// Headless escalation: park the run until an operator answer is supplied.
    Park(ParkRequest),
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Report {
    pub summary: String,
    #[serde(default)]
    pub success: bool,
}

/// Payload produced by the `escalate` tool (operator decision required).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ParkRequest {
    /// Why the run cannot continue unattended.
    pub reason: String,
    /// Question or decision for the operator (may equal reason).
    #[serde(default)]
    pub question: String,
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
    #[error("multi_edit index {index}: old must occur exactly once, found {count}")]
    MultiEditMatch { index: usize, count: usize },
    #[error("multi_edit requires a non-empty edits array")]
    MultiEditEmpty,
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
    #[error("invalid search pattern: {0}")]
    InvalidPattern(String),
    #[error("search truncated after {0} matches")]
    SearchTruncated(usize),
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    Message(String),
}

/// Catalog entry for a tool the registry can enable.
#[derive(Debug, Clone)]
pub struct ToolDef {
    pub name: String,
    pub description: String,
    pub schema: String,
}

fn def(name: &str, description: &str, schema: &str) -> ToolDef {
    ToolDef {
        name: name.into(),
        description: description.into(),
        schema: schema.into(),
    }
}

/// Builtin tool catalog (registration bootstrap). Dynamic plugins are out of scope;
/// future MCP/skill tools register into [`ToolRegistry`] without changing the turn loop.
pub fn builtin_catalog() -> Vec<ToolDef> {
    vec![
        def(
            "read_file",
            "Read a UTF-8 text file relative to the workspace root.",
            r#"{"type":"object","properties":{"path":{"type":"string"}},"required":["path"]}"#,
        ),
        def(
            "write_file",
            "Write a UTF-8 text file relative to the workspace root.",
            r#"{"type":"object","properties":{"path":{"type":"string"},"content":{"type":"string"}},"required":["path","content"]}"#,
        ),
        def(
            "edit",
            "Replace exactly one occurrence of old with new in a file.",
            r#"{"type":"object","properties":{"path":{"type":"string"},"old":{"type":"string"},"new":{"type":"string"}},"required":["path","old","new"]}"#,
        ),
        def(
            "multi_edit",
            "Apply multiple exact single-occurrence replacements to one file atomically (all succeed or none).",
            r#"{"type":"object","properties":{"path":{"type":"string"},"edits":{"type":"array","items":{"type":"object","properties":{"old":{"type":"string"},"new":{"type":"string"}},"required":["old","new"]}},"required":["path","edits"]}"#,
        ),
        def(
            "glob",
            "List workspace-relative file paths matching a glob (supports * and **). Results are capped.",
            r#"{"type":"object","properties":{"pattern":{"type":"string"},"path":{"type":"string"}},"required":["pattern"]}"#,
        ),
        def(
            "grep",
            "Search file contents under the workspace with a regex. Results are capped.",
            r#"{"type":"object","properties":{"pattern":{"type":"string"},"path":{"type":"string"},"max_matches":{"type":"integer"}},"required":["pattern"]}"#,
        ),
        def(
            "bash",
            "Run a shell command inside the workspace (timeout-bounded).",
            r#"{"type":"object","properties":{"command":{"type":"string"},"timeout_ms":{"type":"integer"}},"required":["command"]}"#,
        ),
        def(
            "report",
            "Finish the run with a structured summary. Must be the only call in the batch.",
            r#"{"type":"object","properties":{"summary":{"type":"string"},"success":{"type":"boolean"}},"required":["summary"]}"#,
        ),
        def(
            "escalate",
            "Park the headless run and ask an operator a question. Must be the only call in the batch. Resume later with an answer.",
            r#"{"type":"object","properties":{"reason":{"type":"string"},"question":{"type":"string"}},"required":["reason"]}"#,
        ),
    ]
}

/// Whether this tool must be the only call in a model batch.
pub fn must_be_exclusive_batch(name: &str) -> bool {
    matches!(name, "report" | "escalate")
}

/// External tool provider (e.g. MCP-backed tool).
#[async_trait::async_trait]
pub trait ExternalTool: Send + Sync {
    fn definition(&self) -> ToolDef;
    async fn call(&self, args_json: &str) -> Result<String, ToolError>;
}

/// Run-scoped tool registry: definitions + jailed execution for enabled tools.
///
/// The turn loop talks only to this type (not individual dispatch tables).
pub struct ToolRegistry {
    executor: ToolExecutor,
    external: Vec<std::sync::Arc<dyn ExternalTool>>,
}

impl ToolRegistry {
    /// Bootstrap builtins filtered by the settings allow-list.
    pub fn with_builtins(
        workspace: impl Into<PathBuf>,
        enabled: Vec<String>,
        bash_timeout_secs: u64,
    ) -> Result<Self, ToolError> {
        Ok(Self {
            executor: ToolExecutor::new(workspace, enabled, bash_timeout_secs)?,
            external: Vec::new(),
        })
    }

    pub fn register_external(&mut self, tool: std::sync::Arc<dyn ExternalTool>) {
        self.external.push(tool);
    }

    /// Model-facing tool definitions for enabled builtins + external tools.
    pub fn definitions(&self) -> Vec<ToolDef> {
        let mut defs = definitions_for_enabled(&self.executor.enabled);
        for t in &self.external {
            defs.push(t.definition());
        }
        defs
    }

    pub fn enabled(&self) -> &[String] {
        &self.executor.enabled
    }

    pub fn is_enabled(&self, name: &str) -> bool {
        self.executor.enabled.iter().any(|e| e == name)
            || self.external.iter().any(|t| t.definition().name == name)
    }

    pub async fn execute(&self, name: &str, args_json: &str) -> Result<ToolOutput, ToolError> {
        if let Some(t) = self.external.iter().find(|t| t.definition().name == name) {
            return Ok(ToolOutput::Text(t.call(args_json).await?));
        }
        self.executor.execute(name, args_json).await
    }
}

/// Definitions for an allow-list against the builtin catalog.
pub fn definitions_for_enabled(enabled: &[String]) -> Vec<ToolDef> {
    builtin_catalog()
        .into_iter()
        .filter(|d| enabled.iter().any(|e| e == d.name.as_str()))
        .collect()
}

/// Workspace-jailed executor used by [`ToolRegistry`] (and tests).
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

    /// Backward-compatible alias for [`definitions_for_enabled`].
    pub fn definitions_json(enabled: &[String]) -> Vec<ToolDef> {
        definitions_for_enabled(enabled)
    }

    pub async fn execute(&self, name: &str, args_json: &str) -> Result<ToolOutput, ToolError> {
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

    fn resolve(&self, relative: &Path) -> Result<PathBuf, ToolError> {
        if is_unsafe_relative_path(relative) {
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

    fn multi_edit(&self, path: &Path, edits: &[EditHunk]) -> Result<usize, ToolError> {
        if edits.is_empty() {
            return Err(ToolError::MultiEditEmpty);
        }
        let mut text = self.read_file(path)?;
        for (index, hunk) in edits.iter().enumerate() {
            let count = text.matches(&hunk.old).count();
            if count != 1 {
                return Err(ToolError::MultiEditMatch { index, count });
            }
            text = text.replacen(&hunk.old, &hunk.new, 1);
        }
        self.write_file(path, &text)?;
        Ok(edits.len())
    }

    fn glob_files(&self, pattern: &str, under: Option<&Path>) -> Result<String, ToolError> {
        if pattern.is_empty() {
            return Err(ToolError::InvalidPattern("empty glob pattern".into()));
        }
        let base = self.search_base(under)?;
        let mut matches = Vec::new();
        let mut truncated = false;
        self.walk_files(&base, &mut |rel| {
            let rel_str = rel.to_string_lossy();
            if glob_match(pattern, rel_str.as_ref()) {
                if matches.len() >= MAX_SEARCH_MATCHES {
                    truncated = true;
                    return false;
                }
                matches.push(rel_str.into_owned());
            }
            true
        })?;
        matches.sort();
        let mut out = matches.join("\n");
        if truncated {
            out.push_str(&format!("\n… truncated after {MAX_SEARCH_MATCHES} matches"));
        }
        if out.len() > MAX_SEARCH_OUTPUT_BYTES {
            out.truncate(MAX_SEARCH_OUTPUT_BYTES);
            out.push_str("\n… output byte limit");
        }
        if out.is_empty() {
            out = "(no matches)".into();
        }
        Ok(out)
    }

    fn grep_files(
        &self,
        pattern: &str,
        under: Option<&Path>,
        max_matches: usize,
    ) -> Result<String, ToolError> {
        let re =
            regex::Regex::new(pattern).map_err(|e| ToolError::InvalidPattern(e.to_string()))?;
        let base = self.search_base(under)?;
        let mut lines = Vec::new();
        let mut truncated = false;
        self.walk_files(&base, &mut |rel| {
            if truncated {
                return false;
            }
            let abs = self.workspace.join(&rel);
            let Ok(meta) = std::fs::metadata(&abs) else {
                return true;
            };
            if !meta.is_file() || meta.len() > MAX_FILE_BYTES {
                return true;
            }
            let Ok(text) = std::fs::read_to_string(&abs) else {
                return true; // skip binary / invalid utf-8
            };
            for (i, line) in text.lines().enumerate() {
                if re.is_match(line) {
                    if lines.len() >= max_matches {
                        truncated = true;
                        return false;
                    }
                    lines.push(format!("{}:{}:{line}", rel.display(), i + 1));
                }
            }
            true
        })?;
        let mut out = lines.join("\n");
        if truncated {
            out.push_str(&format!("\n… truncated after {max_matches} matches"));
        }
        if out.len() > MAX_SEARCH_OUTPUT_BYTES {
            out.truncate(MAX_SEARCH_OUTPUT_BYTES);
            out.push_str("\n… output byte limit");
        }
        if out.is_empty() {
            out = "(no matches)".into();
        }
        Ok(out)
    }

    fn search_base(&self, under: Option<&Path>) -> Result<PathBuf, ToolError> {
        match under {
            None => Ok(self.workspace.clone()),
            Some(p) if p.as_os_str().is_empty() || p == Path::new(".") => {
                Ok(self.workspace.clone())
            }
            Some(p) => {
                if is_unsafe_relative_path(p) {
                    return Err(ToolError::UnsafePath(p.to_path_buf()));
                }
                let joined = self.workspace.join(p);
                let canon = std::fs::canonicalize(&joined).map_err(ToolError::Io)?;
                if !canon.starts_with(&self.workspace) {
                    return Err(ToolError::PathEscape(p.to_path_buf()));
                }
                Ok(canon)
            }
        }
    }

    /// Walk files under `base` (absolute, inside workspace). Callback gets workspace-relative path.
    /// Return false from callback to stop early.
    fn walk_files(
        &self,
        base: &Path,
        visit: &mut dyn FnMut(PathBuf) -> bool,
    ) -> Result<(), ToolError> {
        if base.is_file() {
            let rel = base
                .strip_prefix(&self.workspace)
                .map_err(|_| ToolError::PathEscape(base.to_path_buf()))?
                .to_path_buf();
            if is_unsafe_relative_path(&rel) && rel != Path::new("") {
                return Err(ToolError::UnsafePath(rel));
            }
            let _ = visit(rel);
            return Ok(());
        }
        let mut stack = vec![base.to_path_buf()];
        let mut seen = 0usize;
        while let Some(dir) = stack.pop() {
            let rd = std::fs::read_dir(&dir)?;
            for entry in rd {
                let entry = entry?;
                let path = entry.path();
                let meta = entry.metadata()?;
                if meta.is_symlink() {
                    continue; // do not follow symlinks out of jail
                }
                if meta.is_dir() {
                    stack.push(path);
                    continue;
                }
                if !meta.is_file() {
                    continue;
                }
                let canon = match std::fs::canonicalize(&path) {
                    Ok(c) => c,
                    Err(_) => continue,
                };
                if !canon.starts_with(&self.workspace) {
                    continue;
                }
                let rel = canon
                    .strip_prefix(&self.workspace)
                    .map_err(|_| ToolError::PathEscape(path))?
                    .to_path_buf();
                seen += 1;
                if seen > MAX_WALK_FILES {
                    return Ok(());
                }
                if !visit(rel) {
                    return Ok(());
                }
            }
        }
        Ok(())
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

/// True when a path must be rejected by the workspace jail before I/O.
/// Relative paths may contain only normal components (no `..`, roots, prefixes).
pub fn is_unsafe_relative_path(relative: &Path) -> bool {
    relative.is_absolute()
        || relative.components().any(|c| {
            matches!(
                c,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
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
struct EditHunk {
    old: String,
    new: String,
}

#[derive(Deserialize)]
struct MultiEditArgs {
    path: PathBuf,
    edits: Vec<EditHunk>,
}

#[derive(Deserialize)]
struct BashArgs {
    command: String,
    timeout_ms: Option<u64>,
}

#[derive(Deserialize)]
struct GlobArgs {
    pattern: String,
    #[serde(default)]
    path: Option<PathBuf>,
}

#[derive(Deserialize)]
struct GrepArgs {
    pattern: String,
    #[serde(default)]
    path: Option<PathBuf>,
    #[serde(default)]
    max_matches: Option<usize>,
}

/// Simple glob: `*` (within segment), `**` (across segments). No `{a,b}` classes.
fn glob_match(pattern: &str, path: &str) -> bool {
    let path = path.replace('\\', "/");
    let pattern = pattern.replace('\\', "/");
    let pat: Vec<&str> = pattern.split('/').filter(|s| !s.is_empty()).collect();
    let segs: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    glob_match_segs(&pat, &segs)
}

fn glob_match_segs(pat: &[&str], path: &[&str]) -> bool {
    match (pat.first().copied(), path.first().copied()) {
        (None, None) => true,
        (Some("**"), _) => {
            // Match empty prefix or consume one path segment and retry.
            glob_match_segs(&pat[1..], path)
                || (!path.is_empty() && glob_match_segs(pat, &path[1..]))
        }
        (Some(p), Some(s)) if segment_match(p, s) => glob_match_segs(&pat[1..], &path[1..]),
        (Some(_), None) => pat.iter().all(|p| *p == "**"),
        _ => false,
    }
}

fn segment_match(pat: &str, seg: &str) -> bool {
    if pat == "*" || pat == "**" {
        return true;
    }
    let pb: Vec<char> = pat.chars().collect();
    let sb: Vec<char> = seg.chars().collect();
    let (mut i, mut j) = (0usize, 0usize);
    let mut star = None;
    let mut star_j = 0usize;
    while j < sb.len() {
        if i < pb.len() && (pb[i] == '?' || pb[i] == sb[j]) {
            i += 1;
            j += 1;
        } else if i < pb.len() && pb[i] == '*' {
            star = Some(i);
            star_j = j;
            i += 1;
        } else if let Some(si) = star {
            i = si + 1;
            star_j += 1;
            j = star_j;
        } else {
            return false;
        }
    }
    while i < pb.len() && pb[i] == '*' {
        i += 1;
    }
    i == pb.len()
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

    #[test]
    fn property_parent_or_absolute_paths_are_unsafe() {
        use proptest::prelude::*;
        proptest!(|(suffix in "[a-zA-Z0-9._-]{1,24}")| {
            let parent = PathBuf::from(format!("../{suffix}"));
            prop_assert!(is_unsafe_relative_path(&parent));
            let abs = PathBuf::from(format!("/{suffix}"));
            prop_assert!(is_unsafe_relative_path(&abs));
            let nested = PathBuf::from(format!("ok/../../{suffix}"));
            prop_assert!(is_unsafe_relative_path(&nested));
        });
    }

    #[test]
    fn property_simple_relative_paths_are_safe() {
        use proptest::prelude::*;
        proptest!(|(name in "[a-zA-Z0-9][a-zA-Z0-9._-]{0,31}")| {
            // No separators, no dots-only — always a single normal component.
            let path = PathBuf::from(&name);
            prop_assert!(!is_unsafe_relative_path(&path), "{path:?}");
        });
    }

    #[test]
    fn registry_definitions_match_enabled_builtins() {
        let dir = tempdir().unwrap();
        let reg = ToolRegistry::with_builtins(
            dir.path(),
            vec!["read_file".into(), "report".into(), "not_a_tool".into()],
            30,
        )
        .unwrap();
        let defs = reg.definitions();
        let names: Vec<_> = defs.iter().map(|d| d.name.as_str()).collect();
        assert_eq!(names, vec!["read_file", "report"]);
        assert!(must_be_exclusive_batch("report"));
        assert!(must_be_exclusive_batch("escalate"));
        assert!(!must_be_exclusive_batch("bash"));
    }

    #[tokio::test]
    async fn registry_unknown_enabled_name_fails_closed() {
        let dir = tempdir().unwrap();
        let reg =
            ToolRegistry::with_builtins(dir.path(), vec!["not_registered".into()], 30).unwrap();
        let err = reg.execute("not_registered", "{}").await.unwrap_err();
        assert!(matches!(err, ToolError::UnknownTool(_)));
    }

    #[tokio::test]
    async fn glob_and_grep_respect_workspace() {
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("src")).unwrap();
        std::fs::write(dir.path().join("src/a.rs"), "fn hello() {}\n").unwrap();
        std::fs::write(dir.path().join("src/b.txt"), "hello world\n").unwrap();
        let tools = ToolExecutor::new(dir.path(), vec!["glob".into(), "grep".into()], 30).unwrap();
        let glob_out = tools
            .execute("glob", r#"{"pattern":"**/*.rs"}"#)
            .await
            .unwrap();
        let ToolOutput::Text(g) = glob_out else {
            panic!("expected text");
        };
        assert!(g.contains("src/a.rs") || g.contains("src\\a.rs"), "{g}");
        assert!(!g.contains("b.txt"), "{g}");

        let grep_out = tools
            .execute("grep", r#"{"pattern":"hello","path":"src"}"#)
            .await
            .unwrap();
        let ToolOutput::Text(t) = grep_out else {
            panic!("expected text");
        };
        assert!(t.contains("hello"), "{t}");

        let err = tools
            .execute("grep", r#"{"pattern":"[","path":"."}"#)
            .await
            .unwrap_err();
        assert!(matches!(err, ToolError::InvalidPattern(_)));

        let jail = tools
            .execute("glob", r#"{"pattern":"*","path":".."}"#)
            .await
            .unwrap_err();
        assert!(matches!(jail, ToolError::UnsafePath(_)));
    }

    #[test]
    fn glob_match_doublestar() {
        assert!(glob_match("**/*.rs", "src/lib.rs"));
        assert!(glob_match("*.txt", "a.txt"));
        assert!(!glob_match("*.txt", "src/a.txt"));
        assert!(glob_match("src/**", "src/a/b"));
    }

    #[tokio::test]
    async fn multi_edit_atomic() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("f.txt"), "one two three\n").unwrap();
        let tools = ToolExecutor::new(dir.path(), vec!["multi_edit".into()], 30).unwrap();
        tools
            .execute(
                "multi_edit",
                r#"{"path":"f.txt","edits":[{"old":"one","new":"1"},{"old":"three","new":"3"}]}"#,
            )
            .await
            .unwrap();
        let text = std::fs::read_to_string(dir.path().join("f.txt")).unwrap();
        assert_eq!(text, "1 two 3\n");

        // Ambiguous second edit fails; file unchanged from previous success only after failure path:
        std::fs::write(dir.path().join("g.txt"), "aa aa\n").unwrap();
        let err = tools
            .execute(
                "multi_edit",
                r#"{"path":"g.txt","edits":[{"old":"aa","new":"b"}]}"#,
            )
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            ToolError::MultiEditMatch { index: 0, count: 2 }
        ));
        assert_eq!(
            std::fs::read_to_string(dir.path().join("g.txt")).unwrap(),
            "aa aa\n"
        );
    }
}
