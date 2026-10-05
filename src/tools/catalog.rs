//! Builtin tool catalog and parallel/exclusive batch helpers.

/// Catalog entry for a tool the registry can enable.
#[derive(Debug, Clone)]
pub struct ToolDef {
    pub name: String,
    pub description: String,
    pub schema: String,
}

/// Replay authority assigned before a tool call can enter execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReplayToolAuthority {
    Observation,
    Terminal,
    Denied,
}

fn def(name: &str, description: &str, schema: &str) -> ToolDef {
    ToolDef {
        name: name.into(),
        description: description.into(),
        schema: schema.into(),
    }
}

/// Builtin tool catalog (registration bootstrap). Dynamic native plugins are
/// out of scope. MCP servers register as `mcp.<name>.<tool>` into
/// [`crate::tools::ToolRegistry`] without changing the turn loop; skills load
/// as prompt context, not catalog tools.
/// Workspace-mutating builtins. Session hosts park these until ask=park allow.
pub fn mutates_workspace(name: &str) -> bool {
    matches!(
        name,
        "write_file" | "edit" | "multi_edit" | "apply_patch" | "bash" | "bash_background"
    )
}

/// Harness-owned plan file. Plan-jail runs may write only this workspace path.
pub const PLAN_JAIL_PATH: &str = ".shikigami/plan.md";

/// Whether a tool is allowed while plan-jail is active.
///
/// Unknown and external names (including `mcp.*`) fail closed. Observation
/// builtins, report/escalate, todos, and bash job polling stay allowed.
/// Mutating builtins may write only [`PLAN_JAIL_PATH`]. Shared-workspace
/// `child_run` is allowed; `worktree=true` is not, because materialize
/// runs unsandboxed `git worktree add` against the parent checkout.
pub fn plan_jail_allows(name: &str, args_json: &str) -> bool {
    match name {
        "read_file" | "glob" | "grep" | "web_fetch" | "todo_write" | "report" | "escalate"
        | "bash_job_status" | "bash_job_logs" | "child_status" => true,
        "child_run" => child_run_plan_jail_ok(args_json),
        "write_file" | "edit" | "multi_edit" => json_path_is_plan(args_json, "path"),
        "apply_patch" => apply_patch_is_plan(args_json),
        _ => false,
    }
}

/// True when every existing component of [`PLAN_JAIL_PATH`] under `workspace`
/// is a real directory or file (not a symlink). Missing components are ok:
/// the executor creates them as regular paths.
pub fn plan_jail_destination_ok(workspace: &std::path::Path) -> bool {
    use std::path::{Component, Path};
    let mut current = workspace.to_path_buf();
    for component in Path::new(PLAN_JAIL_PATH).components() {
        let Component::Normal(name) = component else {
            return false;
        };
        current.push(name);
        let last = current == workspace.join(PLAN_JAIL_PATH);
        match std::fs::symlink_metadata(&current) {
            Ok(meta) if meta.file_type().is_symlink() => return false,
            Ok(meta) if last && (!meta.is_file() || file_link_count(&meta) > 1) => return false,
            Ok(meta) if !last && !meta.is_dir() => return false,
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return false,
        }
    }
    true
}

/// Read the plan file without following symlinks. Missing, redirected, or
/// oversized files yield `None` so review never copies a host path into the
/// park payload.
pub fn read_plan_jail_file(workspace: &std::path::Path) -> Option<Vec<u8>> {
    use std::io::Read;
    if !plan_jail_destination_ok(workspace) {
        return None;
    }
    let path = workspace.join(PLAN_JAIL_PATH);
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    #[cfg(not(unix))]
    {
        let meta = std::fs::symlink_metadata(&path).ok()?;
        if meta.file_type().is_symlink() || !meta.is_file() {
            return None;
        }
    }
    let mut file = options.open(&path).ok()?;
    let meta = file.metadata().ok()?;
    if !meta.is_file() || meta.len() > 2 * 1024 * 1024 {
        return None;
    }
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).ok()?;
    Some(bytes)
}

fn file_link_count(meta: &std::fs::Metadata) -> u64 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        meta.nlink()
    }
    #[cfg(not(unix))]
    {
        let _ = meta;
        1
    }
}

fn normalize_rel_path(raw: &str) -> Option<String> {
    use std::path::{Component, Path};
    // Do not trim: the executor writes the raw path bytes.
    let path = Path::new(raw);
    if super::is_unsafe_relative_path(path) {
        return None;
    }
    let mut parts = Vec::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::Normal(part) => parts.push(part.to_string_lossy().into_owned()),
            _ => return None,
        }
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join("/"))
    }
}

fn child_run_plan_jail_ok(args_json: &str) -> bool {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(args_json) else {
        return false;
    };
    value.get("worktree") != Some(&serde_json::Value::Bool(true))
}

fn json_path_is_plan(args_json: &str, field: &str) -> bool {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(args_json) else {
        return false;
    };
    value
        .get(field)
        .and_then(|v| v.as_str())
        .and_then(normalize_rel_path)
        .is_some_and(|path| path == PLAN_JAIL_PATH)
}

fn apply_patch_is_plan(args_json: &str) -> bool {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(args_json) else {
        return false;
    };
    let Some(patches) = value.get("patches").and_then(|v| v.as_array()) else {
        return false;
    };
    !patches.is_empty()
        && patches.iter().all(|patch| {
            patch
                .get("path")
                .and_then(|v| v.as_str())
                .and_then(normalize_rel_path)
                .is_some_and(|path| path == PLAN_JAIL_PATH)
        })
}

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
            "apply_patch",
            "Apply structured multi-hunk patches with optional surrounding context. Atomic across all files/hunks (all succeed or none). Prefer when multi_edit exact matches are too brittle. Fails closed on 0 or >1 matches.",
            r#"{"type":"object","properties":{"patches":{"type":"array","items":{"type":"object","properties":{"path":{"type":"string"},"hunks":{"type":"array","items":{"type":"object","properties":{"context_before":{"type":"string"},"old":{"type":"string"},"new":{"type":"string"},"context_after":{"type":"string"}},"required":["old","new"]}}},"required":["path","hunks"]}}},"required":["patches"]}"#,
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
            "bash_background",
            "Start a background shell command in the workspace; returns job_id. Poll with bash_job_status / bash_job_logs. Jobs are killed when the run ends.",
            r#"{"type":"object","properties":{"command":{"type":"string"}},"required":["command"]}"#,
        ),
        def(
            "bash_job_status",
            "Status of a background bash job (running|exited|unknown).",
            r#"{"type":"object","properties":{"job_id":{"type":"string"}},"required":["job_id"]}"#,
        ),
        def(
            "bash_job_logs",
            "Tail combined stdout/stderr of a background bash job (capped).",
            r#"{"type":"object","properties":{"job_id":{"type":"string"},"max_bytes":{"type":"integer"}},"required":["job_id"]}"#,
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
        def(
            "todo_write",
            "Replace the run-scoped todo checklist (max 32 items). Not a substitute for escalate/park or plane work-units. Persist across checkpoint resume.",
            r#"{"type":"object","properties":{"items":{"type":"array","items":{"type":"object","properties":{"id":{"type":"string"},"content":{"type":"string"},"status":{"type":"string","enum":["pending","in_progress","completed","cancelled"]}},"required":["id","content","status"]}}},"required":["items"]}"#,
        ),
        def(
            "web_fetch",
            "HTTP(S) GET a URL and return truncated text (status, final URL, body). Opt-in tool; respects [network] egress. Blocks private/link-local targets. Not a browser.",
            r#"{"type":"object","properties":{"url":{"type":"string"}},"required":["url"]}"#,
        ),
        def(
            "child_run",
            "Start a nested child Run. Profiles: explore (read-only), plan (write-jail), full (parent authority). The child shares the parent workspace; worktree=true isolates with git-worktree. wait (default true) returns the child summary; false returns the child run_id.",
            r#"{"type":"object","properties":{"profile":{"type":"string","enum":["explore","plan","full"]},"task":{"type":"string"},"wait":{"type":"boolean"},"worktree":{"type":"boolean"}},"required":["profile","task"]}"#,
        ),
        def(
            "child_status",
            "Poll a nested child started by this run.",
            r#"{"type":"object","properties":{"run_id":{"type":"string"}},"required":["run_id"]}"#,
        ),
    ]
}

/// Whether this tool must be the only call in a model batch.
pub fn must_be_exclusive_batch(name: &str) -> bool {
    matches!(name, "report" | "escalate")
}

/// Tools safe to run concurrently with each other (workspace reads plus
/// `web_fetch`; no workspace mutation).
///
/// Write tools, bash, todo_write, report/escalate stay serial for the whole batch.
pub fn is_parallel_safe_tool(name: &str) -> bool {
    matches!(name, "read_file" | "glob" | "grep" | "web_fetch")
}

/// Conservative replay classification. Unknown and external tools are denied.
pub(crate) fn replay_tool_authority(name: &str) -> ReplayToolAuthority {
    match name {
        "read_file" | "glob" | "grep" => ReplayToolAuthority::Observation,
        "report" => ReplayToolAuthority::Terminal,
        _ => ReplayToolAuthority::Denied,
    }
}

/// Background bash tools that share `bash` allow-list authority.
pub(crate) const BASH_HELPER_TOOLS: &[&str] =
    &["bash_background", "bash_job_status", "bash_job_logs"];

/// Definitions for an allow-list against the builtin catalog.
pub fn definitions_for_enabled(enabled: &[String]) -> Vec<ToolDef> {
    builtin_catalog()
        .into_iter()
        .filter(|d| enabled.iter().any(|e| e == d.name.as_str()))
        .collect()
}

/// Whether a builtin name is authorized by the allow-list, including bash
/// helpers that share `bash` authority. Helper names are not independently
/// enableable; they piggyback on `bash`.
pub fn builtin_is_authorized(enabled: &[String], name: &str) -> bool {
    if BASH_HELPER_TOOLS.contains(&name) {
        return enabled.iter().any(|tool| tool == "bash");
    }
    enabled.iter().any(|tool| tool == name)
}

fn with_bash_helpers(enabled: &[String]) -> Vec<String> {
    let mut expanded: Vec<String> = enabled
        .iter()
        .filter(|tool| !BASH_HELPER_TOOLS.contains(&tool.as_str()))
        .cloned()
        .collect();
    if enabled.iter().any(|tool| tool == "bash") {
        for implicit in BASH_HELPER_TOOLS {
            expanded.push((*implicit).into());
        }
    }
    expanded
}

/// Model-visible builtin definitions, including helpers that share bash
/// authority and excluding unknown configured names.
pub fn model_visible_builtin_definitions(enabled: &[String]) -> Vec<ToolDef> {
    definitions_for_enabled(&with_bash_helpers(enabled))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_jail_allows_only_the_plan_path() {
        let plan = format!(r#"{{"path":"{PLAN_JAIL_PATH}","content":"x"}}"#);
        assert!(plan_jail_allows("write_file", &plan));
        assert!(plan_jail_allows(
            "write_file",
            r#"{"path":"./.shikigami/plan.md","content":"x"}"#
        ));
        assert!(!plan_jail_allows(
            "write_file",
            r#"{"path":"ok.txt","content":"x"}"#
        ));
        assert!(!plan_jail_allows(
            "write_file",
            r#"{"path":"../.shikigami/plan.md","content":"x"}"#
        ));
        assert!(!plan_jail_allows("bash", r#"{"command":"echo hi"}"#));
        assert!(plan_jail_allows("read_file", r#"{"path":"ok.txt"}"#));
        assert!(plan_jail_allows(
            "apply_patch",
            &format!(r#"{{"patches":[{{"path":"{PLAN_JAIL_PATH}","hunks":[]}}]}}"#)
        ));
        assert!(!plan_jail_allows(
            "apply_patch",
            r#"{"patches":[{"path":"a.md","hunks":[]}]}"#
        ));
        assert!(!plan_jail_allows(
            "write_file",
            r#"{"path":".shikigami/plan.md ","content":"x"}"#
        ));
        assert!(!plan_jail_allows(
            "mcp.fs.write",
            &format!(r#"{{"path":"{PLAN_JAIL_PATH}","content":"x"}}"#)
        ));
        assert!(plan_jail_allows(
            "child_run",
            r#"{"profile":"explore","task":"scout"}"#
        ));
        assert!(plan_jail_allows(
            "child_run",
            r#"{"profile":"explore","task":"scout","worktree":false}"#
        ));
        assert!(!plan_jail_allows(
            "child_run",
            r#"{"profile":"explore","task":"scout","worktree":true}"#
        ));
        assert!(!plan_jail_allows("child_run", "not-json"));
        assert!(plan_jail_allows("child_status", r#"{"run_id":"child"}"#));
        assert!(!plan_jail_allows(
            "bash_background",
            r#"{"command":"echo hi"}"#
        ));
    }

    #[cfg(unix)]
    #[test]
    fn plan_jail_destination_rejects_symlinks() {
        let missing = tempfile::tempdir().unwrap();
        assert!(plan_jail_destination_ok(missing.path()));

        let regular = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(regular.path().join(".shikigami")).unwrap();
        std::fs::write(regular.path().join(PLAN_JAIL_PATH), "plan\n").unwrap();
        assert!(plan_jail_destination_ok(regular.path()));

        let file_link = tempfile::tempdir().unwrap();
        let secret = file_link.path().join("secret.txt");
        std::fs::write(&secret, "keep\n").unwrap();
        std::fs::create_dir_all(file_link.path().join(".shikigami")).unwrap();
        std::os::unix::fs::symlink(&secret, file_link.path().join(PLAN_JAIL_PATH)).unwrap();
        assert!(!plan_jail_destination_ok(file_link.path()));

        let parent_link = tempfile::tempdir().unwrap();
        let other = parent_link.path().join("other");
        std::fs::create_dir_all(&other).unwrap();
        std::os::unix::fs::symlink(&other, parent_link.path().join(".shikigami")).unwrap();
        assert!(!plan_jail_destination_ok(parent_link.path()));

        assert!(read_plan_jail_file(regular.path()).as_deref() == Some(b"plan\n".as_slice()));
        assert!(read_plan_jail_file(file_link.path()).is_none());
        assert!(read_plan_jail_file(parent_link.path()).is_none());

        let hard = tempfile::tempdir().unwrap();
        let secret = hard.path().join("secret.txt");
        std::fs::write(&secret, "keep\n").unwrap();
        std::fs::create_dir_all(hard.path().join(".shikigami")).unwrap();
        std::fs::hard_link(&secret, hard.path().join(PLAN_JAIL_PATH)).unwrap();
        assert!(!plan_jail_destination_ok(hard.path()));
        assert!(read_plan_jail_file(hard.path()).is_none());

        let fifo = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(fifo.path().join(".shikigami")).unwrap();
        let fifo_path = fifo.path().join(PLAN_JAIL_PATH);
        let c_path = std::ffi::CString::new(fifo_path.to_str().unwrap()).unwrap();
        let made = unsafe { libc::mkfifo(c_path.as_ptr(), 0o644) };
        assert_eq!(made, 0);
        assert!(!plan_jail_destination_ok(fifo.path()));
    }

    #[test]
    fn bash_enables_background_helpers() {
        let enabled = vec!["bash".into()];
        assert!(builtin_is_authorized(&enabled, "bash"));
        for helper in BASH_HELPER_TOOLS {
            assert!(builtin_is_authorized(&enabled, helper), "{helper}");
        }
        assert!(!builtin_is_authorized(&enabled, "web_fetch"));
        let names: Vec<_> = model_visible_builtin_definitions(&enabled)
            .into_iter()
            .map(|definition| definition.name)
            .collect();
        assert_eq!(
            names,
            vec![
                "bash",
                "bash_background",
                "bash_job_status",
                "bash_job_logs",
            ]
        );
    }

    #[test]
    fn helpers_are_not_authorized_without_bash() {
        let enabled = vec!["read_file".into(), "bash_background".into()];
        assert!(builtin_is_authorized(&enabled, "read_file"));
        assert!(!builtin_is_authorized(&enabled, "bash_background"));
        let names: Vec<_> = model_visible_builtin_definitions(&enabled)
            .into_iter()
            .map(|definition| definition.name)
            .collect();
        assert_eq!(names, vec!["read_file"]);
    }

    #[test]
    fn replay_authority_allows_only_observation_and_terminal_tools() {
        for definition in builtin_catalog() {
            let expected = match definition.name.as_str() {
                "read_file" | "glob" | "grep" => ReplayToolAuthority::Observation,
                "report" => ReplayToolAuthority::Terminal,
                _ => ReplayToolAuthority::Denied,
            };
            assert_eq!(
                replay_tool_authority(&definition.name),
                expected,
                "{}",
                definition.name
            );
        }
        assert_eq!(
            replay_tool_authority("external_or_unknown"),
            ReplayToolAuthority::Denied
        );
    }
}
