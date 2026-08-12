//! Builtin tool catalog and parallel/exclusive batch helpers.

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
/// future MCP/skill tools register into [`crate::tools::ToolRegistry`] without changing the turn loop.
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
    ]
}

/// Whether this tool must be the only call in a model batch.
pub fn must_be_exclusive_batch(name: &str) -> bool {
    matches!(name, "report" | "escalate")
}

/// Tools safe to run concurrently with each other (reads; no workspace mutation).
///
/// Write tools, bash, todo_write, report/escalate stay serial for the whole batch.
pub fn is_parallel_safe_tool(name: &str) -> bool {
    matches!(name, "read_file" | "glob" | "grep" | "web_fetch")
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
}
