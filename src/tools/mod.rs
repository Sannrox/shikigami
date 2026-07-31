//! Workspace-jailed tools for the agent loop.

mod bash;
mod catalog;
mod environment;
mod executor;
mod fs;
mod path;
mod registry;
mod todo;
mod web_fetch;

pub use catalog::{
    ToolDef, builtin_catalog, definitions_for_enabled, is_parallel_safe_tool,
    model_visible_builtin_definitions, must_be_exclusive_batch,
};
pub use executor::ToolExecutor;
pub use path::{is_unsafe_relative_path, path_is_ignored};
pub use registry::{ExternalTool, ToolRegistry};
pub use todo::{MAX_TODO_CONTENT_CHARS, MAX_TODO_ID_CHARS, MAX_TODO_ITEMS, TodoItem, TodoStatus};
pub use web_fetch::{MockWebFetcher, WebFetchResponse, WebFetcher, validate_web_fetch_url};

#[cfg(feature = "model-http")]
pub use web_fetch::ReqwestWebFetcher;

use std::path::PathBuf;
use std::time::Duration;

use serde::Deserialize;
use thiserror::Error;

use fs::MAX_FILE_BYTES;

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
    #[error("apply_patch: {0}")]
    ApplyPatch(String),
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

pub(crate) fn parse<T: for<'de> Deserialize<'de>>(tool: &str, raw: &str) -> Result<T, ToolError> {
    serde_json::from_str(raw).map_err(|source| ToolError::InvalidArguments {
        tool: tool.into(),
        source,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::path::glob_match;
    use super::*;
    use crate::config::NetworkSettings;
    use tempfile::tempdir;

    #[tokio::test]
    async fn background_bash_job_poll_and_logs() {
        let dir = tempdir().unwrap();
        let reg = ToolRegistry::with_builtins(
            dir.path(),
            vec!["bash".into()],
            30,
            NetworkSettings::default(),
        )
        .unwrap();
        let start = reg
            .execute(
                "bash_background",
                r#"{"command":"printf 'hello-bg\\n'; sleep 0.2; printf 'done\\n'"}"#,
            )
            .await
            .unwrap();
        let ToolOutput::Text(start_text) = start else {
            panic!("text");
        };
        let job_id = start_text
            .lines()
            .find_map(|l| l.strip_prefix("job_id="))
            .expect("job_id line")
            .to_string();
        // Poll until exited
        let mut status = String::new();
        for _ in 0..50 {
            let s = reg
                .execute("bash_job_status", &format!(r#"{{"job_id":"{job_id}"}}"#))
                .await
                .unwrap();
            let ToolOutput::Text(t) = s else { panic!() };
            status = t;
            if status.contains("exited") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(status.contains("exited"), "{status}");
        let logs = reg
            .execute("bash_job_logs", &format!(r#"{{"job_id":"{job_id}"}}"#))
            .await
            .unwrap();
        let ToolOutput::Text(log_text) = logs else {
            panic!()
        };
        assert!(log_text.contains("hello-bg"), "{log_text}");
        reg.kill_background_jobs().await;
    }

    #[tokio::test]
    async fn bash_environment_protects_credentials_foreground_and_background() {
        let dir = tempdir().unwrap();
        let safe_name = "SHIKIGAMI_TEST_SAFE_BUILD_FLAG_153";
        let token_name = "SHIKIGAMI_TEST_SYNTHETIC_PLANE_TOKEN_153";
        // SAFETY: unique test-only names are removed immediately after the
        // registry snapshots the parent environment.
        unsafe {
            std::env::set_var(safe_name, "visible");
            std::env::set_var(token_name, "must-not-reach-tools");
        }
        let protected = vec![token_name.into()];
        let reg = ToolRegistry::with_builtins_protected_environment(
            dir.path(),
            vec!["bash".into()],
            30,
            NetworkSettings::default(),
            true,
            &protected,
        )
        .unwrap();
        // SAFETY: cleanup of the unique test-only names above.
        unsafe {
            std::env::remove_var(safe_name);
            std::env::remove_var(token_name);
        }

        let foreground = reg
            .execute(
                "bash",
                r#"{"command":"printf '%s|%s|%s' \"${SHIKIGAMI_TEST_SAFE_BUILD_FLAG_153-unset}\" \"${SHIKIGAMI_TEST_SYNTHETIC_PLANE_TOKEN_153-unset}\" \"${BASH_ENV-unset}\""}"#,
            )
            .await
            .unwrap();
        assert_eq!(foreground, ToolOutput::Text("visible|unset|unset".into()));

        let start = reg
            .execute(
                "bash_background",
                r#"{"command":"printf '%s|%s|%s' \"${SHIKIGAMI_TEST_SAFE_BUILD_FLAG_153-unset}\" \"${SHIKIGAMI_TEST_SYNTHETIC_PLANE_TOKEN_153-unset}\" \"${BASH_ENV-unset}\""}"#,
            )
            .await
            .unwrap();
        let ToolOutput::Text(start_text) = start else {
            panic!("expected background job metadata");
        };
        let job_id = start_text
            .lines()
            .find_map(|line| line.strip_prefix("job_id="))
            .expect("job id")
            .to_string();
        for _ in 0..50 {
            let status = reg
                .execute("bash_job_status", &format!(r#"{{"job_id":"{job_id}"}}"#))
                .await
                .unwrap();
            if matches!(status, ToolOutput::Text(ref text) if text.contains("exited")) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let logs = reg
            .execute("bash_job_logs", &format!(r#"{{"job_id":"{job_id}"}}"#))
            .await
            .unwrap();
        assert_eq!(logs, ToolOutput::Text("visible|unset|unset".into()));
        reg.kill_background_jobs().await;
    }

    #[tokio::test]
    async fn todo_write_caps_and_round_trip() {
        let dir = tempdir().unwrap();
        let reg = ToolRegistry::with_builtins(
            dir.path(),
            vec!["todo_write".into()],
            30,
            NetworkSettings::default(),
        )
        .unwrap();
        let out = reg
            .execute(
                "todo_write",
                r#"{"items":[{"id":"1","content":"first","status":"pending"},{"id":"2","content":"second","status":"in_progress"}]}"#,
            )
            .await
            .unwrap();
        match out {
            ToolOutput::Text(t) => {
                assert!(t.contains("2 item"), "{t}");
                assert!(t.contains("first"), "{t}");
            }
            _ => panic!("expected text"),
        }
        assert_eq!(reg.todos().len(), 2);
        assert_eq!(reg.todos()[1].status, TodoStatus::InProgress);

        let err = reg
            .execute(
                "todo_write",
                r#"{"items":[{"id":"a","content":"x","status":"pending"},{"id":"a","content":"y","status":"pending"}]}"#,
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("duplicate"), "{err}");

        let too_many: Vec<String> = (0..MAX_TODO_ITEMS + 1)
            .map(|i| format!(r#"{{"id":"{i}","content":"c","status":"pending"}}"#))
            .collect();
        let payload = format!(r#"{{"items":[{}]}}"#, too_many.join(","));
        let err = reg.execute("todo_write", &payload).await.unwrap_err();
        assert!(err.to_string().contains("at most"), "{err}");
    }

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
            NetworkSettings::default(),
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
        let reg = ToolRegistry::with_builtins(
            dir.path(),
            vec!["not_registered".into()],
            30,
            NetworkSettings::default(),
        )
        .unwrap();
        let err = reg.execute("not_registered", "{}").await.unwrap_err();
        assert!(matches!(err, ToolError::UnknownTool(_)));
    }

    #[tokio::test]
    async fn web_fetch_respects_egress_and_ssrf_blocks() {
        use crate::config::EgressMode;

        let dir = tempdir().unwrap();
        let net = NetworkSettings {
            egress: EgressMode::Deny,
            ..Default::default()
        };
        let mut reg =
            ToolRegistry::with_builtins(dir.path(), vec!["web_fetch".into()], 30, net).unwrap();
        reg.set_web_fetcher(Arc::new(MockWebFetcher {
            status: 200,
            body: "should-not-run".into(),
        }));
        let err = reg
            .execute("web_fetch", r#"{"url":"https://example.com/doc"}"#)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("denied"), "{err}");

        let allow = NetworkSettings {
            egress: EgressMode::Allowlist,
            allow_hosts: vec!["example.com".into()],
        };
        let mut reg2 =
            ToolRegistry::with_builtins(dir.path(), vec!["web_fetch".into()], 30, allow).unwrap();
        reg2.set_web_fetcher(Arc::new(MockWebFetcher {
            status: 200,
            body: "hello docs".into(),
        }));
        let out = reg2
            .execute("web_fetch", r#"{"url":"https://example.com/doc"}"#)
            .await
            .unwrap();
        match out {
            ToolOutput::Text(t) => {
                assert!(t.contains("status=200"), "{t}");
                assert!(t.contains("hello docs"), "{t}");
                assert!(t.contains("final_url=https://example.com/doc"), "{t}");
            }
            _ => panic!("expected text"),
        }
        let err = reg2
            .execute("web_fetch", r#"{"url":"https://evil.example/x"}"#)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("allow_hosts"), "{err}");

        // SSRF baseline even when unrestricted
        let open = NetworkSettings {
            egress: EgressMode::Unrestricted,
            ..Default::default()
        };
        let reg3 =
            ToolRegistry::with_builtins(dir.path(), vec!["web_fetch".into()], 30, open).unwrap();
        for bad in [
            r#"{"url":"http://127.0.0.1/"}"#,
            r#"{"url":"http://localhost/"}"#,
            r#"{"url":"http://10.0.0.1/"}"#,
            r#"{"url":"file:///etc/passwd"}"#,
        ] {
            let err = reg3.execute("web_fetch", bad).await.unwrap_err();
            assert!(
                err.to_string().contains("blocked")
                    || err.to_string().contains("only http")
                    || err.to_string().contains("web_fetch"),
                "url={bad} err={err}"
            );
        }
    }

    struct RedirectFetcher {
        calls: Arc<AtomicUsize>,
        destination: String,
    }

    #[async_trait::async_trait]
    impl WebFetcher for RedirectFetcher {
        async fn get(&self, url: &str) -> Result<WebFetchResponse, ToolError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(WebFetchResponse {
                status: 302,
                final_url: self.destination.clone(),
                body: format!("redirected from {url}"),
            })
        }
    }

    #[tokio::test]
    async fn web_fetch_rejects_redirect_destination_before_second_request() {
        let dir = tempdir().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = ToolRegistry::with_builtins(
            dir.path(),
            vec!["web_fetch".into()],
            30,
            NetworkSettings {
                egress: crate::config::EgressMode::Unrestricted,
                ..Default::default()
            },
        )
        .unwrap();
        registry.set_web_fetcher(Arc::new(RedirectFetcher {
            calls: Arc::clone(&calls),
            destination: "http://127.0.0.1/admin".into(),
        }));

        let err = registry
            .execute("web_fetch", r#"{"url":"https://example.com/start"}"#)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("blocked"), "{err}");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn web_fetch_not_in_default_coding_tools() {
        use crate::config::ToolsSettings;
        assert!(!ToolsSettings::default_coding_tools().contains(&"web_fetch".into()));
    }

    #[tokio::test]
    async fn glob_and_grep_honor_ignore_patterns() {
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("src")).unwrap();
        std::fs::create_dir_all(dir.path().join("node_modules/pkg")).unwrap();
        std::fs::write(dir.path().join("src/keep.rs"), "fn keep() {}").unwrap();
        std::fs::write(dir.path().join("node_modules/pkg/x.js"), "secret").unwrap();
        std::fs::write(dir.path().join(".shikigamiignore"), "secret.txt\n").unwrap();
        std::fs::write(dir.path().join("secret.txt"), "nope").unwrap();
        std::fs::write(dir.path().join("visible.txt"), "ok").unwrap();

        let tools = ToolExecutor::new_with_ignore(
            dir.path(),
            vec!["glob".into(), "grep".into(), "read_file".into()],
            30,
            true,
        )
        .unwrap();
        let glob_out = tools
            .execute("glob", r#"{"pattern":"**/*"}"#)
            .await
            .unwrap();
        let ToolOutput::Text(g) = glob_out else {
            panic!("text");
        };
        assert!(g.contains("keep.rs"), "{g}");
        assert!(g.contains("visible.txt"), "{g}");
        assert!(!g.contains("node_modules"), "{g}");
        assert!(!g.contains("secret.txt"), "{g}");

        // Explicit read_file still works for ignored paths.
        let read = tools
            .execute("read_file", r#"{"path":"secret.txt"}"#)
            .await
            .unwrap();
        assert_eq!(read, ToolOutput::Text("nope".into()));

        let tools_off =
            ToolExecutor::new_with_ignore(dir.path(), vec!["glob".into()], 30, false).unwrap();
        let glob_all = tools_off
            .execute("glob", r#"{"pattern":"**/*"}"#)
            .await
            .unwrap();
        let ToolOutput::Text(g2) = glob_all else {
            panic!("text");
        };
        assert!(g2.contains("node_modules") || g2.contains("x.js"), "{g2}");
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
    async fn apply_patch_context_hunks_atomic() {
        let dir = tempdir().unwrap();
        std::fs::write(
            dir.path().join("a.txt"),
            "header\nkeep\nold1\nmid\nold2\nfooter\n",
        )
        .unwrap();
        std::fs::write(dir.path().join("b.txt"), "x\ny\nz\n").unwrap();
        let tools = ToolExecutor::new(dir.path(), vec!["apply_patch".into()], 30).unwrap();
        let out = tools
            .execute(
                "apply_patch",
                r#"{
                  "patches": [
                    {
                      "path": "a.txt",
                      "hunks": [
                        {"context_before":"keep\n","old":"old1\n","new":"new1\n","context_after":"mid\n"},
                        {"context_before":"mid\n","old":"old2\n","new":"new2\n","context_after":"footer\n"}
                      ]
                    },
                    {
                      "path": "b.txt",
                      "hunks": [
                        {"old":"y\n","new":"Y\n"}
                      ]
                    }
                  ]
                }"#,
            )
            .await
            .unwrap();
        assert!(matches!(out, ToolOutput::Text(t) if t.contains("3 hunk")));
        assert_eq!(
            std::fs::read_to_string(dir.path().join("a.txt")).unwrap(),
            "header\nkeep\nnew1\nmid\nnew2\nfooter\n"
        );
        assert_eq!(
            std::fs::read_to_string(dir.path().join("b.txt")).unwrap(),
            "x\nY\nz\n"
        );

        // Ambiguous old without unique context fails closed (no partial write).
        std::fs::write(dir.path().join("c.txt"), "a\nfoo\nb\nfoo\nc\n").unwrap();
        let err = tools
            .execute(
                "apply_patch",
                r#"{"patches":[{"path":"c.txt","hunks":[{"old":"foo\n","new":"bar\n"}]}]}"#,
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("exactly one match"), "{err}");
        assert_eq!(
            std::fs::read_to_string(dir.path().join("c.txt")).unwrap(),
            "a\nfoo\nb\nfoo\nc\n"
        );
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
