use shikigami::{Config, Harness, RunRequest, StateRoot};
use std::path::Path;
use tempfile::tempdir;

#[tokio::test]
async fn local_scripted_end_to_end() {
    let dir = tempdir().unwrap();
    let state = StateRoot::new(dir.path().join("state"));
    let mut config = Config::default();
    config.governance.adapter = "local".into();
    config.model.adapter = "scripted".into();
    config.events.adapter = "none".into();
    config.workspace.root = dir.path().join("ws-root").to_string_lossy().into();

    let harness = Harness::from_config(config, state).unwrap();
    let mut request = RunRequest::new("demo");
    request.keep_workspace = true;
    request.resume_run_id = None;
    let result = harness.run(request).await.unwrap();
    assert!(result.success);
    assert!(result.turns >= 2);
    assert_eq!(result.termination, shikigami::RunTermination::Completed);
    let marker = result.workspace.join("SHIKIGAMI_OK.txt");
    assert!(marker.is_file(), "expected {}", marker.display());
}

#[tokio::test]
async fn custom_script_edit_flow() {
    let dir = tempdir().unwrap();
    let state = StateRoot::new(dir.path().join("state"));
    let mut config = Config::default();
    config.governance.adapter = "none".into();
    config.model.adapter = "scripted".into();
    config.model.script_json = Some(
        r#"[
        {"tool_calls":[{"name":"write_file","args_json":"{\"path\":\"x.txt\",\"content\":\"one\"}"}]},
        {"tool_calls":[{"name":"edit","args_json":"{\"path\":\"x.txt\",\"old\":\"one\",\"new\":\"two\"}"}]},
        {"tool_calls":[{"name":"report","args_json":"{\"summary\":\"edited\",\"success\":true}"}]}
    ]"#
        .into(),
    );
    config.events.adapter = "jsonl".into();
    config.workspace.root = dir.path().join("ws").to_string_lossy().into();

    let harness = Harness::from_config(config, state).unwrap();
    let mut request = RunRequest::new("edit flow");
    request.keep_workspace = true;
    let result = harness.run(request).await.unwrap();
    assert!(result.success);
    let text = std::fs::read_to_string(result.workspace.join("x.txt")).unwrap();
    assert_eq!(text, "two");
}

#[tokio::test]
async fn live_event_stream_receives_scripted_sequence() {
    use shikigami::{ChannelSink, HarnessEvent};
    use std::sync::Arc;

    let dir = tempdir().unwrap();
    let state = StateRoot::new(dir.path().join("state"));
    let mut config = Config::default();
    config.governance.adapter = "local".into();
    config.model.adapter = "scripted".into();
    config.events.adapter = "none".into();
    config.workspace.root = dir.path().join("ws").to_string_lossy().into();

    let harness = Harness::from_config(config, state).unwrap();
    let (sink, rx) = ChannelSink::pair();
    let mut request = RunRequest::new("demo");
    request.keep_workspace = true;
    let result = harness
        .run_with_events(request, Some(Arc::new(sink)))
        .await
        .unwrap();
    assert!(result.success);

    let mut events = Vec::new();
    while let Ok(ev) = rx.try_recv() {
        events.push(ev);
    }
    assert!(
        events
            .iter()
            .any(|e| matches!(e, HarnessEvent::Prompt { .. })),
        "missing Prompt: {events:?}"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, HarnessEvent::ToolStart { name, .. } if name == "write_file")),
        "missing write_file start: {events:?}"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, HarnessEvent::RunFinished { success: true, .. })),
        "missing RunFinished: {events:?}"
    );
}

#[tokio::test]
async fn repeated_tools_in_one_turn_have_distinct_call_ids() {
    use shikigami::{ChannelSink, HarnessEvent};
    use std::sync::Arc;

    let dir = tempdir().unwrap();
    let state = StateRoot::new(dir.path().join("state"));
    let mut config = Config::default();
    config.governance.adapter = "local".into();
    config.model.adapter = "scripted".into();
    config.model.script_json = Some(
        r#"[
        {"tool_calls":[
            {"name":"write_file","args_json":"{\"path\":\"a.txt\",\"content\":\"a\"}"},
            {"name":"write_file","args_json":"{\"path\":\"b.txt\",\"content\":\"b\"}"}
        ]},
        {"tool_calls":[{"name":"report","args_json":"{\"summary\":\"two writes\",\"success\":true}"}]}
    ]"#
        .into(),
    );
    config.events.adapter = "none".into();
    config.workspace.root = dir.path().join("ws").to_string_lossy().into();

    let harness = Harness::from_config(config, state).unwrap();
    let (sink, rx) = ChannelSink::pair();
    let mut request = RunRequest::new("two writes");
    request.keep_workspace = true;
    let result = harness
        .run_with_events(request, Some(Arc::new(sink)))
        .await
        .unwrap();
    assert!(result.success);

    let starts: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok())
        .filter_map(|event| match event {
            HarnessEvent::ToolStart {
                name,
                call_id,
                turn,
                run_id,
                ..
            } if name == "write_file" => Some((run_id, turn, call_id)),
            _ => None,
        })
        .collect();
    assert_eq!(starts.len(), 2, "{starts:?}");
    assert_eq!(starts[0].0, result.run_id);
    assert_eq!(starts[0].1, starts[1].1);
    assert_ne!(starts[0].2, starts[1].2);
    assert!(starts[0].2.starts_with("tool-"), "{}", starts[0].2);
    assert!(starts[1].2.starts_with("tool-"), "{}", starts[1].2);
}

#[tokio::test]
async fn live_event_stream_receives_handoff_brief() {
    use shikigami::{ChannelSink, HarnessEvent};
    use std::sync::Arc;

    let dir = tempdir().unwrap();
    let state = StateRoot::new(dir.path().join("state"));
    let mut config = Config::default();
    config.governance.adapter = "local".into();
    config.model.adapter = "scripted".into();
    config.model.script_json = Some(
        r#"[
        {"tool_calls":[{"name":"handoff","args_json":"{\"task\":\"resume auth\",\"decisions\":[\"keep JWT\"],\"files\":[\"src/auth.rs\"],\"ignore\":[\"vendor\"]}"}]},
        {"tool_calls":[{"name":"report","args_json":"{\"summary\":\"briefed\",\"success\":true}"}]}
    ]"#
        .into(),
    );
    config.events.adapter = "none".into();
    config.workspace.root = dir.path().join("ws").to_string_lossy().into();
    assert!(!config.run.nested);

    let harness = Harness::from_config(config, state).unwrap();
    let (sink, rx) = ChannelSink::pair();
    let mut request = RunRequest::new("write a brief");
    request.keep_workspace = true;
    let result = harness
        .run_with_events(request, Some(Arc::new(sink)))
        .await
        .unwrap();
    assert!(result.success);
    assert_eq!(result.termination, shikigami::RunTermination::Completed);

    let events: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
    let brief = events.iter().find_map(|event| match event {
        HarnessEvent::HandoffBrief {
            task,
            decisions,
            files,
            ignore,
        } => Some((
            task.clone(),
            decisions.clone(),
            files.clone(),
            ignore.clone(),
        )),
        _ => None,
    });
    assert_eq!(
        brief,
        Some((
            "resume auth".into(),
            vec!["keep JWT".into()],
            vec!["src/auth.rs".into()],
            vec!["vendor".into()]
        )),
        "{events:?}"
    );
    assert!(
        !events.iter().any(|event| matches!(
            event,
            HarnessEvent::ToolStart { name, .. } if name == "child_run"
        )),
        "handoff must not start a child run: {events:?}"
    );
}

#[tokio::test]
async fn live_event_stream_keeps_each_handoff_brief_in_a_batch() {
    use shikigami::{ChannelSink, HarnessEvent};
    use std::sync::Arc;

    let dir = tempdir().unwrap();
    let state = StateRoot::new(dir.path().join("state"));
    let mut config = Config::default();
    config.governance.adapter = "local".into();
    config.model.adapter = "scripted".into();
    config.model.script_json = Some(
        r#"[
        {"tool_calls":[
            {"name":"handoff","args_json":"{\"task\":\"first\"}"},
            {"name":"handoff","args_json":"{\"task\":\"second\"}"}
        ]},
        {"tool_calls":[{"name":"report","args_json":"{\"summary\":\"briefed\",\"success\":true}"}]}
    ]"#
        .into(),
    );
    config.events.adapter = "none".into();
    config.workspace.root = dir.path().join("ws").to_string_lossy().into();

    let harness = Harness::from_config(config, state).unwrap();
    let (sink, rx) = ChannelSink::pair();
    let mut request = RunRequest::new("two briefs");
    request.keep_workspace = true;
    let result = harness
        .run_with_events(request, Some(Arc::new(sink)))
        .await
        .unwrap();
    assert!(result.success);

    let tasks: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok())
        .filter_map(|event| match event {
            HarnessEvent::HandoffBrief { task, .. } => Some(task),
            _ => None,
        })
        .collect();
    assert_eq!(tasks, vec!["first".to_string(), "second".to_string()]);
}

#[tokio::test]
async fn bash_tool_events_cannot_emit_configured_harness_credentials() {
    use shikigami::{ChannelSink, HarnessEvent};
    use std::sync::Arc;

    let token_name = "SHIKIGAMI_TEST_EVENT_PLANE_TOKEN_153";
    let token_value = "must-not-reach-events";
    // SAFETY: unique integration-test name, removed immediately after the run.
    unsafe {
        std::env::set_var(token_name, token_value);
    }
    let dir = tempdir().unwrap();
    let state = StateRoot::new(dir.path().join("state"));
    let mut config = Config::default();
    config.governance.adapter = "local".into();
    config.governance.token_env = Some(token_name.into());
    config.model.adapter = "scripted".into();
    config.model.script_json = Some(
        r#"[
        {"tool_calls":[{"name":"bash","args_json":"{\"command\":\"printf '%s' \\\"${SHIKIGAMI_TEST_EVENT_PLANE_TOKEN_153-unset}\\\"\"}"}]},
        {"tool_calls":[{"name":"report","args_json":"{\"summary\":\"credential isolated\",\"success\":true}"}]}
    ]"#
        .into(),
    );
    config.tools.enabled = vec!["bash".into(), "report".into()];
    config.events.adapter = "none".into();
    config.workspace.root = dir.path().join("ws").to_string_lossy().into();

    let harness = Harness::from_config(config, state).unwrap();
    let (sink, rx) = ChannelSink::pair();
    let mut request = RunRequest::new("prove credential isolation");
    request.keep_workspace = true;
    let result = harness.run_with_events(request, Some(Arc::new(sink))).await;
    // SAFETY: cleanup of the unique integration-test name above.
    unsafe {
        std::env::remove_var(token_name);
    }
    let result = result.unwrap();
    assert!(result.success);

    let events: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
    assert!(
        events.iter().any(|event| matches!(
            event,
            HarnessEvent::ToolEnd {
                name,
                ok: true,
                detail,
                ..
            } if name == "bash" && detail == "unset"
        )),
        "missing isolated Bash ToolEnd: {events:?}"
    );
    assert!(
        !format!("{events:?}").contains(token_value),
        "synthetic credential appeared in event stream"
    );
}

/// Denied authorize_tool must not execute the host tool (allow-list path).
/// Governed external-action deny uses the same run-loop branch.
#[tokio::test]
async fn denied_tool_authorization_does_not_execute() {
    let dir = tempdir().unwrap();
    let state = StateRoot::new(dir.path().join("state"));
    let mut config = Config::default();
    config.governance.adapter = "local".into();
    config.model.adapter = "scripted".into();
    // Only report is enabled — write_file must be denied and not create the file.
    config.tools.enabled = vec!["report".into()];
    config.events.adapter = "none".into();
    config.workspace.root = dir.path().join("ws").to_string_lossy().into();
    config.model.script_json = Some(
        r#"[
        {"tool_calls":[{"name":"write_file","args_json":"{\"path\":\"FORBIDDEN.txt\",\"content\":\"nope\"}"}]},
        {"tool_calls":[{"name":"report","args_json":"{\"summary\":\"done without write\",\"success\":true}"}]}
    ]"#
        .into(),
    );

    let harness = Harness::from_config(config, state).unwrap();
    let mut request = RunRequest::new("deny write");
    request.keep_workspace = true;
    let result = harness.run(request).await.unwrap();
    assert!(result.success);
    assert!(
        !result.workspace.join("FORBIDDEN.txt").exists(),
        "denied write_file must not create the file"
    );
}

#[tokio::test]
async fn edit_outcomes_are_redacted_and_attributed_in_the_run_journal() {
    for name in ["edit", "multi_edit", "apply_patch"] {
        for outcome in [
            "applied",
            "applied_normalized",
            "no_match",
            "ambiguous",
            "overlap",
            "invalid_input",
            "limit",
            "io",
        ]
        .into_iter()
        .filter(|outcome| {
            (name != "apply_patch" || *outcome != "applied_normalized")
                && (name != "edit" || *outcome != "overlap")
        }) {
            let dir = tempdir().unwrap();
            let workspace = dir.path().join("workspace");
            std::fs::create_dir(&workspace).unwrap();
            let path = workspace.join("secret-path.txt");
            match outcome {
                "applied" | "invalid_input" | "overlap" => {
                    std::fs::write(&path, "secret-old\n").unwrap()
                }
                "applied_normalized" => std::fs::write(&path, "secret–old\n").unwrap(),
                "no_match" => std::fs::write(&path, "unmatched\n").unwrap(),
                "ambiguous" => std::fs::write(&path, "secret-old\nsecret-old\n").unwrap(),
                "limit" => std::fs::File::create(&path)
                    .unwrap()
                    .set_len(2 * 1024 * 1024 + 1)
                    .unwrap(),
                _ => {}
            }
            let hunk = serde_json::json!({"old":"secret-old\n", "new":"secret-new\n"});
            let args = if outcome == "invalid_input" {
                "not-json-secret".to_owned()
            } else if outcome == "overlap" {
                let hunks = serde_json::json!([hunk.clone(), hunk]);
                if name == "multi_edit" {
                    serde_json::json!({"path":"secret-path.txt", "edits":hunks}).to_string()
                } else {
                    serde_json::json!({"patches":[{"path":"secret-path.txt", "hunks":hunks}]})
                        .to_string()
                }
            } else {
                match name {
                    "edit" => serde_json::json!({"path":"secret-path.txt", "old":"secret-old\n", "new":"secret-new\n"}),
                    "multi_edit" => serde_json::json!({"path":"secret-path.txt", "edits":[hunk]}),
                    _ => serde_json::json!({"patches":[{"path":"secret-path.txt", "hunks":[hunk]}]}),
                }.to_string()
            };
            let mut config = Config::default();
            config.governance.adapter = "local".into();
            config.model.adapter = "scripted".into();
            config.model.model = "edit-test-model".into();
            config.events.adapter = "none".into();
            config.workspace.adapter = "inplace".into();
            config.workspace.root = workspace.to_string_lossy().into_owned();
            config.model.script_json = Some(serde_json::json!([
                {"tool_calls":[{"name":name, "args_json":args}]},
                {"tool_calls":[{"name":"report", "args_json":"{\"summary\":\"done\",\"success\":true}"}]}
            ]).to_string());
            let harness =
                Harness::from_config(config, StateRoot::new(dir.path().join("state"))).unwrap();
            let result = harness
                .run(RunRequest::new("test edit outcomes"))
                .await
                .unwrap();
            let journal = harness.registry.event_log(&result.run_id).unwrap();
            assert!(!journal.contains("secret-"), "{journal}");
            let records: Vec<serde_json::Value> = journal
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect();
            let edits: Vec<_> = records
                .iter()
                .filter(|record| record["event"] == "edit_outcome")
                .collect();
            assert_eq!(edits.len(), 1, "{name} {outcome}: {journal}");
            assert_eq!(edits[0]["edit_outcome"]["tool"], name);
            assert_eq!(edits[0]["edit_outcome"]["model"], "edit-test-model");
            assert_eq!(edits[0]["edit_outcome"]["outcome"], outcome);
            if let Some(count) = match outcome {
                "no_match" => Some(0),
                "ambiguous" => Some(2),
                _ => None,
            } {
                assert_eq!(edits[0]["edit_outcome"]["match_count"], count);
            }
        }
    }
}

#[tokio::test]
async fn load_skill_attributes_each_pack_once_at_load() {
    use shikigami::{ChannelSink, HarnessEvent};
    use std::sync::Arc;

    let dir = tempdir().unwrap();
    let ws = dir.path().join("ws");
    let skills_root = dir.path().join("skills");
    for id in ["listed", "idle", "unlisted"] {
        let pack = skills_root.join(id);
        std::fs::create_dir_all(&pack).unwrap();
        std::fs::write(pack.join("SKILL.md"), format!("{id} procedure\n")).unwrap();
    }
    let state = StateRoot::new(dir.path().join("state"));
    let mut config = Config::default();
    config.governance.adapter = "local".into();
    config.model.adapter = "scripted".into();
    config.model.script_json = Some(
        r#"[
        {"tool_calls":[{"name":"load_skill","args_json":"{\"id\":\"listed\"}"}]},
        {"tool_calls":[{"name":"load_skill","args_json":"{\"id\":\"listed\"}"}]},
        {"tool_calls":[{"name":"load_skill","args_json":"{\"id\":\"unlisted\"}"}]},
        {"tool_calls":[{"name":"report","args_json":"{\"summary\":\"done\",\"success\":true}"}]}
    ]"#
        .into(),
    );
    config.context.skills_root = Some(skills_root.to_string_lossy().into());
    config.context.skills = vec!["listed".into(), "idle".into()];
    config.events.adapter = "none".into();
    config.workspace.root = ws.to_string_lossy().into();

    let harness = Harness::from_config(config, state).unwrap();
    let (sink, rx) = ChannelSink::pair();
    let result = harness
        .run_with_events(RunRequest::new("use a skill"), Some(Arc::new(sink)))
        .await
        .unwrap();
    assert!(result.success);

    let events: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
    let digests: Vec<_> = events
        .iter()
        .enumerate()
        .filter_map(|(at, event)| match event {
            HarnessEvent::Message { text, .. } if text.starts_with("skill ") => Some((at, text)),
            _ => None,
        })
        .collect();
    assert_eq!(digests.len(), 1, "{events:?}");
    assert!(digests[0].1.starts_with("skill listed digest="));
    let first_load = events
        .iter()
        .position(
            |event| matches!(event, HarnessEvent::ToolStart { name, .. } if name == "load_skill"),
        )
        .unwrap();
    assert!(digests[0].0 > first_load, "digest logs at load: {events:?}");
    let load_results: Vec<bool> = events
        .iter()
        .filter_map(|event| match event {
            HarnessEvent::ToolEnd { name, ok, .. } if name == "load_skill" => Some(*ok),
            _ => None,
        })
        .collect();
    assert_eq!(load_results, [true, true, false], "{events:?}");
}

#[tokio::test]
async fn listed_project_rules_all_attach_on_a_scripted_run() {
    use shikigami::{ChannelSink, HarnessEvent};
    use std::sync::Arc;

    let dir = tempdir().unwrap();
    let ws = dir.path().join("ws");
    std::fs::create_dir_all(&ws).unwrap();
    std::fs::write(ws.join("AGENTS.md"), "agents house rules\n").unwrap();
    std::fs::write(ws.join("shikigami.rules.md"), "harness house rules\n").unwrap();
    std::fs::create_dir_all(ws.join("nested")).unwrap();
    std::fs::write(ws.join("nested/secret.md"), "must not attach\n").unwrap();

    let state = StateRoot::new(dir.path().join("state"));
    let mut config = Config::default();
    config.governance.adapter = "local".into();
    config.model.adapter = "scripted".into();
    config.model.script_json = Some(
        r#"[{"tool_calls":[{"name":"report","args_json":"{\"summary\":\"done\",\"success\":true}"}]}]"#
            .into(),
    );
    config.events.adapter = "none".into();
    config.workspace.adapter = "inplace".into();
    config.workspace.root = ws.to_string_lossy().into();
    config.context.rules_filenames = vec![
        "AGENTS.md".into(),
        "nested/secret.md".into(),
        "missing.md".into(),
        "shikigami.rules.md".into(),
    ];

    let harness = Harness::from_config(config, state).unwrap();
    let (sink, rx) = ChannelSink::pair();
    let mut request = RunRequest::new("follow the house rules");
    request.keep_workspace = true;
    let result = harness
        .run_with_events(request, Some(Arc::new(sink)))
        .await
        .unwrap();
    assert!(result.success);

    let events: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
    let rules: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            HarnessEvent::Message { text, .. } if text.starts_with("project_rules ") => {
                Some(text.as_str())
            }
            _ => None,
        })
        .collect();
    assert_eq!(rules.len(), 2, "{events:?}");
    assert!(
        rules[0].starts_with("project_rules AGENTS.md digest="),
        "{rules:?}"
    );
    assert!(
        rules[1].starts_with("project_rules shikigami.rules.md digest="),
        "{rules:?}"
    );
}

#[tokio::test]
async fn git_ancestor_project_rules_attach_and_read_file_stays_jailed() {
    use shikigami::{ChannelSink, HarnessEvent};
    use std::sync::Arc;

    let dir = tempdir().unwrap();
    let repo = dir.path().join("repo");
    let pkg = repo.join("pkg");
    std::fs::create_dir_all(repo.join(".git")).unwrap();
    std::fs::create_dir_all(&pkg).unwrap();
    std::fs::write(repo.join("AGENTS.md"), "root house rules\n").unwrap();
    std::fs::write(pkg.join("AGENTS.md"), "pkg house rules\n").unwrap();

    let state = StateRoot::new(dir.path().join("state"));
    let mut config = Config::default();
    config.governance.adapter = "local".into();
    config.model.adapter = "scripted".into();
    config.model.script_json = Some(
        r#"[
        {"tool_calls":[{"name":"read_file","args_json":"{\"path\":\"../AGENTS.md\"}"}]},
        {"tool_calls":[{"name":"report","args_json":"{\"summary\":\"done\",\"success\":true}"}]}
    ]"#
        .into(),
    );
    config.events.adapter = "none".into();
    config.workspace.adapter = "inplace".into();
    config.workspace.root = pkg.to_string_lossy().into();

    let harness = Harness::from_config(config, state).unwrap();
    let (sink, rx) = ChannelSink::pair();
    let mut request = RunRequest::new("follow the house rules");
    request.keep_workspace = true;
    let result = harness
        .run_with_events(request, Some(Arc::new(sink)))
        .await
        .unwrap();
    assert!(result.success);

    let events: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
    let rules: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            HarnessEvent::Message { text, .. } if text.starts_with("project_rules ") => {
                Some(text.as_str())
            }
            _ => None,
        })
        .collect();
    assert_eq!(rules.len(), 2, "{events:?}");
    assert!(
        rules[0].starts_with("project_rules AGENTS.md digest="),
        "{rules:?}"
    );
    assert!(
        rules[1].starts_with("project_rules pkg/AGENTS.md digest="),
        "{rules:?}"
    );
    assert!(
        events.iter().any(|event| matches!(
            event,
            HarnessEvent::ToolEnd { name, ok: false, detail, .. }
                if name == "read_file"
                    && (detail.contains("escapes workspace")
                        || detail.contains("must not traverse parents"))
        )),
        "ancestor read_file must stay jailed: {events:?}"
    );
}

#[tokio::test]
async fn disabled_project_rules_attach_nothing_on_a_scripted_run() {
    use shikigami::{ChannelSink, HarnessEvent};
    use std::sync::Arc;

    let dir = tempdir().unwrap();
    let ws = dir.path().join("ws");
    std::fs::create_dir_all(&ws).unwrap();
    std::fs::write(ws.join("AGENTS.md"), "agents house rules\n").unwrap();
    std::fs::write(ws.join("shikigami.rules.md"), "harness house rules\n").unwrap();

    let state = StateRoot::new(dir.path().join("state"));
    let mut config = Config::default();
    config.governance.adapter = "local".into();
    config.model.adapter = "scripted".into();
    config.model.script_json = Some(
        r#"[{"tool_calls":[{"name":"report","args_json":"{\"summary\":\"done\",\"success\":true}"}]}]"#
            .into(),
    );
    config.events.adapter = "none".into();
    config.workspace.adapter = "inplace".into();
    config.workspace.root = ws.to_string_lossy().into();
    config.context.load_project_rules = false;

    let harness = Harness::from_config(config, state).unwrap();
    let (sink, rx) = ChannelSink::pair();
    let mut request = RunRequest::new("ignore house rules");
    request.keep_workspace = true;
    let result = harness
        .run_with_events(request, Some(Arc::new(sink)))
        .await
        .unwrap();
    assert!(result.success);

    let events: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
    assert!(
        !events.iter().any(|event| matches!(
            event,
            HarnessEvent::Message { text, .. } if text.starts_with("project_rules ")
        )),
        "{events:?}"
    );
}

/// Unattended `run` keeps reaping background jobs when it parks; only
/// session hosts keep them across a park.
#[tokio::test]
async fn unattended_park_reaps_background_jobs() {
    let dir = tempdir().unwrap();
    let state = StateRoot::new(dir.path().join("state"));
    let mut config = Config::default();
    config.governance.adapter = "local".into();
    config.tools.mode = shikigami::PermissionMode::WorkspaceExec;
    config.model.adapter = "scripted".into();
    config.model.script_json = Some(
        r#"[
        {"tool_calls":[{"name":"bash_background","args_json":"{\"command\":\"echo $$ > pid.txt; exec sleep 30\"}"}]},
        {"tool_calls":[{"name":"bash","args_json":"{\"command\":\"until [ -s pid.txt ]; do sleep 0.01; done\"}"}]},
        {"tool_calls":[{"name":"escalate","args_json":"{\"reason\":\"need input\"}"}]}
    ]"#
        .into(),
    );
    config.events.adapter = "none".into();
    config.workspace.root = dir.path().join("ws").to_string_lossy().into();

    let harness = Harness::from_config(config, state).unwrap();
    let mut request = RunRequest::new("start then park");
    request.keep_workspace = true;
    let result = harness.run(request).await.unwrap();
    assert_eq!(result.termination, shikigami::RunTermination::Parked);

    let pid = std::fs::read_to_string(result.workspace.join("pid.txt")).unwrap();
    let pid = pid.trim();
    assert!(!pid.is_empty(), "job wrote no pid");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let out = std::process::Command::new("ps")
            .args(["-o", "stat=", "-p", pid])
            .output()
            .unwrap();
        let stat = String::from_utf8_lossy(&out.stdout);
        if stat.trim().is_empty() || stat.trim().starts_with('Z') {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "job {pid} outlived the park"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

/// Run a scripted local run that compacts after four messages and return its events.
async fn compacting_run(script: serde_json::Value) -> Vec<shikigami::HarnessEvent> {
    use shikigami::ChannelSink;
    use std::sync::Arc;

    let dir = tempdir().unwrap();
    let state = StateRoot::new(dir.path().join("state"));
    let mut config = Config::default();
    config.governance.adapter = "local".into();
    config.model.adapter = "scripted".into();
    config.model.script_json = Some(script.to_string());
    config.events.adapter = "none".into();
    config.run.compact_after_messages = Some(4);
    config.run.compact_keep_tail = 2;
    config.workspace.root = dir.path().join("ws").to_string_lossy().into();

    let harness = Harness::from_config(config, state).unwrap();
    let (sink, rx) = ChannelSink::pair();
    let result = harness
        .run_with_events(RunRequest::new("write three files"), Some(Arc::new(sink)))
        .await
        .unwrap();
    assert!(result.success, "run failed: {result:?}");
    std::iter::from_fn(|| rx.try_recv().ok()).collect()
}

fn write_turn(path: &str) -> serde_json::Value {
    let args = serde_json::json!({"path": path, "content": "x".repeat(400)}).to_string();
    serde_json::json!({"tool_calls": [{"name": "write_file", "args_json": args}]})
}

fn write_turn_with_usage(path: &str, input_tokens: Option<u64>) -> serde_json::Value {
    let mut turn = write_turn(path);
    if let Some(tokens) = input_tokens {
        turn["usage"] = serde_json::json!({"input_tokens": tokens, "output_tokens": 1});
    }
    turn
}

fn compacted_history_event(events: &[shikigami::HarnessEvent]) -> bool {
    events.iter().any(|event| {
        matches!(
            event,
            shikigami::HarnessEvent::ContextCompacted { .. }
                | shikigami::HarnessEvent::ContextSummarized { .. }
        )
    })
}

/// Two writes so `compact_keep_tail = 2` can cut, then a final turn.
async fn usage_compact_run(
    input_tokens: Option<u64>,
    window: Option<u64>,
    compact_after_messages: Option<u32>,
    require_compacted: bool,
) -> (bool, Vec<shikigami::HarnessEvent>) {
    use shikigami::ChannelSink;
    use std::sync::Arc;

    let dir = tempdir().unwrap();
    let state = StateRoot::new(dir.path().join("state"));
    let mut config = Config::default();
    config.governance.adapter = "local".into();
    config.model.adapter = "scripted".into();
    let mut report = report_turn();
    if require_compacted {
        report["requires_context"] = serde_json::json!(["[context compacted"]);
    }
    config.model.script_json = Some(
        serde_json::json!([
            write_turn_with_usage("a.txt", input_tokens),
            write_turn_with_usage("b.txt", input_tokens),
            report,
        ])
        .to_string(),
    );
    config.events.adapter = "none".into();
    config.run.context_window_tokens = window;
    config.run.compact_at_usage_percent = 80;
    config.run.compact_after_messages = compact_after_messages;
    config.run.compact_keep_tail = 2;
    config.workspace.root = dir.path().join("ws").to_string_lossy().into();

    let harness = Harness::from_config(config, state).unwrap();
    let (sink, rx) = ChannelSink::pair();
    let result = harness
        .run_with_events(RunRequest::new("write two files"), Some(Arc::new(sink)))
        .await
        .unwrap();
    (
        result.success,
        std::iter::from_fn(|| rx.try_recv().ok()).collect(),
    )
}

fn report_turn() -> serde_json::Value {
    serde_json::json!({"tool_calls": [{"name": "report", "args_json": "{\"summary\":\"done\",\"success\":true}"}]})
}

#[tokio::test]
async fn compaction_replaces_older_turns_with_a_summary() {
    use shikigami::HarnessEvent;

    // The summary entry sits anywhere in the script; it never consumes a turn.
    let events = compacting_run(serde_json::json!([
        write_turn("a.txt"),
        {"summary": "Wrote a.txt with 400 x characters."},
        write_turn("b.txt"),
        write_turn("c.txt"),
        report_turn(),
    ]))
    .await;
    assert!(
        events.iter().any(|e| matches!(
            e,
            HarnessEvent::ContextSummarized { before, after, through_turn }
                if after < before && *through_turn >= 1
        )),
        "missing ContextSummarized: {events:?}"
    );
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, HarnessEvent::ContextCompacted { .. })),
        "dropped instead of summarizing: {events:?}"
    );
}

#[tokio::test]
async fn compaction_without_a_summary_drops_and_says_why() {
    use shikigami::HarnessEvent;

    let events = compacting_run(serde_json::json!([
        write_turn("a.txt"),
        write_turn("b.txt"),
        write_turn("c.txt"),
        report_turn(),
    ]))
    .await;
    assert!(
        events
            .iter()
            .any(|e| matches!(e, HarnessEvent::ContextCompacted { .. })),
        "missing ContextCompacted: {events:?}"
    );
    assert!(
        events.iter().any(|e| matches!(
            e,
            HarnessEvent::Status { status } if status.starts_with("context summary unavailable")
        )),
        "missing fallback status: {events:?}"
    );
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, HarnessEvent::ContextSummarized { .. })),
        "{events:?}"
    );
}

#[tokio::test]
async fn fill_compact_runs_when_last_turn_input_tokens_cross_the_window() {
    let (ok, events) = usage_compact_run(Some(800), Some(1000), None, true).await;
    assert!(ok, "run failed: {events:?}");
    assert!(
        compacted_history_event(&events),
        "missing compact event: {events:?}"
    );
}

#[tokio::test]
async fn fill_compact_skips_without_a_due_last_turn() {
    for (tokens, window, why) in [
        (Some(100), Some(1000), "below the fill percent"),
        (Some(800), None, "without a window"),
        (None, Some(1000), "from missing usage"),
    ] {
        let (ok, events) = usage_compact_run(tokens, window, None, false).await;
        assert!(ok, "{why}: run failed: {events:?}");
        assert!(
            !compacted_history_event(&events),
            "compacted {why}: {events:?}"
        );
    }
}

#[tokio::test]
async fn count_compact_still_runs_when_fill_window_is_set_but_usage_is_low() {
    let (ok, events) = usage_compact_run(Some(100), Some(1000), Some(4), true).await;
    assert!(ok, "run failed: {events:?}");
    assert!(
        compacted_history_event(&events),
        "count compact did not run: {events:?}"
    );
}

async fn overflow_compact_run(
    script: serde_json::Value,
    compact_on_context_length: bool,
) -> (
    Result<shikigami::RunResult, shikigami::HarnessError>,
    Vec<shikigami::HarnessEvent>,
    Option<Vec<shikigami::model::ChatMessage>>,
) {
    use shikigami::ChannelSink;
    use std::sync::Arc;

    let dir = tempdir().unwrap();
    let state = StateRoot::new(dir.path().join("state"));
    let mut config = Config::default();
    config.governance.adapter = "local".into();
    config.model.adapter = "scripted".into();
    config.model.script_json = Some(script.to_string());
    config.events.adapter = "none".into();
    config.run.compact_on_context_length = compact_on_context_length;
    config.run.compact_keep_tail = 2;
    config.workspace.root = dir.path().join("ws").to_string_lossy().into();

    let harness = Harness::from_config(config, state).unwrap();
    let runs = harness.state.runs_dir();
    let (sink, rx) = ChannelSink::pair();
    let outcome = harness
        .run_with_events(RunRequest::new("write two files"), Some(Arc::new(sink)))
        .await;
    let messages = outcome.as_ref().ok().and_then(|result| {
        shikigami::checkpoint::Checkpoint::load(&runs, &result.run_id)
            .ok()
            .map(|checkpoint| checkpoint.messages)
    });
    (
        outcome,
        std::iter::from_fn(|| rx.try_recv().ok()).collect(),
        messages,
    )
}

fn overflow_report_requiring_compact() -> serde_json::Value {
    let mut report = report_turn();
    report["requires_context"] = serde_json::json!(["[context compacted"]);
    report
}

#[tokio::test]
async fn overflow_compact_retries_the_turn_after_a_context_length_refusal() {
    let (outcome, events, messages) = overflow_compact_run(
        serde_json::json!([
            write_turn("a.txt"),
            write_turn("b.txt"),
            {"fail": "context_length"},
            overflow_report_requiring_compact(),
        ]),
        true,
    )
    .await;
    let result = outcome.expect("run should complete after compact");
    assert!(result.success, "run failed: {result:?}");
    assert!(
        compacted_history_event(&events),
        "missing compact event: {events:?}"
    );
    let messages = messages.expect("successful run should leave a checkpoint");
    assert_eq!(messages[0].content, "write two files");
    assert!(
        messages
            .iter()
            .any(|message| message.content.contains("[context compacted")),
        "checkpoint prompt was not compacted: {messages:?}"
    );
}

#[tokio::test]
async fn overflow_compact_fails_closed_on_a_second_context_length_refusal() {
    let (outcome, events, _) = overflow_compact_run(
        serde_json::json!([
            write_turn("a.txt"),
            write_turn("b.txt"),
            {"fail": "context_length"},
            {"fail": "context_length"},
        ]),
        true,
    )
    .await;
    let error = outcome.expect_err("second refusal should fail the run");
    assert!(
        error.to_string().contains("context length"),
        "error should name context length: {error}"
    );
    assert!(
        !error.to_string().contains("400"),
        "error should not be a generic HTTP 400: {error}"
    );
    assert!(
        compacted_history_event(&events),
        "first refusal should compact: {events:?}"
    );
}

#[tokio::test]
async fn overflow_compact_stays_idle_when_the_setting_is_off() {
    let (outcome, events, _) = overflow_compact_run(
        serde_json::json!([
            write_turn("a.txt"),
            write_turn("b.txt"),
            {"fail": "context_length"},
            overflow_report_requiring_compact(),
        ]),
        false,
    )
    .await;
    let error = outcome.expect_err("refusal should fail when compact is off");
    assert!(
        error.to_string().contains("context length"),
        "error should name context length: {error}"
    );
    assert!(
        !compacted_history_event(&events),
        "compacted with the setting off: {events:?}"
    );
}

/// A `stop` hook that passes once the workspace's value.txt says `fixed`.
/// It lives outside the workspace, as a real check script would.
fn verify_hook(dir: &std::path::Path) -> serde_json::Value {
    let script = dir.join("verify.sh");
    std::fs::write(
        &script,
        "#!/bin/sh\ncat >/dev/null\ngrep -q fixed value.txt && exit 0\necho 'value.txt is still broken' >&2\nexit 2\n",
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    serde_json::json!({"event": "stop", "command": script, "timeout_ms": 30_000})
}

fn verified_config(dir: &std::path::Path, script: serde_json::Value, max_repairs: u32) -> Config {
    let mut config = Config::default();
    config.governance.adapter = "local".into();
    config.model.adapter = "scripted".into();
    config.model.script_json = Some(script.to_string());
    config.events.adapter = "none".into();
    config.workspace.root = dir.join("ws").to_string_lossy().into();
    config.hooks = vec![serde_json::from_value(verify_hook(dir)).unwrap()];
    config.run.max_stop_repairs = max_repairs;
    config
}

fn write_value(content: &str) -> serde_json::Value {
    let args = serde_json::json!({"path": "value.txt", "content": content}).to_string();
    serde_json::json!({"tool_calls": [{"name": "write_file", "args_json": args}]})
}

fn report_done() -> serde_json::Value {
    serde_json::json!({"tool_calls": [{"name": "report", "args_json": "{\"summary\":\"done\",\"success\":true}"}]})
}

async fn verified_run(
    config: Config,
    state: StateRoot,
    request: RunRequest,
) -> (shikigami::RunResult, Vec<shikigami::HarnessEvent>) {
    use shikigami::ChannelSink;
    use std::sync::Arc;

    let harness = Harness::from_config(config, state).unwrap();
    let (sink, rx) = ChannelSink::pair();
    let result = harness
        .run_with_events(request, Some(Arc::new(sink)))
        .await
        .unwrap();
    (result, std::iter::from_fn(|| rx.try_recv().ok()).collect())
}

fn verification_results(events: &[shikigami::HarnessEvent]) -> Vec<(u32, bool)> {
    events
        .iter()
        .filter_map(|event| match event {
            shikigami::HarnessEvent::VerificationFinished {
                attempt, passed, ..
            } => Some((*attempt, *passed)),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn stop_hook_sends_a_regression_back_until_it_is_repaired() {
    let dir = tempdir().unwrap();
    let script = serde_json::json!([
        write_value("broken"),
        report_done(),
        write_value("fixed"),
        report_done(),
    ]);
    let (result, events) = verified_run(
        verified_config(dir.path(), script, 3),
        StateRoot::new(dir.path().join("state")),
        RunRequest::new("make value.txt pass"),
    )
    .await;
    assert!(result.success, "{result:?}");
    assert_eq!(verification_results(&events), [(1, false), (2, true)]);
}

#[tokio::test]
async fn stop_hook_fails_the_run_when_repairs_run_out() {
    let dir = tempdir().unwrap();
    let script = serde_json::json!([write_value("broken"), report_done(), report_done()]);
    let (result, events) = verified_run(
        verified_config(dir.path(), script, 1),
        StateRoot::new(dir.path().join("state")),
        RunRequest::new("make value.txt pass"),
    )
    .await;
    assert!(!result.success, "{result:?}");
    assert_eq!(result.termination, shikigami::RunTermination::Failed);
    assert!(
        result
            .summary
            .starts_with("verification failed after 1 repair attempts"),
        "{}",
        result.summary
    );
    assert!(result.summary.contains("value.txt is still broken"));
    assert_eq!(verification_results(&events), [(1, false), (2, false)]);
}

#[tokio::test]
async fn stop_hook_repair_bound_survives_a_restart() {
    let dir = tempdir().unwrap();
    let state = || StateRoot::new(dir.path().join("state"));
    // The script ends right after the first refusal, so the run stops there
    // with one repair spent, as a crash would leave it.
    let first = serde_json::json!([write_value("broken"), report_done()]);
    let harness = Harness::from_config(verified_config(dir.path(), first, 1), state()).unwrap();
    let mut request = RunRequest::new("make value.txt pass");
    request.keep_workspace = true;
    let stopped = harness.run(request).await;
    assert!(stopped.is_err(), "script should run out: {stopped:?}");
    let run_ids: Vec<_> = std::fs::read_dir(state().runs_dir())
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    let [run_id] = run_ids.as_slice() else {
        panic!("expected one run: {run_ids:?}");
    };
    let run_id = run_id.clone();
    let checkpoint = shikigami::checkpoint::Checkpoint::load(&state().runs_dir(), &run_id).unwrap();
    assert_eq!(checkpoint.stop_repairs, 1);

    let resumed = serde_json::json!([write_value("broken"), report_done(), report_done()]);
    let mut request = RunRequest::new("make value.txt pass");
    request.resume_run_id = Some(run_id);
    let (result, events) =
        verified_run(verified_config(dir.path(), resumed, 1), state(), request).await;
    assert!(!result.success, "{result:?}");
    assert_eq!(verification_results(&events), [(2, false)]);
}

/// Script whose final file needs a fact (port 7341) that only the compacted
/// span states. `with_summary` adds the summary that carries the fact forward.
fn fact_across_compaction_script(with_summary: bool) -> serde_json::Value {
    let write = |path: &str, content: &str| {
        let args = serde_json::json!({"path": path, "content": content}).to_string();
        serde_json::json!({"tool_calls": [{"name": "write_file", "args_json": args}]})
    };
    let mut turns = vec![
        serde_json::json!({
            "content": "The release port from the task notes is 7341. Writing the notes file first.",
            "tool_calls": write("notes.txt", "draft")["tool_calls"],
        }),
        write("a.txt", "a"),
        write("b.txt", "b"),
        write("c.txt", "c"),
        write("d.txt", "d"),
    ];
    if with_summary {
        turns.push(serde_json::json!({
            "summary": "Task: write the service config. Found earlier: the release port is 7341."
        }));
    }
    let mut needs_port = write("service.conf", "port=7341\n");
    needs_port["requires_context"] = serde_json::json!(["7341"]);
    turns.push(needs_port);
    turns.push(report_turn());
    serde_json::Value::Array(turns)
}

async fn run_fact_across_compaction(
    dir: &Path,
    with_summary: bool,
) -> Result<shikigami::RunResult, shikigami::HarnessError> {
    let mut config = Config::default();
    config.governance.adapter = "local".into();
    config.model.adapter = "scripted".into();
    config.model.script_json = Some(fact_across_compaction_script(with_summary).to_string());
    config.events.adapter = "none".into();
    config.run.compact_after_messages = Some(10);
    config.workspace.root = dir.join("ws").to_string_lossy().into();
    let harness = Harness::from_config(config, StateRoot::new(dir.join("state"))).unwrap();
    let mut request = RunRequest::new("Write service.conf with the release port.");
    request.keep_workspace = true;
    harness.run(request).await
}

#[tokio::test]
async fn a_fact_from_the_compacted_span_survives_only_with_a_summary() {
    let dir = tempdir().unwrap();
    let result = run_fact_across_compaction(dir.path(), true).await.unwrap();
    assert!(result.success, "{result:?}");
    let conf = std::fs::read_to_string(result.workspace.join("service.conf")).unwrap();
    assert!(conf.contains("port=7341"));

    let dir = tempdir().unwrap();
    let dropped = run_fact_across_compaction(dir.path(), false).await;
    assert!(
        !dropped.as_ref().is_ok_and(|result| result.success),
        "compaction without a summary kept the fact: {dropped:?}"
    );
}

#[tokio::test]
async fn report_data_reaches_the_result_and_run_record_and_non_objects_are_refused() {
    let dir = tempdir().unwrap();
    let state = StateRoot::new(dir.path().join("state"));
    let mut config = Config::default();
    config.governance.adapter = "none".into();
    config.model.adapter = "scripted".into();
    config.model.script_json = Some(
        serde_json::json!([
            {"tool_calls":[{"name":"report","args_json":
                r#"{"summary":"too early","success":true,"data":"p-1"}"#}]},
            {"tool_calls":[{"name":"report","args_json":
                r#"{"summary":"proposed","success":true,"data":{"proposal":"p-1"}}"#}]}
        ])
        .to_string(),
    );
    config.events.adapter = "none".into();
    config.workspace.root = dir.path().join("ws").to_string_lossy().into();

    let harness = Harness::from_config(config, state.clone()).unwrap();
    let result = harness.run(RunRequest::new("propose")).await.unwrap();

    // The string `data` was refused as a tool error, so the run went on to
    // the second report instead of finishing on the first.
    let expected = serde_json::json!({"proposal":"p-1"});
    assert_eq!(result.summary, "proposed");
    assert_eq!(
        result.data.clone().map(serde_json::Value::Object),
        Some(expected.clone())
    );
    let record = shikigami::registry::RunRegistry::new(state.path())
        .unwrap()
        .load(&result.run_id)
        .unwrap();
    assert_eq!(record.data.map(serde_json::Value::Object), Some(expected));
}
