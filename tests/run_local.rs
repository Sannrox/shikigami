use shikigami::{Config, Harness, RunRequest, StateRoot};
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
            "no_match",
            "ambiguous",
            "invalid_input",
            "limit",
            "io",
        ] {
            let dir = tempdir().unwrap();
            let workspace = dir.path().join("workspace");
            std::fs::create_dir(&workspace).unwrap();
            let path = workspace.join("secret-path.txt");
            match outcome {
                "applied" | "invalid_input" => std::fs::write(&path, "secret-old\n").unwrap(),
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
