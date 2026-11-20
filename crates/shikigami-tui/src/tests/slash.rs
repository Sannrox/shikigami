use super::support::*;
use crate::input::handle_key;
use crate::protocol::apply_prompt_result;
use crate::render::status_line;
use crate::session::*;
use crate::slash::*;

use std::sync::Arc;

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use serde_json::json;
use shikigami::acp::AcpHost;
use shikigami::config::{
    Config, MCP_GATEWAY_NAME, McpGatewaySettings, McpServerSettings, ToolsSettings,
};
use shikigami::harness::Harness;
use shikigami::state::StateRoot;
use tempfile::tempdir;

fn with_demo_mcp(config: &mut Config) {
    config
        .tools
        .mcp_servers
        .push(McpServerSettings::stdio("demo", "mock", Vec::new()));
    let mut enabled = ToolsSettings::default_coding_tools();
    enabled.push("mcp.demo.echo".into());
    config.tools.enabled = enabled;
}

#[tokio::test]
async fn slash_run_names_the_session_run() {
    let dir = tempdir().unwrap();
    let session = start_session(&dir, r#"[{"content":"ok"}]"#).await;
    type_text(&session, "/run");
    assert!(matches!(
        handle_key(&session, enter_key()),
        KeyResult::Continue
    ));
    assert!(
        session
            .transcript_text()
            .contains("No run yet. Send a prompt to start one."),
        "{}",
        session.transcript_text()
    );

    session.spawn_prompt("hello".into()).await.unwrap();
    let stop = session.wait_prompt().await;
    apply_prompt_result(&mut session.lock_shared(), stop.as_ref());
    let run_id = session.lock_shared().run_id.clone().expect("run id");
    assert!(
        dir.path().join("state/runs").join(&run_id).exists(),
        "{run_id} is a run on disk"
    );
    session.lock_shared().transcript.clear();
    type_text(&session, "/run");
    assert!(matches!(
        handle_key(&session, enter_key()),
        KeyResult::Continue
    ));
    let rows = drawn_rows(&session, 80, 24);
    assert_screen_rules(&rows, 80);
    let screen = rows.join("\n").replace(&run_id, "<run>");
    let rule = "─".repeat(80);
    let expected = format!(
        "{}Run <run>\nshikigami runs <run>\nshikigami export <run>\n{rule}\n>\n{rule}\n  done  ^c quit  pgup/pgdn/wheel",
        "\n".repeat(17)
    );
    assert_eq!(screen, expected);
    // Narrow: the id may wrap onto its own row but is never split.
    let rows = drawn_rows(&session, 40, 12);
    assert_screen_rules(&rows, 40);
    assert!(rows.iter().any(|row| row.contains(&run_id)), "{rows:#?}");
}

/// CJK, emoji and one unbroken long line wrap inside the screen.
#[test]
fn slash_window_scrolls_to_keep_selection_visible() {
    assert_eq!(slash_window(0, 12, 8), (0, 8));
    assert_eq!(slash_window(7, 12, 8), (0, 8));
    assert_eq!(slash_window(8, 12, 8), (1, 8));
    assert_eq!(slash_window(11, 12, 8), (4, 8));
    assert_eq!(slash_window(0, 3, 8), (0, 3));
    assert_eq!(slash_window(0, 0, 8), (0, 0));
}

#[tokio::test]
async fn at_mention_lists_workspace_files_and_inserts_the_pick() {
    let dir = tempdir().unwrap();
    let session = start_session(&dir, r#"[{"content":"ok"}]"#).await;
    let project = dir.path().join("project");
    std::fs::create_dir_all(project.join("src")).unwrap();
    std::fs::write(project.join("src/main.rs"), "").unwrap();
    std::fs::write(project.join(".gitignore"), "build.log\n").unwrap();
    std::fs::write(project.join("build.log"), "").unwrap();
    std::fs::write(dir.path().join("outside.rs"), "").unwrap();

    type_text(&session, "fix @");
    // The files are listed off the draw thread; the list fills in after.
    wait_for_mention_files(&session).await;
    let screen = drawn_text(&session);
    assert!(screen.contains("  src/main.rs"), "{screen}");
    assert!(screen.contains("tab pick"), "{screen}");
    assert!(!screen.contains("build.log"), "{screen}");
    assert!(!screen.contains("outside.rs"), "{screen}");

    type_text(&session, "mai");
    handle_key(&session, KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
    assert_eq!(session.lock_shared().input, "fix @src/main.rs ");
    assert!(!drawn_text(&session).contains("  src/main.rs"));

    type_text(&session, "@zzz");
    assert!(session.mention_matches().is_empty());
    session.lock_shared().clear_input();
    type_text(&session, "see @src");
    wait_for_mention_files(&session).await;
    assert!(!session.mention_matches().is_empty());
    handle_key(&session, KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
    assert!(session.mention_matches().is_empty());
    type_text(&session, "/");
    assert!(
        session.mention_matches().is_empty(),
        "esc keeps the word closed"
    );
    assert_eq!(session.lock_shared().input, "see @src/");
    for _ in 0.."@src/".len() {
        handle_key(
            &session,
            KeyEvent::new(KeyCode::Backspace, KeyModifiers::NONE),
        );
    }
    type_text(&session, "@");
    assert!(
        mention_visible(&session.lock_shared()).is_some(),
        "a new word opens again"
    );
}

#[tokio::test]
async fn model_command_lists_and_switches_the_sessions_model() {
    let dir = tempdir().unwrap();
    let cwd = dir.path().join("project");
    std::fs::create_dir_all(&cwd).unwrap();
    let state = StateRoot::new(dir.path().join("state"));
    let mut config = Config::default();
    config.governance.adapter = "local".into();
    config.model.adapter = "scripted".into();
    config.model.script_json = Some(r#"[{"content":"ok"}]"#.into());
    config.model.model = "model-a".into();
    config.model.alternates = vec!["model-b".into()];
    config.events.adapter = "none".into();
    let harness = Harness::from_config(config, state).unwrap();
    let mut session = TuiSession::start(Arc::new(AcpHost::new(harness)), &cwd, false)
        .await
        .unwrap();
    session.model = "scripted".into();
    let run = |session: &TuiSession, line: &str| {
        session.lock_shared().input = line.into();
        handle_key(session, KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE))
    };

    assert!(matches!(run(&session, "/model"), KeyResult::Continue));
    let text = session.transcript_text();
    assert!(
        text.contains("Models: model-a (current), model-b"),
        "{text}"
    );

    let KeyResult::SetModel(id) = run(&session, "/model model-z") else {
        panic!("expected a switch");
    };
    session.set_model(&id).await;
    assert!(
        session
            .transcript_text()
            .contains("The model stays: model `model-z` is not offered"),
        "{}",
        session.transcript_text()
    );
    let screen = drawn_text(&session);
    assert!(screen.contains("ready  scripted"), "{screen}");

    let KeyResult::SetModel(id) = run(&session, "/model model-b") else {
        panic!("expected a switch");
    };
    session.set_model(&id).await;
    assert!(
        session
            .transcript_text()
            .contains("Next prompts use model-b.")
    );
    let screen = drawn_text(&session);
    assert!(screen.contains("ready  model-b"), "{screen}");
}

#[tokio::test]
async fn mcp_command_lists_disables_and_refuses_the_gateway() {
    let empty_dir = tempdir().unwrap();
    let empty = start_session(&empty_dir, r#"[{"content":"ok"}]"#).await;
    let run = |session: &TuiSession, line: &str| {
        session.lock_shared().input = line.into();
        handle_key(session, KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE))
    };
    let KeyResult::Mcp { action, name } = run(&empty, "/mcp") else {
        panic!("expected a list");
    };
    assert_eq!(action, "list");
    assert!(name.is_none());
    empty.mcp(&action, name.as_deref()).await;
    assert_eq!(last_system(&empty), "No MCP servers attached.");

    type_text(&empty, "/m");
    assert_eq!(
        empty
            .slash_matches()
            .iter()
            .map(|cmd| cmd.name.as_str())
            .collect::<Vec<_>>(),
        vec!["model", "mcp"]
    );

    let dir = tempdir().unwrap();
    let session = start_session_with(&dir, r#"[{"content":"ok"}]"#, |config| {
        with_demo_mcp(config);
        config.tools.mcp_gateway = Some(McpGatewaySettings::new("http://127.0.0.1:1/mcp"));
    })
    .await;
    let KeyResult::Mcp { action, name } = run(&session, "/mcp") else {
        panic!("expected a list");
    };
    session.mcp(&action, name.as_deref()).await;
    let listed = last_system(&session);
    assert!(listed.contains("MCP"), "{listed}");
    assert!(listed.contains("demo  mcp.demo.echo"), "{listed}");
    assert!(listed.contains(MCP_GATEWAY_NAME), "{listed}");
    assert!(!listed.contains("disabled"), "{listed}");
    let rows = drawn_rows(&session, 80, 24);
    assert_screen_rules(&rows, 80);
    let narrow = drawn_rows(&session, 40, 12);
    assert_screen_rules(&narrow, 40);

    assert!(matches!(run(&session, "/mcp disable"), KeyResult::Continue));
    assert_eq!(last_system(&session), MCP_USAGE);

    let KeyResult::Mcp { action, name } = run(&session, "/mcp disable other") else {
        panic!("expected disable");
    };
    session.mcp(&action, name.as_deref()).await;
    assert_eq!(
        last_system(&session),
        "No MCP server named other is attached."
    );

    let gateway_line = format!("/mcp disable {MCP_GATEWAY_NAME}");
    let KeyResult::Mcp { action, name } = run(&session, &gateway_line) else {
        panic!("expected disable");
    };
    session.mcp(&action, name.as_deref()).await;
    assert_eq!(last_system(&session), "gateway cannot be disabled.");

    let KeyResult::Mcp { action, name } = run(&session, "/mcp disable demo") else {
        panic!("expected disable");
    };
    session.mcp(&action, name.as_deref()).await;
    assert_eq!(last_system(&session), "Disabled demo for this session.");
    let KeyResult::Mcp { action, name } = run(&session, "/mcp") else {
        panic!("expected a list");
    };
    session.mcp(&action, name.as_deref()).await;
    assert!(
        last_system(&session).contains("demo  mcp.demo.echo  disabled"),
        "{}",
        last_system(&session)
    );

    let KeyResult::Mcp { action, name } = run(&session, "/mcp enable demo") else {
        panic!("expected enable");
    };
    session.mcp(&action, name.as_deref()).await;
    assert_eq!(last_system(&session), "Enabled demo for this session.");
}

#[tokio::test]
async fn mcp_disable_hides_the_tool_until_a_new_host() {
    let dir = tempdir().unwrap();
    let script = r#"[{"tool_calls":[{"name":"mcp.demo.echo","args_json":"{\"text\":\"hi\"}"}]},{"content":"first"},{"tool_calls":[{"name":"mcp.demo.echo","args_json":"{\"text\":\"hi\"}"}]},{"content":"second"}]"#;
    let session = start_session_with(&dir, script, with_demo_mcp).await;
    let run = |session: &TuiSession, line: &str| {
        session.lock_shared().input = line.into();
        handle_key(session, KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE))
    };

    session.spawn_prompt("one".into()).await.unwrap();
    let stop = session.wait_prompt().await;
    apply_prompt_result(&mut session.lock_shared(), stop.as_ref());
    let first = session.transcript_text();
    assert!(first.contains("echo:hi"), "{first}");

    let KeyResult::Mcp { action, name } = run(&session, "/mcp disable demo") else {
        panic!("expected disable");
    };
    session.mcp(&action, name.as_deref()).await;
    session.lock_shared().transcript.clear();
    session.spawn_prompt("two".into()).await.unwrap();
    let stop = session.wait_prompt().await;
    apply_prompt_result(&mut session.lock_shared(), stop.as_ref());
    let second = session.transcript_text();
    assert!(
        second.contains("mcp.demo.echo")
            && (second.contains("not enabled")
                || second.contains("unknown")
                || second.contains("denies")),
        "{second}"
    );
    assert!(!second.contains("echo:hi"), "{second}");

    drop(session);
    let session = continue_session_with(&dir, script, with_demo_mcp).await;
    let KeyResult::Mcp { action, name } = run(&session, "/mcp") else {
        panic!("expected a list");
    };
    session.mcp(&action, name.as_deref()).await;
    let listed = last_system(&session);
    assert!(listed.contains("demo  mcp.demo.echo"), "{listed}");
    assert!(!listed.contains("disabled"), "{listed}");
}

#[tokio::test]
async fn slash_enter_maps_compact_exit_new_and_unknown() {
    let dir = tempdir().unwrap();
    let cwd = dir.path().join("project");
    std::fs::create_dir_all(&cwd).unwrap();
    let host = Arc::new(scripted_host(dir.path(), r#"[{"content":"ok"}]"#));
    let session = TuiSession::start(host, &cwd, false).await.unwrap();

    type_text(&session, "/c");
    assert_eq!(
        session
            .slash_matches()
            .iter()
            .map(|cmd| cmd.name.as_str())
            .collect::<Vec<_>>(),
        vec!["compact", "copy"]
    );
    assert!(matches!(
        handle_key(&session, KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)),
        KeyResult::Compact
    ));

    type_text(&session, "/exit");
    assert!(matches!(
        handle_key(&session, KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)),
        KeyResult::Quit
    ));

    type_text(&session, "/new");
    assert!(matches!(
        handle_key(&session, KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)),
        KeyResult::NewSession
    ));

    type_text(&session, "/exti");
    assert!(matches!(
        handle_key(&session, KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)),
        KeyResult::Continue
    ));
    assert!(
        last_system(&session).contains("Unknown command /exti. Did you mean /exit?"),
        "{}",
        last_system(&session)
    );
    assert_eq!(session.lock_shared().take_input(), "/exti");

    type_text(&session, "/EXIT");
    handle_key(&session, KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
    assert!(last_system(&session).contains("Did you mean /exit?"));
    session.lock_shared().take_input();

    type_text(&session, "/zzzzzz");
    handle_key(&session, KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
    assert!(
        last_system(&session).contains("/help lists commands"),
        "{}",
        last_system(&session)
    );
    session.lock_shared().take_input();

    for (typed, sent) in [
        ("//exti now", "/exti now"),
        ("/usr/bin/env fails", "/usr/bin/env fails"),
    ] {
        type_text(&session, typed);
        match handle_key(&session, KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)) {
            KeyResult::Prompt { display, send, .. } => {
                assert_eq!(display, sent);
                assert_eq!(send, sent);
            }
            other => panic!("{typed}: expected prompt, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn slash_help_writes_system_block_and_stays_idle() {
    let dir = tempdir().unwrap();
    let cwd = dir.path().join("project");
    std::fs::create_dir_all(&cwd).unwrap();
    let host = Arc::new(scripted_host(dir.path(), r#"[{"content":"ok"}]"#));
    let session = TuiSession::start(host, &cwd, false).await.unwrap();
    assert!(session.slash_catalog().iter().any(|cmd| cmd.name == "help"));
    type_text(&session, "/help");
    assert!(matches!(
        handle_key(&session, KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)),
        KeyResult::Continue
    ));
    assert!(session.lock_shared().input.is_empty());
    assert!(!session.lock_shared().busy);
    assert!(session.overlay_text().is_none());
    let text = session.transcript_text();
    assert!(text.contains("Enter"), "{text}");
    assert!(text.contains("Shift+Enter"), "{text}");
    assert!(text.contains("^c cancel/quit"), "{text}");
    assert!(text.contains("/compact"), "{text}");
    assert!(text.contains("/new"), "{text}");
    assert!(text.contains("/resume"), "{text}");
    assert!(text.contains("/run"), "{text}");
    assert!(text.contains("/copy"), "{text}");
    assert!(text.contains("/attach"), "{text}");
    assert!(text.contains("/exit"), "{text}");
    assert!(text.contains("/quit"), "{text}");
    assert!(
        session
            .lock_shared()
            .transcript
            .iter()
            .any(|line| matches!(line, TranscriptLine::System(_))),
        "{text}"
    );
}

#[tokio::test]
async fn slash_attach_sends_acp_image_part() {
    let dir = tempdir().unwrap();
    let cwd = dir.path().join("project");
    std::fs::create_dir_all(&cwd).unwrap();
    let png = b"\x89PNG UNIQUE-TUI-401-IMAGE";
    std::fs::write(cwd.join("shot.png"), png).unwrap();
    let host = Arc::new(scripted_host(dir.path(), r#"[{"content":"saw-image"}]"#));
    let session = TuiSession::start(host, &cwd, false).await.unwrap();
    assert!(
        session
            .slash_catalog()
            .iter()
            .any(|cmd| cmd.name == "attach")
    );
    type_text(&session, "/attach shot.png");
    assert!(matches!(
        handle_key(&session, KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)),
        KeyResult::Continue
    ));
    let staged = session.lock_shared().pending_parts.clone();
    assert_eq!(staged.len(), 1, "{staged:?}");
    assert_eq!(staged[0]["type"], "image");
    assert_eq!(staged[0]["mimeType"], "image/png");
    type_text(&session, "inspect");
    match handle_key(&session, KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)) {
        KeyResult::Prompt {
            display,
            send,
            parts,
        } => {
            assert_eq!(send, "inspect");
            assert!(display.contains("image/png"), "{display}");
            assert!(!display.contains("UNIQUE-TUI-401-IMAGE"), "{display}");
            assert_eq!(parts[0]["type"], "text");
            assert_eq!(parts[1]["type"], "image");
            session
                .spawn_prompt_with(display, send, parts)
                .await
                .unwrap();
            let stop = session.wait_prompt().await;
            assert_eq!(
                stop.as_ref().and_then(|v| v.pointer("/result/stopReason")),
                Some(&json!("end_turn"))
            );
        }
        other => panic!("expected prompt, got {other:?}"),
    }
    let leaked = std::fs::read_dir(&cwd)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .any(|entry| {
            let path = entry.path();
            path.file_name().and_then(|name| name.to_str()) != Some("shot.png")
                && std::fs::read(&path)
                    .ok()
                    .is_some_and(|bytes| bytes.windows(png.len()).any(|window| window == png))
        });
    assert!(!leaked, "attachment bytes copied into the workspace");
}

#[tokio::test]
async fn slash_attach_rejects_oversize_file_before_staging() {
    let dir = tempdir().unwrap();
    let cwd = dir.path().join("project");
    std::fs::create_dir_all(&cwd).unwrap();
    let huge = cwd.join("huge.png");
    std::fs::write(&huge, vec![0u8; 8 * 1024 * 1024 + 1]).unwrap();
    let host = Arc::new(scripted_host(dir.path(), r#"[{"content":"unused"}]"#));
    let session = TuiSession::start(host, &cwd, false).await.unwrap();
    type_text(&session, "/attach huge.png");
    assert!(matches!(
        handle_key(&session, KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)),
        KeyResult::Continue
    ));
    let shared = session.lock_shared();
    assert!(
        shared.pending_parts.is_empty(),
        "{:?}",
        shared.pending_parts
    );
    let system: Vec<&str> = shared
        .transcript
        .iter()
        .filter_map(|line| match line {
            TranscriptLine::System(text) => Some(text.as_str()),
            _ => None,
        })
        .collect();
    assert!(
        system.iter().any(|text| text.contains("8MiB")),
        "{system:?}"
    );
}

#[tokio::test]
async fn slash_copy_records_last_assistant_as_osc52_payload() {
    let dir = tempdir().unwrap();
    let session = session_with_assistant(&dir, "last assistant reply").await;
    run_copy(&session);
    assert_eq!(
        session.lock_shared().last_osc52.as_deref(),
        Some("last assistant reply")
    );
    assert_eq!(session.lock_shared().status, "copied");
    let footer = status_line(
        &session.model,
        &session.look.keys,
        &session.lock_shared(),
        80,
    );
    assert!(footer.contains("copied"), "{footer}");
    let text = session.transcript_text();
    assert!(text.contains("last assistant reply"), "{text}");
    assert!(!text.contains("nothing to copy"), "{text}");
    assert!(
        !session
            .lock_shared()
            .transcript
            .iter()
            .any(|line| matches!(line, TranscriptLine::System(_))),
        "{text}"
    );
    assert_eq!(
        osc52_sequence("last assistant reply"),
        format!(
            "\x1b]52;c;{}\x07",
            base64_encode("last assistant reply".as_bytes())
        )
    );
}

#[tokio::test]
async fn slash_copy_without_assistant_is_a_system_line() {
    let dir = tempdir().unwrap();
    let session = start_session(&dir, r#"[{"content":"ok"}]"#).await;
    session
        .lock_shared()
        .transcript
        .push(TranscriptLine::System("keep-me".into()));
    session
        .lock_shared()
        .transcript
        .push(TranscriptLine::User("hi".into()));
    run_copy(&session);
    assert!(session.lock_shared().last_osc52.is_none());
    let text = session.transcript_text();
    assert!(text.contains("keep-me"), "{text}");
    assert!(text.contains("nothing to copy"), "{text}");
    assert_ne!(session.lock_shared().status, "copied");
}

#[tokio::test]
async fn ctrl_y_while_idle_copies_last_assistant() {
    let dir = tempdir().unwrap();
    let session = session_with_assistant(&dir, "idle copy").await;
    type_text(&session, "draft");
    assert!(matches!(
        handle_key(&session, ctrl_y()),
        KeyResult::Continue
    ));
    assert_eq!(session.lock_shared().input, "draft");
    assert_eq!(
        session.lock_shared().last_osc52.as_deref(),
        Some("idle copy")
    );
    assert_eq!(session.lock_shared().status, "copied");
}

#[tokio::test]
async fn ctrl_y_while_busy_does_not_copy() {
    let dir = tempdir().unwrap();
    let session = session_with_assistant(&dir, "previous").await;
    session.spawn_prompt("again".into()).await.unwrap();
    assert!(session.lock_shared().busy);
    session.lock_shared().last_osc52 = None;
    assert!(matches!(
        handle_key(&session, ctrl_y()),
        KeyResult::Continue
    ));
    assert!(session.lock_shared().last_osc52.is_none());
    assert_ne!(session.lock_shared().status, "copied");
    let _ = session.wait_prompt().await;
}

#[tokio::test]
async fn copy_caps_huge_assistant_payload() {
    let dir = tempdir().unwrap();
    let session = start_session(&dir, r#"[{"content":"ok"}]"#).await;
    let huge = "x".repeat(OSC52_MAX_BYTES + 64);
    session
        .lock_shared()
        .transcript
        .push(TranscriptLine::Assistant(huge));
    run_copy(&session);
    let payload = session
        .lock_shared()
        .last_osc52
        .clone()
        .expect("capped payload");
    assert_eq!(payload.len(), OSC52_MAX_BYTES);
    assert!(payload.bytes().all(|b| b == b'x'));
    assert_eq!(session.lock_shared().status, "copied");
}

#[tokio::test]
async fn copy_write_error_is_a_system_line() {
    let dir = tempdir().unwrap();
    let session = session_with_assistant(&dir, "secret reply").await;
    session.fail_osc_write();
    run_copy(&session);
    assert!(session.lock_shared().last_osc52.is_none());
    let text = session.transcript_text();
    assert!(text.contains("copy failed"), "{text}");
    assert!(text.contains("secret reply"), "{text}");
    assert_eq!(session.lock_shared().status, "error");
}

#[test]
fn osc52_sequence_encodes_payload() {
    assert_eq!(base64_encode(b""), "");
    assert_eq!(base64_encode(b"M"), "TQ==");
    assert_eq!(base64_encode(b"Ma"), "TWE=");
    assert_eq!(base64_encode(b"Man"), "TWFu");
    assert_eq!(base64_encode(b"hi"), "aGk=");
    assert_eq!(osc52_sequence("hi"), "\x1b]52;c;aGk=\x07");
    assert_eq!(cap_bytes("éé", 3), "é");
    assert_eq!(cap_bytes("abc", 8), "abc");
}

#[tokio::test]
async fn slash_esc_dismisses_list_and_keeps_draft() {
    let dir = tempdir().unwrap();
    let cwd = dir.path().join("project");
    std::fs::create_dir_all(&cwd).unwrap();
    let host = Arc::new(scripted_host(dir.path(), r#"[{"content":"ok"}]"#));
    let session = TuiSession::start(host, &cwd, false).await.unwrap();
    type_text(&session, "/compact");
    assert!(slash_visible(&session.lock_shared()));
    handle_key(&session, KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
    assert!(!slash_visible(&session.lock_shared()));
    assert_eq!(session.lock_shared().input, "/compact");
    assert!(matches!(
        handle_key(&session, KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)),
        KeyResult::Compact
    ));
}

#[tokio::test]
async fn slash_arrows_move_selection_not_history() {
    let dir = tempdir().unwrap();
    let cwd = dir.path().join("project");
    std::fs::create_dir_all(&cwd).unwrap();
    let host = Arc::new(scripted_host(dir.path(), r#"[{"content":"ok"}]"#));
    let session = TuiSession::start(host, &cwd, false).await.unwrap();
    session.spawn_prompt("first".into()).await.unwrap();
    let _ = session.wait_prompt().await;
    type_text(&session, "/");
    handle_key(&session, KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));
    assert_eq!(session.lock_shared().input, "/");
    assert_eq!(session.lock_shared().slash_selected, 1);
    handle_key(&session, KeyEvent::new(KeyCode::Up, KeyModifiers::NONE));
    assert_eq!(session.lock_shared().slash_selected, 0);
    assert_eq!(session.lock_shared().input, "/");
}

#[tokio::test]
async fn slash_enter_runs_highlighted_skill_from_prefix() {
    let dir = tempdir().unwrap();
    let cwd = dir.path().join("project");
    let skill_dir = cwd.join(".shikigami/skills/demo");
    std::fs::create_dir_all(&skill_dir).unwrap();
    std::fs::write(skill_dir.join("SKILL.md"), "prefer tests first\n").unwrap();
    let host = Arc::new(scripted_host(dir.path(), r#"[{"content":"ok"}]"#));
    let session = TuiSession::start(host, &cwd, false).await.unwrap();
    type_text(&session, "/de");
    assert_eq!(slash_names(&session), vec!["demo"]);
    match handle_key(&session, KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)) {
        KeyResult::Prompt { display, send, .. } => {
            assert_eq!(display, "/demo");
            assert!(send.contains("prefer tests first"), "{send}");
        }
        other => panic!("expected highlighted skill prompt, got {other:?}"),
    }
}

#[tokio::test]
async fn slash_skill_name_sends_pack_without_prefix() {
    let dir = tempdir().unwrap();
    let session = session_with_skills(&dir, &["demo"]).await;
    type_text(&session, "/");
    let names = slash_names(&session);
    assert!(names.iter().any(|name| name == "demo"), "{names:?}");
    assert!(
        !names.iter().any(|name| name.starts_with("skill:")),
        "{names:?}"
    );
    type_text(&session, "d");
    let screen = drawn_text(&session);
    assert!(screen.contains("  demo      load skill pack"), "{screen}");
    session.lock_shared().clear_input();

    type_text(&session, "/demo extra");
    match handle_key(&session, enter_key()) {
        KeyResult::Prompt {
            display,
            send,
            parts,
        } => {
            assert_eq!(display, "/demo extra");
            assert_eq!(send, "demo pack body\n\n\nextra");
            assert_eq!(parts.len(), 1);
        }
        other => panic!("expected skill prompt, got {other:?}"),
    }
    assert!(session.lock_shared().input.is_empty());

    type_text(&session, "/skill:demo");
    match handle_key(&session, enter_key()) {
        KeyResult::Prompt { display, send, .. } => {
            assert_eq!(display, "/skill:demo");
            assert!(send.starts_with("demo pack body"), "{send}");
        }
        other => panic!("expected alias skill prompt, got {other:?}"),
    }
}

/// #650: a skill named like a built-in does not shadow it; it stays
/// reachable, and listed, as `/skill:<name>`.
#[tokio::test]
async fn slash_builtin_wins_over_a_skill_with_its_name() {
    let dir = tempdir().unwrap();
    let session = session_with_skills(&dir, &["help", "new"]).await;
    type_text(&session, "/");
    let names = slash_names(&session);
    for name in ["help", "new"] {
        assert_eq!(names.iter().filter(|n| *n == name).count(), 1, "{names:?}");
        assert!(names.contains(&format!("skill:{name}")), "{names:?}");
    }
    session.lock_shared().clear_input();

    type_text(&session, "/new");
    assert!(matches!(
        handle_key(&session, enter_key()),
        KeyResult::NewSession
    ));
    type_text(&session, "/help");
    assert!(matches!(
        handle_key(&session, enter_key()),
        KeyResult::Continue
    ));
    assert!(last_system(&session).contains("/compact"));

    type_text(&session, "/skill:help more");
    match handle_key(&session, enter_key()) {
        KeyResult::Prompt { display, send, .. } => {
            assert_eq!(display, "/skill:help more");
            assert_eq!(send, "help pack body\n\n\nmore");
        }
        other => panic!("expected colliding skill via alias, got {other:?}"),
    }
}

/// #650: a skill cannot take over a built-in's abbreviation. Built-ins
/// list first, so `/co` + Enter still compacts; the pack stays reachable
/// as `/skill:co`.
#[tokio::test]
async fn slash_builtin_abbreviation_wins_over_a_skill() {
    let dir = tempdir().unwrap();
    let session = session_with_skills(&dir, &["co"]).await;
    type_text(&session, "/co");
    assert_eq!(slash_names(&session), vec!["compact", "copy", "co"]);
    assert!(matches!(
        handle_key(&session, enter_key()),
        KeyResult::Compact
    ));
    session.lock_shared().clear_input();
    type_text(&session, "/skill:co");
    match handle_key(&session, enter_key()) {
        KeyResult::Prompt { display, .. } => assert_eq!(display, "/skill:co"),
        other => panic!("expected skill co via alias, got {other:?}"),
    }
}

#[tokio::test]
async fn slash_skill_sends_pack_body() {
    let dir = tempdir().unwrap();
    let cwd = dir.path().join("project");
    let skill_dir = cwd.join(".shikigami/skills/demo");
    std::fs::create_dir_all(&skill_dir).unwrap();
    std::fs::write(skill_dir.join("SKILL.md"), "prefer tests first\n").unwrap();
    let host = Arc::new(scripted_host(dir.path(), r#"[{"content":"ok"}]"#));
    let session = TuiSession::start(host, &cwd, false).await.unwrap();
    type_text(&session, "/skill:demo extra");
    match handle_key(&session, KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)) {
        KeyResult::Prompt { display, send, .. } => {
            assert_eq!(display, "/skill:demo extra");
            assert!(send.contains("prefer tests first"), "{send}");
            assert!(send.contains("extra"), "{send}");
        }
        other => panic!("expected skill prompt, got {other:?}"),
    }
}

#[tokio::test]
async fn slash_skill_includes_staged_attachments() {
    let dir = tempdir().unwrap();
    let cwd = dir.path().join("project");
    let skill_dir = cwd.join(".shikigami/skills/demo");
    std::fs::create_dir_all(&skill_dir).unwrap();
    std::fs::write(skill_dir.join("SKILL.md"), "prefer tests first\n").unwrap();
    let png = b"\x89PNG UNIQUE-TUI-401-SKILL";
    std::fs::write(cwd.join("shot.png"), png).unwrap();
    let host = Arc::new(scripted_host(dir.path(), r#"[{"content":"ok"}]"#));
    let session = TuiSession::start(host, &cwd, false).await.unwrap();
    type_text(&session, "/attach shot.png");
    assert!(matches!(
        handle_key(&session, KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)),
        KeyResult::Continue
    ));
    type_text(&session, "/skill:demo");
    match handle_key(&session, KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)) {
        KeyResult::Prompt {
            display,
            send,
            parts,
        } => {
            assert!(display.contains("/skill:demo"), "{display}");
            assert!(display.contains("image/png"), "{display}");
            assert!(send.contains("prefer tests first"), "{send}");
            assert_eq!(parts[0]["type"], "text");
            assert_eq!(parts[1]["type"], "image");
            assert_eq!(parts[1]["mimeType"], "image/png");
        }
        other => panic!("expected skill prompt with attachment, got {other:?}"),
    }
    assert!(
        session.lock_shared().pending_parts.is_empty(),
        "staged parts must be consumed by /skill:"
    );
}

#[tokio::test]
async fn slash_skill_loads_agents_skills() {
    let dir = tempdir().unwrap();
    let cwd = dir.path().join("project");
    let skill_dir = cwd.join(".agents/skills/verify-change");
    std::fs::create_dir_all(&skill_dir).unwrap();
    std::fs::write(skill_dir.join("SKILL.md"), "run make validate\n").unwrap();
    let host = Arc::new(scripted_host(dir.path(), r#"[{"content":"ok"}]"#));
    let session = TuiSession::start(host, &cwd, false).await.unwrap();
    type_text(&session, "/skill:verify-change");
    match handle_key(&session, KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)) {
        KeyResult::Prompt { display, send, .. } => {
            assert_eq!(display, "/skill:verify-change");
            assert!(send.contains("run make validate"), "{send}");
        }
        other => panic!("expected agents skill prompt, got {other:?}"),
    }
}

#[tokio::test]
async fn slash_new_clears_transcript_and_changes_session() {
    let dir = tempdir().unwrap();
    let cwd = dir.path().join("project");
    std::fs::create_dir_all(&cwd).unwrap();
    let host = Arc::new(scripted_host(dir.path(), r#"[{"content":"hello"}]"#));
    let session = TuiSession::start(host, &cwd, false).await.unwrap();
    session.spawn_prompt("hi".into()).await.unwrap();
    let _ = session.wait_prompt().await;
    assert!(session.transcript_text().contains("hello"));
    let before = session.session_id();
    session.new_session().await.unwrap();
    assert_ne!(session.session_id(), before);
    assert!(session.transcript_text().is_empty());
}

/// Sets when a session last did work, as `/resume` and `--continue` order it.
#[tokio::test]
async fn resume_lists_age_turns_and_first_prompt() {
    let dir = tempdir().unwrap();
    let (session, older, newer) = two_cwd_sessions(&dir).await;
    open_resume(&session);
    let ids: Vec<String> = session
        .lock_shared()
        .resume
        .as_ref()
        .expect("resume picker")
        .iter()
        .map(|s| s.session_id.clone())
        .collect();
    assert_eq!(ids, vec![newer.clone(), older.clone()]);
    let rows: Vec<String> = session
        .overlay_cmds()
        .iter()
        .map(|cmd| slash_row_text(cmd, 11))
        .collect();
    assert_eq!(
        rows,
        vec![
            "  just now     1 turn     add a health check",
            "  3 hours ago  1 turn     fix the flaky login test",
        ]
    );
    let drawn = drawn_text(&session);
    assert!(drawn.contains("fix the flaky login test"), "{drawn}");
    assert!(!drawn.contains("elsewhere"), "{drawn}");
    assert!(!drawn.contains("sess-"), "{drawn}");
    assert!(
        !drawn.contains(&session.cwd.display().to_string()),
        "{drawn}"
    );
}

#[tokio::test]
async fn resume_enter_loads_older_session() {
    let dir = tempdir().unwrap();
    let (session, older, newer) = two_cwd_sessions(&dir).await;
    assert_eq!(session.session_id(), newer);
    open_resume(&session);
    handle_key(&session, KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));
    match handle_key(&session, enter_key()) {
        KeyResult::LoadSession(id) => {
            assert_eq!(id, older);
            session.load_session(&id).await;
        }
        other => panic!("expected load, got {other:?}"),
    }
    assert_eq!(session.session_id(), older);
    assert!(session.lock_shared().resume.is_none());
    let text = session.transcript_text();
    assert!(text.contains("fix the flaky login test"), "{text}");
    assert!(!text.contains("add a health check"), "{text}");
}

/// #595: a session another window holds is not taken over. `--continue`
/// starts a new session and says why; `/resume` marks it and refuses it.
#[tokio::test]
async fn a_session_held_by_another_window_is_not_taken_over() {
    let dir = tempdir().unwrap();
    let holder = start_session(&dir, r#"[{"content":"ok"}]"#).await;
    holder.spawn_prompt("held prompt".into()).await.unwrap();
    holder.wait_prompt().await;
    let held = holder.session_id();

    let host = Arc::new(scripted_host(dir.path(), r#"[{"content":"ok"}]"#));
    let second = TuiSession::start(host, &dir.path().join("project"), true)
        .await
        .unwrap();
    assert_ne!(second.session_id(), held);
    assert_eq!(second.transcript_text(), CONTINUE_IN_USE);

    open_resume(&second);
    let rows: Vec<String> = second
        .overlay_cmds()
        .iter()
        .map(|cmd| slash_row_text(cmd, 11))
        .collect();
    assert_eq!(rows, vec!["  just now     1 turn     in use  held prompt"]);
    assert!(matches!(
        handle_key(&second, enter_key()),
        KeyResult::Continue
    ));
    assert_ne!(second.session_id(), held);
    assert!(
        second.transcript_text().ends_with(SESSION_IN_USE),
        "{}",
        second.transcript_text()
    );
}

#[tokio::test]
async fn resume_esc_keeps_current_session() {
    let dir = tempdir().unwrap();
    let (session, _, newer) = two_cwd_sessions(&dir).await;
    open_resume(&session);
    assert!(session.lock_shared().resume.is_some());
    assert!(matches!(
        handle_key(&session, KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE)),
        KeyResult::Continue
    ));
    assert!(session.lock_shared().resume.is_none());
    assert_eq!(session.session_id(), newer);
    assert!(session.transcript_text().contains("add a health check"));
    assert!(session.overlay_cmds().is_empty());
}

#[tokio::test]
async fn resume_unknown_id_stays_on_current_session() {
    let dir = tempdir().unwrap();
    let (session, _, newer) = two_cwd_sessions(&dir).await;
    session
        .lock_shared()
        .transcript
        .push(TranscriptLine::System("keep-me".into()));
    session.load_session("sess-missing").await;
    assert_eq!(session.session_id(), newer);
    let text = session.transcript_text();
    assert!(text.contains("keep-me"), "{text}");
    assert!(text.contains("unknown session"), "{text}");
    assert!(session.lock_shared().resume.is_none());
}

#[tokio::test]
async fn resume_empty_list_is_a_system_line() {
    let dir = tempdir().unwrap();
    let session = start_session(&dir, r#"[{"content":"ok"}]"#).await;
    std::fs::remove_dir_all(dir.path().join("state/acp-sessions")).unwrap();
    open_resume(&session);
    assert!(session.lock_shared().resume.is_none());
    assert!(session.overlay_cmds().is_empty());
    let text = session.transcript_text();
    assert!(text.contains("no sessions"), "{text}");
    assert!(!session.lock_shared().busy);
}
