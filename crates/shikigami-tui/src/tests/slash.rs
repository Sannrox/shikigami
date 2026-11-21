use super::support::*;
use crate::clipboard::{ClipboardPayload, ClipboardSource};
use crate::input::{handle_event, handle_key};
use crate::protocol::apply_prompt_result;
use crate::render::status_line;
use crate::session::*;
use crate::slash::*;

use std::sync::Arc;

use crossterm::event::{Event, KeyCode, KeyEvent, KeyModifiers};
use serde_json::json;
use shikigami::acp::{AcpClient, AcpHost};
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

async fn prompt_once(session: &TuiSession, text: &str) {
    session.spawn_prompt(text.into()).await.unwrap();
    let stop = session.wait_prompt().await;
    apply_prompt_result(&mut session.lock_shared(), stop.as_ref());
}

fn run_context(session: &TuiSession) -> String {
    session.lock_shared().transcript.clear();
    type_text(session, "/context");
    assert!(matches!(
        handle_key(session, enter_key()),
        KeyResult::Continue
    ));
    last_system(session)
}

/// #685: `/context` names the rules file and catalog skill the run attached.
#[tokio::test]
async fn slash_context_lists_attached_rules_and_catalog_skill() {
    let dir = tempdir().unwrap();
    let session = start_session_with(&dir, r#"[{"content":"ok"}]"#, |config| {
        config.context.skills = vec!["demo".into()];
    })
    .await;
    let project = dir.path().join("project");
    std::fs::write(project.join("AGENTS.md"), "prefer tests\n").unwrap();
    let skill = project.join(".shikigami/skills/demo");
    std::fs::create_dir_all(&skill).unwrap();
    std::fs::write(
        skill.join("SKILL.md"),
        "---\ndescription: demo pack\n---\nbody\n",
    )
    .unwrap();

    prompt_once(&session, "hello").await;
    let text = run_context(&session);
    assert!(text.contains("AGENTS.md"), "{text}");
    assert!(!text.contains("prefer tests"), "{text}");
    assert!(text.contains("catalog: demo"), "{text}");
    assert!(!text.contains("eager:"), "{text}");
    assert!(text.contains("nested  off"), "{text}");
    assert!(text.contains("compact  none"), "{text}");
    let rows = drawn_rows(&session, 80, 24);
    assert_screen_rules(&rows, 80);
    let narrow = drawn_rows(&session, 40, 12);
    assert_screen_rules(&narrow, 40);
}

/// #685: a run that loaded no rules file says so.
#[tokio::test]
async fn slash_context_says_no_project_rules_when_none_loaded() {
    let dir = tempdir().unwrap();
    let session = start_session(&dir, r#"[{"content":"ok"}]"#).await;
    type_text(&session, "/context");
    handle_key(&session, enter_key());
    assert!(
        last_system(&session).contains("No attached context yet"),
        "{}",
        last_system(&session)
    );

    prompt_once(&session, "hello").await;
    let text = run_context(&session);
    assert!(text.contains("no project rules"), "{text}");
    assert!(text.contains("skills  none"), "{text}");
    assert!(text.contains("compact  none"), "{text}");
    assert_screen_rules(&drawn_rows(&session, 80, 24), 80);
}

/// #685: `/context` reports compact after `/compact` shrinks history.
#[tokio::test]
async fn slash_context_reports_compact_after_compact() {
    let dir = tempdir().unwrap();
    let session = start_session_with(
        &dir,
        r#"[{"content":"a"},{"content":"b"},{"content":"c"},{"content":"d"}]"#,
        |config| {
            config.run.compact_keep_tail = 2;
        },
    )
    .await;
    for prompt in ["one", "two", "three", "four"] {
        prompt_once(&session, prompt).await;
    }
    session.compact().await.unwrap();
    let compacted = last_system(&session);
    assert!(compacted.contains("compacted  "), "{compacted}");
    let text = run_context(&session);
    assert!(
        text.contains("compact  ") && !text.contains("compact  none"),
        "{text}"
    );
    assert_screen_rules(&drawn_rows(&session, 80, 24), 80);
}

/// #701: the running slot says `compacting` while compact runs.
#[tokio::test]
async fn slash_compact_shows_compacting_in_the_running_slot() {
    let dir = tempdir().unwrap();
    let session = start_session(&dir, r#"[{"content":"ok"}]"#).await;
    session.lock_shared().busy = true;
    session
        .client
        .notify(
            "session/update",
            json!({
                "sessionId": session.session_id(),
                "update": { "_meta": { "shikigami": { "compacting": true } } }
            }),
        )
        .await
        .unwrap();
    let (row, dim) = slot_above_composer_rule(&session);
    assert!(row.contains(crate::protocol::COMPACTING_STATUS), "{row:?}");
    assert!(dim, "compacting marker must be dim: {row:?}");
    assert_screen_rules(&drawn_rows(&session, 80, 24), 80);
    assert_screen_rules(&drawn_rows(&session, 40, 12), 40);
}

/// #701: `/compact` with nothing to cut says so and does not stick on compacting.
#[tokio::test]
async fn slash_compact_nothing_to_cut_is_ready() {
    let dir = tempdir().unwrap();
    let session = start_session(&dir, r#"[{"content":"ok"}]"#).await;
    prompt_once(&session, "hello").await;
    session.compact().await.unwrap();
    assert_eq!(last_system(&session), "nothing to compact");
    assert_eq!(session.lock_shared().status, "ready");
    assert!(!session.lock_shared().busy);
    let (row, _) = slot_above_composer_rule(&session);
    assert!(
        !row.contains(crate::protocol::COMPACTING_STATUS),
        "nothing to compact must not stick: {row:?}"
    );
    assert_screen_rules(&drawn_rows(&session, 80, 24), 80);
}

async fn run_plan(session: &TuiSession) -> String {
    session.lock_shared().transcript.clear();
    type_text(session, "/plan");
    assert!(matches!(handle_key(session, enter_key()), KeyResult::Plan));
    session.plan().await;
    last_system(session)
}

/// #686: `/plan` on an idle session flags the next prompt for plan write-jail.
#[tokio::test]
async fn slash_plan_flags_an_idle_session() {
    let dir = tempdir().unwrap();
    let session = start_session(&dir, r#"[{"content":"ok"}]"#).await;
    let text = run_plan(&session).await;
    assert!(text.contains(".shikigami/plan.md"), "{text}");
    assert!(!text.contains("plan_jail"), "{text}");
    let again = run_plan(&session).await;
    assert!(again.contains("already in plan write-jail"), "{again}");
    assert_screen_rules(&drawn_rows(&session, 80, 24), 80);
    assert_screen_rules(&drawn_rows(&session, 40, 12), 40);
}

/// #686: `/plan` after a run that started without the jail is refused.
#[tokio::test]
async fn slash_plan_refuses_after_a_run_without_the_jail() {
    let dir = tempdir().unwrap();
    let session = start_session(&dir, r#"[{"content":"ok"}]"#).await;
    prompt_once(&session, "hello").await;
    let text = run_plan(&session).await;
    assert!(text.contains("already ran without the jail"), "{text}");
    assert!(!text.contains("plan_jail"), "{text}");
    assert_screen_rules(&drawn_rows(&session, 80, 24), 80);
}

/// #686: `/plan` then a prompt writes only the plan path until Accept.
#[tokio::test]
async fn slash_plan_then_prompt_parks_as_plan_and_accept_writes() {
    let dir = tempdir().unwrap();
    let write = |path: &str, content: &str| {
        json!({
            "tool_calls": [{
                "name": "write_file",
                "args_json": json!({"path": path, "content": content}).to_string()
            }]
        })
    };
    let script = json!([
        write("other.txt", "no\n"),
        write(".shikigami/plan.md", "# do it\n"),
        {"tool_calls":[{"name":"report","args_json": json!({"summary":"planned","success":true}).to_string()}]},
        write("ok.txt", "yes\n"),
        {"content": "executed"}
    ])
    .to_string();
    let session = start_session(&dir, &script).await;
    let cwd = dir.path().join("project");
    let text = run_plan(&session).await;
    assert!(text.contains(".shikigami/plan.md"), "{text}");

    session
        .spawn_prompt("draft the change".into())
        .await
        .unwrap();
    wait_until(|| session.overlay_text().is_some()).await;
    let overlay = session.overlay_text().expect("plan overlay");
    assert!(overlay.contains("y allow"), "overlay={overlay}");
    assert!(
        !overlay.contains("a allow for session"),
        "plan review has no session allow: {overlay}"
    );
    assert!(!cwd.join("other.txt").exists());
    assert_eq!(
        std::fs::read_to_string(cwd.join(".shikigami/plan.md")).unwrap(),
        "# do it\n"
    );
    session.answer_permission(true);
    wait_until(|| {
        session
            .overlay_text()
            .is_some_and(|text| text.contains("write_file") || text.contains("ok.txt"))
    })
    .await;
    session.answer_permission(true);
    let stop = session.wait_prompt().await;
    apply_prompt_result(&mut session.lock_shared(), stop.as_ref());
    assert_eq!(
        std::fs::read_to_string(cwd.join("ok.txt")).unwrap(),
        "yes\n"
    );
    assert!(!cwd.join("other.txt").exists());
    assert_screen_rules(&drawn_rows(&session, 80, 24), 80);
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
        last_system(&session).contains("demo  mcp.demo.echo mcp.demo.wide  disabled"),
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
    assert!(
        first.contains("mcp.demo.echo") && first.contains("  ok") && first.contains("first"),
        "{first}"
    );

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
        second.contains("mcp.demo.echo") && second.contains("  fail"),
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
        vec!["compact", "context", "copy"]
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
    assert!(text.contains("/context"), "{text}");
    assert!(text.contains("/plan"), "{text}");
    assert!(text.contains("/new"), "{text}");
    assert!(text.contains("/resume"), "{text}");
    assert!(text.contains("/run"), "{text}");
    assert!(text.contains("/copy"), "{text}");
    assert!(text.contains("/attach"), "{text}");
    assert!(text.contains("/paste"), "{text}");
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
            load_skill,
        } => {
            assert_eq!(send, "inspect");
            assert!(load_skill.is_none());
            assert!(display.contains("image/png"), "{display}");
            assert!(!display.contains("UNIQUE-TUI-401-IMAGE"), "{display}");
            assert_eq!(parts[0]["type"], "text");
            assert_eq!(parts[1]["type"], "image");
            session
                .spawn_prompt_with(display, send, parts, load_skill)
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

const PASTE_PNG: &[u8] = b"\x89PNG\r\n\x1a\n UNIQUE-TUI-702-IMAGE";

fn pin_clipboard(session: &TuiSession, payload: ClipboardPayload) {
    *session.clipboard.lock().unwrap_or_else(|e| e.into_inner()) = ClipboardSource::Fixed(payload);
}

fn workspace_has_bytes(cwd: &std::path::Path, needle: &[u8]) -> bool {
    std::fs::read_dir(cwd)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .any(|entry| {
            std::fs::read(entry.path())
                .ok()
                .is_some_and(|bytes| bytes.windows(needle.len()).any(|window| window == needle))
        })
}

#[tokio::test]
async fn paste_png_payload_stages_image_part() {
    let dir = tempdir().unwrap();
    let cwd = dir.path().join("project");
    std::fs::create_dir_all(&cwd).unwrap();
    let host = Arc::new(scripted_host(dir.path(), r#"[{"content":"saw-image"}]"#));
    let session = TuiSession::start(host, &cwd, false).await.unwrap();
    assert!(
        session
            .slash_catalog()
            .iter()
            .any(|cmd| cmd.name == "paste")
    );
    pin_clipboard(&session, ClipboardPayload::Image(PASTE_PNG.to_vec()));
    type_text(&session, "/paste");
    assert!(matches!(
        handle_key(&session, KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)),
        KeyResult::Continue
    ));
    let staged = session.lock_shared().pending_parts.clone();
    assert_eq!(staged.len(), 1, "{staged:?}");
    assert_eq!(staged[0]["type"], "image");
    assert_eq!(staged[0]["mimeType"], "image/png");
    assert!(
        last_system(&session).contains("attached clipboard.png"),
        "{}",
        last_system(&session)
    );
    let rows = drawn_rows(&session, 80, 24);
    assert_screen_rules(&rows, 80);
    assert!(rows.iter().any(|row| row.contains("attached")), "{rows:#?}");
    type_text(&session, "inspect");
    match handle_key(&session, KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)) {
        KeyResult::Prompt {
            display,
            send,
            parts,
            load_skill,
        } => {
            assert_eq!(send, "inspect");
            assert!(load_skill.is_none());
            assert!(display.contains("image/png"), "{display}");
            assert!(!display.contains("UNIQUE-TUI-702-IMAGE"), "{display}");
            assert_eq!(parts[0]["type"], "text");
            assert_eq!(parts[1]["type"], "image");
            session
                .spawn_prompt_with(display, send, parts, load_skill)
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
    assert!(
        !workspace_has_bytes(&cwd, PASTE_PNG),
        "attachment bytes copied into the workspace"
    );
}

#[tokio::test]
async fn paste_oversize_or_plain_text_does_not_stage() {
    let dir = tempdir().unwrap();
    let cwd = dir.path().join("project");
    std::fs::create_dir_all(&cwd).unwrap();
    let host = Arc::new(scripted_host(dir.path(), r#"[{"content":"unused"}]"#));
    let session = TuiSession::start(host, &cwd, false).await.unwrap();

    let mut huge = b"\x89PNG\r\n\x1a\n".to_vec();
    huge.resize(8 * 1024 * 1024 + 1, 0);
    session.apply_clipboard(ClipboardPayload::Image(huge));
    assert!(
        session.lock_shared().pending_parts.is_empty(),
        "{:?}",
        session.lock_shared().pending_parts
    );
    assert!(
        last_system(&session).contains("8MiB") && last_system(&session).contains("/attach"),
        "{}",
        last_system(&session)
    );

    session.apply_clipboard(ClipboardPayload::Text("hello from clipboard".into()));
    assert!(
        session.lock_shared().pending_parts.is_empty(),
        "{:?}",
        session.lock_shared().pending_parts
    );
    assert_eq!(session.lock_shared().input, "hello from clipboard");

    session.lock_shared().input.clear();
    session.apply_clipboard(ClipboardPayload::Empty);
    assert!(session.lock_shared().pending_parts.is_empty());
    assert!(
        last_system(&session).contains("/attach"),
        "{}",
        last_system(&session)
    );
}

#[tokio::test]
async fn bracketed_paste_of_image_path_stages_like_attach() {
    let dir = tempdir().unwrap();
    let cwd = dir.path().join("project");
    std::fs::create_dir_all(&cwd).unwrap();
    std::fs::write(cwd.join("shot.png"), PASTE_PNG).unwrap();
    let host = Arc::new(scripted_host(dir.path(), r#"[{"content":"ok"}]"#));
    let session = TuiSession::start(host, &cwd, false).await.unwrap();
    handle_event(&session, Event::Paste("shot.png".into()));
    let staged = session.lock_shared().pending_parts.clone();
    assert_eq!(staged.len(), 1, "{staged:?}");
    assert_eq!(staged[0]["type"], "image");
    assert!(last_system(&session).contains("attached shot.png"));
    assert!(session.lock_shared().input.is_empty());

    handle_event(&session, Event::Paste("plain note".into()));
    assert_eq!(session.lock_shared().input, "plain note");
    assert_eq!(session.lock_shared().pending_parts.len(), 1);
}

#[tokio::test]
async fn text_session_image_paste_fails_closed() {
    let dir = tempdir().unwrap();
    let cwd = dir.path().join("project");
    std::fs::create_dir_all(&cwd).unwrap();
    let host = Arc::new(scripted_host(dir.path(), r#"[{"content":"first"}]"#));
    let session = TuiSession::start(host, &cwd, false).await.unwrap();
    session.spawn_prompt("hello".into()).await.unwrap();
    let stop = session.wait_prompt().await;
    apply_prompt_result(&mut session.lock_shared(), stop.as_ref());

    type_text(&session, "inspect");
    session.apply_clipboard(ClipboardPayload::Image(PASTE_PNG.to_vec()));
    assert_eq!(session.lock_shared().input, "inspect", "draft stays");
    assert_eq!(session.lock_shared().pending_parts.len(), 1);

    match handle_key(&session, KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)) {
        KeyResult::Prompt {
            display,
            send,
            parts,
            load_skill,
        } => {
            assert_eq!(send, "inspect");
            assert_eq!(parts[1]["type"], "image");
            session
                .spawn_prompt_with(display, send, parts, load_skill)
                .await
                .unwrap();
            let _ = session.wait_prompt().await;
            let err = last_system(&session);
            assert!(
                err.contains("attachments") || err.contains("new session"),
                "{err}"
            );
        }
        other => panic!("expected prompt, got {other:?}"),
    }
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
        KeyResult::Prompt {
            display,
            send,
            load_skill,
            ..
        } => {
            assert_eq!(display, "/demo");
            assert_eq!(send, "follow skill `demo`");
            assert_eq!(load_skill.as_deref(), Some("demo"));
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
            load_skill,
        } => {
            assert_eq!(display, "/demo extra");
            assert_eq!(send, "extra");
            assert_eq!(load_skill.as_deref(), Some("demo"));
            assert_eq!(parts.len(), 1);
        }
        other => panic!("expected skill prompt, got {other:?}"),
    }
    assert!(session.lock_shared().input.is_empty());

    type_text(&session, "/skill:demo");
    match handle_key(&session, enter_key()) {
        KeyResult::Prompt {
            display,
            send,
            load_skill,
            ..
        } => {
            assert_eq!(display, "/skill:demo");
            assert_eq!(send, "follow skill `demo`");
            assert_eq!(load_skill.as_deref(), Some("demo"));
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
        KeyResult::Prompt {
            display,
            send,
            load_skill,
            ..
        } => {
            assert_eq!(display, "/skill:help more");
            assert_eq!(send, "more");
            assert_eq!(load_skill.as_deref(), Some("help"));
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
    assert_eq!(
        slash_names(&session),
        vec!["compact", "context", "copy", "co"]
    );
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
async fn slash_skill_sends_short_prompt() {
    let dir = tempdir().unwrap();
    let cwd = dir.path().join("project");
    let skill_dir = cwd.join(".shikigami/skills/demo");
    std::fs::create_dir_all(&skill_dir).unwrap();
    std::fs::write(skill_dir.join("SKILL.md"), "prefer tests first\n").unwrap();
    let host = Arc::new(scripted_host(dir.path(), r#"[{"content":"ok"}]"#));
    let session = TuiSession::start(host, &cwd, false).await.unwrap();
    type_text(&session, "/skill:demo extra");
    match handle_key(&session, KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)) {
        KeyResult::Prompt {
            display,
            send,
            load_skill,
            ..
        } => {
            assert_eq!(display, "/skill:demo extra");
            assert_eq!(send, "extra");
            assert!(!send.contains("prefer tests first"), "{send}");
            assert_eq!(load_skill.as_deref(), Some("demo"));
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
            load_skill,
        } => {
            assert!(display.contains("/skill:demo"), "{display}");
            assert!(display.contains("image/png"), "{display}");
            assert_eq!(send, "follow skill `demo`");
            assert!(!send.contains("prefer tests first"), "{send}");
            assert_eq!(load_skill.as_deref(), Some("demo"));
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
async fn slash_skill_host_loads_pack_as_tool_result() {
    let dir = tempdir().unwrap();
    let cwd = dir.path().join("project");
    let skill_dir = cwd.join(".agents/skills/demo");
    std::fs::create_dir_all(&skill_dir).unwrap();
    std::fs::write(skill_dir.join("SKILL.md"), "UNIQUE-SKILL-BODY\n").unwrap();
    let host = Arc::new(scripted_host(
        dir.path(),
        r#"[{"requires_context":["UNIQUE-SKILL-BODY","extra"],"content":"skill-reply"}]"#,
    ));
    let session = TuiSession::start(host, &cwd, false).await.unwrap();
    type_text(&session, "/demo extra");
    match handle_key(&session, enter_key()) {
        KeyResult::Prompt {
            display,
            send,
            parts,
            load_skill,
        } => {
            assert_eq!(display, "/demo extra");
            assert_eq!(send, "extra");
            session
                .spawn_prompt_with(display, send, parts, load_skill)
                .await
                .unwrap();
            let stop = session.wait_prompt().await;
            assert_eq!(
                stop.as_ref().and_then(|v| v.pointer("/result/stopReason")),
                Some(&json!("end_turn"))
            );
        }
        other => panic!("expected skill prompt, got {other:?}"),
    }
    let text = session.transcript_text();
    assert!(text.contains("you  /demo extra"), "{text}");
    assert!(
        !text
            .lines()
            .any(|line| line.starts_with("you  ") && line.contains("UNIQUE-SKILL-BODY")),
        "{text}"
    );
    assert!(text.contains("load_skill"), "{text}");
    assert!(text.contains("skill-reply"), "{text}");
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
        KeyResult::Prompt {
            display,
            send,
            load_skill,
            ..
        } => {
            assert_eq!(display, "/skill:verify-change");
            assert_eq!(send, "follow skill `verify-change`");
            assert!(!send.contains("run make validate"), "{send}");
            assert_eq!(load_skill.as_deref(), Some("verify-change"));
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
    open_resume(&session).await;
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
    open_resume(&session).await;
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

    open_resume(&second).await;
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
    open_resume(&session).await;
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
    open_resume(&session).await;
    assert!(session.lock_shared().resume.is_none());
    assert!(session.overlay_cmds().is_empty());
    let text = session.transcript_text();
    assert!(text.contains("no sessions"), "{text}");
    assert!(!session.lock_shared().busy);
}
