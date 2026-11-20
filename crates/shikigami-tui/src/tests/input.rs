use super::support::*;
use crate::input::{handle_event, handle_key};
use crate::look::Look;
use crate::render::{format_tool, status_line};
use crate::session::*;
use crate::slash::*;
use shikigami::acp::AcpClient;

use std::sync::Arc;

use crossterm::event::{Event, KeyCode, KeyEvent, KeyModifiers, MouseEventKind};
use serde_json::json;
use tempfile::tempdir;

#[tokio::test]
async fn park_mutating_tool_renders_permission_overlay_then_resumes() {
    let dir = tempdir().unwrap();
    let cwd = dir.path().join("project");
    std::fs::create_dir_all(&cwd).unwrap();
    let host = Arc::new(scripted_host(
        dir.path(),
        r#"[
          {"tool_calls":[{"name":"write_file","args_json":"{\"path\":\"ok.txt\",\"content\":\"hi\\n\"}"}]},
          {"content":"wrote it"}
        ]"#,
    ));
    let session = TuiSession::start(host, &cwd, false).await.unwrap();
    session.spawn_prompt("write".into()).await.unwrap();
    wait_until(|| session.overlay_text().is_some()).await;
    let overlay = session.overlay_text().expect("permission overlay");
    assert!(
        overlay.contains("write_file") || overlay.contains("allow"),
        "overlay={overlay}"
    );
    assert!(overlay.contains("y allow"));
    assert!(
        overlay.contains("ok.txt"),
        "ask should show path field: {overlay}"
    );
    assert!(!overlay.contains("{\"path\""), "ask should not dump JSON");
    assert!(!cwd.join("ok.txt").exists());
    session.answer_permission(true);
    let stop = session.wait_prompt().await;
    assert_eq!(
        stop.as_ref().and_then(|v| v.pointer("/result/stopReason")),
        Some(&json!("end_turn"))
    );
    assert_eq!(std::fs::read_to_string(cwd.join("ok.txt")).unwrap(), "hi\n");
    assert!(session.transcript_text().contains("wrote it"));
}

/// #592: a host that dies at the permission prompt leaves the run parked;
/// the next TUI on the same state says so and marks the tool as not run.
#[tokio::test]
async fn rebound_interrupt_key_quits_and_is_shown() {
    let dir = tempdir().unwrap();
    let mut session = start_session(&dir, r#"[{"content":"ok"}]"#).await;
    let mut settings = shikigami::config::TuiSettings::default();
    settings.keys.insert("interrupt".into(), "ctrl+g".into());
    session.look = Look::from_settings(&settings, true).unwrap();
    let rows = drawn_rows(&session, 80, 24);
    assert_eq!(rows.last().unwrap(), "  ready  ^g quit  pgup/pgdn/wheel");
    let ctrl = |ch| KeyEvent::new(KeyCode::Char(ch), KeyModifiers::CONTROL);
    assert!(matches!(
        handle_key(&session, ctrl('c')),
        KeyResult::Continue
    ));
    assert!(matches!(handle_key(&session, ctrl('g')), KeyResult::Quit));
    assert!(help_text(&session.look.keys).contains("^g cancel/quit"));
}

#[tokio::test]
async fn cancel_after_spawn_is_observed() {
    let dir = tempdir().unwrap();
    let cwd = dir.path().join("project");
    std::fs::create_dir_all(&cwd).unwrap();
    let host = Arc::new(scripted_host(
        dir.path(),
        r#"[
          {"tool_calls":[{"name":"write_file","args_json":"{\"path\":\"ok.txt\",\"content\":\"hi\\n\"}"}]},
          {"content":"wrote it"}
        ]"#,
    ));
    let session = TuiSession::start(host, &cwd, false).await.unwrap();
    session.spawn_prompt("write".into()).await.unwrap();
    session.cancel();
    let stop = session.wait_prompt().await;
    assert_eq!(
        stop.as_ref().and_then(|v| v.pointer("/result/stopReason")),
        Some(&json!("cancelled"))
    );
    assert!(!cwd.join("ok.txt").exists());
}

#[tokio::test]
async fn enter_while_busy_queues_follow_up_without_second_prompt() {
    let dir = tempdir().unwrap();
    let session = start_session(&dir, r#"[{"content":"ok"}]"#).await;
    spawn_then_queue(&session, "follow up").await;
    assert_eq!(session.lock_shared().input, "");
    assert_eq!(session.prompt_text(), "> ");
    assert_eq!(queued_display(&session).as_deref(), Some("follow up"));
    assert_eq!(user_prompts(&session), vec!["write".to_string()]);
    assert!(
        session
            .prompt
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_some()
    );
    let footer = status_line(
        &session.model,
        &session.look.keys,
        &session.lock_shared(),
        80,
    );
    assert!(footer.contains("queued  follow up"), "{footer}");
    let err = session.spawn_prompt("too soon".into()).await.unwrap_err();
    assert!(err.contains("already in flight"), "{err}");
    let _ = session.wait_prompt().await;
}

#[tokio::test]
async fn enter_while_busy_replaces_queued_follow_up() {
    let dir = tempdir().unwrap();
    let session = start_session(&dir, r#"[{"content":"ok"}]"#).await;
    spawn_then_queue(&session, "first").await;
    type_text(&session, "second");
    assert!(matches!(
        handle_key(&session, enter_key()),
        KeyResult::Continue
    ));
    assert_eq!(queued_display(&session).as_deref(), Some("second"));
    assert_eq!(session.lock_shared().input, "");
    assert_eq!(user_prompts(&session), vec!["write".to_string()]);
    let footer = status_line(
        &session.model,
        &session.look.keys,
        &session.lock_shared(),
        80,
    );
    assert!(footer.contains("queued  second"), "{footer}");
    assert!(!footer.contains("queued  first"), "{footer}");
    let _ = session.wait_prompt().await;
}

#[tokio::test]
async fn esc_drops_queued_follow_up() {
    let dir = tempdir().unwrap();
    let session = start_session(&dir, r#"[{"content":"ok"}]"#).await;
    spawn_then_queue(&session, "follow up").await;
    assert_eq!(queued_display(&session).as_deref(), Some("follow up"));
    assert!(matches!(
        handle_key(&session, KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE)),
        KeyResult::Continue
    ));
    assert!(queued_display(&session).is_none());
    let footer = status_line(
        &session.model,
        &session.look.keys,
        &session.lock_shared(),
        80,
    );
    assert!(!footer.contains("queued"), "{footer}");
    assert_eq!(user_prompts(&session), vec!["write".to_string()]);
    let _ = session.wait_prompt().await;
}

#[tokio::test]
async fn esc_hides_plan_before_dropping_queue() {
    let dir = tempdir().unwrap();
    let session = start_session(&dir, r#"[{"content":"ok"}]"#).await;
    spawn_then_queue(&session, "follow up").await;
    {
        let mut shared = session.lock_shared();
        shared.plan = Some(Plan {
            entries: vec![PlanEntry {
                content: "inspect".into(),
                status: "pending".into(),
            }],
        });
        shared.show_plan = true;
    }
    let esc = KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE);
    assert!(matches!(handle_key(&session, esc), KeyResult::Continue));
    assert!(!session.lock_shared().show_plan);
    assert_eq!(queued_display(&session).as_deref(), Some("follow up"));
    assert!(matches!(handle_key(&session, esc), KeyResult::Continue));
    assert!(queued_display(&session).is_none());
    let _ = session.wait_prompt().await;
}

#[tokio::test]
async fn finish_drains_queued_follow_up_into_session_prompt() {
    let dir = tempdir().unwrap();
    let session = start_session(
        &dir,
        r#"[{"content":"first reply"},{"content":"queued reply"}]"#,
    )
    .await;
    spawn_then_queue(&session, "follow up").await;
    let first = session.wait_prompt().await;
    assert_eq!(
        first.as_ref().and_then(|v| v.pointer("/result/stopReason")),
        Some(&json!("end_turn"))
    );
    session.drain_queued_prompt(first.as_ref()).await.unwrap();
    let second = session.wait_prompt().await;
    assert_eq!(
        second
            .as_ref()
            .and_then(|v| v.pointer("/result/stopReason")),
        Some(&json!("end_turn"))
    );
    assert!(queued_display(&session).is_none());
    assert_eq!(
        user_prompts(&session),
        vec!["write".to_string(), "follow up".to_string()]
    );
    let text = session.transcript_text();
    assert!(text.contains("first reply"), "{text}");
    assert!(text.contains("queued reply"), "{text}");
}

#[tokio::test]
async fn cancel_drops_queued_follow_up_without_sending() {
    let dir = tempdir().unwrap();
    let session = start_session(&dir, r#"[{"content":"ok"}]"#).await;
    spawn_then_queue(&session, "follow up").await;
    let ctrl_c = KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL);
    assert!(matches!(handle_key(&session, ctrl_c), KeyResult::Continue));
    assert!(queued_display(&session).is_none());
    let stop = session.wait_prompt().await;
    session.drain_queued_prompt(stop.as_ref()).await.unwrap();
    assert!(queued_display(&session).is_none());
    assert_eq!(user_prompts(&session), vec!["write".to_string()]);
    assert!(
        session
            .prompt
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_none()
    );
    assert!(!session.transcript_text().contains("you  follow up"));
}

#[tokio::test]
async fn slash_while_busy_is_a_character_not_a_list() {
    let dir = tempdir().unwrap();
    let session = start_session(&dir, r#"[{"content":"ok"}]"#).await;
    session.spawn_prompt("write".into()).await.unwrap();
    type_text(&session, "/compact");
    assert_eq!(session.lock_shared().input, "/compact");
    assert!(!slash_visible(&session.lock_shared()));
    assert!(session.slash_matches().is_empty());
    let _ = session.wait_prompt().await;
}

#[tokio::test]
async fn slash_command_while_busy_stays_in_composer() {
    let dir = tempdir().unwrap();
    let session = start_session(&dir, r#"[{"content":"ok"}]"#).await;
    session.spawn_prompt("write".into()).await.unwrap();
    type_text(&session, "/usage");
    assert!(matches!(
        handle_key(&session, enter_key()),
        KeyResult::Continue
    ));
    assert!(queued_display(&session).is_none());
    assert_eq!(session.lock_shared().input, "/usage");
    assert_eq!(last_system(&session), "/usage runs when this turn ends.");
    assert_eq!(user_prompts(&session), vec!["write".to_string()]);
    let screen = drawn_text(&session);
    assert!(
        screen.contains("/usage runs when this turn ends."),
        "{screen}"
    );
    assert!(screen.contains("> /usage"), "{screen}");
    assert!(!screen.contains("queued"), "{screen}");
    let narrow = drawn_rows(&session, 40, 12).join("\n");
    assert!(
        narrow.contains("/usage runs when this turn ends."),
        "{narrow}"
    );
    assert!(narrow.contains("/usage"), "{narrow}");
    let first = session.wait_prompt().await;
    session.drain_queued_prompt(first.as_ref()).await.unwrap();
    assert!(queued_display(&session).is_none());
    assert!(
        session
            .prompt
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_none()
    );
    assert_eq!(user_prompts(&session), vec!["write".to_string()]);
    assert!(!session.transcript_text().contains("you  /usage"));
    assert!(!session.lock_shared().busy);
    assert!(matches!(
        handle_key(&session, enter_key()),
        KeyResult::Continue
    ));
    assert_eq!(session.lock_shared().input, "");
    let table = last_system(&session);
    assert!(table.contains("turn"), "{table}");
    assert!(table.contains("in"), "{table}");
}

#[tokio::test]
async fn escaped_slash_while_busy_still_queues() {
    let dir = tempdir().unwrap();
    let session = start_session(
        &dir,
        r#"[{"content":"first reply"},{"content":"queued reply"}]"#,
    )
    .await;
    spawn_then_queue(&session, "//usage as text").await;
    assert_eq!(queued_display(&session).as_deref(), Some("//usage as text"));
    assert_eq!(session.lock_shared().input, "");
    let first = session.wait_prompt().await;
    session.drain_queued_prompt(first.as_ref()).await.unwrap();
    let _ = session.wait_prompt().await;
    assert_eq!(
        user_prompts(&session),
        vec!["write".to_string(), "//usage as text".to_string()]
    );
}

#[tokio::test]
async fn letter_p_types_when_a_plan_exists() {
    let dir = tempdir().unwrap();
    let cwd = dir.path().join("project");
    std::fs::create_dir_all(&cwd).unwrap();
    let host = Arc::new(scripted_host(dir.path(), r#"[{"content":"ok"}]"#));
    let session = TuiSession::start(host, &cwd, false).await.unwrap();
    session
        .client
        .notify(
            "session/update",
            json!({
                "sessionId": session.session_id(),
                "update": {
                    "sessionUpdate": "plan",
                    "entries": [{"content": "inspect the jail", "status": "pending"}]
                }
            }),
        )
        .await
        .unwrap();
    let key = KeyEvent::new(KeyCode::Char('p'), KeyModifiers::NONE);
    assert!(matches!(handle_key(&session, key), KeyResult::Continue));
    assert_eq!(session.lock_shared().input, "p");
}

#[tokio::test]
async fn page_up_scrolls_back_and_new_prompt_returns_to_tail() {
    let dir = tempdir().unwrap();
    let cwd = dir.path().join("project");
    std::fs::create_dir_all(&cwd).unwrap();
    let host = Arc::new(scripted_host(dir.path(), r#"[{"content":"ok"}]"#));
    let session = TuiSession::start(host, &cwd, false).await.unwrap();
    session.lock_shared().transcript_h = 10;
    let key = KeyEvent::new(KeyCode::PageUp, KeyModifiers::NONE);
    assert!(matches!(handle_key(&session, key), KeyResult::Continue));
    assert_eq!(session.lock_shared().scroll_back, 10);
    session.spawn_prompt("next".into()).await.unwrap();
    assert_eq!(session.lock_shared().scroll_back, 0);
    let _ = session.wait_prompt().await;
}

#[tokio::test]
async fn mouse_scroll_pages_transcript_like_page_keys() {
    let dir = tempdir().unwrap();
    let session = start_session(&dir, r#"[{"content":"ok"}]"#).await;
    session.lock_shared().transcript_h = 10;
    assert!(matches!(
        handle_event(&session, wheel(MouseEventKind::ScrollUp, 0, 0)),
        KeyResult::Continue
    ));
    assert_eq!(session.lock_shared().scroll_back, 10);
    assert!(matches!(
        handle_event(&session, wheel(MouseEventKind::ScrollDown, 0, 0)),
        KeyResult::Continue
    ));
    assert_eq!(session.lock_shared().scroll_back, 0);
}

#[tokio::test]
async fn ctrl_jk_scrolls_transcript_one_row() {
    let dir = tempdir().unwrap();
    let session = start_session(&dir, r#"[{"content":"ok"}]"#).await;
    session.lock_shared().transcript_h = 10;
    type_text(&session, "hello");
    assert!(matches!(
        handle_key(&session, ctrl_char('k')),
        KeyResult::Continue
    ));
    assert_eq!(session.lock_shared().scroll_back, 1);
    assert!(matches!(
        handle_key(&session, ctrl_char('k')),
        KeyResult::Continue
    ));
    assert_eq!(session.lock_shared().scroll_back, 2);
    assert!(matches!(
        handle_key(&session, ctrl_char('j')),
        KeyResult::Continue
    ));
    assert_eq!(session.lock_shared().scroll_back, 1);
    assert_eq!(session.lock_shared().input, "hello");
    assert!(matches!(
        handle_key(&session, ctrl_char('j')),
        KeyResult::Continue
    ));
    assert_eq!(session.lock_shared().scroll_back, 0);
    assert!(matches!(
        handle_key(&session, ctrl_char('j')),
        KeyResult::Continue
    ));
    assert_eq!(session.lock_shared().scroll_back, 0);
    assert_eq!(session.lock_shared().input, "hello");
    assert!(matches!(
        handle_key(
            &session,
            KeyEvent::new(KeyCode::Char('\n'), KeyModifiers::CONTROL)
        ),
        KeyResult::Continue
    ));
    assert_eq!(session.lock_shared().input, "hello");
    assert_eq!(session.lock_shared().scroll_back, 0);
    assert!(matches!(
        handle_key(
            &session,
            KeyEvent::new(KeyCode::Char('\n'), KeyModifiers::NONE)
        ),
        KeyResult::Continue
    ));
    assert_eq!(session.lock_shared().input, "hello");
    assert!(matches!(
        handle_key(&session, KeyEvent::new(KeyCode::PageUp, KeyModifiers::NONE)),
        KeyResult::Continue
    ));
    assert_eq!(session.lock_shared().scroll_back, 10);
    session.spawn_prompt("next".into()).await.unwrap();
    assert_eq!(session.lock_shared().scroll_back, 0);
    let _ = session.wait_prompt().await;
}

#[tokio::test]
async fn permission_dock_stays_answerable_when_small() {
    let dir = tempdir().unwrap();
    let session = start_session(&dir, r#"[{"content":"ok"}]"#).await;
    install_permission(&session, r#"{"path":"notes/todo.txt"}"#.into());
    for (width, height) in [(80, 24), (40, 12), (20, 8)] {
        session.lock_shared().dock_scroll = 0;
        let mut seen = String::new();
        // Page the dock until it stops moving; both answers must show on the way.
        for _ in 0..8 {
            let rows = drawn_rows(&session, width, height);
            assert_screen_rules(&rows, width);
            seen.push_str(&rows.join("\n"));
            let before = session.lock_shared().dock_scroll;
            handle_event(&session, Event::Key(page_key(KeyCode::PageDown)));
            if session.lock_shared().dock_scroll == before {
                break;
            }
        }
        for hint in ["y allow", "n deny"] {
            assert!(
                seen.contains(hint),
                "{hint} unreachable at {width}x{height}:\n{seen}"
            );
        }
    }
    arm_permission_answers(&session);
    assert!(matches!(
        handle_key(&session, char_key('n')),
        KeyResult::Continue
    ));
    assert!(session.lock_shared().permission.is_none(), "n must answer");
}

/// #646: a letter meant as text must not grant a tool call. Allowing
/// fails closed until the dock has been shown and typing has paused.
#[tokio::test]
async fn letters_typed_as_text_do_not_allow_a_permission() {
    let dir = tempdir().unwrap();
    let session = start_session(&dir, r#"[{"content":"ok"}]"#).await;
    install_permission(&session, r#"{"path":"ok.txt"}"#.into());
    session.lock_shared().note_input();

    for ch in ['y', 'Y', 'a', 'A', '1'] {
        assert!(
            matches!(handle_key(&session, char_key(ch)), KeyResult::Continue),
            "{ch} before the dock is drawn"
        );
        assert!(
            session.lock_shared().permission.is_some(),
            "{ch} must not allow before draw"
        );
    }

    let rows = drawn_rows(&session, 80, 24);
    assert_screen_rules(&rows, 80);
    assert!(rows.join("\n").contains("y allow"), "{}", rows.join("\n"));
    session.lock_shared().note_input();
    assert!(matches!(
        handle_key(&session, char_key('y')),
        KeyResult::Continue
    ));
    assert!(
        session.lock_shared().permission.is_some(),
        "a letter typed straight after the dock must not allow"
    );

    session.lock_shared().mark_input_idle();
    assert!(matches!(
        handle_key(&session, char_key('y')),
        KeyResult::Continue
    ));
    assert!(
        session.lock_shared().permission.is_none(),
        "a deliberate y after the prompt is on screen still allows"
    );
}

/// #646: deny and Esc still work once the prompt is ready; unarmed deny
/// leaves the request open (fail closed).
#[tokio::test]
async fn unarmed_permission_keys_leave_the_request_open() {
    let dir = tempdir().unwrap();
    let session = start_session(&dir, r#"[{"content":"ok"}]"#).await;
    install_permission(&session, r#"{"path":"ok.txt"}"#.into());
    let _ = drawn_text(&session);
    session.lock_shared().note_input();
    assert!(matches!(
        handle_key(&session, char_key('n')),
        KeyResult::Continue
    ));
    assert!(session.lock_shared().permission.is_some());

    assert!(matches!(
        handle_key(&session, KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE)),
        KeyResult::Continue
    ));
    assert!(
        session.lock_shared().permission.is_none(),
        "Esc still cancels while unarmed"
    );

    install_permission(&session, r#"{"path":"ok.txt"}"#.into());
    let _ = drawn_text(&session);
    arm_permission_answers(&session);
    assert!(matches!(
        handle_key(&session, char_key('n')),
        KeyResult::Continue
    ));
    assert!(session.lock_shared().permission.is_none(), "n must deny");
}

/// A turn cancelled at the permission prompt, as the whole screen shows it.
#[tokio::test]
async fn ctrl_o_expands_last_tool_and_collapses() {
    let dir = tempdir().unwrap();
    let cwd = dir.path().join("project");
    std::fs::create_dir_all(&cwd).unwrap();
    let host = Arc::new(scripted_host(dir.path(), r#"[{"content":"ok"}]"#));
    let session = TuiSession::start(host, &cwd, false).await.unwrap();
    notify_multiline_tool(&session).await;
    type_text(&session, "draft");
    assert!(matches!(
        handle_key(&session, ctrl_o()),
        KeyResult::Continue
    ));
    assert_eq!(session.lock_shared().input, "draft");
    let text = session.transcript_text();
    assert!(text.contains("later-unique-line"), "{text}");
    assert!(tool_line_count(&text) > 1, "{text}");
    let drawn = drawn_text(&session);
    assert!(drawn.contains("later-unique-line"), "{drawn}");
    match session.lock_shared().transcript.last() {
        Some(TranscriptLine::Tool(tool)) => {
            assert!(tool.expanded);
            assert!(tool.detail.contains("later-unique-line"));
            assert_eq!(format_tool(tool), text);
        }
        _ => panic!("expected tool line"),
    }
    assert!(matches!(
        handle_key(&session, ctrl_o()),
        KeyResult::Continue
    ));
    let text = session.transcript_text();
    assert_eq!(tool_line_count(&text), 1, "{text}");
    assert!(!text.contains("later-unique-line"), "{text}");
    assert!(!drawn_text(&session).contains("later-unique-line"));
    match session.lock_shared().transcript.last() {
        Some(TranscriptLine::Tool(tool)) => {
            assert!(!tool.expanded);
            assert!(tool.detail.contains("later-unique-line"));
        }
        _ => panic!("expected tool line"),
    }
    assert_eq!(session.lock_shared().input, "draft");
}

#[tokio::test]
async fn ctrl_o_without_tools_is_a_noop() {
    let dir = tempdir().unwrap();
    let cwd = dir.path().join("project");
    std::fs::create_dir_all(&cwd).unwrap();
    let host = Arc::new(scripted_host(dir.path(), r#"[{"content":"ok"}]"#));
    let session = TuiSession::start(host, &cwd, false).await.unwrap();
    type_text(&session, "hello");
    session.lock_shared().dirty = false;
    assert!(matches!(
        handle_key(&session, ctrl_o()),
        KeyResult::Continue
    ));
    assert_eq!(session.lock_shared().input, "hello");
    assert!(session.transcript_text().is_empty());
    assert!(!session.lock_shared().dirty);
}

#[tokio::test]
async fn asking_still_pages_transcript() {
    let dir = tempdir().unwrap();
    let cwd = dir.path().join("project");
    std::fs::create_dir_all(&cwd).unwrap();
    let host = Arc::new(scripted_host(dir.path(), r#"[{"content":"ok"}]"#));
    let session = TuiSession::start(host, &cwd, false).await.unwrap();
    install_permission(&session, r#"{"path":"ok.txt"}"#.into());
    let _ = drawn_text(&session);
    assert_eq!(session.lock_shared().dock_max_scroll(), 0);
    session.lock_shared().transcript_h = 10;
    assert!(matches!(
        handle_key(&session, page_key(KeyCode::PageUp)),
        KeyResult::Continue
    ));
    assert_eq!(session.lock_shared().scroll_back, 10);
    assert_eq!(session.lock_shared().dock_scroll, 0);
    assert!(session.lock_shared().permission.is_some());
}

#[tokio::test]
async fn overflowing_permission_body_is_reachable() {
    let dir = tempdir().unwrap();
    let session = start_session(&dir, r#"[{"content":"ok"}]"#).await;
    install_permission(&session, tall_permission_input());
    let overlay = session.overlay_text().expect("permission overlay");
    assert!(
        overlay.contains("later-unique-arg-line"),
        "ask must keep later argument lines: {overlay}"
    );
    let first = drawn_text(&session);
    assert!(session.lock_shared().dock_max_scroll() > 0);
    assert!(
        !first.contains("later-unique-arg-line"),
        "overflow must clip later arguments until scrolled: {first}"
    );
    let back = session.lock_shared().scroll_back;
    let paged = page_dock_down_until(&session, "later-unique-arg-line");
    assert!(
        paged.contains("later-unique-arg-line"),
        "PageDown on an overflowing dock must reveal later arguments: {paged}"
    );
    assert_eq!(session.lock_shared().scroll_back, back);
    assert!(session.lock_shared().dock_scroll > 0);
    assert!(matches!(
        handle_event(&session, Event::Key(page_key(KeyCode::PageUp))),
        KeyResult::Continue
    ));
    let reset = drawn_text(&session);
    assert!(
        !reset.contains("later-unique-arg-line"),
        "PageUp on a scrolled dock must hide later arguments again: {reset}"
    );
    let (x, y) = {
        let shared = session.lock_shared();
        (shared.composer_area.x, shared.composer_area.y)
    };
    let mut wheeled = reset;
    for _ in 0..8 {
        assert!(matches!(
            handle_event(&session, wheel(MouseEventKind::ScrollDown, x, y)),
            KeyResult::Continue
        ));
        wheeled = drawn_text(&session);
        if wheeled.contains("later-unique-arg-line") {
            break;
        }
    }
    assert!(
        wheeled.contains("later-unique-arg-line"),
        "wheel on the dock must reveal later arguments: {wheeled}"
    );
    assert_eq!(session.lock_shared().scroll_back, back);
    session.lock_shared().dock_scroll = 0;
    session.lock_shared().transcript_h = 10;
    session.lock_shared().scroll_back = 0;
    assert!(matches!(
        handle_event(&session, wheel(MouseEventKind::ScrollUp, 0, 0)),
        KeyResult::Continue
    ));
    assert_eq!(session.lock_shared().scroll_back, 10);
    assert_eq!(session.lock_shared().dock_scroll, 0);
}

#[tokio::test]
async fn cursor_inserts_in_the_middle() {
    let dir = tempdir().unwrap();
    let cwd = dir.path().join("project");
    std::fs::create_dir_all(&cwd).unwrap();
    let host = Arc::new(scripted_host(dir.path(), r#"[{"content":"ok"}]"#));
    let session = TuiSession::start(host, &cwd, false).await.unwrap();
    for ch in ['a', 'b', 'c'] {
        handle_key(
            &session,
            KeyEvent::new(KeyCode::Char(ch), KeyModifiers::NONE),
        );
    }
    handle_key(&session, KeyEvent::new(KeyCode::Left, KeyModifiers::NONE));
    handle_key(
        &session,
        KeyEvent::new(KeyCode::Char('x'), KeyModifiers::NONE),
    );
    assert_eq!(session.lock_shared().input, "abxc");
    assert_eq!(session.lock_shared().cursor, 3);
}

#[tokio::test]
async fn up_recalls_previous_prompt() {
    let dir = tempdir().unwrap();
    let cwd = dir.path().join("project");
    std::fs::create_dir_all(&cwd).unwrap();
    let host = Arc::new(scripted_host(dir.path(), r#"[{"content":"ok"}]"#));
    let session = TuiSession::start(host, &cwd, false).await.unwrap();
    session.spawn_prompt("first".into()).await.unwrap();
    let _ = session.wait_prompt().await;
    handle_key(&session, KeyEvent::new(KeyCode::Up, KeyModifiers::NONE));
    assert_eq!(session.lock_shared().input, "first");
    handle_key(&session, KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));
    assert_eq!(session.lock_shared().input, "");
}

#[tokio::test]
async fn shift_enter_inserts_newline_without_sending() {
    let dir = tempdir().unwrap();
    let cwd = dir.path().join("project");
    std::fs::create_dir_all(&cwd).unwrap();
    let host = Arc::new(scripted_host(dir.path(), r#"[{"content":"ok"}]"#));
    let session = TuiSession::start(host, &cwd, false).await.unwrap();
    type_text(&session, "hi");
    let shift = KeyEvent::new(KeyCode::Enter, KeyModifiers::SHIFT);
    assert!(matches!(handle_key(&session, shift), KeyResult::Continue));
    assert_eq!(session.lock_shared().input, "hi\n");
    let alt = KeyEvent::new(KeyCode::Enter, KeyModifiers::ALT);
    assert!(matches!(handle_key(&session, alt), KeyResult::Continue));
    assert_eq!(session.lock_shared().input, "hi\n\n");
    type_text(&session, "there");
    match handle_key(&session, KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)) {
        KeyResult::Prompt { display, send, .. } => {
            assert_eq!(display, "hi\n\nthere");
            assert_eq!(send, "hi\n\nthere");
        }
        other => panic!("expected send, got {other:?}"),
    }
}

#[test]
fn paste_inserts_newlines() {
    let mut shared = Shared::new();
    shared.insert_str("error\r\n--> src/tui.rs\n");
    assert_eq!(shared.input, "error\n--> src/tui.rs\n");
    assert_eq!(shared.cursor, shared.input.chars().count());
    shared.insert_str("\tfn");
    assert_eq!(shared.input, "error\n--> src/tui.rs\n fn");
    let mut cr = Shared::new();
    cr.insert_str("first\rsecond");
    assert_eq!(cr.input, "first\nsecond");
}

#[tokio::test]
async fn arrows_move_inside_multiline_draft() {
    let dir = tempdir().unwrap();
    let cwd = dir.path().join("project");
    std::fs::create_dir_all(&cwd).unwrap();
    let host = Arc::new(scripted_host(dir.path(), r#"[{"content":"ok"}]"#));
    let session = TuiSession::start(host, &cwd, false).await.unwrap();
    type_text(&session, "ab");
    handle_key(&session, KeyEvent::new(KeyCode::Enter, KeyModifiers::SHIFT));
    type_text(&session, "cd");
    assert_eq!(session.lock_shared().input, "ab\ncd");
    assert_eq!(session.lock_shared().cursor, 5);
    handle_key(&session, KeyEvent::new(KeyCode::Up, KeyModifiers::NONE));
    assert_eq!(session.lock_shared().cursor, 2);
    handle_key(&session, KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));
    assert_eq!(session.lock_shared().cursor, 5);
    session.lock_shared().clear_input();
    session.spawn_prompt("first".into()).await.unwrap();
    let _ = session.wait_prompt().await;
    type_text(&session, "xy");
    handle_key(&session, KeyEvent::new(KeyCode::Up, KeyModifiers::NONE));
    assert_eq!(session.lock_shared().input, "first");
}
