use super::support::*;
use crate::look::KeyMap;
use crate::protocol::{apply_load_result, apply_prompt_result};
use crate::render::{
    ago, composer_height, follow_pad, format_tool, permission_fields, prompt_block, scroll_offset,
    status_line,
};
use crate::session::*;
use crate::slash::*;
use shikigami::acp::AcpClient;
use shikigami_types::terminal::terminal_text;

use std::sync::Arc;
use std::time::Duration;

use ratatui::prelude::Rect;
use serde_json::json;
use tempfile::tempdir;

#[test]
fn scroll_offset_counts_wrapped_rows() {
    let body = "a".repeat(40);
    let area = Rect::new(0, 0, 10, 2);
    assert_eq!(scroll_offset(&body, area), (2, 0));
    let short = Rect::new(0, 0, 80, 20);
    assert_eq!(scroll_offset("hello", short), (0, 0));
    let words = "123456 123456 123456";
    assert_eq!(scroll_offset(words, Rect::new(0, 0, 10, 2)), (1, 0));
}

#[test]
fn follow_pad_pins_short_transcript_to_the_composer() {
    assert_eq!(follow_pad(2, 20, 0), 18);
    assert_eq!(follow_pad(30, 20, 0), 0);
    assert_eq!(follow_pad(2, 20, 5), 0);
}

#[test]
fn prompt_error_lands_in_transcript_not_the_footer() {
    let mut shared = Shared::new();
    apply_prompt_result(
        &mut shared,
        Some(&json!({
            "error": { "message": "the state directory is inside the project /p; set --state or SHIKIGAMI_STATE to a directory outside it" }
        })),
    );
    assert_eq!(shared.status, "error");
    assert!(shared.transcript.iter().any(
        |line| matches!(line, TranscriptLine::System(text) if text.contains("state directory"))
    ));
    let footer = status_line("claude-sonnet-5-5", &KeyMap::default(), &shared, 40);
    assert!(footer.contains("error"));
    assert!(!footer.contains("state directory"));
    assert!(footer.chars().count() <= 40);
}

/// #598: the footer names the model and, once a prompt was sent, the turn.
#[tokio::test]
async fn footer_shows_model_and_turn_not_session_id() {
    let dir = tempdir().unwrap();
    let mut session = start_session(&dir, r#"[{"content":"ok"}]"#).await;
    session.model = "claude-sonnet-5-5".into();
    let rows = drawn_rows(&session, 80, 24);
    assert_eq!(
        rows.last().unwrap(),
        "  ready  claude-sonnet-5-5  ^c quit  pgup/pgdn/wheel"
    );
    session.spawn_prompt("hi".into()).await.unwrap();
    let _ = session.wait_prompt().await;
    let rows = drawn_rows(&session, 80, 24);
    assert_eq!(
        rows.last().unwrap(),
        "  done  claude-sonnet-5-5  turn 1  ? tok  ^c quit  pgup/pgdn/wheel"
    );
    let id = session.session_id();
    let short = &id["sess-".len().."sess-".len() + 8];
    assert!(!rows.join("\n").contains(short));
}

/// #698: last-turn `input_tokens` as a percent of `[run] context_window_tokens`.
#[tokio::test]
async fn footer_shows_last_turn_fill_percent_when_window_is_set() {
    let dir = tempdir().unwrap();
    let mut session = start_session_with(
        &dir,
        r#"[{"content":"ok","usage":{"input_tokens":124000,"output_tokens":10}}]"#,
        |config| {
            // Large enough that standing context does not refuse the run.
            config.run.context_window_tokens = Some(200_000);
        },
    )
    .await;
    session.model = "claude-sonnet-5-5".into();
    session.spawn_prompt("hi".into()).await.unwrap();
    let _ = session.wait_prompt().await;
    let rows = drawn_rows(&session, 80, 24);
    assert_screen_rules(&rows, 80);
    let footer = rows.last().expect("footer");
    assert!(footer.contains("124k tok"), "{footer}");
    assert!(footer.contains("62%"), "{footer}");
    assert_eq!(
        footer,
        "  done  claude-sonnet-5-5  turn 1  124k tok  62%  ^c quit  pgup/pgdn/wheel"
    );
    let narrow = drawn_rows(&session, 40, 12);
    assert_screen_rules(&narrow, 40);
}

/// #698: no window, or a last turn with no usage, keeps the token part only.
#[tokio::test]
async fn footer_omits_fill_percent_without_window_or_last_turn_usage() {
    let dir = tempdir().unwrap();
    let mut counted = start_session(
        &dir,
        r#"[{"content":"ok","usage":{"input_tokens":620,"output_tokens":10}}]"#,
    )
    .await;
    counted.model = "claude-sonnet-5-5".into();
    counted.spawn_prompt("hi".into()).await.unwrap();
    let _ = counted.wait_prompt().await;
    let counted_footer = drawn_rows(&counted, 80, 24);
    assert_screen_rules(&counted_footer, 80);
    let counted_line = counted_footer.last().expect("footer");
    assert!(counted_line.contains("630 tok"), "{counted_line}");
    assert!(!counted_line.contains('%'), "{counted_line}");

    let unknown_dir = tempdir().unwrap();
    let mut unknown = start_session_with(&unknown_dir, r#"[{"content":"ok"}]"#, |config| {
        config.run.context_window_tokens = Some(200_000);
    })
    .await;
    unknown.model = "claude-sonnet-5-5".into();
    unknown.spawn_prompt("hi".into()).await.unwrap();
    let _ = unknown.wait_prompt().await;
    let unknown_footer = drawn_rows(&unknown, 80, 24);
    assert_screen_rules(&unknown_footer, 80);
    let unknown_line = unknown_footer.last().expect("footer");
    assert!(unknown_line.contains("? tok"), "{unknown_line}");
    assert!(!unknown_line.contains('%'), "{unknown_line}");
}

/// #603: a rebound key performs the action, the old key no longer does,
/// and the footer and help name the new key.
#[test]
fn ago_reads_as_words() {
    let mins = |m: u64| ago(Duration::from_secs(m * 60));
    assert_eq!(mins(0), "just now");
    assert_eq!(mins(1), "1 minute ago");
    assert_eq!(mins(59), "59 minutes ago");
    assert_eq!(mins(60), "1 hour ago");
    assert_eq!(mins(23 * 60), "23 hours ago");
    assert_eq!(mins(24 * 60), "yesterday");
    assert_eq!(mins(3 * 24 * 60), "3 days ago");
}

/// #617: the footer says the session state in words, never the wire stop reason.
#[test]
fn footer_shows_state_words_not_stop_reasons() {
    for (reason, word) in [
        ("end_turn", "done"),
        ("cancelled", "cancelled"),
        ("max_turn_requests", "turn limit"),
        ("max_tokens", "length limit"),
        ("refusal", "refused"),
        ("some_future_reason", "ready"),
    ] {
        let mut shared = Shared::new();
        apply_prompt_result(
            &mut shared,
            Some(&json!({"result": {"stopReason": reason}})),
        );
        let footer = status_line("claude-sonnet-5-5", &KeyMap::default(), &shared, 80);
        assert!(
            footer.starts_with(&format!("  {word}  ")),
            "{reason}: {footer}"
        );
        if reason != word {
            assert!(!footer.contains(reason), "{reason}: {footer}");
        }
    }
}

#[test]
fn composer_height_uses_wrapped_rows() {
    let args = "x".repeat(400);
    let text = format!("ask  write_file\n{args}\n> y allow\n  n deny");
    let area = Rect::new(0, 0, 20, 24);
    assert_eq!(composer_height(&text, area), 20);
    assert_eq!(
        composer_height("ask\nok\n> y allow\n  n deny", Rect::new(0, 0, 80, 24)),
        4
    );
    assert_eq!(
        composer_height(
            "ask\nok\n> y allow\n  a allow for session\n  n deny",
            Rect::new(0, 0, 80, 24)
        ),
        5
    );
}

#[tokio::test]
async fn idle_draw_has_no_running_row_above_composer() {
    let dir = tempdir().unwrap();
    let session = start_session(&dir, r#"[{"content":"ok"}]"#).await;
    assert!(!session.lock_shared().busy);
    let (row, _) = slot_above_composer_rule(&session);
    assert_ne!(row.trim(), RUNNING_MARKER, "{row:?}");
    let footer = status_line(
        &session.model,
        &session.look.keys,
        &session.lock_shared(),
        80,
    );
    assert!(!footer.contains(RUNNING_MARKER), "{footer}");
}

#[tokio::test]
async fn spawn_prompt_shows_dim_running_row_until_idle() {
    let dir = tempdir().unwrap();
    let session = start_session(&dir, r#"[{"content":"ok"}]"#).await;
    let (idle_row, _) = slot_above_composer_rule(&session);
    assert_ne!(idle_row.trim(), RUNNING_MARKER, "{idle_row:?}");

    session.spawn_prompt("write".into()).await.unwrap();
    assert!(session.lock_shared().busy);
    let (row, dim) = slot_above_composer_rule(&session);
    assert_eq!(row.trim(), RUNNING_MARKER, "{row:?}");
    assert!(dim, "running marker must be dim: {row:?}");

    let _ = session.wait_prompt().await;
    assert!(!session.lock_shared().busy);
    let (done_row, _) = slot_above_composer_rule(&session);
    assert_ne!(done_row.trim(), RUNNING_MARKER, "{done_row:?}");
}

#[tokio::test]
async fn running_marker_skips_idle_ticks() {
    let dir = tempdir().unwrap();
    let session = start_session(&dir, r#"[{"content":"ok"}]"#).await;
    assert!(draw_if_dirty(&session));
    for _ in 0..3 {
        assert!(!draw_if_dirty(&session));
    }

    session.spawn_prompt("write".into()).await.unwrap();
    wait_until(|| prompt_task_finished(&session)).await;
    assert!(session.lock_shared().busy);
    let mut frames = 0usize;
    for _ in 0..5 {
        if draw_if_dirty(&session) {
            frames += 1;
        }
    }
    assert_eq!(frames, 1, "busy marker must not force extra idle frames");
    assert!(!session.lock_shared().dirty);
    let (row, dim) = slot_above_composer_rule(&session);
    assert_eq!(row.trim(), RUNNING_MARKER, "{row:?}");
    assert!(dim, "running marker must be dim: {row:?}");

    let _ = session.wait_prompt().await;
    frames = 0;
    for _ in 0..5 {
        if draw_if_dirty(&session) {
            frames += 1;
        }
    }
    assert_eq!(frames, 1);
    assert!(!session.lock_shared().busy);
    let (done_row, _) = slot_above_composer_rule(&session);
    assert_ne!(done_row.trim(), RUNNING_MARKER, "{done_row:?}");
}

#[tokio::test]
async fn running_marker_reserves_composer_height() {
    let dir = tempdir().unwrap();
    let session = start_session(&dir, r#"[{"content":"ok"}]"#).await;
    session.spawn_prompt("write".into()).await.unwrap();
    session
        .lock_shared()
        .insert_str(&format!("{}\n", "line").repeat(30));
    let _ = drawn_text(&session);
    let composer_h = session.lock_shared().composer_h;
    assert!(
        composer_h <= 19,
        "busy overlay must stay in the 24-row budget: composer_h={composer_h}"
    );
    let (row, dim) = slot_above_composer_rule(&session);
    assert_eq!(row.trim(), RUNNING_MARKER, "{row:?}");
    assert!(dim, "running marker must be dim: {row:?}");
    let _ = session.wait_prompt().await;
}

#[tokio::test]
async fn nested_children_dim_block_lists_inflight_then_clears() {
    let dir = tempdir().unwrap();
    let session = start_session(&dir, r#"[{"content":"ok"}]"#).await;
    session
        .client
        .notify(
            "session/update",
            json!({
                "sessionId": session.session_id(),
                "update": {
                    "sessionUpdate": "tool_call",
                    "toolCallId": "c1",
                    "title": "child_run",
                    "status": "pending",
                    "rawInput": {"profile": "explore", "task": "scout-a", "wait": false}
                }
            }),
        )
        .await
        .unwrap();
    session
        .client
        .notify(
            "session/update",
            children_update(
                &session.session_id(),
                json!([
                    {"runId": "run-a", "profile": "explore", "status": "running"},
                    {"runId": "run-b", "profile": "explore", "status": "running"}
                ]),
            ),
        )
        .await
        .unwrap();

    for (width, height) in [(80, 24), (40, 12)] {
        let rows = drawn_rows(&session, width, height);
        assert_screen_rules(&rows, width);
        let overlay = overlay_rows_above_composer(&session, width, height);
        assert_eq!(overlay.len(), 2, "{overlay:?}");
        assert!(
            overlay.iter().all(|(_, dim)| *dim),
            "nested children must be dim: {overlay:?}"
        );
        let text = overlay
            .iter()
            .map(|(row, _)| row.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(text.contains("run-a"), "{text}");
        assert!(text.contains("run-b"), "{text}");
        assert!(text.contains("explore"), "{text}");
        assert!(
            overlay
                .iter()
                .any(|(row, _)| row.contains("running") && row.contains("run-a")),
            "{text}"
        );
        assert!(
            overlay.iter().all(|(row, _)| row.trim() != RUNNING_MARKER),
            "{text}"
        );
    }
    assert!(
        session.transcript_text().contains("child_run"),
        "{}",
        session.transcript_text()
    );

    session
        .client
        .notify(
            "session/update",
            children_update(
                &session.session_id(),
                json!([
                    {"runId": "run-a", "profile": "explore", "status": "done"},
                    {"runId": "run-b", "profile": "explore", "status": "failed"}
                ]),
            ),
        )
        .await
        .unwrap();
    let overlay = overlay_rows_above_composer(&session, 80, 24);
    let text = overlay
        .iter()
        .map(|(row, _)| row.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(!text.contains("run-a"), "{text}");
    assert!(!text.contains("run-b"), "{text}");
    assert_ne!(text.trim(), RUNNING_MARKER, "{text}");
}

#[tokio::test]
async fn nested_children_hide_when_slash_list_or_dock_is_open() {
    let dir = tempdir().unwrap();
    let session = start_session(&dir, r#"[{"content":"ok"}]"#).await;
    session
        .client
        .notify(
            "session/update",
            children_update(
                &session.session_id(),
                json!([{"runId": "run-a", "profile": "plan", "status": "parked"}]),
            ),
        )
        .await
        .unwrap();
    let overlay = overlay_rows_above_composer(&session, 80, 24);
    assert!(
        overlay
            .iter()
            .any(|(row, dim)| *dim && row.contains("run-a") && row.contains("parked")),
        "{overlay:?}"
    );

    type_text(&session, "/he");
    let overlay = overlay_rows_above_composer(&session, 80, 24);
    let text = overlay
        .iter()
        .map(|(row, _)| row.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(text.contains("help"), "{text}");
    assert!(!text.contains("run-a"), "{text}");

    session.lock_shared().clear_input();
    install_permission(&session, "{}".into());
    let overlay = overlay_rows_above_composer(&session, 80, 24);
    let text = overlay
        .iter()
        .map(|(row, _)| row.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(!text.contains("run-a"), "{text}");
}

#[tokio::test]
async fn nested_off_session_never_shows_child_block() {
    let dir = tempdir().unwrap();
    let session = start_session(&dir, r#"[{"content":"ok"}]"#).await;
    session.spawn_prompt("write".into()).await.unwrap();
    let (row, _) = slot_above_composer_rule(&session);
    assert_eq!(row.trim(), RUNNING_MARKER, "{row:?}");
    assert!(session.lock_shared().nested_children.is_empty());
    let _ = session.wait_prompt().await;
}

#[tokio::test]
async fn replayed_child_run_json_does_not_fill_nested_overlay() {
    let dir = tempdir().unwrap();
    let session = start_session(&dir, r#"[{"content":"ok"}]"#).await;
    session
        .client
        .notify(
            "session/update",
            json!({
                "sessionId": session.session_id(),
                "update": {
                    "sessionUpdate": "tool_call_update",
                    "toolCallId": "c1",
                    "status": "completed",
                    "content": [{
                        "type": "content",
                        "content": {
                            "type": "text",
                            "text": "{\"run_id\":\"run-a\",\"profile\":\"explore\",\"status\":\"running\"}"
                        }
                    }]
                }
            }),
        )
        .await
        .unwrap();
    let overlay = overlay_rows_above_composer(&session, 80, 24);
    let text = overlay
        .iter()
        .map(|(row, _)| row.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(!text.contains("run-a"), "{text}");
    assert!(session.lock_shared().nested_children.is_empty());
}

#[tokio::test]
async fn nested_children_update_for_another_session_is_ignored() {
    let dir = tempdir().unwrap();
    let session = start_session(&dir, r#"[{"content":"ok"}]"#).await;
    session
        .client
        .notify(
            "session/update",
            children_update(
                "other-session",
                json!([{"runId": "run-a", "profile": "explore", "status": "running"}]),
            ),
        )
        .await
        .unwrap();
    let overlay = overlay_rows_above_composer(&session, 80, 24);
    let text = overlay
        .iter()
        .map(|(row, _)| row.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(!text.contains("run-a"), "{text}");
    assert!(session.lock_shared().nested_children.is_empty());

    session
        .client
        .notify(
            "session/update",
            children_update(
                &session.session_id(),
                json!([{"runId": "run-a", "profile": "explore", "status": "running"}]),
            ),
        )
        .await
        .unwrap();
    let overlay = overlay_rows_above_composer(&session, 80, 24);
    assert!(
        overlay
            .iter()
            .any(|(row, dim)| *dim && row.contains("run-a") && row.contains("running")),
        "{overlay:?}"
    );
}

#[tokio::test]
async fn load_result_children_meta_fills_nested_overlay() {
    let dir = tempdir().unwrap();
    let session = start_session(&dir, r#"[{"content":"ok"}]"#).await;
    {
        let mut shared = session.lock_shared();
        apply_load_result(
            &mut shared,
            &json!({
                "_meta": {
                    "shikigami": {
                        "runId": "parent",
                        "children": [
                            {"runId": "run-a", "profile": "explore", "status": "running"}
                        ]
                    }
                }
            }),
        );
    }
    let overlay = overlay_rows_above_composer(&session, 80, 24);
    assert!(
        overlay
            .iter()
            .any(|(row, dim)| *dim && row.contains("run-a") && row.contains("explore")),
        "{overlay:?}"
    );
}

#[tokio::test]
async fn plan_update_renders_plan_overlay() {
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
                    "entries": [
                        {"content": "inspect the jail", "status": "pending"}
                    ]
                }
            }),
        )
        .await
        .unwrap();
    let overlay = session.overlay_text().expect("plan overlay");
    assert!(overlay.contains("inspect the jail"), "overlay={overlay}");
    assert!(overlay.contains("pending"), "overlay={overlay}");
}

#[tokio::test]
async fn tool_call_summarizes_path_from_raw_input() {
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
                    "sessionUpdate": "tool_call",
                    "toolCallId": "c1",
                    "title": "write_file",
                    "status": "pending",
                    "rawInput": {"path": "hello.txt", "content": "hi\n"}
                }
            }),
        )
        .await
        .unwrap();
    let text = session.transcript_text();
    assert!(text.contains("write_file"), "{text}");
    assert!(text.contains("hello.txt"), "{text}");
    assert!(!text.contains("{\"path\""), "{text}");
}

#[tokio::test]
async fn cancelled_turn_screen() {
    let dir = tempdir().unwrap();
    let session = start_session(
        &dir,
        r#"[{"tool_calls":[{"name":"write_file","args_json":"{\"path\":\"ok.txt\",\"content\":\"hi\\n\"}"}]}]"#,
    )
    .await;
    session
        .spawn_prompt("write the ok file".into())
        .await
        .unwrap();
    wait_until(|| session.overlay_text().is_some()).await;
    session.cancel();
    let stop = session.wait_prompt().await;
    apply_prompt_result(&mut session.lock_shared(), stop.as_ref());
    let rows = drawn_rows(&session, 80, 24);
    assert_screen_rules(&rows, 80);
    let screen = rows.join("\n");
    let rule = "─".repeat(80);
    let expected = format!(
        "{}you  write the ok file\n\n· write_file  ok.txt  cancelled\n{rule}\n>\n{rule}\n  cancelled  turn 1  ? tok  ^c quit  ^o tool  pgup/pgdn/wheel",
        "\n".repeat(17)
    );
    assert_eq!(screen, expected);
}

/// #625: a turn's final answer in a long `report` summary reads in full,
/// wrapped, without expanding tool lines.
#[tokio::test]
async fn long_report_summary_shows_in_full() {
    let dir = tempdir().unwrap();
    let summary = "The project has two files: README.md describes the tool and main.py \
                   holds the entry point. The glob and the write both ran, and the new \
                   file is in place.";
    let script = format!(
        r#"[{{"tool_calls":[{{"name":"report","args_json":"{{\"summary\":\"{summary}\",\"success\":true}}"}}]}}]"#
    );
    let session = start_session(&dir, &script).await;
    session.spawn_prompt("what is here?".into()).await.unwrap();
    session.wait_prompt().await;
    for (width, height) in [(80, 24), (40, 12)] {
        let rows = drawn_rows(&session, width, height);
        assert_screen_rules(&rows, width);
        let words = rows
            .join(" ")
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");
        assert!(
            words.contains(summary),
            "{width}x{height}:\n{}",
            rows.join("\n")
        );
    }
}

/// #616: returning to a session shows the outcomes the user saw live.
#[tokio::test]
async fn wide_and_long_lines_stay_inside_the_screen() {
    let dir = tempdir().unwrap();
    let session = start_session(&dir, r#"[{"content":"ok"}]"#).await;
    {
        let mut shared = session.lock_shared();
        shared.transcript = vec![
            TranscriptLine::User("雪の日 🌨️ を説明して".into()),
            TranscriptLine::Assistant("x".repeat(300)),
            TranscriptLine::Assistant("雪".repeat(70) + " 🎉🎉🎉 done"),
        ];
        shared.dirty = true;
    }
    // 39 columns is odd, so a wide character lands on the last column.
    for (width, height) in [(80, 24), (39, 30), (40, 12), (39, 12)] {
        let rows = drawn_rows(&session, width, height);
        assert_screen_rules(&rows, width);
        let screen = rows.join("\n");
        assert!(
            screen.contains("done"),
            "the newest line is in view: {screen}"
        );
        if height < 24 {
            continue;
        }
        // Tall enough for the whole transcript: wrapping keeps every character.
        let xs: usize = rows
            .iter()
            .filter(|row| !row.is_empty() && row.chars().all(|c| c == 'x'))
            .map(|row| row.len())
            .sum();
        assert_eq!(xs, 300, "{width}x{height}: {screen}");
        assert_eq!(
            screen.matches('雪').count(),
            71,
            "{width}x{height}: {screen}"
        );
        assert_eq!(
            screen.matches('🎉').count(),
            3,
            "{width}x{height}: {screen}"
        );
        assert!(screen.contains("🌨"), "{width}x{height}: {screen}");
    }
}

#[tokio::test]
async fn untrusted_output_controls_are_inert_on_redraw() {
    let dir = tempdir().unwrap();
    let host = Arc::new(scripted_host(dir.path(), r#"[{"content":"ok"}]"#));
    let session = TuiSession::start(host, dir.path(), false).await.unwrap();
    let payload = "visible\x1b]52;c;Y2xpcA==\x07\x1b[2J\x1bPdata\x1b\\\x00\r\u{009b}end";
    {
        let mut shared = session.lock_shared();
        shared.transcript = vec![
            TranscriptLine::Assistant(payload.into()),
            TranscriptLine::Tool(ToolLine {
                id: "tool".into(),
                title: payload.into(),
                status: "completed".into(),
                summary: String::new(),
                detail: payload.into(),
                args: json!({}),
                expanded: true,
            }),
        ];
    }
    let drawn = drawn_text(&session);
    assert!(drawn.contains("visible"));
    assert!(drawn.contains('\u{fffd}'));
    assert!(
        !drawn
            .chars()
            .any(|ch| ch.is_control() && ch != '\n' && ch != '\t')
    );
    install_permission(
        &session,
        serde_json::json!({"command": payload}).to_string(),
    );
    let drawn = drawn_text(&session);
    assert!(drawn.contains("command"));
    assert!(
        !drawn
            .chars()
            .any(|ch| ch.is_control() && ch != '\n' && ch != '\t')
    );
    assert_eq!(terminal_text("plain 雪\n\ttext"), "plain 雪\n\ttext");
}

#[tokio::test]
async fn collapsed_tool_stays_one_line_for_multiline_result() {
    let dir = tempdir().unwrap();
    let cwd = dir.path().join("project");
    std::fs::create_dir_all(&cwd).unwrap();
    let host = Arc::new(scripted_host(dir.path(), r#"[{"content":"ok"}]"#));
    let session = TuiSession::start(host, &cwd, false).await.unwrap();
    notify_multiline_tool(&session).await;
    let text = session.transcript_text();
    assert_eq!(tool_line_count(&text), 1, "{text}");
    assert_eq!(text, "· bash  cat notes  ok");
    assert!(!text.contains("later-unique-line"), "{text}");
    assert!(!drawn_text(&session).contains("later-unique-line"));
}

fn sample_tool(status: &str) -> ToolLine {
    ToolLine {
        id: "t1".into(),
        title: "write_file".into(),
        status: status.into(),
        summary: "a.txt".into(),
        detail: "wrote a.txt".into(),
        args: json!({"path": "a.txt"}),
        expanded: false,
    }
}

#[test]
fn collapsed_tool_line_shows_ok_fail_ask() {
    assert_eq!(
        format_tool(&sample_tool("completed")),
        "· write_file  a.txt  ok"
    );
    assert_eq!(
        format_tool(&sample_tool("failed")),
        "· write_file  a.txt  fail"
    );
    assert_eq!(
        format_tool(&sample_tool("pending")),
        "· write_file  a.txt  ask"
    );
    assert_eq!(
        format_tool(&sample_tool("cancelled")),
        "· write_file  a.txt  cancelled"
    );
    assert_eq!(
        format_tool(&sample_tool("not run")),
        "· write_file  a.txt  not run"
    );
    assert_eq!(
        format_tool(&sample_tool("result unknown")),
        "· write_file  a.txt  result unknown"
    );
}

#[test]
fn expanded_edit_shows_bounded_old_new_snippet() {
    let old = (0..15)
        .map(|i| format!("old-line-{i}"))
        .collect::<Vec<_>>()
        .join("\n");
    let new = (0..15)
        .map(|i| format!("new-line-{i}"))
        .collect::<Vec<_>>()
        .join("\n");
    let mut tool = ToolLine {
        id: "e1".into(),
        title: "edit".into(),
        status: "completed".into(),
        summary: "notes.rs".into(),
        detail: "updated notes.rs".into(),
        args: json!({
            "path": "notes.rs",
            "old": old,
            "new": new,
            "content": "FULL_FILE_SHOULD_NOT_APPEAR"
        }),
        expanded: true,
    };
    let text = format_tool(&tool);
    assert!(text.starts_with("· edit  notes.rs  ok\n"), "{text}");
    assert!(text.contains("· - old-line-0"), "{text}");
    assert!(
        text.contains("· + new-line-0") || text.contains("new-line-0"),
        "{text}"
    );
    assert!(text.contains("· updated notes.rs"), "{text}");
    assert!(!text.contains("FULL_FILE_SHOULD_NOT_APPEAR"), "{text}");
    assert!(
        tool_line_count(&text) <= 22,
        "header plus 20 snippet lines plus result: {text}"
    );
    assert!(
        !text.contains("old-line-14") || !text.contains("new-line-14"),
        "{text}"
    );
    tool.expanded = false;
    let collapsed = format_tool(&tool);
    assert_eq!(collapsed, "· edit  notes.rs  ok");
    assert!(!collapsed.contains("old-line-0"), "{collapsed}");
}

#[test]
fn report_has_no_outcome_word() {
    let tool = ToolLine {
        id: "r1".into(),
        title: "report".into(),
        status: "completed".into(),
        summary: "all written".into(),
        detail: "all written".into(),
        args: json!({"summary": "all written", "success": true}),
        expanded: false,
    };
    assert_eq!(format_tool(&tool), "· report  all written");
}

#[test]
fn prompt_block_grows_and_wraps() {
    let (lines, x, y) = prompt_block("ab\ncd", 5, 80);
    assert_eq!(lines, vec!["> ab".to_string(), "  cd".to_string()]);
    assert_eq!(y, 1);
    assert_eq!(x, 4);
    let (lines, x, y) = prompt_block("ab", 1, 80);
    assert_eq!(lines, vec!["> ab".to_string()]);
    assert_eq!(y, 0);
    assert_eq!(x, 3);
    let (lines, x, y) = prompt_block("abc", 3, 6);
    assert_eq!(lines, vec!["> abc".to_string()]);
    assert_eq!(y, 0);
    assert_eq!(x, 5);
    let (lines, x, y) = prompt_block("abcdefghij", 10, 6);
    assert_eq!(
        lines,
        vec![
            "> abc".to_string(),
            "  def".to_string(),
            "  ghi".to_string(),
            "  j".to_string()
        ]
    );
    assert_eq!(y, 3);
    assert_eq!(x, 3);
    let (lines, x, y) = prompt_block("中文", 1, 6);
    assert_eq!(lines, vec!["> 中".to_string(), "  文".to_string()]);
    assert_eq!(y, 0);
    assert_eq!(x, 4);
    let (lines, _, y) = prompt_block("中文", 2, 4);
    assert_eq!(lines, vec!["> 中".to_string(), "  文".to_string()]);
    assert_eq!(y, 1);
    let mut shared = Shared::new();
    shared.composer_w = 6;
    shared.insert_str("abc\nabc");
    assert_eq!(shared.cursor, 7);
    assert!(shared.move_cursor_row(-1));
    assert_eq!(shared.cursor, 3);
    shared.clear_input();
    shared.insert_str("abcdef");
    assert_eq!(shared.cursor, 6);
    let (_, x, y) = prompt_block(&shared.input, 3, 6);
    assert_eq!((x, y), (5, 0));
    assert!(shared.move_cursor_row(-1));
    assert_eq!(shared.cursor, 3);
    let (_, x, y) = prompt_block(&shared.input, shared.cursor, 6);
    assert_eq!((x, y), (5, 0));
    assert!(shared.move_cursor_row(1));
    assert_eq!(shared.cursor, 6);
}

#[test]
fn permission_fields_keeps_multiline_command() {
    let body = permission_fields(r#"{"command":"echo checking\nrm -rf /tmp/secret"}"#);
    assert!(body.contains("echo checking"), "{body}");
    assert!(body.contains("rm -rf /tmp/secret"), "{body}");
    let long = "a".repeat(2000);
    let body = permission_fields(&format!(r#"{{"command":"{long}"}}"#));
    assert!(
        body.contains(&long),
        "ask must keep the full command, not a truncated prefix"
    );
}
