use super::support::*;
use crate::host::program_status;
use crate::protocol::{
    COMPACTED, NO_ANSWER, apply_load_result, apply_prompt_result, apply_update, model_failure_lead,
};
use crate::render::line_text;
use crate::session::*;

use std::sync::Arc;

use serde_json::json;
use tempfile::tempdir;

#[test]
fn program_status_reports_prompt_lifecycle() {
    let mut shared = Shared::new();
    assert_eq!(
        program_status(&shared),
        b"\x1b]7501;state=idle:app=shikigami\x1b\\"
    );
    shared.busy = true;
    // Cosmetic footer messages must not hide an active prompt.
    shared.status = "copied".into();
    assert_eq!(
        program_status(&shared),
        b"\x1b]7501;state=working:app=shikigami\x1b\\"
    );
    let (tx, _) = tokio::sync::oneshot::channel();
    shared.permission = Some(PendingPermission {
        title: "tool".into(),
        raw_input: "{}".into(),
        tx,
    });
    assert_eq!(
        program_status(&shared),
        b"\x1b]7501;state=blocked:kind=permission:app=shikigami\x1b\\"
    );
    shared.permission = None;
    assert_eq!(
        program_status(&shared),
        b"\x1b]7501;state=working:app=shikigami\x1b\\"
    );
    shared.busy = false;
    for (response, expected) in [
        (
            serde_json::json!({"result":{"stopReason":"end_turn"}}),
            b"\x1b]7501;state=done:app=shikigami\x1b\\".as_slice(),
        ),
        (
            serde_json::json!({"error":{"message":"failed"}}),
            b"\x1b]7501;state=error:app=shikigami\x1b\\".as_slice(),
        ),
        (
            serde_json::json!({"result":{"stopReason":"cancelled"}}),
            b"\x1b]7501;state=idle:app=shikigami\x1b\\".as_slice(),
        ),
    ] {
        apply_prompt_result(&mut shared, Some(&response));
        assert_eq!(program_status(&shared), expected);
    }
}

#[tokio::test]
async fn plain_start_is_new_and_continue_replays_the_latest() {
    let dir = tempdir().unwrap();
    let cwd = dir.path().join("project");
    std::fs::create_dir_all(&cwd).unwrap();
    let host = Arc::new(scripted_host(
        dir.path(),
        r#"[{"content":"hello"},{"content":"again"}]"#,
    ));
    let first = TuiSession::start(Arc::clone(&host), &cwd, false)
        .await
        .unwrap();
    first.spawn_prompt("hi".into()).await.unwrap();
    let _ = first.wait_prompt().await;
    let id = first.session_id();
    drop(first);

    let plain = TuiSession::start(Arc::clone(&host), &cwd, false)
        .await
        .unwrap();
    assert_ne!(plain.session_id(), id);
    assert!(plain.transcript_text().is_empty());
    drop(plain);

    let continued = TuiSession::start(host, &cwd, true).await.unwrap();
    assert_eq!(continued.session_id(), id);
    let text = continued.transcript_text();
    assert!(text.contains("hello"), "load must replay history: {text}");
    assert!(
        text.ends_with("Continued your session from just now (1 turn)."),
        "{text}"
    );
}

/// `--continue` with only an unloadable session record starts new and says so.
#[tokio::test]
async fn continue_without_a_usable_session_says_it_started_new() {
    let dir = tempdir().unwrap();
    let cwd = dir.path().join("project");
    std::fs::create_dir_all(&cwd).unwrap();
    let host = Arc::new(scripted_host(dir.path(), r#"[{"content":"ok"}]"#));
    let fake = format!("sess-{}", uuid::Uuid::new_v4());
    let sessions = dir.path().join("state/acp-sessions");
    std::fs::create_dir_all(&sessions).unwrap();
    std::fs::write(
        sessions.join(format!("{fake}.json")),
        serde_json::to_vec(&json!({
            "session_id": fake,
            "cwd": cwd.display().to_string(),
            "run_id": "missing-run"
        }))
        .unwrap(),
    )
    .unwrap();

    let session = TuiSession::start(host, &cwd, true).await.unwrap();
    assert_ne!(session.session_id(), fake);
    assert_eq!(session.transcript_text(), NOTHING_TO_CONTINUE);
}

#[tokio::test]
async fn restart_after_crash_mid_permission_marks_the_tool_not_run() {
    let dir = tempdir().unwrap();
    let cwd = dir.path().join("project");
    std::fs::create_dir_all(&cwd).unwrap();
    let script = r#"[
      {"tool_calls":[{"name":"write_file","args_json":"{\"path\":\"ok.txt\",\"content\":\"hi\\n\"}"}]},
      {"content":"wrote it"}
    ]"#;
    let first = TuiSession::start(Arc::new(scripted_host(dir.path(), script)), &cwd, false)
        .await
        .unwrap();
    first.spawn_prompt("write".into()).await.unwrap();
    wait_until(|| first.overlay_text().is_some()).await;
    // Crash: the prompt never gets an answer and the host goes away.
    if let Some(task) = first.prompt.lock().unwrap().take() {
        task.abort();
    }
    drop(first);

    let second = continue_session(&dir, script).await;
    let transcript = second.transcript_text();
    assert!(
        transcript.contains("· write_file") && transcript.contains("not run"),
        "interrupted tool must show its outcome: {transcript}"
    );
    assert!(
        transcript.contains("Last session stopped while waiting for permission."),
        "restart must say the run was interrupted: {transcript}"
    );
    let screen = drawn_text(&second);
    assert!(screen.contains("not run"), "{screen}");
    assert!(!cwd.join("ok.txt").exists());
}

#[tokio::test]
async fn relaunch_replays_what_the_user_saw() {
    let dir = tempdir().unwrap();
    let script = r#"[
      {"tool_calls":[{"name":"write_file","args_json":"{\"path\":\"ok.txt\",\"content\":\"hi\\n\"}"}]},
      {"tool_calls":[{"name":"write_file","args_json":"{\"path\":\"no.txt\",\"content\":\"hi\\n\"}"}]},
      {"tool_calls":[{"name":"report","args_json":"{\"summary\":\"all written\",\"success\":true}"}]}
    ]"#;
    let session = start_session(&dir, script).await;
    session
        .spawn_prompt("write two files".into())
        .await
        .unwrap();
    wait_until(|| session.overlay_text().is_some()).await;
    session.answer_permission(true);
    wait_until(|| {
        session
            .overlay_text()
            .is_some_and(|text| text.contains("no.txt"))
    })
    .await;
    session.answer_permission(false);
    session.wait_prompt().await;
    session.spawn_prompt("and then?".into()).await.unwrap();
    session.wait_prompt().await;
    let live = session.transcript_text();
    drop(session);

    let relaunched = continue_session(&dir, script).await;
    let replayed = replayed_text(&relaunched);
    // The history keeps no error text, so the failed prompt's outcome
    // replays as one line instead of the live error.
    let failure = format!(
        "{}\nscripted turns exhausted",
        model_failure_lead(Some("scriptEnded"))
    );
    assert!(live.contains(&failure), "{live}");
    assert_eq!(replayed, live.replace(&failure, NO_ANSWER));
}

/// #616: a compaction marker replays as a note, so the prompt before it is
/// not taken for one that got no answer.
#[test]
fn replayed_compaction_marker_is_a_note_not_a_prompt() {
    let mut shared = Shared::new();
    let user = |id: &str, text: &str| {
        json!({"update": {"sessionUpdate": "user_message_chunk", "messageId": id,
            "content": {"type": "text", "text": text}}})
    };
    apply_update(&mut shared, &user("user-0", "the task"));
    apply_update(
        &mut shared,
        &json!({"update": {"sessionUpdate": "user_message_chunk",
            "content": {"type": "text", "text": "[context compacted: 4 earlier messages omitted; continue the original task]"},
            "_meta": {"shikigami": {"compacted": true}}}}),
    );
    apply_update(&mut shared, &user("user-6", "a follow-up"));
    apply_update(
        &mut shared,
        &json!({"update": {"sessionUpdate": "agent_message_chunk", "messageId": "turn-3",
            "content": {"type": "text", "text": "done it"}}}),
    );
    apply_load_result(&mut shared, &json!({}));
    let text: Vec<String> = shared.transcript.iter().map(line_text).collect();
    assert_eq!(
        text,
        ["you  the task", COMPACTED, "you  a follow-up", "done it"]
    );
}

/// #616: a failed prompt followed by an answered one replays as two prompts.
#[tokio::test]
async fn relaunch_keeps_a_failed_prompt_apart_from_the_next() {
    let dir = tempdir().unwrap();
    // Fails until the second prompt is in the history.
    let script = r#"[{"requires_context":["second-ask"],"content":"answered"}]"#;
    let session = start_session(&dir, script).await;
    session.spawn_prompt("first-ask".into()).await.unwrap();
    session.wait_prompt().await;
    session.spawn_prompt("second-ask".into()).await.unwrap();
    session.wait_prompt().await;
    drop(session);

    let relaunched = continue_session(&dir, script).await;
    assert_eq!(
        replayed_text(&relaunched),
        format!("you  first-ask\n{NO_ANSWER}\nyou  second-ask\nanswered")
    );
}

/// #616: a prompt cancelled before any output stays its own line live.
#[tokio::test]
async fn prompt_after_a_silent_cancel_stays_a_separate_line() {
    let dir = tempdir().unwrap();
    let session = start_session(
        &dir,
        r#"[{"chunks":["slow"],"chunk_delay_ms":300,"content":"slow"},{"content":"ok"}]"#,
    )
    .await;
    session.spawn_prompt("first".into()).await.unwrap();
    session.cancel();
    session.wait_prompt().await;
    session.spawn_prompt("second".into()).await.unwrap();
    session.wait_prompt().await;
    let prompts = user_prompts(&session);
    assert_eq!(prompts, ["first", "second"]);
}

/// #616: a tool cancelled at its permission ask replays as it was shown.
#[tokio::test]
async fn relaunch_after_a_cancelled_ask_replays_what_the_user_saw() {
    let dir = tempdir().unwrap();
    let script = r#"[{"tool_calls":[{"name":"write_file","args_json":"{\"path\":\"ok.txt\",\"content\":\"hi\\n\"}"}]}]"#;
    let session = start_session(&dir, script).await;
    session
        .spawn_prompt("write the ok file".into())
        .await
        .unwrap();
    wait_until(|| session.overlay_text().is_some()).await;
    session.cancel();
    session.wait_prompt().await;
    let live = session.transcript_text();
    drop(session);

    let relaunched = continue_session(&dir, script).await;
    assert_eq!(replayed_text(&relaunched), live);
}

/// #601: `/run` names the run `shikigami runs` and `export` accept, and says
/// so plainly before the first prompt.
#[test]
fn session_update_marks_dirty() {
    let mut shared = Shared::new();
    shared.dirty = false;
    apply_update(
        &mut shared,
        &json!({
            "update": {
                "sessionUpdate": "agent_message_chunk",
                "content": { "type": "text", "text": "hi" }
            }
        }),
    );
    assert!(shared.dirty);
    shared.dirty = false;
    apply_update(
        &mut shared,
        &json!({ "update": { "sessionUpdate": "noop" } }),
    );
    assert!(!shared.dirty);
}
