//! Keyboard, mouse, and paste handling.

use std::time::Instant;

use tokio::sync::mpsc;

use crossterm::event::{
    self, Event, KeyCode, KeyEvent, KeyModifiers, MouseButton, MouseEvent, MouseEventKind,
};

use crate::clipboard::pasted_image_path;
use crate::look::Action;
use crate::session::{KeyResult, SESSION_IN_USE, TranscriptLine, TuiSession};
use crate::slash::{
    SlashLine, SlashOutcome, acp_part_for_path, mention_visible, prompt_display, prompt_parts,
    slash_name_args, slash_visible, slash_waits_for_turn, stage_named_part,
};

pub(crate) fn handle_event(session: &TuiSession, ev: Event) -> KeyResult {
    match ev {
        Event::Key(key) if key.kind == event::KeyEventKind::Press => handle_key(session, key),
        Event::Mouse(mouse) => {
            handle_mouse(session, mouse);
            KeyResult::Continue
        }
        Event::Paste(text) => {
            handle_paste(session, &text);
            KeyResult::Continue
        }
        Event::Resize(_, _) => {
            session.lock_shared().dirty = true;
            KeyResult::Continue
        }
        _ => KeyResult::Continue,
    }
}

pub(crate) fn handle_mouse(session: &TuiSession, mouse: MouseEvent) {
    match mouse.kind {
        MouseEventKind::Down(MouseButton::Left) => {
            let mut shared = session.lock_shared();
            let Some(rel) = crate::render::transcript_row_at(&shared, mouse.column, mouse.row)
            else {
                return;
            };
            let shift = mouse.modifiers.contains(KeyModifiers::SHIFT);
            if shift {
                if let Some(sel) = shared.text_selection.as_mut() {
                    sel.end = rel;
                } else {
                    shared.text_selection = Some(crate::session::TextSelection {
                        start: rel,
                        end: rel,
                    });
                }
                shared.dirty = true;
                return;
            }
            shared.text_selection = Some(crate::session::TextSelection {
                start: rel,
                end: rel,
            });
            shared.dirty = true;
            if let Some(id) = crate::render::tool_at_point(&shared, mouse.column, mouse.row) {
                shared.select_tool(id);
            }
        }
        MouseEventKind::Drag(MouseButton::Left) => {
            let mut shared = session.lock_shared();
            let Some(rel) = crate::render::transcript_row_at(&shared, mouse.column, mouse.row)
            else {
                return;
            };
            if let Some(sel) = shared.text_selection.as_mut()
                && sel.end != rel
            {
                sel.end = rel;
                shared.dirty = true;
            }
        }
        MouseEventKind::ScrollUp => session.lock_shared().wheel(-1, mouse.column, mouse.row),
        MouseEventKind::ScrollDown => session.lock_shared().wheel(1, mouse.column, mouse.row),
        _ => {}
    }
}

pub(crate) fn typing_event(ev: &Event) -> bool {
    match ev {
        Event::Paste(_) => true,
        Event::Key(key) if key.kind == event::KeyEventKind::Press => {
            !key.modifiers.contains(KeyModifiers::CONTROL)
                && matches!(
                    key.code,
                    KeyCode::Char(_) | KeyCode::Backspace | KeyCode::Delete
                )
        }
        _ => false,
    }
}

/// Keys already queued when the permission dock first paints were meant for
/// the composer. Drop them so they cannot answer the ask.
pub(crate) fn drain_keys_pressed_before_permission_frame(
    rx: &mut mpsc::Receiver<Event>,
    session: &TuiSession,
) {
    while let Ok(ev) = rx.try_recv() {
        if typing_event(&ev) {
            session.lock_shared().note_input();
        } else if !matches!(ev, Event::Key(_)) {
            let _ = handle_event(session, ev);
        }
    }
}

pub(crate) fn handle_permission_key(session: &TuiSession, key: KeyEvent) -> KeyResult {
    match key.code {
        KeyCode::PageUp => {
            session.lock_shared().page_up();
            return KeyResult::Continue;
        }
        KeyCode::PageDown => {
            session.lock_shared().page_down();
            return KeyResult::Continue;
        }
        KeyCode::Esc => {
            session.cancel();
            return KeyResult::Continue;
        }
        _ => {}
    }
    let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
    let live = {
        let mut shared = session.lock_shared();
        let live = shared.permission_answers_live(Instant::now());
        if !live
            && !ctrl
            && matches!(
                key.code,
                KeyCode::Char(_) | KeyCode::Backspace | KeyCode::Delete
            )
        {
            shared.note_input();
        }
        live
    };
    if !live {
        return KeyResult::Continue;
    }
    match key.code {
        KeyCode::Char('y' | 'Y' | '1') => session.answer_permission(true),
        KeyCode::Char('a' | 'A') => {
            let session_allow = session
                .lock_shared()
                .permission
                .as_ref()
                .is_some_and(|pending| pending.session_allow);
            if session_allow {
                session.answer_session_permission();
            } else {
                session.answer_permission(true);
            }
        }
        KeyCode::Char('n' | 'N' | 'd' | 'D' | '2') => session.answer_permission(false),
        _ => {}
    }
    KeyResult::Continue
}

pub(crate) fn handle_key(session: &TuiSession, key: KeyEvent) -> KeyResult {
    let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
    let action = session.look.keys.action(&key);
    match action {
        Some(Action::Interrupt) => {
            if session.lock_shared().busy {
                session.cancel();
                return KeyResult::Continue;
            }
            return KeyResult::Quit;
        }
        Some(Action::Quit) => {
            return if session.lock_shared().busy {
                KeyResult::Continue
            } else {
                KeyResult::Quit
            };
        }
        Some(Action::TogglePlan) => {
            let mut shared = session.lock_shared();
            if shared.plan.is_some() {
                shared.show_plan = !shared.show_plan;
                shared.dirty = true;
            }
            return KeyResult::Continue;
        }
        Some(Action::ToggleTool) => {
            session.lock_shared().toggle_selected_tool();
            return KeyResult::Continue;
        }
        Some(Action::Copy) => {
            let idle = {
                let shared = session.lock_shared();
                !shared.busy && shared.permission.is_none()
            };
            if idle {
                session.copy_last_assistant();
            }
            return KeyResult::Continue;
        }
        Some(Action::ScrollLineUp) => {
            session.lock_shared().line_up();
            return KeyResult::Continue;
        }
        Some(Action::ScrollLineDown) => {
            session.lock_shared().line_down();
            return KeyResult::Continue;
        }
        Some(Action::LineStart | Action::LineEnd | Action::ClearInput) | None => {}
    }

    if session.lock_shared().permission.is_some() {
        return handle_permission_key(session, key);
    }

    match action {
        Some(Action::LineStart) => {
            session.lock_shared().set_cursor_start();
            return KeyResult::Continue;
        }
        Some(Action::LineEnd) => {
            session.lock_shared().set_cursor_end();
            return KeyResult::Continue;
        }
        Some(Action::ClearInput) => {
            session.lock_shared().clear_input();
            return KeyResult::Continue;
        }
        _ => {}
    }

    let (slash_open, resume_open, mention_open) = {
        let shared = session.lock_shared();
        (
            slash_visible(&shared),
            shared.resume.is_some(),
            mention_visible(&shared).is_some(),
        )
    };
    let list_open = slash_open || resume_open || mention_open;
    match key.code {
        KeyCode::PageUp => session.lock_shared().page_up(),
        KeyCode::PageDown => session.lock_shared().page_down(),
        KeyCode::Left => session.lock_shared().move_cursor(-1),
        KeyCode::Right => session.lock_shared().move_cursor(1),
        KeyCode::Home => session.lock_shared().set_cursor_start(),
        KeyCode::End => session.lock_shared().set_cursor_end(),
        KeyCode::Up if list_open => {
            let mut shared = session.lock_shared();
            if shared.slash_selected > 0 {
                shared.slash_selected -= 1;
                shared.dirty = true;
            }
        }
        KeyCode::Down if list_open => {
            let n = session.overlay_cmds().len();
            let mut shared = session.lock_shared();
            if n > 0 && shared.slash_selected + 1 < n {
                shared.slash_selected += 1;
                shared.dirty = true;
            }
        }
        KeyCode::Up => {
            let mut shared = session.lock_shared();
            if !shared.move_cursor_row(-1) {
                shared.history_up();
            }
        }
        KeyCode::Down => {
            let mut shared = session.lock_shared();
            if !shared.move_cursor_row(1) {
                shared.history_down();
            }
        }
        KeyCode::Tab if slash_open => {
            let matches = session.slash_matches();
            let mut shared = session.lock_shared();
            if let Some(cmd) = matches.get(shared.slash_selected) {
                let args = slash_name_args(&shared.input).1;
                shared.input = if args.is_empty() {
                    format!("/{}", cmd.name)
                } else {
                    format!("/{} {args}", cmd.name)
                };
                shared.cursor = shared.input.chars().count();
                shared.dirty = true;
            }
        }
        KeyCode::Tab if mention_open => {
            session.pick_mention();
        }
        KeyCode::Enter
            if mention_open
                && !key
                    .modifiers
                    .intersects(KeyModifiers::SHIFT | KeyModifiers::ALT)
                && session.pick_mention() => {}
        KeyCode::Char(ch) if !ctrl => session.lock_shared().insert_char(ch),
        KeyCode::Backspace => session.lock_shared().backspace(),
        KeyCode::Delete => session.lock_shared().delete_forward(),
        KeyCode::Enter => {
            if key
                .modifiers
                .intersects(KeyModifiers::SHIFT | KeyModifiers::ALT)
            {
                session.lock_shared().insert_char('\n');
                return KeyResult::Continue;
            }
            if session.lock_shared().busy {
                let text = session.lock_shared().input.clone();
                match session.slash_line(&text) {
                    SlashLine::Prompt => session.lock_shared().queue_follow_up(),
                    SlashLine::Command(name) => {
                        let mut shared = session.lock_shared();
                        shared
                            .transcript
                            .push(TranscriptLine::System(slash_waits_for_turn(&name)));
                        shared.dirty = true;
                    }
                    SlashLine::Unknown => {
                        let _ = session.slash_enter(&text);
                    }
                }
                return KeyResult::Continue;
            }
            {
                let mut shared = session.lock_shared();
                if let Some(sessions) = shared.resume.take() {
                    let idx = shared.slash_selected.min(sessions.len().saturating_sub(1));
                    shared.slash_selected = 0;
                    shared.dirty = true;
                    if let Some(picked) = sessions.into_iter().nth(idx) {
                        if picked.in_use {
                            shared
                                .transcript
                                .push(TranscriptLine::System(SESSION_IN_USE.into()));
                            return KeyResult::Continue;
                        }
                        return KeyResult::LoadSession(picked.session_id);
                    }
                    return KeyResult::Continue;
                }
            }
            let text = session.lock_shared().input.clone();
            match session.slash_enter(&text) {
                Some(SlashOutcome::Ran(result)) => {
                    session.lock_shared().take_input();
                    return result;
                }
                Some(SlashOutcome::Refused) => return KeyResult::Continue,
                None => {}
            }
            let (text, pending) = {
                let mut shared = session.lock_shared();
                let text = shared.take_input();
                let pending = std::mem::take(&mut shared.pending_parts);
                (text, pending)
            };
            let trimmed = text.trim();
            if trimmed.is_empty() {
                if !pending.is_empty() {
                    let mut shared = session.lock_shared();
                    shared.pending_parts = pending;
                    shared
                        .transcript
                        .push(TranscriptLine::System("prompt requires a text part".into()));
                    shared.status = "error".into();
                    shared.dirty = true;
                }
            } else {
                // `//text` is the escape for a prompt that starts with `/`.
                let trimmed = match trimmed.strip_prefix('/') {
                    Some(rest) if rest.starts_with('/') => rest,
                    _ => trimmed,
                };
                let parts = prompt_parts(trimmed, pending);
                return KeyResult::Prompt {
                    display: prompt_display(trimmed, &parts),
                    send: trimmed.to_string(),
                    parts,
                    load_skill: None,
                };
            }
        }
        KeyCode::Esc => {
            let mut shared = session.lock_shared();
            if shared.resume.take().is_some() {
                shared.slash_selected = 0;
                shared.dirty = true;
            } else if slash_visible(&shared) {
                shared.slash_dismissed = true;
                shared.dirty = true;
            } else if let Some((start, _)) = mention_visible(&shared) {
                shared.mention_dismissed = Some(start);
                shared.dirty = true;
            } else if shared.show_plan {
                shared.show_plan = false;
                shared.dirty = true;
            } else if !shared.clear_text_selection() {
                shared.drop_queue();
            }
        }
        _ => {}
    }
    KeyResult::Continue
}

pub(crate) fn handle_paste(session: &TuiSession, text: &str) {
    if session.lock_shared().permission.is_some() {
        session.lock_shared().note_input();
        return;
    }
    if let Some(path) = pasted_image_path(text, &session.cwd)
        && let Ok(part) = acp_part_for_path(&path)
        && part.get("type").and_then(|value| value.as_str()) == Some("image")
    {
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("file");
        let mut shared = session.lock_shared();
        stage_named_part(&mut shared, part, name);
        return;
    }
    session.lock_shared().insert_str(text);
}
