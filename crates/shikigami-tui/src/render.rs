//! Transcript, composer, and overlay drawing.

use ratatui::Frame;
use ratatui::prelude::{Constraint, Layout, Position, Rect, Style};
use ratatui::style::Modifier;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Wrap};
use serde_json::Value;
use std::time::{Duration, SystemTime};
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

use crate::look::{Action, KeyMap, Palette};
use crate::session::{
    PendingPermission, Plan, Shared, TextSelection, ToolLine, TranscriptLine, TuiSession,
};
use crate::slash::{
    inflight_nested_children, mention_visible, nested_child_row, nested_children_visible,
    running_marker_row, running_marker_visible, slash_row_text, slash_visible, slash_window,
};
use shikigami_types::terminal::terminal_text;

pub(crate) fn line_text(line: &TranscriptLine) -> String {
    terminal_text(&match line {
        TranscriptLine::User(text) => format!("you  {text}"),
        TranscriptLine::Assistant(text) => text.clone(),
        TranscriptLine::Tool(tool) => format_tool(tool),
        TranscriptLine::System(text) => text.clone(),
    })
}

/// ACP status as the collapsed-row word. Replay words stay as they are.
pub(crate) fn tool_outcome(status: &str) -> &str {
    match status {
        "completed" => "ok",
        "failed" => "fail",
        "pending" => "ask",
        other => other,
    }
}

const EDIT_DIFF_LINES: usize = 20;

fn arg_text(obj: &serde_json::Map<String, Value>, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| obj.get(*key).and_then(Value::as_str))
        .map(str::to_string)
}

fn push_hunk(lines: &mut Vec<String>, old: &str, new: &str) {
    for line in old.lines() {
        lines.push(format!("- {line}"));
    }
    for line in new.lines() {
        lines.push(format!("+ {line}"));
    }
}

/// Short old/new (or patch) snippet from args already on the tool line.
fn edit_snippet(title: &str, args: &Value) -> Option<String> {
    let mut lines = Vec::new();
    match title {
        "edit" => {
            let obj = args.as_object()?;
            let old = arg_text(obj, &["old", "old_string"])?;
            let new = arg_text(obj, &["new", "new_string"])?;
            push_hunk(&mut lines, &old, &new);
        }
        "multi_edit" => {
            for edit in args.get("edits")?.as_array()? {
                let Some(obj) = edit.as_object() else {
                    continue;
                };
                let old = arg_text(obj, &["old", "old_string"]).unwrap_or_default();
                let new = arg_text(obj, &["new", "new_string"]).unwrap_or_default();
                push_hunk(&mut lines, &old, &new);
            }
        }
        "apply_patch" => {
            for patch in args.get("patches")?.as_array()? {
                if let Some(path) = patch.get("path").and_then(Value::as_str) {
                    lines.push(path.to_string());
                }
                let Some(hunks) = patch.get("hunks").and_then(Value::as_array) else {
                    continue;
                };
                for hunk in hunks {
                    let Some(obj) = hunk.as_object() else {
                        continue;
                    };
                    let old = arg_text(obj, &["old", "old_string"]).unwrap_or_default();
                    let new = arg_text(obj, &["new", "new_string"]).unwrap_or_default();
                    push_hunk(&mut lines, &old, &new);
                }
            }
        }
        _ => return None,
    }
    if lines.is_empty() {
        return None;
    }
    lines.truncate(EDIT_DIFF_LINES);
    Some(lines.join("\n"))
}

fn tool_one_line(tool: &ToolLine) -> String {
    if tool.summary.is_empty() {
        truncate_one_line(&tool.detail, 80)
    } else {
        truncate_one_line(&tool.summary, 80)
    }
}

fn tool_header(tool: &ToolLine) -> String {
    let word = tool_outcome(&tool.status);
    let suffix = if word.is_empty() {
        String::new()
    } else {
        format!("  {word}")
    };
    let one = tool_one_line(tool);
    if one.is_empty() {
        format!("· {}{suffix}", tool.title)
    } else {
        format!("· {}  {one}{suffix}", tool.title)
    }
}

fn tool_expand_body(tool: &ToolLine) -> String {
    match edit_snippet(&tool.title, &tool.args) {
        Some(diff) if tool.detail.is_empty() || tool.detail == tool.summary => diff,
        Some(diff) => format!("{diff}\n{}", tool.detail),
        None if tool.detail.is_empty() || tool.detail == tool.summary => String::new(),
        None => tool.detail.clone(),
    }
}

pub(crate) fn format_tool(tool: &ToolLine) -> String {
    // A turn's final answer comes as a `report` call: show it whole, wrapped.
    if tool.title == "report" {
        let rest = if tool.detail.is_empty() {
            tool.summary.as_str()
        } else {
            tool.detail.as_str()
        };
        let mut lines = rest.lines();
        let first = lines.next().unwrap_or("");
        let mut out = format!("· {}  {first}", tool.title);
        for line in lines {
            out.push('\n');
            out.push_str("· ");
            out.push_str(line);
        }
        return out;
    }
    let mut out = tool_header(tool);
    if !tool.expanded {
        return out;
    }
    let one = tool_one_line(tool);
    for line in tool_expand_body(tool).lines() {
        if line == one {
            continue;
        }
        out.push('\n');
        out.push_str("· ");
        out.push_str(line);
    }
    out
}

pub(crate) fn permission_answers(session_allow: bool) -> &'static str {
    if session_allow {
        "> y allow\n  a allow for session\n  n deny"
    } else {
        "> y allow\n  n deny"
    }
}

pub(crate) fn permission_dock(pending: &PendingPermission) -> String {
    let mut out = format!("ask  {}", pending.title);
    let body = permission_fields(&pending.raw_input);
    if !body.is_empty() {
        out.push('\n');
        out.push_str(&body);
    }
    out
}

pub(crate) fn permission_fields(raw_input: &str) -> String {
    match serde_json::from_str::<Value>(raw_input) {
        Ok(Value::Object(map)) if !map.is_empty() => map
            .iter()
            .filter_map(|(key, value)| {
                let shown = field_preview(value);
                if shown.is_empty() {
                    None
                } else {
                    Some(align_field(key, &shown))
                }
            })
            .collect::<Vec<_>>()
            .join("\n"),
        Ok(Value::String(s)) => field_text(&s),
        _ => {
            let trimmed = raw_input.trim();
            if trimmed.is_empty() || trimmed == "{}" {
                String::new()
            } else {
                field_text(trimmed)
            }
        }
    }
}

pub(crate) fn field_preview(value: &Value) -> String {
    match value {
        Value::String(s) => field_text(s),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => String::new(),
        other => field_text(&other.to_string()),
    }
}

pub(crate) fn field_text(s: &str) -> String {
    s.trim_end_matches(['\r', '\n']).to_string()
}

pub(crate) fn align_field(key: &str, shown: &str) -> String {
    let mut lines = shown.lines();
    let first = lines.next().unwrap_or("");
    let mut out = format!("{key:<8} {first}");
    let indent = " ".repeat(9);
    for line in lines {
        out.push('\n');
        out.push_str(&indent);
        out.push_str(line);
    }
    out
}

pub(crate) fn format_plan(plan: &Plan) -> String {
    let mut out = String::from("plan");
    for (i, entry) in plan.entries.iter().enumerate() {
        if entry.status.is_empty() {
            out.push_str(&format!("\n{}. {}", i + 1, entry.content));
        } else {
            out.push_str(&format!("\n{}. {}  {}", i + 1, entry.content, entry.status));
        }
    }
    out
}

pub(crate) fn composer_inner(shared: &Shared) -> String {
    terminal_text(&if let Some(pending) = shared.permission.as_ref() {
        format!(
            "{}\n{}",
            permission_dock(pending),
            permission_answers(pending.session_allow)
        )
    } else if shared.show_plan
        && let Some(plan) = shared.plan.as_ref()
    {
        format_plan(plan)
    } else {
        prompt_line(shared)
    })
}

pub(crate) fn prompt_line(shared: &Shared) -> String {
    format!("> {}", shared.input)
}

pub(crate) fn follow_pad(content_rows: u16, view_h: u16, scroll_back: u16) -> u16 {
    if scroll_back > 0 {
        0
    } else {
        view_h.saturating_sub(content_rows)
    }
}

pub(crate) fn elapsed_since(when: SystemTime) -> Duration {
    SystemTime::now().duration_since(when).unwrap_or_default()
}

/// Coarse age in words: "just now", "5 minutes ago", "yesterday".
pub(crate) fn ago(elapsed: Duration) -> String {
    let minutes = elapsed.as_secs() / 60;
    let (n, unit) = match minutes {
        0 => return "just now".into(),
        1..60 => (minutes, "minute"),
        60..1440 => (minutes / 60, "hour"),
        1440..2880 => return "yesterday".into(),
        _ => (minutes / 1440, "day"),
    };
    format!("{n} {unit}{} ago", if n == 1 { "" } else { "s" })
}

pub(crate) fn turns_label(n: usize) -> String {
    format!("{n} turn{}", if n == 1 { "" } else { "s" })
}

/// Prompts the user sent in this session, replayed ones included.
pub(crate) fn turn_count(shared: &Shared) -> usize {
    shared
        .transcript
        .iter()
        .filter(|line| matches!(line, TranscriptLine::User(_)))
        .count()
}

pub(crate) fn status_line(model: &str, keys: &KeyMap, shared: &Shared, width: u16) -> String {
    let interrupt = keys.label(Action::Interrupt);
    let mut parts = vec![shared.status.clone()];
    if let Some(custom) = shared.statusline.as_deref() {
        parts.push(truncate_one_line(custom, 48));
    } else {
        if !model.is_empty() {
            parts.push(model.to_string());
        }
        let turns = turn_count(shared);
        if turns > 0 {
            parts.push(format!("turn {turns}"));
        }
    }
    if let Some(tokens) = shared
        .usage
        .footer(turn_count(shared), shared.context_window_tokens)
    {
        parts.push(tokens);
    }
    if shared.permission.is_some() {
        parts.push("esc cancel".into());
    } else if shared.busy {
        parts.push(format!("{interrupt} cancel"));
    } else if shared.resume.is_some() {
        parts.push("esc".into());
        parts.push(format!("{interrupt} quit"));
    } else if slash_visible(shared) || mention_visible(shared).is_some() {
        parts.push("tab pick".into());
        parts.push("esc".into());
        parts.push(format!("{interrupt} quit"));
    } else {
        parts.push(format!("{interrupt} quit"));
    }
    if shared.plan.is_some() {
        parts.push(format!("{} plan", keys.label(Action::TogglePlan)));
    }
    if let Some(queued) = shared.queued.as_ref() {
        parts.push(format!(
            "queued  {}",
            truncate_one_line(&queued.display, 32)
        ));
    }
    if shared
        .transcript
        .iter()
        .any(|line| matches!(line, TranscriptLine::Tool(_)))
    {
        parts.push(format!("{} tool", keys.label(Action::ToggleTool)));
    }
    parts.push("pgup/pgdn/wheel".into());
    truncate_display(&format!("  {}", parts.join("  ")), width as usize)
}

pub(crate) fn char_cols(ch: char) -> usize {
    UnicodeWidthChar::width(ch).unwrap_or(0)
}

pub(crate) fn display_cols(s: &str) -> usize {
    s.chars().map(char_cols).sum()
}

pub(crate) fn take_cols(s: &str, max: usize) -> String {
    let mut out = String::new();
    let mut used = 0;
    for ch in s.chars() {
        let w = char_cols(ch);
        if used + w > max {
            break;
        }
        out.push(ch);
        used += w;
    }
    out
}

pub(crate) fn truncate_display(s: &str, max: usize) -> String {
    if max == 0 {
        return String::new();
    }
    if display_cols(s) <= max {
        return s.to_string();
    }
    if max <= 3 {
        return take_cols(s, max);
    }
    format!("{}...", take_cols(s, max.saturating_sub(3)))
}

pub(crate) fn line_kind(line: &TranscriptLine) -> u8 {
    match line {
        TranscriptLine::User(_) => 0,
        TranscriptLine::Assistant(_) => 1,
        TranscriptLine::Tool(_) => 2,
        TranscriptLine::System(_) => 3,
    }
}

pub(crate) fn spaced_rows(transcript: &[TranscriptLine]) -> Vec<String> {
    let mut rows = Vec::new();
    let mut last: Option<u8> = None;
    for line in transcript {
        let kind = line_kind(line);
        if last.is_some_and(|prev| prev != kind) {
            rows.push(String::new());
        }
        rows.extend(line_text(line).lines().map(str::to_string));
        last = Some(kind);
    }
    rows
}

pub(crate) fn rule_line(width: u16, style: Style) -> Line<'static> {
    Line::from(Span::styled("─".repeat(width as usize), style))
}

pub(crate) fn compact_value(value: &Value) -> String {
    match value {
        Value::Object(map) if map.is_empty() => String::new(),
        Value::Object(map) => {
            const KEYS: &[&str] = &["path", "command", "query", "url", "pattern", "name", "file"];
            let mut parts = Vec::new();
            for key in KEYS {
                if let Some(v) = map.get(*key) {
                    let shown = scalar_preview(v, 60);
                    if !shown.is_empty() {
                        parts.push(shown);
                    }
                }
            }
            if parts.is_empty() {
                for (key, v) in map.iter().take(3) {
                    if key == "content" || key == "contents" {
                        continue;
                    }
                    let shown = scalar_preview(v, 40);
                    if !shown.is_empty() {
                        parts.push(format!("{key} {shown}"));
                    }
                }
            }
            parts.join("  ")
        }
        Value::String(s) => truncate_one_line(s, 80),
        _ => String::new(),
    }
}

pub(crate) fn scalar_preview(value: &Value, max: usize) -> String {
    match value {
        Value::String(s) => truncate_one_line(s, max),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => String::new(),
        other => truncate_one_line(&other.to_string(), max),
    }
}

pub(crate) fn truncate_one_line(s: &str, max: usize) -> String {
    let s = s.lines().next().unwrap_or("").trim();
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let keep: String = s.chars().take(max.saturating_sub(3)).collect();
        format!("{keep}...")
    }
}

pub(crate) struct PromptRow {
    start: usize,
    end: usize,
}

pub(crate) fn wrap_logical(
    chars: &[char],
    start: usize,
    end: usize,
    avail: usize,
) -> Vec<PromptRow> {
    if start == end {
        return vec![PromptRow { start, end }];
    }
    let mut rows = Vec::new();
    let mut i = start;
    while i < end {
        let row_start = i;
        let mut used = 0;
        while i < end {
            let w = char_cols(chars[i]);
            if w == 0 {
                i += 1;
                continue;
            }
            if used > 0 && used + w > avail {
                break;
            }
            used += w;
            i += 1;
            if used >= avail {
                break;
            }
        }
        if i == row_start {
            i += 1;
        }
        rows.push(PromptRow {
            start: row_start,
            end: i,
        });
    }
    rows
}

pub(crate) fn composer_avail(width: u16) -> usize {
    // `> ` takes two cells; keep one so the caret can sit after a full row.
    width.saturating_sub(3).max(1) as usize
}

pub(crate) fn prompt_rows(input: &str, width: u16) -> Vec<PromptRow> {
    let chars: Vec<char> = input.chars().collect();
    let avail = composer_avail(width);
    let mut rows = Vec::new();
    let mut start = 0;
    for (i, &ch) in chars.iter().enumerate() {
        if ch == '\n' {
            rows.extend(wrap_logical(&chars, start, i, avail));
            start = i + 1;
        }
    }
    rows.extend(wrap_logical(&chars, start, chars.len(), avail));
    rows
}

pub(crate) fn prompt_cursor(
    input: &str,
    cursor: usize,
    width: u16,
    rows: &[PromptRow],
) -> (u16, u16) {
    let chars: Vec<char> = input.chars().collect();
    let pos = cursor.min(chars.len());
    let prefix = 2u16;
    if rows.is_empty() {
        return (prefix.min(width.saturating_sub(1)), 0);
    }
    let mut row_i = rows.len() - 1;
    for (i, row) in rows.iter().enumerate() {
        let next = rows.get(i + 1).map(|r| r.start);
        // A soft-wrap boundary index belongs to the filled row so Up/Down
        // stay on that row's reserved caret cell.
        if next.is_none_or(|next| pos < next || (pos == row.end && next == row.end)) {
            row_i = i;
            break;
        }
    }
    let row = &rows[row_i];
    let upto = pos.clamp(row.start, row.end);
    let before: String = chars[row.start..upto].iter().collect();
    let x = prefix.saturating_add(display_cols(&before) as u16);
    (x.min(width.saturating_sub(1)), row_i as u16)
}

pub(crate) fn char_index_at(input: &str, rows: &[PromptRow], row: usize, col: u16) -> usize {
    let Some(target) = rows.get(row) else {
        return input.chars().count();
    };
    let chars: Vec<char> = input.chars().collect();
    let want = col.saturating_sub(2) as usize;
    let mut used = 0;
    let mut i = target.start;
    while i < target.end {
        let w = char_cols(chars[i]);
        if used + w > want {
            break;
        }
        used += w;
        i += 1;
    }
    i
}

pub(crate) fn prompt_block(input: &str, cursor: usize, width: u16) -> (Vec<String>, u16, u16) {
    let chars: Vec<char> = input.chars().collect();
    let rows = prompt_rows(input, width);
    let (x, y) = prompt_cursor(input, cursor, width, &rows);
    let mut lines = Vec::with_capacity(rows.len().max(1));
    for (i, row) in rows.iter().enumerate() {
        let text: String = chars[row.start..row.end].iter().collect();
        if i == 0 {
            lines.push(format!("> {text}"));
        } else {
            lines.push(format!("  {text}"));
        }
    }
    if lines.is_empty() {
        lines.push("> ".into());
    }
    (lines, x, y)
}

pub(crate) fn styled_lines(
    line: &TranscriptLine,
    palette: &Palette,
    selected: bool,
) -> Vec<Line<'static>> {
    let style = if selected {
        palette.selected
    } else {
        match line {
            TranscriptLine::Assistant(_) => palette.text,
            _ => palette.muted,
        }
    };
    line_text(line)
        .lines()
        .map(|row| Line::from(Span::styled(row.to_string(), style)))
        .collect()
}

fn visual_tool_ids(transcript: &[TranscriptLine], width: u16) -> Vec<Option<String>> {
    let mut ids = Vec::new();
    let mut last: Option<u8> = None;
    for line in transcript {
        let kind = line_kind(line);
        if last.is_some_and(|prev| prev != kind) {
            ids.push(None);
        }
        let id = match line {
            TranscriptLine::Tool(tool) => Some(tool.id.clone()),
            _ => None,
        };
        let n = wrapped_rows(&line_text(line), width).max(1);
        for _ in 0..n {
            ids.push(id.clone());
        }
        last = Some(kind);
    }
    ids
}

/// View-relative transcript row for a click, if the point is in the pane.
pub(crate) fn transcript_row_at(shared: &Shared, column: u16, row: u16) -> Option<u16> {
    let view = shared.transcript_area;
    if view.width == 0 || view.height == 0 || !rect_contains(view, column, row) {
        return None;
    }
    Some(row - view.y)
}

/// Tool whose drawn row contains `(column, row)`, if the click is in the transcript.
pub(crate) fn tool_at_point(shared: &Shared, column: u16, row: u16) -> Option<String> {
    let view = shared.transcript_area;
    if view.width == 0 || view.height == 0 || !rect_contains(view, column, row) {
        return None;
    }
    let body = spaced_rows(&shared.transcript).join("\n");
    let content_rows = if body.is_empty() {
        0
    } else {
        wrapped_rows(&body, view.width.max(1))
    };
    let pad = follow_pad(content_rows, view.height, shared.scroll_back);
    let (follow_y, _) = scroll_offset(&body, view);
    let scroll_back = shared.scroll_back.min(follow_y);
    let y = if pad > 0 {
        0
    } else {
        follow_y.saturating_sub(scroll_back)
    };
    let ids = visual_tool_ids(&shared.transcript, view.width.max(1));
    let mut rows: Vec<Option<String>> = vec![None; pad as usize];
    rows.extend(ids);
    let idx = y as usize + (row - view.y) as usize;
    rows.get(idx).cloned().flatten()
}

pub(crate) fn draw(frame: &mut Frame, session: &TuiSession) {
    let palette = &session.look.palette;
    let (slash_cmds, slash_selected) = {
        let cmds = session.overlay_cmds();
        let selected = session.lock_shared().slash_selected;
        (cmds, selected)
    };
    let mut shared = session.lock_shared();
    let area = frame.area();
    let asking = shared.permission.is_some();
    if asking {
        shared.permission_drawn = true;
    }
    let planning = shared.show_plan && shared.plan.is_some() && !asking;
    let inner = composer_inner(&shared);
    let wrap_w = area.width.max(1);
    shared.composer_w = wrap_w;
    let (prompt_lines, cursor_x, cursor_y) = prompt_block(&shared.input, shared.cursor, wrap_w);
    let slash_cap = area.height.saturating_sub(6).min(8) as usize;
    if !slash_cmds.is_empty() {
        shared.slash_selected = slash_selected.min(slash_cmds.len() - 1);
    }
    let (slash_start, slash_vis) = slash_window(shared.slash_selected, slash_cmds.len(), slash_cap);
    let slash_h = slash_vis as u16;
    let child_rows: Vec<String> = if slash_h == 0 && nested_children_visible(&shared) {
        inflight_nested_children(&shared)
            .into_iter()
            .take(slash_cap)
            .map(nested_child_row)
            .collect()
    } else {
        Vec::new()
    };
    let child_h = child_rows.len() as u16;
    let running_row = running_marker_visible(&shared) && slash_h == 0 && child_h == 0;
    let overlay_h = if slash_h > 0 {
        slash_h
    } else if child_h > 0 {
        child_h
    } else if running_row {
        1
    } else {
        0
    };
    shared.overlay_h = overlay_h;
    let composer_budget = area.height.saturating_sub(4 + overlay_h).max(1);
    let composer_h = if asking || planning {
        composer_height(&inner, area)
    } else {
        (prompt_lines.len().max(1) as u16).min(composer_budget)
    };
    let mut constraints = vec![Constraint::Min(1)];
    if overlay_h > 0 {
        constraints.push(Constraint::Length(overlay_h));
    }
    constraints.extend([
        Constraint::Length(1),
        Constraint::Length(composer_h),
        Constraint::Length(1),
        Constraint::Length(1),
    ]);
    let chunks = Layout::vertical(constraints).split(area);

    let rows = spaced_rows(&shared.transcript);
    let body = rows.join("\n");
    let mut idx = 0usize;
    let view = chunks[idx];
    shared.transcript_area = view;
    idx += 1;
    let content_rows = if body.is_empty() {
        0
    } else {
        wrapped_rows(&body, view.width.max(1))
    };
    let pad = follow_pad(content_rows, view.height, shared.scroll_back);
    let mut lines: Vec<Line> = Vec::new();
    for _ in 0..pad {
        lines.push(Line::default());
    }
    let selected_id = shared.selected_tool.clone();
    let mut last: Option<u8> = None;
    for line in &shared.transcript {
        let kind = line_kind(line);
        if last.is_some_and(|prev| prev != kind) {
            lines.push(Line::default());
        }
        let selected = matches!(
            line,
            TranscriptLine::Tool(tool) if selected_id.as_deref() == Some(tool.id.as_str())
        );
        lines.extend(styled_lines(line, palette, selected));
        last = Some(kind);
    }
    shared.transcript_h = view.height;
    let (follow_y, x) = scroll_offset(&body, view);
    if shared.scroll_back > follow_y {
        shared.scroll_back = follow_y;
    }
    let y = if pad > 0 {
        0
    } else {
        follow_y.saturating_sub(shared.scroll_back)
    };
    frame.render_widget(
        Paragraph::new(lines)
            .wrap(Wrap { trim: false })
            .scroll((y, x)),
        view,
    );
    shared.visible_rows = highlight_and_capture(frame, view, shared.text_selection);

    if slash_h > 0 {
        let slash_area = chunks[idx];
        idx += 1;
        let name_w = slash_cmds
            .iter()
            .skip(slash_start)
            .take(slash_vis)
            .map(|cmd| cmd.name.len())
            .max()
            .unwrap_or(8)
            .max(8);
        let selected = shared.slash_selected;
        let rows: Vec<Line> = slash_cmds
            .iter()
            .skip(slash_start)
            .take(slash_vis)
            .enumerate()
            .map(|(i, cmd)| {
                let text = truncate_display(
                    &terminal_text(&slash_row_text(cmd, name_w)),
                    slash_area.width as usize,
                );
                if slash_start + i == selected {
                    Line::from(Span::styled(text, palette.selected))
                } else {
                    Line::from(Span::styled(text, palette.muted))
                }
            })
            .collect();
        frame.render_widget(Paragraph::new(rows), slash_area);
    } else if child_h > 0 {
        let overlay = chunks[idx];
        idx += 1;
        let rows: Vec<Line> = child_rows
            .iter()
            .map(|row| {
                let text = truncate_display(&terminal_text(row), overlay.width as usize);
                Line::from(Span::styled(text, palette.muted))
            })
            .collect();
        frame.render_widget(Paragraph::new(rows), overlay);
    } else if running_row {
        let overlay = chunks[idx];
        idx += 1;
        let text = truncate_display(&running_marker_row(), overlay.width as usize);
        frame.render_widget(
            Paragraph::new(Line::from(Span::styled(text, palette.muted))),
            overlay,
        );
    }

    frame.render_widget(
        Paragraph::new(rule_line(chunks[idx].width, palette.muted)),
        chunks[idx],
    );
    idx += 1;

    let composer = chunks[idx];
    idx += 1;
    let composer_h = composer.height;
    shared.composer_h = composer_h;
    shared.composer_area = composer;
    if asking || planning {
        let max = wrapped_rows(&inner, wrap_w).saturating_sub(composer_h);
        if shared.dock_scroll > max {
            shared.dock_scroll = max;
        }
        let styled: Vec<Line> = inner
            .lines()
            .map(|row| {
                if row.starts_with("> y allow") {
                    Line::from(Span::styled(row.to_string(), palette.selected))
                } else {
                    Line::from(Span::styled(row.to_string(), palette.text))
                }
            })
            .collect();
        frame.render_widget(
            Paragraph::new(styled)
                .wrap(Wrap { trim: false })
                .scroll((shared.dock_scroll, 0)),
            composer,
        );
    } else {
        let scroll = cursor_y.saturating_sub(composer_h.saturating_sub(1));
        let shown: Vec<Line> = prompt_lines
            .iter()
            .skip(scroll as usize)
            .take(composer_h as usize)
            .map(|row| Line::from(row.clone()))
            .collect();
        frame.render_widget(Paragraph::new(shown).style(palette.text), composer);
        frame.set_cursor_position(Position {
            x: composer
                .x
                .saturating_add(cursor_x.min(composer.width.saturating_sub(1))),
            y: composer.y.saturating_add(cursor_y.saturating_sub(scroll)),
        });
    }

    frame.render_widget(
        Paragraph::new(rule_line(chunks[idx].width, palette.muted)),
        chunks[idx],
    );
    idx += 1;
    frame.render_widget(
        Paragraph::new(terminal_text(&status_line(
            &session.model_label(&shared),
            &session.look.keys,
            &shared,
            chunks[idx].width,
        )))
        .style(palette.muted),
        chunks[idx],
    );
    shared.dirty = false;
}

fn highlight_and_capture(
    frame: &mut Frame,
    view: Rect,
    selection: Option<TextSelection>,
) -> Vec<String> {
    if view.width == 0 || view.height == 0 {
        return Vec::new();
    }
    let buf = frame.buffer_mut();
    if let Some(sel) = selection {
        let (lo, hi) = sel.range();
        let last = view.height.saturating_sub(1);
        let lo = lo.min(last);
        let hi = hi.min(last);
        for rel in lo..=hi {
            let y = view.y.saturating_add(rel);
            for x in view.x..view.x.saturating_add(view.width) {
                if let Some(cell) = buf.cell_mut((x, y)) {
                    cell.modifier.insert(Modifier::REVERSED);
                    cell.modifier.insert(Modifier::DIM);
                }
            }
        }
    }
    (0..view.height)
        .map(|i| {
            let y = view.y.saturating_add(i);
            let mut row = String::new();
            let mut x = view.x;
            let end = view.x.saturating_add(view.width);
            while x < end {
                let symbol = buf.cell((x, y)).map_or(" ", |cell| cell.symbol());
                row.push_str(symbol);
                x = x.saturating_add(UnicodeWidthStr::width(symbol).max(1) as u16);
            }
            row.trim_end().to_string()
        })
        .collect()
}

pub(crate) fn wrapped_rows(text: &str, width: u16) -> u16 {
    Paragraph::new(text)
        .wrap(Wrap { trim: false })
        .line_count(width.max(1))
        .min(u16::MAX as usize) as u16
}

pub(crate) fn composer_height(text: &str, area: Rect) -> u16 {
    let rows = wrapped_rows(text, area.width.max(1)).max(1);
    let max = area.height.saturating_sub(4).max(1);
    rows.min(max)
}

pub(crate) fn rect_contains(area: Rect, column: u16, row: u16) -> bool {
    column >= area.x
        && row >= area.y
        && column < area.x.saturating_add(area.width)
        && row < area.y.saturating_add(area.height)
}

pub(crate) fn scroll_offset(body: &str, area: Rect) -> (u16, u16) {
    let rows = wrapped_rows(body, area.width.max(1));
    let h = area.height.max(1);
    (rows.saturating_sub(h), 0)
}
