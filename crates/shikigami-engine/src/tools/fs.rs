//! Filesystem tools: read/write/edit/patch/glob/grep and path resolve.

use std::borrow::Cow;
use std::ops::Range;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use super::ToolError;
use super::executor::ToolExecutor;
use super::path::{glob_match, is_unsafe_relative_path, path_is_ignored};

pub(crate) const MAX_FILE_BYTES: u64 = 2 * 1024 * 1024;
pub(crate) const MAX_SEARCH_MATCHES: usize = 200;
pub(crate) const MAX_SEARCH_OUTPUT_BYTES: usize = 256 * 1024;
pub(crate) const MAX_WALK_FILES: usize = 5_000;
pub(crate) const MAX_APPLY_PATCH_BYTES: usize = 64 * 1024;
pub(crate) const MAX_APPLY_PATCH_HUNKS: usize = 32;
pub(crate) const MAX_APPLY_PATCH_FILES: usize = 16;

#[derive(Deserialize)]
pub(crate) struct PathArgs {
    pub(crate) path: PathBuf,
}

#[derive(Deserialize)]
pub(crate) struct WriteArgs {
    pub(crate) path: PathBuf,
    pub(crate) content: String,
}

#[derive(Deserialize)]
pub(crate) struct EditArgs {
    pub(crate) path: PathBuf,
    pub(crate) old: String,
    pub(crate) new: String,
}

#[derive(Deserialize)]
pub(crate) struct EditHunk {
    pub(crate) old: String,
    pub(crate) new: String,
}

#[derive(Deserialize)]
pub(crate) struct MultiEditArgs {
    pub(crate) path: PathBuf,
    pub(crate) edits: Vec<EditHunk>,
}

#[derive(Deserialize)]
pub(crate) struct ApplyPatchArgs {
    pub(crate) patches: Vec<FilePatch>,
}

#[derive(Deserialize)]
pub(crate) struct FilePatch {
    pub(crate) path: String,
    pub(crate) hunks: Vec<PatchHunk>,
}

#[derive(Deserialize)]
pub(crate) struct PatchHunk {
    #[serde(default)]
    pub(crate) context_before: Option<String>,
    pub(crate) old: String,
    pub(crate) new: String,
    #[serde(default)]
    pub(crate) context_after: Option<String>,
}

#[derive(Deserialize)]
pub(crate) struct GlobArgs {
    pub(crate) pattern: String,
    #[serde(default)]
    pub(crate) path: Option<PathBuf>,
}

#[derive(Deserialize)]
pub(crate) struct GrepArgs {
    pub(crate) pattern: String,
    #[serde(default)]
    pub(crate) path: Option<PathBuf>,
    #[serde(default)]
    pub(crate) max_matches: Option<usize>,
}

pub(crate) struct AppliedEdits {
    pub(crate) count: usize,
    pub(crate) normalized: bool,
}

struct EditMatch {
    span: Range<usize>,
    normalized: bool,
}

// Each normalized byte boundary maps back to a boundary in the original text.
// Dropped trailing whitespace belongs to the preceding matched line.
struct NormalizedText {
    text: String,
    offsets: Vec<usize>,
}

impl NormalizedText {
    fn new(input: &str) -> Self {
        let mut text = String::with_capacity(input.len());
        let mut offsets = Vec::with_capacity(input.len() + 1);
        let mut base = 0;
        for line in input.split_inclusive('\n') {
            let body = line.strip_suffix('\n').unwrap_or(line);
            let trimmed = body.trim_end_matches(|ch: char| ch.is_whitespace() && ch != '\r');
            for (index, ch) in trimmed.char_indices() {
                let normalized = match ch {
                    '\u{2018}'..='\u{201b}' => '\'',
                    '\u{201c}'..='\u{201f}' => '"',
                    '\u{2010}'..='\u{2015}' | '\u{2212}' => '-',
                    '\u{a0}'
                    | '\u{1680}'
                    | '\u{2000}'..='\u{200a}'
                    | '\u{202f}'
                    | '\u{205f}'
                    | '\u{3000}' => ' ',
                    other => other,
                };
                text.push(normalized);
                for byte in 0..normalized.len_utf8() {
                    offsets.push(base + index + if ch == normalized { byte } else { 0 });
                }
            }
            if line.ends_with('\n') {
                text.push('\n');
                offsets.push(base + body.len());
            }
            base += line.len();
        }
        offsets.push(input.len());
        Self { text, offsets }
    }
}

fn locate_edit(text: &str, old: &str, allow_normalized: bool) -> Result<EditMatch, usize> {
    let count = text.matches(old).count();
    if count == 1
        && let Some(start) = text.find(old)
    {
        return Ok(EditMatch {
            span: start..start + old.len(),
            normalized: false,
        });
    }
    if count != 0 || !allow_normalized {
        return Err(count);
    }
    let normalized = NormalizedText::new(text);
    let needle = NormalizedText::new(old).text;
    if needle.is_empty() {
        return Err(0);
    }
    let Some(start) = normalized.text.find(&needle) else {
        return Err(0);
    };
    // Search again one character later, so overlapping candidates also fail.
    let next = start
        + normalized.text[start..]
            .chars()
            .next()
            .map_or(0, char::len_utf8);
    if normalized.text[next..].contains(&needle) {
        return Err(2);
    }
    let end = start + needle.len();
    // A trailing newline ends its own line, before whitespace on the next one.
    let original_end = if needle.ends_with('\n') {
        normalized.offsets[end - 1] + 1
    } else {
        normalized.offsets[end]
    };
    Ok(EditMatch {
        span: normalized.offsets[start]..original_end,
        normalized: true,
    })
}

struct PlannedEdit<'a> {
    index: usize,
    span: Range<usize>,
    new: Cow<'a, str>,
}

enum EditPlanError {
    Match { index: usize, count: usize },
    Overlap { first: usize, second: usize },
}

// Both batch tools locate against the original file and reject intersecting
// spans before assembling output. Patch context is part of its matched span.
fn plan_edits<'a>(
    text: &str,
    hunks: &'a [EditHunk],
    crlf: bool,
    allow_normalized: bool,
) -> Result<(Vec<PlannedEdit<'a>>, bool), EditPlanError> {
    let mut planned = Vec::with_capacity(hunks.len());
    let mut normalized = false;
    for (index, hunk) in hunks.iter().enumerate() {
        let old = edit_fragment(&hunk.old, crlf);
        let found = locate_edit(text, old.as_ref(), allow_normalized)
            .map_err(|count| EditPlanError::Match { index, count })?;
        normalized |= found.normalized;
        planned.push(PlannedEdit {
            index,
            span: found.span,
            new: edit_fragment(&hunk.new, crlf),
        });
    }
    planned.sort_by_key(|edit| (edit.span.start, edit.span.end));
    for pair in planned.windows(2) {
        let left = &pair[0];
        let right = &pair[1];
        if right.span.start < left.span.end || right.span.start == left.span.start {
            return Err(EditPlanError::Overlap {
                first: left.index.min(right.index),
                second: left.index.max(right.index),
            });
        }
    }
    Ok((planned, normalized))
}

fn apply_edits(text: &str, planned: &[PlannedEdit<'_>]) -> String {
    let mut output = String::with_capacity(text.len());
    let mut cursor = 0;
    for edit in planned {
        output.push_str(&text[cursor..edit.span.start]);
        output.push_str(&edit.new);
        cursor = edit.span.end;
    }
    output.push_str(&text[cursor..]);
    output
}

// Only consistently CRLF files opt into LF matching. Mixed endings stay exact.
fn lf_file_text(text: String) -> (String, bool) {
    let crlf = text.contains("\r\n") && text.split("\r\n").all(|line| !line.contains(['\r', '\n']));
    if crlf {
        (text.replace("\r\n", "\n"), true)
    } else {
        (text, false)
    }
}

fn edit_fragment(text: &str, crlf: bool) -> Cow<'_, str> {
    if crlf {
        Cow::Owned(text.replace("\r\n", "\n"))
    } else {
        Cow::Borrowed(text)
    }
}

fn restore_line_endings(text: String, crlf: bool) -> String {
    if crlf {
        text.replace('\n', "\r\n")
    } else {
        text
    }
}

impl ToolExecutor {
    pub(crate) fn resolve(&self, relative: &Path) -> Result<PathBuf, ToolError> {
        self.resolve_path(relative, false)
    }

    fn resolve_write(&self, relative: &Path) -> Result<PathBuf, ToolError> {
        self.resolve_path(relative, true)
    }

    fn resolve_path(&self, relative: &Path, create_parents: bool) -> Result<PathBuf, ToolError> {
        if is_unsafe_relative_path(relative) {
            return Err(ToolError::UnsafePath(relative.to_path_buf()));
        }
        let joined = self.workspace.join(relative);
        if create_parents && let Some(parent) = joined.parent() {
            std::fs::create_dir_all(parent)?;
        }
        // For existing paths, canonicalize and ensure under workspace.
        if joined.exists() {
            let canon = std::fs::canonicalize(&joined)?;
            if !canon.starts_with(&self.workspace) {
                return Err(ToolError::PathEscape(relative.to_path_buf()));
            }
            return Ok(canon);
        }
        // New file: canonicalize parent.
        if let Some(parent) = joined.parent() {
            let parent_canon =
                std::fs::canonicalize(parent).unwrap_or_else(|_| parent.to_path_buf());
            if !parent_canon.starts_with(&self.workspace)
                && parent_canon != self.workspace
                && !self.workspace.starts_with(&parent_canon)
            {
                // parent may be workspace itself after create_dir_all
                let ws = &self.workspace;
                if !parent_canon.starts_with(ws) && parent_canon != *ws {
                    return Err(ToolError::PathEscape(relative.to_path_buf()));
                }
            }
        }
        Ok(joined)
    }

    pub(crate) fn read_file(&self, path: &Path) -> Result<String, ToolError> {
        let path = self.resolve(path)?;
        let meta = std::fs::metadata(&path)?;
        if !meta.is_file() {
            return Err(ToolError::NotRegular(path));
        }
        if meta.len() > MAX_FILE_BYTES {
            return Err(ToolError::FileTooLarge(path));
        }
        Ok(std::fs::read_to_string(path)?)
    }

    pub(crate) fn write_file(&self, path: &Path, content: &str) -> Result<(), ToolError> {
        if content.len() as u64 > MAX_FILE_BYTES {
            return Err(ToolError::FileTooLarge(path.to_path_buf()));
        }
        if super::catalog::is_plan_jail_rel_path(path) {
            super::catalog::write_plan_jail_file(&self.workspace, content.as_bytes())?;
            return Ok(());
        }
        let path = self.resolve_write(path)?;
        std::fs::write(path, content)?;
        Ok(())
    }

    pub(crate) fn edit(
        &self,
        path: &Path,
        old: &str,
        new: &str,
    ) -> Result<AppliedEdits, ToolError> {
        let (mut text, crlf) = lf_file_text(self.read_file(path)?);
        let old = edit_fragment(old, crlf);
        let new = edit_fragment(new, crlf);
        let found = locate_edit(&text, old.as_ref(), true)
            .map_err(|count| ToolError::EditMatch { count })?;
        text.replace_range(found.span, new.as_ref());
        self.write_file(path, &restore_line_endings(text, crlf))?;
        Ok(AppliedEdits {
            count: 1,
            normalized: found.normalized,
        })
    }

    pub(crate) fn multi_edit(
        &self,
        path: &Path,
        edits: &[EditHunk],
    ) -> Result<AppliedEdits, ToolError> {
        if edits.is_empty() {
            return Err(ToolError::MultiEditEmpty);
        }
        let (text, crlf) = lf_file_text(self.read_file(path)?);
        let (planned, normalized) =
            plan_edits(&text, edits, crlf, true).map_err(|error| match error {
                EditPlanError::Match { index, count } => ToolError::MultiEditMatch { index, count },
                EditPlanError::Overlap { first, second } => {
                    ToolError::EditOverlap { first, second }
                }
            })?;
        let text = apply_edits(&text, &planned);
        self.write_file(path, &restore_line_endings(text, crlf))?;
        Ok(AppliedEdits {
            count: edits.len(),
            normalized,
        })
    }

    /// Apply structured context hunks atomically across files (compute then write).
    pub(crate) fn apply_patch(&self, patches: &[FilePatch]) -> Result<usize, ToolError> {
        if patches.is_empty() {
            return Err(ToolError::ApplyPatch(
                "patches array must not be empty".into(),
            ));
        }
        if patches.len() > MAX_APPLY_PATCH_FILES {
            return Err(ToolError::ApplyPatchLimit(format!(
                "at most {MAX_APPLY_PATCH_FILES} files per call"
            )));
        }
        let total_hunks: usize = patches.iter().map(|p| p.hunks.len()).sum();
        if total_hunks == 0 {
            return Err(ToolError::ApplyPatch("no hunks provided".into()));
        }
        if total_hunks > MAX_APPLY_PATCH_HUNKS {
            return Err(ToolError::ApplyPatchLimit(format!(
                "at most {MAX_APPLY_PATCH_HUNKS} hunks per call"
            )));
        }

        enum PlannedWrite {
            Abs(PathBuf, String),
            PlanJail(String),
        }
        let mut planned: Vec<PlannedWrite> = Vec::new();
        let mut applied = 0usize;
        for file in patches {
            let path = PathBuf::from(&file.path);
            if file.hunks.is_empty() {
                return Err(ToolError::ApplyPatch(format!(
                    "{}: hunks must not be empty",
                    file.path
                )));
            }
            let (text, crlf) = lf_file_text(self.read_file(&path)?);
            let mut hunks = Vec::with_capacity(file.hunks.len());
            for (index, hunk) in file.hunks.iter().enumerate() {
                if hunk.old.is_empty() {
                    return Err(ToolError::ApplyPatch(format!(
                        "{} hunk {index}: old must not be empty",
                        file.path
                    )));
                }
                let before = hunk.context_before.as_deref().unwrap_or("");
                let after = hunk.context_after.as_deref().unwrap_or("");
                hunks.push(EditHunk {
                    old: format!("{before}{}{after}", hunk.old),
                    new: format!("{before}{}{after}", hunk.new),
                });
            }
            let (edits, _) =
                plan_edits(&text, &hunks, crlf, false).map_err(|error| match error {
                    EditPlanError::Match { index, count } => ToolError::ApplyPatchMatch {
                        path: path.clone(),
                        index,
                        count,
                    },
                    EditPlanError::Overlap { first, second } => {
                        ToolError::EditOverlap { first, second }
                    }
                })?;
            let text = apply_edits(&text, &edits);
            applied += hunks.len();
            let text = restore_line_endings(text, crlf);
            if text.len() as u64 > MAX_FILE_BYTES {
                return Err(ToolError::FileTooLarge(path));
            }
            if super::catalog::is_plan_jail_rel_path(&path) {
                planned.push(PlannedWrite::PlanJail(text));
            } else {
                let abs = self.resolve_write(&path)?;
                planned.push(PlannedWrite::Abs(abs, text));
            }
        }
        for write in planned {
            match write {
                PlannedWrite::PlanJail(text) => {
                    super::catalog::write_plan_jail_file(&self.workspace, text.as_bytes())?;
                }
                PlannedWrite::Abs(abs, text) => std::fs::write(abs, text)?,
            }
        }
        Ok(applied)
    }

    pub(crate) fn glob_files(
        &self,
        pattern: &str,
        under: Option<&Path>,
    ) -> Result<String, ToolError> {
        if pattern.is_empty() {
            return Err(ToolError::InvalidPattern("empty glob pattern".into()));
        }
        let base = self.search_base(under)?;
        let mut matches = Vec::new();
        let mut truncated = false;
        self.walk_files(&base, &mut |rel| {
            let rel_str = rel.to_string_lossy();
            if glob_match(pattern, rel_str.as_ref()) {
                if matches.len() >= MAX_SEARCH_MATCHES {
                    truncated = true;
                    return false;
                }
                matches.push(rel_str.into_owned());
            }
            true
        })?;
        matches.sort();
        let mut out = matches.join("\n");
        if truncated {
            out.push_str(&format!("\n… truncated after {MAX_SEARCH_MATCHES} matches"));
        }
        if out.len() > MAX_SEARCH_OUTPUT_BYTES {
            out.truncate(MAX_SEARCH_OUTPUT_BYTES);
            out.push_str("\n… output byte limit");
        }
        if out.is_empty() {
            out = "(no matches)".into();
        }
        Ok(out)
    }

    pub(crate) fn grep_files(
        &self,
        pattern: &str,
        under: Option<&Path>,
        max_matches: usize,
    ) -> Result<String, ToolError> {
        let re =
            regex::Regex::new(pattern).map_err(|e| ToolError::InvalidPattern(e.to_string()))?;
        let base = self.search_base(under)?;
        let mut lines = Vec::new();
        let mut truncated = false;
        self.walk_files(&base, &mut |rel| {
            if truncated {
                return false;
            }
            let abs = self.workspace.join(&rel);
            let Ok(meta) = std::fs::metadata(&abs) else {
                return true;
            };
            if !meta.is_file() || meta.len() > MAX_FILE_BYTES {
                return true;
            }
            let Ok(text) = std::fs::read_to_string(&abs) else {
                return true; // skip binary / invalid utf-8
            };
            for (i, line) in text.lines().enumerate() {
                if re.is_match(line) {
                    if lines.len() >= max_matches {
                        truncated = true;
                        return false;
                    }
                    lines.push(format!("{}:{}:{line}", rel.display(), i + 1));
                }
            }
            true
        })?;
        let mut out = lines.join("\n");
        if truncated {
            out.push_str(&format!("\n… truncated after {max_matches} matches"));
        }
        if out.len() > MAX_SEARCH_OUTPUT_BYTES {
            out.truncate(MAX_SEARCH_OUTPUT_BYTES);
            out.push_str("\n… output byte limit");
        }
        if out.is_empty() {
            out = "(no matches)".into();
        }
        Ok(out)
    }

    pub(crate) fn search_base(&self, under: Option<&Path>) -> Result<PathBuf, ToolError> {
        match under {
            None => Ok(self.workspace.clone()),
            Some(p) if p.as_os_str().is_empty() || p == Path::new(".") => {
                Ok(self.workspace.clone())
            }
            Some(p) => {
                if is_unsafe_relative_path(p) {
                    return Err(ToolError::UnsafePath(p.to_path_buf()));
                }
                let joined = self.workspace.join(p);
                let canon = std::fs::canonicalize(&joined).map_err(ToolError::Io)?;
                if !canon.starts_with(&self.workspace) {
                    return Err(ToolError::PathEscape(p.to_path_buf()));
                }
                Ok(canon)
            }
        }
    }

    /// Walk files under `base` (absolute, inside workspace). Callback gets workspace-relative path.
    /// Return false from callback to stop early. Honors ignore patterns for dirs and files.
    pub(crate) fn walk_files(
        &self,
        base: &Path,
        visit: &mut dyn FnMut(PathBuf) -> bool,
    ) -> Result<(), ToolError> {
        if base.is_file() {
            let rel = base
                .strip_prefix(&self.workspace)
                .map_err(|_| ToolError::PathEscape(base.to_path_buf()))?
                .to_path_buf();
            if is_unsafe_relative_path(&rel) && rel != Path::new("") {
                return Err(ToolError::UnsafePath(rel));
            }
            if path_is_ignored(&rel, &self.ignore_patterns) {
                return Ok(());
            }
            let _ = visit(rel);
            return Ok(());
        }
        let mut stack = vec![base.to_path_buf()];
        let mut seen = 0usize;
        while let Some(dir) = stack.pop() {
            let rd = std::fs::read_dir(&dir)?;
            for entry in rd {
                let entry = entry?;
                let path = entry.path();
                let meta = entry.metadata()?;
                if meta.is_symlink() {
                    continue; // do not follow symlinks out of jail
                }
                let rel = match path.strip_prefix(&self.workspace) {
                    Ok(r) => r.to_path_buf(),
                    Err(_) => continue,
                };
                if path_is_ignored(&rel, &self.ignore_patterns) {
                    continue;
                }
                if meta.is_dir() {
                    stack.push(path);
                    continue;
                }
                if !meta.is_file() {
                    continue;
                }
                let canon = match std::fs::canonicalize(&path) {
                    Ok(c) => c,
                    Err(_) => continue,
                };
                if !canon.starts_with(&self.workspace) {
                    continue;
                }
                let rel = canon
                    .strip_prefix(&self.workspace)
                    .map_err(|_| ToolError::PathEscape(path))?
                    .to_path_buf();
                seen += 1;
                if seen > MAX_WALK_FILES {
                    return Ok(());
                }
                if !visit(rel) {
                    return Ok(());
                }
            }
        }
        Ok(())
    }
}
