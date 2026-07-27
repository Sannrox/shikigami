//! Filesystem tools: read/write/edit/patch/glob/grep and path resolve.

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

impl ToolExecutor {
    pub(crate) fn resolve(&self, relative: &Path) -> Result<PathBuf, ToolError> {
        if is_unsafe_relative_path(relative) {
            return Err(ToolError::UnsafePath(relative.to_path_buf()));
        }
        let joined = self.workspace.join(relative);
        if let Some(parent) = joined.parent() {
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
        let path = self.resolve(path)?;
        std::fs::write(path, content)?;
        Ok(())
    }

    pub(crate) fn edit(&self, path: &Path, old: &str, new: &str) -> Result<(), ToolError> {
        let text = self.read_file(path)?;
        let count = text.matches(old).count();
        if count != 1 {
            return Err(ToolError::EditMatch { count });
        }
        let updated = text.replacen(old, new, 1);
        self.write_file(path, &updated)
    }

    pub(crate) fn multi_edit(&self, path: &Path, edits: &[EditHunk]) -> Result<usize, ToolError> {
        if edits.is_empty() {
            return Err(ToolError::MultiEditEmpty);
        }
        let mut text = self.read_file(path)?;
        for (index, hunk) in edits.iter().enumerate() {
            let count = text.matches(&hunk.old).count();
            if count != 1 {
                return Err(ToolError::MultiEditMatch { index, count });
            }
            text = text.replacen(&hunk.old, &hunk.new, 1);
        }
        self.write_file(path, &text)?;
        Ok(edits.len())
    }

    /// Apply structured context hunks atomically across files (compute then write).
    pub(crate) fn apply_patch(&self, patches: &[FilePatch]) -> Result<usize, ToolError> {
        if patches.is_empty() {
            return Err(ToolError::ApplyPatch(
                "patches array must not be empty".into(),
            ));
        }
        if patches.len() > MAX_APPLY_PATCH_FILES {
            return Err(ToolError::ApplyPatch(format!(
                "at most {MAX_APPLY_PATCH_FILES} files per call"
            )));
        }
        let total_hunks: usize = patches.iter().map(|p| p.hunks.len()).sum();
        if total_hunks == 0 {
            return Err(ToolError::ApplyPatch("no hunks provided".into()));
        }
        if total_hunks > MAX_APPLY_PATCH_HUNKS {
            return Err(ToolError::ApplyPatch(format!(
                "at most {MAX_APPLY_PATCH_HUNKS} hunks per call"
            )));
        }

        let mut planned: Vec<(PathBuf, String)> = Vec::new();
        let mut applied = 0usize;
        for file in patches {
            let path = PathBuf::from(&file.path);
            if file.hunks.is_empty() {
                return Err(ToolError::ApplyPatch(format!(
                    "{}: hunks must not be empty",
                    file.path
                )));
            }
            let mut text = self.read_file(&path)?;
            for (index, hunk) in file.hunks.iter().enumerate() {
                let before = hunk.context_before.as_deref().unwrap_or("");
                let after = hunk.context_after.as_deref().unwrap_or("");
                if hunk.old.is_empty() {
                    return Err(ToolError::ApplyPatch(format!(
                        "{} hunk {index}: old must not be empty",
                        file.path
                    )));
                }
                let needle = format!("{before}{}{after}", hunk.old);
                let count = text.matches(&needle).count();
                if count != 1 {
                    return Err(ToolError::ApplyPatch(format!(
                        "{} hunk {index}: expected exactly one match for context+old+context, found {count}",
                        file.path
                    )));
                }
                let replacement = format!("{before}{}{after}", hunk.new);
                text = text.replacen(&needle, &replacement, 1);
                applied += 1;
            }
            if text.len() as u64 > MAX_FILE_BYTES {
                return Err(ToolError::FileTooLarge(path));
            }
            // Resolve path jail before staging write.
            let abs = self.resolve(&path)?;
            planned.push((abs, text));
        }
        for (abs, text) in planned {
            std::fs::write(abs, text)?;
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
