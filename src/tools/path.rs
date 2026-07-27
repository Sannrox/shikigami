//! Workspace path jail and ignore matching.

use std::path::{Component, Path};

/// True when a path must be rejected by the workspace jail before I/O.
/// Relative paths may contain only normal components (no `..`, roots, prefixes).
pub fn is_unsafe_relative_path(relative: &Path) -> bool {
    relative.is_absolute()
        || relative.components().any(|c| {
            matches!(
                c,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
}

/// Built-in defaults always loaded when `respect_ignore` is true.
pub(crate) fn builtin_ignore_patterns() -> Vec<String> {
    vec![
        ".git".into(),
        "node_modules".into(),
        "target".into(),
        "dist".into(),
        "build".into(),
        ".venv".into(),
        "venv".into(),
        "__pycache__".into(),
        ".DS_Store".into(),
        "*.pyc".into(),
        ".shikigami-state".into(),
    ]
}

pub(crate) fn load_ignore_patterns(workspace: &Path) -> Vec<String> {
    let mut patterns = builtin_ignore_patterns();
    for name in [".shikigamiignore", ".gitignore"] {
        let path = workspace.join(name);
        if let Ok(raw) = std::fs::read_to_string(&path) {
            for line in raw.lines() {
                let line = line.trim();
                if line.is_empty() || line.starts_with('#') || line.starts_with('!') {
                    continue; // v1: no negation support
                }
                let line = line.trim_end_matches('/').to_string();
                if !line.is_empty() {
                    patterns.push(line);
                }
            }
        }
    }
    patterns
}

/// Whether a workspace-relative path matches ignore patterns (files or dirs).
pub fn path_is_ignored(rel: &Path, patterns: &[String]) -> bool {
    if patterns.is_empty() {
        return false;
    }
    let s = rel.to_string_lossy().replace('\\', "/");
    let s = s.trim_start_matches("./");
    if s.is_empty() {
        return false;
    }
    for pat in patterns {
        let pat = pat.trim().trim_start_matches('/').trim_end_matches('/');
        if pat.is_empty() {
            continue;
        }
        if glob_match(pat, s) || glob_match(&format!("**/{pat}"), s) {
            return true;
        }
        // Prefix directory: node_modules/foo
        if s.starts_with(&format!("{pat}/")) {
            return true;
        }
        // Component match: any path segment
        for seg in s.split('/') {
            if glob_match(pat, seg) {
                return true;
            }
        }
    }
    false
}

/// Simple glob: `*` (within segment), `**` (across segments). No `{a,b}` classes.
pub(crate) fn glob_match(pattern: &str, path: &str) -> bool {
    let path = path.replace('\\', "/");
    let pattern = pattern.replace('\\', "/");
    let pat: Vec<&str> = pattern.split('/').filter(|s| !s.is_empty()).collect();
    let segs: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    glob_match_segs(&pat, &segs)
}

fn glob_match_segs(pat: &[&str], path: &[&str]) -> bool {
    match (pat.first().copied(), path.first().copied()) {
        (None, None) => true,
        (Some("**"), _) => {
            // Match empty prefix or consume one path segment and retry.
            glob_match_segs(&pat[1..], path)
                || (!path.is_empty() && glob_match_segs(pat, &path[1..]))
        }
        (Some(p), Some(s)) if segment_match(p, s) => glob_match_segs(&pat[1..], &path[1..]),
        (Some(_), None) => pat.iter().all(|p| *p == "**"),
        _ => false,
    }
}

fn segment_match(pat: &str, seg: &str) -> bool {
    if pat == "*" || pat == "**" {
        return true;
    }
    let pb: Vec<char> = pat.chars().collect();
    let sb: Vec<char> = seg.chars().collect();
    let (mut i, mut j) = (0usize, 0usize);
    let mut star = None;
    let mut star_j = 0usize;
    while j < sb.len() {
        if i < pb.len() && (pb[i] == '?' || pb[i] == sb[j]) {
            i += 1;
            j += 1;
        } else if i < pb.len() && pb[i] == '*' {
            star = Some(i);
            star_j = j;
            i += 1;
        } else if let Some(si) = star {
            i = si + 1;
            star_j += 1;
            j = star_j;
        } else {
            return false;
        }
    }
    while i < pb.len() && pb[i] == '*' {
        i += 1;
    }
    i == pb.len()
}
