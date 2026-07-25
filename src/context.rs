//! Project rules and other run context attachments.

use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use crate::config::ContextSettings;

const MAX_DEFAULT: usize = 32 * 1024;

/// Loaded project rules (not executed as code).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectRules {
    pub filename: String,
    pub body: String,
    pub digest: String,
    pub truncated: bool,
}

impl ProjectRules {
    pub fn attribution_id(&self) -> String {
        format!("rules:{}:{}", self.filename, self.digest)
    }
}

/// Discover the first configured rules file under the workspace root.
pub fn load_project_rules(workspace: &Path, settings: &ContextSettings) -> Option<ProjectRules> {
    if !settings.load_project_rules {
        return None;
    }
    let max = settings.max_rules_bytes.clamp(1, MAX_DEFAULT * 4);
    for name in &settings.rules_filenames {
        if name.contains("..") || name.contains('/') || name.contains('\\') {
            continue; // only flat workspace-root names
        }
        let path = workspace.join(name);
        if !path.is_file() {
            continue;
        }
        let raw = std::fs::read(&path).ok()?;
        let truncated = raw.len() > max;
        let slice = if truncated { &raw[..max] } else { &raw[..] };
        let mut body = String::from_utf8_lossy(slice).into_owned();
        if truncated {
            body.push_str("\n\n… [project rules truncated]\n");
        }
        let digest = format!("{:x}", Sha256::digest(body.as_bytes()));
        return Some(ProjectRules {
            filename: name.clone(),
            body,
            digest,
            truncated,
        });
    }
    None
}

/// Compose system prompt with optional project rules.
pub fn compose_system_prompt(base: &str, rules: Option<&ProjectRules>) -> String {
    match rules {
        None => base.to_string(),
        Some(r) => format!("{base}\n\n# Project rules (`{}`)\n\n{}", r.filename, r.body),
    }
}

/// Path used only in tests/helpers.
#[allow(dead_code)]
pub fn rules_path(workspace: &Path, filename: &str) -> PathBuf {
    workspace.join(filename)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ContextSettings;
    use tempfile::tempdir;

    #[test]
    fn loads_agents_md() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("AGENTS.md"), "use small PRs\n").unwrap();
        let rules = load_project_rules(dir.path(), &ContextSettings::default()).unwrap();
        assert_eq!(rules.filename, "AGENTS.md");
        assert!(rules.body.contains("small PRs"));
        assert!(!rules.truncated);
        assert_eq!(rules.digest.len(), 64);
    }

    #[test]
    fn missing_is_none() {
        let dir = tempdir().unwrap();
        assert!(load_project_rules(dir.path(), &ContextSettings::default()).is_none());
    }

    #[test]
    fn disable_skips() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("AGENTS.md"), "x\n").unwrap();
        let s = ContextSettings {
            load_project_rules: false,
            ..Default::default()
        };
        assert!(load_project_rules(dir.path(), &s).is_none());
    }
}
