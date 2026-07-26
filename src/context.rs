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
        let digest = Sha256::digest(body.as_bytes())
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        return Some(ProjectRules {
            filename: name.clone(),
            body,
            digest,
            truncated,
        });
    }
    None
}

/// A loaded skill pack (SKILL.md text + digest).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillPack {
    pub id: String,
    pub body: String,
    pub digest: String,
    pub truncated: bool,
}

/// Load configured skill packs from `skills_root/<id>/SKILL.md`.
pub fn load_skills(workspace: &Path, settings: &ContextSettings) -> Vec<SkillPack> {
    if settings.skills.is_empty() {
        return Vec::new();
    }
    let root = match &settings.skills_root {
        Some(r) if !r.is_empty() => {
            let p = PathBuf::from(r);
            if p.is_absolute() {
                p
            } else {
                workspace.join(p)
            }
        }
        _ => workspace.join(".shikigami/skills"),
    };
    let max = settings.max_skill_bytes.clamp(1, MAX_DEFAULT * 4);
    let mut out = Vec::new();
    for id in &settings.skills {
        if id.contains("..") || id.contains('/') || id.contains('\\') {
            continue;
        }
        let path = root.join(id).join("SKILL.md");
        if !path.is_file() {
            continue;
        }
        let Ok(raw) = std::fs::read(&path) else {
            continue;
        };
        let truncated = raw.len() > max;
        let slice = if truncated { &raw[..max] } else { &raw[..] };
        let mut body = String::from_utf8_lossy(slice).into_owned();
        if truncated {
            body.push_str("\n\n… [skill truncated]\n");
        }
        let digest = Sha256::digest(body.as_bytes())
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        out.push(SkillPack {
            id: id.clone(),
            body,
            digest,
            truncated,
        });
    }
    out
}

/// Compose system prompt with optional project rules and skill packs.
pub fn compose_system_prompt(
    base: &str,
    rules: Option<&ProjectRules>,
    skills: &[SkillPack],
) -> String {
    let mut out = base.to_string();
    if let Some(r) = rules {
        out.push_str(&format!(
            "\n\n# Project rules (`{}`)\n\n{}",
            r.filename, r.body
        ));
    }
    for s in skills {
        out.push_str(&format!("\n\n# Skill `{}`\n\n{}", s.id, s.body));
    }
    out
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

    #[test]
    fn loads_skill_pack() {
        let dir = tempdir().unwrap();
        let skill_dir = dir.path().join(".shikigami/skills/demo");
        std::fs::create_dir_all(&skill_dir).unwrap();
        std::fs::write(skill_dir.join("SKILL.md"), "prefer tests first\n").unwrap();
        let s = ContextSettings {
            skills: vec!["demo".into()],
            ..Default::default()
        };
        let packs = load_skills(dir.path(), &s);
        assert_eq!(packs.len(), 1);
        assert_eq!(packs[0].id, "demo");
        assert!(packs[0].body.contains("tests first"));
        let composed = compose_system_prompt("BASE", None, &packs);
        assert!(composed.contains("Skill `demo`"));
        assert!(composed.contains("tests first"));
    }
}
