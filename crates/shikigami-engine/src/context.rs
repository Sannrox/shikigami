//! Project rules and other run context attachments.

use std::path::{Path, PathBuf};

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
        let (body, digest, truncated) =
            load_truncated_text(&path, max, "\n\n… [project rules truncated]\n")?;
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

fn skills_root(workspace: &Path, settings: &ContextSettings) -> PathBuf {
    match &settings.skills_root {
        Some(r) if !r.is_empty() => {
            let p = PathBuf::from(r);
            if p.is_absolute() {
                p
            } else {
                workspace.join(p)
            }
        }
        _ => workspace.join(".shikigami/skills"),
    }
}

/// Roots searched for `/skill:name`. Runtime packs first, then `.agents/skills`.
fn skill_search_roots(workspace: &Path, settings: &ContextSettings) -> Vec<PathBuf> {
    let primary = skills_root(workspace, settings);
    let agents = workspace.join(".agents/skills");
    if agents == primary {
        vec![primary]
    } else {
        vec![primary, agents]
    }
}

fn skill_md(root: &Path, id: &str) -> PathBuf {
    root.join(id).join("SKILL.md")
}

fn valid_skill_id(id: &str) -> bool {
    !id.is_empty() && !id.contains("..") && !id.contains('/') && !id.contains('\\')
}

fn scan_skill_ids(root: &Path) -> Vec<String> {
    let mut ids = Vec::new();
    let Ok(entries) = std::fs::read_dir(root) else {
        return ids;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(id) = name.to_str() else {
            continue;
        };
        if valid_skill_id(id) && skill_md(root, id).is_file() {
            ids.push(id.to_string());
        }
    }
    ids
}

/// Skill ids the TUI can offer as `/skill:name`.
///
/// Configured `context.skills` is an allow-list. When that list is empty,
/// ids are discovered from `skills_root` and `<workspace>/.agents/skills`
/// directories that contain `SKILL.md`. [`load_skills`] still injects only
/// configured ids into the system prompt.
pub fn list_skill_ids(workspace: &Path, settings: &ContextSettings) -> Vec<String> {
    let roots = skill_search_roots(workspace, settings);
    if !settings.skills.is_empty() {
        return settings
            .skills
            .iter()
            .filter(|id| {
                valid_skill_id(id) && roots.iter().any(|root| skill_md(root, id).is_file())
            })
            .cloned()
            .collect();
    }
    let mut ids = Vec::new();
    for root in &roots {
        for id in scan_skill_ids(root) {
            if !ids.contains(&id) {
                ids.push(id);
            }
        }
    }
    ids.sort();
    ids
}

/// Load one skill pack by id. Honors the configured allow-list when it is set.
/// Prefers `skills_root` over `.agents/skills` when both define the same id.
pub fn load_skill(workspace: &Path, settings: &ContextSettings, id: &str) -> Option<SkillPack> {
    if !valid_skill_id(id) {
        return None;
    }
    if !settings.skills.is_empty() && !settings.skills.iter().any(|listed| listed == id) {
        return None;
    }
    let max = settings.max_skill_bytes.clamp(1, MAX_DEFAULT * 4);
    for root in skill_search_roots(workspace, settings) {
        let path = skill_md(&root, id);
        if !path.is_file() {
            continue;
        }
        let (body, digest, truncated) =
            load_truncated_text(&path, max, "\n\n… [skill truncated]\n")?;
        return Some(SkillPack {
            id: id.to_string(),
            body,
            digest,
            truncated,
        });
    }
    None
}

/// Load configured skill packs from `skills_root/<id>/SKILL.md`.
pub fn load_skills(workspace: &Path, settings: &ContextSettings) -> Vec<SkillPack> {
    if settings.skills.is_empty() {
        return Vec::new();
    }
    settings
        .skills
        .iter()
        .filter_map(|id| load_skill(workspace, settings, id))
        .collect()
}

fn load_truncated_text(path: &Path, max: usize, marker: &str) -> Option<(String, String, bool)> {
    let raw = std::fs::read(path).ok()?;
    let truncated = raw.len() > max;
    let slice = if truncated { &raw[..max] } else { &raw[..] };
    let mut body = String::from_utf8_lossy(slice).into_owned();
    if truncated {
        body.push_str(marker);
    }
    let digest = crate::digest::sha256_hex(body.as_bytes());
    Some((body, digest, truncated))
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

    #[test]
    fn list_skill_ids_scans_root_when_unlisted() {
        let dir = tempdir().unwrap();
        let skill_dir = dir.path().join(".shikigami/skills/demo");
        std::fs::create_dir_all(&skill_dir).unwrap();
        std::fs::write(skill_dir.join("SKILL.md"), "prefer tests first\n").unwrap();
        let ids = list_skill_ids(dir.path(), &ContextSettings::default());
        assert_eq!(ids, vec!["demo"]);
        let pack = load_skill(dir.path(), &ContextSettings::default(), "demo").unwrap();
        assert!(pack.body.contains("tests first"));
        assert!(load_skills(dir.path(), &ContextSettings::default()).is_empty());
    }

    #[test]
    fn list_skill_ids_honors_allow_list() {
        let dir = tempdir().unwrap();
        for id in ["keep", "skip"] {
            let skill_dir = dir.path().join(".shikigami/skills").join(id);
            std::fs::create_dir_all(&skill_dir).unwrap();
            std::fs::write(skill_dir.join("SKILL.md"), format!("{id}\n")).unwrap();
        }
        let s = ContextSettings {
            skills: vec!["keep".into()],
            ..Default::default()
        };
        assert_eq!(list_skill_ids(dir.path(), &s), vec!["keep"]);
        assert!(load_skill(dir.path(), &s, "skip").is_none());
    }

    #[test]
    fn list_skill_ids_includes_agents_skills() {
        let dir = tempdir().unwrap();
        let skill_dir = dir.path().join(".agents/skills/verify-change");
        std::fs::create_dir_all(&skill_dir).unwrap();
        std::fs::write(skill_dir.join("SKILL.md"), "run make validate\n").unwrap();
        let ids = list_skill_ids(dir.path(), &ContextSettings::default());
        assert_eq!(ids, vec!["verify-change"]);
        let pack = load_skill(dir.path(), &ContextSettings::default(), "verify-change").unwrap();
        assert!(pack.body.contains("make validate"));
        assert!(load_skills(dir.path(), &ContextSettings::default()).is_empty());
    }

    #[test]
    fn runtime_pack_wins_over_agents_skill_with_the_same_id() {
        let dir = tempdir().unwrap();
        let runtime = dir.path().join(".shikigami/skills/demo");
        let agents = dir.path().join(".agents/skills/demo");
        std::fs::create_dir_all(&runtime).unwrap();
        std::fs::create_dir_all(&agents).unwrap();
        std::fs::write(runtime.join("SKILL.md"), "from runtime\n").unwrap();
        std::fs::write(agents.join("SKILL.md"), "from agents\n").unwrap();
        let pack = load_skill(dir.path(), &ContextSettings::default(), "demo").unwrap();
        assert!(pack.body.contains("from runtime"));
        assert_eq!(
            list_skill_ids(dir.path(), &ContextSettings::default()),
            vec!["demo"]
        );
    }
}
