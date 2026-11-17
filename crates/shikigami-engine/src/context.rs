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
/// directories that contain `SKILL.md`. Runs still see only configured ids
/// ([`skill_context`]).
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

/// Load every configured skill pack from `skills_root/<id>/SKILL.md`.
pub fn load_skills(workspace: &Path, settings: &ContextSettings) -> Vec<SkillPack> {
    settings
        .skills
        .iter()
        .filter_map(|id| load_skill(workspace, settings, id))
        .collect()
}

/// A lazy skill pack as the model first sees it: id plus one-line description.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillSummary {
    pub id: String,
    pub description: String,
}

/// How a run's configured packs reach the model.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SkillContext {
    /// Bodies composed into the system prompt.
    pub eager: Vec<SkillPack>,
    /// Packs the model loads on demand with `load_skill`.
    pub catalog: Vec<SkillSummary>,
}

const MAX_DESCRIPTION_CHARS: usize = 200;

/// Split configured packs into eager bodies and a lazy catalog. With
/// `lazy = false` (the run may not call `load_skill`) every pack is eager.
pub fn skill_context(workspace: &Path, settings: &ContextSettings, lazy: bool) -> SkillContext {
    let mut context = SkillContext::default();
    for pack in load_skills(workspace, settings) {
        if lazy && !settings.eager_skills.contains(&pack.id) {
            context.catalog.push(SkillSummary {
                description: skill_description(&pack.body),
                id: pack.id,
            });
        } else {
            context.eager.push(pack);
        }
    }
    context
}

/// Load a pack for the `load_skill` tool. Only ids in `context.skills` load,
/// even when that list is empty (unlike [`load_skill`], which then allows any
/// pack on disk for the TUI).
pub fn load_listed_skill(
    workspace: &Path,
    settings: &ContextSettings,
    id: &str,
) -> Option<SkillPack> {
    if !settings.skills.iter().any(|listed| listed == id) {
        return None;
    }
    load_skill(workspace, settings, id)
}

/// `description:` from YAML-style front matter, else the first content line.
fn skill_description(body: &str) -> String {
    let mut lines = body.lines();
    let mut front_matter = None;
    if lines.clone().next().map(str::trim) == Some("---") {
        lines.next();
        for line in lines.by_ref() {
            if line.trim() == "---" {
                break;
            }
            if let Some(value) = line.strip_prefix("description:") {
                front_matter = Some(value.trim().trim_matches(['"', '\'']).to_string());
            }
        }
    }
    let description = front_matter.unwrap_or_else(|| {
        lines
            .map(|line| line.trim().trim_start_matches('#').trim())
            .find(|line| !line.is_empty())
            .unwrap_or_default()
            .to_string()
    });
    description.chars().take(MAX_DESCRIPTION_CHARS).collect()
}

/// Append the lazy skill catalog to a composed system prompt.
pub fn append_skill_catalog(prompt: &mut String, catalog: &[SkillSummary]) {
    if catalog.is_empty() {
        return;
    }
    prompt.push_str(
        "\n\n# Skills\n\nCall `load_skill` with an id to read that procedure before you follow it.\n",
    );
    for skill in catalog {
        prompt.push_str(&format!("\n- `{}`: {}", skill.id, skill.description));
    }
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

    fn write_pack(workspace: &Path, id: &str, body: &str) {
        let pack = workspace.join(".shikigami/skills").join(id);
        std::fs::create_dir_all(&pack).unwrap();
        std::fs::write(pack.join("SKILL.md"), body).unwrap();
    }

    #[test]
    fn listed_packs_reach_the_prompt_as_descriptions_only() {
        let dir = tempdir().unwrap();
        let ids: Vec<String> = (0..10).map(|n| format!("pack-{n}")).collect();
        for id in &ids {
            write_pack(
                dir.path(),
                id,
                &format!("---\nname: {id}\ndescription: when to use {id}\n---\n\nBODY of {id}\n"),
            );
        }
        let s = ContextSettings {
            skills: ids.clone(),
            ..Default::default()
        };
        let skills = skill_context(dir.path(), &s, true);
        let mut prompt = compose_system_prompt("BASE", None, &skills.eager);
        append_skill_catalog(&mut prompt, &skills.catalog);
        for id in &ids {
            assert!(
                prompt.contains(&format!("- `{id}`: when to use {id}")),
                "{prompt}"
            );
        }
        assert!(!prompt.contains("BODY"), "{prompt}");
    }

    #[test]
    fn eager_packs_stay_whole_and_leave_the_catalog() {
        let dir = tempdir().unwrap();
        write_pack(dir.path(), "always", "ALWAYS body\n");
        write_pack(dir.path(), "later", "# Later\n\nLATER body\n");
        let s = ContextSettings {
            skills: vec!["always".into(), "later".into()],
            eager_skills: vec!["always".into()],
            ..Default::default()
        };
        let skills = skill_context(dir.path(), &s, true);
        assert_eq!(skills.eager.len(), 1);
        assert_eq!(skills.eager[0].id, "always");
        assert_eq!(
            skills.catalog,
            [SkillSummary {
                id: "later".into(),
                description: "Later".into()
            }]
        );
        let without_tool = skill_context(dir.path(), &s, false);
        assert_eq!(without_tool.eager.len(), 2);
        assert!(without_tool.catalog.is_empty());
    }

    #[test]
    fn load_listed_skill_requires_the_allow_list() {
        let dir = tempdir().unwrap();
        write_pack(dir.path(), "on-disk", "body\n");
        assert!(load_listed_skill(dir.path(), &ContextSettings::default(), "on-disk").is_none());
        let s = ContextSettings {
            skills: vec!["on-disk".into()],
            ..Default::default()
        };
        assert!(load_listed_skill(dir.path(), &s, "on-disk").is_some());
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
