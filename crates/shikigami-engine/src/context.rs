//! Project rules and other run context attachments.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

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

/// Load every existing listed rules file from the workspace up to the git root.
///
/// Directories are visited git-root first, then down to the workspace, so
/// nearer files sit after repo files. At each directory, names are attached in
/// `rules_filenames` order. Missing names are skipped. `..`, `/`, and `\\` are
/// ignored (flat names only). Without a `.git` ancestor (file or directory),
/// only the workspace directory is read. Files above the git root are never
/// loaded. A shared `max_rules_bytes` budget covers every file: the file that
/// would cross the cap is truncated with the existing marker and later files
/// are omitted. Headings and attribution use the path relative to the git root.
pub fn load_project_rules(workspace: &Path, settings: &ContextSettings) -> Vec<ProjectRules> {
    if !settings.load_project_rules {
        return Vec::new();
    }
    let max = settings.max_rules_bytes.clamp(1, MAX_DEFAULT * 4);
    let mut remaining = max;
    let mut loaded = Vec::new();
    let dirs = rules_search_dirs(workspace);
    let git_root = dirs.first().filter(|dir| dir.join(".git").exists());
    for dir in &dirs {
        if remaining == 0 {
            break;
        }
        for name in &settings.rules_filenames {
            if remaining == 0 {
                break;
            }
            if name.contains("..") || name.contains('/') || name.contains('\\') {
                continue; // only flat names in each directory
            }
            let path = dir.join(name);
            if !path.is_file() {
                continue;
            }
            let Some((body, digest, truncated)) =
                load_truncated_text(&path, remaining, "\n\n… [project rules truncated]\n")
            else {
                continue;
            };
            if truncated {
                remaining = 0;
            } else {
                remaining = remaining.saturating_sub(body.len());
            }
            loaded.push(ProjectRules {
                filename: rules_filename(dir, git_root.map(PathBuf::as_path), name),
                body,
                digest,
                truncated,
            });
        }
    }
    loaded
}

/// Directories to load from, git-root first then down to `workspace`.
/// Without a `.git` ancestor, only `workspace` (never the filesystem root).
fn rules_search_dirs(workspace: &Path) -> Vec<PathBuf> {
    let mut chain = Vec::new();
    let mut cur = workspace;
    loop {
        chain.push(cur.to_path_buf());
        if cur.join(".git").exists() {
            chain.reverse();
            return chain;
        }
        match cur.parent() {
            Some(parent) => cur = parent,
            None => return vec![workspace.to_path_buf()],
        }
    }
}

fn rules_filename(dir: &Path, git_root: Option<&Path>, name: &str) -> String {
    let Some(root) = git_root else {
        return name.to_string();
    };
    match dir.strip_prefix(root) {
        Ok(rel) if rel.as_os_str().is_empty() => name.to_string(),
        Ok(rel) => format!("{}/{name}", rel.to_string_lossy().replace('\\', "/")),
        Err(_) => name.to_string(),
    }
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

/// Roots searched for TUI skill commands (`/name`, `/skill:name`). Runtime packs first, then `.agents/skills`.
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

/// Skill ids the TUI can offer as slash commands (`/name` or `/skill:name`).
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

/// One project rules file the run actually attached (no body).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttachedRule {
    pub filename: String,
    pub bytes: u64,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub truncated: bool,
}

/// Standing attachments composed into this run's system prompt.
///
/// Metadata only: filenames, sizes, skill ids, nested tools, model/mode.
/// File bodies stay in the prompt.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttachedContext {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rules: Vec<AttachedRule>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub eager: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub catalog: Vec<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub nested: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub model: String,
}

impl AttachedContext {
    /// Snapshot what `compose_system_prompt` attached, without the bodies.
    pub fn from_parts(
        rules: &[ProjectRules],
        skills: &SkillContext,
        nested: bool,
        mode: Option<String>,
        model: impl Into<String>,
    ) -> Self {
        Self {
            rules: rules
                .iter()
                .map(|rule| AttachedRule {
                    filename: rule.filename.clone(),
                    bytes: rule.body.len() as u64,
                    truncated: rule.truncated,
                })
                .collect(),
            eager: skills.eager.iter().map(|pack| pack.id.clone()).collect(),
            catalog: skills
                .catalog
                .iter()
                .map(|skill| skill.id.clone())
                .collect(),
            nested,
            mode: mode.filter(|name| !name.is_empty()),
            model: model.into(),
        }
    }
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

/// Append attached MCP names and truncated descriptions for lazy schemas.
pub fn append_mcp_tool_catalog(prompt: &mut String, catalog: &[(String, String)]) {
    if catalog.is_empty() {
        return;
    }
    prompt.push_str(
        "\n\n# MCP tools\n\nCall `load_mcp_tool` with a name to offer that tool's schema before you call it.\n",
    );
    for (name, description) in catalog {
        let description: String = description.chars().take(MAX_DESCRIPTION_CHARS).collect();
        prompt.push_str(&format!("\n- `{name}`: {description}"));
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

/// Conservative standing-token estimate: 4 characters ≈ 1 token.
pub const STANDING_CHARS_PER_TOKEN: u64 = 4;

/// One eager or catalog pack as standing bytes (no body).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StandingPack {
    pub id: String,
    pub bytes: u64,
}

/// Standing prompt the next turn will send before the user message.
///
/// Rules and eager bodies, lazy catalog lines, and JSON of offered tool
/// definitions. Session messages are not included.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StandingContext {
    pub bytes: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rules: Vec<AttachedRule>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub eager: Vec<StandingPack>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub catalog: Vec<StandingPack>,
    pub tool_schema_bytes: u64,
}

impl StandingContext {
    /// `bytes` rounded up at [`STANDING_CHARS_PER_TOKEN`].
    pub fn tokens_est(&self) -> u64 {
        standing_tokens(self.bytes)
    }

    /// Largest named contributor, for fail-closed messages.
    pub fn largest_source(&self) -> Option<(u64, String)> {
        let mut items: Vec<(u64, String)> = self
            .rules
            .iter()
            .map(|rule| (rule.bytes, format!("project rules `{}`", rule.filename)))
            .chain(
                self.eager
                    .iter()
                    .map(|pack| (pack.bytes, format!("eager skill `{}`", pack.id))),
            )
            .chain(
                self.catalog
                    .iter()
                    .map(|pack| (pack.bytes, format!("skill catalog `{}`", pack.id))),
            )
            .collect();
        if self.tool_schema_bytes > 0 {
            items.push((self.tool_schema_bytes, "offered tool schemas".to_string()));
        }
        items.into_iter().max_by_key(|(bytes, _)| *bytes)
    }
}

/// Token estimate for this gate only: 4 characters ≈ 1 token, rounded up.
pub fn standing_tokens(bytes: u64) -> u64 {
    bytes.div_ceil(STANDING_CHARS_PER_TOKEN)
}

/// JSON byte length of offered tool definitions (name, description, parameters).
pub fn offered_tool_schema_bytes<'a, I>(tools: I) -> u64
where
    I: IntoIterator<Item = (&'a str, &'a str, &'a str)>,
{
    let items: Vec<serde_json::Value> = tools
        .into_iter()
        .map(|(name, description, schema)| {
            let parameters: serde_json::Value =
                serde_json::from_str(schema).unwrap_or_else(|_| serde_json::json!({}));
            serde_json::json!({
                "name": name,
                "description": description,
                "parameters": parameters,
            })
        })
        .collect();
    serde_json::to_vec(&items)
        .map(|raw| raw.len() as u64)
        .unwrap_or(0)
}

/// Whether a run with this tool list will keep listed packs lazy.
pub fn offers_lazy_skills(enabled: &[String], settings: &ContextSettings) -> bool {
    enabled.iter().any(|tool| tool == "load_skill")
        && settings
            .skills
            .iter()
            .any(|id| !settings.eager_skills.contains(id))
}

/// Measure standing context the compose path would attach.
///
/// `tool_schema_bytes` is JSON of `definitions()` offered on turn one.
/// `mcp_catalog` is the lazy MCP name/description list (empty when schemas
/// are offered immediately or no MCP is attached).
pub fn measure_standing(
    workspace: &Path,
    settings: &ContextSettings,
    lazy_skills: bool,
    tool_schema_bytes: u64,
    mcp_catalog: &[(String, String)],
) -> StandingContext {
    let rules = load_project_rules(workspace, settings);
    let skills = skill_context(workspace, settings, lazy_skills);
    let mut catalog = Vec::new();
    let mut catalog_bytes = 0_u64;
    if !skills.catalog.is_empty() {
        let mut prompt = String::new();
        append_skill_catalog(&mut prompt, &skills.catalog);
        catalog_bytes += prompt.len() as u64;
        catalog.extend(skills.catalog.iter().map(|skill| StandingPack {
            id: skill.id.clone(),
            bytes: format!("\n- `{}`: {}", skill.id, skill.description).len() as u64,
        }));
    }
    if !mcp_catalog.is_empty() {
        let mut prompt = String::new();
        append_mcp_tool_catalog(&mut prompt, mcp_catalog);
        catalog_bytes += prompt.len() as u64;
        catalog.extend(mcp_catalog.iter().map(|(name, description)| {
            let description: String = description.chars().take(MAX_DESCRIPTION_CHARS).collect();
            StandingPack {
                id: name.clone(),
                bytes: format!("\n- `{name}`: {description}").len() as u64,
            }
        }));
    }
    let eager: Vec<StandingPack> = skills
        .eager
        .iter()
        .map(|pack| StandingPack {
            id: pack.id.clone(),
            bytes: pack.body.len() as u64,
        })
        .collect();
    let attached_rules = AttachedContext::from_parts(&rules, &skills, false, None, "").rules;
    let rules_bytes: u64 = attached_rules.iter().map(|rule| rule.bytes).sum();
    let eager_bytes: u64 = eager.iter().map(|pack| pack.bytes).sum();
    StandingContext {
        bytes: rules_bytes
            .saturating_add(eager_bytes)
            .saturating_add(catalog_bytes)
            .saturating_add(tool_schema_bytes),
        rules: attached_rules,
        eager,
        catalog,
        tool_schema_bytes,
    }
}

/// Fail-closed message when standing tokens exceed `percent` of `window_tokens`.
///
/// `None` when the window is unset or standing fits. Names the largest source
/// and the cap.
pub fn standing_overflow(
    standing: &StandingContext,
    window_tokens: Option<u64>,
    percent: u32,
) -> Option<String> {
    let window = window_tokens?;
    let cap_tokens = window.saturating_mul(u64::from(percent)) / 100;
    let tokens = standing.tokens_est();
    if tokens <= cap_tokens {
        return None;
    }
    let cap_bytes = cap_tokens.saturating_mul(STANDING_CHARS_PER_TOKEN);
    let source = standing
        .largest_source()
        .map(|(bytes, name)| format!("; {name} is {bytes} bytes"))
        .unwrap_or_default();
    Some(format!(
        "standing context {} bytes (~{tokens} tokens) exceeds context.standing_max_percent={percent}% of run.context_window_tokens={window} (cap {cap_tokens} tokens / {cap_bytes} bytes){source}",
        standing.bytes
    ))
}

/// Compose system prompt with project rules and skill packs.
pub fn compose_system_prompt(base: &str, rules: &[ProjectRules], skills: &[SkillPack]) -> String {
    let mut out = base.to_string();
    for r in rules {
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
        let rules = load_project_rules(dir.path(), &ContextSettings::default());
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].filename, "AGENTS.md");
        assert!(rules[0].body.contains("small PRs"));
        assert!(!rules[0].truncated);
        assert_eq!(rules[0].digest.len(), 64);
    }

    #[test]
    fn missing_is_empty() {
        let dir = tempdir().unwrap();
        assert!(load_project_rules(dir.path(), &ContextSettings::default()).is_empty());
    }

    #[test]
    fn disable_skips() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("AGENTS.md"), "x\n").unwrap();
        std::fs::write(dir.path().join("shikigami.rules.md"), "y\n").unwrap();
        let s = ContextSettings {
            load_project_rules: false,
            ..Default::default()
        };
        assert!(load_project_rules(dir.path(), &s).is_empty());
    }

    #[test]
    fn loads_every_listed_file_in_order() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("AGENTS.md"), "agents body\n").unwrap();
        std::fs::write(dir.path().join("shikigami.rules.md"), "harness body\n").unwrap();
        let rules = load_project_rules(dir.path(), &ContextSettings::default());
        assert_eq!(
            rules
                .iter()
                .map(|r| r.filename.as_str())
                .collect::<Vec<_>>(),
            ["AGENTS.md", "shikigami.rules.md"]
        );
        assert_ne!(rules[0].digest, rules[1].digest);
        let prompt = compose_system_prompt("BASE", &rules, &[]);
        let agents_at = prompt.find("# Project rules (`AGENTS.md`)").unwrap();
        let harness_at = prompt
            .find("# Project rules (`shikigami.rules.md`)")
            .unwrap();
        assert!(agents_at < harness_at, "{prompt}");
        assert!(prompt.contains("agents body"));
        assert!(prompt.contains("harness body"));
    }

    #[test]
    fn slash_and_missing_names_are_skipped() {
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("nested")).unwrap();
        std::fs::write(dir.path().join("nested/secret.md"), "nope\n").unwrap();
        std::fs::write(dir.path().join("AGENTS.md"), "keep\n").unwrap();
        let s = ContextSettings {
            rules_filenames: vec![
                "nested/secret.md".into(),
                "missing.md".into(),
                "AGENTS.md".into(),
            ],
            ..Default::default()
        };
        let rules = load_project_rules(dir.path(), &s);
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].filename, "AGENTS.md");
        assert!(rules[0].body.contains("keep"));
        let prompt = compose_system_prompt("BASE", &rules, &[]);
        assert!(!prompt.contains("nope"));
    }

    #[test]
    fn shared_byte_budget_truncates_and_omits_later_files() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("AGENTS.md"), "AAAA").unwrap();
        std::fs::write(dir.path().join("shikigami.rules.md"), "BBBB").unwrap();
        let s = ContextSettings {
            max_rules_bytes: 3,
            ..Default::default()
        };
        let rules = load_project_rules(dir.path(), &s);
        assert_eq!(rules.len(), 1);
        assert!(rules[0].truncated);
        assert!(rules[0].body.starts_with("AAA"));
        assert!(rules[0].body.contains("… [project rules truncated]"));
        let prompt = compose_system_prompt("BASE", &rules, &[]);
        assert!(!prompt.contains("BBBB"));
        assert!(!prompt.contains("shikigami.rules.md"));
    }

    #[test]
    fn later_file_is_truncated_when_it_crosses_the_cap() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("AGENTS.md"), "AA").unwrap();
        std::fs::write(dir.path().join("shikigami.rules.md"), "BBBB").unwrap();
        let s = ContextSettings {
            max_rules_bytes: 4,
            ..Default::default()
        };
        let rules = load_project_rules(dir.path(), &s);
        assert_eq!(rules.len(), 2);
        assert!(!rules[0].truncated);
        assert_eq!(rules[0].body, "AA");
        assert!(rules[1].truncated);
        assert!(rules[1].body.starts_with("BB"));
        assert!(rules[1].body.contains("… [project rules truncated]"));
    }

    fn write_dummy_git(root: &Path) {
        std::fs::create_dir_all(root.join(".git")).unwrap();
    }

    #[test]
    fn loads_git_ancestors_root_first_nearest_last() {
        let dir = tempdir().unwrap();
        let repo = dir.path().join("repo");
        let pkg = repo.join("pkg");
        std::fs::create_dir_all(&pkg).unwrap();
        write_dummy_git(&repo);
        std::fs::write(repo.join("AGENTS.md"), "root rules\n").unwrap();
        std::fs::write(repo.join("shikigami.rules.md"), "root harness\n").unwrap();
        std::fs::write(pkg.join("AGENTS.md"), "pkg rules\n").unwrap();
        let rules = load_project_rules(&pkg, &ContextSettings::default());
        assert_eq!(
            rules
                .iter()
                .map(|r| r.filename.as_str())
                .collect::<Vec<_>>(),
            ["AGENTS.md", "shikigami.rules.md", "pkg/AGENTS.md"]
        );
        assert!(rules[0].body.contains("root rules"));
        assert!(rules[1].body.contains("root harness"));
        assert!(rules[2].body.contains("pkg rules"));
        let prompt = compose_system_prompt("BASE", &rules, &[]);
        let root_at = prompt.find("# Project rules (`AGENTS.md`)").unwrap();
        let harness_at = prompt
            .find("# Project rules (`shikigami.rules.md`)")
            .unwrap();
        let pkg_at = prompt.find("# Project rules (`pkg/AGENTS.md`)").unwrap();
        assert!(root_at < harness_at && harness_at < pkg_at, "{prompt}");
    }

    #[test]
    fn git_file_counts_as_root() {
        let dir = tempdir().unwrap();
        let repo = dir.path().join("repo");
        let pkg = repo.join("pkg");
        std::fs::create_dir_all(&pkg).unwrap();
        std::fs::write(repo.join(".git"), "gitdir: /elsewhere\n").unwrap();
        std::fs::write(repo.join("AGENTS.md"), "root rules\n").unwrap();
        std::fs::write(pkg.join("AGENTS.md"), "pkg rules\n").unwrap();
        let rules = load_project_rules(&pkg, &ContextSettings::default());
        assert_eq!(
            rules
                .iter()
                .map(|r| r.filename.as_str())
                .collect::<Vec<_>>(),
            ["AGENTS.md", "pkg/AGENTS.md"]
        );
    }

    #[test]
    fn without_git_loads_only_workspace() {
        let dir = tempdir().unwrap();
        let parent = dir.path().join("outer");
        let ws = parent.join("ws");
        std::fs::create_dir_all(&ws).unwrap();
        std::fs::write(parent.join("AGENTS.md"), "parent must not load\n").unwrap();
        std::fs::write(ws.join("AGENTS.md"), "workspace rules\n").unwrap();
        let rules = load_project_rules(&ws, &ContextSettings::default());
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].filename, "AGENTS.md");
        assert!(rules[0].body.contains("workspace rules"));
        let prompt = compose_system_prompt("BASE", &rules, &[]);
        assert!(!prompt.contains("parent must not load"));
    }

    #[test]
    fn rules_above_git_root_are_not_loaded() {
        let dir = tempdir().unwrap();
        let outer = dir.path().join("outer");
        let repo = outer.join("repo");
        let pkg = repo.join("pkg");
        std::fs::create_dir_all(&pkg).unwrap();
        write_dummy_git(&repo);
        std::fs::write(outer.join("AGENTS.md"), "above git root\n").unwrap();
        std::fs::write(repo.join("AGENTS.md"), "root rules\n").unwrap();
        std::fs::write(pkg.join("AGENTS.md"), "pkg rules\n").unwrap();
        let rules = load_project_rules(&pkg, &ContextSettings::default());
        assert_eq!(
            rules
                .iter()
                .map(|r| r.filename.as_str())
                .collect::<Vec<_>>(),
            ["AGENTS.md", "pkg/AGENTS.md"]
        );
        let prompt = compose_system_prompt("BASE", &rules, &[]);
        assert!(!prompt.contains("above git root"));
    }

    #[test]
    fn shared_byte_budget_spans_git_levels() {
        let dir = tempdir().unwrap();
        let repo = dir.path().join("repo");
        let pkg = repo.join("pkg");
        std::fs::create_dir_all(&pkg).unwrap();
        write_dummy_git(&repo);
        std::fs::write(repo.join("AGENTS.md"), "AAAA").unwrap();
        std::fs::write(pkg.join("AGENTS.md"), "BBBB").unwrap();
        let s = ContextSettings {
            max_rules_bytes: 3,
            ..Default::default()
        };
        let rules = load_project_rules(&pkg, &s);
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].filename, "AGENTS.md");
        assert!(rules[0].truncated);
        let prompt = compose_system_prompt("BASE", &rules, &[]);
        assert!(!prompt.contains("BBBB"));
        assert!(!prompt.contains("pkg/AGENTS.md"));
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
        let mut prompt = compose_system_prompt("BASE", &[], &skills.eager);
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
        let composed = compose_system_prompt("BASE", &[], &packs);
        assert!(composed.contains("Skill `demo`"));
        assert!(composed.contains("tests first"));
    }

    #[test]
    fn attached_context_omits_bodies_and_splits_eager_from_catalog() {
        let rules = vec![ProjectRules {
            filename: "AGENTS.md".into(),
            body: "prefer tests\n".into(),
            digest: "abc".into(),
            truncated: false,
        }];
        let skills = SkillContext {
            eager: vec![SkillPack {
                id: "always".into(),
                body: "secret body".into(),
                digest: "def".into(),
                truncated: true,
            }],
            catalog: vec![SkillSummary {
                id: "demo".into(),
                description: "demo pack".into(),
            }],
        };
        let attached = AttachedContext::from_parts(&rules, &skills, false, None, "auto");
        assert_eq!(attached.rules[0].filename, "AGENTS.md");
        assert_eq!(attached.rules[0].bytes, "prefer tests\n".len() as u64);
        assert!(!attached.rules[0].truncated);
        assert_eq!(attached.eager, vec!["always"]);
        assert_eq!(attached.catalog, vec!["demo"]);
        assert!(!attached.nested);
        assert_eq!(attached.model, "auto");
        let json = serde_json::to_string(&attached).unwrap();
        assert!(!json.contains("secret body"), "{json}");
        assert!(!json.contains("prefer tests"), "{json}");
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

    #[test]
    fn mcp_tool_catalog_lists_names_and_caps_descriptions() {
        let long = "x".repeat(250);
        let catalog = vec![
            ("mcp.demo.echo".into(), "echo one line".into()),
            ("mcp.demo.wide".into(), long),
        ];
        let mut prompt = String::from("BASE");
        append_mcp_tool_catalog(&mut prompt, &catalog);
        assert!(prompt.contains("`load_mcp_tool`"), "{prompt}");
        assert!(
            prompt.contains("- `mcp.demo.echo`: echo one line"),
            "{prompt}"
        );
        assert!(prompt.contains("- `mcp.demo.wide`: "), "{prompt}");
        let wide_line = prompt
            .lines()
            .find(|line| line.contains("`mcp.demo.wide`"))
            .unwrap();
        let description = wide_line.rsplit(": ").next().unwrap();
        assert_eq!(description.chars().count(), MAX_DESCRIPTION_CHARS);
        let mut empty = String::from("BASE");
        append_mcp_tool_catalog(&mut empty, &[]);
        assert_eq!(empty, "BASE");
    }

    #[test]
    fn standing_names_rules_eager_and_sum() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("AGENTS.md"), "a".repeat(100)).unwrap();
        write_pack(dir.path(), "demo", &"b".repeat(200));
        let s = ContextSettings {
            skills: vec!["demo".into()],
            eager_skills: vec!["demo".into()],
            ..Default::default()
        };
        let standing = measure_standing(dir.path(), &s, true, 50, &[]);
        assert_eq!(standing.rules[0].filename, "AGENTS.md");
        assert_eq!(standing.rules[0].bytes, 100);
        assert_eq!(standing.eager[0].id, "demo");
        assert_eq!(standing.eager[0].bytes, 200);
        assert_eq!(standing.tool_schema_bytes, 50);
        assert_eq!(standing.bytes, 350);
        assert!(standing_overflow(&standing, None, 25).is_none());
        assert!(standing_overflow(&standing, Some(1000), 25).is_none());
    }

    #[test]
    fn standing_overflow_names_the_eager_skill_and_cap() {
        let dir = tempdir().unwrap();
        write_pack(dir.path(), "huge", &"x".repeat(2000));
        let s = ContextSettings {
            skills: vec!["huge".into()],
            eager_skills: vec!["huge".into()],
            ..Default::default()
        };
        let standing = measure_standing(dir.path(), &s, true, 0, &[]);
        assert_eq!(standing.bytes, 2000);
        let error = standing_overflow(&standing, Some(1000), 25).unwrap();
        assert!(error.contains("eager skill `huge`"), "{error}");
        assert!(error.contains("2000 bytes"), "{error}");
        assert!(error.contains("standing_max_percent=25"), "{error}");
        assert!(error.contains("context_window_tokens=1000"), "{error}");
        assert!(error.contains("cap 250 tokens"), "{error}");
    }

    #[test]
    fn standing_without_load_skill_counts_listed_packs_as_eager() {
        let dir = tempdir().unwrap();
        write_pack(dir.path(), "listed", "BODY of listed\n");
        let s = ContextSettings {
            skills: vec!["listed".into()],
            ..Default::default()
        };
        let lazy = measure_standing(dir.path(), &s, true, 0, &[]);
        assert!(lazy.eager.is_empty(), "{lazy:?}");
        assert_eq!(lazy.catalog[0].id, "listed");
        let eager = measure_standing(dir.path(), &s, false, 0, &[]);
        assert_eq!(eager.eager[0].id, "listed");
        assert_eq!(eager.eager[0].bytes, "BODY of listed\n".len() as u64);
        assert!(eager.catalog.is_empty(), "{eager:?}");
    }
}
