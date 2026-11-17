//! Narrowing presets (ADR 0022).
//!
//! A preset selects a subset of the base config's tools and adds a prompt
//! section. Applying one is the last step of settings resolution and fails
//! closed on anything that would widen authority.

use std::path::Path;

use serde::Deserialize;

use super::{
    Config, ConfigError, MCP_GATEWAY_NAME, PermissionMode, SECURITY_FINDER_PROFILE,
    is_site_agent_profile,
};

/// Presets compiled into the binary, by name. `code` is no preset.
const BUILT_IN: &[(&str, &str)] = &[(
    "definition-work",
    include_str!("../../presets/definition-work.toml"),
)];

/// Name of the implicit default: the base config unchanged.
pub const DEFAULT_PRESET: &str = "code";

/// A preset applied to a [`Config`]. Not part of the settings file.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct AppliedPreset {
    pub name: String,
    /// `<name>@sha256:<hex>` over the preset text. Recorded in the checkpoint.
    pub id: String,
    /// Appended to the system prompt as a `# Preset` section.
    pub prompt: String,
    /// `mcp.<server>.<tool>` names; only these MCP tools are attached.
    pub mcp_tools: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PresetFile {
    name: String,
    #[serde(default)]
    #[allow(dead_code)] // documentation for operators; not used at run time
    description: String,
    #[serde(default)]
    prompt: String,
    tools: PresetTools,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PresetTools {
    enabled: Vec<String>,
}

/// Names of the built-in presets, the default first.
pub fn built_in_presets() -> Vec<&'static str> {
    std::iter::once(DEFAULT_PRESET)
        .chain(BUILT_IN.iter().map(|(name, _)| *name))
        .collect()
}

/// Apply the preset `selector` names: a built-in name, or a path to a preset
/// file when it has a path separator or a `.toml` suffix.
pub(super) fn apply(config: &mut Config, selector: &str) -> Result<(), ConfigError> {
    let selector = selector.trim();
    if selector.is_empty() || selector == DEFAULT_PRESET {
        return Ok(());
    }
    let is_path = selector.contains(std::path::MAIN_SEPARATOR)
        || selector.contains('/')
        || selector.ends_with(".toml");
    let (raw, origin) = if is_path {
        let path = Path::new(selector);
        let raw = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        (raw, selector.to_owned())
    } else {
        let (_, raw) = BUILT_IN
            .iter()
            .find(|(name, _)| *name == selector)
            .ok_or_else(|| {
                ConfigError::Invalid(format!(
                    "unknown preset `{selector}`; built-in presets: {}",
                    built_in_presets().join(", ")
                ))
            })?;
        ((*raw).to_owned(), format!("built-in preset `{selector}`"))
    };
    let file: PresetFile =
        toml::from_str(&raw).map_err(|error| ConfigError::Invalid(format!("{origin}: {error}")))?;
    narrow(config, file, &raw)
}

fn narrow(config: &mut Config, file: PresetFile, raw: &str) -> Result<(), ConfigError> {
    let name = file.name.trim();
    let invalid = |reason: String| ConfigError::Invalid(format!("preset `{name}`: {reason}"));
    if name.is_empty() {
        return Err(ConfigError::Invalid("preset name must not be empty".into()));
    }
    if config.preset.is_some() {
        return Err(invalid("a preset is already applied".into()));
    }
    let profile = config.profile.name.as_str();
    if is_site_agent_profile(profile) || profile == SECURITY_FINDER_PROFILE {
        return Err(invalid(format!(
            "profile `{profile}` has a fixed role and takes no preset"
        )));
    }
    let (mcp_tools, built_in): (Vec<String>, Vec<String>) = file
        .tools
        .enabled
        .into_iter()
        .partition(|tool| tool.starts_with("mcp."));
    if built_in.is_empty() {
        return Err(invalid(
            "tools.enabled must list at least one built-in tool".into(),
        ));
    }
    let base = config.tools.effective_enabled();
    let widened: Vec<_> = built_in
        .iter()
        .filter(|tool| !base.contains(tool))
        .cloned()
        .collect();
    if !widened.is_empty() {
        return Err(invalid(format!(
            "the base config does not enable {}",
            widened.join(", ")
        )));
    }
    let servers: Vec<&str> = config
        .tools
        .mcp_servers
        .iter()
        .map(|server| server.name.as_str())
        .chain(
            config
                .tools
                .mcp_gateway
                .is_some()
                .then_some(MCP_GATEWAY_NAME),
        )
        .collect();
    let unattached: Vec<_> = mcp_tools
        .iter()
        .filter(|tool| {
            !servers.iter().any(|server| {
                tool.strip_prefix("mcp.")
                    .and_then(|rest| rest.strip_prefix(server))
                    .is_some_and(|rest| rest.starts_with('.') && rest.len() > 1)
            })
        })
        .cloned()
        .collect();
    if !unattached.is_empty() {
        return Err(invalid(format!(
            "the base config attaches no MCP server for {}",
            unattached.join(", ")
        )));
    }
    // MCP tools keep the base authority: an in-process governance adapter
    // allows one only when the base `tools.enabled` lists it, so a preset keeps
    // exactly those and never adds one.
    let mut enabled = built_in;
    enabled.extend(mcp_tools.iter().filter(|tool| base.contains(tool)).cloned());
    config.tools.mode = PermissionMode::Custom;
    config.tools.enabled = enabled;
    config.preset = Some(AppliedPreset {
        name: name.to_owned(),
        id: format!(
            "{name}@sha256:{}",
            crate::digest::sha256_hex(raw.replace("\r\n", "\n").as_bytes())
        ),
        prompt: file.prompt.trim().to_owned(),
        mcp_tools,
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SITE_AGENT_PROFILE;

    /// Base config attaching one MCP server named `sekai`.
    fn base() -> Config {
        let dir = tempfile::tempdir().unwrap();
        let path = Config::path_in(dir.path());
        std::fs::write(
            &path,
            "version = 1\n[[tools.mcp_servers]]\nname = \"sekai\"\ncommand = \"sekai-mcp\"\n",
        )
        .unwrap();
        Config::load(&path).unwrap()
    }

    fn preset(tools: &str) -> String {
        format!("name = \"p\"\nprompt = \"Do it.\"\n[tools]\nenabled = [{tools}]\n")
    }

    fn apply_text(config: &mut Config, text: &str) -> Result<(), ConfigError> {
        let file: PresetFile =
            toml::from_str(text).map_err(|error| ConfigError::Invalid(error.to_string()))?;
        narrow(config, file, text)
    }

    #[test]
    fn narrows_tools_and_records_id() {
        let mut config = base();
        apply_text(
            &mut config,
            &preset(r#""read_file", "report", "mcp.sekai.diff""#),
        )
        .unwrap();
        assert_eq!(config.tools.effective_enabled(), ["read_file", "report"]);
        let applied = config.preset.as_ref().unwrap();
        assert_eq!(applied.mcp_tools, ["mcp.sekai.diff"]);
        assert_eq!(applied.prompt, "Do it.");
        assert!(applied.id.starts_with("p@sha256:"), "{}", applied.id);
    }

    #[test]
    fn refuses_widening_unattached_servers_and_no_built_in() {
        let mut config = base();
        config.tools.mode = PermissionMode::Read;
        let err = apply_text(&mut config, &preset(r#""write_file""#)).unwrap_err();
        assert!(
            err.to_string().contains("does not enable write_file"),
            "{err}"
        );

        let err = apply_text(&mut base(), &preset(r#""report", "mcp.other.x""#)).unwrap_err();
        assert!(
            err.to_string().contains("no MCP server for mcp.other.x"),
            "{err}"
        );

        let err = apply_text(&mut base(), &preset(r#""mcp.sekai.diff""#)).unwrap_err();
        assert!(err.to_string().contains("at least one built-in"), "{err}");
    }

    #[test]
    fn refuses_unknown_keys_names_and_fixed_role_profiles() {
        let err = apply_text(
            &mut base(),
            &format!("model = \"x\"\n{}", preset(r#""report""#)),
        )
        .unwrap_err();
        assert!(err.to_string().contains("unknown field"), "{err}");

        let err = apply(&mut base(), "nope").unwrap_err();
        assert!(err.to_string().contains("definition-work"), "{err}");

        let mut site = base();
        site.profile.name = SITE_AGENT_PROFILE.into();
        let err = apply_text(&mut site, &preset(r#""report""#)).unwrap_err();
        assert!(err.to_string().contains("fixed role"), "{err}");
    }

    #[test]
    fn default_name_is_no_preset_and_built_ins_parse() {
        let mut config = base();
        apply(&mut config, DEFAULT_PRESET).unwrap();
        assert!(config.preset.is_none());
        for (name, raw) in BUILT_IN {
            let file: PresetFile = toml::from_str(raw).unwrap();
            assert_eq!(file.name, *name);
        }
        apply(&mut config, "definition-work").unwrap();
        assert_eq!(config.tools.effective_enabled(), ["report"]);
    }
}
