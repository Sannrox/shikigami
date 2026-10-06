//! Effective settings resolution protocol.
//!
//! This private deep module owns source discovery and the complete precedence
//! order. Callers receive one validated effective [`Config`] and never need to
//! reproduce the ordering themselves.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use super::{Config, ConfigError, ConfigResolutionError, ConfigSource};

pub(super) fn resolve(
    path: &Path,
    model_override: Option<&str>,
) -> Result<(Config, ConfigSource), ConfigError> {
    let (mut config, source) = if path.is_file() {
        (load(path)?, ConfigSource::File(path.to_path_buf()))
    } else {
        (Config::default(), ConfigSource::Defaults)
    };
    config.apply_profile_presets();
    config.apply_env();
    apply_model_override(&mut config, model_override)?;
    config.validate()?;
    Ok((config, source))
}

pub(super) fn resolve_search(
    explicit: Option<&Path>,
    state_root: &Path,
    cwd: &Path,
) -> Result<(Config, ConfigSource), ConfigError> {
    let path = if let Some(path) = explicit {
        path.to_path_buf()
    } else if let Ok(path) = env::var(Config::CONFIG_PATH_ENV)
        && !path.is_empty()
    {
        PathBuf::from(path)
    } else {
        let under_state = Config::path_in(state_root);
        if under_state.is_file() {
            under_state
        } else {
            let under_cwd = cwd.join(Config::FILENAME);
            if under_cwd.is_file() {
                under_cwd
            } else {
                under_state
            }
        }
    };
    resolve(&path, None)
}

pub(super) fn resolve_search_with_model(
    explicit: Option<&Path>,
    state_root: &Path,
    cwd: &Path,
    model_override: Option<&str>,
) -> Result<(Config, ConfigSource), ConfigResolutionError> {
    let (mut config, source) =
        resolve_search(explicit, state_root, cwd).map_err(ConfigResolutionError::Search)?;
    apply_model_override(&mut config, model_override).map_err(ConfigResolutionError::Override)?;
    config.validate().map_err(ConfigResolutionError::Override)?;
    Ok((config, source))
}

pub(super) fn load(path: &Path) -> Result<Config, ConfigError> {
    let raw = fs::read_to_string(path).map_err(|source| ConfigError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    let mut config: Config = toml::from_str(&raw).map_err(|source| ConfigError::Parse {
        path: path.to_path_buf(),
        source,
    })?;
    if config.version != Config::CURRENT_VERSION {
        return Err(ConfigError::UnsupportedVersion {
            found: config.version,
            expected: Config::CURRENT_VERSION,
        });
    }
    config.apply_profile_presets();
    Ok(config)
}

fn apply_model_override(config: &mut Config, model: Option<&str>) -> Result<(), ConfigError> {
    let Some(model) = model.map(str::trim) else {
        return Ok(());
    };
    if model.is_empty() {
        return Err(ConfigError::Invalid(
            "model override must not be empty".into(),
        ));
    }
    config.model.model = model.into();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn host_model_override_is_applied_after_file_settings() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join(Config::FILENAME);
        fs::write(
            &path,
            "version = 1\n[model]\nadapter = \"scripted\"\nmodel = \"from-file\"\n",
        )
        .expect("write config");

        let (config, source) = resolve(&path, Some("from-host")).expect("resolve");

        assert_eq!(config.model.model, "from-host");
        assert_eq!(source, ConfigSource::File(path));
    }

    #[test]
    fn empty_host_model_override_fails_in_resolution() {
        let dir = tempdir().expect("tempdir");
        let error = resolve(&dir.path().join("missing.toml"), Some("  "))
            .expect_err("empty model must fail");
        assert!(
            error
                .to_string()
                .contains("model override must not be empty")
        );
    }
}
