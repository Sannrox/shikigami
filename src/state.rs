//! Local harness state root (not the control-plane store).
//!
//! Created lazily when a run needs workspace storage. No install/`init` step.

use std::fs;
use std::path::{Path, PathBuf};

use thiserror::Error;

use crate::config::{Config, ConfigError, ConfigSource};

/// Filesystem layout for local Shikigami state.
///
/// Operational truth for governed operations lives in the governance plane
/// (sekai-chisei when selected). This root holds optional host config, scratch,
/// and run workspaces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateRoot {
    path: PathBuf,
}

#[derive(Debug, Error)]
pub enum StateError {
    #[error("failed to prepare state at {path}: {source}")]
    Prepare {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error(transparent)]
    Config(#[from] ConfigError),
}

impl StateRoot {
    pub const DEFAULT_DIRNAME: &'static str = ".shikigami-state";

    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn default_in(cwd: impl AsRef<Path>) -> Self {
        Self::new(cwd.as_ref().join(Self::DEFAULT_DIRNAME))
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn config_path(&self) -> PathBuf {
        Config::path_in(&self.path)
    }

    pub fn runs_dir(&self) -> PathBuf {
        self.path.join("runs")
    }

    pub fn exists(&self) -> bool {
        self.path.is_dir()
    }

    /// Resolve effective config: optional file under this root, then env.
    pub fn config(&self) -> Result<(Config, ConfigSource), StateError> {
        Ok(Config::resolve(self.config_path())?)
    }

    /// Resolve with full search path (CLI config, env, state, cwd).
    pub fn config_search(
        &self,
        explicit_config: Option<&Path>,
        cwd: &Path,
    ) -> Result<(Config, ConfigSource), StateError> {
        Ok(Config::resolve_search(explicit_config, self.path(), cwd)?)
    }

    /// Create directories needed to host run workspaces. Idempotent.
    pub fn ensure_ready_for_runs(&self) -> Result<(), StateError> {
        fs::create_dir_all(self.runs_dir()).map_err(|source| StateError::Prepare {
            path: self.path.clone(),
            source,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn config_works_without_prior_setup() {
        let dir = tempdir().expect("tempdir");
        let root = StateRoot::default_in(dir.path());
        assert!(!root.exists());
        let (config, source) = root.config().expect("config");
        assert_eq!(config.version, Config::CURRENT_VERSION);
        assert!(matches!(source, ConfigSource::Defaults));
    }

    #[test]
    fn ensure_ready_for_runs_is_idempotent() {
        let dir = tempdir().expect("tempdir");
        let root = StateRoot::default_in(dir.path());
        root.ensure_ready_for_runs().expect("prepare");
        root.ensure_ready_for_runs().expect("prepare again");
        assert!(root.runs_dir().is_dir());
    }
}
