//! Local harness state root (not the control-plane store).

use std::fs;
use std::path::{Path, PathBuf};

use thiserror::Error;

use crate::config::{Config, ConfigError};

/// Filesystem layout for local Shikigami state.
///
/// Operational truth for governed operations lives in sekai-chisei. This root
/// holds only harness-local install config, scratch, and run workspaces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateRoot {
    path: PathBuf,
}

#[derive(Debug, Error)]
pub enum StateError {
    #[error("state root does not exist: {0}")]
    Missing(PathBuf),
    #[error("failed to create state at {path}: {source}")]
    Create {
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
        self.path.is_dir() && self.config_path().is_file()
    }

    /// Create the state root and a default config if missing.
    pub fn init(&self) -> Result<Config, StateError> {
        fs::create_dir_all(self.runs_dir()).map_err(|source| StateError::Create {
            path: self.path.clone(),
            source,
        })?;
        let config_path = self.config_path();
        if config_path.is_file() {
            return Ok(Config::load(config_path)?);
        }
        let config = Config::default();
        config.save(config_path)?;
        Ok(config)
    }

    pub fn load_config(&self) -> Result<Config, StateError> {
        if !self.exists() {
            return Err(StateError::Missing(self.path.clone()));
        }
        Ok(Config::load(self.config_path())?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn init_is_idempotent() {
        let dir = tempdir().expect("tempdir");
        let root = StateRoot::default_in(dir.path());
        let first = root.init().expect("init");
        let second = root.init().expect("re-init");
        assert_eq!(first, second);
        assert!(root.exists());
        assert!(root.runs_dir().is_dir());
    }
}
