//! Local configuration for a Shikigami installation.

use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// On-disk configuration for a workspace-scoped harness install.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Config {
    /// Schema version for future migrations.
    pub version: u32,
    /// Optional absolute or relative path to a sekai-chisei endpoint/socket.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub control_plane: Option<String>,
    /// Optional tenkai environment name this host belongs to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenkai_environment: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            version: 1,
            control_plane: None,
            tenkai_environment: None,
        }
    }
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read config at {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to write config at {path}: {source}")]
    Write {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse config at {path}: {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },
    #[error("failed to serialize config: {0}")]
    Serialize(#[from] toml::ser::Error),
    #[error("unsupported config version {found}; expected {expected}")]
    UnsupportedVersion { found: u32, expected: u32 },
}

impl Config {
    pub const FILENAME: &'static str = "shikigami.toml";
    pub const CURRENT_VERSION: u32 = 1;

    pub fn path_in(root: impl AsRef<Path>) -> PathBuf {
        root.as_ref().join(Self::FILENAME)
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let raw = fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        let config: Self = toml::from_str(&raw).map_err(|source| ConfigError::Parse {
            path: path.to_path_buf(),
            source,
        })?;
        if config.version != Self::CURRENT_VERSION {
            return Err(ConfigError::UnsupportedVersion {
                found: config.version,
                expected: Self::CURRENT_VERSION,
            });
        }
        Ok(config)
    }

    pub fn save(&self, path: impl AsRef<Path>) -> Result<(), ConfigError> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|source| ConfigError::Write {
                path: path.to_path_buf(),
                source,
            })?;
        }
        let raw = toml::to_string_pretty(self)?;
        fs::write(path, raw).map_err(|source| ConfigError::Write {
            path: path.to_path_buf(),
            source,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn round_trips_default_config() {
        let dir = tempdir().expect("tempdir");
        let path = Config::path_in(dir.path());
        Config::default().save(&path).expect("save");
        let loaded = Config::load(&path).expect("load");
        assert_eq!(loaded, Config::default());
    }
}
