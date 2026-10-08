//! Local harness state root (not the control-plane store).
//!
//! Created when `Harness` is constructed (`ensure_ready_for_runs`). No
//! install/`init` step.

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
    #[error(
        "no user state directory; set --state or SHIKIGAMI_STATE (need XDG_STATE_HOME, HOME, or LOCALAPPDATA)"
    )]
    MissingUserState,
    #[error(transparent)]
    Config(#[from] ConfigError),
}

impl StateRoot {
    /// Project-local dirname for embedders and operators who set `--state` here.
    pub const DEFAULT_DIRNAME: &'static str = ".shikigami-state";

    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// Cwd-local root (`<cwd>/.shikigami-state`). Embedders and tests use this
    /// when they want an explicit directory. The CLI default is [`user_default`].
    pub fn default_in(cwd: impl AsRef<Path>) -> Self {
        Self::new(cwd.as_ref().join(Self::DEFAULT_DIRNAME))
    }

    /// Platform user-state directory keyed by `cwd`.
    ///
    /// `XDG_STATE_HOME` wins on every OS. Otherwise macOS uses Application
    /// Support, Windows uses `%LOCALAPPDATA%`, and other Unix uses
    /// `~/.local/state`.
    pub fn user_default(cwd: impl AsRef<Path>) -> Result<Self, StateError> {
        Ok(Self::new(user_base_dir()?.join(encode_cwd(cwd.as_ref()))))
    }

    /// `--state` / `SHIKIGAMI_STATE` when set, else [`user_default`].
    pub fn resolve_host(explicit: Option<&Path>, cwd: &Path) -> Result<Self, StateError> {
        match explicit {
            Some(path) => Ok(Self::new(path)),
            None => Self::user_default(cwd),
        }
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
    /// Unix directories are created mode `0700`.
    pub fn ensure_ready_for_runs(&self) -> Result<(), StateError> {
        create_private_dir_all(&self.runs_dir()).map_err(|source| StateError::Prepare {
            path: self.path.clone(),
            source,
        })
    }
}

fn create_private_dir_all(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(path)
    }
    #[cfg(not(unix))]
    {
        fs::create_dir_all(path)
    }
}

const USER_APP_DIR: &str = "shikigami";

fn env_nonempty(name: &str) -> Option<PathBuf> {
    match std::env::var_os(name) {
        Some(value) if !value.is_empty() => Some(PathBuf::from(value)),
        _ => None,
    }
}

fn env_absolute(name: &str) -> Option<PathBuf> {
    env_nonempty(name).filter(|path| path.is_absolute())
}

fn home_dir() -> Result<PathBuf, StateError> {
    #[cfg(windows)]
    if let Some(path) = env_absolute("USERPROFILE") {
        return Ok(path);
    }
    env_absolute("HOME").ok_or(StateError::MissingUserState)
}

fn user_base_dir() -> Result<PathBuf, StateError> {
    if let Some(xdg) = env_absolute("XDG_STATE_HOME") {
        return Ok(xdg.join(USER_APP_DIR));
    }
    #[cfg(target_os = "macos")]
    {
        Ok(home_dir()?
            .join("Library")
            .join("Application Support")
            .join(USER_APP_DIR))
    }
    #[cfg(windows)]
    {
        if let Some(local) = env_absolute("LOCALAPPDATA") {
            return Ok(local.join(USER_APP_DIR));
        }
        Ok(home_dir()?.join("AppData").join("Local").join(USER_APP_DIR))
    }
    #[cfg(not(any(target_os = "macos", windows)))]
    {
        Ok(home_dir()?.join(".local").join("state").join(USER_APP_DIR))
    }
}

/// Readable dash-encoding plus a short digest so `/tmp/a-b` and `/tmp/a/b`
/// do not share host state. The fingerprint is over canonical OS path bytes
/// so non-UTF-8 names that share a lossy display still diverge. The readable
/// prefix is truncated so the name stays within a 255-byte directory component.
fn encode_cwd(cwd: &Path) -> String {
    const MAX_NAME_BYTES: usize = 255;
    let path = cwd.canonicalize().unwrap_or_else(|_| cwd.to_path_buf());
    let fingerprint = &path_fingerprint(&path)[..16];
    let displayed = display_path(&path);
    let trimmed = displayed.trim_start_matches(['/', '\\']);
    let encoded = trimmed.replace(['/', '\\', ':'], "-");
    let overhead = 2 + 2 + fingerprint.len();
    let encoded = take_utf8_prefix(&encoded, MAX_NAME_BYTES.saturating_sub(overhead));
    format!("--{encoded}--{fingerprint}")
}

fn take_utf8_prefix(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

fn display_path(path: &Path) -> String {
    let mut displayed = path.to_string_lossy().into_owned();
    if let Some(rest) = displayed.strip_prefix(r"\\?\") {
        displayed = rest.to_string();
    }
    displayed
}

fn path_fingerprint(path: &Path) -> String {
    crate::digest::sha256_hex(&path_key(path))
}

fn path_key(path: &Path) -> Vec<u8> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        path.as_os_str().as_bytes().to_vec()
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        path.as_os_str()
            .encode_wide()
            .flat_map(u16::to_le_bytes)
            .collect()
    }
    #[cfg(not(any(unix, windows)))]
    {
        path.to_string_lossy().into_owned().into_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;
    use std::sync::Mutex;
    use tempfile::tempdir;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    struct EnvGuard {
        key: &'static str,
        previous: Option<OsString>,
    }

    impl EnvGuard {
        fn set(key: &'static str, value: impl AsRef<std::ffi::OsStr>) -> Self {
            let previous = std::env::var_os(key);
            unsafe { std::env::set_var(key, value) };
            Self { key, previous }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            unsafe {
                match &self.previous {
                    Some(value) => std::env::set_var(self.key, value),
                    None => std::env::remove_var(self.key),
                }
            }
        }
    }

    fn lock_env() -> std::sync::MutexGuard<'static, ()> {
        ENV_LOCK.lock().unwrap_or_else(|poison| poison.into_inner())
    }

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

    #[cfg(unix)]
    #[test]
    fn ensure_ready_for_runs_creates_private_directories() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempdir().expect("tempdir");
        let root = StateRoot::default_in(dir.path());
        root.ensure_ready_for_runs().expect("prepare");
        for path in [root.path(), root.runs_dir().as_path()] {
            let mode = fs::metadata(path).expect("metadata").permissions().mode() & 0o777;
            assert_eq!(mode, 0o700, "{path:?} mode {mode:o}");
        }
    }

    #[test]
    fn encode_cwd_strips_root_and_replaces_separators() {
        let encoded = encode_cwd(Path::new("/Users/example/proj"));
        assert!(encoded.starts_with("--"));
        assert!(!encoded.contains('/'));
        assert!(!encoded.contains('\\'));
        assert!(!encoded.contains(':'));
        let fingerprint = encoded.rsplit_once("--").expect("fingerprint").1;
        assert_eq!(fingerprint.len(), 16);
        assert!(fingerprint.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn encode_cwd_distinguishes_dash_name_from_nested_dir() {
        let root = tempdir().expect("tempdir");
        let dashed = root.path().join("a-b");
        let nested = root.path().join("a").join("b");
        fs::create_dir_all(&dashed).expect("dashed dir");
        fs::create_dir_all(&nested).expect("nested dir");
        let dashed_name = encode_cwd(&dashed);
        let nested_name = encode_cwd(&nested);
        assert_ne!(dashed_name, nested_name);
        assert!(dashed_name.contains("a-b"));
        assert!(nested_name.contains("a-b"));
    }

    #[test]
    fn encode_cwd_stays_within_directory_name_limit() {
        let long = "x".repeat(300);
        let cwd = PathBuf::from("/").join(&long).join(&long);
        let encoded = encode_cwd(&cwd);
        assert!(encoded.len() <= 255, "len {}", encoded.len());
        let fingerprint = encoded.rsplit_once("--").expect("fingerprint").1;
        assert_eq!(fingerprint.len(), 16);
        assert!(fingerprint.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[cfg(unix)]
    #[test]
    fn encode_cwd_fingerprints_os_bytes_not_lossy_display() {
        use std::os::unix::ffi::OsStrExt;
        let a = Path::new(std::ffi::OsStr::from_bytes(b"/tmp/a\xfe"));
        let b = Path::new(std::ffi::OsStr::from_bytes(b"/tmp/a\xff"));
        assert_eq!(a.to_string_lossy(), b.to_string_lossy());
        assert_ne!(encode_cwd(a), encode_cwd(b));
    }

    #[test]
    fn relative_xdg_state_home_is_ignored() {
        let _lock = lock_env();
        let cwd = tempdir().expect("cwd");
        let _xdg = EnvGuard::set("XDG_STATE_HOME", ".");
        let root = StateRoot::user_default(cwd.path()).expect("user default");
        assert!(root.path().is_absolute());
        assert!(!root.path().starts_with(cwd.path()));
    }

    #[test]
    fn xdg_state_home_wins_on_every_os() {
        let _lock = lock_env();
        let xdg = tempdir().expect("xdg");
        let cwd = tempdir().expect("cwd");
        let _xdg = EnvGuard::set("XDG_STATE_HOME", xdg.path());
        let root = StateRoot::user_default(cwd.path()).expect("user default");
        let expected = xdg.path().join(USER_APP_DIR).join(encode_cwd(cwd.path()));
        assert_eq!(root.path(), expected);
        assert!(!root.path().starts_with(cwd.path()));
    }

    #[test]
    fn resolve_host_prefers_explicit_path() {
        let cwd = tempdir().expect("cwd");
        let explicit = cwd.path().join("custom-state");
        let root = StateRoot::resolve_host(Some(&explicit), cwd.path()).expect("resolve");
        assert_eq!(root.path(), explicit);
    }

    #[test]
    fn user_default_lets_inplace_materialize() {
        let _lock = lock_env();
        let xdg = tempdir().expect("xdg");
        let cwd = tempdir().expect("cwd");
        let _xdg = EnvGuard::set("XDG_STATE_HOME", xdg.path());
        let mut config = Config::default();
        config.workspace.adapter = "inplace".into();
        config.workspace.root = cwd.path().display().to_string();
        let workspace = crate::workspace::from_config(&config).expect("workspace");
        let state = StateRoot::user_default(cwd.path()).expect("user default");
        workspace
            .materialize("run", &state.runs_dir())
            .expect("inplace outside user state");
    }
}
