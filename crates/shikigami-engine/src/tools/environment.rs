//! Explicit environment construction for agent-controlled subprocesses.

use std::collections::HashMap;
use std::ffi::OsString;
use std::fmt;

const SHELL_STARTUP_ENVIRONMENT_NAMES: &[&str] = &["BASH_ENV", "ENV", "SHELLOPTS", "BASHOPTS"];

#[derive(Clone)]
pub(crate) struct ToolEnvironment {
    vars: HashMap<OsString, OsString>,
}

impl fmt::Debug for ToolEnvironment {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut names: Vec<String> = self
            .vars
            .keys()
            .map(|name| name.to_string_lossy().into_owned())
            .collect();
        names.sort_unstable();
        formatter
            .debug_struct("ToolEnvironment")
            .field("variable_names", &names)
            .finish()
    }
}

impl ToolEnvironment {
    pub(crate) fn resolve(protected_names: &[String]) -> Self {
        Self::resolve_from_vars(std::env::vars_os(), protected_names)
    }

    fn resolve_from_vars<I>(vars: I, protected_names: &[String]) -> Self
    where
        I: IntoIterator<Item = (OsString, OsString)>,
    {
        let mut vars: HashMap<OsString, OsString> = vars.into_iter().collect();
        vars.retain(|name, _| {
            let Some(name) = name.to_str() else {
                return true;
            };
            !protected_names
                .iter()
                .any(|protected| protected.eq_ignore_ascii_case(name))
                && !SHELL_STARTUP_ENVIRONMENT_NAMES
                    .iter()
                    .any(|startup| startup.eq_ignore_ascii_case(name))
        });

        Self { vars }
    }

    pub(crate) fn apply(&self, command: &mut tokio::process::Command) {
        command.env_clear();
        command.envs(&self.vars);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsStr;

    fn vars() -> Vec<(String, String)> {
        vec![
            ("PATH".into(), "/bin".into()),
            ("HOME".into(), "/home/test".into()),
            ("LANG".into(), "C".into()),
            ("PROJECT_FLAG".into(), "yes".into()),
            ("PLANE_TOKEN".into(), "synthetic-secret".into()),
        ]
    }

    fn os_vars() -> Vec<(OsString, OsString)> {
        vars()
            .into_iter()
            .map(|(name, value)| (name.into(), value.into()))
            .collect()
    }

    #[test]
    fn preserves_parent_variables_but_removes_credentials_and_startup_controls() {
        let mut input = os_vars();
        input.push(("BASH_ENV".into(), "/must/not/be/read".into()));
        input.push(("SHELLOPTS".into(), "xtrace".into()));
        let resolved = ToolEnvironment::resolve_from_vars(input, &["plane_token".into()]);

        assert_eq!(
            resolved
                .vars
                .get(OsStr::new("PATH"))
                .map(OsString::as_os_str),
            Some(OsStr::new("/bin"))
        );
        assert_eq!(
            resolved
                .vars
                .get(OsStr::new("PROJECT_FLAG"))
                .map(OsString::as_os_str),
            Some(OsStr::new("yes"))
        );
        assert!(!resolved.vars.contains_key(OsStr::new("PLANE_TOKEN")));
        assert!(!resolved.vars.contains_key(OsStr::new("BASH_ENV")));
        assert!(!resolved.vars.contains_key(OsStr::new("SHELLOPTS")));
    }

    #[test]
    fn debug_output_never_contains_environment_values() {
        let resolved = ToolEnvironment::resolve_from_vars(os_vars(), &[]);
        let debug = format!("{resolved:?}");
        assert!(debug.contains("PLANE_TOKEN"));
        assert!(!debug.contains("synthetic-secret"));
    }
}
