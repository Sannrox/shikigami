//! Explicit environment construction for agent-controlled subprocesses.

use std::collections::HashMap;
use std::ffi::OsString;
use std::fmt;

use super::job_credential::{JOB_CREDENTIAL_ENV_NAMES, JobCredential};

const SHELL_STARTUP_ENVIRONMENT_NAMES: &[&str] = &["BASH_ENV", "ENV", "SHELLOPTS", "BASHOPTS"];

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

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
                && !JOB_CREDENTIAL_ENV_NAMES
                    .iter()
                    .any(|job_name| job_name.eq_ignore_ascii_case(name))
        });
        Self { vars }
    }

    /// Parent snapshot plus a live job grant. Expired grants stay omitted so a
    /// child spawned after the claim fence cannot present a stale token.
    pub(crate) fn materialize(&self, job: Option<&JobCredential>) -> HashMap<OsString, OsString> {
        let mut vars = self.vars.clone();
        if let Some(job) = job {
            job.inject_into(&mut vars, now_ms());
        }
        vars
    }

    pub(crate) fn apply(&self, command: &mut tokio::process::Command) {
        self.apply_job(command, None);
    }

    pub(crate) fn apply_job(
        &self,
        command: &mut tokio::process::Command,
        job: Option<&JobCredential>,
    ) {
        command.env_clear();
        command.envs(self.materialize(job));
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

    #[test]
    fn injects_a_live_job_grant_and_strips_leftover_job_env() {
        let mut input = os_vars();
        input.push(("SHIKIGAMI_JOB_TOKEN".into(), "stale-parent".into()));
        input.push(("SHIKIGAMI_JOB_TENANT".into(), "stale-tenant".into()));
        let job = JobCredential::live("job-token", i64::MAX, "tenant-a", "ns-a", 0).unwrap();
        let resolved = ToolEnvironment::resolve_from_vars(input, &["PLANE_TOKEN".into()]);
        assert!(
            !resolved
                .vars
                .contains_key(OsStr::new("SHIKIGAMI_JOB_TOKEN"))
        );
        assert!(!resolved.vars.contains_key(OsStr::new("PLANE_TOKEN")));
        let vars = resolved.materialize(Some(&job));
        assert_eq!(
            vars.get(OsStr::new("SHIKIGAMI_JOB_TOKEN"))
                .map(OsString::as_os_str),
            Some(OsStr::new("job-token"))
        );
        assert_eq!(
            vars.get(OsStr::new("SHIKIGAMI_JOB_TENANT"))
                .map(OsString::as_os_str),
            Some(OsStr::new("tenant-a"))
        );
        assert_eq!(
            vars.get(OsStr::new("SHIKIGAMI_JOB_NAMESPACE"))
                .map(OsString::as_os_str),
            Some(OsStr::new("ns-a"))
        );
        assert!(!vars.contains_key(OsStr::new("PLANE_TOKEN")));
    }

    #[test]
    fn expired_job_grant_is_not_injected() {
        let job = JobCredential::live("job-token", 5, "tenant-a", "ns-a", 0).unwrap();
        let mut vars: HashMap<OsString, OsString> = HashMap::new();
        job.inject_into(&mut vars, 5);
        assert!(vars.is_empty());
    }
}
