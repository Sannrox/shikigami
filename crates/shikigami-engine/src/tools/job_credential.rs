//! Short-lived job credentials injected into sandboxed tool children (#442).
//!
//! The worker's long-lived secrets stay in the harness process. Children receive
//! only this grant, and only while it is live.

use std::collections::HashMap;
use std::ffi::OsString;
use std::sync::{Arc, Mutex};

/// Environment names used for the job grant. Always stripped from the parent
/// snapshot so a leftover host export cannot impersonate a live grant.
pub const JOB_TOKEN_ENV: &str = "SHIKIGAMI_JOB_TOKEN";
pub const JOB_TENANT_ENV: &str = "SHIKIGAMI_JOB_TENANT";
pub const JOB_NAMESPACE_ENV: &str = "SHIKIGAMI_JOB_NAMESPACE";

pub const JOB_CREDENTIAL_ENV_NAMES: &[&str] = &[JOB_TOKEN_ENV, JOB_TENANT_ENV, JOB_NAMESPACE_ENV];

/// Job-scoped token the sandboxed child may present. `Debug` never prints the
/// secret. Issuance is the plane's; this type only carries a live grant.
#[derive(Clone, PartialEq, Eq)]
pub struct JobCredential {
    token: String,
    expires_at_ms: i64,
    tenant_id: String,
    namespace: String,
}

impl JobCredential {
    /// Accept a live grant. Missing, blank, or already-expired tokens are
    /// omitted so children never see a worker secret or a stale job token.
    pub fn live(
        token: impl Into<String>,
        expires_at_ms: i64,
        tenant_id: impl Into<String>,
        namespace: impl Into<String>,
        now_ms: i64,
    ) -> Option<Self> {
        let token = token.into();
        if token.trim().is_empty() || expires_at_ms <= 0 || now_ms >= expires_at_ms {
            return None;
        }
        Some(Self {
            token,
            expires_at_ms,
            tenant_id: tenant_id.into(),
            namespace: namespace.into(),
        })
    }

    pub fn expires_at_ms(&self) -> i64 {
        self.expires_at_ms
    }

    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// True when the grant is still usable at `now_ms`.
    pub fn is_live(&self, now_ms: i64) -> bool {
        now_ms < self.expires_at_ms
    }

    pub(crate) fn inject_into(&self, vars: &mut HashMap<OsString, OsString>, now_ms: i64) {
        if !self.is_live(now_ms) {
            return;
        }
        vars.insert(JOB_TOKEN_ENV.into(), self.token.clone().into());
        if !self.tenant_id.is_empty() {
            vars.insert(JOB_TENANT_ENV.into(), self.tenant_id.clone().into());
        }
        if !self.namespace.is_empty() {
            vars.insert(JOB_NAMESPACE_ENV.into(), self.namespace.clone().into());
        }
    }
}

impl std::fmt::Debug for JobCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobCredential")
            .field("token", &"[REDACTED]")
            .field("expires_at_ms", &self.expires_at_ms)
            .field("tenant_id", &self.tenant_id)
            .field("namespace", &self.namespace)
            .finish()
    }
}

/// Shared live grant for one run. Heartbeat refresh replaces the inner snapshot
/// so later tool children receive the current token, not the admission copy.
#[derive(Clone)]
pub struct JobCredentialHandle {
    inner: Arc<Mutex<Option<JobCredential>>>,
}

impl JobCredentialHandle {
    pub fn new(credential: Option<JobCredential>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(credential)),
        }
    }

    pub fn from_live(
        token: impl Into<String>,
        expires_at_ms: i64,
        tenant_id: impl Into<String>,
        namespace: impl Into<String>,
        now_ms: i64,
    ) -> Option<Self> {
        JobCredential::live(token, expires_at_ms, tenant_id, namespace, now_ms)
            .map(|credential| Self::new(Some(credential)))
    }

    pub fn replace(&self, credential: Option<JobCredential>) {
        *self.lock() = credential;
    }

    pub fn snapshot(&self) -> Option<JobCredential> {
        self.lock().clone()
    }

    /// The token while the grant is live now; `None` once it expired.
    pub(crate) fn live_token(&self) -> Option<String> {
        self.snapshot()
            .filter(|credential| credential.is_live(super::environment::now_ms()))
            .map(|credential| credential.token)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Option<JobCredential>> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl std::fmt::Debug for JobCredentialHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("JobCredentialHandle")
            .field(&self.snapshot())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_refuses_blank_and_expired_grants() {
        assert!(JobCredential::live("tok", 10, "t", "ns", 10).is_none());
        assert!(JobCredential::live("tok", 10, "t", "ns", 11).is_none());
        assert!(JobCredential::live(" ", 100, "t", "ns", 0).is_none());
        assert!(JobCredential::live("", 100, "t", "ns", 0).is_none());
        let cred = JobCredential::live("tok", 100, "tenant-a", "ns-a", 0).unwrap();
        assert_eq!(cred.tenant_id(), "tenant-a");
        assert_eq!(cred.namespace(), "ns-a");
        assert!(cred.is_live(99));
        assert!(!cred.is_live(100));
    }

    #[test]
    fn debug_redacts_the_token() {
        let cred = JobCredential::live("super-secret", 100, "t", "ns", 0).unwrap();
        let debug = format!("{cred:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("super-secret"));
    }

    #[test]
    fn handle_refresh_replaces_the_snapshot() {
        let handle = JobCredentialHandle::from_live("old", 100, "t", "ns", 0).unwrap();
        handle.replace(JobCredential::live("new", i64::MAX, "t", "ns", 0));
        let cred = handle.snapshot().unwrap();
        let mut vars = HashMap::new();
        cred.inject_into(&mut vars, 0);
        assert_eq!(
            vars.get(std::ffi::OsStr::new(JOB_TOKEN_ENV)),
            Some(&OsString::from("new"))
        );
    }
}
