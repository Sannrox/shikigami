//! Shared plane session protocol behind the existing governance and plane-intake seams.
//!
//! Owns connect, token, auth-source metadata, CallOptions, SdkError mapping, and
//! the live probe. Claim contention mapping and harvest wrappers stay with their
//! modules. This is not a new public port.

use std::env;
use std::time::Duration;

use sekai_client::{
    CallContext, CallOptions, ClientConfig, CoreLoopClient, CoreLoopTransport, GrpcTransport,
    SdkError, SdkErrorCode,
};

use super::{SekaiChiseiGovernance, proto};
use crate::governance::GovernanceError;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const RPC_TIMEOUT: Duration = Duration::from_secs(120);
const AUTH_SOURCE_METADATA: &str = "x-sekai-auth-source";

pub(super) type PlaneClient = CoreLoopClient<GrpcTransport>;

fn auth_source(governance: &SekaiChiseiGovernance) -> &'static str {
    if governance.client_secret_env.is_some() {
        "token"
    } else {
        "local"
    }
}

/// Short-lived service token for the worker's registered service principal.
pub(super) struct ServiceToken {
    value: String,
    issued_at_ms: i64,
    expires_at_ms: i64,
}

impl ServiceToken {
    /// Accept an issued token. Empty or already-expired grants are refused.
    fn from_grant(value: String, expires_at_ms: i64, now_ms: i64) -> Result<Self, GovernanceError> {
        if value.trim().is_empty() || expires_at_ms <= now_ms {
            return Err(GovernanceError::Message(
                "IssueServicePrincipalToken returned no usable token".into(),
            ));
        }
        Ok(Self {
            value,
            issued_at_ms: now_ms,
            expires_at_ms,
        })
    }

    /// Reuse until four fifths of the lifetime have passed, so the refresh
    /// lands well before expiry.
    fn fresh(&self, now_ms: i64) -> bool {
        let refresh_at = self.issued_at_ms + (self.expires_at_ms - self.issued_at_ms) * 4 / 5;
        now_ms < refresh_at
    }
}

/// Holds the current service token; shared by every plane call of one adapter.
#[derive(Default)]
pub(super) struct ServiceTokenCache(tokio::sync::Mutex<Option<ServiceToken>>);

/// Map a refused credential exchange. The plane refuses unregistered
/// principals and invalid secrets; those are governance refusals, not outages.
fn issuance_error(error: SdkError) -> GovernanceError {
    match error.code {
        SdkErrorCode::Unavailable | SdkErrorCode::DeadlineExceeded => {
            GovernanceError::Unavailable(format!("IssueServicePrincipalToken: {error}"))
        }
        _ => GovernanceError::Message(format!(
            "IssueServicePrincipalToken refused the worker credentials: {error}"
        )),
    }
}

/// The current service token, issued or refreshed via client credentials when
/// needed. `None` when no client secret is configured (unauthenticated local
/// planes).
async fn service_token(
    governance: &SekaiChiseiGovernance,
    plane: &PlaneClient,
) -> Result<Option<String>, GovernanceError> {
    let Some(name) = &governance.client_secret_env else {
        return Ok(None);
    };
    let mut slot = governance.service_token.0.lock().await;
    let now_ms = crate::digest::unix_now_ms_i64();
    if let Some(token) = slot.as_ref().filter(|token| token.fresh(now_ms)) {
        return Ok(Some(token.value.clone()));
    }
    let secret = env::var(name)
        .ok()
        .filter(|secret| !secret.trim().is_empty())
        .ok_or_else(|| {
            GovernanceError::Unavailable(format!(
                "governance.client_secret_env {name} is unset or empty"
            ))
        })?;
    let response: proto::sekai::IssueServicePrincipalTokenResponse = plane
        .raw()
        .unary(
            "/sekai.SekaiService/IssueServicePrincipalToken",
            proto::sekai::IssueServicePrincipalTokenRequest {
                principal: governance.principal.clone(),
                client_secret: secret,
            },
            CallOptions::new().with_timeout(CONNECT_TIMEOUT),
        )
        .await
        .map_err(issuance_error)?;
    let token = ServiceToken::from_grant(
        response.token,
        response.expires_at_ms,
        crate::digest::unix_now_ms_i64(),
    )?;
    let value = token.value.clone();
    *slot = Some(token);
    Ok(Some(value))
}

/// A client on the same connection that authenticates with `token`, or sends
/// no bearer at all for `None`.
pub(super) fn with_bearer<T: CoreLoopTransport + Clone>(
    plane: &CoreLoopClient<T>,
    token: Option<&str>,
) -> Result<CoreLoopClient<T>, SdkError> {
    let mut config = plane.config().clone();
    config.credential = None;
    if let Some(token) = token {
        config = config.with_token(token)?;
    }
    CoreLoopClient::new(config, plane.transport().clone())
}

pub(super) fn call_options(
    governance: &SekaiChiseiGovernance,
    namespace: Option<&str>,
    operation_id: Option<&str>,
    request_id: Option<&str>,
) -> CallOptions {
    let context =
        CallContext::default().with_metadata(AUTH_SOURCE_METADATA, auth_source(governance));
    let mut options = CallOptions::new()
        .with_timeout(RPC_TIMEOUT)
        .with_context(context);
    if let Some(namespace) = namespace {
        options = options.with_namespace(namespace);
    }
    if let Some(operation_id) = operation_id {
        options = options.with_operation_id(operation_id);
    }
    if let Some(request_id) = request_id {
        options = options.with_request_id(request_id);
    }
    options
}

pub(super) fn map_error(operation: &str, error: SdkError) -> GovernanceError {
    let detail = format!("{operation}: {error}");
    match error.code {
        SdkErrorCode::Unavailable | SdkErrorCode::DeadlineExceeded => {
            GovernanceError::Unavailable(detail)
        }
        _ => GovernanceError::Message(detail),
    }
}

pub(super) async fn connect(
    governance: &SekaiChiseiGovernance,
) -> Result<PlaneClient, GovernanceError> {
    if governance.endpoint.trim().is_empty() {
        return Err(GovernanceError::Unavailable(
            "sekai-chisei endpoint not set (governance.endpoint or SHIKIGAMI_CONTROL_PLANE)".into(),
        ));
    }
    if crate::config::is_remote_plaintext_http_endpoint(&governance.endpoint)
        && !governance.allow_insecure_remote
    {
        return Err(GovernanceError::Message(format!(
            "connect: {}",
            crate::config::REMOTE_PLAINTEXT_GOVERNANCE_HINT
        )));
    }
    let config = ClientConfig::new(governance.endpoint.clone(), governance.principal.clone())
        .with_namespace(governance.namespace.clone())
        .with_default_timeout(CONNECT_TIMEOUT)
        .allow_insecure_remote(governance.allow_insecure_remote);
    let plane = CoreLoopClient::connect(config)
        .await
        .map_err(|error| map_error("connect", error))?;
    authorize(governance, &plane).await
}

/// `plane` carrying the current service token (refreshed when due), or
/// unchanged without client credentials. Cached clients call this before each
/// use so a long-lived connection never sends an expired token.
pub(super) async fn authorize(
    governance: &SekaiChiseiGovernance,
    plane: &PlaneClient,
) -> Result<PlaneClient, GovernanceError> {
    let credential = |error| map_error("configure plane credential", error);
    // The credential exchange authenticates from the request body; never send
    // a (possibly expired) bearer with it.
    let anonymous = with_bearer(plane, None).map_err(credential)?;
    let token = service_token(governance, &anonymous).await?;
    with_bearer(plane, token.as_deref()).map_err(credential)
}

pub(super) async fn probe(governance: &SekaiChiseiGovernance) -> Result<(), GovernanceError> {
    let client = connect(governance).await?;
    let _: proto::sekai::ListSchemaTypesResponse = client
        .raw()
        .unary(
            "/sekai.SekaiService/ListSchemaTypes",
            proto::sekai::ListSchemaTypesRequest {},
            call_options(governance, Some(&governance.namespace), None, None),
        )
        .await
        .map_err(|error| map_error("probe", error))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    #[test]
    fn service_token_is_refreshed_before_expiry() {
        let token = ServiceToken::from_grant("st".into(), 1_000_000, 0).unwrap();
        assert!(token.fresh(0));
        assert!(token.fresh(799_999));
        assert!(
            !token.fresh(800_000),
            "refresh at four fifths of the lifetime"
        );
        assert!(!token.fresh(1_000_000));
    }

    #[test]
    fn empty_or_expired_service_token_grant_is_refused() {
        assert!(ServiceToken::from_grant(String::new(), 10, 0).is_err());
        assert!(ServiceToken::from_grant("st".into(), 10, 10).is_err());
    }

    #[test]
    fn refused_credential_exchange_is_a_governance_refusal() {
        for code in [
            SdkErrorCode::PermissionDenied,
            SdkErrorCode::Unauthenticated,
            SdkErrorCode::NotFound,
        ] {
            let error = issuance_error(SdkError::new(code));
            assert!(matches!(error, GovernanceError::Message(_)), "{error}");
        }
        let error = issuance_error(SdkError::new(SdkErrorCode::Unavailable));
        assert!(matches!(error, GovernanceError::Unavailable(_)), "{error}");
    }

    #[test]
    fn call_options_preserve_auth_source_and_correlation() {
        let governance = SekaiChiseiGovernance::from_config(&Config::default()).unwrap();
        let options = call_options(
            &governance,
            Some("default"),
            Some("operation-1"),
            Some("request-1"),
        );
        assert_eq!(
            options.context.metadata,
            vec![(AUTH_SOURCE_METADATA.into(), "local".into())]
        );
        assert_eq!(options.context.namespace.as_deref(), Some("default"));
        assert_eq!(options.context.operation_id.as_deref(), Some("operation-1"));
        assert_eq!(options.request_id.as_deref(), Some("request-1"));
    }

    fn plane_config(endpoint: &str, allow_insecure_remote: bool) -> crate::config::Config {
        let mut config = Config::default();
        config.governance.adapter = "sekai-chisei".into();
        config.governance.endpoint = Some(endpoint.into());
        config.governance.allow_insecure_remote = allow_insecure_remote;
        config
    }

    async fn connect_error(endpoint: &str, allow_insecure_remote: bool) -> GovernanceError {
        let governance =
            SekaiChiseiGovernance::from_config(&plane_config(endpoint, allow_insecure_remote))
                .unwrap();
        match connect(&governance).await {
            Ok(_) => panic!("connect must not succeed without a plane"),
            Err(error) => error,
        }
    }

    #[tokio::test]
    async fn loopback_http_passes_uri_policy() {
        let error = connect_error("http://127.0.0.1:1", false).await;
        let detail = error.to_string();
        assert!(
            !detail.contains(crate::config::REMOTE_PLAINTEXT_GOVERNANCE_HINT),
            "{detail}"
        );
        assert!(matches!(error, GovernanceError::Unavailable(_)), "{error}");
    }

    #[tokio::test]
    async fn remote_http_without_flag_rejects_before_dial() {
        let error = connect_error("http://192.0.2.1:1", false).await;
        let detail = error.to_string();
        assert!(
            detail.contains(crate::config::REMOTE_PLAINTEXT_GOVERNANCE_HINT),
            "{detail}"
        );
        assert!(matches!(error, GovernanceError::Message(_)), "{error}");
    }

    #[tokio::test]
    async fn remote_http_with_flag_passes_uri_policy() {
        let error = connect_error("http://192.0.2.1:1", true).await;
        let detail = error.to_string();
        assert!(
            !detail.contains(crate::config::REMOTE_PLAINTEXT_GOVERNANCE_HINT),
            "{detail}"
        );
        assert!(matches!(error, GovernanceError::Unavailable(_)), "{error}");
    }
}
