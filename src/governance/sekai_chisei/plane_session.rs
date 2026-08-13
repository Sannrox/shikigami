//! Shared plane session protocol behind the existing governance and plane-intake seams.
//!
//! Owns connect, token, auth-source metadata, CallOptions, SdkError mapping, and
//! the live probe. Claim contention mapping and harvest wrappers stay with their
//! modules. This is not a new public port.

use std::env;
use std::time::Duration;

use sekai_client::{
    CallContext, CallOptions, ClientConfig, CoreLoopClient, GrpcTransport, SdkError, SdkErrorCode,
};

use super::{SekaiChiseiGovernance, proto};
use crate::governance::GovernanceError;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const RPC_TIMEOUT: Duration = Duration::from_secs(120);
const AUTH_SOURCE_METADATA: &str = "x-sekai-auth-source";

pub(super) type PlaneClient = CoreLoopClient<GrpcTransport>;

fn auth_source(governance: &SekaiChiseiGovernance) -> &'static str {
    governance
        .token_env
        .as_ref()
        .and_then(|name| env::var(name).ok())
        .filter(|token| !token.trim().is_empty())
        .map(|_| "token")
        .unwrap_or("local")
}

fn token(governance: &SekaiChiseiGovernance) -> Option<String> {
    let token = governance
        .token_env
        .as_ref()
        .and_then(|name| env::var(name).ok())
        .map(|token| token.trim().to_string())
        .filter(|token| !token.is_empty())?;
    Some(
        token
            .strip_prefix("Bearer ")
            .unwrap_or(&token)
            .trim()
            .to_string(),
    )
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
    let mut config = ClientConfig::new(governance.endpoint.clone(), governance.principal.clone())
        .with_namespace(governance.namespace.clone())
        .with_default_timeout(CONNECT_TIMEOUT);
    if let Some(token) = token(governance) {
        config = config
            .with_token(token)
            .map_err(|error| map_error("configure plane credential", error))?;
    }
    CoreLoopClient::connect(config)
        .await
        .map_err(|error| map_error("connect", error))
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
}
