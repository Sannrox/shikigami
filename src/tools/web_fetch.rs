//! `web_fetch` HTTP client, SSRF validation, and fetcher trait.

use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;

use super::ToolError;

const MAX_WEB_FETCH_BYTES: usize = 256 * 1024;
const WEB_FETCH_TIMEOUT: Duration = Duration::from_secs(15);
const WEB_FETCH_MAX_REDIRECTS: usize = 5;

/// Pluggable HTTP GET for `web_fetch` (real client or offline mock).
#[async_trait::async_trait]
pub trait WebFetcher: Send + Sync {
    async fn get(&self, url: &str) -> Result<WebFetchResponse, ToolError>;
}

/// Successful web_fetch HTTP result (before truncation formatting).
#[derive(Debug, Clone)]
pub struct WebFetchResponse {
    pub status: u16,
    pub final_url: String,
    pub body: String,
}

/// Offline test fetcher: returns a fixed body for any allowed URL.
pub struct MockWebFetcher {
    pub status: u16,
    pub body: String,
}

#[async_trait::async_trait]
impl WebFetcher for MockWebFetcher {
    async fn get(&self, url: &str) -> Result<WebFetchResponse, ToolError> {
        Ok(WebFetchResponse {
            status: self.status,
            final_url: url.to_string(),
            body: self.body.clone(),
        })
    }
}

/// Reqwest-backed fetcher (requires `model-http` feature).
#[cfg(feature = "model-http")]
pub struct ReqwestWebFetcher {
    client: reqwest::Client,
}

#[cfg(feature = "model-http")]
impl ReqwestWebFetcher {
    pub fn new() -> Result<Self, ToolError> {
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::limited(WEB_FETCH_MAX_REDIRECTS))
            .timeout(WEB_FETCH_TIMEOUT)
            .user_agent(format!(
                "shikigami/{} (web_fetch)",
                env!("CARGO_PKG_VERSION")
            ))
            .build()
            .map_err(|e| ToolError::Message(format!("web_fetch client: {e}")))?;
        Ok(Self { client })
    }
}

#[cfg(feature = "model-http")]
#[async_trait::async_trait]
impl WebFetcher for ReqwestWebFetcher {
    async fn get(&self, url: &str) -> Result<WebFetchResponse, ToolError> {
        let resp = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|e| ToolError::Message(format!("web_fetch request failed: {e}")))?;
        let status = resp.status().as_u16();
        let final_url = resp.url().to_string();
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| ToolError::Message(format!("web_fetch body: {e}")))?;
        let truncated = if bytes.len() > MAX_WEB_FETCH_BYTES {
            &bytes[..MAX_WEB_FETCH_BYTES]
        } else {
            &bytes[..]
        };
        let mut body = String::from_utf8_lossy(truncated).into_owned();
        if bytes.len() > MAX_WEB_FETCH_BYTES {
            body.push_str("\n…[truncated]");
        }
        Ok(WebFetchResponse {
            status,
            final_url,
            body,
        })
    }
}

pub(crate) fn default_web_fetcher() -> Result<Arc<dyn WebFetcher>, ToolError> {
    #[cfg(feature = "model-http")]
    {
        Ok(Arc::new(ReqwestWebFetcher::new()?))
    }
    #[cfg(not(feature = "model-http"))]
    {
        Ok(Arc::new(UnavailableWebFetcher))
    }
}

#[cfg(not(feature = "model-http"))]
struct UnavailableWebFetcher;

#[cfg(not(feature = "model-http"))]
#[async_trait::async_trait]
impl WebFetcher for UnavailableWebFetcher {
    async fn get(&self, _url: &str) -> Result<WebFetchResponse, ToolError> {
        Err(ToolError::Message(
            "web_fetch requires the model-http feature (HTTP client)".into(),
        ))
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct WebFetchArgs {
    pub(crate) url: String,
}

/// Validate scheme and block private/link-local/loopback targets (SSRF baseline).
pub fn validate_web_fetch_url(url: &str) -> Result<(), ToolError> {
    let parsed =
        url::Url::parse(url).map_err(|e| ToolError::Message(format!("web_fetch url: {e}")))?;
    match parsed.scheme() {
        "http" | "https" => {}
        other => {
            return Err(ToolError::Message(format!(
                "web_fetch: only http/https allowed, got `{other}`"
            )));
        }
    }
    let host = parsed
        .host_str()
        .ok_or_else(|| ToolError::Message("web_fetch: URL missing host".into()))?;
    if is_blocked_fetch_host(host) {
        return Err(ToolError::Message(format!(
            "web_fetch: blocked host `{host}` (private/link-local/loopback not allowed)"
        )));
    }
    Ok(())
}

fn is_blocked_fetch_host(host: &str) -> bool {
    let h = host.to_ascii_lowercase();
    if h == "localhost" || h.ends_with(".localhost") || h == "metadata.google.internal" {
        return true;
    }
    if let Ok(ip) = h.parse::<IpAddr>() {
        return match ip {
            IpAddr::V4(v4) => {
                v4.is_loopback()
                    || v4.is_private()
                    || v4.is_link_local()
                    || v4.is_unspecified()
                    || v4.octets()[0] == 169 && v4.octets()[1] == 254
            }
            IpAddr::V6(v6) => {
                v6.is_loopback()
                    || v6.is_unique_local()
                    || v6.is_unicast_link_local()
                    || v6.is_unspecified()
            }
        };
    }
    false
}
