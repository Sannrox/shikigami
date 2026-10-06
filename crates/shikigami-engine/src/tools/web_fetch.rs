//! `web_fetch` HTTP client, SSRF validation, and fetcher trait.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;

use super::ToolError;

const MAX_WEB_FETCH_BYTES: usize = 256 * 1024;
const WEB_FETCH_TIMEOUT: Duration = Duration::from_secs(15);
pub(crate) const WEB_FETCH_MAX_REDIRECTS: usize = 5;

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
    user_agent: String,
}

#[cfg(feature = "model-http")]
impl ReqwestWebFetcher {
    pub fn new() -> Result<Self, ToolError> {
        Ok(Self {
            user_agent: format!("shikigami/{} (web_fetch)", env!("CARGO_PKG_VERSION")),
        })
    }

    async fn get_once(&self, url: &str) -> Result<WebFetchResponse, ToolError> {
        let parsed =
            url::Url::parse(url).map_err(|e| ToolError::Message(format!("web_fetch url: {e}")))?;
        let host = parsed
            .host_str()
            .ok_or_else(|| ToolError::Message("web_fetch: URL missing host".into()))?;
        let port = parsed
            .port_or_known_default()
            .ok_or_else(|| ToolError::Message("web_fetch: URL missing port".into()))?;
        let addrs: Vec<SocketAddr> = tokio::net::lookup_host((host, port))
            .await
            .map_err(|e| ToolError::Message(format!("web_fetch DNS lookup failed: {e}")))?
            .collect();
        validate_resolved_addrs(host, &addrs)?;

        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .timeout(WEB_FETCH_TIMEOUT)
            .user_agent(&self.user_agent)
            .resolve_to_addrs(host, &addrs)
            .build()
            .map_err(|e| ToolError::Message(format!("web_fetch client: {e}")))?;
        let mut resp = client
            .get(url)
            .send()
            .await
            .map_err(|e| ToolError::Message(format!("web_fetch request failed: {e}")))?;
        let status = resp.status().as_u16();
        if let Some(final_url) = redirect_destination(&parsed, resp.status(), resp.headers())? {
            return Ok(WebFetchResponse {
                status,
                final_url,
                body: String::new(),
            });
        }

        let mut bytes = Vec::with_capacity(
            resp.content_length()
                .unwrap_or(0)
                .min(MAX_WEB_FETCH_BYTES as u64) as usize,
        );
        let mut truncated = false;
        while let Some(chunk) = resp
            .chunk()
            .await
            .map_err(|e| ToolError::Message(format!("web_fetch body: {e}")))?
        {
            if append_bounded_body(&mut bytes, &chunk) {
                truncated = true;
                break;
            }
        }
        let mut body = String::from_utf8_lossy(&bytes).into_owned();
        if truncated {
            body.push_str("\n…[truncated]");
        }
        Ok(WebFetchResponse {
            status,
            final_url: url.to_string(),
            body,
        })
    }
}

#[cfg(feature = "model-http")]
#[async_trait::async_trait]
impl WebFetcher for ReqwestWebFetcher {
    async fn get(&self, url: &str) -> Result<WebFetchResponse, ToolError> {
        tokio::time::timeout(WEB_FETCH_TIMEOUT, self.get_once(url))
            .await
            .map_err(|_| ToolError::Message("web_fetch request timed out".into()))?
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
        return is_blocked_fetch_ip(ip);
    }
    false
}

fn validate_resolved_addrs(host: &str, addrs: &[SocketAddr]) -> Result<(), ToolError> {
    if addrs.is_empty() {
        return Err(ToolError::Message(format!(
            "web_fetch: host `{host}` resolved to no addresses"
        )));
    }
    if addrs.iter().any(|addr| is_blocked_fetch_ip(addr.ip())) {
        return Err(ToolError::Message(format!(
            "web_fetch: blocked DNS result for `{host}` (private/link-local/loopback not allowed)"
        )));
    }
    Ok(())
}

fn is_blocked_fetch_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            let octets = v4.octets();
            v4.is_loopback()
                || v4.is_private()
                || v4.is_link_local()
                || v4.is_unspecified()
                || v4.is_multicast()
                || v4.is_broadcast()
                || octets[0] == 0
                || octets[0] == 100 && (64..=127).contains(&octets[1])
                || octets[0] == 192 && octets[1] == 0 && octets[2] == 0
                || octets[0] == 192 && octets[1] == 0 && octets[2] == 2
                || octets[0] == 198 && (octets[1] == 18 || octets[1] == 19)
                || octets[0] == 198 && octets[1] == 51 && octets[2] == 100
                || octets[0] == 203 && octets[1] == 0 && octets[2] == 113
                || octets[0] >= 240
        }
        IpAddr::V6(v6) => {
            v6.is_loopback()
                || v6.is_unique_local()
                || v6.is_unicast_link_local()
                || v6.is_unspecified()
                || v6.is_multicast()
                || v6.segments()[0] == 0x2001 && v6.segments()[1] == 0x0db8
                || v6
                    .to_ipv4_mapped()
                    .is_some_and(|v4| is_blocked_fetch_ip(IpAddr::V4(v4)))
        }
    }
}

fn append_bounded_body(buffer: &mut Vec<u8>, chunk: &[u8]) -> bool {
    let remaining = MAX_WEB_FETCH_BYTES.saturating_sub(buffer.len());
    let retained = remaining.min(chunk.len());
    buffer.extend_from_slice(&chunk[..retained]);
    retained < chunk.len()
}

#[cfg(feature = "model-http")]
fn redirect_destination(
    base: &url::Url,
    status: reqwest::StatusCode,
    headers: &reqwest::header::HeaderMap,
) -> Result<Option<String>, ToolError> {
    if !matches!(
        status,
        reqwest::StatusCode::MOVED_PERMANENTLY
            | reqwest::StatusCode::FOUND
            | reqwest::StatusCode::SEE_OTHER
            | reqwest::StatusCode::TEMPORARY_REDIRECT
            | reqwest::StatusCode::PERMANENT_REDIRECT
    ) {
        return Ok(None);
    }
    let Some(location) = headers.get(reqwest::header::LOCATION) else {
        return Ok(None);
    };
    let location = location
        .to_str()
        .map_err(|e| ToolError::Message(format!("web_fetch redirect Location: {e}")))?;
    let destination = base
        .join(location)
        .map_err(|e| ToolError::Message(format!("web_fetch redirect URL: {e}")))?;
    Ok(Some(destination.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolved_private_addresses_are_rejected() {
        let private = [SocketAddr::from(([127, 0, 0, 1], 80))];
        let err = validate_resolved_addrs("example.test", &private).unwrap_err();
        assert!(err.to_string().contains("blocked DNS result"));

        let public = [SocketAddr::from(([93, 184, 216, 34], 443))];
        validate_resolved_addrs("example.com", &public).unwrap();

        for address in ["100.64.0.1:80", "192.0.2.1:80", "[::ffff:127.0.0.1]:80"] {
            let blocked = [address.parse().unwrap()];
            assert!(validate_resolved_addrs("example.test", &blocked).is_err());
        }
    }

    #[test]
    fn response_body_limit_is_enforced_before_growth() {
        let mut body = vec![b'a'; MAX_WEB_FETCH_BYTES];
        assert!(append_bounded_body(&mut body, b"x"));
        assert_eq!(body.len(), MAX_WEB_FETCH_BYTES);

        let mut allowed = Vec::new();
        assert!(!append_bounded_body(&mut allowed, b"ok"));
        assert_eq!(allowed, b"ok");
    }

    #[cfg(feature = "model-http")]
    #[test]
    fn headerless_3xx_is_a_response_not_a_redirect_hop() {
        let base = url::Url::parse("https://example.com/start").unwrap();
        let headers = reqwest::header::HeaderMap::new();
        assert_eq!(
            redirect_destination(&base, reqwest::StatusCode::NOT_MODIFIED, &headers).unwrap(),
            None
        );

        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::LOCATION,
            reqwest::header::HeaderValue::from_static("/next"),
        );
        assert_eq!(
            redirect_destination(&base, reqwest::StatusCode::NOT_MODIFIED, &headers).unwrap(),
            None
        );
        assert_eq!(
            redirect_destination(&base, reqwest::StatusCode::FOUND, &headers).unwrap(),
            Some("https://example.com/next".into())
        );
    }
}
