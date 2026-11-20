//! Opt-in `web_search`: POST a query to an operator HTTP endpoint.

use std::time::Duration;

use serde::Deserialize;

use super::ToolError;
use crate::config::WebSearchSettings;

const MAX_RESULTS: usize = 5;
const MAX_QUERY_CHARS: usize = 512;
const MAX_TITLE_CHARS: usize = 200;
const MAX_SNIPPET_CHARS: usize = 400;
const MAX_BODY_BYTES: usize = 64 * 1024;
const MAX_MESSAGE_BYTES: usize = 8 * 1024;
const WEB_SEARCH_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Debug, Deserialize)]
pub(crate) struct WebSearchArgs {
    pub(crate) query: String,
}

#[derive(Debug, Deserialize)]
struct SearchResponse {
    results: Vec<SearchHit>,
}

#[derive(Debug, Deserialize)]
struct SearchHit {
    #[serde(default)]
    title: String,
    url: String,
    #[serde(default)]
    snippet: String,
}

/// True when the operator set an endpoint so the builtin can register.
pub(crate) fn is_configured(settings: &WebSearchSettings) -> bool {
    settings.endpoint().is_some()
}

/// POST `{"query"}` and format a bounded title/URL/snippet list.
pub(crate) async fn search(
    settings: &WebSearchSettings,
    check_url: impl Fn(&str) -> Result<(), String>,
    args_json: &str,
) -> Result<String, ToolError> {
    let args: WebSearchArgs = super::parse("web_search", args_json)?;
    let query = args.query.trim();
    if query.is_empty() {
        return Err(ToolError::Message(
            "web_search: query must not be empty".into(),
        ));
    }
    if query.chars().count() > MAX_QUERY_CHARS {
        return Err(ToolError::Message(format!(
            "web_search: query exceeds {MAX_QUERY_CHARS} characters"
        )));
    }
    let endpoint = settings
        .endpoint()
        .ok_or_else(|| ToolError::Disabled("web_search".into()))?;
    validate_search_url(endpoint)?;
    check_url(endpoint).map_err(ToolError::Message)?;
    let token = match settings.token_env_name() {
        None => None,
        Some(name) => {
            let value = std::env::var(name).ok().filter(|v| !v.is_empty());
            Some(value.ok_or_else(|| {
                ToolError::Message(format!("web_search: token_env `{name}` is unset or empty"))
            })?)
        }
    };
    let body = post_query(endpoint, query, token.as_deref()).await?;
    Ok(format_results(&parse_results(&body)?))
}

fn validate_search_url(url: &str) -> Result<(), ToolError> {
    let parsed =
        url::Url::parse(url).map_err(|e| ToolError::Message(format!("web_search url: {e}")))?;
    match parsed.scheme() {
        "http" | "https" => {}
        other => {
            return Err(ToolError::Message(format!(
                "web_search: only http/https allowed, got `{other}`"
            )));
        }
    }
    let host = parsed
        .host_str()
        .ok_or_else(|| ToolError::Message("web_search: URL missing host".into()))?;
    if host.eq_ignore_ascii_case("metadata.google.internal") {
        return Err(ToolError::Message(format!(
            "web_search: blocked host `{host}`"
        )));
    }
    Ok(())
}

fn parse_results(body: &str) -> Result<Vec<SearchHit>, ToolError> {
    let parsed: SearchResponse = serde_json::from_str(body).map_err(|e| {
        ToolError::Message(format!(
            "web_search: response is not the documented JSON: {e}"
        ))
    })?;
    Ok(parsed.results.into_iter().take(MAX_RESULTS).collect())
}

fn format_results(hits: &[SearchHit]) -> String {
    if hits.is_empty() {
        return "0 results".into();
    }
    let mut out = String::new();
    for (index, hit) in hits.iter().enumerate() {
        let url = hit.url.trim();
        if url.is_empty() {
            continue;
        }
        let title = clip(&hit.title, MAX_TITLE_CHARS);
        let title = if title.is_empty() {
            url
        } else {
            title.as_str()
        };
        let snippet = clip(&hit.snippet, MAX_SNIPPET_CHARS);
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str(&format!("{}. {title}\nurl={url}\n{snippet}\n", index + 1));
        if out.len() > MAX_MESSAGE_BYTES {
            truncate_to_char_boundary(&mut out, MAX_MESSAGE_BYTES);
            out.push_str("\n…[truncated]");
            break;
        }
    }
    if out.is_empty() {
        "0 results".into()
    } else {
        out
    }
}

fn truncate_to_char_boundary(text: &mut String, max_bytes: usize) {
    let mut end = max_bytes.min(text.len());
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    text.truncate(end);
}

fn clip(text: &str, max_chars: usize) -> String {
    let trimmed = text.trim();
    let mut out = String::new();
    for (i, ch) in trimmed.chars().enumerate() {
        if i >= max_chars {
            out.push('…');
            break;
        }
        out.push(ch);
    }
    out
}

async fn post_query(url: &str, query: &str, token: Option<&str>) -> Result<String, ToolError> {
    #[cfg(feature = "model-http")]
    {
        tokio::time::timeout(WEB_SEARCH_TIMEOUT, post_once(url, query, token))
            .await
            .map_err(|_| ToolError::Message("web_search request timed out".into()))?
    }
    #[cfg(not(feature = "model-http"))]
    {
        let _ = (url, query, token);
        Err(ToolError::Message(
            "web_search requires the model-http feature (HTTP client)".into(),
        ))
    }
}

#[cfg(feature = "model-http")]
async fn post_once(url: &str, query: &str, token: Option<&str>) -> Result<String, ToolError> {
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .timeout(WEB_SEARCH_TIMEOUT)
        .user_agent(format!(
            "shikigami/{} (web_search)",
            env!("CARGO_PKG_VERSION")
        ))
        .build()
        .map_err(|e| ToolError::Message(format!("web_search client: {e}")))?;
    let mut req = client
        .post(url)
        .header(reqwest::header::ACCEPT, "application/json")
        .json(&serde_json::json!({"query": query}));
    if let Some(token) = token {
        req = req.bearer_auth(token);
    }
    let mut resp = req
        .send()
        .await
        .map_err(|e| ToolError::Message(format!("web_search request failed: {e}")))?;
    let status = resp.status();
    if !status.is_success() {
        return Err(ToolError::Message(format!("web_search: HTTP {status}")));
    }
    let mut bytes = Vec::new();
    let mut truncated = false;
    while let Some(chunk) = resp
        .chunk()
        .await
        .map_err(|e| ToolError::Message(format!("web_search body: {e}")))?
    {
        let remaining = MAX_BODY_BYTES.saturating_sub(bytes.len());
        let take = remaining.min(chunk.len());
        bytes.extend_from_slice(&chunk[..take]);
        if take < chunk.len() {
            truncated = true;
            break;
        }
    }
    let body = String::from_utf8_lossy(&bytes).into_owned();
    if truncated {
        return Err(ToolError::Message(
            "web_search: response exceeded size limit".into(),
        ));
    }
    Ok(body)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn empty_query_is_rejected_before_http() {
        let settings = WebSearchSettings {
            url: Some("https://search.example/v1".into()),
            token_env: None,
        };
        let err = search(&settings, |_| Ok(()), r#"{"query":"   "}"#)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("empty"), "{err}");
    }

    #[test]
    fn extra_hits_are_dropped_and_fields_clipped() {
        let hits = (1..=8)
            .map(|i| SearchHit {
                title: format!("t{i}"),
                url: format!("https://example.com/{i}"),
                snippet: "ok".into(),
            })
            .collect::<Vec<_>>();
        let parsed = parse_results(
            &serde_json::json!({"results": hits.iter().map(|h| {
            serde_json::json!({"title": h.title, "url": h.url, "snippet": h.snippet})
        }).collect::<Vec<_>>()})
            .to_string(),
        )
        .unwrap();
        assert_eq!(parsed.len(), MAX_RESULTS);
        let text = format_results(&parsed);
        assert!(text.contains("https://example.com/1"), "{text}");
        assert!(!text.contains("https://example.com/6"), "{text}");
        let long = SearchHit {
            title: "x".repeat(MAX_TITLE_CHARS + 20),
            url: "https://example.com/x".into(),
            snippet: "y".repeat(MAX_SNIPPET_CHARS + 20),
        };
        let clipped = format_results(&[long]);
        assert!(clipped.contains('…'), "{clipped}");
        assert!(clipped.len() < MAX_MESSAGE_BYTES, "{}", clipped.len());
    }

    #[test]
    fn message_cap_truncates_on_a_char_boundary() {
        let hits = (0..5)
            .map(|_| SearchHit {
                title: "字".repeat(MAX_TITLE_CHARS),
                url: "https://example.com/wide".into(),
                snippet: "語".repeat(MAX_SNIPPET_CHARS),
            })
            .collect::<Vec<_>>();
        let text = format_results(&hits);
        assert!(text.contains("…[truncated]"), "{text}");
        assert!(text.is_char_boundary(text.len()));
        assert!(text.len() <= MAX_MESSAGE_BYTES + "\n…[truncated]".len());
    }

    #[test]
    fn file_url_is_rejected() {
        let err = validate_search_url("file:///etc/passwd").unwrap_err();
        assert!(err.to_string().contains("http/https"), "{err}");
    }
}
