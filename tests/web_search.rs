use shikigami::{ChannelSink, Config, Harness, HarnessEvent, RunRequest, StateRoot};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tempfile::tempdir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn scripted_run_searches_a_mock_endpoint() {
    let dir = tempdir().unwrap();
    let seen = Arc::new(AtomicUsize::new(0));
    let url = spawn_search_endpoint(Arc::clone(&seen)).await;

    let mut config = offline_config(dir.path());
    config.tools.enabled = vec![
        "read_file".into(),
        "web_search".into(),
        "report".into(),
        "escalate".into(),
    ];
    config.tools.web_search.url = Some(url);
    config.model.script_json = Some(
        r#"[
        {"tool_calls":[{"name":"web_search","args_json":"{\"query\":\"mars rust\"}"}]},
        {"tool_calls":[{"name":"report","args_json":"{\"summary\":\"found\",\"success\":true}"}]}
    ]"#
        .into(),
    );

    let harness = Harness::from_config(config, StateRoot::new(dir.path().join("state"))).unwrap();
    let (sink, rx) = ChannelSink::pair();
    let result = harness
        .run_with_events(RunRequest::new("search"), Some(Arc::new(sink)))
        .await
        .unwrap();
    assert!(result.success, "{}", result.summary);

    let mut details = Vec::new();
    while let Ok(event) = rx.try_recv() {
        if let HarnessEvent::ToolEnd {
            name, ok, detail, ..
        } = event
        {
            details.push((name, ok, detail));
        }
    }
    let search = details
        .iter()
        .find(|(name, _, _)| name == "web_search")
        .expect("web_search tool_end");
    assert!(search.1, "{search:?}");
    assert!(search.2.contains("Title 1"), "{}", search.2);
    assert!(
        search.2.contains("url=https://example.com/1"),
        "{}",
        search.2
    );
    assert!(seen.load(Ordering::SeqCst) >= 1);
}

#[tokio::test]
async fn unset_endpoint_keeps_web_search_off_a_scripted_run() {
    let dir = tempdir().unwrap();
    let mut config = offline_config(dir.path());
    config.model.script_json = Some(
        r#"[
        {"tool_calls":[{"name":"web_search","args_json":"{\"query\":\"mars rust\"}"}]},
        {"tool_calls":[{"name":"report","args_json":"{\"summary\":\"skipped\",\"success\":true}"}]}
    ]"#
        .into(),
    );
    let harness = Harness::from_config(config, StateRoot::new(dir.path().join("state"))).unwrap();
    let (sink, rx) = ChannelSink::pair();
    let result = harness
        .run_with_events(RunRequest::new("no search"), Some(Arc::new(sink)))
        .await
        .unwrap();
    assert!(result.success, "{}", result.summary);
    let mut saw_disabled = false;
    while let Ok(event) = rx.try_recv() {
        if let HarnessEvent::ToolEnd {
            name, ok, detail, ..
        } = event
            && name == "web_search"
        {
            assert!(!ok, "{detail}");
            assert!(
                detail.contains("not enabled") || detail.contains("disabled"),
                "{detail}"
            );
            saw_disabled = true;
        }
    }
    assert!(saw_disabled);
}

fn offline_config(root: &Path) -> Config {
    let mut config = Config::default();
    config.governance.adapter = "local".into();
    config.model.adapter = "scripted".into();
    config.events.adapter = "none".into();
    config.workspace.root = root.join("ws").to_string_lossy().into();
    config
}

async fn spawn_search_endpoint(seen: Arc<AtomicUsize>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                break;
            };
            seen.fetch_add(1, Ordering::SeqCst);
            let mut buf = vec![0; 8192];
            let _ = sock.read(&mut buf).await;
            let body = serde_json::json!({
                "results": [{
                    "title": "Title 1",
                    "url": "https://example.com/1",
                    "snippet": "Mars looks red because its soil rusts."
                }]
            })
            .to_string();
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = sock.write_all(response.as_bytes()).await;
        }
    });
    format!("http://127.0.0.1:{}/search", addr.port())
}
