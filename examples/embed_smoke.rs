//! Minimal **host proof**: embed `Harness` offline without the CLI.
//!
//! Run:
//! ```bash
//! cargo run --example embed_smoke
//! ```
//!
//! This is the smallest external-host pattern for ADR 0004 offline embed smoke.

use std::sync::Arc;

use shikigami::{
    ChannelSink, Config, ExportOptions, Harness, HarnessEvent, RunRequest, StateRoot,
    export_run_transcript,
};
use tempfile::tempdir;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let state = StateRoot::new(dir.path().join("state"));
    let mut config = Config::default();
    config.governance.adapter = "local".into();
    config.model.adapter = "scripted".into();
    config.events.adapter = "none".into();
    config.workspace.root = dir.path().join("ws").to_string_lossy().into();

    let harness = Harness::from_config(config, state.clone())?;

    let doctor = harness.doctor_async().await;
    assert!(doctor.ok, "doctor failed: {:?}", doctor.lines);
    println!("doctor ok profile={}", doctor.profile);

    let (sink, rx) = ChannelSink::pair();
    let mut req = RunRequest::new("embed smoke demo");
    req.keep_workspace = true;
    let result = harness.run_with_events(req, Some(Arc::new(sink))).await?;

    let mut tools = 0u32;
    while let Ok(ev) = rx.try_recv() {
        if matches!(ev, HarnessEvent::ToolStart { .. }) {
            tools += 1;
        }
    }

    println!(
        "run ok id={} success={} turns={} tools_seen={} summary={}",
        result.run_id, result.success, result.turns, tools, result.summary
    );
    assert!(result.success);
    assert!(result.turns >= 1);
    assert!(
        result.workspace.join("SHIKIGAMI_OK.txt").is_file(),
        "marker missing"
    );

    let jsonl =
        export_run_transcript(&state.runs_dir(), &result.run_id, &ExportOptions::default())?;
    assert!(jsonl.contains("schema_version"));
    println!(
        "transcript export ok ({} bytes, schema lines present)",
        jsonl.len()
    );

    println!("embed_smoke: PASS");
    Ok(())
}
