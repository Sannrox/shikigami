//! Live plane tests — ignored by default.
//!
//! Run with a local sekai-chisei:
//!   SEKAI_LIVE=1 cargo test --test plane_live -- --ignored --nocapture

use shikigami::{Config, Harness, StateRoot};
use std::env;
use tempfile::tempdir;

#[tokio::test]
#[ignore = "requires live sekai-chisei; set SEKAI_LIVE=1 and SHIKIGAMI_CONTROL_PLANE"]
async fn doctor_probes_live_plane() {
    if env::var("SEKAI_LIVE").ok().as_deref() != Some("1") {
        return;
    }
    let endpoint =
        env::var("SHIKIGAMI_CONTROL_PLANE").unwrap_or_else(|_| "http://127.0.0.1:50051".into());
    let dir = tempdir().unwrap();
    let state = StateRoot::new(dir.path().join("state"));
    let mut config = Config::default();
    config.profile.name = "governed".into();
    config.governance.adapter = "sekai-chisei".into();
    config.governance.endpoint = Some(endpoint);
    config.governance.fail_closed = true;
    config.model.adapter = "plane".into();

    let harness = Harness::from_config(config, state).unwrap();
    let report = harness.doctor_async().await;
    assert!(
        report.ok,
        "doctor should pass against live plane: {:?}",
        report.lines
    );
}
