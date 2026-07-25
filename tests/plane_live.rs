//! Live plane tests — ignored by default.
//!
//! ```bash
//! SEKAI_LIVE=1 SHIKIGAMI_CONTROL_PLANE=http://127.0.0.1:50051 \
//!   cargo test --test plane_live -- --ignored --nocapture
//! ```

use shikigami::{Config, Harness, StateRoot};
use std::env;
use tempfile::tempdir;

fn live_enabled() -> bool {
    env::var("SEKAI_LIVE").ok().as_deref() == Some("1")
}

fn endpoint() -> String {
    env::var("SHIKIGAMI_CONTROL_PLANE").unwrap_or_else(|_| "http://127.0.0.1:50051".into())
}

fn governed_config() -> Config {
    let mut config = Config::default();
    config.profile.name = "governed".into();
    config.governance.adapter = "sekai-chisei".into();
    config.governance.endpoint = Some(endpoint());
    config.governance.fail_closed = true;
    config.model.adapter = "plane".into();
    config
}

#[tokio::test]
#[ignore = "requires live sekai-chisei; set SEKAI_LIVE=1 and SHIKIGAMI_CONTROL_PLANE"]
async fn doctor_probes_live_plane() {
    if !live_enabled() {
        return;
    }
    let dir = tempdir().unwrap();
    let state = StateRoot::new(dir.path().join("state"));
    let harness = Harness::from_config(governed_config(), state).unwrap();
    let report = harness.doctor_async().await;
    assert!(
        report.ok,
        "doctor should pass against live plane: {:?}",
        report.lines
    );
    assert_eq!(report.schema_version, 1);
    assert!(
        report.lines.iter().any(|l| l.contains("plane:")),
        "expected plane line: {:?}",
        report.lines
    );
}

#[tokio::test]
#[ignore = "requires live sekai-chisei; set SEKAI_LIVE=1 and SHIKIGAMI_CONTROL_PLANE"]
async fn doctor_fails_closed_when_endpoint_wrong() {
    if !live_enabled() {
        return;
    }
    let dir = tempdir().unwrap();
    let state = StateRoot::new(dir.path().join("state"));
    let mut config = governed_config();
    config.governance.endpoint = Some("http://127.0.0.1:1".into());
    let harness = Harness::from_config(config, state).unwrap();
    let report = harness.doctor_async().await;
    assert!(
        !report.ok,
        "wrong endpoint must fail closed: {:?}",
        report.lines
    );
}
