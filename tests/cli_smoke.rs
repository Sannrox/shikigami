use assert_cmd::cargo::cargo_bin_cmd;
use predicates::prelude::*;
use std::fs;
use tempfile::tempdir;

#[test]
fn version_prints_product_identity() {
    cargo_bin_cmd!("shikigami")
        .arg("version")
        .assert()
        .success()
        .stdout(predicate::str::contains(format!(
            "shikigami {}",
            env!("CARGO_PKG_VERSION")
        )));
}

#[test]
fn doctor_succeeds_on_local_defaults() {
    let dir = tempdir().expect("tempdir");
    let state = dir.path().join("state");

    cargo_bin_cmd!("shikigami")
        .args(["--state", state.to_str().unwrap(), "doctor"])
        .assert()
        .success()
        .stdout(predicate::str::contains("status: ok"))
        .stdout(predicate::str::contains("profile:   local"))
        .stdout(predicate::str::contains("gov:       none"))
        .stdout(predicate::str::contains("tools.mode:       custom"))
        .stdout(predicate::str::contains("tools.configured: (none)"))
        .stdout(predicate::str::contains("tools.implicit:   (none)"))
        .stdout(predicate::str::contains(
            "tools.visible:    [read_file, write_file",
        ));
}

#[test]
fn doctor_fails_governed_without_endpoint() {
    let dir = tempdir().expect("tempdir");
    let state = dir.path().join("state");
    let config = dir.path().join("governed.toml");
    fs::write(
        &config,
        r#"
version = 1
[profile]
name = "governed"
"#,
    )
    .expect("write config");

    cargo_bin_cmd!("shikigami")
        .args([
            "--state",
            state.to_str().unwrap(),
            "--config",
            config.to_str().unwrap(),
            "doctor",
        ])
        .assert()
        .failure()
        .stdout(predicate::str::contains("status: fail"));
}

#[test]
fn local_scripted_run_writes_marker() {
    let dir = tempdir().expect("tempdir");
    let state = dir.path().join("state");
    let config = dir.path().join("local.toml");
    fs::write(
        &config,
        r#"
version = 1
[profile]
name = "local"
[governance]
adapter = "local"
[model]
adapter = "scripted"
[workspace]
adapter = "directory"
root = "."
[events]
adapter = "none"
"#,
    )
    .expect("write");

    cargo_bin_cmd!("shikigami")
        .args([
            "--state",
            state.to_str().unwrap(),
            "--config",
            config.to_str().unwrap(),
            "run",
            "scripted demo",
            "--keep-workspace",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("success=true"));
}

#[test]
fn plane_intake_rejects_ungoverned_host() {
    let dir = tempdir().expect("tempdir");
    let state = dir.path().join("state");

    cargo_bin_cmd!("shikigami")
        .args([
            "--state",
            state.to_str().unwrap(),
            "serve",
            "--intake",
            "plane",
            "--max-jobs",
            "1",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "plane intake requires governance.adapter = \"sekai-chisei\"",
        ));
}
