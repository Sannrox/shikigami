use assert_cmd::cargo::cargo_bin_cmd;
use predicates::prelude::*;
use tempfile::tempdir;

#[test]
fn version_prints_product_identity() {
    cargo_bin_cmd!("shikigamictl")
        .arg("version")
        .assert()
        .success()
        .stdout(predicate::str::contains("shikigami 0.1.0"));
}

#[test]
fn init_then_doctor_succeeds() {
    let dir = tempdir().expect("tempdir");
    let state = dir.path().join("state");

    cargo_bin_cmd!("shikigamictl")
        .args(["--state", state.to_str().unwrap(), "init"])
        .assert()
        .success()
        .stdout(predicate::str::contains("initialized"));

    cargo_bin_cmd!("shikigamictl")
        .args(["--state", state.to_str().unwrap(), "doctor"])
        .assert()
        .success()
        .stdout(predicate::str::contains("status: ok"));
}

#[test]
fn run_requires_init_and_is_not_implemented() {
    let dir = tempdir().expect("tempdir");
    let state = dir.path().join("state");

    cargo_bin_cmd!("shikigamictl")
        .args(["--state", state.to_str().unwrap(), "run", "hello"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("state not initialized"));

    cargo_bin_cmd!("shikigamictl")
        .args(["--state", state.to_str().unwrap(), "init"])
        .assert()
        .success();

    cargo_bin_cmd!("shikigamictl")
        .args(["--state", state.to_str().unwrap(), "run", "hello"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("run is not implemented yet"));
}
