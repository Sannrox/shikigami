# Embedding Shikigami

The `shikigami` CLI is a thin host. Prefer the **library** when you need
structured results, cancellation hooks, or an in-process UI.

## Minimal example

```rust
use shikigami::{Config, Harness, RunRequest, StateRoot};

async fn example() -> Result<(), shikigami::HarnessError> {
    let state = StateRoot::default_in(".");
    let mut config = Config::default();
    config.governance.adapter = "local".into();
    config.model.adapter = "scripted".into();
    config.events.adapter = "none".into();

    let harness = Harness::from_config(config, state)?;

    let report = harness.doctor_async().await;
    if !report.ok {
        return Err(shikigami::HarnessError::Doctor(report.lines.join("; ")));
    }

    let mut request = RunRequest::new("implement the change described in the issue");
    request.keep_workspace = true;
    // Optional: request.timeout = Some(std::time::Duration::from_secs(600));
    // Optional: request.cancel = Some(cancel_rx); // watch::Receiver<bool>
    let result = harness.run(request).await?;

    println!(
        "run={} success={} turns={} termination={:?} summary={}",
        result.run_id, result.success, result.turns, result.termination, result.summary
    );
    println!("workspace={}", result.workspace.display());
    Ok(())
}
```

## Resolving settings like the CLI

```rust
use std::env;
use shikigami::{Harness, StateRoot};

async fn from_cwd() -> Result<Harness, shikigami::HarnessError> {
    let cwd = env::current_dir()?;
    let state = StateRoot::default_in(&cwd);
    // Optional: Some(Path::new("/etc/shikigami.toml"))
    Harness::resolve(None, state, &cwd)
}
```

## Public surface (v0 expectations)

| Item | Notes |
| --- | --- |
| `Harness` | Primary entry: `doctor`, `doctor_async`, `run` |
| `Config` / `ConfigSource` | Versioned settings |
| `StateRoot` | Local state layout |
| `RunRequest` / `RunResult` | Run I/O |
| `DoctorReport` / `HarnessError` | Diagnostics and errors |
| `governance::GovernancePort` | Trait for custom governance (pre-1.0: may evolve) |
| CLI subcommands | `version`, `doctor`, `run` — flags may grow |

Pre-1.0: treat trait methods and settings fields as **evolving**. Prefer
depending on `Harness` + `Config` for the most stable path.

## Host responsibilities

- Provide task text and any human approval outside the loop.
- Configure fail-closed profiles only when a plane is actually available.
- Treat workspace paths as untrusted filesystem roots for concurrent hosts.
- Do not shell out to the CLI when you need structured errors or metrics.

## Related

- [settings.md](settings.md) — configuration
- [adapters.md](adapters.md) — ports
- [../DESIGN.md](../DESIGN.md) — architecture


## Pre-1.0 freeze candidates vs evolving surfaces

Until **1.0**, treat the following as *relatively stable* for embedders.
Changes still require CHANGELOG entries; avoid drive-by renames.

### Prefer depending on (freeze candidates)

| Surface | Notes |
| --- | --- |
| `Harness::{from_config, resolve, doctor, doctor_async, run}` | Primary entry |
| `Config` / settings `version = 1` fields with defaults | Unknown keys rejected |
| `RunRequest::new` + `timeout` / `cancel` / `resume_run_id` / `keep_workspace` | Bounds and resume |
| `RunResult` fields including `termination` | Structured outcomes |
| `DoctorReport` JSON `schema_version = 1` keys | Automation contract |
| CLI subcommands `version` / `doctor` / `run` | Flags may grow |

### Evolving (expect churn)

| Surface | Notes |
| --- | --- |
| `GovernancePort` trait methods | Will grow for authz/harvest |
| Checkpoint file format beyond v1 | Versioned; migrations may appear |
| Event sink payload shapes | Additive preferred |
| Feature flags and optional deps | May split further |
| `serve` / daemon host | Not yet shipped |

### CHANGELOG policy for embedders

- **Breaking** embed API or doctor JSON key removals: call out under `### Changed` / `### Breaking` and bump the relevant schema version when applicable.
- **Additive** fields with defaults: same settings `version` or same doctor `schema_version` is OK.
