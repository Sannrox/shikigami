# Embedding Shikigami

The `shikigami` CLI is a thin host. Prefer the **library** when you need
structured results, cancellation hooks, or an in-process UI.

## Host proof ranking (v0.2+)

| Rank | Surface | Status | Proof |
| --- | --- | --- | --- |
| 1 | Library `Harness` (in-repo) | **Primary** offline host; ADR 0004 embed path | `cargo run --locked --example embed_smoke` — **CI-gated** on PR/`main` |
| 1b | Library `Harness` (external) | ADR 0004 **external** embed smoke | [`Sannrox/shikigami-embed-smoke`](https://github.com/Sannrox/shikigami-embed-smoke) — out-of-tree consumer on git tag `v0.2.0`, CI `cargo run --locked` |
| 2 | CLI (`doctor` / `run` / `serve` / `export`) | Operator path; thin over the same core | Offline `examples/local-run.toml` demo in README |
| 3 | MCP stdio server (`shikigami mcp`) | **Optional** host for MCP-native clients | [mcp.md](mcp.md) + [examples/mcp-host.example.json](../examples/mcp-host.example.json) |

Do **not** treat MCP or a future interactive TUI as the 1.0 library freeze
surface. The external consumer depends on a **released tag** of this crate and
is maintained in a separate repository so the freeze checklist is not circular.

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

## Public surface (1.0 expectations)

| Item | Notes |
| --- | --- |
| `Harness` | Primary entry: `doctor`, `doctor_async`, `run`, `run_with_events` |
| `Config` / `ConfigSource` | Versioned settings (`version = 1`, deny unknown) |
| `StateRoot` | Local state layout |
| `RunRequest` / `RunResult` | Run I/O (incl. park, usage, optional `cost`) |
| `DoctorReport` / `HarnessError` | Diagnostics and errors |
| `export_run_transcript` / `ExportOptions` | Offline JSONL from checkpoints |
| `governance::GovernancePort` | Trait for custom governance (may still grow; not freeze-core) |
| CLI subcommands | `version`, `doctor`, `run`, `serve` freeze-core; `mcp`, `export` host-adjacent — flags may grow |

Prefer depending on freeze-core surfaces below and
[ADR 0004](decisions/0004-v1-contract.md).

## Host responsibilities

- Provide task text and any human approval outside the loop.
- Configure fail-closed profiles only when a plane is actually available.
- Treat workspace paths as untrusted filesystem roots for concurrent hosts.
- Do not shell out to the CLI when you need structured errors or metrics.

## Related

- [settings.md](settings.md) — configuration
- [adapters.md](adapters.md) — ports
- [../DESIGN.md](../DESIGN.md) — architecture


## 1.0 freeze core vs evolving surfaces

At **1.0**, the following are **freeze-core** for embedders (semver breakage
requires a major bump). Additive fields with defaults remain OK.

### Prefer depending on (freeze core)

Aligned with [ADR 0004](decisions/0004-v1-contract.md) medium 1.0 contract.

| Surface | Notes |
| --- | --- |
| `Harness::{from_config, resolve, doctor, doctor_async, run, run_with_events}` | Primary entry |
| `Config` / settings `version = 1` fields with defaults | Unknown keys rejected |
| `RunRequest::new` + `timeout` / `cancel` / `resume_run_id` / `keep_workspace` / `logical_operation_id` / `resume_answer` | Bounds, resume, plane op correlation |
| `RunResult` fields including `termination`, `park`, `prompt_id`, token `usage` | Structured outcomes |
| `RunResult.cost` when rates configured | Optional estimate only; absent ≠ zero |
| `HarnessEvent` + `ChannelSink` / `EventSink` | Live in-process progress (additive events OK) |
| `export_run_transcript` + export `schema_version = 1` line shapes | Offline host audit path |
| `DoctorReport` JSON `schema_version = 1` keys | Automation contract |
| CLI subcommands `version` / `doctor` / `run` / `serve` | Flags may grow; core names stable |

### Live event stream

```rust
use shikigami::{ChannelSink, Harness, HarnessEvent, RunRequest};
use std::sync::Arc;

// CLI can keep events.adapter = "stderr"; library adds a channel:
let (sink, rx) = ChannelSink::pair();
let result = harness
    .run_with_events(RunRequest::new("task"), Some(Arc::new(sink)))
    .await?;
while let Ok(ev) = rx.try_recv() {
    match ev {
        HarnessEvent::ToolStart { name, .. } => { /* progress UI */ }
        HarnessEvent::RunFinished { .. } => {}
        _ => {}
    }
}
```

Events are best-effort for UI (not durable plane truth). Crash recovery uses checkpoints.

### Offline transcript export

Export a completed or parked run from local checkpoint state (no plane):

```rust
use shikigami::{export_run_transcript, ExportOptions, StateRoot};

let state = StateRoot::default_in(".");
let jsonl = export_run_transcript(
    &state.runs_dir(),
    "RUN_ID",
    &ExportOptions::default(),
)?;
// JSONL lines: meta, message*, todos?, park?, end — each has schema_version = 1
```

CLI: `shikigami export <run_id> [-o transcript.jsonl]`. Fields are truncated and
optional config redaction applies the same secret scrubbing as doctor.

### Evolving / host-only (not freeze core)

| Surface | Notes |
| --- | --- |
| `GovernancePort` trait methods | Will grow for authz/harvest |
| Checkpoint file format beyond v1 | Versioned; migrations may appear |
| Event sink payload shapes | Additive preferred |
| Feature flags and optional deps | May split further |
| Serve queue protocol | Local FS queue first; HTTP later |
| MCP server tool set / framing details | Optional host; tools may grow; stdio-only for now |
| MCP client transports and settings | Integration surface; not embed freeze list |
| Lifecycle hooks (`[[hooks]]`) | Settings-driven; schema may grow |
| Interactive TUI | Explicit non-goal for default product; not in this crate’s 1.0 must-haves |
| Cost rate settings field names | Optional ops; misconfig is operator error |

### CHANGELOG policy for embedders

- **Breaking** embed API or doctor JSON key removals: call out under `### Changed` / `### Breaking` and bump the relevant schema version when applicable.
- **Additive** fields with defaults: same settings `version` or same doctor `schema_version` is OK.
