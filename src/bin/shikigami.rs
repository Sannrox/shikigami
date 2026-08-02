//! `shikigami` — thin CLI host over the embeddable harness core.

use std::env;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand, ValueEnum};
use shikigami::{
    AvailableModel, Harness, PRODUCT, PRODUCT_DESCRIPTION, QueueLayout, RunRequest, ServeOptions,
    StateRoot, VERSION, WorkerLifecycle, WorkerLifecycleIdentity, serve_lifecycle_http,
};

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ServeIntake {
    Filesystem,
    Plane,
}

#[derive(Debug, Parser)]
#[command(
    name = "shikigami",
    version = VERSION,
    about = PRODUCT_DESCRIPTION,
    long_about = None
)]
struct Cli {
    #[arg(long, global = true, env = "SHIKIGAMI_STATE")]
    state: Option<PathBuf>,

    #[arg(long, global = true, env = "SHIKIGAMI_CONFIG")]
    config: Option<PathBuf>,

    /// Override the configured model; use `auto` for plane routing.
    #[arg(long, global = true, env = "SHIKIGAMI_MODEL")]
    model: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Version {
        #[arg(long)]
        json: bool,
    },
    /// Check effective settings, adapters, and plane reachability.
    Doctor {
        #[arg(long)]
        json: bool,
        /// Include the available model catalog (or the configured local model).
        #[arg(long)]
        models: bool,
    },
    /// Execute a harness run.
    Run {
        /// Task specification (optional when --resume is set).
        #[arg(default_value = "")]
        task: String,
        /// Read task from a UTF-8 file (avoids exposing prompts on argv).
        #[arg(long)]
        task_file: Option<PathBuf>,
        /// Keep the workspace directory after a successful run.
        #[arg(long)]
        keep_workspace: bool,
        /// Overall wall-clock timeout in seconds (checked at turn boundaries).
        #[arg(long, env = "SHIKIGAMI_RUN_TIMEOUT_SECS")]
        timeout_secs: Option<u64>,
        /// Resume a previous run from its local checkpoint.
        #[arg(long)]
        resume: Option<String>,
        /// Operator answer when resuming a parked (`escalate`) run.
        #[arg(long)]
        answer: Option<String>,
        /// Read operator answer from a file (UTF-8); alternative to --answer.
        #[arg(long)]
        answer_file: Option<PathBuf>,
    },
    /// Long-running filesystem-queue or plane-claim host (see docs/serve.md).
    Serve {
        /// Work intake source. Filesystem remains the offline default.
        #[arg(long, value_enum, default_value_t = ServeIntake::Filesystem)]
        intake: ServeIntake,
        /// Poll interval for the inbox in milliseconds.
        #[arg(long, default_value_t = 200)]
        poll_ms: u64,
        /// Exit after processing this many jobs (tests / oneshot drain).
        #[arg(long)]
        max_jobs: Option<u64>,
        /// Stable runtime id used to filter and hold plane claims.
        #[arg(long, default_value = "shikigami")]
        runtime_id: String,
        /// Plane claim lease TTL. Heartbeats run at one third of this duration.
        #[arg(long, default_value_t = 60)]
        claim_ttl_secs: u64,
        /// Plane-allowlisted logical checkpoint store id for local run checkpoints.
        #[arg(long)]
        checkpoint_store_id: Option<String>,
        /// Opaque worker identity written into the plane lifecycle snapshot.
        /// Defaults to hostname or `shikigami-worker`.
        #[arg(long, env = "SHIKIGAMI_WORKER_ID")]
        worker_id: Option<String>,
        /// Optional loopback HTTP bind for lifecycle probes (e.g. `127.0.0.1:8080`).
        /// Serves GET /lifecycle, /readyz, /livez. Plane intake only.
        #[arg(long, env = "SHIKIGAMI_LIFECYCLE_LISTEN")]
        lifecycle_listen: Option<String>,
    },
    /// MCP server over stdio (`doctor` + `run` tools). See docs/mcp.md.
    ///
    /// Stdio only — no network bind. Not a multi-tenant control plane;
    /// prefer library embed for in-process hosts.
    Mcp,
    /// Export a run transcript as JSONL from local checkpoint state.
    Export {
        /// Run id under the state root (`runs/<id>/checkpoint.json`).
        run_id: String,
        /// Write to this path instead of stdout.
        #[arg(long, short = 'o')]
        output: Option<PathBuf>,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("error: {err:#}");
            ExitCode::FAILURE
        }
    }
}

fn default_worker_id() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("HOST"))
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "shikigami-worker".into())
}

async fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let cwd = env::current_dir()?;
    let model = cli.model.as_deref();
    let state = match cli.state {
        Some(path) => StateRoot::new(path),
        None => StateRoot::default_in(&cwd),
    };

    match cli.command {
        Command::Version { json } => {
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "product": PRODUCT,
                        "version": VERSION,
                        "description": PRODUCT_DESCRIPTION,
                    }))?
                );
            } else {
                println!("{PRODUCT} {VERSION}");
            }
        }
        Command::Doctor { json, models } => {
            let harness = Harness::resolve_with_model(cli.config.as_deref(), state, &cwd, model)?;
            let report = harness.doctor_async().await;
            let (available_models, model_catalog_error) = if models {
                match harness.available_models().await {
                    Ok(models) => (Some(with_auto_route(&harness, models)), None),
                    Err(error) => (Some(Vec::new()), Some(error.to_string())),
                }
            } else {
                (None, None)
            };
            let doctor_ok = report.ok && model_catalog_error.is_none();
            if json {
                let mut output = report.to_json_value();
                if let Some(models) = available_models {
                    output["available_models"] = serde_json::to_value(models)?;
                    output["default_model"] = serde_json::json!(harness.effective_model_name());
                }
                if let Some(error) = &model_catalog_error {
                    output["ok"] = serde_json::json!(false);
                    output["model_catalog_error"] = serde_json::json!(error);
                }
                println!("{}", serde_json::to_string_pretty(&output)?);
            } else {
                println!("{PRODUCT} doctor");
                println!("  version:  {VERSION}");
                for line in &report.lines {
                    println!("  {line}");
                }
                if let Some(models) = available_models {
                    let default_model = harness.effective_model_name();
                    println!("  models:");
                    for model in models {
                        let marker = if model.canonical_model == default_model {
                            " (default)"
                        } else {
                            ""
                        };
                        println!("    {}{}", model.canonical_model, marker);
                    }
                    if let Some(error) = &model_catalog_error {
                        println!("    error: {error}");
                    }
                }
                println!("status: {}", if doctor_ok { "ok" } else { "fail" });
            }
            if !doctor_ok {
                if let Some(error) = model_catalog_error {
                    anyhow::bail!("model catalog unavailable: {error}");
                }
                anyhow::bail!("doctor found problems");
            }
        }
        Command::Run {
            task,
            keep_workspace,
            timeout_secs,
            resume,
            answer,
            answer_file,
            task_file,
        } => {
            let task = match (task.is_empty(), task_file) {
                (false, Some(_)) => {
                    anyhow::bail!("use only one of task argument or --task-file");
                }
                (true, Some(path)) => std::fs::read_to_string(path)?,
                (_, None) => task,
            };
            if resume.is_none() && task.is_empty() {
                anyhow::bail!("task is required unless --resume is set");
            }
            let answer = match (answer, answer_file) {
                (Some(_), Some(_)) => {
                    anyhow::bail!("use only one of --answer or --answer-file");
                }
                (Some(a), None) => Some(a),
                (None, Some(path)) => Some(std::fs::read_to_string(path)?),
                (None, None) => None,
            };
            let harness = Harness::resolve_with_model(cli.config.as_deref(), state, &cwd, model)?;
            let mut request = RunRequest::new(task);
            request.keep_workspace = keep_workspace;
            request.timeout = timeout_secs.map(std::time::Duration::from_secs);
            request.resume_run_id = resume;
            request.resume_answer = answer;
            let result = harness.run(request).await?;
            println!(
                "run {} turns={} success={} termination={} summary={}",
                result.run_id,
                result.turns,
                result.success,
                result.termination.as_str(),
                result.summary
            );
            println!("workspace {}", result.workspace.display());
            if let Some(park) = &result.park {
                println!("parked reason={}", park.reason);
                println!("parked question={}", park.question);
                println!(
                    "resume with: shikigami run --resume {} --answer \"...\"",
                    result.run_id
                );
                // Distinct exit code for park (not silent success).
                return Err(anyhow::anyhow!(
                    "run parked awaiting operator answer (exit semantics: non-zero)"
                ));
            }
            if !result.success {
                anyhow::bail!("run reported failure");
            }
        }
        Command::Serve {
            intake,
            poll_ms,
            max_jobs,
            runtime_id,
            claim_ttl_secs,
            checkpoint_store_id,
            worker_id,
            lifecycle_listen,
        } => {
            let harness =
                Harness::resolve_with_model(cli.config.as_deref(), state.clone(), &cwd, model)?;
            let (tx, rx) = tokio::sync::watch::channel(false);
            let sig_tx = tx.clone();
            tokio::spawn(async move {
                let _ = tokio::signal::ctrl_c().await;
                let _ = sig_tx.send(true);
            });
            // Also handle SIGTERM on Unix for fleet drain (K8s/Tenkai).
            #[cfg(unix)]
            {
                let sig_tx = tx.clone();
                tokio::spawn(async move {
                    let mut sig = match tokio::signal::unix::signal(
                        tokio::signal::unix::SignalKind::terminate(),
                    ) {
                        Ok(s) => s,
                        Err(_) => return,
                    };
                    let _ = sig.recv().await;
                    let _ = sig_tx.send(true);
                });
            }

            let n = match intake {
                ServeIntake::Filesystem => {
                    if lifecycle_listen.is_some() {
                        anyhow::bail!(
                            "--lifecycle-listen applies only to plane intake (managed fleet contract)"
                        );
                    }
                    let layout = QueueLayout::under_state(state.path());
                    layout.ensure().map_err(|e| anyhow::anyhow!(e))?;
                    println!(
                        "serve intake=filesystem inbox={} health={}",
                        layout.inbox.display(),
                        layout.health.display()
                    );
                    let options = ServeOptions {
                        poll_interval: std::time::Duration::from_millis(poll_ms.max(10)),
                        max_jobs,
                    };
                    shikigami::serve::run_serve(&harness, &layout, options, rx)
                        .await
                        .map_err(|e| anyhow::anyhow!(e))?
                }
                ServeIntake::Plane => {
                    // Drop any prior process snapshot immediately so a failed
                    // restart cannot leave fleet-ready state on disk.
                    let _ = std::fs::remove_file(shikigami::lifecycle_path(state.path()));
                    if harness.config.governance.adapter != "sekai-chisei" {
                        anyhow::bail!(
                            "plane intake requires governance.adapter = \"sekai-chisei\""
                        );
                    }
                    if runtime_id.trim().is_empty() {
                        anyhow::bail!("--runtime-id must not be empty");
                    }
                    if claim_ttl_secs == 0 {
                        anyhow::bail!("--claim-ttl-secs must be greater than zero");
                    }
                    if checkpoint_store_id
                        .as_deref()
                        .is_some_and(|store_id| store_id.trim().is_empty())
                    {
                        anyhow::bail!("--checkpoint-store-id must not be empty");
                    }
                    #[cfg(feature = "governance-sekai-chisei")]
                    {
                        let ttl = std::time::Duration::from_secs(claim_ttl_secs);
                        let worker_id = worker_id.unwrap_or_else(default_worker_id);
                        // Open lifecycle before any other fallible plane startup so
                        // supervisors never keep a prior process's ready snapshot.
                        let lifecycle = WorkerLifecycle::open(
                            state.path(),
                            WorkerLifecycleIdentity {
                                worker_id: worker_id.clone(),
                                namespace: harness.config.governance.namespace.clone(),
                                runtime_id: runtime_id.clone(),
                            },
                        )
                        .map_err(|e| anyhow::anyhow!(e))?;
                        let startup_fail =
                            |lc: &WorkerLifecycle, kind: &str, err: anyhow::Error| {
                                let _ = lc.set_unhealthy(kind);
                                let _ = std::fs::remove_file(lc.path());
                                err
                            };
                        let client =
                            shikigami::governance::sekai_chisei::SekaiClaimClient::from_config(
                                &harness.config,
                            )
                            .map_err(|error| {
                                startup_fail(
                                    &lifecycle,
                                    "plane_client_failed",
                                    anyhow::anyhow!(error),
                                )
                            })?;
                        let report = harness.doctor();
                        if !report.ok {
                            return Err(startup_fail(
                                &lifecycle,
                                "doctor_failed",
                                anyhow::anyhow!(
                                    "doctor failed before plane serve; fix configuration first: {}",
                                    report.lines.join("; ")
                                ),
                            ));
                        }
                        if let Some(bind) = &lifecycle_listen {
                            let addr: std::net::SocketAddr = bind.parse().map_err(|e| {
                                startup_fail(
                                    &lifecycle,
                                    "lifecycle_listen_invalid",
                                    anyhow::anyhow!("invalid --lifecycle-listen: {e}"),
                                )
                            })?;
                            // Loopback for local probes; unspecified (0.0.0.0 / ::) for
                            // in-pod K8s probes. Never bind a concrete public interface —
                            // the surface is unauthenticated operational JSON only.
                            if !addr.ip().is_loopback() && !addr.ip().is_unspecified() {
                                return Err(startup_fail(
                                    &lifecycle,
                                    "lifecycle_listen_invalid",
                                    anyhow::anyhow!(
                                        "--lifecycle-listen must be loopback or unspecified (0.0.0.0/[::]), got {addr}"
                                    ),
                                ));
                            }
                            let listen_rx = rx.clone();
                            let bound = serve_lifecycle_http(addr, lifecycle.clone(), listen_rx)
                                .await
                                .map_err(|e| {
                                    startup_fail(
                                        &lifecycle,
                                        "lifecycle_listen_failed",
                                        anyhow::anyhow!(e),
                                    )
                                })?;
                            println!(
                                "serve lifecycle http={} file={}",
                                bound,
                                lifecycle.path().display()
                            );
                        } else {
                            println!("serve lifecycle file={}", lifecycle.path().display());
                        }
                        lifecycle.mark_serving().map_err(|e| anyhow::anyhow!(e))?;
                        let options = shikigami::PlaneServeOptions {
                            poll_interval: std::time::Duration::from_millis(poll_ms.max(10)),
                            max_jobs,
                            claim_ttl: ttl,
                            heartbeat_interval: ttl / 3,
                            ack_retry_limit: 5,
                            checkpoint_store_id,
                            policy: shikigami::ClaimedWorkPolicy {
                                expected_runtime: runtime_id.clone(),
                                host_timeout: harness
                                    .config
                                    .run
                                    .timeout_secs
                                    .map(std::time::Duration::from_secs),
                                ..Default::default()
                            },
                            lifecycle: Some(lifecycle),
                        };
                        println!(
                            "serve intake=plane runtime={} namespace={} ttl_secs={} worker={}",
                            runtime_id,
                            harness.config.governance.namespace,
                            claim_ttl_secs,
                            worker_id
                        );
                        shikigami::run_plane_serve(&harness, &client, options, rx)
                            .await
                            .map_err(|error| anyhow::anyhow!(error))?
                    }
                    #[cfg(not(feature = "governance-sekai-chisei"))]
                    {
                        let _ = (worker_id, lifecycle_listen);
                        anyhow::bail!("plane intake requires the governance-sekai-chisei feature");
                    }
                }
            };
            println!("serve stopped after {n} job(s)");
            drop(tx);
        }
        Command::Mcp => {
            // Protocol uses stdout; keep diagnostics on stderr only.
            eprintln!(
                "{PRODUCT} mcp server (stdio) — tools: doctor, run, run_start, run_status, run_wait"
            );
            let harness = Harness::resolve_with_model(cli.config.as_deref(), state, &cwd, model)?;
            shikigami::mcp_server::run_stdio(harness)
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
        }
        Command::Export { run_id, output } => {
            let harness =
                Harness::resolve_with_model(cli.config.as_deref(), state.clone(), &cwd, model)?;
            let opts = shikigami::ExportOptions {
                max_field_chars: 2_000,
                config: Some(harness.config.clone()),
            };
            let jsonl = shikigami::export_run_transcript(&state.runs_dir(), &run_id, &opts)
                .map_err(|e| anyhow::anyhow!(e))?;
            if let Some(path) = output {
                std::fs::write(&path, &jsonl)?;
                eprintln!(
                    "wrote transcript {} bytes to {}",
                    jsonl.len(),
                    path.display()
                );
            } else {
                print!("{jsonl}");
            }
        }
    }
    Ok(())
}

fn with_auto_route(harness: &Harness, mut models: Vec<AvailableModel>) -> Vec<AvailableModel> {
    if harness.config.governance.adapter == "sekai-chisei"
        && !models.iter().any(|model| model.canonical_model == "auto")
    {
        models.insert(
            0,
            AvailableModel {
                provider: "sekai-chisei".into(),
                upstream_model: "auto".into(),
                canonical_model: "auto".into(),
                lifecycle: "routing".into(),
            },
        );
    }
    models
}
