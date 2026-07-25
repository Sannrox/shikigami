//! `shikigami` — thin CLI host over the embeddable harness core.

use std::env;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use shikigami::{Harness, PRODUCT, PRODUCT_DESCRIPTION, RunRequest, StateRoot, VERSION};

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
    },
    /// Execute a harness run.
    Run {
        /// Task specification (optional when --resume is set).
        #[arg(default_value = "")]
        task: String,
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

async fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let cwd = env::current_dir()?;
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
        Command::Doctor { json } => {
            let harness = Harness::resolve(cli.config.as_deref(), state, &cwd)?;
            let report = harness.doctor_async().await;
            if json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                println!("{PRODUCT} doctor");
                println!("  version:  {VERSION}");
                for line in &report.lines {
                    println!("  {line}");
                }
                println!("status: {}", if report.ok { "ok" } else { "fail" });
            }
            if !report.ok {
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
        } => {
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
            let harness = Harness::resolve(cli.config.as_deref(), state, &cwd)?;
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
    }
    Ok(())
}
