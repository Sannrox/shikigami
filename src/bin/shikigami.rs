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
        /// Task specification (required).
        task: String,
        /// Keep the workspace directory after a successful run.
        #[arg(long)]
        keep_workspace: bool,
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
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "ok": report.ok,
                        "profile": report.profile,
                        "governance": report.governance,
                        "workspace": report.workspace,
                        "events": report.events,
                        "model": report.model,
                        "lines": report.lines,
                    }))?
                );
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
        } => {
            let harness = Harness::resolve(cli.config.as_deref(), state, &cwd)?;
            let result = harness
                .run(RunRequest {
                    task,
                    keep_workspace,
                })
                .await?;
            println!(
                "run {} turns={} success={} summary={}",
                result.run_id, result.turns, result.success, result.summary
            );
            println!("workspace {}", result.workspace.display());
            if !result.success {
                anyhow::bail!("run reported failure");
            }
        }
    }
    Ok(())
}
