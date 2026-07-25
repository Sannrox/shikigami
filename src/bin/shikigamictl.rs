//! `shikigamictl` — embedded CLI host for the Shikigami harness core.

use std::env;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use shikigami::{Config, PRODUCT, PRODUCT_DESCRIPTION, StateRoot, VERSION};

#[derive(Debug, Parser)]
#[command(
    name = "shikigamictl",
    version = VERSION,
    about = PRODUCT_DESCRIPTION,
    long_about = None
)]
struct Cli {
    /// Override the state root (default: ./.shikigami-state).
    #[arg(long, global = true, env = "SHIKIGAMI_STATE")]
    state: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Print product identity as human text or JSON.
    Version {
        #[arg(long)]
        json: bool,
    },
    /// Initialize local harness state in the current directory.
    Init,
    /// Check local harness prerequisites and configuration.
    Doctor,
    /// Run a harness task (not yet implemented).
    Run {
        /// Task prompt or path placeholder for the future run contract.
        #[arg(value_name = "TASK")]
        task: Option<String>,
    },
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("error: {err:#}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let cwd = env::current_dir()?;
    let state = match cli.state {
        Some(path) => StateRoot::new(path),
        None => StateRoot::default_in(cwd),
    };

    match cli.command {
        Command::Version { json } => {
            if json {
                let body = serde_json::json!({
                    "product": PRODUCT,
                    "version": VERSION,
                    "description": PRODUCT_DESCRIPTION,
                });
                println!("{}", serde_json::to_string_pretty(&body)?);
            } else {
                println!("{PRODUCT} {VERSION}");
            }
        }
        Command::Init => {
            let config = state.init()?;
            println!(
                "initialized {} ({})",
                state.path().display(),
                Config::FILENAME
            );
            if config.control_plane.is_none() {
                println!("control plane: not configured (optional until a run requires it)");
            }
        }
        Command::Doctor => {
            let mut ok = true;
            println!("{PRODUCT} doctor");
            println!("  version: {VERSION}");
            println!("  state:   {}", state.path().display());
            if state.exists() {
                let config = state.load_config()?;
                println!("  config:  ok (version {})", config.version);
                match config.control_plane.as_deref() {
                    Some(endpoint) => println!("  chisei:  {endpoint}"),
                    None => println!("  chisei:  not configured"),
                }
                match config.tenkai_environment.as_deref() {
                    Some(env_name) => println!("  tenkai:  environment {env_name}"),
                    None => println!("  tenkai:  environment not configured"),
                }
            } else {
                println!("  config:  missing (run `shikigamictl init`)");
                ok = false;
            }
            if !ok {
                anyhow::bail!("doctor found problems");
            }
            println!("status: ok");
        }
        Command::Run { task } => {
            let _config = state.load_config().map_err(|_| {
                anyhow::anyhow!("state not initialized; run `shikigamictl init` first")
            })?;
            let task = task.unwrap_or_default();
            anyhow::bail!(
                "run is not implemented yet{}",
                if task.is_empty() {
                    String::new()
                } else {
                    format!(" (received task: {task})")
                }
            );
        }
    }

    Ok(())
}
