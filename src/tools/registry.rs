//! Tool registry: enabled builtins, external tools, todos, web_fetch, background bash.

use std::path::PathBuf;
use std::process::Stdio;
use std::sync::{Arc, Mutex};

use serde::Deserialize;
use tokio::process::Command;

use crate::config::NetworkSettings;

use super::bash::{BackgroundJobs, BgJob, MAX_BG_JOBS, MAX_BG_LOG_BYTES};
use super::catalog::model_visible_builtin_definitions;
use super::executor::{BashArgs, ToolExecutor};
use super::todo::{TodoItem, apply_todo_write, format_todo_summary};
use super::web_fetch::{
    WEB_FETCH_MAX_REDIRECTS, WebFetchArgs, WebFetcher, default_web_fetcher, validate_web_fetch_url,
};
use super::{ToolDef, ToolError, ToolOutput, parse};

/// External tool provider (e.g. MCP-backed tool).
#[async_trait::async_trait]
pub trait ExternalTool: Send + Sync {
    fn definition(&self) -> ToolDef;
    async fn call(&self, args_json: &str) -> Result<String, ToolError>;
}

/// Run-scoped tool registry: definitions + jailed execution for enabled tools.
///
/// The turn loop talks only to this type (not individual dispatch tables).
pub struct ToolRegistry {
    executor: ToolExecutor,
    external: Vec<std::sync::Arc<dyn ExternalTool>>,
    /// Run-scoped checklist (shared; updated by `todo_write`).
    todos: Arc<Mutex<Vec<TodoItem>>>,
    network: NetworkSettings,
    web_fetcher: Arc<dyn WebFetcher>,
    bg_jobs: Arc<Mutex<BackgroundJobs>>,
}

impl ToolRegistry {
    /// Bootstrap builtins filtered by the settings allow-list.
    pub fn with_builtins(
        workspace: impl Into<PathBuf>,
        enabled: Vec<String>,
        bash_timeout_secs: u64,
        network: NetworkSettings,
    ) -> Result<Self, ToolError> {
        Self::with_builtins_ignore(workspace, enabled, bash_timeout_secs, network, true)
    }

    pub fn with_builtins_ignore(
        workspace: impl Into<PathBuf>,
        enabled: Vec<String>,
        bash_timeout_secs: u64,
        network: NetworkSettings,
        respect_ignore: bool,
    ) -> Result<Self, ToolError> {
        Self::with_builtins_protected_environment(
            workspace,
            enabled,
            bash_timeout_secs,
            network,
            respect_ignore,
            &[],
        )
    }

    pub(crate) fn with_builtins_protected_environment(
        workspace: impl Into<PathBuf>,
        enabled: Vec<String>,
        bash_timeout_secs: u64,
        network: NetworkSettings,
        respect_ignore: bool,
        protected_environment_names: &[String],
    ) -> Result<Self, ToolError> {
        let web_fetcher = default_web_fetcher()?;
        Ok(Self {
            executor: ToolExecutor::new_with_protected_environment(
                workspace,
                enabled,
                bash_timeout_secs,
                respect_ignore,
                protected_environment_names,
            )?,
            external: Vec::new(),
            todos: Arc::new(Mutex::new(Vec::new())),
            network,
            web_fetcher,
            bg_jobs: Arc::new(Mutex::new(BackgroundJobs::new())),
        })
    }

    /// Kill all background bash jobs (run end cleanup).
    pub async fn kill_background_jobs(&self) {
        let children: Vec<tokio::process::Child> = {
            let mut guard = match self.bg_jobs.lock() {
                Ok(g) => g,
                Err(_) => return,
            };
            guard.jobs.drain().map(|(_id, job)| job.child).collect()
        };
        for mut child in children {
            let _ = child.start_kill();
            let _ = child.wait().await;
        }
    }

    /// Override the HTTP client (offline tests).
    pub fn set_web_fetcher(&mut self, fetcher: Arc<dyn WebFetcher>) {
        self.web_fetcher = fetcher;
    }

    pub fn register_external(&mut self, tool: std::sync::Arc<dyn ExternalTool>) {
        self.external.push(tool);
    }

    /// Replace the in-memory todo list (e.g. when resuming a checkpoint).
    pub fn set_todos(&self, items: Vec<TodoItem>) {
        if let Ok(mut guard) = self.todos.lock() {
            *guard = items;
        }
    }

    /// Snapshot of the current run-scoped todo list.
    pub fn todos(&self) -> Vec<TodoItem> {
        self.todos.lock().map(|g| g.clone()).unwrap_or_default()
    }

    /// Model-facing tool definitions for enabled builtins + external tools.
    pub fn definitions(&self) -> Vec<ToolDef> {
        let mut defs = model_visible_builtin_definitions(&self.executor.enabled);
        for t in &self.external {
            defs.push(t.definition());
        }
        defs
    }

    pub fn enabled(&self) -> &[String] {
        &self.executor.enabled
    }

    pub fn is_enabled(&self, name: &str) -> bool {
        self.executor.enabled.iter().any(|e| e == name)
            || self.external.iter().any(|t| t.definition().name == name)
    }

    pub async fn execute(&self, name: &str, args_json: &str) -> Result<ToolOutput, ToolError> {
        if name == "todo_write" {
            if !self.executor.enabled.iter().any(|e| e == name) {
                return Err(ToolError::Disabled(name.into()));
            }
            let items = apply_todo_write(args_json)?;
            {
                let mut guard = self
                    .todos
                    .lock()
                    .map_err(|_| ToolError::Message("todo list lock poisoned".into()))?;
                *guard = items.clone();
            }
            let summary = format_todo_summary(&items);
            return Ok(ToolOutput::Text(summary));
        }
        if name == "web_fetch" {
            if !self.executor.enabled.iter().any(|e| e == name) {
                return Err(ToolError::Disabled(name.into()));
            }
            let text = self.web_fetch(args_json).await?;
            return Ok(ToolOutput::Text(text));
        }
        if matches!(
            name,
            "bash_background" | "bash_job_status" | "bash_job_logs"
        ) {
            // Same authority as bash: require bash in the allow-list.
            if !self.executor.enabled.iter().any(|e| e == "bash") {
                return Err(ToolError::Disabled(name.into()));
            }
            let text = match name {
                "bash_background" => self.bash_background(args_json).await?,
                "bash_job_status" => self.bash_job_status(args_json).await?,
                "bash_job_logs" => self.bash_job_logs(args_json)?,
                _ => unreachable!(),
            };
            return Ok(ToolOutput::Text(text));
        }
        if let Some(t) = self.external.iter().find(|t| t.definition().name == name) {
            return Ok(ToolOutput::Text(t.call(args_json).await?));
        }
        self.executor.execute(name, args_json).await
    }

    async fn bash_background(&self, args_json: &str) -> Result<String, ToolError> {
        let args: BashArgs = parse("bash_background", args_json)?;
        let mut guard = self
            .bg_jobs
            .lock()
            .map_err(|_| ToolError::Message("bg jobs lock poisoned".into()))?;
        if guard.jobs.len() >= MAX_BG_JOBS {
            return Err(ToolError::Message(format!(
                "bash_background: at most {MAX_BG_JOBS} concurrent jobs"
            )));
        }
        let job_id = format!("job-{}", guard.next_id);
        guard.next_id += 1;
        let log_dir = self.executor.workspace.join(".shikigami/jobs");
        std::fs::create_dir_all(&log_dir)?;
        let log_path = log_dir.join(format!("{job_id}.log"));
        let log_file = std::fs::File::create(&log_path)?;
        let stdout = Stdio::from(log_file.try_clone()?);
        let stderr = Stdio::from(log_file);
        let mut command = Command::new("bash");
        command
            .arg("--noprofile")
            .arg("--norc")
            .arg("-c")
            .arg(&args.command)
            .current_dir(&self.executor.workspace)
            .stdout(stdout)
            .stderr(stderr)
            .kill_on_drop(true);
        self.executor.environment.apply(&mut command);
        let child = command.spawn()?;
        guard.jobs.insert(
            job_id.clone(),
            BgJob {
                child,
                log_path: log_path.clone(),
            },
        );
        Ok(format!("job_id={job_id}\nlog={}", log_path.display()))
    }

    async fn bash_job_status(&self, args_json: &str) -> Result<String, ToolError> {
        #[derive(Deserialize)]
        struct Args {
            job_id: String,
        }
        let args: Args = parse("bash_job_status", args_json)?;
        let mut guard = self
            .bg_jobs
            .lock()
            .map_err(|_| ToolError::Message("bg jobs lock poisoned".into()))?;
        let Some(job) = guard.jobs.get_mut(&args.job_id) else {
            return Ok(format!("job_id={} status=unknown", args.job_id));
        };
        match job.child.try_wait() {
            Ok(Some(status)) => Ok(format!(
                "job_id={} status=exited code={}",
                args.job_id,
                status.code().unwrap_or(-1)
            )),
            Ok(None) => Ok(format!("job_id={} status=running", args.job_id)),
            Err(e) => Err(ToolError::Message(format!("bash_job_status: {e}"))),
        }
    }

    fn bash_job_logs(&self, args_json: &str) -> Result<String, ToolError> {
        #[derive(Deserialize)]
        struct Args {
            job_id: String,
            max_bytes: Option<usize>,
        }
        let args: Args = parse("bash_job_logs", args_json)?;
        let max = args
            .max_bytes
            .unwrap_or(MAX_BG_LOG_BYTES)
            .clamp(1, MAX_BG_LOG_BYTES);
        let guard = self
            .bg_jobs
            .lock()
            .map_err(|_| ToolError::Message("bg jobs lock poisoned".into()))?;
        let Some(job) = guard.jobs.get(&args.job_id) else {
            return Err(ToolError::Message(format!(
                "bash_job_logs: unknown job {}",
                args.job_id
            )));
        };
        let data = std::fs::read(&job.log_path)?;
        let slice = if data.len() > max {
            &data[data.len() - max..]
        } else {
            &data[..]
        };
        let mut text = String::from_utf8_lossy(slice).into_owned();
        if data.len() > max {
            text = format!("…[truncated]\n{text}");
        }
        if text.is_empty() {
            text = "(empty)".into();
        }
        Ok(text)
    }

    async fn web_fetch(&self, args_json: &str) -> Result<String, ToolError> {
        let args: WebFetchArgs = parse("web_fetch", args_json)?;
        let mut url = args.url.trim().to_string();
        for redirects in 0..=WEB_FETCH_MAX_REDIRECTS {
            validate_web_fetch_url(&url)?;
            self.network
                .check_http_url(&url)
                .map_err(ToolError::Message)?;
            let resp = self.web_fetcher.get(&url).await?;
            if resp.final_url != url {
                if redirects == WEB_FETCH_MAX_REDIRECTS {
                    return Err(ToolError::Message(format!(
                        "web_fetch exceeded {WEB_FETCH_MAX_REDIRECTS} redirects"
                    )));
                }
                url = resp.final_url;
                continue;
            }
            return Ok(format!(
                "status={}\nfinal_url={}\n\n{}",
                resp.status, resp.final_url, resp.body
            ));
        }
        unreachable!("bounded redirect loop always returns")
    }
}
