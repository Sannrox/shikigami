//! Tool registry: enabled builtins, external tools, todos, web_fetch, background bash.

use std::collections::HashSet;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::{Arc, Mutex};

use serde::Deserialize;

use crate::config::{Config, ContextSettings, NetworkSettings, SandboxSettings};

use super::JobCredentialHandle;
use super::bash::{BackgroundJobs, BgJob, MAX_BG_JOBS, MAX_BG_LOG_BYTES};
use super::catalog::{
    BASH_HELPER_TOOLS, builtin_is_authorized, load_skill_definition,
    model_visible_builtin_definitions,
};
use super::executor::{BashArgs, ToolExecutor};
use super::handoff::{apply_handoff, format_handoff_summary};
use super::todo::{TodoItem, apply_todo_write, format_todo_summary};
use super::web_fetch::{
    WEB_FETCH_MAX_REDIRECTS, WebFetchArgs, WebFetcher, default_web_fetcher, validate_web_fetch_url,
};
use super::{ExecutedTool, ToolDef, ToolError, ToolOutput, parse};

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
    /// Set when the run lists lazy skill packs; enables `load_skill`.
    skills: Option<SkillLoader>,
}

/// `load_skill` state: the allow-list plus which packs this run has loaded.
struct SkillLoader {
    settings: ContextSettings,
    loaded: Mutex<LoadedSkills>,
}

#[derive(Default)]
struct LoadedSkills {
    seen: HashSet<String>,
    /// `skill <id> digest=<sha256>` lines not yet emitted as events.
    pending: Vec<String>,
}

impl ToolRegistry {
    /// Build the run-scoped tool module from resolved harness settings.
    ///
    /// This is the preferred harness-facing constructor: tool authority,
    /// network policy, ignore behavior, protected environment names, and
    /// sandbox policy stay coordinated behind the registry interface.
    pub fn from_config(workspace: impl Into<PathBuf>, config: &Config) -> Result<Self, ToolError> {
        Self::from_config_enabled(workspace, config, config.tools.effective_enabled())
    }

    /// Same as [`Self::from_config`] with an explicit allow-list (nested tools).
    pub fn from_config_enabled(
        workspace: impl Into<PathBuf>,
        config: &Config,
        enabled: Vec<String>,
    ) -> Result<Self, ToolError> {
        Self::with_builtins_sandbox_protected_environment(
            workspace,
            enabled,
            config.tools.bash_timeout_secs,
            config.network.clone(),
            config.tools.respect_ignore,
            &config.protected_tool_environment_names(),
            config.sandbox.clone(),
            None,
        )
    }

    /// Same as [`Self::from_config_enabled`] with a live job grant injected
    /// into sandboxed tool children.
    pub fn from_config_enabled_with_job(
        workspace: impl Into<PathBuf>,
        config: &Config,
        enabled: Vec<String>,
        job: Option<&JobCredentialHandle>,
    ) -> Result<Self, ToolError> {
        Self::with_builtins_sandbox_protected_environment(
            workspace,
            enabled,
            config.tools.bash_timeout_secs,
            config.network.clone(),
            config.tools.respect_ignore,
            &config.protected_tool_environment_names(),
            config.sandbox.clone(),
            job,
        )
    }

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
        Self::with_builtins_sandbox_protected_environment(
            workspace,
            enabled,
            bash_timeout_secs,
            network,
            respect_ignore,
            protected_environment_names,
            SandboxSettings::default(),
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn with_builtins_sandbox_protected_environment(
        workspace: impl Into<PathBuf>,
        enabled: Vec<String>,
        bash_timeout_secs: u64,
        network: NetworkSettings,
        respect_ignore: bool,
        protected_environment_names: &[String],
        sandbox_settings: SandboxSettings,
        job: Option<&JobCredentialHandle>,
    ) -> Result<Self, ToolError> {
        let web_fetcher = default_web_fetcher()?;
        Ok(Self {
            executor: ToolExecutor::new(
                workspace,
                enabled,
                bash_timeout_secs,
                respect_ignore,
                protected_environment_names,
                sandbox_settings,
                job,
            )?,
            external: Vec::new(),
            todos: Arc::new(Mutex::new(Vec::new())),
            network,
            web_fetcher,
            bg_jobs: Arc::new(Mutex::new(BackgroundJobs::new())),
            skills: None,
        })
    }

    /// Move this run's background jobs out, leaving an empty table. A session
    /// host parks them in the run registry so they outlive the park.
    pub(crate) fn take_background_jobs(&self) -> Option<BackgroundJobs> {
        let mut guard = self.bg_jobs.lock().ok()?;
        Some(std::mem::replace(&mut *guard, BackgroundJobs::new()))
    }

    /// Adopt jobs parked by an earlier attempt of this run.
    pub(crate) fn adopt_background_jobs(&self, jobs: BackgroundJobs) {
        if let Ok(mut guard) = self.bg_jobs.lock() {
            *guard = jobs;
        }
    }

    /// Kill all background bash jobs (run end cleanup).
    /// Run an operator command in the workspace under the tool sandbox.
    /// See [`ToolExecutor::run_sandboxed`].
    pub(crate) async fn run_sandboxed(
        &self,
        program: &std::path::Path,
        args: &[String],
        envs: &[(&str, &str)],
        stdin: &[u8],
        limit: std::time::Duration,
    ) -> Result<std::process::Output, String> {
        self.executor
            .run_sandboxed(program, args, envs, stdin, limit)
            .await
    }

    pub async fn kill_background_jobs(&self) {
        let jobs: Vec<BgJob> = {
            let mut guard = match self.bg_jobs.lock() {
                Ok(g) => g,
                Err(_) => return,
            };
            guard.jobs.drain().map(|(_id, job)| job).collect()
        };
        for mut job in jobs {
            job.kill();
            let _ = job.child.wait().await;
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

    /// Offer `load_skill` for the packs in `settings.skills`. Call when the
    /// run prompt carries a lazy skill catalog.
    pub fn enable_skill_loading(&mut self, settings: ContextSettings) {
        self.skills = Some(SkillLoader {
            settings,
            loaded: Mutex::default(),
        });
    }

    /// Drain attribution lines for packs loaded since the last call. Each
    /// pack yields one line, at its first load in this run.
    pub fn take_skill_loads(&self) -> Vec<String> {
        self.skills
            .as_ref()
            .and_then(|skills| skills.loaded.lock().ok())
            .map(|mut loaded| std::mem::take(&mut loaded.pending))
            .unwrap_or_default()
    }

    fn load_skill(&self, args_json: &str) -> Result<String, ToolError> {
        #[derive(Deserialize)]
        struct LoadSkillArgs {
            id: String,
        }
        let Some(skills) = &self.skills else {
            return Err(ToolError::Disabled("load_skill".into()));
        };
        let args: LoadSkillArgs = parse("load_skill", args_json)?;
        let pack =
            crate::context::load_listed_skill(&self.executor.workspace, &skills.settings, &args.id)
                .ok_or_else(|| ToolError::Message(format!("load_skill: no skill `{}`", args.id)))?;
        let mut loaded = skills
            .loaded
            .lock()
            .map_err(|_| ToolError::Message("skill loads lock poisoned".into()))?;
        if loaded.seen.insert(pack.id.clone()) {
            loaded
                .pending
                .push(format!("skill {} digest={}", pack.id, pack.digest));
        }
        Ok(format!("# Skill `{}`\n\n{}", pack.id, pack.body))
    }

    /// Model-facing tool definitions for enabled builtins + external tools.
    pub fn definitions(&self) -> Vec<ToolDef> {
        let mut defs = model_visible_builtin_definitions(&self.executor.enabled);
        if self.skills.is_some() {
            defs.push(load_skill_definition());
        }
        for t in &self.external {
            defs.push(t.definition());
        }
        defs
    }

    pub fn enabled(&self) -> &[String] {
        &self.executor.enabled
    }

    pub fn is_enabled(&self, name: &str) -> bool {
        builtin_is_authorized(&self.executor.enabled, name)
            || (name == "load_skill" && self.skills.is_some())
            || self.external.iter().any(|t| t.definition().name == name)
    }

    pub async fn execute(&self, name: &str, args_json: &str) -> Result<ToolOutput, ToolError> {
        self.execute_for_run(name, args_json)
            .await
            .map(|executed| executed.output)
    }

    pub(crate) async fn execute_for_run(
        &self,
        name: &str,
        args_json: &str,
    ) -> Result<ExecutedTool, ToolError> {
        if let Some(t) = self.external.iter().find(|t| t.definition().name == name) {
            return Ok(ExecutedTool {
                output: ToolOutput::Text(t.call(args_json).await?),
                edit_outcome: None,
            });
        }
        if name == "load_skill" {
            return Ok(ExecutedTool {
                output: ToolOutput::Text(self.load_skill(args_json)?),
                edit_outcome: None,
            });
        }
        if !builtin_is_authorized(&self.executor.enabled, name) {
            return Err(ToolError::Disabled(name.into()));
        }
        if name == "todo_write" {
            let items = apply_todo_write(args_json)?;
            {
                let mut guard = self
                    .todos
                    .lock()
                    .map_err(|_| ToolError::Message("todo list lock poisoned".into()))?;
                *guard = items.clone();
            }
            let summary = format_todo_summary(&items);
            return Ok(ExecutedTool {
                output: ToolOutput::Text(summary),
                edit_outcome: None,
            });
        }
        if name == "handoff" {
            let brief = apply_handoff(args_json)?;
            return Ok(ExecutedTool {
                output: ToolOutput::Text(format_handoff_summary(&brief)),
                edit_outcome: None,
            });
        }
        if name == "web_fetch" {
            let text = self.web_fetch(args_json).await?;
            return Ok(ExecutedTool {
                output: ToolOutput::Text(text),
                edit_outcome: None,
            });
        }
        if BASH_HELPER_TOOLS.contains(&name) {
            let text = match name {
                "bash_background" => self.bash_background(args_json).await?,
                "bash_job_status" => self.bash_job_status(args_json).await?,
                "bash_job_logs" => self.bash_job_logs(args_json)?,
                _ => unreachable!("BASH_HELPER_TOOLS names are exhaustive"),
            };
            return Ok(ExecutedTool {
                output: ToolOutput::Text(text),
                edit_outcome: None,
            });
        }
        self.executor.execute(name, args_json).await
    }

    async fn bash_background(&self, args_json: &str) -> Result<String, ToolError> {
        let args: BashArgs = parse("bash_background", args_json)?;
        let mut guard = self
            .bg_jobs
            .lock()
            .map_err(|_| ToolError::Message("bg jobs lock poisoned".into()))?;
        if guard.running() >= MAX_BG_JOBS {
            return Err(ToolError::Message(format!(
                "bash_background: at most {MAX_BG_JOBS} concurrent jobs"
            )));
        }
        guard.forget_oldest_exited();
        let job_id = format!("job-{}", guard.next_id);
        guard.next_id += 1;
        let log_dir = self.executor.workspace.join(".shikigami/jobs");
        std::fs::create_dir_all(&log_dir)?;
        let log_path = log_dir.join(format!("{job_id}.log"));
        let log_file = std::fs::File::create(&log_path)?;
        let stdout = Stdio::from(log_file.try_clone()?);
        let stderr = Stdio::from(log_file);
        let (mut command, applied) = self.executor.bash_command(&args.command)?;
        command.stdout(stdout).stderr(stderr);
        let child = command.spawn()?;
        let process_group = child
            .id()
            .filter(|_| self.executor.sandbox.starts_process_group());
        guard.jobs.insert(
            job_id.clone(),
            BgJob {
                child,
                log_path: log_path.clone(),
                _applied: applied,
                process_group,
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
            // Includes jobs lost to a host restart: they are never restarted.
            return Ok(format!("job_id={} status=unknown", args.job_id));
        };
        match job.try_wait() {
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

impl Drop for ToolRegistry {
    fn drop(&mut self) {
        // Fence-loss / cancel can drop the harness future before the async
        // reaper runs. Dropping a job kills its process group, so bash
        // descendants cannot outlive the claim.
        if let Ok(mut guard) = self.bg_jobs.lock() {
            guard.jobs.clear();
        }
    }
}
