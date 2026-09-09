//! Process-kill matrix for governed tool recovery (#248).
//!
//! The parent owns termination and an independent effect ledger. The child
//! drives `Engine` (the same run engine `Harness` uses) with scripted model
//! turns, local durability, and a durable bash fixture. Barriers are FIFO
//! rendezvous points, not timing sleeps.

use std::io::Write;

fn main() {
    #[cfg(unix)]
    if let Err(error) = unix::main() {
        let _ = writeln!(std::io::stderr(), "FAILED: {error}");
        std::process::exit(1);
    }
    #[cfg(not(unix))]
    eprintln!("skip: governed tool crash recovery requires Unix FIFOs and process groups");
}

#[cfg(unix)]
mod unix {
    use std::fs::{self, File};
    use std::io::Read;
    use std::os::unix::process::CommandExt;
    use std::path::{Path, PathBuf};
    use std::process::{Command, Stdio};
    use std::sync::Arc;
    use std::sync::mpsc;
    use std::time::Duration;

    use async_trait::async_trait;
    use shikigami::checkpoint::{
        Checkpoint, StagedToolExecution, StagedToolReport, ToolExecutionStatus,
    };
    use shikigami::config::Config;
    use shikigami::content::ContentModelTurnV1;
    use shikigami::governance::{
        ContentTurnContext, GovernanceError, GovernancePort, LocalGovernance, RunHandle, RunOutcome,
    };
    use shikigami::model::{ChatMessage, ModelTurn};
    use shikigami::registry::RunRegistry;
    use shikigami::run::{Engine, RunRequest};
    use shikigami::state::StateRoot;
    use shikigami::tools::ToolDef;
    use shikigami::{events, model, workspace};

    const HOST_FLAG: &str = "--host";
    const DEADLINE: Duration = Duration::from_secs(20);
    const EFFECT_TOOL_ID: &str = "effect-1";

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Barrier {
        BeforeDispatch,
        AfterEffect,
        BeforeReport,
        BeforeComplete,
    }

    impl Barrier {
        fn as_str(self) -> &'static str {
            match self {
                Self::BeforeDispatch => "before_dispatch",
                Self::AfterEffect => "after_effect",
                Self::BeforeReport => "before_report",
                Self::BeforeComplete => "before_complete",
            }
        }

        fn parse(value: &str) -> Option<Self> {
            match value {
                "before_dispatch" => Some(Self::BeforeDispatch),
                "after_effect" => Some(Self::AfterEffect),
                "before_report" => Some(Self::BeforeReport),
                "before_complete" => Some(Self::BeforeComplete),
                _ => None,
            }
        }
    }

    pub(super) fn main() -> Result<(), String> {
        let mut args = std::env::args();
        let _ = args.next();
        match args.next().as_deref() {
            Some(HOST_FLAG) => {
                let runtime = tokio::runtime::Runtime::new().map_err(|error| error.to_string())?;
                runtime.block_on(host_main())
            }
            _ => parent_main(),
        }
    }

    fn parent_main() -> Result<(), String> {
        run_case("no_interrupt", None, ResumeExpect::Complete { effects: 1 })?;
        run_case(
            "before_dispatch",
            Some(Barrier::BeforeDispatch),
            ResumeExpect::Complete { effects: 1 },
        )?;
        run_case(
            "after_effect",
            Some(Barrier::AfterEffect),
            ResumeExpect::InDoubt { effects: 1 },
        )?;
        run_case(
            "before_report",
            Some(Barrier::BeforeReport),
            ResumeExpect::Complete { effects: 1 },
        )?;
        run_case(
            "before_complete",
            Some(Barrier::BeforeComplete),
            ResumeExpect::Complete { effects: 1 },
        )?;
        rerun_completed_resume()?;
        changed_script_does_not_repeat_in_doubt_effect()?;
        eprintln!("ok: governed tool crash recovery matrix");
        Ok(())
    }

    #[derive(Clone, Copy)]
    enum ResumeExpect {
        Complete { effects: usize },
        InDoubt { effects: usize },
    }

    fn run_case(name: &str, barrier: Option<Barrier>, expect: ResumeExpect) -> Result<(), String> {
        eprint!("test {name} ... ");
        let control = tempfile::tempdir().map_err(io)?;
        let control_path = control.path().to_path_buf();
        prepare_control(&control_path)?;
        spawn_and_interrupt(&control_path, barrier)?;
        let run_id = find_run_id(&control_path)?;
        let checkpoint = load_checkpoint(&control_path, &run_id)?;
        match barrier {
            Some(Barrier::BeforeDispatch) => assert_authorizing_or_absent(&checkpoint)?,
            Some(Barrier::AfterEffect) => assert_started(&checkpoint)?,
            Some(Barrier::BeforeReport) => assert_pending_report(&checkpoint)?,
            Some(Barrier::BeforeComplete) | None => {}
        }
        release_dead_owner(&control_path, &run_id)?;
        fs::write(control_path.join("proceed"), b"1\n").map_err(io)?;
        let resume = run_host(&control_path, None, Some(&run_id))?;
        let effects = effect_count(&control_path)?;
        match expect {
            ResumeExpect::Complete { effects: expected } => {
                if !resume.success {
                    return Err(format!(
                        "{name}: expected successful resume, got {}",
                        resume.detail
                    ));
                }
                if effects != expected {
                    return Err(format!(
                        "{name}: effect count {effects}, expected {expected}"
                    ));
                }
                assert_stable_tool_identity(&control_path, &run_id)?;
            }
            ResumeExpect::InDoubt { effects: expected } => {
                if resume.success {
                    return Err(format!("{name}: expected in-doubt refusal, run completed"));
                }
                if !resume.detail.contains("in-doubt") {
                    return Err(format!(
                        "{name}: expected in-doubt error, got {}",
                        resume.detail
                    ));
                }
                if effects != expected {
                    return Err(format!(
                        "{name}: effect count {effects}, expected {expected}"
                    ));
                }
            }
        }
        eprintln!("ok");
        Ok(())
    }

    fn rerun_completed_resume() -> Result<(), String> {
        eprint!("test repeated_resume ... ");
        let control = tempfile::tempdir().map_err(io)?;
        let control_path = control.path().to_path_buf();
        prepare_control(&control_path)?;
        spawn_and_interrupt(&control_path, Some(Barrier::BeforeComplete))?;
        let run_id = find_run_id(&control_path)?;
        release_dead_owner(&control_path, &run_id)?;
        fs::write(control_path.join("proceed"), b"1\n").map_err(io)?;
        let first = run_host(&control_path, None, Some(&run_id))?;
        if !first.success {
            return Err(format!("first resume failed: {}", first.detail));
        }
        release_dead_owner(&control_path, &run_id)?;
        let second = run_host(&control_path, None, Some(&run_id))?;
        if !second.success {
            return Err(format!("second resume failed: {}", second.detail));
        }
        if effect_count(&control_path)? != 1 {
            return Err("repeated resume duplicated the fixture effect".into());
        }
        if first.run_id != second.run_id {
            return Err("repeated resume changed run identity".into());
        }
        eprintln!("ok");
        Ok(())
    }

    fn changed_script_does_not_repeat_in_doubt_effect() -> Result<(), String> {
        eprint!("test changed_script_in_doubt ... ");
        let control = tempfile::tempdir().map_err(io)?;
        let control_path = control.path().to_path_buf();
        prepare_control(&control_path)?;
        spawn_and_interrupt(&control_path, Some(Barrier::AfterEffect))?;
        let run_id = find_run_id(&control_path)?;
        release_dead_owner(&control_path, &run_id)?;
        fs::write(control_path.join("proceed"), b"1\n").map_err(io)?;
        fs::write(control_path.join("mutate_script"), b"1\n").map_err(io)?;
        let resume = run_host(&control_path, None, Some(&run_id))?;
        if resume.success || !resume.detail.contains("in-doubt") {
            return Err(format!(
                "changed script should remain in-doubt, got {}",
                resume.detail
            ));
        }
        if effect_count(&control_path)? != 1 {
            return Err("changed script re-dispatched an in-doubt effect".into());
        }
        eprintln!("ok");
        Ok(())
    }

    fn prepare_control(control: &Path) -> Result<(), String> {
        fs::create_dir_all(control.join("state")).map_err(io)?;
        fs::create_dir_all(control.join("ws")).map_err(io)?;
        unix_fifo(control.join("barrier"))?;
        Ok(())
    }

    fn spawn_and_interrupt(control: &Path, barrier: Option<Barrier>) -> Result<(), String> {
        match barrier {
            None => {
                let result = run_host(control, None, None)?;
                if !result.success {
                    return Err(format!("uninterrupted run failed: {}", result.detail));
                }
                Ok(())
            }
            Some(barrier) => {
                let mut child = host_command(control, Some(barrier), None)
                    .spawn()
                    .map_err(io)?;
                let _hold = wait_for_barrier(control)?;
                let pid = child.id();
                // SAFETY: `pid` is the child we spawned into its own process
                // group; negative pid sends SIGKILL to that group only.
                unsafe {
                    libc::kill(-(pid as i32), libc::SIGKILL);
                }
                let _ = child.wait();
                if effect_count(control)? > 1 {
                    return Err("fixture applied more than one effect before interrupt".into());
                }
                Ok(())
            }
        }
    }

    fn host_command(control: &Path, barrier: Option<Barrier>, resume: Option<&str>) -> Command {
        let exe = std::env::current_exe().expect("current test executable");
        let mut command = Command::new(exe);
        command
            .arg(HOST_FLAG)
            .env("SHIKIGAMI_CRASH_CONTROL", control)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .process_group(0);
        match barrier {
            Some(barrier) => {
                command.env("SHIKIGAMI_CRASH_BARRIER", barrier.as_str());
            }
            None => {
                command.env_remove("SHIKIGAMI_CRASH_BARRIER");
            }
        }
        if let Some(run_id) = resume {
            command.env("SHIKIGAMI_CRASH_RESUME", run_id);
        }
        command
    }

    fn run_host(
        control: &Path,
        barrier: Option<Barrier>,
        resume: Option<&str>,
    ) -> Result<HostResult, String> {
        let output = host_command(control, barrier, resume)
            .output()
            .map_err(io)?;
        let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
        if let Ok(result) = serde_json::from_str::<HostResult>(&stdout) {
            return Ok(result);
        }
        Ok(HostResult {
            success: output.status.success(),
            run_id: String::new(),
            detail: format!("status={} stdout={stdout} stderr={stderr}", output.status),
        })
    }

    fn wait_for_barrier(control: &Path) -> Result<File, String> {
        let fifo = control.join("barrier");
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let result = fs::OpenOptions::new().read(true).open(&fifo);
            let _ = tx.send(result);
        });
        match rx.recv_timeout(DEADLINE) {
            Ok(Ok(file)) => Ok(file),
            Ok(Err(error)) => Err(format!("crash barrier open failed: {error}")),
            Err(_) => Err("timed out waiting for crash barrier".into()),
        }
    }

    fn find_run_id(control: &Path) -> Result<String, String> {
        let runs = control.join("state").join("runs");
        let entries = fs::read_dir(&runs).map_err(io)?;
        for entry in entries.filter_map(Result::ok) {
            if entry.path().join("checkpoint.json").is_file() {
                return Ok(entry.file_name().to_string_lossy().into_owned());
            }
        }
        Err("checkpoint was not retained after interrupt".into())
    }

    fn load_checkpoint(control: &Path, run_id: &str) -> Result<Checkpoint, String> {
        Checkpoint::load(&control.join("state").join("runs"), run_id)
            .map_err(|error| error.to_string())
    }

    fn release_dead_owner(control: &Path, run_id: &str) -> Result<(), String> {
        let run_dir = control.join("state").join("runs").join(run_id);
        let _ = fs::remove_file(run_dir.join("owner"));
        let path = run_dir.join("run.json");
        let mut record: serde_json::Value = serde_json::from_slice(&fs::read(&path).map_err(io)?)
            .map_err(|error| error.to_string())?;
        record["last_heartbeat_at_ms"] = serde_json::json!(0);
        fs::write(
            &path,
            serde_json::to_vec_pretty(&record).map_err(|error| error.to_string())?,
        )
        .map_err(io)
    }

    fn effect_count(control: &Path) -> Result<usize, String> {
        match fs::read_to_string(control.join("effects.log")) {
            Ok(text) => Ok(text.lines().filter(|line| *line == "effect").count()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
            Err(error) => Err(error.to_string()),
        }
    }

    fn assert_authorizing_or_absent(checkpoint: &Checkpoint) -> Result<(), String> {
        let executions = checkpoint
            .governance
            .as_ref()
            .map(|governance| governance.pending_tool_executions.as_slice())
            .unwrap_or(&[]);
        if executions
            .iter()
            .any(|execution| execution.status != ToolExecutionStatus::Authorizing)
        {
            return Err(format!(
                "before-dispatch checkpoint should be authorizing or empty, got {executions:?}"
            ));
        }
        Ok(())
    }

    fn assert_started(checkpoint: &Checkpoint) -> Result<(), String> {
        let Some(execution) = checkpoint
            .governance
            .as_ref()
            .and_then(|governance| governance.pending_tool_executions.first())
        else {
            return Err("after-effect checkpoint is missing a started execution marker".into());
        };
        if execution.status != ToolExecutionStatus::Started {
            return Err(format!(
                "after-effect status {:?} should be started",
                execution.status
            ));
        }
        Ok(())
    }

    fn assert_pending_report(checkpoint: &Checkpoint) -> Result<(), String> {
        let reports = checkpoint
            .governance
            .as_ref()
            .map(|governance| governance.pending_tool_reports.as_slice())
            .unwrap_or(&[]);
        if reports.is_empty() {
            return Err("before-report checkpoint is missing staged tool reports".into());
        }
        if checkpoint
            .governance
            .as_ref()
            .is_some_and(|governance| !governance.pending_tool_executions.is_empty())
        {
            return Err("before-report checkpoint still has execution markers".into());
        }
        Ok(())
    }

    fn assert_stable_tool_identity(control: &Path, run_id: &str) -> Result<(), String> {
        let checkpoint = load_checkpoint(control, run_id)?;
        let call_ids: Vec<_> = checkpoint
            .messages
            .iter()
            .flat_map(|message| message.tool_calls.iter().map(|call| call.id.clone()))
            .filter(|id| !id.is_empty())
            .collect();
        if !call_ids.iter().any(|id| id == EFFECT_TOOL_ID) {
            return Err(format!(
                "stable effect tool id `{EFFECT_TOOL_ID}` missing from {call_ids:?}"
            ));
        }
        Ok(())
    }

    fn unix_fifo(path: PathBuf) -> Result<(), String> {
        let c_path = std::ffi::CString::new(path.to_string_lossy().as_bytes())
            .map_err(|error| error.to_string())?;
        // SAFETY: `c_path` is a NUL-terminated path we just built from a
        // filesystem PathBuf; mkfifo creates a new FIFO and does not follow it.
        let rc = unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) };
        if rc == 0 {
            Ok(())
        } else {
            Err(format!(
                "mkfifo {}: {}",
                path.display(),
                std::io::Error::last_os_error()
            ))
        }
    }

    fn io(error: std::io::Error) -> String {
        error.to_string()
    }

    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    struct HostResult {
        success: bool,
        run_id: String,
        detail: String,
    }

    async fn host_main() -> Result<(), String> {
        let control = PathBuf::from(
            std::env::var("SHIKIGAMI_CRASH_CONTROL")
                .map_err(|_| "SHIKIGAMI_CRASH_CONTROL missing")?,
        );
        let barrier = std::env::var("SHIKIGAMI_CRASH_BARRIER")
            .ok()
            .as_deref()
            .and_then(Barrier::parse);
        let resume = std::env::var("SHIKIGAMI_CRASH_RESUME").ok();
        let mutate_script = control.join("mutate_script").is_file();
        let mut config = Config::default();
        config.governance.adapter = "local".into();
        config.model.adapter = "scripted".into();
        config.events.adapter = "jsonl".into();
        config.workspace.root = control.join("ws").to_string_lossy().into();
        config.tools.enabled = vec!["bash".into(), "report".into()];
        config.tools.bash_timeout_secs = 60;
        config.model.script_json = Some(script_json(&control, barrier, mutate_script));
        let state = StateRoot::new(control.join("state"));
        state
            .ensure_ready_for_runs()
            .map_err(|error| error.to_string())?;
        let inner = LocalGovernance::from_config(&config);
        let governance: Arc<dyn GovernancePort> = Arc::new(CrashHostGovernance {
            inner,
            control: control.clone(),
            barrier,
        });
        let engine = Engine::new(
            config.clone(),
            governance,
            Arc::from(workspace::from_config(&config).map_err(|error| error.to_string())?),
            Arc::from(model::from_config(&config).map_err(|error| error.to_string())?),
            Arc::from(
                events::from_config(&config, &state.runs_dir())
                    .map_err(|error| error.to_string())?,
            ),
            state.runs_dir(),
            Arc::new(RunRegistry::new(state.path()).map_err(|error| error.to_string())?),
        );
        let mut request = RunRequest::new("prove governed tool crash recovery");
        request.keep_workspace = true;
        request.resume_run_id = resume;
        match engine.run(request).await {
            Ok(result) => emit_result(HostResult {
                success: result.success,
                run_id: result.run_id,
                detail: result.summary,
            }),
            Err(error) => emit_result(HostResult {
                success: false,
                run_id: String::new(),
                detail: error.to_string(),
            }),
        }
    }

    fn emit_result(result: HostResult) -> Result<(), String> {
        println!(
            "{}",
            serde_json::to_string(&result).map_err(|error| error.to_string())?
        );
        Ok(())
    }

    fn script_json(control: &Path, barrier: Option<Barrier>, mutate: bool) -> String {
        let wait = if barrier == Some(Barrier::AfterEffect) {
            r#"exec 3>"$CONTROL/barrier"
exec 4<"$CONTROL/barrier"
read -r _ <&4
"#
        } else {
            ""
        };
        let extra = if mutate {
            "printf 'mutated\\n' >> \"$CONTROL/effects.log\"\n"
        } else {
            ""
        };
        let command = format!(
            r#"CONTROL={control}
printf 'effect\n' >> "$CONTROL/effects.log"
{extra}{wait}"#,
            control = sh_single_quote(&control.display().to_string()),
            extra = extra,
            wait = wait
        );
        let args = serde_json::json!({
            "command": command,
            "timeout_ms": 60_000
        });
        serde_json::json!([
            {"tool_calls":[{"id": EFFECT_TOOL_ID, "name":"bash","args_json": args.to_string()}]},
            {"tool_calls":[{"id":"report-1","name":"report","args_json":"{\"summary\":\"effected\",\"success\":true}"}]}
        ])
        .to_string()
    }

    fn sh_single_quote(value: &str) -> String {
        format!("'{}'", value.replace('\'', r#"'"'"'"#))
    }

    struct CrashHostGovernance {
        inner: LocalGovernance,
        control: PathBuf,
        barrier: Option<Barrier>,
    }

    impl CrashHostGovernance {
        async fn wait_if(&self, needed: Barrier) {
            if self.barrier != Some(needed) || self.control.join("proceed").is_file() {
                return;
            }
            let fifo = self.control.join("barrier");
            let _ = tokio::task::spawn_blocking(move || wait_on_fifo(&fifo)).await;
        }
    }

    fn wait_on_fifo(fifo: &Path) -> std::io::Result<()> {
        let _writer = fs::OpenOptions::new().write(true).open(fifo)?;
        let mut reader = fs::OpenOptions::new().read(true).open(fifo)?;
        let mut buf = [0u8; 1];
        let _ = reader.read(&mut buf);
        Ok(())
    }

    #[async_trait]
    impl GovernancePort for CrashHostGovernance {
        fn id(&self) -> &'static str {
            self.inner.id()
        }

        fn health_detail(&self) -> String {
            self.inner.health_detail()
        }

        fn health_ok(&self) -> bool {
            self.inner.health_ok()
        }

        async fn begin_run(
            &self,
            run_id: &str,
            task: &str,
            logical_operation_id: Option<&str>,
        ) -> Result<RunHandle, GovernanceError> {
            self.inner
                .begin_run(run_id, task, logical_operation_id)
                .await
        }

        async fn begin_run_with_checkpoint(
            &self,
            run_id: &str,
            task: &str,
            logical_operation_id: Option<&str>,
            checkpoint: Option<&shikigami::checkpoint::GovernanceCheckpoint>,
        ) -> Result<RunHandle, GovernanceError> {
            self.inner
                .begin_run_with_checkpoint(run_id, task, logical_operation_id, checkpoint)
                .await
        }

        fn checkpoint_state(
            &self,
            run_id: &str,
        ) -> Option<shikigami::checkpoint::GovernanceCheckpoint> {
            self.inner.checkpoint_state(run_id)
        }

        async fn stage_tool_execution(
            &self,
            handle: &RunHandle,
            execution: StagedToolExecution,
        ) -> Result<(), GovernanceError> {
            self.inner.stage_tool_execution(handle, execution).await
        }

        async fn mark_tool_execution_started(
            &self,
            handle: &RunHandle,
            call_id: &str,
        ) -> Result<(), GovernanceError> {
            self.inner
                .mark_tool_execution_started(handle, call_id)
                .await
        }

        async fn mark_tool_execution_complete(
            &self,
            handle: &RunHandle,
            call_id: &str,
        ) -> Result<(), GovernanceError> {
            self.inner
                .mark_tool_execution_complete(handle, call_id)
                .await
        }

        async fn clear_staged_tool_executions(
            &self,
            handle: &RunHandle,
        ) -> Result<(), GovernanceError> {
            self.inner.clear_staged_tool_executions(handle).await
        }

        async fn recover_staged_tool_executions(
            &self,
            handle: &RunHandle,
        ) -> Result<(), GovernanceError> {
            self.inner.recover_staged_tool_executions(handle).await
        }

        async fn stage_tool_reports(
            &self,
            handle: &RunHandle,
            reports: Vec<StagedToolReport>,
        ) -> Result<(), GovernanceError> {
            self.inner.stage_tool_reports(handle, reports).await
        }

        async fn replay_staged_tool_reports(
            &self,
            handle: &RunHandle,
        ) -> Result<(), GovernanceError> {
            self.inner.replay_staged_tool_reports(handle).await
        }

        async fn plan_turn(
            &self,
            handle: &RunHandle,
            system: &str,
            messages: &[ChatMessage],
            tools: &[ToolDef],
            local_model: &dyn shikigami::model::ModelPort,
        ) -> Result<ModelTurn, GovernanceError> {
            self.inner
                .plan_turn(handle, system, messages, tools, local_model)
                .await
        }

        async fn plan_content_turn(
            &self,
            handle: &RunHandle,
            system: &str,
            context: ContentTurnContext<'_>,
        ) -> Result<ContentModelTurnV1, GovernanceError> {
            self.inner.plan_content_turn(handle, system, context).await
        }

        async fn authorize_tool(
            &self,
            handle: &RunHandle,
            name: &str,
            args_json: &str,
        ) -> Result<(), GovernanceError> {
            self.inner.authorize_tool(handle, name, args_json).await
        }

        async fn authorize_tool_with_id(
            &self,
            handle: &RunHandle,
            call_id: &str,
            name: &str,
            args_json: &str,
        ) -> Result<(), GovernanceError> {
            if name != "report" && name != "escalate" {
                self.wait_if(Barrier::BeforeDispatch).await;
            }
            self.inner
                .authorize_tool_with_id(handle, call_id, name, args_json)
                .await
        }

        async fn report_tool(
            &self,
            handle: &RunHandle,
            name: &str,
            ok: bool,
            detail: &str,
        ) -> Result<(), GovernanceError> {
            self.inner.report_tool(handle, name, ok, detail).await
        }

        async fn report_tool_with_id(
            &self,
            handle: &RunHandle,
            call_id: &str,
            name: &str,
            ok: bool,
            detail: &str,
        ) -> Result<(), GovernanceError> {
            if name != "report" && name != "escalate" {
                self.wait_if(Barrier::BeforeReport).await;
            }
            self.inner
                .report_tool_with_id(handle, call_id, name, ok, detail)
                .await
        }

        async fn complete_run(
            &self,
            handle: &RunHandle,
            outcome: RunOutcome,
        ) -> Result<(), GovernanceError> {
            self.wait_if(Barrier::BeforeComplete).await;
            self.inner.complete_run(handle, outcome).await
        }
    }
}
