//! Nested child runs: a child is a first-class [`super::Engine`] Run.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use serde::Deserialize;
use serde_json::json;
use uuid::Uuid;

use crate::checkpoint::Checkpoint;
use crate::model::ToolCall;
use crate::tools::{ToolOutput, ToolRegistry};
use crate::workspace;

use super::session::RunSession;
use super::supervision::check_bounds;
use super::{ChildProfile, Engine, RunError, RunRequest, RunResult};

#[derive(Debug, Deserialize)]
struct ChildRunArgs {
    profile: String,
    task: String,
    #[serde(default = "default_wait")]
    wait: bool,
    #[serde(default)]
    worktree: bool,
}

fn default_wait() -> bool {
    true
}

/// Process-wide runtime for `wait=false` children. `Engine` + `RunRequest`
/// are Send and move onto a blocking worker; the `!Send` `Engine::run`
/// future is built there and driven with `Handle::block_on`. Blocking
/// materialize (including `git worktree add`) therefore cannot stall other
/// children or spawn acknowledgement.
static BACKGROUND_RUNTIME_BUILDS: AtomicU32 = AtomicU32::new(0);
static BACKGROUND_SPAWNS: AtomicU32 = AtomicU32::new(0);

#[cfg(test)]
pub(super) fn background_runtime_builds() -> u32 {
    BACKGROUND_RUNTIME_BUILDS.load(Ordering::SeqCst)
}

#[cfg(test)]
pub(super) fn background_spawns() -> u32 {
    BACKGROUND_SPAWNS.load(Ordering::SeqCst)
}

fn background_runtime() -> Result<&'static tokio::runtime::Runtime, String> {
    static CELL: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    if let Some(runtime) = CELL.get() {
        return Ok(runtime);
    }
    static INIT: Mutex<()> = Mutex::new(());
    let _guard = INIT.lock().unwrap_or_else(|poison| poison.into_inner());
    if let Some(runtime) = CELL.get() {
        return Ok(runtime);
    }
    // Shared multi-thread runtime: `Engine::run` stays on blocking workers
    // so the future can remain `!Send`. JoinSet/heartbeat tasks share these
    // workers instead of one current-thread runtime per child.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("shikigami-nested")
        .build()
        .map_err(|error| format!("runtime: {error}"))?;
    BACKGROUND_RUNTIME_BUILDS.fetch_add(1, Ordering::SeqCst);
    let _ = CELL.set(runtime);
    CELL.get()
        .ok_or_else(|| "runtime: worker exited before start".into())
}

/// Dispatch one background child onto the shared runtime. Runtime-build
/// failure is reported before the caller records `running`, so the fan-out
/// slot can roll back. The JoinHandle is returned as soon as the blocking
/// job is queued, so parent cancel/timeout is observed in
/// [`wait_for_background_child_start`].
fn spawn_background_child(
    engine: Engine,
    request: RunRequest,
) -> Result<tokio::task::JoinHandle<()>, RunError> {
    BACKGROUND_SPAWNS.fetch_add(1, Ordering::SeqCst);
    let runtime = background_runtime()
        .map_err(|error| RunError::Message(format!("child_run background {error}")))?;
    let handle = runtime.handle().clone();
    Ok(runtime.spawn_blocking(move || {
        let _ = handle.block_on(engine.run(request));
    }))
}

#[derive(Debug, Deserialize)]
struct ChildStatusArgs {
    run_id: String,
}

pub(super) fn parse_profile(raw: &str) -> Result<ChildProfile, String> {
    match raw.trim() {
        "explore" => Ok(ChildProfile::Explore),
        "plan" => Ok(ChildProfile::Plan),
        "full" => Ok(ChildProfile::Full),
        other => Err(format!(
            "unknown child profile `{other}`; expected explore, plan, or full"
        )),
    }
}

/// Session hosts ask=park these before execution.
///
/// Uses [`parse_profile`] so whitespace around `full`/`plan` cannot skip
/// the park and still spawn a mutating child. `worktree=true` is mutating
/// for every profile (`git worktree add` against the parent checkout).
pub(super) fn session_ask_park(name: &str, args_json: &str) -> bool {
    if crate::tools::mutates_workspace(name) {
        return true;
    }
    if name != "child_run" {
        return false;
    }
    let Ok(args) = serde_json::from_str::<ChildRunArgs>(args_json) else {
        return false;
    };
    if args.worktree {
        return true;
    }
    matches!(
        parse_profile(&args.profile),
        Ok(ChildProfile::Full | ChildProfile::Plan)
    )
}

pub(super) fn nested_tools_enabled(
    engine: &Engine,
    request: &RunRequest,
    is_content: bool,
    nested_depth: u32,
    parent_run_id: &str,
    stored_nested: bool,
) -> bool {
    // Children never inherit nested tools (an explore child must not spawn a
    // full grandchild with host write/MCP). nested_max_depth therefore only
    // gates the root; values above 1 are reserved.
    if is_content
        || nested_depth > 0
        || !parent_run_id.is_empty()
        || request
            .parent_run_id
            .as_ref()
            .is_some_and(|id| !id.is_empty())
    {
        return false;
    }
    if !(engine.config.run.nested || request.nested || stored_nested) {
        return false;
    }
    nested_depth < engine.config.run.nested_max_depth.max(1)
}

pub(super) async fn execute_child_tool(
    engine: &Engine,
    session: &mut RunSession,
    request: &RunRequest,
    tools: &ToolRegistry,
    call: &ToolCall,
    started: tokio::time::Instant,
    timeout: Option<std::time::Duration>,
) -> Result<ToolOutput, RunError> {
    if !nested_tools_enabled(
        engine,
        request,
        session.is_content(),
        session.nested_depth,
        &session.parent_run_id,
        session.nested,
    ) {
        return Err(RunError::Message(format!(
            "tool not enabled: {}",
            call.name
        )));
    }
    match call.name.as_str() {
        "child_run" => {
            child_run(
                engine,
                session,
                request,
                tools,
                &call.args_json,
                started,
                timeout,
            )
            .await
        }
        "child_status" => child_status(engine, session, &call.args_json),
        _ => Err(RunError::Message(format!(
            "internal nested dispatch for `{}`",
            call.name
        ))),
    }
}

async fn child_run(
    engine: &Engine,
    session: &mut RunSession,
    request: &RunRequest,
    tools: &ToolRegistry,
    args_json: &str,
    started: tokio::time::Instant,
    timeout: Option<std::time::Duration>,
) -> Result<ToolOutput, RunError> {
    if session.is_content() {
        return Err(RunError::Message(
            "bounded content runs do not start nested children".into(),
        ));
    }
    let args: ChildRunArgs = serde_json::from_str(args_json)
        .map_err(|error| RunError::Message(format!("child_run arguments: {error}")))?;
    if args.task.trim().is_empty() {
        return Err(RunError::Message("child_run task must be non-empty".into()));
    }
    let profile = parse_profile(&args.profile).map_err(RunError::Message)?;
    if session.plan_jail && args.worktree {
        return Err(RunError::Message(
            "child_run worktree=true is denied while plan-jail is active".into(),
        ));
    }
    let max_depth = engine.config.run.nested_max_depth.max(1);
    let max_children = engine.config.run.nested_max_children.max(1);
    if session.nested_depth >= max_depth {
        return Err(RunError::Message(format!(
            "nested depth cap ({max_depth}) refuses another child_run"
        )));
    }
    if session.children.len() as u32 >= max_children {
        return Err(RunError::Message(format!(
            "nested fan-out cap ({max_children}) refuses another child_run"
        )));
    }

    let child_id = Uuid::new_v4().to_string();
    let child_request = child_request(session, request, &child_id, profile, &args);
    let child_engine = child_engine(engine, session, profile, args.worktree)?;
    session.children.push(crate::checkpoint::ChildRunRecord {
        run_id: child_id.clone(),
        profile: profile.as_str().into(),
        task: args.task.clone(),
    });
    if let Err(error) = session.save(tools) {
        session.children.pop();
        return Err(error);
    }
    if !args.wait {
        // Shared runtime + blocking worker so this recursive Engine::run
        // path does not have to be `Send` through tokio::spawn. Queueing
        // the job is enough to report toward `running`; a Builder failure
        // still rolls back the fan-out slot.
        let handle = match spawn_background_child(child_engine, child_request) {
            Ok(handle) => handle,
            Err(error) => {
                session.children.pop();
                let _ = session.save(tools);
                return Err(error);
            }
        };
        return wait_for_background_child_start(
            engine, session, request, tools, handle, &child_id, profile, started, timeout,
        )
        .await;
    }
    // Wait while still honoring parent cancel/timeout. A durable
    // `shikigami cancel <parent>` marker is not the child's run id.
    let mut child_fut = Box::pin(child_engine.run(child_request));
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(50));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let result = loop {
        tokio::select! {
            result = &mut child_fut => break result,
            _ = interval.tick() => {
                if let Err(error) = check_bounds(
                    engine,
                    &session.run_id,
                    request,
                    started,
                    timeout,
                    &session.parent_run_id,
                ) {
                    let _ = engine.registry.request_cancel(&child_id);
                    // Bound the drain so a stalled child cannot block parent
                    // cancel/timeout. If the child finishes, its transaction
                    // reaps. Otherwise reap an isolated worktree here.
                    tokio::select! {
                        _ = &mut child_fut => {}
                        _ = tokio::time::sleep(std::time::Duration::from_secs(2)) => {}
                    }
                    reap_nested_worktree(engine, &child_id);
                    return Err(error);
                }
            }
        }
    };
    match result {
        Ok(run_result) => Ok(ToolOutput::Text(summary_json(profile, &run_result))),
        Err(error) => {
            // Independent child cancel/timeout/start failure is a tool
            // result. Only parent `check_bounds` above aborts the parent.
            // Registry start can succeed before workspace materialize
            // (`git worktree add`); only a child checkpoint means the
            // fan-out slot was a real attempt.
            if Checkpoint::load(&engine.state_runs, &child_id).is_err() {
                session.children.retain(|child| child.run_id != child_id);
                let _ = session.save(tools);
            }
            Ok(ToolOutput::Text(
                json!({
                    "run_id": child_id,
                    "profile": profile.as_str(),
                    "termination": error.termination().as_str(),
                    "success": false,
                    "summary": error.to_string(),
                })
                .to_string(),
            ))
        }
    }
}

/// Wait until `registry.start` wrote a row, or the worker exited.
///
/// Returning `running` at runtime-build lets a pre-start cancel leave the
/// child recorded as `starting` with a burned fan-out slot.
#[allow(clippy::too_many_arguments)]
async fn wait_for_background_child_start(
    engine: &Engine,
    session: &mut RunSession,
    request: &RunRequest,
    tools: &ToolRegistry,
    handle: tokio::task::JoinHandle<()>,
    child_id: &str,
    profile: ChildProfile,
    started: tokio::time::Instant,
    timeout: Option<std::time::Duration>,
) -> Result<ToolOutput, RunError> {
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(20));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        interval.tick().await;
        if Checkpoint::load(&engine.state_runs, child_id).is_ok() {
            session.push_background_child(handle);
            return Ok(ToolOutput::Text(
                json!({
                    "run_id": child_id,
                    "profile": profile.as_str(),
                    "status": "running",
                })
                .to_string(),
            ));
        }
        if handle.is_finished() {
            let _ = handle.await;
            rollback_unstarted_child(session, tools, child_id);
            return Ok(ToolOutput::Text(
                json!({
                    "run_id": child_id,
                    "profile": profile.as_str(),
                    "termination": "cancelled",
                    "success": false,
                    "summary": "child_run cancelled before start",
                })
                .to_string(),
            ));
        }
        if let Err(error) = check_bounds(
            engine,
            &session.run_id,
            request,
            started,
            timeout,
            &session.parent_run_id,
        ) {
            let _ = engine.registry.request_cancel(child_id);
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
            while !handle.is_finished() && tokio::time::Instant::now() < deadline {
                interval.tick().await;
            }
            if handle.is_finished() {
                let _ = handle.await;
                rollback_unstarted_child(session, tools, child_id);
            } else {
                session.push_background_child(handle);
            }
            return Err(error);
        }
    }
}

fn reap_nested_worktree(engine: &Engine, child_id: &str) {
    let Ok(child) = Checkpoint::load(&engine.state_runs, child_id) else {
        return;
    };
    // Nested `worktree=true` spawn sets `request.keep_workspace=false`.
    // `save_recoverable` later forces the checkpoint keep bit for root
    // resume/inspection; that recovery flag must not skip this fallback
    // when the child's transaction is still stuck in `complete_run`.
    if child.workspace_adapter != "git-worktree"
        || child.parent_run_id.is_empty()
        || child.park.is_some()
    {
        return;
    }
    let cleanup = workspace::git_worktree_cleanup(
        &child.workspace,
        child_id,
        &engine.config.workspace.branch_prefix,
        std::path::Path::new(&engine.config.workspace.root),
    );
    let ws = workspace::MaterializedWorkspace {
        path: child.workspace,
        adapter: child.workspace_adapter,
        cleanup,
    };
    let _ = workspace::apply_cleanup(&ws);
}

fn rollback_unstarted_child(session: &mut RunSession, tools: &ToolRegistry, child_id: &str) {
    session.children.retain(|child| child.run_id != child_id);
    let _ = session.save(tools);
}

fn child_status(
    engine: &Engine,
    session: &RunSession,
    args_json: &str,
) -> Result<ToolOutput, RunError> {
    let args: ChildStatusArgs = serde_json::from_str(args_json)
        .map_err(|error| RunError::Message(format!("child_status arguments: {error}")))?;
    if !session
        .children
        .iter()
        .any(|child| child.run_id == args.run_id)
    {
        return Err(RunError::Message(format!(
            "child_status run_id `{}` is not a child of this run",
            args.run_id
        )));
    }
    let record = engine.registry.load(&args.run_id).ok();
    match Checkpoint::load(&engine.state_runs, &args.run_id) {
        Ok(checkpoint) => {
            let park = checkpoint.park.as_ref().map(|park| {
                json!({
                    "kind": park.kind.as_str(),
                    "reason": park.reason,
                    "question": park.question,
                })
            });
            let status = if checkpoint.park.is_some() {
                "parked".into()
            } else {
                record
                    .as_ref()
                    .map(|record| record.status.clone())
                    .unwrap_or_else(|| "running".into())
            };
            Ok(ToolOutput::Text(
                json!({
                    "run_id": checkpoint.run_id,
                    "status": status,
                    "success": record.as_ref().and_then(|record| record.success),
                    "summary": record.as_ref().map(|record| record.summary.clone()),
                    "termination": record.as_ref().and_then(|record| record.termination.clone()),
                    "turns": checkpoint.completed_turns,
                    "park": park,
                })
                .to_string(),
            ))
        }
        Err(_) => Ok(ToolOutput::Text(
            json!({
                "run_id": args.run_id,
                "status": record
                    .as_ref()
                    .map(|record| record.status.clone())
                    .unwrap_or_else(|| "starting".into()),
                "success": record.as_ref().and_then(|record| record.success),
                "summary": record.as_ref().map(|record| record.summary.clone()),
                "termination": record.as_ref().and_then(|record| record.termination.clone()),
            })
            .to_string(),
        )),
    }
}

fn child_request(
    session: &RunSession,
    parent: &RunRequest,
    child_id: &str,
    profile: ChildProfile,
    args: &ChildRunArgs,
) -> RunRequest {
    let mut request = RunRequest::new(args.task.clone());
    // Inplace children share the parent tree and must not delete it.
    // Isolated git-worktrees clean up on successful non-park completion.
    request.keep_workspace = !args.worktree;
    request.timeout = parent.timeout;
    request.cancel = parent.cancel.clone();
    request.assigned_run_id = Some(child_id.into());
    request.nested_depth = session.nested_depth.saturating_add(1);
    request.parent_run_id = Some(session.run_id.clone());
    request.nested_profile = Some(profile);
    request.plan_jail = match profile {
        ChildProfile::Plan => true,
        ChildProfile::Explore => false,
        ChildProfile::Full => session.plan_jail,
    };
    request.nested = false;
    // Children run unattended to a summary. ACP/TUI session_wait on the
    // parent would otherwise park the child at PromptWait/Ask with no
    // child_resume path.
    request.session_wait = false;
    // Harvest correlates parent and child through this field (ADR 0015).
    request.logical_operation_id = parent
        .logical_operation_id
        .clone()
        .or_else(|| Some(session.run_id.clone()));
    request
}

fn child_engine(
    parent: &Engine,
    session: &RunSession,
    profile: ChildProfile,
    worktree: bool,
) -> Result<Engine, RunError> {
    let config = child_config(
        &parent.config,
        &session.workspace,
        session.plan_jail,
        profile,
        worktree,
    )?;
    let workspace = workspace::from_config(&config).map_err(RunError::Workspace)?;
    // HTTP adapters reuse the parent client when adapter/url/key/model match.
    // Scripted adapters start an independent cursor.
    let model = parent.model.fresh_for_child(&config)?;
    let events = crate::events::from_config(&config, &parent.state_runs)
        .map_err(|error| RunError::Message(error.to_string()))?;
    Ok(Engine::new(
        config,
        Arc::clone(&parent.governance),
        Arc::from(workspace),
        Arc::from(model),
        Arc::from(events),
        parent.state_runs.clone(),
        Arc::clone(&parent.registry),
    ))
}

fn child_config(
    parent: &crate::config::Config,
    workspace: &std::path::Path,
    plan_jail: bool,
    profile: ChildProfile,
    worktree: bool,
) -> Result<crate::config::Config, RunError> {
    let mut config = parent.clone();
    match profile {
        ChildProfile::Explore => {
            config.tools.mode = crate::config::PermissionMode::Read;
            config.tools.enabled.clear();
            config.tools.mcp_servers.clear();
            config.run.plan_jail = false;
        }
        ChildProfile::Plan => {
            config.run.plan_jail = true;
            config.tools.mcp_servers.clear();
        }
        ChildProfile::Full => {
            config.run.plan_jail = plan_jail;
            if plan_jail {
                config.tools.mcp_servers.clear();
            }
        }
    }
    config.run.nested = false;
    if worktree {
        config.workspace.adapter = "git-worktree".into();
        config.workspace.root = workspace.display().to_string();
        config.workspace.snapshot = false;
        if !workspace.join(".git").exists() {
            return Err(RunError::Message(
                "child_run worktree=true requires a git checkout at the parent workspace".into(),
            ));
        }
    } else {
        // Share the parent's materialized tree so explore/plan/full see the
        // same files as an ACP/TUI inplace session. Isolation is worktree=true.
        config.workspace.adapter = "inplace".into();
        config.workspace.root = workspace.display().to_string();
        config.workspace.snapshot = false;
    }
    config.events.adapter = "none".into();
    Ok(config)
}

fn summary_json(profile: ChildProfile, result: &RunResult) -> String {
    let park = result.park.as_ref().map(|park| {
        json!({
            "kind": park.kind.as_str(),
            "reason": park.reason,
            "question": park.question,
        })
    });
    // Isolated `worktree=true` trees live outside the parent jail, so the
    // harvest is this summary. Parent `read_file`/`glob` cannot follow a
    // child workspace path even if we returned one.
    json!({
        "run_id": result.run_id,
        "profile": profile.as_str(),
        "termination": result.termination.as_str(),
        "success": result.success,
        "summary": result.summary,
        "turns": result.turns,
        "park": park,
    })
    .to_string()
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{ChildProfile, Engine, RunRequest, child_config, parse_profile, session_ask_park};
    use crate::config::{Config, McpServerSettings, PermissionMode};

    #[test]
    fn background_child_job_values_are_send() {
        fn assert_send<T: Send>() {}
        assert_send::<Engine>();
        assert_send::<RunRequest>();
    }

    fn parent_with_mcp() -> Config {
        let mut config = Config::default();
        config.run.nested = true;
        config
            .tools
            .mcp_servers
            .push(McpServerSettings::stdio("demo", "mock", Vec::new()));
        config
    }

    #[test]
    fn parse_profile_trims_and_session_ask_uses_it() {
        assert_eq!(parse_profile(" full ").unwrap(), ChildProfile::Full);
        assert_eq!(parse_profile("plan\n").unwrap(), ChildProfile::Plan);
        assert!(session_ask_park(
            "child_run",
            r#"{"profile":" full","task":"edit"}"#
        ));
        assert!(session_ask_park(
            "child_run",
            "{\"profile\":\"plan\\n\",\"task\":\"draft\"}"
        ));
        assert!(!session_ask_park(
            "child_run",
            r#"{"profile":"explore","task":"scout"}"#
        ));
        assert!(session_ask_park(
            "child_run",
            r#"{"profile":"explore","task":"scout","worktree":true}"#
        ));
        assert!(!session_ask_park("child_status", r#"{"run_id":"x"}"#));
        assert!(session_ask_park("write_file", r#"{"path":"ok.txt"}"#));
    }

    #[test]
    fn child_config_explore_is_read_only_and_clears_mcp() {
        let parent = parent_with_mcp();
        let child = child_config(
            &parent,
            Path::new("parent-ws"),
            false,
            ChildProfile::Explore,
            false,
        )
        .unwrap();
        assert_eq!(child.tools.mode, PermissionMode::Read);
        assert!(child.tools.enabled.is_empty());
        assert!(child.tools.mcp_servers.is_empty());
        assert!(!child.run.plan_jail);
        assert!(!child.run.nested);
        assert_eq!(child.events.adapter, "none");
        assert_eq!(child.workspace.adapter, "inplace");
        assert_eq!(child.model.adapter, parent.model.adapter);
    }

    #[test]
    fn child_config_plan_jails_and_clears_mcp() {
        let parent = parent_with_mcp();
        let child = child_config(
            &parent,
            Path::new("parent-ws"),
            false,
            ChildProfile::Plan,
            false,
        )
        .unwrap();
        assert!(child.run.plan_jail);
        assert!(child.tools.mcp_servers.is_empty());
        assert!(!child.run.nested);
        assert_eq!(child.events.adapter, "none");
        assert_eq!(child.tools.mode, parent.tools.mode);
    }

    #[test]
    fn child_config_full_keeps_mcp_unless_parent_jailed() {
        let parent = parent_with_mcp();
        let child = child_config(
            &parent,
            Path::new("parent-ws"),
            false,
            ChildProfile::Full,
            false,
        )
        .unwrap();
        assert!(!child.run.plan_jail);
        assert_eq!(child.tools.mcp_servers.len(), 1);
        assert!(!child.run.nested);

        let jailed = child_config(
            &parent,
            Path::new("parent-ws"),
            true,
            ChildProfile::Full,
            false,
        )
        .unwrap();
        assert!(jailed.run.plan_jail);
        assert!(jailed.tools.mcp_servers.is_empty());
    }
}
