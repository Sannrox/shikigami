//! Host-local Run preparation behind one private interface.
//!
//! This deep module owns fresh/resumed state construction, workspace and
//! snapshot preparation, artifact baseline capture, context composition,
//! tool attachment, governed admission, initial durability, recovery replay,
//! and pre-run hooks. The Run transaction receives only ready durable state.

use std::sync::Arc;

use serde_json::json;

use crate::checkpoint::{Checkpoint, GovernanceCheckpoint};
use crate::events::HarnessEvent;
use crate::governance::{GovernanceError, RunHandle};
use crate::hooks::{self, HookEvent};
use crate::model::ChatMessage;
use crate::tools::{ToolDef, ToolRegistry};
use crate::workspace::{
    MaterializedWorkspace, SnapshotOutcome, SnapshotPlan, WorkspaceCleanup, WorkspaceSnapshots,
};

use super::resume::{configured_workspace_adapter, validate_resumed_workspace};
use super::session::RunSession;
use super::{Engine, RunError, RunRequest, SYSTEM_PROMPT};

pub(super) struct PreparedRun {
    pub(super) session: RunSession,
    pub(super) workspace: MaterializedWorkspace,
    pub(super) tools: Arc<ToolRegistry>,
    pub(super) tool_defs: Vec<ToolDef>,
    pub(super) system_prompt: String,
    pub(super) prompt_id: String,
    pub(super) handle: RunHandle,
    pub(super) governance_checkpoint: Option<GovernanceCheckpoint>,
}

pub(super) async fn prepare(
    engine: &Engine,
    request: &RunRequest,
    fresh_run_id: String,
    resume_checkpoint: Option<Checkpoint>,
) -> Result<PreparedRun, RunError> {
    let (run_id, messages, turns, workspace, task, keep_workspace, todos, governance_checkpoint) =
        initial_state(engine, request, fresh_run_id, resume_checkpoint)?;

    engine
        .registry
        .update_running(
            &run_id,
            &task,
            request.logical_operation_id.as_deref(),
            &workspace.path,
            turns,
        )
        .map_err(|error| RunError::Message(format!("run registry update failed: {error}")))?;
    prepare_workspace(engine, request, &run_id, &workspace)?;
    capture_baseline(engine, &run_id, &workspace);
    let (prompt_id, system_prompt) = compose_context(engine, &run_id, &workspace);

    let mut tools = ToolRegistry::from_config(&workspace.path, &engine.config)?;
    tools.set_todos(todos);
    if !engine.config.tools.mcp_servers.is_empty() {
        crate::mcp::attach_mcp_servers(&mut tools, &engine.config).await?;
    }
    let tool_defs = tools.definitions();
    let tools = Arc::new(tools);
    let session = RunSession::new(
        engine.state_runs.clone(),
        Arc::clone(&engine.governance),
        run_id,
        task,
        workspace.path.clone(),
        workspace.adapter.clone(),
        keep_workspace,
        messages,
        turns,
    );
    let handle = engine
        .governance
        .begin_run_with_checkpoint(
            &session.run_id,
            &session.task,
            request.logical_operation_id.as_deref(),
            governance_checkpoint.as_ref(),
        )
        .await?;
    persist_initial_state(
        engine,
        &session,
        tools.as_ref(),
        &handle,
        governance_checkpoint.as_ref(),
    )
    .await?;
    run_pre_hook(engine, request, &session).await?;

    Ok(PreparedRun {
        session,
        workspace,
        tools,
        tool_defs,
        system_prompt,
        prompt_id,
        handle,
        governance_checkpoint,
    })
}

#[allow(clippy::type_complexity)]
fn initial_state(
    engine: &Engine,
    request: &RunRequest,
    fresh_run_id: String,
    resume_checkpoint: Option<Checkpoint>,
) -> Result<
    (
        String,
        Vec<ChatMessage>,
        u32,
        MaterializedWorkspace,
        String,
        bool,
        Vec<crate::tools::TodoItem>,
        Option<GovernanceCheckpoint>,
    ),
    RunError,
> {
    if let Some(resume_id) = &request.resume_run_id {
        let checkpoint = resume_checkpoint.ok_or_else(|| {
            RunError::Message(format!("checkpoint snapshot missing for run {resume_id}"))
        })?;
        let resumed_workspace =
            validate_resumed_workspace(&engine.config, &engine.state_runs, resume_id, &checkpoint)?;
        engine.emit(
            resume_id,
            HarnessEvent::Status {
                status: "resuming".into(),
            },
        );
        let workspace = MaterializedWorkspace {
            path: resumed_workspace,
            adapter: if checkpoint.workspace_adapter.is_empty() {
                configured_workspace_adapter(&engine.config).into()
            } else {
                checkpoint.workspace_adapter.clone()
            },
            cleanup: if checkpoint.keep_workspace || checkpoint.workspace_adapter == "inplace" {
                WorkspaceCleanup::None
            } else {
                WorkspaceCleanup::RemoveDir
            },
        };
        let task = if request.task.is_empty() {
            checkpoint.task.clone()
        } else {
            request.task.clone()
        };
        let mut messages = checkpoint.messages;
        if let Some(park) = &checkpoint.park {
            let answer = request.resume_answer.as_ref().ok_or_else(|| {
                RunError::Message(format!(
                    "run {resume_id} is parked (reason: {}); supply resume_answer / --answer to continue",
                    park.reason
                ))
            })?;
            messages.push(ChatMessage {
                role: "tool".into(),
                content: format!("operator answer: {answer}"),
                tool_call_id: park.tool_call_id.clone(),
                tool_calls: vec![],
            });
        } else if request.resume_answer.is_some() {
            return Err(RunError::Message(
                "resume_answer provided but run is not parked".into(),
            ));
        }
        return Ok((
            checkpoint.run_id,
            messages,
            checkpoint.completed_turns,
            workspace,
            task,
            checkpoint.keep_workspace || request.keep_workspace,
            checkpoint.todos,
            checkpoint.governance,
        ));
    }

    engine.emit(
        &fresh_run_id,
        HarnessEvent::Status {
            status: "starting".into(),
        },
    );
    let workspace = engine
        .workspace
        .materialize(&fresh_run_id, &engine.state_runs)?;
    Ok((
        fresh_run_id,
        vec![ChatMessage {
            role: "user".into(),
            content: request.task.clone(),
            tool_call_id: String::new(),
            tool_calls: vec![],
        }],
        0,
        workspace,
        request.task.clone(),
        request.keep_workspace,
        Vec::new(),
        None,
    ))
}

fn prepare_workspace(
    engine: &Engine,
    request: &RunRequest,
    run_id: &str,
    workspace: &MaterializedWorkspace,
) -> Result<(), RunError> {
    let plan = match request.restore_snapshot.as_deref() {
        Some(name) => SnapshotPlan::Restore(name),
        None if engine.config.workspace.snapshot => SnapshotPlan::CaptureInitial,
        None => SnapshotPlan::None,
    };
    match WorkspaceSnapshots::new(&engine.state_runs).prepare(workspace, run_id, plan)? {
        SnapshotOutcome::Unchanged => {}
        SnapshotOutcome::Captured { name, path } => engine.emit(
            run_id,
            HarnessEvent::Message {
                level: "info".into(),
                text: format!("snapshot {name} at {}", path.display()),
            },
        ),
        SnapshotOutcome::Restored { name } => engine.emit(
            run_id,
            HarnessEvent::Message {
                level: "info".into(),
                text: format!("restored snapshot `{name}`"),
            },
        ),
    }
    Ok(())
}

fn capture_baseline(engine: &Engine, run_id: &str, workspace: &MaterializedWorkspace) {
    super::artifact_lifecycle::RunArtifactLifecycle::new(engine).begin(run_id, &workspace.path);
}

fn compose_context(
    engine: &Engine,
    run_id: &str,
    workspace: &MaterializedWorkspace,
) -> (String, String) {
    let prompt_id = crate::prompts::versioned_id(&crate::prompts::DEFAULT_PROMPT);
    let rules = crate::context::load_project_rules(&workspace.path, &engine.config.context);
    let skills = crate::context::load_skills(&workspace.path, &engine.config.context);
    let prompt = crate::context::compose_system_prompt(SYSTEM_PROMPT, rules.as_ref(), &skills);
    engine.emit(
        run_id,
        HarnessEvent::Prompt {
            prompt_id: prompt_id.clone(),
        },
    );
    if let Some(rules) = rules {
        engine.emit(
            run_id,
            HarnessEvent::Message {
                level: "info".into(),
                text: format!("project_rules {} digest={}", rules.filename, rules.digest),
            },
        );
    }
    for skill in skills {
        engine.emit(
            run_id,
            HarnessEvent::Message {
                level: "info".into(),
                text: format!("skill {} digest={}", skill.id, skill.digest),
            },
        );
    }
    engine.emit(
        run_id,
        HarnessEvent::Message {
            level: "info".into(),
            text: format!("workspace {}", workspace.path.display()),
        },
    );
    (prompt_id, prompt)
}

async fn persist_initial_state(
    engine: &Engine,
    session: &RunSession,
    tools: &ToolRegistry,
    handle: &RunHandle,
    checkpoint: Option<&GovernanceCheckpoint>,
) -> Result<(), RunError> {
    let host_receipt_was_durable = checkpoint.is_some_and(|cp| !cp.operation_id.is_empty());
    if let Err(error) = session.save(tools) {
        if !host_receipt_was_durable
            && let Err(compensation) = engine
                .governance
                .abort_uncheckpointed_run(handle, &format!("initial checkpoint failed: {error}"))
                .await
        {
            return Err(RunError::Message(format!(
                "initial checkpoint failed: {error}; governance receipt compensation failed: {compensation}"
            )));
        }
        return Err(error);
    }
    engine
        .governance
        .recover_staged_tool_executions(handle)
        .await?;
    if let Err(error) = engine.governance.replay_staged_tool_reports(handle).await
        && engine.config.requires_governance()
    {
        return Err(error.into());
    }
    session.save(tools)
}

async fn run_pre_hook(
    engine: &Engine,
    request: &RunRequest,
    session: &RunSession,
) -> Result<(), RunError> {
    hooks::run_hooks(
        &engine.config.hooks,
        HookEvent::PreRun,
        json!({
            "run_id": session.run_id,
            "task": session.task,
            "resume": request.resume_run_id.is_some(),
        }),
    )
    .await
    .map_err(|error| {
        RunError::Governance(GovernanceError::Message(format!(
            "pre-run hook failed; governed state remains checkpointed for resume: {error}"
        )))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::registry::RunRegistry;
    use crate::state::StateRoot;
    use crate::{events, governance, workspace};
    use tempfile::tempdir;

    #[tokio::test]
    async fn preparation_returns_checkpointed_state_ready_for_a_turn() {
        let directory = tempdir().unwrap();
        let state = StateRoot::new(directory.path().join("state"));
        state.ensure_ready_for_runs().unwrap();
        let mut config = Config::default();
        config.governance.adapter = "local".into();
        config.events.adapter = "none".into();
        config.workspace.root = directory.path().join("ws").to_string_lossy().into();
        config.model.adapter = "scripted".into();
        let registry = Arc::new(RunRegistry::new(state.path()).unwrap());
        let engine = Engine::new(
            config.clone(),
            Arc::from(governance::from_config(&config).unwrap()),
            Arc::from(workspace::from_config(&config).unwrap()),
            Arc::from(crate::model::from_config(&config).unwrap()),
            Arc::from(events::from_config(&config, &state.runs_dir()).unwrap()),
            state.runs_dir(),
            Arc::clone(&registry),
        );
        let run_id = "preparation-test";
        registry.start(run_id, "task", None, None).unwrap();

        let prepared = prepare(
            &engine,
            &RunRequest {
                task: "task".into(),
                keep_workspace: true,
                ..RunRequest::new("")
            },
            run_id.into(),
            None,
        )
        .await
        .unwrap();

        assert_eq!(prepared.session.run_id, run_id);
        assert_eq!(prepared.session.messages[0].content, "task");
        assert!(!prepared.tool_defs.is_empty());
        assert!(!prepared.system_prompt.is_empty());
        let checkpoint = Checkpoint::load(&state.runs_dir(), run_id).unwrap();
        assert_eq!(checkpoint.run_id, run_id);
        assert_eq!(checkpoint.workspace, prepared.workspace.path);
    }
}
