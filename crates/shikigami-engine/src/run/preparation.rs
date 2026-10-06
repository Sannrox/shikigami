//! Host-local Run preparation behind one private interface.
//!
//! This deep module owns fresh/resumed state construction, workspace and
//! snapshot preparation, artifact baseline capture, context composition,
//! tool attachment, governed admission, initial durability, recovery replay,
//! and pre-run hooks. The Run transaction receives only ready durable state.

use std::sync::Arc;

use serde_json::json;

use crate::checkpoint::{Checkpoint, GovernanceCheckpoint};
use crate::content::{ContentCheckpointBinding, initial_messages_match};
use crate::events::HarnessEvent;
use crate::governance::{GovernanceError, RunHandle};
use crate::hooks::{self, HookEvent};
use crate::model::ChatMessage;
use crate::replay::{ReplayCheckpoint, ReplayExecution};
use crate::tools::{ToolDef, ToolRegistry};
use crate::workspace::{
    MaterializedWorkspace, SnapshotOutcome, SnapshotPlan, WorkspaceCleanup, WorkspaceSnapshots,
};

use super::resume::{configured_workspace_adapter, validate_resumed_workspace};
use super::session::{ContentResumeState, RunSession};
use super::{ContentExecution, Engine, RunError, RunRequest, SYSTEM_PROMPT};

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
    replay: Option<&ReplayExecution>,
    content: Option<&ContentExecution>,
) -> Result<PreparedRun, RunError> {
    if content.is_some()
        && let Some(checkpoint) = resume_checkpoint.as_ref()
        && !request.task.is_empty()
        && request.task != checkpoint.task
    {
        return Err(RunError::Message(format!(
            "content task changed across resume for run {}",
            checkpoint.run_id
        )));
    }
    let is_resume = resume_checkpoint.is_some();
    let stored_prompt_start = resume_checkpoint
        .as_ref()
        .and_then(|checkpoint| checkpoint.prompt_start_turns);
    let stored_plan_jail = resume_checkpoint
        .as_ref()
        .is_some_and(|checkpoint| checkpoint.plan_jail);
    let stored_nested = resume_checkpoint
        .as_ref()
        .is_some_and(|checkpoint| checkpoint.nested);
    let stored_children = resume_checkpoint
        .as_ref()
        .map(|checkpoint| checkpoint.children.clone())
        .unwrap_or_default();
    let stored_nested_depth = resume_checkpoint
        .as_ref()
        .map(|checkpoint| checkpoint.nested_depth)
        .unwrap_or(request.nested_depth);
    let stored_parent_run_id = resume_checkpoint
        .as_ref()
        .map(|checkpoint| checkpoint.parent_run_id.clone())
        .filter(|value| !value.is_empty())
        .or_else(|| request.parent_run_id.clone())
        .unwrap_or_default();
    let stored_nested_profile = resume_checkpoint
        .as_ref()
        .map(|checkpoint| checkpoint.nested_profile.clone())
        .filter(|value| !value.is_empty())
        .or_else(|| {
            request
                .nested_profile
                .map(|profile| profile.as_str().into())
        })
        .unwrap_or_default();
    let nested_profile = super::nested::parse_profile(&stored_nested_profile).ok();
    let stored_tools_mode = resume_checkpoint
        .as_ref()
        .map(|checkpoint| checkpoint.tools_mode.clone())
        .filter(|value| !value.is_empty())
        .unwrap_or_default();
    let stored_tools_enabled = resume_checkpoint
        .as_ref()
        .map(|checkpoint| checkpoint.tools_enabled.clone())
        .unwrap_or_default();
    let resumed_park = resume_checkpoint
        .as_ref()
        .and_then(|checkpoint| checkpoint.park.clone());
    let (
        run_id,
        messages,
        turns,
        workspace,
        task,
        keep_workspace,
        todos,
        governance_checkpoint,
        replay_checkpoint,
        content_binding,
    ) = initial_state(engine, request, fresh_run_id, resume_checkpoint, replay)?;

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
    prepare_workspace(engine, request, &run_id, &workspace, is_resume)?;
    capture_baseline(engine, &run_id, &workspace);
    // Root Accept restores execute authority. Nested children keep the stored
    // jail so Accept cannot exceed spawn-time / parent plan-jail (a plan child
    // or a full child of a jailed parent). Reject keeps the stored jail so a
    // later `--resume` cannot write the workspace; fresh runs use the request.
    let plan_jail = if request.resume_plan == Some(super::PlanDecision::Accept)
        && stored_parent_run_id.is_empty()
    {
        false
    } else if is_resume {
        stored_plan_jail
    } else {
        request.plan_jail
    };
    let nested_tools = super::nested::nested_tools_enabled(
        engine,
        request,
        content.is_some(),
        stored_nested_depth,
        &stored_parent_run_id,
        stored_nested,
    );
    let (prompt_id, system_prompt) = compose_context(
        engine,
        &run_id,
        &workspace,
        plan_jail,
        nested_tools,
        nested_profile,
    );

    let explore = nested_profile == Some(super::ChildProfile::Explore);
    let host_enabled = engine.config.tools.effective_enabled();
    // `tools_mode` present means this checkpoint recorded spawn-time authority,
    // including an empty effective set. Do not expand empty via the mode preset.
    let spawn_enabled = if !stored_tools_mode.is_empty() {
        stored_tools_enabled.clone()
    } else if explore {
        crate::config::ToolsSettings::tools_for_mode(crate::config::PermissionMode::Read)
    } else {
        host_enabled.clone()
    };
    // Nested resume must not union host tools onto spawn-time authority.
    let mut enabled = if is_resume && !stored_parent_run_id.is_empty() {
        spawn_enabled
            .into_iter()
            .filter(|name| host_enabled.iter().any(|host| host == name))
            .collect()
    } else {
        spawn_enabled
    };
    if nested_tools {
        for name in ["child_run", "child_status"] {
            if !enabled.iter().any(|tool| tool == name) {
                enabled.push(name.into());
            }
        }
    }
    let mut tools =
        ToolRegistry::from_config_enabled(&workspace.path, &engine.config, enabled.clone())?;
    tools.set_todos(todos);
    if !explore && !plan_jail && !engine.config.tools.mcp_servers.is_empty() {
        crate::mcp::attach_mcp_servers(&mut tools, &engine.config).await?;
    }
    let tool_defs = tools.definitions();
    let tools = Arc::new(tools);
    if let Some(replay) = replay {
        crate::replay::validate_runtime_bindings(
            &engine.config,
            replay,
            &task,
            &system_prompt,
            &tool_defs,
            &workspace.path,
        )
        .map_err(|error| RunError::Message(error.to_string()))?;
    }
    let mut session = RunSession::new(
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
    session.set_replay(replay_checkpoint);
    session.set_resumed_approval_park(resumed_park.clone());
    if request.session_wait {
        session.prompt_start_turns =
            if request.resume_prompt.is_some() || request.resume_run_id.is_none() {
                Some(session.turns)
            } else {
                stored_prompt_start.or(Some(session.turns))
            };
    }
    if request.resume_ask == Some(super::AskDecision::Allow) {
        session.set_ask_allow_call_id(
            resumed_park
                .as_ref()
                .filter(|park| park.kind == crate::checkpoint::ParkKind::Ask)
                .map(|park| {
                    if park.allow_call_id.is_empty() {
                        park.tool_call_id.clone()
                    } else {
                        park.allow_call_id.clone()
                    }
                }),
        );
        session.set_resumed_ask_park(resumed_park.clone());
    }
    session.plan_jail = plan_jail;
    session.nested = nested_tools;
    session.children = stored_children;
    session.nested_depth = stored_nested_depth;
    session.parent_run_id = stored_parent_run_id;
    session.nested_profile = stored_nested_profile;
    session.tools_mode = if !stored_tools_mode.is_empty() {
        stored_tools_mode
    } else if !session.parent_run_id.is_empty() {
        engine.config.tools.mode.as_str().into()
    } else {
        String::new()
    };
    session.tools_enabled = if !stored_tools_enabled.is_empty() {
        stored_tools_enabled
    } else if !session.parent_run_id.is_empty() {
        enabled.clone()
    } else {
        Vec::new()
    };
    if let Some(content) = content {
        let (messages, capabilities, initial_message_count, terminal, usage) =
            if let Some(binding) = content_binding.as_ref() {
                let sidecar =
                    RunSession::load_content_sidecar(&engine.state_runs, &session.run_id, binding)?;
                if !initial_messages_match(&sidecar, &content.messages)
                    || sidecar.capabilities != content.capabilities
                {
                    return Err(RunError::Message(format!(
                        "content request changed across resume for run {}",
                        session.run_id
                    )));
                }
                (
                    sidecar.messages,
                    sidecar.capabilities,
                    sidecar.initial_message_count,
                    sidecar.terminal,
                    sidecar.usage,
                )
            } else {
                (
                    content.messages.clone(),
                    content.capabilities.clone(),
                    content.messages.len() as u32,
                    None,
                    crate::model::TokenUsage::default(),
                )
            };
        session.set_content(
            Arc::clone(&content.resolver),
            capabilities,
            messages,
            ContentResumeState {
                initial_message_count,
                binding: content_binding,
                terminal,
                usage,
            },
        )?;
        session.revalidate_content().await?;
        if let Some(prompt) = request.resume_prompt.clone() {
            session.append_content_user_text(prompt).await?;
        }
        if request.resume_ask == Some(super::AskDecision::Deny) {
            let call_id = resumed_park
                .as_ref()
                .map(|park| {
                    if park.tool_call_id.is_empty() {
                        park.allow_call_id.clone()
                    } else {
                        park.tool_call_id.clone()
                    }
                })
                .filter(|id| !id.is_empty())
                .ok_or_else(|| RunError::Message("ask deny is missing a tool call id".into()))?;
            session
                .append_content_tool_text(call_id, "permission denied".into())
                .await?;
        }
        if request.resume_plan == Some(super::PlanDecision::Reject) {
            let call_id = resumed_park
                .as_ref()
                .map(|park| park.tool_call_id.clone())
                .filter(|id| !id.is_empty())
                .ok_or_else(|| RunError::Message("plan reject is missing a tool call id".into()))?;
            session
                .append_content_tool_text(call_id, "plan rejected".into())
                .await?;
        }
    }
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
        &mut session,
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
    replay: Option<&ReplayExecution>,
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
        Option<ReplayCheckpoint>,
        Option<ContentCheckpointBinding>,
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
        let adapter = if checkpoint.workspace_adapter.is_empty() {
            configured_workspace_adapter(&engine.config).into()
        } else {
            checkpoint.workspace_adapter.clone()
        };
        let cleanup = if adapter == "inplace" {
            WorkspaceCleanup::None
        } else if adapter == "git-worktree" {
            crate::workspace::git_worktree_cleanup(
                &resumed_workspace,
                resume_id,
                &engine.config.workspace.branch_prefix,
                std::path::Path::new(&engine.config.workspace.root),
            )
        } else if checkpoint.keep_workspace {
            WorkspaceCleanup::None
        } else {
            WorkspaceCleanup::RemoveDir
        };
        let workspace = MaterializedWorkspace {
            path: resumed_workspace,
            adapter,
            cleanup,
        };
        let task = if request.task.is_empty() {
            checkpoint.task.clone()
        } else {
            request.task.clone()
        };
        let escalate_park = checkpoint.is_escalate_park();
        let ask_park = checkpoint.is_ask_park();
        let prompt_wait = checkpoint.is_prompt_wait();
        let plan_park = checkpoint.is_plan_park();
        let approval_wait = checkpoint
            .park
            .as_ref()
            .is_some_and(|park| park.kind == crate::checkpoint::ParkKind::Approval);
        let park_tool_call_id = checkpoint
            .park
            .as_ref()
            .map(|park| park.tool_call_id.clone());
        let park_reason = checkpoint.park.as_ref().map(|park| park.reason.clone());
        let mut messages = checkpoint.messages;
        if escalate_park {
            let answer = request.resume_answer.as_ref().ok_or_else(|| {
                RunError::Message(format!(
                    "run {resume_id} is parked (reason: {}); supply resume_answer / --answer to continue",
                    park_reason.as_deref().unwrap_or("escalate")
                ))
            })?;
            messages.push(ChatMessage {
                role: "tool".into(),
                content: format!("operator answer: {answer}"),
                tool_call_id: park_tool_call_id.expect("escalate park"),
                tool_calls: vec![],
            });
        } else if ask_park {
            match request.resume_ask {
                Some(super::AskDecision::Deny) => {
                    let park = checkpoint.park.as_ref().expect("ask park");
                    let call_id = if park.allow_call_id.is_empty() {
                        park.tool_call_id.clone()
                    } else {
                        park.allow_call_id.clone()
                    };
                    let name = park
                        .reason
                        .strip_prefix("ask:")
                        .unwrap_or("tool")
                        .to_string();
                    engine.emit(
                        resume_id,
                        HarnessEvent::ToolEnd {
                            name,
                            ok: false,
                            detail: "permission denied".into(),
                            run_id: resume_id.clone(),
                            turn: checkpoint.completed_turns,
                            call_id,
                        },
                    );
                    messages.push(ChatMessage {
                        role: "tool".into(),
                        content: "permission denied".into(),
                        tool_call_id: park_tool_call_id.expect("ask park"),
                        tool_calls: vec![],
                    });
                }
                Some(super::AskDecision::Allow) => {}
                None => {
                    return Err(RunError::Message(format!(
                        "run {resume_id} is parked for ask=park; supply resume_ask to continue"
                    )));
                }
            }
        } else if plan_park {
            match request.resume_plan {
                Some(super::PlanDecision::Reject) => {
                    messages.push(ChatMessage {
                        role: "tool".into(),
                        content: "plan rejected".into(),
                        tool_call_id: park_tool_call_id.expect("plan park"),
                        tool_calls: vec![],
                    });
                }
                Some(super::PlanDecision::Accept) => {
                    let digest = checkpoint
                        .park
                        .as_ref()
                        .map(|park| park.plan_digest.as_str())
                        .filter(|digest| !digest.is_empty())
                        .unwrap_or("sha256:missing");
                    messages.push(ChatMessage {
                        role: "tool".into(),
                        content: format!("plan accepted {digest}"),
                        tool_call_id: park_tool_call_id.expect("plan park"),
                        tool_calls: vec![],
                    });
                }
                None => {
                    return Err(RunError::Message(format!(
                        "run {resume_id} is parked for plan review; supply resume_plan to continue"
                    )));
                }
            }
        } else if prompt_wait {
            let prompt = request.resume_prompt.as_ref().ok_or_else(|| {
                RunError::Message(format!(
                    "run {resume_id} is waiting for the next session prompt"
                ))
            })?;
            messages.push(ChatMessage {
                role: "user".into(),
                content: prompt.clone(),
                tool_call_id: String::new(),
                tool_calls: vec![],
            });
        } else if request.resume_answer.is_some() {
            return Err(RunError::Message(if approval_wait {
                "resume_answer is not used for approval parks".into()
            } else {
                "resume_answer provided but run is not parked".into()
            }));
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
            checkpoint.replay,
            checkpoint.content,
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
    if replay.is_some() && workspace.adapter == "inplace" {
        return Err(RunError::Message(
            "replay requires an isolated `directory` or `git-worktree` workspace".into(),
        ));
    }
    let replay_checkpoint =
        replay.map(|execution| crate::replay::replay_checkpoint(execution, &workspace.path));
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
        replay_checkpoint,
        None,
    ))
}

fn prepare_workspace(
    engine: &Engine,
    request: &RunRequest,
    run_id: &str,
    workspace: &MaterializedWorkspace,
    is_resume: bool,
) -> Result<(), RunError> {
    let plan = match request.restore_snapshot.as_deref() {
        Some(name) => SnapshotPlan::Restore(name),
        None if engine.config.workspace.snapshot && is_resume => SnapshotPlan::KeepInitial,
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
    plan_jail: bool,
    nested_tools: bool,
    nested_profile: Option<super::ChildProfile>,
) -> (String, String) {
    let prompt_id = crate::prompts::versioned_id(&crate::prompts::DEFAULT_PROMPT);
    let rules = crate::context::load_project_rules(&workspace.path, &engine.config.context);
    let skills = crate::context::load_skills(&workspace.path, &engine.config.context);
    let mut prompt = crate::context::compose_system_prompt(SYSTEM_PROMPT, rules.as_ref(), &skills);
    if plan_jail {
        prompt.push_str("\n\nPlan write-jail is active. Mutating tools may only write `");
        prompt.push_str(crate::tools::PLAN_JAIL_PATH);
        prompt.push_str(
            "`. Call report when the plan is ready for operator review. After accept, execute authority is restored on this run.",
        );
    }
    if nested_tools {
        prompt.push_str(
            "\n\nNested child runs are available via child_run. Profiles: explore (read-only), plan (write-jail), full (parent authority). Depth and fan-out are capped. child_status polls a child of this run.",
        );
    }
    if let Some(profile) = nested_profile {
        prompt.push_str("\n\nThis is a nested child run (profile=");
        prompt.push_str(profile.as_str());
        prompt.push_str("). Report a structured summary; do not assume parent execute authority.");
    }
    if let Some(extra) = engine.config.context.session_prompt.as_deref()
        && !extra.trim().is_empty()
    {
        prompt.push_str("\n\n# Session mode\n\n");
        prompt.push_str(extra);
    }
    if nested_profile.is_none()
        && let Some(mode) = engine.config.session.selected.as_deref()
    {
        engine.emit(
            run_id,
            HarnessEvent::SessionMode {
                mode: mode.to_string(),
                model: crate::model::effective_model_name(&engine.config),
                effort: engine.config.model.effort.clone(),
                tools: engine.config.tools.effective_enabled(),
            },
        );
    }
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
    session: &mut RunSession,
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
            None,
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
        assert!(!prepared.system_prompt.contains("Plan write-jail"));
    }

    #[tokio::test]
    async fn plan_jail_adds_mode_instructions_to_the_system_prompt() {
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
        let run_id = "plan-jail-prompt";
        registry.start(run_id, "task", None, None).unwrap();
        let mut request = RunRequest::new("task");
        request.keep_workspace = true;
        request.plan_jail = true;
        let prepared = prepare(&engine, &request, run_id.into(), None, None, None)
            .await
            .unwrap();
        assert!(prepared.session.plan_jail);
        assert!(
            prepared.system_prompt.contains("Plan write-jail"),
            "{}",
            prepared.system_prompt
        );
        assert!(
            prepared
                .system_prompt
                .contains(crate::tools::PLAN_JAIL_PATH)
        );
    }
}
