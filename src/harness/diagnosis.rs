//! Harness diagnosis behind the stable doctor interface and schema.

use crate::tools::model_visible_builtin_definitions;

use super::{DoctorReport, Harness, credential_summary, display_tools, redact_secrets_in_line};

pub(super) fn doctor(harness: &Harness) -> DoctorReport {
    let mut lines = Vec::new();
    let governance_ok = harness.governance.health_ok();
    let governance_detail = harness.governance.health_detail();
    let workspace_detail = harness.workspace.health_detail();
    let events_detail = harness.events.health_detail();

    lines.push(format!("profile:   {}", harness.config.profile.name));
    lines.push(format!(
        "config:    {}",
        harness.config_source.description()
    ));
    lines.push(format!("state:     {}", harness.state.path().display()));
    lines.push(format!(
        "gov:       {} — {}",
        harness.governance.id(),
        governance_detail
    ));
    lines.push(format!(
        "workspace: {} — {}",
        harness.workspace.id(),
        workspace_detail
    ));
    lines.push(format!(
        "events:    {} — {}",
        harness.events.id(),
        events_detail
    ));
    lines.push(format!("model:     {}", harness.model.id()));
    append_tool_authority(harness, &mut lines);
    lines.push(format!(
        "network:   egress={:?} allow_hosts={}",
        harness.config.network.egress,
        if harness.config.network.allow_hosts.is_empty() {
            "(none)".into()
        } else {
            harness.config.network.allow_hosts.join(",")
        }
    ));
    lines.push(format!(
        "sandbox:   backend={:?} cpu={:?} memory_mb={:?} user_processes={:?}",
        harness.config.sandbox.backend,
        harness.config.sandbox.cpu_time_secs,
        harness.config.sandbox.memory_mb,
        harness.config.sandbox.user_processes,
    ));
    lines.push(format!("max_turns: {}", harness.config.run.max_turns));
    if harness.config.hooks.is_empty() {
        lines.push("hooks:     (none)".into());
    } else {
        let names = harness
            .config
            .hooks
            .iter()
            .map(|hook| format!("{}:{}", hook.event, hook.command))
            .collect::<Vec<_>>();
        lines.push(format!(
            "hooks:     {} [{}]",
            harness.config.hooks.len(),
            names.join(", ")
        ));
    }
    lines.push(format!(
        "credentials: {}",
        credential_summary(&harness.config)
    ));

    let mut ok = true;
    if harness.config.requires_governance() && !governance_ok {
        ok = false;
        lines.push("error: governance unhealthy under fail-closed profile".into());
    }
    if harness.config.governance.adapter == "sekai-chisei"
        && let Err(error) = harness.config.governance_endpoint_required()
    {
        ok = false;
        lines.push(format!("error: {error}"));
    }
    let lines = lines
        .into_iter()
        .map(|line| redact_secrets_in_line(&line, &harness.config))
        .collect();

    DoctorReport {
        schema_version: DoctorReport::SCHEMA_VERSION,
        ok,
        profile: harness.config.profile.name.clone(),
        governance: harness.governance.id().into(),
        governance_detail: redact_secrets_in_line(&governance_detail, &harness.config),
        workspace: harness.workspace.id().into(),
        workspace_detail,
        events: harness.events.id().into(),
        events_detail,
        model: harness.model.id().into(),
        lines,
    }
}

pub(super) async fn doctor_async(harness: &Harness) -> DoctorReport {
    let mut report = doctor(harness);
    #[cfg(feature = "governance-sekai-chisei")]
    if harness.config.governance.adapter == "sekai-chisei"
        && harness.config.governance.endpoint.is_some()
    {
        match crate::governance::sekai_chisei::live_probe(&harness.config).await {
            Ok(message) => report.lines.push(redact_secrets_in_line(
                &format!("plane:     {message}"),
                &harness.config,
            )),
            Err(error) => {
                report.lines.push(redact_secrets_in_line(
                    &format!("plane:     unreachable ({error})"),
                    &harness.config,
                ));
                if harness.config.requires_governance() {
                    report.ok = false;
                }
            }
        }
    }
    report
}

fn append_tool_authority(harness: &Harness, lines: &mut Vec<String>) {
    let authority = harness.config.tools.authority_summary();
    lines.push(format!(
        "tools.mode:       {}",
        harness.config.tools.mode.as_str()
    ));
    lines.push(format!(
        "tools.configured: {}",
        display_tools(&authority.configured_enabled)
    ));
    lines.push(format!(
        "tools.preset:     {}",
        display_tools(&authority.preset_enabled)
    ));
    lines.push(format!(
        "tools.excluded:   {}",
        display_tools(&authority.excluded_by_intersection)
    ));
    let visible = model_visible_builtin_definitions(&authority.effective_enabled)
        .into_iter()
        .map(|definition| definition.name)
        .collect::<Vec<_>>();
    let implicit = visible
        .iter()
        .filter(|tool| !authority.effective_enabled.contains(tool))
        .cloned()
        .collect::<Vec<_>>();
    lines.push(format!("tools.implicit:   {}", display_tools(&implicit)));
    lines.push(format!(
        "tools.effective:  {}",
        display_tools(&authority.effective_enabled)
    ));
    lines.push(format!("tools.visible:    {}", display_tools(&visible)));
    lines.push(format!(
        "tools.environment: parent minus protected/startup controls; protected={}",
        display_tools(&harness.config.protected_tool_environment_names()),
    ));
    if !harness.config.tools.mcp_servers.is_empty() {
        let servers = harness
            .config
            .tools
            .mcp_servers
            .iter()
            .map(|server| server.name.as_str())
            .collect::<Vec<_>>();
        lines.push(format!(
            "tools.external:   MCP servers [{}] (resolved at run start)",
            servers.join(", ")
        ));
    }
}
