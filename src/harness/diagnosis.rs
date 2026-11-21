//! Harness diagnosis behind the stable doctor interface and schema.

use std::path::Path;

use crate::context::{
    measure_standing, offered_tool_schema_bytes, offers_lazy_skills, standing_overflow,
};
use crate::tools::{
    load_mcp_tool_definition, load_skill_definition, model_visible_builtin_definitions,
};

use super::{DoctorReport, Harness, credential_summary, display_tools, redact_secrets_in_line};

pub(super) fn doctor(harness: &Harness) -> DoctorReport {
    let mut lines = Vec::new();
    let governance_ok = harness.governance.health_ok();
    let governance_detail = harness.governance.health_detail();
    let workspace_detail = harness.workspace.health_detail();
    let events_detail = harness.events.health_detail();

    lines.push(format!("profile:   {}", harness.config.profile.name));
    if let Some(preset) = &harness.config.preset {
        lines.push(format!("preset:    {}", preset.id));
    }
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
    lines.push(harness.config.tracing.doctor_line());
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
    let mut sandbox_error = None;
    match crate::sandbox::health(&harness.config.sandbox) {
        crate::sandbox::SandboxHealth::Ok(line) | crate::sandbox::SandboxHealth::Warning(line) => {
            lines.push(line);
        }
        crate::sandbox::SandboxHealth::Error(error) => {
            sandbox_error = Some(error);
        }
    }
    lines.push(format!("max_turns: {}", harness.config.run.max_turns));
    if let Some(window) = harness.config.run.context_window_tokens {
        lines.push(format!(
            "context_window: {window} tokens (compact at {}%)",
            harness.config.run.compact_at_usage_percent
        ));
    }
    let standing = standing_for_doctor(harness);
    append_standing_lines(&standing, harness, &mut lines);
    if harness.config.run.plan_jail {
        lines.push(format!("plan_jail:  on ({})", crate::tools::PLAN_JAIL_PATH));
    }
    if harness.config.run.nested {
        lines.push(format!(
            "nested:     on (depth<={} children<={})",
            harness.config.run.nested_max_depth, harness.config.run.nested_max_children
        ));
    }
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
    if let Some(error) = standing_overflow(
        &standing,
        harness.config.run.context_window_tokens,
        harness.config.context.standing_max_percent,
    ) {
        // Report only: the run-start gate refuses. Doctor stays adapter health.
        lines.push(format!("standing.over: {error}"));
    }
    if let Some(error) = sandbox_error {
        ok = false;
        lines.push(format!("error: {error}"));
    }
    if harness.config.requires_governance() && !governance_ok {
        ok = false;
        lines.push("error: governance unhealthy under fail-closed profile".into());
    }
    if let Err(error) = harness.config.validate() {
        // Defense in depth for in-memory configs that skipped resolve/validate
        // (e.g. fail_closed + bash + sandbox.backend=none).
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

pub(super) fn doctor_json(harness: &Harness, report: &DoctorReport) -> serde_json::Value {
    let mut value = report.to_json_value();
    let standing = standing_for_doctor(harness);
    value["standing"] =
        serde_json::to_value(&standing).expect("StandingContext is always serializable");
    value["standing_bytes"] = serde_json::json!(standing.bytes);
    value
}

pub(super) async fn doctor_async(harness: &Harness) -> DoctorReport {
    let mut report = doctor(harness);
    if matches!(
        harness.config.governance.adapter.as_str(),
        "http-callback" | "host-authz"
    ) && let Some(endpoint) = harness.config.governance.endpoint.as_deref()
        && let Err(error) = crate::governance::http_callback::probe_reachable(endpoint).await
    {
        report.lines.push(redact_secrets_in_line(
            &format!("callback:  unreachable ({error})"),
            &harness.config,
        ));
        if harness.config.requires_governance() {
            report.ok = false;
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
    let mut visible = model_visible_builtin_definitions(&authority.effective_enabled)
        .into_iter()
        .map(|definition| definition.name)
        .collect::<Vec<_>>();
    // Runs offer `load_skill` for lazy packs (listed, not eager) found on disk.
    if offers_lazy_skills(&authority.effective_enabled, &harness.config.context) {
        visible.push("load_skill".into());
    }
    if offers_load_mcp_tool(harness) {
        visible.push("load_mcp_tool".into());
    }
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

fn offers_load_mcp_tool(harness: &Harness) -> bool {
    harness.config.tools.mcp_lazy_schemas
        && (!harness.config.tools.mcp_servers.is_empty()
            || harness.config.tools.mcp_gateway.is_some())
}

fn standing_for_doctor(harness: &Harness) -> crate::context::StandingContext {
    let enabled = harness.config.tools.effective_enabled();
    let lazy_skills = offers_lazy_skills(&enabled, &harness.config.context);
    let mut defs = model_visible_builtin_definitions(&enabled);
    if lazy_skills {
        defs.push(load_skill_definition());
    }
    if offers_load_mcp_tool(harness) {
        defs.push(load_mcp_tool_definition());
    }
    let tool_schema_bytes = offered_tool_schema_bytes(defs.iter().map(|def| {
        (
            def.name.as_str(),
            def.description.as_str(),
            def.schema.as_str(),
        )
    }));
    measure_standing(
        Path::new(&harness.config.workspace.root),
        &harness.config.context,
        lazy_skills,
        tool_schema_bytes,
        &[],
    )
}

fn append_standing_lines(
    standing: &crate::context::StandingContext,
    harness: &Harness,
    lines: &mut Vec<String>,
) {
    lines.push(format!(
        "standing:  {} bytes (~{} tokens at 4 chars/token)",
        standing.bytes,
        standing.tokens_est()
    ));
    if !standing.rules.is_empty() {
        let rules = standing
            .rules
            .iter()
            .map(|rule| format!("{}={}", rule.filename, rule.bytes))
            .collect::<Vec<_>>();
        lines.push(format!("standing.rules: {}", rules.join(", ")));
    }
    if !standing.eager.is_empty() {
        let eager = standing
            .eager
            .iter()
            .map(|pack| format!("{}={}", pack.id, pack.bytes))
            .collect::<Vec<_>>();
        lines.push(format!("standing.eager: {}", eager.join(", ")));
    }
    if !standing.catalog.is_empty() {
        let catalog = standing
            .catalog
            .iter()
            .map(|pack| format!("{}={}", pack.id, pack.bytes))
            .collect::<Vec<_>>();
        lines.push(format!("standing.catalog: {}", catalog.join(", ")));
    }
    lines.push(format!(
        "standing.tools: {} bytes",
        standing.tool_schema_bytes
    ));
    if !harness.config.tools.mcp_servers.is_empty() || harness.config.tools.mcp_gateway.is_some() {
        lines.push("standing.mcp: schemas resolved at run start".into());
    }
    if let Some(window) = harness.config.run.context_window_tokens {
        let percent = harness.config.context.standing_max_percent;
        let cap_tokens = window.saturating_mul(u64::from(percent)) / 100;
        lines.push(format!(
            "standing.cap:  {percent}% of {window} tokens = {cap_tokens} tokens"
        ));
    }
}
