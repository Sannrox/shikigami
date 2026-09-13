//! Governed ontology read → Action → receipt proof through an actual MCP
//! stdio transport (#279).
//!
//! The parent drives a matrix of scripted harness attempts. Each attempt is a
//! child process running `Engine` with `local` governance, a scripted model,
//! and one `[[tools.mcp_servers]]` entry that spawns a stdio server shaped
//! exactly like `sekai-mcp`: newline-delimited JSON-RPC, the three
//! allowlisted tools, `operation_id` + `input` arguments, session binding on
//! every dispatch, reserved-metadata rejection, idempotent replay, digest
//! conflicts, policy denials, and canonical receipts. The fake keeps a
//! durable ledger of admitted effects and a journal of every native RPC it
//! dispatched — the reference view an ordinary SDK client would observe — so
//! the parent can compare harness-side transcripts with plane-side truth.
//!
//! No live plane is required. `docs/mcp.md` documents the equivalent recipe
//! against a real `sekai-mcp` process.

use std::io::Write;

fn main() {
    #[cfg(unix)]
    if let Err(error) = unix::main() {
        let _ = writeln!(std::io::stderr(), "FAILED: {error}");
        std::process::exit(1);
    }
    #[cfg(not(unix))]
    eprintln!("skip: governed MCP action proof requires Unix process groups");
}

#[cfg(unix)]
mod unix {
    use std::collections::BTreeMap;
    use std::fs;
    use std::os::unix::process::CommandExt;
    use std::path::{Path, PathBuf};
    use std::process::{Command, Stdio};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use serde::{Deserialize, Serialize};
    use serde_json::{Value, json};
    use shikigami::checkpoint::Checkpoint;
    use shikigami::config::{Config, McpServerSettings};
    use shikigami::events::HarnessEvent;
    use shikigami::governance::{GovernancePort, LocalGovernance};
    use shikigami::registry::RunRegistry;
    use shikigami::run::{Engine, RunRequest};
    use shikigami::state::StateRoot;
    use shikigami::{events, model, workspace};

    const FAKE_FLAG: &str = "--fake-sekai-mcp";
    const HOST_FLAG: &str = "--host";
    const DEADLINE: Duration = Duration::from_secs(30);

    /// Plane-owned logical operation shared by every attempt of one case.
    const LOGICAL_OPERATION: &str = "op-279";
    const PRINCIPAL: &str = "tester";
    const NAMESPACE: &str = "acme";
    const OBJECT_ID: &str = "widget-1";
    const ACTION_TYPE: &str = "review.intake";
    const ACTION_VERSION: &str = "1.0.0";
    const IDEMPOTENCY_KEY: &str = "op-279/review.intake";

    const SERVER: &str = "sekai";
    const GET_OBJECT_TOOL: &str = "sekai.objects.get";
    const SUBMIT_ACTION_TOOL: &str = "sekai.actions.submit";
    const GET_RECEIPT_TOOL: &str = "chisei.receipt.read";

    pub(super) fn main() -> Result<(), String> {
        let mut args = std::env::args();
        let _ = args.next();
        match args.next().as_deref() {
            Some(FAKE_FLAG) => {
                let state = args.next().ok_or("fake server needs a state directory")?;
                let runtime = tokio::runtime::Runtime::new().map_err(|error| error.to_string())?;
                runtime.block_on(fake::serve(PathBuf::from(state)))
            }
            Some(HOST_FLAG) => {
                let runtime = tokio::runtime::Runtime::new().map_err(|error| error.to_string())?;
                runtime.block_on(host_main())
            }
            _ => parent_main(),
        }
    }

    // ------------------------------------------------------------------
    // Parent: the proof matrix
    // ------------------------------------------------------------------

    fn parent_main() -> Result<(), String> {
        governed_read_action_receipt()?;
        harness_denial_prevents_the_effect()?;
        plane_denial_prevents_the_effect()?;
        revoked_discovery_fails_closed()?;
        revoked_admission_fails_closed()?;
        repeated_submit_replays_one_effect()?;
        changed_parameters_conflict_without_a_second_effect()?;
        deadline_reconciles_through_the_receipt()?;
        killed_attempt_replacement_reconciles_without_a_second_effect()?;
        eprintln!("ok: governed MCP read → Action → receipt matrix");
        Ok(())
    }

    fn governed_read_action_receipt() -> Result<(), String> {
        eprint!("test governed_read_action_receipt ... ");
        let case = Case::new("happy")?;
        let result = case.run_host(Scenario::ReadSubmitReceipt, None)?;
        result.expect_success()?;
        case.expect_effects(1)?;

        // Harness-side transcript.
        let transcript = case.transcript(&result.run_id)?;
        let read = transcript.tool_result(GET_OBJECT_TOOL)?;
        let submit = transcript.tool_result(SUBMIT_ACTION_TOOL)?;
        let receipt = transcript.tool_result(GET_RECEIPT_TOOL)?;
        for (name, body) in [
            (GET_OBJECT_TOOL, &read),
            (SUBMIT_ACTION_TOOL, &submit),
            (GET_RECEIPT_TOOL, &receipt),
        ] {
            if body["operation_id"] != LOGICAL_OPERATION {
                return Err(format!(
                    "{name} result carries operation {} instead of {LOGICAL_OPERATION}",
                    body["operation_id"]
                ));
            }
        }
        expect_eq(&read["output"]["object"]["id"], OBJECT_ID, "object id")?;
        expect_eq(&read["output"]["object"]["kind"], "widget", "object kind")?;
        expect_eq(
            &read["output"]["object"]["namespace"],
            NAMESPACE,
            "object namespace",
        )?;
        let instance = &submit["output"]["instance"];
        expect_eq(&instance["status"], "admitted", "instance status")?;
        expect_eq(&instance["type_id"], ACTION_TYPE, "instance type")?;
        expect_eq(
            &instance["version"],
            ACTION_VERSION,
            "selected Action version",
        )?;
        expect_eq(
            &instance["operation_id"],
            LOGICAL_OPERATION,
            "instance operation",
        )?;
        expect_eq(
            &instance["idempotency_key"],
            IDEMPOTENCY_KEY,
            "idempotency key",
        )?;
        expect_eq(
            &submit["output"]["replay"],
            false,
            "first submit replay flag",
        )?;
        let receipt_json: Value = serde_json::from_str(
            receipt["output"]["receipt_json"]
                .as_str()
                .ok_or("receipt_json missing")?,
        )
        .map_err(|error| format!("receipt_json is not JSON: {error}"))?;
        expect_eq(&receipt["output"]["complete"], true, "receipt completeness")?;
        expect_eq(
            &receipt_json["operation_id"],
            LOGICAL_OPERATION,
            "receipt operation",
        )?;
        expect_eq(&receipt_json["status"], "admitted", "receipt status")?;
        expect_eq(
            &receipt_json["instance_id"],
            instance["instance_id"].clone(),
            "receipt attributes the effect to the admitted instance",
        )?;

        // Plane-side reference view (what a direct SDK caller observes).
        let journal = case.journal()?;
        let rpcs: Vec<&str> = journal.iter().map(|entry| entry.rpc.as_str()).collect();
        if rpcs != ["GetObject", "SubmitActionInstance", "GetOperationReceipt"] {
            return Err(format!("unexpected native dispatch order {rpcs:?}"));
        }
        for entry in &journal {
            if entry.principal != PRINCIPAL || entry.namespace != NAMESPACE {
                return Err(format!("dispatch lost session identity: {entry:?}"));
            }
            if entry.operation_id != LOGICAL_OPERATION {
                return Err(format!("dispatch lost operation identity: {entry:?}"));
            }
        }
        expect_eq(
            &journal[0].output["object"]["id"],
            read["output"]["object"]["id"].clone(),
            "object identity matches the reference read",
        )?;
        expect_eq(
            &journal[1].input["version"],
            ACTION_VERSION,
            "reference call selected the same Action version",
        )?;
        expect_eq(
            &journal[1].output["instance"]["request_digest"],
            instance["request_digest"].clone(),
            "request digest matches the reference submit",
        )?;
        expect_eq(
            &journal[1].output["instance"]["instance_id"],
            instance["instance_id"].clone(),
            "instance identity matches the reference submit",
        )?;
        if journal[1].request_id != LOGICAL_OPERATION {
            return Err("submit request_id was not bound to the operation".into());
        }

        // Run/tool correlation alongside plane identity.
        let record = case.run_record(&result.run_id)?;
        if record.logical_operation_id.as_deref() != Some(LOGICAL_OPERATION) {
            return Err(format!(
                "run record lost the logical operation: {:?}",
                record.logical_operation_id
            ));
        }
        let events = case.events(&result.run_id)?;
        for tool in [GET_OBJECT_TOOL, SUBMIT_ACTION_TOOL, GET_RECEIPT_TOOL] {
            let end = events.tool_end(tool)?;
            if !end.ok {
                return Err(format!("{tool} reported ok=false: {}", end.detail));
            }
            if end.call_id.is_empty() || end.run_id != result.run_id {
                return Err(format!("{tool} event lost run/tool correlation: {end:?}"));
            }
            if !end
                .detail
                .contains(&format!("\"operation_id\":\"{LOGICAL_OPERATION}\""))
            {
                return Err(format!(
                    "{tool} event detail does not carry the plane operation: {}",
                    end.detail
                ));
            }
        }
        if !result.summary.contains(LOGICAL_OPERATION) {
            return Err(format!(
                "final report does not cite the operation: {}",
                result.summary
            ));
        }
        eprintln!("ok");
        Ok(())
    }

    fn harness_denial_prevents_the_effect() -> Result<(), String> {
        eprint!("test harness_denial_prevents_the_effect ... ");
        let case = Case::new("harness-denied")?;
        let result = case.run_host(Scenario::ReadSubmitReceipt, Some(Grant::WithoutSubmit))?;
        result.expect_failure("harness-denied")?;
        case.expect_effects(0)?;
        let journal = case.journal()?;
        if journal
            .iter()
            .any(|entry| entry.rpc == "SubmitActionInstance")
        {
            return Err("plane received a submit the harness policy denied".into());
        }
        let events = case.events(&result.run_id)?;
        let end = events.tool_end(SUBMIT_ACTION_TOOL)?;
        if end.ok || !end.detail.contains("denies") {
            return Err(format!("submit should be denied by local policy: {end:?}"));
        }
        if !events.tool_end(GET_OBJECT_TOOL)?.ok {
            return Err("object read should still be authorized".into());
        }
        eprintln!("ok");
        Ok(())
    }

    fn plane_denial_prevents_the_effect() -> Result<(), String> {
        eprint!("test plane_denial_prevents_the_effect ... ");
        let case = Case::new("plane-denied")?;
        let result = case.run_host(Scenario::SubmitDeniedByPolicy, None)?;
        result.expect_failure("plane-denied")?;
        case.expect_effects(0)?;
        let transcript = case.transcript(&result.run_id)?;
        let submit = transcript.tool_result(SUBMIT_ACTION_TOOL)?;
        expect_eq(
            &submit["output"]["instance"]["status"],
            "denied",
            "denied status",
        )?;
        if !submit["output"]["instance"]["deny_reason"]
            .as_str()
            .is_some_and(|reason| reason.contains("policy"))
        {
            return Err(format!("deny_reason missing: {submit}"));
        }
        let receipt = transcript.tool_result(GET_RECEIPT_TOOL)?;
        let receipt_json: Value =
            serde_json::from_str(receipt["output"]["receipt_json"].as_str().unwrap_or("{}"))
                .map_err(|error| error.to_string())?;
        expect_eq(
            &receipt_json["status"],
            "denied",
            "receipt records the denial",
        )?;
        let journal = case.journal()?;
        let submit_entry = journal
            .iter()
            .find(|entry| entry.rpc == "SubmitActionInstance")
            .ok_or("plane never saw the submit")?;
        expect_eq(
            &submit_entry.output["instance"]["status"],
            "denied",
            "reference submit is denied too",
        )?;
        eprintln!("ok");
        Ok(())
    }

    fn revoked_discovery_fails_closed() -> Result<(), String> {
        eprint!("test revoked_discovery_fails_closed ... ");
        let case = Case::new("revoked-discovery")?;
        case.mutate_plane(|plane| plane.revoked = true)?;
        let result = case.run_host(Scenario::ReadSubmitReceipt, None)?;
        if result.success {
            return Err("run succeeded although discovery was revoked".into());
        }
        if !result.summary.contains("permission_denied") {
            return Err(format!(
                "run should fail closed on revoked discovery, got {}",
                result.summary
            ));
        }
        case.expect_effects(0)?;
        if !case.journal()?.is_empty() {
            return Err("revoked principal still dispatched native RPCs".into());
        }
        eprintln!("ok");
        Ok(())
    }

    fn revoked_admission_fails_closed() -> Result<(), String> {
        eprint!("test revoked_admission_fails_closed ... ");
        let case = Case::new("revoked-admission")?;
        case.mutate_plane(|plane| plane.deny_submit = true)?;
        let result = case.run_host(Scenario::ReadSubmitRevoked, None)?;
        result.expect_failure("revoked-admission")?;
        case.expect_effects(0)?;
        let events = case.events(&result.run_id)?;
        let end = events.tool_end(SUBMIT_ACTION_TOOL)?;
        if end.ok || !end.detail.contains("permission_denied") {
            return Err(format!(
                "submit should fail with permission_denied: {end:?}"
            ));
        }
        eprintln!("ok");
        Ok(())
    }

    fn repeated_submit_replays_one_effect() -> Result<(), String> {
        eprint!("test repeated_submit_replays_one_effect ... ");
        let case = Case::new("replay")?;
        let result = case.run_host(Scenario::SubmitTwice, None)?;
        result.expect_success()?;
        case.expect_effects(1)?;
        let transcript = case.transcript(&result.run_id)?;
        let submits = transcript.tool_results(SUBMIT_ACTION_TOOL)?;
        if submits.len() != 2 {
            return Err(format!("expected two submits, got {}", submits.len()));
        }
        expect_eq(&submits[0]["output"]["replay"], false, "first submit")?;
        expect_eq(
            &submits[1]["output"]["replay"],
            true,
            "second submit replays",
        )?;
        expect_eq(
            &submits[1]["output"]["instance"]["instance_id"],
            submits[0]["output"]["instance"]["instance_id"].clone(),
            "replay returns the same instance",
        )?;
        eprintln!("ok");
        Ok(())
    }

    fn changed_parameters_conflict_without_a_second_effect() -> Result<(), String> {
        eprint!("test changed_parameters_conflict_without_a_second_effect ... ");
        let case = Case::new("conflict")?;
        let result = case.run_host(Scenario::SubmitThenChangeParameters, None)?;
        result.expect_failure("conflict")?;
        case.expect_effects(1)?;
        let events = case.events(&result.run_id)?;
        let ends = events.tool_ends(SUBMIT_ACTION_TOOL);
        if ends.len() != 2 || !ends[0].ok || ends[1].ok {
            return Err(format!("expected admitted then conflict: {ends:?}"));
        }
        if !ends[1].detail.contains("already_exists") {
            return Err(format!(
                "conflict should be already_exists: {}",
                ends[1].detail
            ));
        }
        eprintln!("ok");
        Ok(())
    }

    fn deadline_reconciles_through_the_receipt() -> Result<(), String> {
        eprint!("test deadline_reconciles_through_the_receipt ... ");
        let case = Case::new("deadline")?;
        case.mutate_plane(|plane| plane.slow_submit_ms = 4_000)?;
        let result = case.run_host(Scenario::SubmitReconcileThroughReceipt, None)?;
        result.expect_success()?;
        case.expect_effects(1)?;
        let events = case.events(&result.run_id)?;
        let submit = events.tool_end(SUBMIT_ACTION_TOOL)?;
        if submit.ok || !submit.detail.contains("deadline_exceeded") {
            return Err(format!(
                "submit must fail closed at the deadline, got {submit:?}"
            ));
        }
        let receipt = events.tool_end(GET_RECEIPT_TOOL)?;
        if !receipt.ok {
            return Err(format!(
                "receipt read failed after the deadline: {receipt:?}"
            ));
        }
        let transcript = case.transcript(&result.run_id)?;
        let receipt_body = transcript.tool_result(GET_RECEIPT_TOOL)?;
        let receipt_json: Value = serde_json::from_str(
            receipt_body["output"]["receipt_json"]
                .as_str()
                .unwrap_or("{}"),
        )
        .map_err(|error| error.to_string())?;
        expect_eq(
            &receipt_json["status"],
            "admitted",
            "receipt proves the single effect",
        )?;
        if !result.summary.contains("receipt") {
            return Err(format!(
                "final report must cite the receipt, not the timed-out call: {}",
                result.summary
            ));
        }
        eprintln!("ok");
        Ok(())
    }

    fn killed_attempt_replacement_reconciles_without_a_second_effect() -> Result<(), String> {
        eprint!("test killed_attempt_replacement_reconciles_without_a_second_effect ... ");
        let case = Case::new("replacement")?;
        case.mutate_plane(|plane| plane.hold_submit = true)?;
        let mut child = case
            .host_command(Scenario::ReadSubmitReceipt, None)
            .spawn()
            .map_err(io)?;
        // The plane admitted the effect and is holding the response; the
        // attempt dies mid-call, exactly like a lost lease or host crash.
        case.wait_for_dispatch("SubmitActionInstance")?;
        // SAFETY: `child` was spawned into its own process group; the negative
        // pid targets that group (attempt plus its MCP child) only.
        unsafe {
            libc::kill(-(child.id() as i32), libc::SIGKILL);
        }
        let _ = child.wait();
        case.expect_effects(1)?;

        // A replacement attempt under the same operation reconciles first,
        // then re-submits with the same idempotency key.
        case.mutate_plane(|plane| plane.hold_submit = false)?;
        let replacement = case.run_host(Scenario::ReconcileThenSubmit, None)?;
        replacement.expect_success()?;
        case.expect_effects(1)?;
        let transcript = case.transcript(&replacement.run_id)?;
        let receipt = transcript.tool_result(GET_RECEIPT_TOOL)?;
        let receipt_json: Value =
            serde_json::from_str(receipt["output"]["receipt_json"].as_str().unwrap_or("{}"))
                .map_err(|error| error.to_string())?;
        expect_eq(
            &receipt_json["status"],
            "admitted",
            "receipt shows the prior effect",
        )?;
        let submit = transcript.tool_result(SUBMIT_ACTION_TOOL)?;
        expect_eq(
            &submit["output"]["replay"],
            true,
            "replacement submit replays",
        )?;
        let record = case.run_record(&replacement.run_id)?;
        if record.logical_operation_id.as_deref() != Some(LOGICAL_OPERATION) {
            return Err("replacement attempt lost the logical operation".into());
        }
        eprintln!("ok");
        Ok(())
    }

    // ------------------------------------------------------------------
    // Case plumbing
    // ------------------------------------------------------------------

    struct Case {
        _dir: tempfile::TempDir,
        control: PathBuf,
    }

    impl Case {
        fn new(name: &str) -> Result<Self, String> {
            let dir = tempfile::Builder::new()
                .prefix(&format!("mcp-279-{name}-"))
                .tempdir()
                .map_err(io)?;
            let control = dir.path().to_path_buf();
            fs::create_dir_all(control.join("state")).map_err(io)?;
            fs::create_dir_all(control.join("ws")).map_err(io)?;
            fs::create_dir_all(control.join("plane")).map_err(io)?;
            fake::PlaneState::default().save(&control.join("plane"))?;
            Ok(Self { _dir: dir, control })
        }

        fn plane_dir(&self) -> PathBuf {
            self.control.join("plane")
        }

        fn mutate_plane(&self, apply: impl FnOnce(&mut fake::PlaneState)) -> Result<(), String> {
            let mut plane = fake::PlaneState::load(&self.plane_dir())?;
            apply(&mut plane);
            plane.save(&self.plane_dir())
        }

        fn host_command(&self, scenario: Scenario, grant: Option<Grant>) -> Command {
            let exe = std::env::current_exe().expect("current test executable");
            let mut command = Command::new(exe);
            command
                .arg(HOST_FLAG)
                .env("SHIKIGAMI_MCP_PROOF_CONTROL", &self.control)
                .env("SHIKIGAMI_MCP_PROOF_SCENARIO", scenario.as_str())
                .env(
                    "SHIKIGAMI_MCP_PROOF_GRANT",
                    grant.unwrap_or(Grant::Full).as_str(),
                )
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .process_group(0);
            command
        }

        fn run_host(&self, scenario: Scenario, grant: Option<Grant>) -> Result<HostResult, String> {
            let output = self.host_command(scenario, grant).output().map_err(io)?;
            let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
            let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
            if let Ok(result) = serde_json::from_str::<HostResult>(stdout.trim()) {
                return Ok(result);
            }
            Err(format!(
                "host attempt did not report: status={} stdout={stdout} stderr={stderr}",
                output.status
            ))
        }

        fn wait_for_dispatch(&self, rpc: &str) -> Result<(), String> {
            let start = Instant::now();
            while start.elapsed() < DEADLINE {
                if self.journal()?.iter().any(|entry| entry.rpc == rpc) {
                    return Ok(());
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            Err(format!("timed out waiting for the plane to dispatch {rpc}"))
        }

        fn expect_effects(&self, expected: usize) -> Result<(), String> {
            let plane = fake::PlaneState::load(&self.plane_dir())?;
            if plane.effects.len() != expected {
                return Err(format!(
                    "effect ledger has {} entries, expected {expected}: {:?}",
                    plane.effects.len(),
                    plane.effects
                ));
            }
            Ok(())
        }

        fn journal(&self) -> Result<Vec<fake::JournalEntry>, String> {
            match fs::read_to_string(self.plane_dir().join("journal.jsonl")) {
                Ok(text) => text
                    .lines()
                    .filter(|line| !line.trim().is_empty())
                    .map(|line| serde_json::from_str(line).map_err(|error| error.to_string()))
                    .collect(),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
                Err(error) => Err(error.to_string()),
            }
        }

        fn transcript(&self, run_id: &str) -> Result<Transcript, String> {
            let checkpoint = Checkpoint::load(&self.control.join("state").join("runs"), run_id)
                .map_err(|error| error.to_string())?;
            Ok(Transcript { checkpoint })
        }

        fn run_record(&self, run_id: &str) -> Result<shikigami::registry::RunRecord, String> {
            RunRegistry::new(self.control.join("state"))
                .map_err(|error| error.to_string())?
                .load(run_id)
                .map_err(|error| error.to_string())
        }

        fn events(&self, run_id: &str) -> Result<Events, String> {
            let path = self.control.join("state").join("runs").join("events.jsonl");
            let text = fs::read_to_string(&path).map_err(io)?;
            let mut events = Vec::new();
            for line in text.lines().filter(|line| !line.trim().is_empty()) {
                let event: HarnessEvent =
                    serde_json::from_str(line).map_err(|error| error.to_string())?;
                events.push(event);
            }
            Ok(Events {
                run_id: run_id.to_string(),
                events,
            })
        }
    }

    /// Full tool payloads from the durable checkpoint (events truncate detail).
    struct Transcript {
        checkpoint: Checkpoint,
    }

    impl Transcript {
        fn tool_results(&self, tool: &str) -> Result<Vec<Value>, String> {
            let full_name = format!("mcp.{SERVER}.{tool}");
            let mut call_ids = Vec::new();
            for message in &self.checkpoint.messages {
                for call in &message.tool_calls {
                    if call.name == full_name {
                        call_ids.push(call.id.clone());
                    }
                }
            }
            let mut results = Vec::new();
            for call_id in call_ids {
                let message = self
                    .checkpoint
                    .messages
                    .iter()
                    .find(|message| message.role == "tool" && message.tool_call_id == call_id)
                    .ok_or_else(|| format!("no tool result for {full_name} call {call_id}"))?;
                results.push(
                    serde_json::from_str(&message.content)
                        .map_err(|error| format!("{full_name} result is not JSON: {error}"))?,
                );
            }
            Ok(results)
        }

        fn tool_result(&self, tool: &str) -> Result<Value, String> {
            self.tool_results(tool)?
                .into_iter()
                .next()
                .ok_or_else(|| format!("no {tool} call in the transcript"))
        }
    }

    #[derive(Debug)]
    struct ToolEndView {
        ok: bool,
        detail: String,
        run_id: String,
        call_id: String,
    }

    struct Events {
        run_id: String,
        events: Vec<HarnessEvent>,
    }

    impl Events {
        fn tool_ends(&self, tool: &str) -> Vec<ToolEndView> {
            let full_name = format!("mcp.{SERVER}.{tool}");
            self.events
                .iter()
                .filter_map(|event| match event {
                    HarnessEvent::ToolEnd {
                        name,
                        ok,
                        detail,
                        run_id,
                        call_id,
                        ..
                    } if *name == full_name && *run_id == self.run_id => Some(ToolEndView {
                        ok: *ok,
                        detail: detail.clone(),
                        run_id: run_id.clone(),
                        call_id: call_id.clone(),
                    }),
                    _ => None,
                })
                .collect()
        }

        fn tool_end(&self, tool: &str) -> Result<ToolEndView, String> {
            self.tool_ends(tool)
                .into_iter()
                .next()
                .ok_or_else(|| format!("no ToolEnd event for {tool}"))
        }
    }

    fn expect_eq(actual: &Value, expected: impl Into<Value>, what: &str) -> Result<(), String> {
        let expected = expected.into();
        if *actual == expected {
            Ok(())
        } else {
            Err(format!("{what}: expected {expected}, got {actual}"))
        }
    }

    fn io(error: std::io::Error) -> String {
        error.to_string()
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct HostResult {
        success: bool,
        run_id: String,
        summary: String,
    }

    impl HostResult {
        fn expect_success(&self) -> Result<(), String> {
            if self.success {
                Ok(())
            } else {
                Err(format!("attempt failed: {}", self.summary))
            }
        }

        fn expect_failure(&self, case: &str) -> Result<(), String> {
            if self.success {
                Err(format!(
                    "{case}: attempt reported success: {}",
                    self.summary
                ))
            } else {
                Ok(())
            }
        }
    }

    // ------------------------------------------------------------------
    // Host attempt: Engine + scripted model + sekai-shaped MCP server
    // ------------------------------------------------------------------

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Scenario {
        /// read → submit → receipt → report.
        ReadSubmitReceipt,
        /// Same calls, but the principal's admission right was revoked, so the
        /// script reports failure.
        ReadSubmitRevoked,
        /// read → submit (policy-denied parameters) → receipt → report failure.
        SubmitDeniedByPolicy,
        /// submit → submit (same key, same parameters) → report.
        SubmitTwice,
        /// submit → submit (same key, changed parameters) → report failure.
        SubmitThenChangeParameters,
        /// submit (expected to time out) → receipt → report from the receipt.
        SubmitReconcileThroughReceipt,
        /// receipt → submit (same key) → report.
        ReconcileThenSubmit,
    }

    impl Scenario {
        fn as_str(self) -> &'static str {
            match self {
                Self::ReadSubmitReceipt => "read_submit_receipt",
                Self::ReadSubmitRevoked => "read_submit_revoked",
                Self::SubmitDeniedByPolicy => "submit_denied_by_policy",
                Self::SubmitTwice => "submit_twice",
                Self::SubmitThenChangeParameters => "submit_then_change_parameters",
                Self::SubmitReconcileThroughReceipt => "submit_reconcile_through_receipt",
                Self::ReconcileThenSubmit => "reconcile_then_submit",
            }
        }

        fn parse(value: &str) -> Option<Self> {
            [
                Self::ReadSubmitReceipt,
                Self::ReadSubmitRevoked,
                Self::SubmitDeniedByPolicy,
                Self::SubmitTwice,
                Self::SubmitThenChangeParameters,
                Self::SubmitReconcileThroughReceipt,
                Self::ReconcileThenSubmit,
            ]
            .into_iter()
            .find(|scenario| scenario.as_str() == value)
        }

        /// Per-request MCP deadline. Only the reconciliation scenario needs a
        /// short one; everywhere else a generous bound keeps slow CI honest.
        fn mcp_timeout_secs(self) -> u64 {
            match self {
                Self::SubmitReconcileThroughReceipt => 1,
                _ => 20,
            }
        }
    }

    /// Which MCP tools the harness allow-list grants to the run.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Grant {
        Full,
        WithoutSubmit,
    }

    impl Grant {
        fn as_str(self) -> &'static str {
            match self {
                Self::Full => "full",
                Self::WithoutSubmit => "without_submit",
            }
        }

        fn parse(value: &str) -> Option<Self> {
            match value {
                "full" => Some(Self::Full),
                "without_submit" => Some(Self::WithoutSubmit),
                _ => None,
            }
        }

        fn enabled_tools(self) -> Vec<String> {
            let mut tools = vec![
                "report".to_string(),
                format!("mcp.{SERVER}.{GET_OBJECT_TOOL}"),
                format!("mcp.{SERVER}.{GET_RECEIPT_TOOL}"),
            ];
            if self == Self::Full {
                tools.push(format!("mcp.{SERVER}.{SUBMIT_ACTION_TOOL}"));
            }
            tools
        }
    }

    async fn host_main() -> Result<(), String> {
        let control = PathBuf::from(
            std::env::var("SHIKIGAMI_MCP_PROOF_CONTROL")
                .map_err(|_| "SHIKIGAMI_MCP_PROOF_CONTROL missing")?,
        );
        let scenario = std::env::var("SHIKIGAMI_MCP_PROOF_SCENARIO")
            .ok()
            .as_deref()
            .and_then(Scenario::parse)
            .ok_or("SHIKIGAMI_MCP_PROOF_SCENARIO missing")?;
        let grant = std::env::var("SHIKIGAMI_MCP_PROOF_GRANT")
            .ok()
            .as_deref()
            .and_then(Grant::parse)
            .unwrap_or(Grant::Full);
        let exe = std::env::current_exe().map_err(io)?;

        let mut config = Config::default();
        config.governance.adapter = "local".into();
        config.governance.principal = PRINCIPAL.into();
        config.model.adapter = "scripted".into();
        config.events.adapter = "jsonl".into();
        config.workspace.root = control.join("ws").to_string_lossy().into();
        config.tools.enabled = grant.enabled_tools();
        config.model.script_json = Some(script_json(scenario, grant));
        let mut server = McpServerSettings::stdio(
            SERVER,
            exe.to_string_lossy(),
            vec![
                FAKE_FLAG.into(),
                control.join("plane").to_string_lossy().into_owned(),
            ],
        );
        server.timeout_secs = scenario.mcp_timeout_secs();
        config.tools.mcp_servers = vec![server];
        config.validate().map_err(|error| error.to_string())?;

        let state = StateRoot::new(control.join("state"));
        state
            .ensure_ready_for_runs()
            .map_err(|error| error.to_string())?;
        let governance: Arc<dyn GovernancePort> = Arc::new(LocalGovernance::from_config(&config));
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
        let mut request = RunRequest::new("prove a governed ontology read and Action through MCP");
        request.keep_workspace = true;
        request.logical_operation_id = Some(LOGICAL_OPERATION.into());
        let result = match engine.run(request).await {
            Ok(result) => HostResult {
                success: result.success,
                run_id: result.run_id,
                summary: result.summary,
            },
            Err(error) => HostResult {
                success: false,
                run_id: String::new(),
                summary: error.to_string(),
            },
        };
        println!(
            "{}",
            serde_json::to_string(&result).map_err(|error| error.to_string())?
        );
        Ok(())
    }

    /// Scripted model turns. Every MCP call binds the plane operation
    /// identity the run was started with, so tool correlation stays
    /// attached to the same operation the receipt is read for.
    fn script_json(scenario: Scenario, grant: Grant) -> String {
        let read = mcp_call("read-1", GET_OBJECT_TOOL, json!({"id": OBJECT_ID}));
        let submit = |id: &str, summary: &str| {
            mcp_call(
                id,
                SUBMIT_ACTION_TOOL,
                json!({
                    "type_id": ACTION_TYPE,
                    "version": ACTION_VERSION,
                    "parameters_json": json!({"summary": summary}).to_string(),
                    "idempotency_key": IDEMPOTENCY_KEY,
                }),
            )
        };
        let receipt = mcp_call(
            "receipt-1",
            GET_RECEIPT_TOOL,
            json!({"operation_id": LOGICAL_OPERATION}),
        );
        let report = |success: bool, summary: &str| {
            json!({"tool_calls":[{
                "id": "report-1",
                "name": "report",
                "args_json": json!({"summary": summary, "success": success}).to_string()
            }]})
        };
        let turns = match scenario {
            Scenario::ReadSubmitReceipt => vec![
                read,
                submit("submit-1", "approve"),
                receipt,
                report(
                    grant == Grant::Full,
                    &format!(
                        "read {OBJECT_ID}; {ACTION_TYPE}@{ACTION_VERSION} under operation {LOGICAL_OPERATION}; receipt inspected"
                    ),
                ),
            ],
            Scenario::ReadSubmitRevoked => vec![
                read,
                submit("submit-1", "approve"),
                receipt,
                report(false, "admission right revoked; no effect"),
            ],
            Scenario::SubmitDeniedByPolicy => vec![
                read,
                submit("submit-1", "deny-me"),
                receipt,
                report(false, "admission denied by plane policy; no effect"),
            ],
            Scenario::SubmitTwice => vec![
                submit("submit-1", "approve"),
                submit("submit-2", "approve"),
                report(
                    true,
                    &format!("submitted twice under {IDEMPOTENCY_KEY}; replayed"),
                ),
            ],
            Scenario::SubmitThenChangeParameters => vec![
                submit("submit-1", "approve"),
                submit("submit-2", "changed"),
                report(
                    false,
                    "changed parameters conflict with the admitted instance",
                ),
            ],
            Scenario::SubmitReconcileThroughReceipt => vec![
                submit("submit-1", "approve"),
                receipt,
                report(
                    true,
                    &format!(
                        "submit deadline exceeded; outcome reconciled through receipt for {LOGICAL_OPERATION}"
                    ),
                ),
            ],
            Scenario::ReconcileThenSubmit => vec![
                receipt,
                submit("submit-1", "approve"),
                report(
                    true,
                    &format!(
                        "replacement attempt reconciled {LOGICAL_OPERATION} through receipt then replayed"
                    ),
                ),
            ],
        };
        Value::Array(turns).to_string()
    }

    fn mcp_call(id: &str, tool: &str, input: Value) -> Value {
        json!({"tool_calls":[{
            "id": id,
            "name": format!("mcp.{SERVER}.{tool}"),
            "args_json": json!({"operation_id": LOGICAL_OPERATION, "input": input}).to_string()
        }]})
    }

    // ------------------------------------------------------------------
    // Fake sekai-mcp: stdio projection host over a durable fixture plane
    // ------------------------------------------------------------------

    pub(super) mod fake {
        use std::io::Write;

        use super::*;
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        use tokio::sync::Mutex;

        const PROTOCOL_VERSION: &str = "2024-11-05";
        const RESERVED_ARGUMENT_KEYS: &[&str] = &[
            "authorization",
            "x-principal",
            "x-sekai-namespace",
            "x-sekai-capability",
            "x-sekai-operation-id",
            "x-chisei-work-unit",
            "x-sekai-catalog-version",
            "x-chisei-request-id",
            "principal",
        ];

        #[derive(Debug, Clone, Serialize, Deserialize)]
        pub(crate) struct Instance {
            pub instance_id: String,
            pub type_id: String,
            pub version: String,
            pub status: String,
            pub deny_reason: String,
            pub operation_id: String,
            pub request_digest: String,
            pub idempotency_key: String,
        }

        /// Durable fixture plane shared by every fake process of one case.
        #[derive(Debug, Clone, Default, Serialize, Deserialize)]
        pub(crate) struct PlaneState {
            /// Discovery denied: the principal lost namespace access.
            pub revoked: bool,
            /// Admission denied at call time while discovery still succeeds.
            pub deny_submit: bool,
            /// Hold new admitted submits until `<plane>/release` exists.
            pub hold_submit: bool,
            /// Delay new admitted submit responses (the effect is already recorded).
            pub slow_submit_ms: u64,
            /// Idempotency key → admitted or denied instance.
            pub instances: BTreeMap<String, Instance>,
            /// Instance ids whose effect was applied, in order.
            pub effects: Vec<String>,
        }

        impl PlaneState {
            pub(crate) fn load(dir: &Path) -> Result<Self, String> {
                let bytes = fs::read(dir.join("plane.json")).map_err(io)?;
                serde_json::from_slice(&bytes).map_err(|error| error.to_string())
            }

            pub(crate) fn save(&self, dir: &Path) -> Result<(), String> {
                let tmp = dir.join("plane.json.tmp");
                fs::write(
                    &tmp,
                    serde_json::to_vec_pretty(self).map_err(|error| error.to_string())?,
                )
                .map_err(io)?;
                fs::rename(&tmp, dir.join("plane.json")).map_err(io)
            }
        }

        /// One native dispatch with its bound session identity: the view a
        /// direct SDK caller of the same RPC would have.
        #[derive(Debug, Clone, Serialize, Deserialize)]
        pub(crate) struct JournalEntry {
            pub rpc: String,
            pub principal: String,
            pub namespace: String,
            pub capability: String,
            pub operation_id: String,
            pub request_id: String,
            pub input: Value,
            pub output: Value,
        }

        struct Server {
            dir: PathBuf,
            writer: Arc<Mutex<tokio::io::Stdout>>,
        }

        pub(crate) async fn serve(dir: PathBuf) -> Result<(), String> {
            let server = Arc::new(Server {
                dir,
                writer: Arc::new(Mutex::new(tokio::io::stdout())),
            });
            let mut lines = BufReader::new(tokio::io::stdin()).lines();
            while let Some(line) = lines.next_line().await.map_err(io)? {
                if line.trim().is_empty() {
                    continue;
                }
                let message: Value = serde_json::from_str(&line)
                    .map_err(|error| format!("frame is not JSON: {error}"))?;
                let Some(id) = message.get("id").cloned() else {
                    continue; // notification
                };
                let method = message.get("method").and_then(Value::as_str).unwrap_or("");
                let params = message.get("params").cloned().unwrap_or_else(|| json!({}));
                let (result, delay) = server.handle(method, params);
                let response = match result {
                    Ok(value) => json!({"jsonrpc":"2.0","id":id,"result":value}),
                    Err(error) => json!({"jsonrpc":"2.0","id":id,"error":error}),
                };
                let server = Arc::clone(&server);
                tokio::spawn(async move {
                    match delay {
                        Delay::None => {}
                        Delay::Sleep(duration) => tokio::time::sleep(duration).await,
                        Delay::Hold => {
                            while !server.dir.join("release").is_file() {
                                tokio::time::sleep(Duration::from_millis(20)).await;
                            }
                        }
                    }
                    let mut body = serde_json::to_vec(&response).expect("response");
                    body.push(b'\n');
                    let mut writer = server.writer.lock().await;
                    let _ = writer.write_all(&body).await;
                    let _ = writer.flush().await;
                });
            }
            Ok(())
        }

        enum Delay {
            None,
            Sleep(Duration),
            Hold,
        }

        impl Server {
            fn handle(&self, method: &str, params: Value) -> (Result<Value, Value>, Delay) {
                match method {
                    "initialize" => (
                        Ok(json!({
                            "protocolVersion": PROTOCOL_VERSION,
                            "capabilities": {"tools": {"listChanged": false}},
                            "serverInfo": {"name": "fake-sekai-mcp", "version": "0"},
                            "instructions": "Projection host over GetObject, SubmitActionInstance, and GetOperationReceipt. Discovery is not a grant."
                        })),
                        Delay::None,
                    ),
                    "ping" => (Ok(json!({})), Delay::None),
                    "tools/list" => (self.list_tools(), Delay::None),
                    "tools/call" => self.call_tool(params),
                    _ => (
                        Err(json!({"code": -32601, "message": "method not found"})),
                        Delay::None,
                    ),
                }
            }

            fn discover(&self) -> Result<PlaneState, Value> {
                let plane = PlaneState::load(&self.dir)
                    .map_err(|error| json!({"code": -32000, "message": error}))?;
                if plane.revoked {
                    return Err(json!({
                        "code": -32000,
                        "message": "permission_denied: capability discovery denied"
                    }));
                }
                Ok(plane)
            }

            fn list_tools(&self) -> Result<Value, Value> {
                self.discover()?;
                let tools = [
                    (
                        GET_OBJECT_TOOL,
                        "Read one authorized typed object.",
                        "sekai.GetObjectRequest",
                    ),
                    (
                        SUBMIT_ACTION_TOOL,
                        "Submit one governed Action instance.",
                        "sekai.SubmitActionInstanceRequest",
                    ),
                    (
                        GET_RECEIPT_TOOL,
                        "Inspect the canonical operation receipt.",
                        "chisei.GetOperationReceiptRequest",
                    ),
                ]
                .into_iter()
                .map(|(name, description, input_type)| {
                    json!({
                        "name": name,
                        "description": description,
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "operation_id": {"type": "string"},
                                "input": {"type": "object", "description": input_type}
                            },
                            "required": ["operation_id", "input"]
                        }
                    })
                })
                .collect::<Vec<_>>();
                Ok(json!({"tools": tools}))
            }

            fn call_tool(&self, params: Value) -> (Result<Value, Value>, Delay) {
                let Some(name) = params.get("name").and_then(Value::as_str) else {
                    return (
                        Err(json!({"code": -32602, "message": "tool name is required"})),
                        Delay::None,
                    );
                };
                let arguments = params
                    .get("arguments")
                    .cloned()
                    .unwrap_or_else(|| json!({}));
                if contains_reserved_metadata(&arguments) {
                    return (
                        Ok(tool_error(
                            "invalid_argument",
                            "forged reserved metadata is rejected",
                            name,
                            "",
                        )),
                        Delay::None,
                    );
                }
                let rpc = match name {
                    GET_OBJECT_TOOL => "GetObject",
                    SUBMIT_ACTION_TOOL => "SubmitActionInstance",
                    GET_RECEIPT_TOOL => "GetOperationReceipt",
                    _ => {
                        return (
                            Ok(tool_error(
                                "unimplemented",
                                "unsupported RPC mapping",
                                name,
                                "",
                            )),
                            Delay::None,
                        );
                    }
                };
                let plane = match self.discover() {
                    Ok(plane) => plane,
                    Err(error) => return (Err(error), Delay::None),
                };
                let Some(operation_id) = arguments
                    .get("operation_id")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_string)
                else {
                    return (
                        Err(json!({"code": -32602, "message": "operation_id is required"})),
                        Delay::None,
                    );
                };
                let input = arguments.get("input").cloned().unwrap_or_else(|| json!({}));
                if !input.is_object() {
                    return (
                        Ok(tool_error(
                            "invalid_argument",
                            "input must be an object",
                            name,
                            &operation_id,
                        )),
                        Delay::None,
                    );
                }
                if contains_reserved_metadata(&input) {
                    return (
                        Ok(tool_error(
                            "invalid_argument",
                            "forged reserved metadata is rejected",
                            name,
                            &operation_id,
                        )),
                        Delay::None,
                    );
                }
                let (dispatched, delay) = match rpc {
                    "GetObject" => (get_object(&input), Delay::None),
                    "SubmitActionInstance" => self.submit(plane, &input, &operation_id),
                    _ => (receipt(&plane, &input, &operation_id), Delay::None),
                };
                let output_type = match rpc {
                    "GetObject" => "sekai.GetObjectResponse",
                    "SubmitActionInstance" => "sekai.SubmitActionInstanceResponse",
                    _ => "chisei.GetOperationReceiptResponse",
                };
                match dispatched {
                    Ok((bound_input, output)) => {
                        self.journal(rpc, name, &operation_id, &bound_input, &output);
                        (
                            Ok(json!({
                                "content": [{"type": "text", "text": json!({
                                    "operation_id": operation_id,
                                    "output_type": output_type,
                                    "output": output,
                                }).to_string()}],
                                "structuredContent": {
                                    "operation_id": operation_id,
                                    "output_type": output_type,
                                    "output": output,
                                },
                                "isError": false
                            })),
                            delay,
                        )
                    }
                    Err((code, message)) => (
                        Ok(tool_error(code, &message, name, &operation_id)),
                        Delay::None,
                    ),
                }
            }

            fn submit(
                &self,
                mut plane: PlaneState,
                input: &Value,
                operation_id: &str,
            ) -> (Dispatch, Delay) {
                if plane.deny_submit {
                    return (
                        Err((
                            "permission_denied",
                            "namespace_access denies review.intake".into(),
                        )),
                        Delay::None,
                    );
                }
                if let Some(namespace) = input.get("namespace").and_then(Value::as_str)
                    && namespace != NAMESPACE
                {
                    return (
                        Err((
                            "invalid_argument",
                            "Action namespace must match the authenticated adapter session".into(),
                        )),
                        Delay::None,
                    );
                }
                let Some(type_id) = input.get("type_id").and_then(Value::as_str) else {
                    return (
                        Err(("invalid_argument", "type_id is required".into())),
                        Delay::None,
                    );
                };
                let Some(version) = input.get("version").and_then(Value::as_str) else {
                    return (
                        Err(("invalid_argument", "version is required".into())),
                        Delay::None,
                    );
                };
                if type_id != ACTION_TYPE || version != ACTION_VERSION {
                    return (
                        Err((
                            "not_found",
                            format!("governed action type {type_id}@{version} not found"),
                        )),
                        Delay::None,
                    );
                }
                let parameters_json = match input.get("parameters_json") {
                    Some(Value::String(raw)) => raw.clone(),
                    Some(other) => other.to_string(),
                    None => "{}".into(),
                };
                let parameters: Value = match serde_json::from_str(&parameters_json) {
                    Ok(value) => value,
                    Err(error) => {
                        return (
                            Err(("invalid_argument", format!("parameters_json: {error}"))),
                            Delay::None,
                        );
                    }
                };
                let idempotency_key = input
                    .get("idempotency_key")
                    .and_then(Value::as_str)
                    .filter(|value| !value.is_empty())
                    .unwrap_or(operation_id)
                    .to_string();
                let request_digest = format!(
                    "sha256:{}",
                    shikigami::fallback::sha256_hex(
                        json!({
                            "namespace": NAMESPACE,
                            "type_id": type_id,
                            "version": version,
                            "parameters": parameters,
                        })
                        .to_string()
                        .as_bytes()
                    )
                );
                let bound = json!({
                    "namespace": NAMESPACE,
                    "type_id": type_id,
                    "version": version,
                    "parameters_json": parameters_json,
                    "idempotency_key": idempotency_key,
                    "request_id": operation_id,
                });
                if let Some(existing) = plane.instances.get(&idempotency_key) {
                    if existing.request_digest != request_digest {
                        return (
                            Err((
                                "already_exists",
                                format!(
                                    "idempotency key {idempotency_key} is bound to request digest {}",
                                    existing.request_digest
                                ),
                            )),
                            Delay::None,
                        );
                    }
                    return (
                        Ok((
                            bound,
                            json!({"instance": instance_json(existing), "replay": true}),
                        )),
                        Delay::None,
                    );
                }
                let denied = parameters.get("summary").and_then(Value::as_str) == Some("deny-me");
                let instance = Instance {
                    instance_id: format!("act-{}", plane.instances.len() + 1),
                    type_id: type_id.into(),
                    version: version.into(),
                    status: if denied { "denied" } else { "admitted" }.into(),
                    deny_reason: if denied {
                        format!("policy:{ACTION_TYPE} denies summary deny-me")
                    } else {
                        String::new()
                    },
                    operation_id: operation_id.into(),
                    request_digest,
                    idempotency_key: idempotency_key.clone(),
                };
                if !denied {
                    plane.effects.push(instance.instance_id.clone());
                }
                let output = json!({"instance": instance_json(&instance), "replay": false});
                let delay = if denied {
                    Delay::None
                } else if plane.hold_submit {
                    Delay::Hold
                } else if plane.slow_submit_ms > 0 {
                    Delay::Sleep(Duration::from_millis(plane.slow_submit_ms))
                } else {
                    Delay::None
                };
                plane.instances.insert(idempotency_key, instance);
                if let Err(error) = plane.save(&self.dir) {
                    return (Err(("internal", error)), Delay::None);
                }
                (Ok((bound, output)), delay)
            }

            fn journal(
                &self,
                rpc: &str,
                capability: &str,
                operation_id: &str,
                input: &Value,
                output: &Value,
            ) {
                let entry = JournalEntry {
                    rpc: rpc.into(),
                    principal: PRINCIPAL.into(),
                    namespace: NAMESPACE.into(),
                    capability: capability.into(),
                    operation_id: operation_id.into(),
                    request_id: operation_id.into(),
                    input: input.clone(),
                    output: output.clone(),
                };
                if let Ok(mut file) = fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(self.dir.join("journal.jsonl"))
                {
                    let _ = writeln!(file, "{}", serde_json::to_string(&entry).expect("journal"));
                }
            }
        }

        type Dispatch = Result<(Value, Value), (&'static str, String)>;

        fn get_object(input: &Value) -> Dispatch {
            let id = input
                .get("id")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or((
                    "invalid_argument",
                    "GetObject requires input.id".to_string(),
                ))?;
            let bound = json!({"id": id});
            let objects = fixture_objects();
            let object = objects
                .get(id)
                .ok_or(("not_found", "object not found".to_string()))?;
            if object["namespace"] != NAMESPACE {
                return Err(("permission_denied", "access denied".into()));
            }
            Ok((bound, json!({"object": object})))
        }

        fn receipt(plane: &PlaneState, input: &Value, operation_id: &str) -> Dispatch {
            let target = input
                .get("operation_id")
                .or_else(|| input.get("operationId"))
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
                .unwrap_or(operation_id)
                .to_string();
            let bound = json!({"operation_id": target});
            let instance = plane
                .instances
                .values()
                .find(|instance| instance.operation_id == target);
            let output = match instance {
                Some(instance) => json!({
                    "receipt_json": json!({
                        "operation_id": target,
                        "status": instance.status,
                        "instance_id": instance.instance_id,
                        "request_digest": instance.request_digest,
                        "effects": plane
                            .effects
                            .iter()
                            .filter(|id| **id == instance.instance_id)
                            .count(),
                    })
                    .to_string(),
                    "complete": true,
                    "missing_surfaces": []
                }),
                None => json!({
                    "receipt_json": json!({"operation_id": target, "status": "unknown"}).to_string(),
                    "complete": false,
                    "missing_surfaces": ["action_instance"]
                }),
            };
            Ok((bound, output))
        }

        fn fixture_objects() -> BTreeMap<String, Value> {
            BTreeMap::from([
                (
                    OBJECT_ID.to_string(),
                    json!({
                        "id": OBJECT_ID,
                        "kind": "widget",
                        "name": "spinner",
                        "namespace": NAMESPACE,
                        "external_id": "",
                        "properties": {"name": "spinner", "color": "blue"},
                        "created": 0,
                        "updated": 0,
                    }),
                ),
                (
                    "hidden-1".to_string(),
                    json!({
                        "id": "hidden-1",
                        "kind": "widget",
                        "name": "other-team",
                        "namespace": "other",
                        "external_id": "",
                        "properties": {},
                        "created": 0,
                        "updated": 0,
                    }),
                ),
            ])
        }

        fn instance_json(instance: &Instance) -> Value {
            json!({
                "instance_id": instance.instance_id,
                "namespace": NAMESPACE,
                "type_id": instance.type_id,
                "version": instance.version,
                "status": instance.status,
                "deny_reason": instance.deny_reason,
                "operation_id": instance.operation_id,
                "request_digest": instance.request_digest,
                "idempotency_key": instance.idempotency_key,
            })
        }

        fn contains_reserved_metadata(value: &Value) -> bool {
            value.as_object().is_some_and(|object| {
                object.keys().any(|key| {
                    RESERVED_ARGUMENT_KEYS
                        .iter()
                        .any(|reserved| key.eq_ignore_ascii_case(reserved))
                })
            })
        }

        fn tool_error(code: &str, message: &str, capability: &str, operation_id: &str) -> Value {
            let body = json!({
                "code": code,
                "message": message,
                "capability": capability,
                "operation_id": operation_id,
                "retryable": matches!(code, "aborted" | "unavailable" | "deadline_exceeded"),
            });
            json!({
                "content": [{"type": "text", "text": body.to_string()}],
                "structuredContent": body,
                "isError": true
            })
        }
    }
}
