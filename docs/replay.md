# Content-bound run replay

Run replay executes one new isolated attempt and compares it with retained
evidence. It is an additive library API with a thin CLI host
(`shikigami replay`); it does not change checkpoint resume, transcript export,
serve intake, MCP, or plane-claim behavior.

Use replay when a host needs to answer: “Given the same bound task, prompt,
model, tools, policy context, and inputs, which ordered model, tool, and
terminal surfaces remain comparable?”

## Contract

The host supplies:

- a schema-v1 `ReplayEvidenceBundle` containing the source identity, task,
  digest bindings, expected ordered step digests, and one expected terminal;
- a schema-v1 `ReplayManifest` that binds the canonical SHA-256 digest of that
  exact evidence bundle.

`ReplayManifest::for_evidence` constructs the manifest after validating the
bundle. `ReplayBindings::for_replay` constructs bindings for the built-in
observation-only replay surface when the host has the exact composed prompt.
Both schemas reject unknown fields and unsupported versions.

Use `workspace_digest` to bind the expected input tree. It hashes sorted
relative paths, lengths, and file content, excludes `.git` administration
state, opens directories and files without following symbolic links, rejects
symbolic links and non-UTF-8 paths, and fails above 10,000 files or 64 MiB.
`empty_workspace_digest` is the explicit binding for an empty directory
workspace.

The evidence bundle contains bounded digests rather than unrestricted model or
tool payloads. Admission rejects common credential-shaped values in the
remaining human-readable task, identity, and tool-name fields. Hosts must still
keep credentials and secrets out of all source evidence they use to construct
the bundle.

## Admission and execution

`Harness::replay` performs these checks before the first model call:

1. Validate schema versions, field bounds, digest syntax, and bundle size.
2. Recompute the evidence and manifest digests.
3. Require the manifest and bundle bindings to match.
4. Materialize a new `directory` or `git-worktree` workspace. Replay rejects
   `inplace` and `directory-inplace`.
5. Recompute the task, composed prompt, selected model, observation-only tool
   catalog, and local policy-context digests.
6. Fail on any changed binding or unverifiable required governance evidence.

Replay mints a new `run_id`. `source_run_id` remains evidence; it is never
passed to ordinary resume. An optional source logical-operation id can preserve
lineage without reusing the source attempt id.

`ReplayRequest::new` retains the isolated workspace by default so callers can
inspect the comparison inputs. Set `keep_workspace = false` to apply ordinary
successful-run cleanup after the terminal checkpoint and artifacts are
captured.

The model binding includes credential-free source configuration: the normalized
HTTP base URL for a direct HTTP model, or a digest of the scripted turn source
for the scripted adapter. API keys and token values are never included.

The policy binding includes the configured maximum turns, timeout, tool
concurrency, effective tool authority, ignore behavior, workspace adapter,
profile, and governance mode. Replay uses `run.timeout_secs`; callers may still
request cooperative cancellation, but cannot supply an unbound per-request
timeout.

## Tool authority

Replay exposes only:

- `read_file`
- `glob`
- `grep`
- `report` as the terminal control and comparison signal

Writes, edits, patches, bash and background jobs, `web_fetch`, todo mutation,
escalation, MCP/external tools, and unknown tools are denied before execution.
Replay also disables lifecycle hooks and harness-managed network egress.
Plain assistant text is not a terminal replay result; completion requires one
valid, exclusive `report` call.

Filesystem read tools still operate inside the normal workspace jail. A
successful replay does not prove that external state, a nondeterministic model,
or an unavailable source artifact was identical.

## Results

`ReplayResult` wraps the ordinary `RunResult` and adds:

- the admitted manifest digest;
- ordered step comparisons;
- one terminal comparison.

Each comparison is `equal`, `changed`, `missing`, or `unsupported`. Natural
language differences are comparative evidence; they do not by themselves mean
that the replay failed or that governance accepted the outcome.

## Export

`Harness::export_replay_inputs` and CLI `replay-export` reconstruct the same
schema-v1 package from retained artifacts, or return `ReplayExportReport`
schema v1 with `complete: false` and a `missing` list. Export is read-only and
does not execute replay.

Original inputs come only from `state/runs/<id>/snapshots/initial`, which is
captured once after first materialize when the source run had
`workspace.snapshot = true`. Resume never creates or recaptures that
directory, even if snapshotting is enabled later.
The live workspace, transcript export, and reconstructed files are not
original-input proof. Content runs, compacted history, non-terminal runs, and unmatched
prompt ids are incomplete. The prompt digest is the current
`SYSTEM_PROMPT` composed with project rules and skills loaded from
`snapshots/initial`. Configured skill packs that live outside that snapshot
make the prompt binding incomplete. Snapshot copies skip symbolic-link
directory entries, open directories with `O_NOFOLLOW`, and open remaining
files without following a swapped symlink. `workspace_digest` opens directories
and files with `O_NOFOLLOW` and rejects symbolic links, so exported inputs
are the retained regular-file tree. Model, policy, and the observation-only tool
catalog come from the exporting process configuration. A complete package is
validated with `ReplayManifest::for_evidence` before it is returned; digests
are never guessed. Checkpoints remain local scratch, not receipts. The
package is digest-only and is never uploaded.

```
shikigami replay-export <run_id> [--json] [-o DIR]
```

`--json` prints `ReplayExportReport`. `-o DIR` writes `manifest.json` and
`evidence.json` only when complete. The command resolves settings without
constructing execution adapters or creating state directories. Exit `0` is a
well-formed report (complete or incomplete). Exit `1` means the run cannot
be inspected. See [ADR 0012](decisions/0012-replay-export.md).

## CLI

```
shikigami replay --manifest FILE --evidence FILE [--resume ID] [--json]
```

`--manifest` and `--evidence` are schema-v1 JSON documents. Reads are bounded
by `MAX_REPLAY_BUNDLE_BYTES`. `--json` prints `ReplayReport` schema v1, whose
`steps` and `terminal` fields match `ReplayResult`. Exit `0` means a comparison
completed (including `changed` evidence). Exit `1` means admission or execution
failed. `--resume` is a replay-attempt id, never the source run. See
[ADR 0010](decisions/0010-cli-run-replay.md).

Model-step digests normalize JSON arguments and omit provider-generated tool
call IDs. Those IDs are used only to associate tool responses inside their own
execution.

## Restart

Replay checkpoints add the manifest digest, source identity, isolated workspace
binding, and comparison cursor. Restart with `ReplayRequest.resume_run_id`
using the replay attempt id and the same manifest and evidence bundle.

A normal `RunRequest.resume_run_id` cannot open a replay checkpoint. Changed
manifest, source, or workspace bindings fail closed. A checkpointed assistant
turn is reconstructed rather than invoking the model again, and an in-doubt
effect is never retried.

Replay disables conversation compaction so the ordered digest evidence remains
append-only until comparison. A matching request for a terminal replay
checkpoint reconstructs the completed result without calling the model or
tools. Terminal outcome durability and finalization are separate checkpoint
phases: recovery waits for any live owner, then idempotently completes artifact
capture, workspace cleanup, event emission, and registry finalization before
returning. Before any recovered artifact or cleanup operation, the checkpoint
workspace is revalidated against the configured run-scoped boundary.

Replay checkpoints also retain cumulative token usage. Scripted-model replay
restores its turn cursor from the durable completed-turn count, so rebuilding a
`Harness` before resume neither repeats earlier script turns nor loses usage and
cost evidence.

## Governance boundary

Replay evidence and checkpoints are harness-local comparative state, not
sekai-chisei receipts. Profiles that require governance fail before model or
tool execution unless their adapter can verify the bound policy and receipt
evidence.

The current sekai-chisei facade does not expose that replay-verification
contract, so required governed replay is reported as unverifiable rather than
falling back to `local` or `none`. Offline deterministic replay remains fully
testable without a live service.

## Related boundaries

- [ADR 0005](decisions/0005-governed-run-replay.md) — accepted architecture
- [ADR 0010](decisions/0010-cli-run-replay.md) — CLI host contract
- [ADR 0012](decisions/0012-replay-export.md) — export from retained artifacts
- [Embedding](embedding.md) — additive `Harness` API
- [Identity](identity.md) — source, replay, and resume identities
- [Runs](runs.md) — local state and cleanup
- [Governed path](governed-path.md) — external governance authority
