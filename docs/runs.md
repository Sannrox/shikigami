# Run registry, artifacts, and local control

Every harness run writes host-local operational state under
$SHIKIGAMI_STATE/runs/<run_id>/:

~~~
run.json       # status, outcome, digests, usage, workspace, artifact path
events.jsonl   # redacted event journal; tool arguments are not persisted
checkpoint.json # resumable conversation plus optional replay binding
snapshots/
  initial/     # original workspace copy when workspace.snapshot is true
cancel         # presence requests cooperative cancellation
artifacts/
  baseline.json # hash-only workspace baseline used to scope changes
  manifest.json
  diff.patch   # only when the workspace is a git worktree with a bounded diff
~~~

The registry is an operator convenience and crash-recovery aid. Governed
operation truth, policy, leases, budgets, and retry limits remain owned by
sekai-chisei. A durable per-run ownership lease prevents another process from
resuming an active run; the lease is refreshed independently while a model or
tool call is in progress and expires only after the owner stops heartbeating.
Local checkpoints are not governed receipts. After an abrupt host death,
authorizing-only tool markers may retry the original identity; a started or
completed host effect is in-doubt and is not redispatched; staged tool reports
are replayed without repeating the effect; a checkpointed terminal `report`
completes the same run. A `require_approval` park keeps the staged tool
`Authorizing`, records the plane approval identity, and resumes without
`--answer`; the next wake re-validates current authority. The process-kill
matrix lives in `tests/governed_tool_crash_recovery.rs`.

Replay attempts use the same run directory layout and registry lifecycle. Their
checkpoint adds a manifest digest, source identity, isolated workspace binding,
and comparison cursor. That block is recovery metadata only; the immutable
host-supplied replay evidence bundle remains the comparison input, and the
governance plane remains authoritative when used. `replay-export` may
recompute that bundle from retained artifacts when `snapshots/initial` and the
other bindings are present; the live workspace is not original-input proof.
See [replay.md](replay.md).

## CLI

~~~
shikigami runs
shikigami runs <run-id>
shikigami runs <run-id> --json
shikigami runs <run-id> --diagnose
shikigami runs <run-id> --diagnose --json
shikigami logs <run-id>
shikigami cancel <run-id>
shikigami cleanup <run-id>
shikigami cleanup <run-id> --force
shikigami artifacts <run-id>
shikigami artifacts <run-id> --patch
shikigami replay-export <run-id> [--json] [-o DIR]
~~~

Inspecting one run also prints a read-only recovery diagnosis ([ADR 0008](decisions/0008-recovery-diagnosis.md)): `safe_resume`, `report_only`, `uncertain_tool`, `invalid_checkpoint`, or `terminal`, plus allowed next-step categories. `--diagnose --json` emits the same `RecoveryDiagnosis` object as `Harness::diagnose_run`. Diagnosis does not call the model, dispatch tools, redeem permits, or mutate state, and it does not grant execution authority. `runs` and `runs <id>` (with or without `--diagnose`) use a read-only registry view and do not create `runs/` or `run-controls/` when they are absent. `runs <id> --json` without `--diagnose` remains the existing `RunRecord` document.

cleanup removes the run record, event journal, checkpoint, and retained
artifact directory. Active runs are never deleted in place: --force requests
cancellation through a marker outside the run directory and returns a conflict;
retry cleanup after the run reaches a terminal state.

The artifact manifest contains bounded file metadata, SHA-256 hashes, and
added/modified/deleted paths relative to an optional initial workspace
snapshot. The hash-only baseline is also used to exclude pre-existing dirty or
untracked files from the retained patch. File contents are not copied into the
manifest. The manifest and patch are retained even when a successful run
removes its temporary workspace.

## Edit tools and outcomes

| Tool | Use |
| --- | --- |
| `edit` | One unique `old`/`new` replacement in a file. |
| `multi_edit` | Several unique replacements in one file, computed before writing. |
| `apply_patch` | Replacements with optional surrounding context, computed across files before writing. |

`edit` and `multi_edit` try exact matching first. A unique exact match succeeds;
multiple exact matches fail without trying normalization. Only zero exact matches
allow a unique normalized match. This supersedes the exact-only policy in #79
as accepted in #415. The fixed normalization set is:

- Ignore trailing whitespace on each line except CR.
- Map U+2018–U+201B and U+201C–U+201F to ASCII single and double quotes.
- Map U+2010–U+2015 and U+2212 to ASCII hyphen.
- Map U+00A0, U+1680, U+2000–U+200A, U+202F, U+205F, and U+3000 to space.

There is no case folding, indentation normalization, NFKC, or fuzzy matching.
Normalized ambiguity fails closed, including overlapping candidates; its
`match_count` is 2 (at least two), because searching stops at ambiguity.
A normalized replacement changes only its matched original span and reports
`normalized match` in the tool response. Unmatched lines retain their bytes.
`apply_patch` remains exact, including its context.

Consistently CRLF
files accept LF or CRLF edit fragments and retain CRLF when written, including
untouched lines. Mixed-ending files are matched byte-for-byte without line-ending
normalization. Path and plan-jail permissions still apply. A failed match writes
nothing. For `multi_edit` and `apply_patch`, each `old` is matched against the
file as read. All spans are located before replacements are assembled in one
pass, so request order does not change the result. Patch context belongs to the
span: overlapping or nested spans fail with both hunk indexes (zero-based)
and a suggestion to merge nearby changes into one hunk. Adjacent spans succeed.
Chained edits that depend on newly inserted text must be merged into one hunk;
these batches now fail without writing. Files in `apply_patch` remain independent.

Each completed edit-tool execution attempt emits an argument-free
`edit_outcome` journal record. `edit_outcome` contains `tool`, `model`,
`outcome`, and an optional `match_count` for match failures. Outcomes are
`applied`, `applied_normalized`, `no_match`, `ambiguous`, `overlap`, `invalid_input`, `limit`, and `io`.
`model` is the configured effective model alias, including `auto` when routing
is delegated; it is not a claim about the provider model chosen by the plane.
No path, `old`, `new`, context, tool arguments, or error text is stored in this
metadata. Authorization denials and parked calls that have not executed remain
in the existing lifecycle events rather than counting as matcher failures.
Events are best-effort operational observations, not governed receipts.

Count failures by model, tool, and reason across retained run journals:

```sh
jq -s '
  [ .[] | select(.event == "edit_outcome") | .edit_outcome
    | select(.outcome != "applied" and .outcome != "applied_normalized") ]
  | group_by([.model, .tool, .outcome])
  | map({model: .[0].model, tool: .[0].tool,
         outcome: .[0].outcome, count: length})
' "${SHIKIGAMI_STATE:-.shikigami-state}"/runs/*/events.jsonl
```

Compatibility: journal schema v1 gains the optional `edit_outcome` object and
new event name; older records omit it. Readers should ignore unknown event
names and optional fields. Live `HarnessEvent` consumers must handle the
additive `EditOutcome` variant. `ToolError` now distinguishes structured
`ApplyPatchMatch`, `ApplyPatchLimit`, and `EditOverlap` variants from invalid patch input.

## HTTP control and intake

Filesystem serve can expose a small authenticated operator surface:

~~~
export SHIKIGAMI_SERVE_TOKEN="$(openssl rand -hex 32)"
shikigami serve \
  --listen 127.0.0.1:8080 \
  --auth-token-env SHIKIGAMI_SERVE_TOKEN \
  --concurrency 4 \
  --queue-capacity 256 \
  --retry-limit 1
~~~

Routes:

| Method | Path | Purpose |
| --- | --- | --- |
| GET | /healthz | Queue health snapshot |
| GET | /metrics | Prometheus text aggregate |
| GET | /runs | Recent run records |
| GET | /runs/<id> | One run record |
| GET | /runs/<id>/events | Redacted JSONL journal |
| POST | /runs | Authenticated filesystem queue admission |
| POST | /runs/<id>/cancel | Durable cancellation request |
| POST | /runs/<id>/cleanup[?force=1] | Terminal record cleanup |

Use Authorization: Bearer <token> on every request. Tokens are required even
for loopback binds to prevent browser-based cross-site task submission. The HTTP surface does not admit governed work or
override governance; plane intake remains the explicit --intake plane path.
