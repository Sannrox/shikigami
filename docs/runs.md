# Run registry, artifacts, and local control

Every harness run writes host-local operational state under
`$SHIKIGAMI_STATE/runs/<run_id>/` (default:
`~/.shikigami/projects/<encoded-cwd>`; see [cli.md](cli.md)):

~~~
run.json       # status, outcome, report data, model, prompt and preset ids,
               # artifacts, digests, usage, workspace, artifact path
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

Each `model_turn` (and `content_turn`) line names the model that answered it
in its detail, `turn=3 model=<id>`, because an ACP or TUI session can switch
models between prompts of one run (`session/set_model`, `/model`). `run.json`
`model` is the model when the run finished. Older journals show `turn=<n>`
only.

The registry is an operator convenience and crash-recovery aid. A durable
per-run ownership lease prevents another process from resuming an active run;
the lease is refreshed independently while a model or tool call is in progress
and expires only after the owner stops heartbeating. After an abrupt host
death, authorizing-only tool markers may retry the original identity; a
started or completed host effect is in-doubt and is not redispatched; staged
tool reports are replayed without repeating the effect; a checkpointed
terminal `report` completes the same run, with its `data`. Session `ask=park`
keeps the staged tool `Authorizing` and resumes through the same permission
path.

Replay attempts use the same run directory layout and registry lifecycle. Their
checkpoint adds a manifest digest, source identity, isolated workspace binding,
and comparison cursor. That block is recovery metadata only; the immutable
host-supplied replay evidence bundle remains the comparison input. `replay-export` may
recompute that bundle from retained artifacts when `snapshots/initial` and the
other bindings are present; the live workspace is not original-input proof.
See [replay.md](replay.md).

## Artifacts a run produced

A finished `run.json` names what produced the run (`model`, `prompt_id`,
`preset_id`) and what the run produced outside its workspace (`artifacts`).
Each artifact is `{tool, call_id, uri}`: a `resource_link` that an MCP tool
result returned, recorded as the harness observed it. A URI the model writes in
its `report` is not an artifact. The list is kept in the checkpoint, so it
survives a restart. Tools that name no resources leave it empty and the field
absent. A recovered replay or content run also keeps its list. The run result
and the MCP run summary carry the same fields, and governed harvest sends them
too ([harvest.md](harvest.md)).

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

Cancellation controllers write only the durable control marker. They do not
rewrite the owner's `run.json`, so cancellation cannot overwrite a concurrent
terminal transition. Registry reads project `cancel_requested` from the marker
for active runs, including after reopening the registry.

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

Count failures by model, tool, and reason across retained run journals.
The host state root is `$SHIKIGAMI_STATE` when set; otherwise the `state:`
line from `shikigami doctor`:

```sh
state=${SHIKIGAMI_STATE:-$(shikigami doctor | sed -n 's/^[[:space:]]*state:[[:space:]]*//p')}
jq -s '
  [ .[] | select(.event == "edit_outcome") | .edit_outcome
    | select(.outcome != "applied" and .outcome != "applied_normalized") ]
  | group_by([.model, .tool, .outcome])
  | map({model: .[0].model, tool: .[0].tool,
         outcome: .[0].outcome, count: length})
' "$state"/runs/*/events.jsonl
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
for loopback binds to prevent browser-based cross-site task submission. The
HTTP surface admits filesystem-queue work only. Plane-claim intake was
removed ([ADR 0024](decisions/0024-local-guest-rusui-plane-caller.md)).

`POST /runs` accepts a W3C `traceparent` (and optional `tracestate`) header.
The queued run then continues the caller's trace; see
[tracing.md](tracing.md#caller-trace-context).
