# Run registry, artifacts, and local control

Every harness run writes host-local operational state under
$SHIKIGAMI_STATE/runs/<run_id>/:

~~~
run.json       # status, outcome, digests, usage, workspace, artifact path
events.jsonl   # redacted event journal; tool arguments are not persisted
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

## CLI

~~~
shikigami runs
shikigami runs <run-id> --json
shikigami logs <run-id>
shikigami cancel <run-id>
shikigami cleanup <run-id>
shikigami cleanup <run-id> --force
shikigami artifacts <run-id>
shikigami artifacts <run-id> --patch
~~~

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
