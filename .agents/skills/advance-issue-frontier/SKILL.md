---
name: advance-issue-frontier
description: Advance the sekai-chisei GitHub Issue frontier after work merges or when asked what is ready next. Use to evaluate dependency-linked open Issues, update blocked or ready status, enforce active-lane limits, and recommend the next deliverable without implementing it.
---

# Advance Issue Frontier

Compute the delivery frontier from live GitHub state. A frontier Issue is open,
has no unresolved dependency, and has no implementation already in flight.

## Establish authority and scope

Default to **report-only**. Modify Issue state only when the user explicitly
asks to update or advance the frontier.

Resolve:

- the repository and the relevant Issue set;
- whether a merge, closure, or full backlog review triggered the run;
- the active-lane limit, defaulting to three and accepting a documented limit
  of two or three;
- the permitted mutation: none, labels, or the documented body fallback.

Read the project operating system and inspect live Issues and Pull Requests.
Do not rely on a stale local backlog export.

## Build the dependency graph

1. Fetch open backlog Issues and implementation Pull Requests. Include recently
   closed predecessors needed to evaluate dependencies.
2. Parse dependencies only from each Issue's `## GitHub dependencies` section.
   Resolve referenced Issues in the same repository unless the text explicitly
   names another repository.
3. Treat a referenced closed Issue as delivered only when its closure state is
   consistent with the dependency wording. If it was closed as unplanned or
   superseded, require evidence that the dependent outcome remains valid.
4. Treat textual gates such as an active predecessor or external decision as
   blocking until the Issue explicitly records them as delivered.
5. Detect missing references, contradictory status, self-dependencies, and
   cycles. Keep affected Issues blocked and report the anomaly rather than
   guessing.

Never infer a dependency from ordinary prose, issue numbering, milestones, or
similar subject matter.

## Compute the frontier

For each open Issue, classify it as:

- **blocked**: at least one dependency is unresolved or ambiguous;
- **ready**: every dependency is delivered and no implementation is in flight;
- **active**: an implementation Pull Request is open or ownership is explicitly
  recorded under the project's workflow;
- **anomalous**: the dependency graph cannot be evaluated safely.

Count active Issues before recommending more work. Recommend no more candidates
than the remaining active-lane capacity. When several Issues are equally ready,
order the report by downstream-unblocking depth and then Issue number. Call this
a deterministic presentation order, not project priority.

Parallel candidates must not depend on each other or visibly collide on the
same contract, migration, or ownership boundary. Report possible collisions for
maintainer judgment.

## Apply authorized status changes

When mutation is authorized:

1. Prefer one existing repository status label such as `status:ready` or
   `status:blocked`. Do not create a new label taxonomy implicitly.
2. If status labels are unavailable and the operating system permits the
   fallback, update exactly one `Workflow status:` line in the Issue body while
   preserving all other content.
3. Update only Issues whose computed state changed. Avoid status comments that
   add notification noise without becoming the source of truth.
4. Re-read changed Issues to confirm the intended state and dependency section
   survived intact.

Readiness does not authorize assignment, implementation, closure, milestone
changes, or priority changes. Never mark an Issue active merely because a lane
is available.

## Report the frontier

Return:

- trigger and mutation authority;
- newly unblocked Issues;
- active Issues and remaining lane capacity;
- still-blocked Issues with their unresolved dependencies;
- anomalous Issues and the exact evidence needed to resolve them;
- recommended candidates in deterministic presentation order;
- every Issue mutation performed.

If nothing changed, say so without manufacturing work.

## Boundaries

- Report only unless Issue mutation was explicitly authorized.
- Do not create or close Issues, start branches, open Pull Requests, or merge.
- Do not assign contributors or invent priority, deadlines, or milestones.
- Do not silently repair dependency text or choose between conflicting sources.
- Do not exceed the active-lane limit when recommending simultaneous delivery.
