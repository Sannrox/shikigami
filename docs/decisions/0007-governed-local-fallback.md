# ADR 0007: Governed local-model fallback

- Status: Accepted
- Date: 2026-08-25
- Supersedes: —
- Resolves: [#233](https://github.com/Sannrox/shikigami/issues/233)
- Discussion: [Design: governed local-model fallback authorization](https://github.com/Sannrox/shikigami/discussions/240)

## Context

A governed worker that loses connectivity today fails closed. That is correct
when no current grant exists. It is insufficient when governance has already
issued a bounded, content-bound permission to use one local model digest under
a live lease and fence.

Treating an on-disk model artifact as permission would move admission into the
executor. Waiting on a delivery connectivity-class workflow would confuse
artifact placement with grant verification (ADR 0001: delivery is not a
runtime port).

## Decision

1. **Typed grant.** Fallback consumes `FallbackAuthorization` schema version 1:
   stable identities, lease/fence, policy revision, model/prompt/tool digests,
   validity window, revocation, signature. Unknown fields deny.
2. **Ownership.** Governance issues grants, leases, fences, and receipts.
   Shikigami verifies and executes. Delivery places bytes and never appears in
   harness process settings.
3. **Fail closed.** Missing, stale, expired, mismatched, revoked, or
   unverifiable evidence denies before a local model call. Installation is not
   a grant. Shikigami never mints `accepted` reconciliation.
4. **Connected path unchanged.** When plane planning succeeds, fallback is
   ignored. `none` / `local` ungoverned paths do not consume fallback grants.
5. **Opt-in settings.** Additive `[model.fallback]` (`enabled` default false,
   optional `adapter`, `artifact_path`, `script_json`). No delivery-system keys.
6. **Scratch, not authority.** Selection, held fence, and evidence identities
   persist on the existing governance checkpoint. Resume re-validates. Duplicate
   payloads are idempotent; conflicting payloads stay `conflicted`.
7. **Signatures.** `test-hmac-sha256` is a fixture verifier gated by
   `model.fallback.allow_test_signatures` (default false). Unknown algorithms
   fail closed until governance publishes a production verifier.

## Consequences

- Deterministic tests inject a signed envelope and a local digest; no live
  plane or installer is required.
- Production fallback stays denied until a verifiable governance-issued
  envelope exists and the local digest matches.
- Delayed-evidence spooling remains Issue #234.

## Rejected alternatives

1. Treat a locally installed model as sufficient governed offline work.
2. Wait for delivery connectivity-class upgrades before defining admission.
3. Have Shikigami mint leases or receipts while the plane is down.
4. Replace the configured governance adapter with `local` during outage.
