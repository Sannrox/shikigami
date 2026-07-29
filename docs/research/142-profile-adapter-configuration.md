# Profile and adapter configuration semantics

Research closeout for
[#142](https://github.com/Sannrox/shikigami/issues/142), accepted 2026-07-29.
Future-schema design proposal:
[#146](https://github.com/Sannrox/shikigami/discussions/146).

## Recommendation

Select option 4: **deprecate behavioral profiles in a future settings schema**.

Settings version 1 remains unchanged. For new version-1 configurations,
authors should use the existing explicit local and governed examples and set
governance adapter, model adapter, and `governance.fail_closed` directly.

In a future schema:

- adapters and `governance.fail_closed` become the sole behavioral authority;
- named local and governed files remain copyable recipes, not runtime presets;
- a retained profile-like field, if any, is descriptive metadata only; and
- governed recipes explicitly select `sekai-chisei`, model `plane`, and
  fail-closed behavior.

## Current resolution behavior

Version 1 resolves defaults, the selected file, environment variables, and
available CLI flags in that order. Profile expansion complicates that summary:
the file profile is expanded while loading and again before environment
application; an environment profile is expanded when applied; explicit adapter
environment variables are applied afterward.

| Starting configuration | Overrides | Effective profile | Governance | Model | Fail-closed |
| --- | --- | --- | --- | --- | --- |
| Defaults | none | `local` | `none` | `scripted` | no |
| File contains only profile `governed` | none | `governed` | `sekai-chisei` | `plane` | yes |
| File contains profile `governed`, governance `local`, model `scripted` | none | `governed` | `local` | `plane` | yes |
| Explicit governed example | `SHIKIGAMI_PROFILE=local` | `local` | `sekai-chisei` | `plane` | yes |
| Explicit local example | profile `governed`, then governance `local` and model `scripted` from environment | `governed` | `local` | `scripted` | yes |
| Any explicit configuration | custom profile name | custom label | unchanged | unchanged | unchanged |

The fourth row demonstrates that later profile precedence changes the label but
does not undo values expanded from the earlier file profile. The fifth row
demonstrates that `governed` is not a complete deployment recipe: later adapter
overrides can replace its selected adapters while its fail-closed effect
remains. The third row shows that an explicit `model = "scripted"` is
indistinguishable from the default during preset expansion and is replaced by
`plane`.

No CLI flags currently override profile, governance adapter, model adapter, or
fail-closed behavior directly.

## Evidence

- Both repository examples specify profile, adapters, and fail-closed behavior
  explicitly. They do not need preset expansion to communicate their intent.
- Runtime and integration tests generally construct adapters explicitly.
  Dedicated tests prove the profile-only governed expansion and fail-closed
  behavior.
- CI does not rely on profile environment overrides.
- No external deployment configuration or owner commitment was available in
  the repository or issue. The compatibility plan therefore assumes private
  version-1 consumers may rely on every currently accepted combination.
- `doctor` reports the effective profile, adapters, fail-closed governance
  detail, and configuration file, but does not attribute each value to the
  source that produced it.

## Options considered

| Option | Decision | Reason |
| --- | --- | --- |
| Keep behavioral presets and improve visibility | Rejected | Better diagnostics would not remove the multiple behavioral authorities or the asymmetric environment behavior. |
| Make profiles complete deployment recipes | Rejected | It conflicts with active explicit alternative adapters and would reject combinations accepted by version 1. |
| Make profiles descriptive labels immediately | Rejected for version 1 | This is the desired end state, but applying it in place could silently remove fail-closed behavior. |
| Deprecate profiles in a future schema | **Selected** | It preserves version-1 compatibility while giving a future schema one explicit behavioral authority. |

## Before and after

Version 1 shorthand remains valid:

```toml
version = 1

[profile]
name = "governed"
```

The proposed future recipe is explicit:

```toml
version = 2

[governance]
adapter = "sekai-chisei"
fail_closed = true

[model]
adapter = "plane"
```

Endpoint, principal, namespace, credentials, workspace, tools, events, and
model selection remain explicit settings.

## Compatibility and migration

1. Preserve version-1 parsing, profile expansion, environment ordering, and
   fail-closed behavior exactly.
2. Do not auto-rewrite a version-1 file by deleting its profile.
3. A future migrator must resolve the version-1 configuration with the old
   algorithm and write the complete effective adapter and fail-closed fields.
4. Migration must never silently reduce governance authority. Ambiguous or
   unsupported combinations must require an explicit operator choice.
5. Keep `doctor` available to inspect version-1 effective wiring throughout any
   deprecation period.
6. Treat a settings-version change and migrator as a separately shaped,
   reviewed implementation after Discussion #146 reaches an accepted design.

## Outcome and scope

There is **no runtime implementation action for settings version 1**. This
closeout changes authoring guidance, documents the existing resolution matrix,
and opens the required future-schema Design Discussion. It does not create a
version-2 implementation issue before that design is accepted.

No API, settings parser, runtime, governance, persistence, migration, or
security behavior changes in this closeout.
