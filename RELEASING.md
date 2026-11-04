# Releasing shikigami

Family versioning, tagging, and release policy:
[tenkai ADR 0040](https://github.com/Sannrox/tenkai/blob/main/docs/decisions/0040-family-versioning-and-release-policy.md).
Changelog and version preparation: [CONTRIBUTING.md](CONTRIBUTING.md#releasing).
This page is how a maintainer tags this repository.

## Tag

Agents never push `v*` tags. After the release pull request lands, the
maintainer creates an annotated tag on the merged commit, signed with a key
listed in `security/release-tag-fingerprints` (public key in
`security/release-tag-key.asc`):

```bash
git tag -s vX.Y.Z -m vX.Y.Z
git push origin vX.Y.Z
```

A tag is never moved, deleted, or reused; a bad release gets a new patch
version. The tagged commit must be on `main`. `bash scripts/validate-release.sh`
checks the release path without publishing.

## Release candidates

Cut `vX.Y.Z-rc.N` (or `-beta.N`) before a final tag. The rc's release pull
request sets the `[workspace.package]` version and the README status stamp to
`X.Y.Z-rc.N` and adds a dated `## [X.Y.Z-rc.N]` section to `CHANGELOG.md`.
The final `vX.Y.Z` needs its own section; it never reuses the rc notes.

A tag containing `-` publishes as a GitHub pre-release and is never marked
latest. It ships binaries only, no OCI image. A failed release candidate costs
only an rc number. A final tag on a commit whose version is still an rc is
refused.

## Start, retry, and dry-run

The tag push runs `.github/workflows/release-dispatch.yml`, which does only
this:

```bash
gh workflow run release.yml --ref main -f tag=vX.Y.Z
```

So a release always runs **main's** `release.yml` and verifier, never the
copies at the tagged commit. `release.yml` refuses a tagged run that does not
start from `main`.

To retry a failed release, fix the workflow on `main` and run the same
command for the same tag. A retry picks up fixes to `release.yml` and to the
tag verifier. Everything else (the release scripts, `CHANGELOG.md`, the
Dockerfile, and the sources) comes from the tagged commit, so a bug there needs
a new rc or patch version. A re-run resumes the draft release and replaces its
assets. Every job that uploads, pushes the image, or publishes refuses a
release that is already published, so neither a new run nor "Re-run failed
jobs" on an old one can change it. Only one run per tag executes at a time.

`gh workflow run release.yml --ref <branch>` without a tag is a dry run: it
resolves that commit and runs the OCI image smoke, and never touches a GitHub
Release, the registry, or attestations.

## What the tag runs

`.github/workflows/release.yml`, from `main`:

1. Checks out the tag and fetches the annotated tag object. Main's
   `scripts/verify-release-tag.sh` refuses a lightweight or unsigned tag, a
   signer outside the fingerprint list, an expired or revoked key, or a tag
   that is not the tagged commit's `[workspace.package]` version. The key and
   fingerprint list are read from `main`, so changing them takes a reviewed
   pull request. The tagged commit must be on `main`.
2. Every later job checks out that commit and confirms HEAD is that commit.
   The binaries, the image tags, labels and `VCS_REF`, the gate flags and the
   manifest `git_sha` all come from it.
3. Creates a draft GitHub Release with `gh release create <tag> --verify-tag`,
   or resumes the draft an earlier run left and drops its assets. The body is the matching
   `CHANGELOG.md` section.
4. Builds and uploads the binaries. For a stable version it also publishes
   the OCI image by version (never `latest`) with BuildKit provenance and
   SBOM attestations.
5. Writes a CycloneDX SBOM of the linked crates (`shikigami.cdx.json`) and
   attests provenance and the SBOM for the binary archives.
6. Writes `shikigami-release.json`, the family release manifest: component,
   version, git SHA, archive and image digests, attestation URLs, and gate
   flags (`risk:breaking`, `risk:migration`, `risk:realm`, or the matching
   `gate:*` labels on every pull request squashed in since the previous final
   tag). The manifest gets its own provenance attestation, which is its
   signature for Tenkai.
7. Attaches the SBOM and manifest, then publishes the release, as a
   pre-release for a tag containing `-`.

## Provenance

Attestations record the workflow that built the artifacts, which is main's
`release.yml` at `refs/heads/main`, not the tag. The signed tag and the
manifest `git_sha` (the tagged commit) pin the source. The `v*` tag ruleset
from the family policy (create limited to maintainers, no update or delete)
keeps a tag pointing at the commit that was signed.

Verify a downloaded file with
`gh attestation verify <file> --repo Sannrox/shikigami`.
