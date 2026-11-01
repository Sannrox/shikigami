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
version. `bash scripts/validate-release.sh` checks the release path without
publishing.

## What the tag runs

`.github/workflows/release.yml`:

1. `scripts/verify-release-tag.sh` refuses a lightweight or unsigned tag, a
   signer outside the fingerprint list, an expired or revoked key, a commit
   that is not on `main` or a `release/v*` branch, or a tag that is not the
   `[workspace.package]` version. The key and fingerprint list are read from
   `main`, so changing them takes a reviewed pull request.
2. Creates a draft GitHub Release whose body is the matching `CHANGELOG.md`
   section, and refuses to run if that release is already published.
3. Builds and uploads the binaries. For a stable version it also publishes
   the OCI image by version (never `latest`) with BuildKit provenance and
   SBOM attestations; pre-releases (`-rc.N`, `-beta.N`) ship binaries only.
4. Writes a CycloneDX SBOM of the linked crates (`shikigami.cdx.json`) and
   attests provenance and the SBOM for the binary archives.
5. Writes `shikigami-release.json`, the family release manifest: component,
   version, git SHA, archive and image digests, attestation URLs, and gate
   flags (`risk:breaking`, `risk:migration`, `risk:realm`, or the matching
   `gate:*` labels on every pull request squashed in since the previous final
   tag). The manifest gets its own provenance attestation, which is its
   signature for Tenkai.
6. Attaches the SBOM and manifest, then publishes the release.

The workflow and the verifier run from the tagged commit, so the `v*` tag
ruleset from the family policy (create limited to maintainers, no update or
delete) is what keeps an unreviewed commit from being released.

Verify a downloaded file with
`gh attestation verify <file> --repo Sannrox/shikigami`.
