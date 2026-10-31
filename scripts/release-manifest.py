#!/usr/bin/env python3
"""Write the family release manifest Tenkai admits for one shikigami tag.

The release workflow runs this after attestation, then attests the manifest
itself, so Tenkai receives a signed record of: component, version, git SHA,
file and image digests, attestation URLs, and gate flags. Images are always
referenced by digest; a `latest` tag is refused.

Usage:
  release-manifest.py --version V --git-sha SHA --dist DIR --out FILE
      [--tag vV] [--image NAME=sha256:...] [--attestation URL] [--label NAME]
  release-manifest.py --self-test
"""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import re
import sys
import tempfile

SCHEMA = "tenkai.family-release-manifest.v1"
COMPONENT = "shikigami"
MANIFEST_NAME = "shikigami-release.json"
# Files in dist that describe the release rather than ship in it.
SKIP_NAMES = {"SHA256SUMS", "RELEASE_NOTES.md", MANIFEST_NAME}
SKIP_SUFFIXES = (".sha256", "-oci.json")
# Merged pull request labels that hold a release for human approval.
GATE_LABELS = {
    "risk:breaking": "breaking",
    "gate:break": "breaking",
    "risk:migration": "migration",
    "gate:mig": "migration",
    "risk:realm": "realm",
    "gate:realm": "realm",
}
VERSION_RE = re.compile(r"[0-9]+\.[0-9]+\.[0-9]+(-(rc|beta)\.[0-9]+)?")
DIGEST_RE = re.compile(r"sha256:[0-9a-f]{64}")


def sha256_file(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def gate_flags(labels: list[str]) -> list[str]:
    return sorted({GATE_LABELS[n] for n in (l.strip().lower() for l in labels) if n in GATE_LABELS})


def parse_image(raw: str) -> dict[str, str]:
    name, _, digest = raw.partition("=")
    if not name or not DIGEST_RE.fullmatch(digest):
        raise SystemExit(f"image must be NAME=sha256:<64 hex>, got {raw}")
    if name.endswith(":latest"):
        raise SystemExit("images must not use a latest tag")
    return {"name": name, "digest": digest}


def build_manifest(
    *,
    version: str,
    git_sha: str,
    tag: str | None,
    dist: pathlib.Path,
    images: list[dict[str, str]],
    attestations: list[str],
    labels: list[str],
) -> dict:
    if not re.fullmatch(r"[0-9a-f]{40}", git_sha):
        raise SystemExit("git_sha must be a full lowercase SHA")
    if not VERSION_RE.fullmatch(version):
        raise SystemExit(f"invalid version: {version}")
    if tag is not None and tag != f"v{version}":
        raise SystemExit(f"tag {tag} does not match version {version}")
    files = [
        {"name": p.name, "digest": sha256_file(p)}
        for p in sorted(dist.iterdir())
        if p.is_file() and p.name not in SKIP_NAMES and not p.name.endswith(SKIP_SUFFIXES)
    ]
    if not files:
        raise SystemExit(f"no release files in {dist}")
    return {
        "schema": SCHEMA,
        "component": COMPONENT,
        "version": version,
        "git_sha": git_sha,
        "tag": tag,
        "binaries": files,
        "images": images,
        "attestations": attestations,
        "gate_flags": gate_flags(labels),
    }


def expect_exit(fn, message: str) -> None:
    try:
        fn()
    except SystemExit as exc:
        assert message in str(exc), exc
    else:
        raise AssertionError(f"expected failure: {message}")


def self_test() -> None:
    assert gate_flags(["risk:breaking", "area:ops", "GATE:mig", "gate:realm", "docs"]) == [
        "breaking",
        "migration",
        "realm",
    ]
    assert gate_flags([]) == []
    expect_exit(lambda: parse_image("ghcr.io/sannrox/shikigami:latest=sha256:" + "a" * 64), "latest")
    expect_exit(lambda: parse_image("ghcr.io/sannrox/shikigami:0.1.0=sha256:abc"), "NAME=sha256")
    with tempfile.TemporaryDirectory() as tmp:
        dist = pathlib.Path(tmp)
        args = dict(version="0.1.0", git_sha="a" * 40, tag="v0.1.0", dist=dist, images=[], attestations=[], labels=[])
        expect_exit(lambda: build_manifest(**args), "no release files")
        (dist / "shikigami-v0.1.0-x86_64-unknown-linux-gnu.tar.gz").write_bytes(b"bin")
        (dist / "shikigami-v0.1.0-x86_64-unknown-linux-gnu.sha256").write_text("skip\n")
        (dist / "shikigami-0.1.0-oci.json").write_text("{}")
        (dist / "shikigami.cdx.json").write_text("{}")
        for name in SKIP_NAMES:
            (dist / name).write_text("skip\n")
        expect_exit(lambda: build_manifest(**{**args, "git_sha": "deadbeef"}), "full lowercase SHA")
        expect_exit(lambda: build_manifest(**{**args, "tag": "v0.2.0"}), "does not match")
        image = parse_image("ghcr.io/sannrox/shikigami:0.1.0=sha256:" + "b" * 64)
        manifest = build_manifest(
            **{**args, "images": [image], "attestations": ["https://github.com/Sannrox/shikigami/attestations/1"], "labels": ["risk:migration"]}
        )
        assert manifest["schema"] == SCHEMA and manifest["component"] == "shikigami"
        assert [b["name"] for b in manifest["binaries"]] == [
            "shikigami-v0.1.0-x86_64-unknown-linux-gnu.tar.gz",
            "shikigami.cdx.json",
        ]
        assert all(DIGEST_RE.fullmatch(b["digest"]) for b in manifest["binaries"])
        assert manifest["images"] == [image]
        assert manifest["gate_flags"] == ["migration"]
        assert "latest" not in json.dumps(manifest)
    print("release-manifest self-test passed")


def main(argv: list[str]) -> None:
    if argv == ["--self-test"]:
        self_test()
        return
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--version", required=True)
    parser.add_argument("--git-sha", required=True)
    parser.add_argument("--tag")
    parser.add_argument("--dist", required=True, type=pathlib.Path)
    parser.add_argument("--out", required=True, type=pathlib.Path)
    parser.add_argument("--image", action="append", default=[])
    parser.add_argument("--attestation", action="append", default=[])
    parser.add_argument("--label", action="append", default=[])
    args = parser.parse_args(argv)
    manifest = build_manifest(
        version=args.version,
        git_sha=args.git_sha,
        tag=args.tag,
        dist=args.dist,
        images=[parse_image(raw) for raw in args.image],
        attestations=args.attestation,
        labels=args.label,
    )
    args.out.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    print(args.out)


if __name__ == "__main__":
    main(sys.argv[1:])
