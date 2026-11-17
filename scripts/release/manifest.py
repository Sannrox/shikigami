#!/usr/bin/env python3
"""Write the family release manifest Tenkai admits for one shikigami tag.

The release workflow runs this after attestation, then attests the manifest
itself, so Tenkai receives a signed record of: component, version, git SHA,
file and image digests, attestation URLs, and gate flags. Images are always
referenced by digest; a `latest` tag is refused.

Gate flags come from the labels of the pull requests squashed into the
release range, read with git in the current checkout and gh (GH_TOKEN).

Usage:
  manifest.py --version V --git-sha SHA --dist DIR --out FILE
      [--tag vV] [--image NAME=sha256:...] [--attestation URL] [--notes FILE]
"""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import re
import subprocess
import sys

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
# A release declares a break in its notes (a Breaking or Upgrade heading) or
# in a squashed Conventional Commit subject (`type!:` / `type(scope)!:`).
BREAK_HEADING_RE = re.compile(r"^#{2,4}[ \t]+(breaking|upgrade)\b", re.I | re.M)
BREAK_SUBJECT_RE = re.compile(r"^(?:[0-9a-f]{7,40} )?[a-z]+(?:\([^)]*\))?!:")
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


def declares_break(notes: str, subjects: list[str]) -> bool:
    return bool(BREAK_HEADING_RE.search(notes)) or any(BREAK_SUBJECT_RE.match(s) for s in subjects)


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
    notes: str = "",
    subjects: list[str] | None = None,
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
    flags = gate_flags(labels)
    # Fail closed: a documented break the manifest omits would let an
    # automated delivery policy admit it without human approval (#514).
    if "breaking" not in flags and declares_break(notes, subjects or []):
        raise SystemExit(
            "release declares a breaking change but no merged pull request carries "
            "risk:breaking or gate:break; label the pull request, then re-run"
        )
    return {
        "schema": SCHEMA,
        "component": COMPONENT,
        "version": version,
        "git_sha": git_sha,
        "tag": tag,
        "binaries": files,
        "images": images,
        "attestations": attestations,
        "gate_flags": flags,
    }


# Squashed pull request subjects end in "(#N)".
PR_RE = re.compile(r"\(#([0-9]+)\)$")


def git(*args: str) -> str:
    return subprocess.run(["git", *args], check=True, capture_output=True, text=True).stdout


def release_range(git_sha: str) -> str:
    """prev..sha, where prev is the nearest final (non-pre-release) ancestor
    tag, so a final release also carries gates first seen in its release
    candidates. Without a final tag, start at the first pull request: the
    commits before it are the repository bootstrap and carry no labels."""
    try:
        prev = git("describe", "--tags", "--abbrev=0", "--match", "v*", "--exclude", "*-*", f"{git_sha}^").strip()
    except subprocess.CalledProcessError:
        first = next(
            (line.split(" ", 1)[0] for line in git("log", "--reverse", "--format=%H %s", git_sha).splitlines() if PR_RE.search(line)),
            None,
        )
        if first is None:
            raise SystemExit(f"no pull request commit before {git_sha}")
        prev = f"{first}^"
    return f"{prev}..{git_sha}"


def pr_numbers(subjects: list[str]) -> list[int]:
    """Fail closed: a commit with no pull request would drop its gate."""
    unattributed = [s for s in subjects if not PR_RE.search(s)]
    if unattributed:
        raise SystemExit("commits without a pull request reference:\n" + "\n".join(unattributed))
    return sorted({int(PR_RE.search(s).group(1)) for s in subjects})


def gate_inputs(git_sha: str) -> tuple[list[str], list[str]]:
    """Subjects of the release range and the labels of every pull request
    squashed into it, whatever branch it merged into. Any lookup error fails."""
    subjects = git("log", "--format=%h %s", release_range(git_sha)).splitlines()
    labels: list[str] = []
    for pr in pr_numbers(subjects):
        result = subprocess.run(
            ["gh", "pr", "view", str(pr), "--json", "labels", "--jq", ".labels[].name"],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            raise SystemExit(f"cannot read labels of pull request #{pr}: {result.stderr.strip()}")
        labels += [name for name in result.stdout.splitlines() if name]
    return subjects, labels


def main(argv: list[str]) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--version", required=True)
    parser.add_argument("--git-sha", required=True)
    parser.add_argument("--tag")
    parser.add_argument("--dist", required=True, type=pathlib.Path)
    parser.add_argument("--out", required=True, type=pathlib.Path)
    parser.add_argument("--image", action="append", default=[])
    parser.add_argument("--attestation", action="append", default=[])
    parser.add_argument("--notes", type=pathlib.Path, help="release notes as published")
    args = parser.parse_args(argv)
    subjects, labels = gate_inputs(args.git_sha)
    manifest = build_manifest(
        version=args.version,
        git_sha=args.git_sha,
        tag=args.tag,
        dist=args.dist,
        images=[parse_image(raw) for raw in args.image],
        attestations=args.attestation,
        labels=labels,
        notes=args.notes.read_text() if args.notes else "",
        subjects=subjects,
    )
    args.out.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    print(args.out)


if __name__ == "__main__":
    main(sys.argv[1:])
