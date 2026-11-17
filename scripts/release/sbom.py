#!/usr/bin/env python3
"""Write a CycloneDX 1.5 SBOM of the crates a shikigami release links.

Components are the non-workspace crates reachable from the `shikigami`
package through normal and build dependencies (not dev-dependencies), for
each release target. The OCI image carries its own BuildKit SBOM attestation.

Usage:
  sbom.py OUT
"""

from __future__ import annotations

import json
import subprocess
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ROOT_PACKAGE = "shikigami"
# The release workflow's binary targets.
TARGETS = (
    "aarch64-apple-darwin",
    "x86_64-apple-darwin",
    "x86_64-unknown-linux-gnu",
    "aarch64-unknown-linux-gnu",
)


# Cargo license strings are not guaranteed SPDX IDs, so emit them as an
# expression (a free string in CycloneDX), with the legacy "/" as " OR ".
def license_entry(raw: str) -> dict:
    return {"expression": " OR ".join(part.strip() for part in raw.split("/"))}


def component(pkg: dict, kind: str) -> dict:
    purl = f"pkg:cargo/{pkg['name']}@{pkg['version']}"
    out = {"type": kind, "bom-ref": purl, "name": pkg["name"], "version": pkg["version"], "purl": purl}
    if kind == "library" and pkg.get("license"):
        out["licenses"] = [license_entry(pkg["license"])]
    return out


def linked(meta: dict) -> tuple[dict, list[dict]]:
    """Root package and the non-workspace packages it links, from one
    `cargo metadata` run (already filtered to one platform)."""
    packages = {pkg["id"]: pkg for pkg in meta["packages"]}
    workspace = set(meta["workspace_members"])
    root = next((i for i in workspace if packages[i]["name"] == ROOT_PACKAGE), None)
    if root is None:
        raise SystemExit(f"cargo metadata did not include the {ROOT_PACKAGE} package")
    deps = {node["id"]: node["deps"] for node in meta["resolve"]["nodes"]}
    seen, todo = {root}, [root]
    while todo:
        for dep in deps[todo.pop()]:
            if dep["pkg"] not in seen and any(k["kind"] in (None, "build") for k in dep["dep_kinds"]):
                seen.add(dep["pkg"])
                todo.append(dep["pkg"])
    return packages[root], [packages[i] for i in seen if i not in workspace]


def bom(metas: list[dict]) -> dict:
    components: dict[str, dict] = {}
    for meta in metas:
        root, libs = linked(meta)
        for lib in libs:
            entry = component(lib, "library")
            components[entry["bom-ref"]] = entry
    document = {
        "bomFormat": "CycloneDX",
        "specVersion": "1.5",
        "version": 1,
        "metadata": {"component": component(root, "application")},
        "components": sorted(components.values(), key=lambda c: c["bom-ref"]),
    }

    canonical = json.dumps(document, sort_keys=True, separators=(",", ":"))
    document["serialNumber"] = uuid.uuid5(uuid.NAMESPACE_URL, canonical).urn
    return document


def main(argv: list[str]) -> None:
    if len(argv) != 1:
        raise SystemExit("usage: sbom.py OUT")
    metas = [
        json.loads(
            subprocess.check_output(
                ["cargo", "metadata", "--locked", "--format-version", "1", "--filter-platform", target],
                cwd=ROOT,
                text=True,
            )
        )
        for target in TARGETS
    ]
    out = Path(argv[0])
    out.write_text(json.dumps(bom(metas), indent=2) + "\n")
    print(out)


if __name__ == "__main__":
    main(sys.argv[1:])
