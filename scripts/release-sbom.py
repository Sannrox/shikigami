#!/usr/bin/env python3
"""Write a CycloneDX 1.5 SBOM of the crates a shikigami release links.

Components are the non-workspace crates reachable from the `shikigami`
package through normal and build dependencies (not dev-dependencies), for
each release target. The OCI image carries its own BuildKit SBOM attestation.

Usage:
  release-sbom.py OUT
  release-sbom.py --self-test
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
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
    return {
        "bomFormat": "CycloneDX",
        "specVersion": "1.5",
        "version": 1,
        "metadata": {"component": component(root, "application")},
        "components": sorted(components.values(), key=lambda c: c["bom-ref"]),
    }


def self_test() -> None:
    def dep(pkg: str, *kinds: str | None) -> dict:
        return {"pkg": pkg, "dep_kinds": [{"kind": k, "target": None} for k in kinds]}

    def meta(extra_dep: str) -> dict:
        root, types = "path+file:///w#shikigami@3.0.0", "path+file:///w/crates/shikigami-types#3.0.0"
        packages = [
            {"id": root, "name": "shikigami", "version": "3.0.0", "license": "Apache-2.0"},
            {"id": types, "name": "shikigami-types", "version": "3.0.0"},
            {"id": "r#serde@1.0.0", "name": "serde", "version": "1.0.0", "license": "MIT OR Apache-2.0"},
            {"id": "r#memchr@2.0.0", "name": "memchr", "version": "2.0.0", "license": "MIT/Unlicense"},
            {"id": "r#cc@1.0.0", "name": "cc", "version": "1.0.0", "license": "MIT"},
            {"id": "r#proptest@1.0.0", "name": "proptest", "version": "1.0.0", "license": "MIT"},
            {"id": "r#windows@1.0.0", "name": "windows", "version": "1.0.0", "license": "MIT"},
            {"id": "r#unused@1.0.0", "name": "unused", "version": "1.0.0", "license": "MIT"},
        ]
        nodes = [
            {"id": root, "deps": [dep(types, None), dep("r#cc@1.0.0", "build"), dep("r#proptest@1.0.0", "dev"), dep(extra_dep, None)]},
            {"id": types, "deps": [dep("r#serde@1.0.0", None, "dev")]},
        ] + [{"id": p["id"], "deps": []} for p in packages[2:]]
        return {"workspace_members": [root, types], "packages": packages, "resolve": {"nodes": nodes}}

    out = bom([meta("r#memchr@2.0.0"), meta("r#windows@1.0.0")])
    assert out["metadata"]["component"]["name"] == "shikigami"
    by_ref = {c["bom-ref"]: c for c in out["components"]}
    assert set(by_ref) == {
        "pkg:cargo/serde@1.0.0",
        "pkg:cargo/memchr@2.0.0",
        "pkg:cargo/cc@1.0.0",
        "pkg:cargo/windows@1.0.0",
    }, set(by_ref)
    assert by_ref["pkg:cargo/serde@1.0.0"]["licenses"] == [{"expression": "MIT OR Apache-2.0"}]
    assert by_ref["pkg:cargo/memchr@2.0.0"]["licenses"] == [{"expression": "MIT OR Unlicense"}]
    try:
        bom([{"workspace_members": [], "packages": [], "resolve": {"nodes": []}}])
    except SystemExit as exc:
        assert "shikigami package" in str(exc)
    else:
        raise AssertionError("expected missing shikigami package to fail")
    print("release-sbom self-test passed")


def main(argv: list[str]) -> None:
    if argv == ["--self-test"]:
        self_test()
        return
    if len(argv) != 1:
        raise SystemExit("usage: release-sbom.py OUT")
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
