import unittest
import uuid

from scripts.tests.release import load

sbom = load("sbom")

ROOT_ID = "path+file:///w#shikigami@3.0.0"
TYPES_ID = "path+file:///w/crates/shikigami-types#3.0.0"


def dep(pkg: str, *kinds):
    return {"pkg": pkg, "dep_kinds": [{"kind": k, "target": None} for k in kinds]}


def metadata(extra_dep: str) -> dict:
    """One `cargo metadata --filter-platform` result: a workspace crate, a
    normal, a build, a dev-only, and an unreachable dependency."""
    packages = [
        {"id": ROOT_ID, "name": "shikigami", "version": "3.0.0", "license": "Apache-2.0"},
        {"id": TYPES_ID, "name": "shikigami-types", "version": "3.0.0"},
        {"id": "r#serde@1.0.0", "name": "serde", "version": "1.0.0", "license": "MIT OR Apache-2.0"},
        {"id": "r#memchr@2.0.0", "name": "memchr", "version": "2.0.0", "license": "MIT/Unlicense"},
        {"id": "r#cc@1.0.0", "name": "cc", "version": "1.0.0", "license": "MIT"},
        {"id": "r#proptest@1.0.0", "name": "proptest", "version": "1.0.0", "license": "MIT"},
        {"id": "r#windows@1.0.0", "name": "windows", "version": "1.0.0", "license": "MIT"},
        {"id": "r#unused@1.0.0", "name": "unused", "version": "1.0.0", "license": "MIT"},
    ]
    nodes = [
        {"id": ROOT_ID, "deps": [dep(TYPES_ID, None), dep("r#cc@1.0.0", "build"), dep("r#proptest@1.0.0", "dev"), dep(extra_dep, None)]},
        {"id": TYPES_ID, "deps": [dep("r#serde@1.0.0", None, "dev")]},
    ] + [{"id": p["id"], "deps": []} for p in packages[2:]]
    return {"workspace_members": [ROOT_ID, TYPES_ID], "packages": packages, "resolve": {"nodes": nodes}}


class Bom(unittest.TestCase):
    def test_lists_linked_crates_of_every_target_without_dev_or_workspace_crates(self):
        doc = sbom.bom([metadata("r#memchr@2.0.0"), metadata("r#windows@1.0.0")])
        self.assertEqual(doc["metadata"]["component"]["name"], "shikigami")
        by_ref = {c["bom-ref"]: c for c in doc["components"]}
        self.assertEqual(
            set(by_ref),
            {"pkg:cargo/serde@1.0.0", "pkg:cargo/memchr@2.0.0", "pkg:cargo/cc@1.0.0", "pkg:cargo/windows@1.0.0"},
        )
        self.assertEqual(by_ref["pkg:cargo/memchr@2.0.0"]["licenses"], [{"expression": "MIT OR Unlicense"}])

    def test_serial_number_is_a_stable_function_of_the_content(self):
        serial = sbom.bom([metadata("r#memchr@2.0.0")])["serialNumber"]
        self.assertEqual(uuid.UUID(serial.removeprefix("urn:uuid:")).version, 5)
        self.assertEqual(sbom.bom([metadata("r#memchr@2.0.0")])["serialNumber"], serial)
        changed = metadata("r#memchr@2.0.0")
        changed["packages"][2]["version"] = "1.0.1"
        self.assertNotEqual(sbom.bom([changed])["serialNumber"], serial)

    def test_missing_root_package_fails(self):
        with self.assertRaisesRegex(SystemExit, "shikigami package"):
            sbom.bom([{"workspace_members": [], "packages": [], "resolve": {"nodes": []}}])


if __name__ == "__main__":
    unittest.main()
