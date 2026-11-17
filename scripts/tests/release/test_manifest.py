"""manifest.py: the manifest contents, and gate flags read from a scratch
repository's history through a fake gh."""

import json
import os
import pathlib
import subprocess
import sys
import tempfile
import unittest

from scripts.tests.release import RELEASE, load

manifest = load("manifest")
SCRIPT = RELEASE / "manifest.py"

# Prints the labels in $FAKE_LABELS_<pr>, logs every pull request it is asked
# about to $FAKE_LOG, and fails for the pull request in $FAKE_FAIL.
FAKE_GH = """#!/usr/bin/env bash
printf '%s\\n' "$3" >>"$FAKE_LOG"
[[ "$3" == "${FAKE_FAIL:-}" ]] && { echo "HTTP 502" >&2; exit 1; }
var="FAKE_LABELS_$3"
printf '%s' "${!var:-}"
"""


class BuildManifest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix=f"man{os.getpid()}-")
        self.addCleanup(tmp.cleanup)
        self.dist = pathlib.Path(tmp.name)
        self.args = dict(version="0.1.0", git_sha="a" * 40, tag="v0.1.0", dist=self.dist, images=[], attestations=[], labels=[])

    def build(self, **overrides):
        return manifest.build_manifest(**{**self.args, **overrides})

    def test_lists_release_files_by_digest_and_skips_descriptions(self):
        with self.assertRaisesRegex(SystemExit, "no release files"):
            self.build()
        for name in ("shikigami-v0.1.0-x86_64-unknown-linux-gnu.tar.gz", "shikigami.cdx.json"):
            (self.dist / name).write_bytes(b"bin")
        for name in ("shikigami-v0.1.0-x86_64-unknown-linux-gnu.sha256", "shikigami-0.1.0-oci.json", *manifest.SKIP_NAMES):
            (self.dist / name).write_text("skip\n")
        image = manifest.parse_image("ghcr.io/sannrox/shikigami:0.1.0=sha256:" + "b" * 64)
        doc = self.build(images=[image], labels=["risk:migration", "area:ops"])
        self.assertEqual(doc["schema"], manifest.SCHEMA)
        self.assertEqual(
            [b["name"] for b in doc["binaries"]],
            ["shikigami-v0.1.0-x86_64-unknown-linux-gnu.tar.gz", "shikigami.cdx.json"],
        )
        self.assertTrue(all(b["digest"].startswith("sha256:") for b in doc["binaries"]))
        self.assertEqual(doc["images"], [image])
        self.assertEqual(doc["gate_flags"], ["migration"])

    def test_refuses_a_short_sha_and_a_tag_for_another_version(self):
        (self.dist / "a.tar.gz").write_bytes(b"bin")
        with self.assertRaisesRegex(SystemExit, "full lowercase SHA"):
            self.build(git_sha="deadbeef")
        with self.assertRaisesRegex(SystemExit, "does not match"):
            self.build(tag="v0.2.0")

    def test_images_need_a_digest_and_never_a_latest_tag(self):
        with self.assertRaisesRegex(SystemExit, "latest"):
            manifest.parse_image("ghcr.io/sannrox/shikigami:latest=sha256:" + "a" * 64)
        with self.assertRaisesRegex(SystemExit, "NAME=sha256"):
            manifest.parse_image("ghcr.io/sannrox/shikigami:0.1.0=sha256:abc")

    def test_a_declared_break_needs_the_breaking_gate(self):
        # #514: v3.0.0 notes declared a break and its manifest had no gate.
        (self.dist / "a.tar.gz").write_bytes(b"bin")
        notes = "### Breaking changes and upgrade\n\n- `token_env` is removed.\n"
        with self.assertRaisesRegex(SystemExit, "risk:breaking"):
            self.build(notes=notes)
        with self.assertRaisesRegex(SystemExit, "risk:breaking"):
            self.build(subjects=["a1b2c3d feat(governance)!: drop the bearer (#9)"])
        self.assertEqual(self.build(notes=notes, labels=["risk:breaking"])["gate_flags"], ["breaking"])
        self.build(notes="### Fixed\n\n- breaking news in a bullet\n", subjects=["a1b2c3d fix: x (#1)"])


class GatesFromHistory(unittest.TestCase):
    """History: bootstrap, #1, v0.1.0, #2, v0.2.0-rc.1, #3."""

    @classmethod
    def setUpClass(cls):
        tmp = tempfile.TemporaryDirectory(prefix=f"gates{os.getpid()}-")
        cls.addClassCleanup(tmp.cleanup)
        cls.tmp = pathlib.Path(tmp.name)
        cls.repo = cls.tmp / "repo"
        bin_dir = cls.tmp / "bin"
        bin_dir.mkdir()
        (bin_dir / "gh").write_text(FAKE_GH)
        (bin_dir / "gh").chmod(0o755)
        (cls.tmp / "dist").mkdir()
        (cls.tmp / "dist" / "a.tar.gz").write_bytes(b"bin")
        cls.env = {**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}",
                   "GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1"}

        def git(*args):
            return subprocess.run(["git", "-C", str(cls.repo), *args], env=cls.env, check=True,
                                  capture_output=True, text=True).stdout.strip()

        subprocess.run(["git", "init", "-q", str(cls.repo)], env=cls.env, check=True)
        git("config", "user.name", "Release test")
        git("config", "user.email", "release-test@example.invalid")
        cls.sha = {}
        for name, subject, tag in (
            ("bootstrap", "initial import", None),
            ("one", "feat: one (#1)", "v0.1.0"),
            ("two", "fix: two (#2)", "v0.2.0-rc.1"),
            ("three", "feat!: three (#3)", None),
        ):
            git("commit", "-q", "--allow-empty", "-m", subject)
            cls.sha[name] = git("rev-parse", "HEAD")
            if tag:
                git("tag", tag)

    def setUp(self):
        self.log = self.tmp / f"gh-{self.id()}.log"
        self.log.write_text("")
        self.out = self.tmp / f"{self.id()}.json"

    def run_manifest(self, version: str, sha: str, **env) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, str(SCRIPT), "--version", version, "--git-sha", sha, "--tag", f"v{version}",
             "--dist", str(self.tmp / "dist"), "--out", str(self.out)],
            cwd=self.repo, env={**self.env, "FAKE_LOG": str(self.log), **env}, capture_output=True, text=True,
        )

    def asked(self) -> list[str]:
        return self.log.read_text().split()

    def test_final_release_counts_every_pull_request_since_the_previous_final_tag(self):
        result = self.run_manifest("0.2.0", self.sha["three"], FAKE_LABELS_2="gate:mig\n", FAKE_LABELS_3="risk:breaking\n")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.asked(), ["2", "3"])
        self.assertEqual(json.loads(self.out.read_text())["gate_flags"], ["breaking", "migration"])

    def test_without_a_final_tag_the_range_starts_at_the_first_pull_request(self):
        result = self.run_manifest("0.1.0", self.sha["one"])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.asked(), ["1"])

    def test_a_break_subject_without_its_label_is_refused(self):
        result = self.run_manifest("0.2.0", self.sha["three"])
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("risk:breaking", result.stderr)
        self.assertFalse(self.out.exists())

    def test_a_failed_label_lookup_fails_closed(self):
        result = self.run_manifest("0.2.0", self.sha["three"], FAKE_FAIL="2", FAKE_LABELS_3="risk:breaking\n")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("cannot read labels of pull request #2", result.stderr)
        self.assertFalse(self.out.exists())

    def test_a_commit_without_a_pull_request_fails_closed(self):
        subprocess.run(["git", "-C", str(self.repo), "checkout", "-q", "-b", "direct", self.sha["three"]],
                       env=self.env, check=True)
        subprocess.run(["git", "-C", str(self.repo), "commit", "-q", "--allow-empty", "-m", "pushed to main"],
                       env=self.env, check=True)
        head = subprocess.run(["git", "-C", str(self.repo), "rev-parse", "HEAD"], env=self.env, check=True,
                              capture_output=True, text=True).stdout.strip()
        result = self.run_manifest("0.2.0", head, FAKE_LABELS_3="risk:breaking\n")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("pushed to main", result.stderr)


if __name__ == "__main__":
    unittest.main()
