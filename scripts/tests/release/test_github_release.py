"""github-release.sh against a fake gh that records every call."""

import os
import pathlib
import subprocess
import tempfile
import unittest

from scripts.tests.release import RELEASE

SCRIPT = RELEASE / "github-release.sh"

# Answers `gh release view` from $FAKE_STATE (draft|published|missing|error)
# and logs every call, one per line, to $FAKE_LOG.
FAKE_GH = """#!/usr/bin/env bash
printf '%s\\n' "$*" >>"$FAKE_LOG"
if [[ "$1 $2" == "release view" ]]; then
  if [[ "$*" == *"--json assets"* ]]; then printf 'old.tar.gz\\nSHA256SUMS\\n'; exit 0; fi
  case "$FAKE_STATE" in
    draft) echo true ;;
    published) echo false ;;
    missing) echo "release not found" >&2; exit 1 ;;
    *) echo "HTTP 502" >&2; exit 1 ;;
  esac
fi
"""


class GithubRelease(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix=f"ghr{os.getpid()}-")
        self.addCleanup(tmp.cleanup)
        self.tmp = pathlib.Path(tmp.name)
        bin_dir = self.tmp / "bin"
        bin_dir.mkdir()
        (bin_dir / "gh").write_text(FAKE_GH)
        (bin_dir / "gh").chmod(0o755)
        self.log = self.tmp / "gh.log"
        self.log.touch()
        self.env = {**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}", "FAKE_LOG": str(self.log)}

    def run_script(self, state: str, *args: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            ["bash", str(SCRIPT), *args],
            env={**self.env, "FAKE_STATE": state}, capture_output=True, text=True,
        )

    def mutations(self) -> list[str]:
        return [c for c in self.log.read_text().splitlines() if not c.startswith("release view")]

    def test_guard_allows_a_draft_or_a_missing_release(self):
        for state in ("draft", "missing"):
            with self.subTest(state=state):
                self.assertEqual(self.run_script(state, "guard", "v1.0.0").returncode, 0)

    def test_guard_refuses_a_published_release_and_a_failed_lookup(self):
        for state, message in (("published", "already published"), ("error", "cannot read release")):
            with self.subTest(state=state):
                result = self.run_script(state, "guard", "v1.0.0")
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(message, result.stderr)

    def test_no_command_changes_a_published_release_or_one_it_cannot_read(self):
        for state in ("published", "error"):
            for args in (("draft", "v1.0.0", "notes.md"), ("upload", "v1.0.0", "a.tar.gz"), ("publish", "v1.0.0")):
                with self.subTest(state=state, command=args[0]):
                    self.assertNotEqual(self.run_script(state, *args).returncode, 0)
        self.assertEqual(self.mutations(), [])

    def test_draft_creates_a_prerelease_that_is_never_latest(self):
        result = self.run_script("missing", "draft", "v1.0.0-rc.1", "notes.md")
        self.assertEqual(result.returncode, 0, result.stderr)
        [create] = self.mutations()
        self.assertTrue(create.startswith("release create v1.0.0-rc.1 --draft --verify-tag"))
        self.assertIn("--prerelease --latest=false", create)

    def test_draft_resume_drops_the_earlier_runs_assets(self):
        result = self.run_script("draft", "draft", "v1.0.0", "notes.md")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            self.mutations(),
            [
                "release delete-asset v1.0.0 old.tar.gz --yes",
                "release delete-asset v1.0.0 SHA256SUMS --yes",
                "release edit v1.0.0 --title v1.0.0 --notes-file notes.md --draft",
            ],
        )

    def test_upload_needs_a_draft(self):
        self.assertNotEqual(self.run_script("missing", "upload", "v1.0.0", "a.tar.gz").returncode, 0)
        self.assertEqual(self.run_script("draft", "upload", "v1.0.0", "a.tar.gz", "b.json").returncode, 0)
        self.assertEqual(self.mutations(), ["release upload v1.0.0 a.tar.gz b.json --clobber"])

    def test_publish_marks_a_final_release_without_forcing_latest(self):
        result = self.run_script("draft", "publish", "v1.0.0")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.mutations(), ["release edit v1.0.0 --draft=false"])

    def test_publish_keeps_a_prerelease_off_latest(self):
        self.run_script("draft", "publish", "v1.0.0-rc.1")
        self.assertEqual(self.mutations(), ["release edit v1.0.0-rc.1 --draft=false --prerelease --latest=false"])

    def test_publish_refuses_without_a_draft(self):
        self.assertNotEqual(self.run_script("missing", "publish", "v1.0.0").returncode, 0)
        self.assertEqual(self.mutations(), [])


if __name__ == "__main__":
    unittest.main()
