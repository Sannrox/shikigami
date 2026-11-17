"""Release invariants no workflow linter expresses, asserted on the parsed YAML.

scripts/release/ enforces these at run time; these tests keep the workflows
from bypassing the scripts. Assert on structure (triggers, permissions, job
graph, step order, which script a step runs), never on the wording of a step.
"""

import re
import unittest

import yaml

from scripts.tests.release import ROOT

WORKFLOWS = ROOT / ".github" / "workflows"
VERIFIED_SHA = "${{ needs.verify-tag.outputs.sha }}"
TAGGED_ONLY = "needs.verify-tag.outputs.tag != ''"
# gh release subcommands that change a release; only github-release.sh runs them.
GH_RELEASE_MUTATION = re.compile(r"\bgh release (create|edit|upload|delete|delete-asset)\b")


def load(name: str) -> dict:
    doc = yaml.safe_load((WORKFLOWS / name).read_text())
    doc["on"] = doc.pop(True, doc.get("on"))  # YAML 1.1 reads a bare `on` key as True.
    return doc


def runs(step: dict) -> str:
    return step.get("run", "")


def uses(step: dict) -> str:
    return step.get("uses", "")


def is_action_mutation(step: dict) -> bool:
    """An action that uploads to the release, pushes the image, or attests."""
    return (
        uses(step).startswith(("actions/attest", "taiki-e/upload-rust-binary-action@"))
        or (uses(step).startswith("docker/build-push-action@") and step["with"].get("push") is True)
    )


def is_guard(step: dict) -> bool:
    return "scripts/release/github-release.sh guard" in runs(step)


def needs(job: dict) -> set:
    value = job.get("needs", [])
    return {value} if isinstance(value, str) else set(value)


def strings(node):
    if isinstance(node, dict):
        for value in node.values():
            yield from strings(value)
    elif isinstance(node, list):
        for value in node:
            yield from strings(value)
    elif isinstance(node, str):
        yield node


class Dispatcher(unittest.TestCase):
    """A v* tag push only starts release.yml from main; it can do nothing else."""

    def setUp(self):
        self.wf = load("release-dispatch.yml")

    def test_triggers_only_on_v_tags(self):
        self.assertEqual(self.wf["on"], {"push": {"tags": ["v*"]}})

    def test_can_only_dispatch_workflows(self):
        self.assertEqual(self.wf["permissions"], {"actions": "write", "contents": "read"})

    def test_runs_one_step_that_dispatches_release_on_main(self):
        [job] = self.wf["jobs"].values()
        self.assertNotIn("permissions", job)
        [step] = job["steps"]
        self.assertNotIn("uses", step)
        self.assertEqual(step["run"].strip(), 'gh workflow run release.yml --ref main -f tag="$TAG"')


class Release(unittest.TestCase):
    def setUp(self):
        self.wf = load("release.yml")
        self.jobs = self.wf["jobs"]

    def test_runs_only_from_a_dispatch(self):
        self.assertEqual(set(self.wf["on"]), {"workflow_dispatch"})
        self.assertIn("tag", self.wf["on"]["workflow_dispatch"]["inputs"])

    def test_one_run_per_tag(self):
        self.assertIn("inputs.tag", self.wf["concurrency"]["group"])
        self.assertFalse(self.wf["concurrency"]["cancel-in-progress"])

    def test_a_tagged_run_must_start_from_main(self):
        first = self.jobs["verify-tag"]["steps"][0]
        self.assertEqual(first["if"], "inputs.tag != ''")
        self.assertEqual(first["env"]["WORKFLOW_REF"], "${{ github.ref }}")
        self.assertIn('"$WORKFLOW_REF" == refs/heads/main', first["run"])

    def test_verifies_the_tag_with_mains_verifier_and_requires_it_on_main(self):
        steps = self.jobs["verify-tag"]["steps"]
        main_checkout = next(s for s in steps if s.get("with", {}).get("path") == "main-tree")
        self.assertNotIn("ref", main_checkout["with"], "main's verifier must come from the workflow ref")
        verify = next(s for s in steps if "verify-tag.sh" in runs(s))
        self.assertTrue(verify["env"]["MAIN_TREE"].endswith("/main-tree"))
        self.assertIn('RELEASE_TRUST_REF=origin/main bash "${MAIN_TREE}/scripts/release/verify-tag.sh" verify', verify["run"])
        self.assertIn("git merge-base --is-ancestor \"$sha\" refs/remotes/origin/main", verify["run"])
        guard = next(s for s in steps if is_guard(s))
        self.assertIn("main-tree/scripts/release/github-release.sh", guard["run"])
        self.assertLess(steps.index(verify), steps.index(guard))

    def test_every_later_job_builds_the_verified_commit(self):
        for name, job in self.jobs.items():
            if name == "verify-tag":
                continue
            with self.subTest(job=name):
                self.assertIn("verify-tag", needs(job))
                steps = job["steps"]
                checkouts = [i for i, s in enumerate(steps) if uses(s).startswith("actions/checkout@")]
                self.assertTrue(checkouts)
                for i in checkouts:
                    self.assertEqual(steps[i]["with"]["ref"], VERIFIED_SHA)
                    self.assertIn('test "$(git rev-parse HEAD)" = "$SHA"', runs(steps[i + 1]))

    def test_binaries_build_from_the_verified_tag(self):
        [upload] = [s for s in self.jobs["shikigami-binaries"]["steps"] if is_action_mutation(s)]
        self.assertEqual(upload["with"]["ref"], "refs/tags/${{ needs.verify-tag.outputs.tag }}")

    def test_never_uses_the_event_commit_or_ref_name(self):
        for value in strings(self.jobs):
            for banned in ("github.sha", "github.ref_name", "GITHUB_SHA", "GITHUB_REF"):
                self.assertNotIn(banned, value)

    def test_release_changes_go_through_the_release_script(self):
        for value in strings(self.jobs):
            self.assertIsNone(GH_RELEASE_MUTATION.search(value), value)

    def test_every_job_refuses_a_published_release_before_its_first_action_mutation(self):
        # github-release.sh refuses a published release itself; actions do not.
        mutating = 0
        for name, job in self.jobs.items():
            steps = job["steps"]
            first = next((i for i, s in enumerate(steps) if is_action_mutation(s)), None)
            if first is None:
                continue
            mutating += 1
            with self.subTest(job=name):
                guards = [s for s in steps[:first] if is_guard(s)]
                self.assertTrue(guards, f"{name} mutates before the guard")
                self.assertTrue(all("if" not in s for s in guards), f"{name} guard must always run")
                self.assertIn(TAGGED_ONLY, job["if"])
        self.assertGreaterEqual(mutating, 3)

    def test_only_tagged_runs_change_the_release(self):
        for name, job in self.jobs.items():
            if any("scripts/release/github-release.sh" in runs(s) and not is_guard(s) for s in job["steps"]):
                with self.subTest(job=name):
                    self.assertIn(TAGGED_ONLY, job["if"])

    def test_draft_notes_come_from_the_changelog(self):
        steps = self.jobs["create-release"]["steps"]
        notes = next(i for i, s in enumerate(steps) if "scripts/release/notes.py" in runs(s))
        draft = next(i for i, s in enumerate(steps) if "github-release.sh draft" in runs(s))
        self.assertLess(notes, draft)

    def test_assets_are_drafted_before_any_upload_and_published_last(self):
        draft_job = next(n for n, j in self.jobs.items() if any("github-release.sh draft" in runs(s) for s in j["steps"]))
        for name in ("shikigami-binaries", "oci-image"):
            self.assertIn(draft_job, needs(self.jobs[name]))
        publish_job = next(n for n, j in self.jobs.items() if any("github-release.sh publish" in runs(s) for s in j["steps"]))
        self.assertLessEqual({"shikigami-binaries", "oci-image"}, needs(self.jobs[publish_job]))
        self.assertFalse(any(publish_job in needs(job) for job in self.jobs.values()))
        steps = self.jobs[publish_job]["steps"]
        self.assertIn("github-release.sh publish", runs(steps[-1]))

    def test_publishes_the_attested_manifest_unchanged(self):
        steps = self.jobs["finalize"]["steps"]
        sbom = next(i for i, s in enumerate(steps) if "scripts/release/sbom.py" in runs(s))
        attest_sbom = next(i for i, s in enumerate(steps) if uses(s).startswith("actions/attest-sbom@"))
        write = next(i for i, s in enumerate(steps) if "scripts/release/manifest.py" in runs(s))
        sign = next(
            i for i, s in enumerate(steps)
            if uses(s).startswith("actions/attest-build-provenance@") and s["with"]["subject-path"] == "dist/shikigami-release.json"
        )
        upload = next(i for i, s in enumerate(steps) if "github-release.sh upload" in runs(s))
        self.assertLess(sbom, attest_sbom)
        self.assertLess(attest_sbom, write)
        self.assertLess(write, sign)
        self.assertLess(sign, upload)
        self.assertFalse(any("shikigami-release.json" in runs(s) for s in steps[sign + 1:upload]))

    def test_never_tags_an_image_latest(self):
        for value in strings(self.jobs):
            self.assertNotRegex(value, r":latest\b|--tag[= ]latest")

    def test_write_permissions_are_job_scoped(self):
        self.assertEqual(self.wf["permissions"], {"contents": "read"})
        writers = {
            name: {k for k, v in job.get("permissions", {}).items() if v == "write"}
            for name, job in self.jobs.items()
        }
        self.assertEqual(
            {name: perms for name, perms in writers.items() if perms},
            {
                "create-release": {"contents"},
                "shikigami-binaries": {"contents"},
                "oci-image": {"contents", "packages"},
                "finalize": {"contents", "id-token", "attestations"},
            },
        )


if __name__ == "__main__":
    unittest.main()
