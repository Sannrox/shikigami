import unittest

from scripts.tests.release import load

notes = load("notes")

CHANGELOG = (
    "# Changelog\n\n"
    "## Unreleased\n\n"
    "- pending\n\n"
    "## [3.1.0-rc.1] — 2026-11-01\n\n"
    "- candidate\n\n"
    "## [3.0.0] — 2026-10-30\n\n"
    "### Added\n\n"
    "- release workflow\n"
)


class NotesFor(unittest.TestCase):
    def test_returns_only_the_requested_section(self):
        self.assertEqual(notes.notes_for(CHANGELOG, "3.0.0"), "### Added\n\n- release workflow")

    def test_final_release_never_reuses_its_release_candidate_notes(self):
        self.assertEqual(notes.notes_for(CHANGELOG, "3.1.0-rc.1"), "- candidate")
        with self.assertRaisesRegex(SystemExit, r"no ## \[3.1.0\] section"):
            notes.notes_for(CHANGELOG, "3.1.0")

    def test_empty_section_fails(self):
        with self.assertRaisesRegex(SystemExit, "empty CHANGELOG section"):
            notes.notes_for("# Changelog\n\n## [1.0.0]\n\n", "1.0.0")


if __name__ == "__main__":
    unittest.main()
