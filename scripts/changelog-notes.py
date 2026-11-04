#!/usr/bin/env python3
"""Print the CHANGELOG.md body for one ``## [version]`` heading.

The release workflow uses it for the GitHub Release notes:
``python3 scripts/changelog-notes.py X.Y.Z[-rc.N]``. ``--self-test`` runs
the checks scripts/validate-release.sh relies on.
"""

from __future__ import annotations

import pathlib
import re
import sys


def notes_for(text: str, version: str) -> str:
    """Return the section body for exactly `version`; an rc never matches its final."""
    match = re.search(rf"^## \[{re.escape(version)}\][^\n]*\n", text, re.M)
    if match is None:
        raise SystemExit(f"CHANGELOG.md has no ## [{version}] section")
    nxt = re.search(r"^## ", text[match.end() :], re.M)
    end = match.end() + nxt.start() if nxt else len(text)
    body = text[match.end() : end].strip()
    if not body:
        raise SystemExit(f"empty CHANGELOG section for {version}")
    return body


def expect_exit(version: str, text: str, message: str) -> None:
    try:
        notes_for(text, version)
    except SystemExit as exc:
        assert message in str(exc), exc
    else:
        raise SystemExit(f"expected failure for {version}: {message}")


def self_test() -> None:
    sample = (
        "# Changelog\n\n"
        "## Unreleased\n\n"
        "- pending\n\n"
        "## [3.1.0-rc.1] — 2026-11-01\n\n"
        "- candidate\n\n"
        "## [3.0.0] — 2026-10-30\n\n"
        "### Added\n\n"
        "- release workflow\n"
    )
    assert notes_for(sample, "3.0.0") == "### Added\n\n- release workflow"
    assert notes_for(sample, "3.1.0-rc.1") == "- candidate"
    expect_exit("3.1.0", sample, "no ## [3.1.0] section")
    expect_exit("1.0.0", "# Changelog\n\n## [1.0.0]\n\n", "empty CHANGELOG section")
    print("changelog-notes self-test passed")


def main() -> None:
    if sys.argv[1:] == ["--self-test"]:
        self_test()
        return
    if len(sys.argv) != 2:
        raise SystemExit("usage: changelog-notes.py <version> | --self-test")
    path = pathlib.Path(__file__).resolve().parent.parent / "CHANGELOG.md"
    print(notes_for(path.read_text(), sys.argv[1]))


if __name__ == "__main__":
    main()
