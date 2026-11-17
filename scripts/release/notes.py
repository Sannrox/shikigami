#!/usr/bin/env python3
"""Print the CHANGELOG.md body for one ``## [version]`` heading.

The release workflow uses it for the GitHub Release notes:
``python3 scripts/release/notes.py X.Y.Z[-rc.N]``.
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


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: notes.py <version>")
    path = pathlib.Path(__file__).resolve().parents[2] / "CHANGELOG.md"
    print(notes_for(path.read_text(), sys.argv[1]))


if __name__ == "__main__":
    main()
