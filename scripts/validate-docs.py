#!/usr/bin/env python3
"""Check local Markdown link targets without third-party dependencies."""

from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
SKIP_DIRS = {".git", "target", ".shikigami-state"}
LINK = re.compile(r"(?<!!)\[[^\]]+\]\(([^)\s]+)")
EXTERNAL = ("#", "/", "//", "http:", "https:", "mailto:", "tel:", "data:")


def main() -> int:
    root = ROOT.resolve()
    errors: list[str] = []
    files = sorted(
        path
        for path in root.rglob("*")
        if path.is_file()
        and path.suffix in {".md", ".mdx"}
        and not any(part in SKIP_DIRS for part in path.relative_to(root).parts)
    )
    for source in files:
        for line_number, line in enumerate(source.read_text(encoding="utf-8").splitlines(), 1):
            for match in LINK.finditer(line):
                target = match.group(1)
                if target.startswith(EXTERNAL):
                    continue
                target = target.split("#", 1)[0].split("?", 1)[0]
                if not target:
                    continue
                path = (source.parent / target).resolve()
                try:
                    path.relative_to(root)
                except ValueError:
                    continue
                if not path.exists():
                    errors.append(
                        f"{source.relative_to(root)}:{line_number}: missing local link target `{match.group(1)}`"
                    )
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
