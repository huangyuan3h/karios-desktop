#!/usr/bin/env python3
"""Repo docs dead-link checker (H4 drift guard).

Scans docs/**/*.md + AGENTS.md + root *.md for relative markdown links and
fails when a target file does not exist. Rules that keep CI red-iff-real:
- http(s)://, mailto:, bare #anchors: skipped.
- Fenced code blocks are skipped (examples, not links).
- `archive/` targets must exist (snapshots are immutable, never renamed).

Usage: python3 scripts/linkcheck.py [--root DIR]
Exit 0 = clean, 1 = broken links (printed as file:line -> target).
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

LINK_RE = re.compile(r"(?<!!)\[([^\]]*)\]\(([^)]+)\)")
IMAGE_RE = re.compile(r"!\[([^\]]*)\]\(([^)]+)\)")

SKIP_PREFIXES = ("http://", "https://", "mailto:", "data:", "#")


def _strip_fences(lines: list[str]) -> list[tuple[int, str]]:
    out: list[tuple[int, str]] = []
    fenced = False
    for i, line in enumerate(lines, start=1):
        if line.lstrip().startswith("```"):
            fenced = not fenced
            continue
        if not fenced:
            out.append((i, line))
    return out


def check_file(path: Path) -> list[str]:
    errors: list[str] = []
    content = path.read_text(encoding="utf-8").splitlines()
    for lineno, line in _strip_fences(content):
        for m in list(LINK_RE.finditer(line)) + list(IMAGE_RE.finditer(line)):
            target = m.group(2).strip().strip("<>")
            if not target or target.startswith(SKIP_PREFIXES):
                continue
            file_part = target.split("#", 1)[0].strip()
            if not file_part:
                continue
            resolved = (path.parent / file_part).resolve()
            if not resolved.exists():
                errors.append(f"{path}:{lineno} -> {target}")
    return errors


def main() -> int:
    root = Path(sys.argv[sys.argv.index("--root") + 1]) if "--root" in sys.argv else Path(__file__).resolve().parents[1]
    files = sorted(
        f for f in (root / "docs").rglob("*.md")
        if "archive" not in f.relative_to(root / "docs").parts  # snapshots are frozen, never rewritten
    )
    files += [root / "AGENTS.md"]
    files += sorted(root.glob("*.md"))
    files = [f for f in files if f.is_file()]
    errors: list[str] = []
    for f in files:
        errors.extend(check_file(f))
    if errors:
        print(f"linkcheck: {len(errors)} broken link(s) in {len(files)} files:")
        for e in errors:
            print(f"  {e}")
        return 1
    print(f"linkcheck: clean ({len(files)} files)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
