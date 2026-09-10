#!/usr/bin/env python3
"""Fail when a repository Markdown link points to a missing local path."""

from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import unquote

LINK_PATTERN = re.compile(r"\[[^]]*]\(([^)]+)\)")
EXTERNAL_PREFIXES = ("http://", "https://", "mailto:", "#")


def markdown_files(root: Path) -> list[Path]:
    files = [root / "README.md", root / "transform" / "README.md"]
    files.extend(sorted((root / "docs").rglob("*.md")))
    return [path for path in files if path.is_file()]


def broken_links(root: Path) -> list[tuple[Path, str]]:
    broken: list[tuple[Path, str]] = []
    for document in markdown_files(root):
        content = document.read_text(encoding="utf-8")
        for raw_target in LINK_PATTERN.findall(content):
            target = raw_target.strip().strip("<>")
            if not target or target.startswith(EXTERNAL_PREFIXES):
                continue
            relative_path = unquote(target.split("#", 1)[0])
            if (
                relative_path
                and not (document.parent / relative_path).resolve().exists()
            ):
                broken.append((document.relative_to(root), raw_target))
    return broken


def main() -> None:
    root = Path(__file__).resolve().parent.parent
    missing = broken_links(root)
    if missing:
        for document, target in missing:
            print(f"{document}: missing local link {target}")
        raise SystemExit(1)
    print(f"Documentation links: OK ({len(markdown_files(root))} Markdown files)")


if __name__ == "__main__":
    main()
