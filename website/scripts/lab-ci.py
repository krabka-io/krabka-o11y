#!/usr/bin/env python3
"""Select the full lab unless every changed path is documentation."""

from pathlib import PurePosixPath
import sys


def documentation(path):
    parts = PurePosixPath(path).parts
    if not parts or ".." in parts:
        return False
    if path.startswith("docs/"):
        return PurePosixPath(path).suffix.lower() in {".md", ".mdx", ".rst", ".svg", ".png", ".jpg", ".webp"}
    if len(parts) == 1:
        return path.endswith(".md")
    if parts[0] == "crates" and len(parts) == 3 and parts[-1].lower() in {"readme.md", "changelog.md", "test_coverage_report.md"}:
        return True
    return False


def full_lab(paths):
    # Empty or unknown diffs use full qualification. Renames supply both paths.
    return not paths or not all(documentation(path) for path in paths)


if __name__ == "__main__":
    paths = [path for path in sys.stdin.buffer.read().decode("utf-8").split("\0") if path]
    print(str(full_lab(paths)).lower())
