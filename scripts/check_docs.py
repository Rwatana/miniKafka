#!/usr/bin/env python3
"""Validate local Markdown links and editable Draw.io documents."""

from pathlib import Path
import re
import sys
import xml.etree.ElementTree as ET


ROOT = Path(__file__).resolve().parents[1]
LINK_PATTERN = re.compile(r"(?<!!)\[[^\]]+\]\(([^)]+)\)")


def markdown_files():
    return sorted(ROOT.rglob("*.md"))


def validate_markdown_links():
    errors = []
    for markdown in markdown_files():
        content = markdown.read_text(encoding="utf-8")
        for target in LINK_PATTERN.findall(content):
            if target.startswith(("http://", "https://", "mailto:")) or target.startswith("#"):
                continue
            path_part = target.split("#", 1)[0]
            if path_part and not (markdown.parent / path_part).exists():
                errors.append(f"{markdown.relative_to(ROOT)}: missing link target {target}")
    return errors


def validate_drawio_files():
    errors = []
    for diagram in sorted((ROOT / "docs" / "diagrams").glob("*.drawio")):
        try:
            ET.parse(diagram)
        except ET.ParseError as error:
            errors.append(f"{diagram.relative_to(ROOT)}: invalid Draw.io XML: {error}")
    return errors


def main():
    errors = validate_markdown_links() + validate_drawio_files()
    if errors:
        print("Documentation validation failed:", file=sys.stderr)
        print("\n".join(errors), file=sys.stderr)
        return 1
    print(f"Documentation validation passed ({len(markdown_files())} Markdown files).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
