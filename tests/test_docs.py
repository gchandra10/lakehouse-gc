"""Keep the docs honest: code blocks must compile and relative links must resolve."""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

from conftest import ROOT

DOCS = [ROOT / "README.md", *sorted((ROOT / "docs").glob("*.md"))]
_BLOCK = re.compile(r"```(\w+)\n(.*?)```", re.S)
_LINK = re.compile(r"\]\(([^)#]+)(?:#[^)]*)?\)")


@pytest.mark.parametrize("doc", DOCS, ids=lambda p: p.name)
def test_code_blocks_are_valid(doc: Path) -> None:
    for language, body in _BLOCK.findall(doc.read_text(encoding="utf-8")):
        if language == "python":
            compile(body, f"{doc.name}:python", "exec")
        elif language == "yaml":
            yaml.safe_load(body)


@pytest.mark.parametrize("doc", DOCS, ids=lambda p: p.name)
def test_relative_links_resolve(doc: Path) -> None:
    for target in _LINK.findall(doc.read_text(encoding="utf-8")):
        if "://" in target:
            continue
        assert (doc.parent / target).exists(), f"{doc.name} links to missing {target}"


def test_readme_has_no_notebook_magics() -> None:
    assert "%pip" not in (ROOT / "README.md").read_text(encoding="utf-8")
