"""Guards for the report-only, no-connectivity guarantees of Phase 1."""

from __future__ import annotations

import ast
import logging
from pathlib import Path

import lakehouse_gc
from lakehouse_gc.core.log import JsonFormatter

SRC = Path(lakehouse_gc.__file__).resolve().parent
FORBIDDEN_IMPORTS = {
    "databricks",
    "requests",
    "urllib.request",
    "http.client",
    "socket",
    "subprocess",
    "shutil",
}


def _imports(path: Path) -> set[str]:
    names: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
    return names


def test_no_network_sdk_or_process_imports() -> None:
    for path in SRC.rglob("*.py"):
        for name in _imports(path):
            root_match = any(name == f or name.startswith(f"{f}.") for f in FORBIDDEN_IMPORTS)
            assert not root_match, f"{path.relative_to(SRC)} imports {name}"


def test_library_installs_only_null_handler() -> None:
    handlers = logging.getLogger("lakehouse_gc").handlers
    assert any(isinstance(h, logging.NullHandler) for h in handlers)
    assert all(type(h) is logging.NullHandler for h in handlers)
    root_modules = {type(h).__module__ for h in logging.getLogger().handlers}
    assert not any(module.startswith("lakehouse_gc") for module in root_modules)


def test_json_formatter_includes_extra_fields() -> None:
    record = logging.LogRecord("lakehouse_gc.runner", logging.INFO, "f", 1, "hello", (), None)
    record.event = "run.started"
    record.run_id = "abc"
    output = JsonFormatter().format(record)
    assert '"event": "run.started"' in output
    assert '"run_id": "abc"' in output
    assert '"message": "hello"' in output


def test_version_exposed() -> None:
    assert lakehouse_gc.__version__ == "0.2.2"
