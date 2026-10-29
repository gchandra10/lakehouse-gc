"""lakehouse_gc: report-only governance and cost assessment for Databricks lakehouses."""

from __future__ import annotations

import logging

from lakehouse_gc.rules import describe_rules, rules_dataframe, rules_text
from lakehouse_gc.runner import run
from lakehouse_gc.simple import check, define_scanner, fail, ok, unknown

__version__ = "0.2.1"

logging.getLogger("lakehouse_gc").addHandler(logging.NullHandler())

__all__ = [
    "__version__",
    "check",
    "define_scanner",
    "describe_rules",
    "fail",
    "ok",
    "rules_dataframe",
    "rules_text",
    "run",
    "unknown",
]
