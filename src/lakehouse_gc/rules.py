"""Explain the rules a run would apply, with the settings taken from the policy.

from lakehouse_gc import rules_dataframe
display(rules_dataframe(spark, POLICY_PATH, scanners=["uc"]))
"""

from __future__ import annotations

import dataclasses
import os
from collections.abc import Iterable
from typing import Any

from lakehouse_gc.core.registry import ScannerRegistry
from lakehouse_gc.core.scanner import Scanner
from lakehouse_gc.runner import EXCLUDED_RULE_ID, prepare

RULES_SCHEMA = (
    "scanner STRING, rule STRING, title STRING, meaning STRING, severity STRING, "
    "affects STRING, on_fail STRING, settings STRING"
)
_COLUMNS = tuple(column.split()[0] for column in RULES_SCHEMA.split(", "))


def describe_rules(
    config_path: str | os.PathLike[str] | None = None,
    *,
    policy: Any = None,
    scanners: Iterable[str | type[Scanner]] | None = None,
    registry: ScannerRegistry | None = None,
) -> list[dict[str, str]]:
    """One row per rule: what it checks, how it scores, and its effective policy settings.

    Validates the policy exactly like ``run()`` but scans nothing. A final row explains the
    policy exclusions.
    """
    prepared = prepare(config_path, policy=policy, scanners=scanners, registry=registry)
    severity_weights = prepared.policy.scoring.severity_weights
    rows = []
    for scanner_cls in prepared.scanner_classes:
        config = prepared.configs.get(scanner_cls.name)
        values = dataclasses.asdict(config) if config is not None else {}
        for rule in scanner_cls.rules:
            affects = ", ".join(sorted(d.value for d in rule.dimensions)) or "none"
            rows.append(
                {
                    "scanner": scanner_cls.name,
                    "rule": rule.rule_id,
                    "title": rule.title,
                    "meaning": rule.description or rule.title,
                    "severity": (
                        f"{rule.severity.value} (weight {severity_weights[rule.severity]:g})"
                    ),
                    "affects": affects,
                    "on_fail": rule.on_fail.value if rule.on_fail else "review",
                    "settings": "; ".join(
                        f"{name} = {_format(values.get(name))}" for name in rule.settings
                    ),
                }
            )
    exclusions = prepared.policy.exclusions
    rows.append(
        {
            "scanner": "framework",
            "rule": EXCLUDED_RULE_ID,
            "title": "Excluded by policy",
            "meaning": (
                "Resources matching an exclusion (same resource_type, case-insensitive glob on "
                "the full name) are not analyzed and are listed as excluded with the reason. "
                "Exclusions past expires_on are ignored with a warning."
            ),
            "severity": "info (weight 0)",
            "affects": "none",
            "on_fail": "excluded",
            "settings": "; ".join(
                f"{e.resource_type}: {e.pattern}"
                + (f" (until {e.expires_on.isoformat()})" if e.expires_on else "")
                for e in exclusions
            )
            or "no exclusions",
        }
    )
    return rows


def rules_dataframe(
    spark: Any,
    config_path: str | os.PathLike[str] | None = None,
    *,
    policy: Any = None,
    scanners: Iterable[str | type[Scanner]] | None = None,
    registry: ScannerRegistry | None = None,
) -> Any:
    """``describe_rules`` as a Spark DataFrame, ready for ``display()``."""
    rows = describe_rules(config_path, policy=policy, scanners=scanners, registry=registry)
    return spark.createDataFrame(
        [tuple(row[c] for c in _COLUMNS) for row in rows], schema=RULES_SCHEMA
    )


def rules_text(
    config_path: str | os.PathLike[str] | None = None,
    *,
    policy: Any = None,
    scanners: Iterable[str | type[Scanner]] | None = None,
    registry: ScannerRegistry | None = None,
) -> str:
    """``describe_rules`` as plain text, for ``print()`` outside notebooks."""
    blocks = []
    for row in describe_rules(config_path, policy=policy, scanners=scanners, registry=registry):
        lines = [
            f"{row['rule']} - {row['title']}",
            f"  {row['meaning']}",
            f"  severity: {row['severity']} | affects: {row['affects']} | "
            f"on fail: {row['on_fail']}",
        ]
        if row["settings"]:
            lines.append(f"  settings: {row['settings']}")
        blocks.append("\n".join(lines))
    return "\n\n".join(blocks)


def _format(value: object) -> str:
    if isinstance(value, (list, tuple)):
        return "[" + ", ".join(str(item) for item in value) + "]" if value else "[] (off)"
    if isinstance(value, bool):
        return str(value).lower()
    return "" if value is None else f"{value:g}" if isinstance(value, float) else str(value)
