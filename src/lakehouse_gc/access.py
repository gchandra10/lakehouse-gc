"""Check which data sources the current user can read, before running a scan.

    from lakehouse_gc import check_access
    report = check_access(spark, POLICY_PATH, scanners=["uc"])
    print(report.summary())
    display(report.to_dataframe(spark))

Each source is probed for its column names only (``SELECT * ... LIMIT 0`` returns no rows).
Nothing is scanned and nothing is changed.
"""

from __future__ import annotations

import os
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any

from lakehouse_gc.core.policy import Policy
from lakehouse_gc.core.registry import ScannerRegistry
from lakehouse_gc.core.scanner import Scanner
from lakehouse_gc.core.sources import (
    ResolvedSource,
    SourceReader,
    SourceUnavailableError,
    describe_error,
)
from lakehouse_gc.core.sql import SqlClient, as_sql_client, default_sql_client
from lakehouse_gc.runner import prepare

AVAILABLE, PARTIAL, UNAVAILABLE, UNVERIFIED = "available", "partial", "unavailable", "unverified"
ACCESS_SCHEMA = (
    "scanner STRING, source STRING, object STRING, configured_by STRING, status STRING, "
    "missing_columns STRING, impact STRING, how_to_fix STRING"
)
_COLUMNS = tuple(column.split()[0] for column in ACCESS_SCHEMA.split(", "))
_MARK = {AVAILABLE: "OK  ", PARTIAL: "PART", UNAVAILABLE: "NO  ", UNVERIFIED: "??  "}


@dataclass(frozen=True)
class AccessCheck:
    scanner: str
    source: str
    object: str
    configured_by: str
    """``default`` or ``policy``."""
    status: str
    """``available``, ``partial`` (optional columns missing), ``unavailable``, or
    ``unverified`` (could not be probed, e.g. no catalog to sample)."""
    missing_columns: tuple[str, ...]
    impact: str
    how_to_fix: str
    essential: bool = False


@dataclass(frozen=True)
class AccessReport:
    checks: tuple[AccessCheck, ...]
    scanners: tuple[str, ...]
    notes: tuple[str, ...] = ()

    def can_run(self, scanner: str) -> bool:
        """False if an essential source of ``scanner`` is unavailable."""
        return not any(
            c.scanner == scanner and c.essential and c.status == UNAVAILABLE for c in self.checks
        )

    @property
    def all_available(self) -> bool:
        return all(c.status == AVAILABLE for c in self.checks)

    def to_rows(self) -> list[dict[str, str]]:
        return [
            {
                "scanner": c.scanner,
                "source": c.source,
                "object": c.object,
                "configured_by": c.configured_by,
                "status": c.status,
                "missing_columns": ", ".join(c.missing_columns),
                "impact": c.impact,
                "how_to_fix": c.how_to_fix,
            }
            for c in self.checks
        ]

    def to_dataframe(self, spark: Any) -> Any:
        rows = [tuple(row[c] for c in _COLUMNS) for row in self.to_rows()]
        return spark.createDataFrame(rows, schema=ACCESS_SCHEMA)

    def summary(self) -> str:
        lines = []
        for scanner in self.scanners:
            checks = [c for c in self.checks if c.scanner == scanner]
            lines.append(f"Access check for '{scanner}':")
            if not checks:
                lines.append("  reads no data sources")
                continue
            width = max(len(c.source) for c in checks)
            for c in checks:
                lines.append(f"  [{_MARK[c.status]}] {c.source:<{width}}  {c.object}  ({c.status})")
                if c.missing_columns:
                    lines.append(f"         missing columns: {', '.join(c.missing_columns)}")
                if c.status != AVAILABLE:
                    lines.append(f"         impact: {c.impact}")
                if c.how_to_fix:
                    lines.append(f"         how to fix: {c.how_to_fix}")
            degraded = sum(1 for c in checks if c.status != AVAILABLE)
            if not self.can_run(scanner):
                verdict = "cannot run until the essential source(s) above are readable"
            elif degraded:
                verdict = f"can run; {degraded} source(s) limited, so some checks will be unknown"
            else:
                verdict = "can run with full coverage"
            lines.append(f"  Verdict: {scanner} {verdict}.")
        lines.extend(f"Note: {note}" for note in self.notes)
        return "\n".join(lines)


def check_access(
    spark: Any = None,
    config_path: str | os.PathLike[str] | None = None,
    *,
    policy: Policy | None = None,
    scanners: Iterable[str | type[Scanner]] | None = None,
    registry: ScannerRegistry | None = None,
) -> AccessReport:
    """Probe every source the requested scanners would read, using the policy's
    ``sources`` and ``scope``. Reads column names only."""
    prepared = prepare(config_path, policy=policy, scanners=scanners, registry=registry)
    sql = as_sql_client(spark) if spark is not None else default_sql_client()
    reader = SourceReader(sql)
    checks: list[AccessCheck] = []
    notes: list[str] = []
    for scanner_cls in prepared.scanner_classes:
        plan = prepared.sources.get(scanner_cls.name, {})
        sample, catalog_check = _sample_catalog(sql, prepared.policy, scanner_cls.name, plan)
        if catalog_check:
            checks.append(catalog_check)
        for source in plan.values():
            checks.append(_probe(reader, scanner_cls.name, source, sample))
        notes.extend(f"{scanner_cls.name}: {note}" for note in scanner_cls.access_notes)
    return AccessReport(
        tuple(checks), tuple(c.name for c in prepared.scanner_classes), tuple(notes)
    )


def _sample_catalog(
    sql: SqlClient, policy: Policy, scanner: str, plan: dict[str, ResolvedSource] | Any
) -> tuple[str | None, AccessCheck | None]:
    """For per-catalog sources, list catalogs and pick the first in scope to probe."""
    if not any(source.per_catalog for source in plan.values()):
        return None, None
    try:
        rows = sql.query("SHOW CATALOGS")
    except Exception as exc:
        return None, AccessCheck(
            scanner,
            "catalog list",
            "SHOW CATALOGS",
            "policy",
            UNAVAILABLE,
            (),
            f"{scanner} cannot list catalogs in per_catalog mode",
            "Ask for USE CATALOG on the catalogs to scan, or point sources.catalogs at a view "
            f"that lists them. ({describe_error(exc)})",
            essential=True,
        )
    names = sorted(str(row.get("catalog") or next(iter(row.values()), "")) for row in rows)
    in_scope = [name for name in names if name and policy.scope.includes_catalog(name)]
    impact = (
        f"{len(in_scope)} catalog(s) in scope; per-catalog sources probed in '{in_scope[0]}'"
        if in_scope
        else "no visible catalog matches the policy scope"
    )
    check = AccessCheck(
        scanner,
        "catalog list",
        "SHOW CATALOGS",
        "policy",
        AVAILABLE if in_scope else PARTIAL,
        (),
        impact,
        "" if in_scope else "Check scope.include_catalogs, or ask for USE CATALOG.",
    )
    return (in_scope[0] if in_scope else None), check


def _rules(rule_ids: Sequence[str]) -> str:
    unique = list(dict.fromkeys(rule_ids))
    return ", ".join(unique) if unique else "no checks"


def _probe(
    reader: SourceReader, scanner: str, source: ResolvedSource, sample: str | None
) -> AccessCheck:
    spec = source.spec
    if source.per_catalog and sample is None:
        return AccessCheck(
            scanner,
            spec.name,
            source.display_name,
            source.origin,
            UNVERIFIED,
            (),
            "not probed: no catalog available to sample",
            "Make at least one in-scope catalog visible, then run check_access again.",
            spec.essential,
        )
    object_name = source.object_for(sample)
    shown = f"{object_name} (sampled)" if source.per_catalog else object_name
    try:
        missing_required, missing_optional = reader.missing(source, object_name)
    except SourceUnavailableError as exc:
        return AccessCheck(
            scanner,
            spec.name,
            shown,
            source.origin,
            UNAVAILABLE,
            (),
            f"{scanner} cannot run" if spec.essential else f"unknown: {_rules(spec.used_by)}",
            _fix_unreadable(source, object_name, exc.reason),
            spec.essential,
        )
    actual = tuple(source.actual(c) for c in (*missing_required, *missing_optional))
    if missing_required:
        return AccessCheck(
            scanner,
            spec.name,
            shown,
            source.origin,
            UNAVAILABLE,
            actual,
            f"{scanner} cannot run" if spec.essential else f"unknown: {_rules(spec.used_by)}",
            f"Required column(s) are missing. If your view uses other names, map them under "
            f"sources.{spec.name}.columns.",
            spec.essential,
        )
    if missing_optional:
        uses = [rule for c in missing_optional for rule in spec.column(c).used_by]
        return AccessCheck(
            scanner,
            spec.name,
            shown,
            source.origin,
            PARTIAL,
            actual,
            f"unknown: {_rules(uses)}",
            f"Map the column(s) under sources.{spec.name}.columns if your view names them "
            "differently, or ask for a view that includes them.",
            spec.essential,
        )
    return AccessCheck(
        scanner, spec.name, shown, source.origin, AVAILABLE, (), "full", "", spec.essential
    )


def _fix_unreadable(source: ResolvedSource, object_name: str, reason: str) -> str:
    name, group = source.spec.name, source.spec.group
    if source.origin == "policy" and not source.per_catalog:
        return (
            f"Check that {object_name} exists and that you can SELECT from it, or correct "
            f"sources.{name} in the policy. ({reason})"
        )
    if source.per_catalog:
        return (
            f"Ask for USE CATALOG / USE SCHEMA on that catalog, or point sources.{name} at a "
            f"view your organization provides. ({reason})"
        )
    hint = f"set sources.{group}: per_catalog, or " if group else ""
    return (
        f"Ask your admin for read access to {object_name}, or {hint}point sources.{name} at a "
        f"view your organization provides. ({reason})"
    )
