"""Per-scanner and per-run result models."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from lakehouse_gc.core import export
from lakehouse_gc.core.models import (
    Finding,
    Phase,
    Recommendation,
    Resource,
    RunStatus,
    ScannerStatus,
    ScoredEvaluation,
)
from lakehouse_gc.core.savings import SavingsSummary
from lakehouse_gc.core.scoring import DimensionScore, ScoreReport

FINDINGS_SCHEMA = (
    "severity_level INT, severity STRING, scanner STRING, resource_type STRING, "
    "resource STRING, rule STRING, recommendation STRING, confidence INT, reasons STRING, "
    "downgraded_from STRING, excluded_reason STRING, run_id STRING, run_time TIMESTAMP"
)
FINDINGS_COLUMNS = tuple(column.split()[0] for column in FINDINGS_SCHEMA.split(", "))


@dataclass(frozen=True)
class ScannerError:
    scanner: str
    phase: Phase
    error_type: str
    message: str


@dataclass(frozen=True)
class ScannerResult:
    """Outcome of one scanner. A failed scanner contributes no resources, evaluations, or
    findings; only its error and any warnings raised before the failure."""

    name: str
    status: ScannerStatus
    started_at: datetime
    finished_at: datetime
    resources: tuple[Resource, ...] = ()
    evaluations: tuple[ScoredEvaluation, ...] = ()
    findings: tuple[Finding, ...] = ()
    warnings: tuple[str, ...] = ()
    limitations: tuple[str, ...] = ()
    error: ScannerError | None = None


@dataclass(frozen=True)
class RunResult:
    run_id: str
    started_at: datetime
    finished_at: datetime
    status: RunStatus
    policy_version: int
    scanners: tuple[str, ...]
    scanner_results: tuple[ScannerResult, ...]
    findings: tuple[Finding, ...]
    errors: tuple[ScannerError, ...]
    warnings: tuple[str, ...]
    scores: ScoreReport
    savings: SavingsSummary
    limitations: tuple[str, ...] = ()
    """What couldn't be seen (missing access, columns, or detail) and which checks it made
    unknown. See ``check_access`` for the sources behind them."""

    def scanner_result(self, name: str) -> ScannerResult:
        for result in self.scanner_results:
            if result.name == name:
                return result
        raise KeyError(name)

    @property
    def excluded_findings(self) -> tuple[Finding, ...]:
        return tuple(f for f in self.findings if f.recommendation is Recommendation.EXCLUDED)

    def to_rows(self) -> list[dict[str, Any]]:
        """One plain dict per finding (columns match ``FINDINGS_SCHEMA``), most severe first.

        ``severity_level`` is 3 high, 2 medium, 1 low, 0 info, so it sorts numerically.
        ``run_id`` and ``run_time`` tell saved runs apart."""
        findings = sorted(
            self.findings,
            key=lambda f: (-f.severity.level, f.scanner, f.resource.resource_id, f.rule_id),
        )
        return [
            {
                "severity_level": f.severity.level,
                "severity": f.severity.value,
                "scanner": f.scanner,
                "resource_type": f.resource.resource_type,
                "resource": f.resource.resource_id,
                "rule": f.rule_id,
                "recommendation": f.recommendation.value,
                "confidence": f.confidence,
                "reasons": " ".join(f.reasons),
                "downgraded_from": f.downgraded_from.value if f.downgraded_from else None,
                "excluded_reason": f.exclusion.reason if f.exclusion else None,
                "run_id": self.run_id,
                "run_time": self.started_at,
            }
            for f in findings
        ]

    def to_dataframe(self, spark: Any) -> Any:
        """Findings as a Spark DataFrame with an explicit schema, e.g. for ``display()``."""
        rows = [tuple(row[c] for c in FINDINGS_COLUMNS) for row in self.to_rows()]
        return spark.createDataFrame(rows, schema=FINDINGS_SCHEMA)

    def to_json(self, indent: int | None = 2) -> str:
        """The run as JSON text: status, scores, limitations, warnings, errors, and findings.
        Nothing is written; see ``save`` for that."""
        return export.run_to_json(self, indent)

    def save(
        self, spark: Any = None, *, table: str | None = None, json_path: str | None = None
    ) -> list[str]:
        """Save the results only where you say, and return what was written.

        - ``table="catalog.schema.table"`` appends the findings to a Delta table (created
          if missing). Needs ``spark``.
        - ``json_path="/Volumes/.../run.json"`` writes ``to_json()`` to a new file. An
          existing file is never overwritten.

        With neither, nothing is saved and a ``ValueError`` explains the options. Saving
        writes only the results; it never changes a scanned resource.
        """
        return export.save(self, spark, table=table, json_path=json_path)

    def summary(self) -> str:
        """Short human-readable overview of the run."""
        lines = [
            f"Run {self.run_id}: {self.status.value} ({', '.join(self.scanners)})",
            _score_line("Governance", self.scores.governance),
            _score_line("Cost", self.scores.cost),
            f"  Overall: {_fmt(self.scores.overall)}  grade: {self.scores.grade or 'unknown'}"
            + ("" if self.scores.complete else "  (incomplete: some scanners failed)"),
        ]
        counts = Counter(f.recommendation.value for f in self.findings)
        if counts:
            lines.append(
                "  Findings: " + ", ".join(f"{name}={n}" for name, n in sorted(counts.items()))
            )
        for error in self.errors:
            lines.append(f"  FAILED {error.scanner} during {error.phase.value}: {error.message}")
        for warning in self.warnings:
            lines.append(f"  Warning: {warning}")
        if self.limitations:
            lines.append("  Limitations (these checks are unknown, not passed):")
            lines.extend(f"    - {item}" for item in self.limitations)
            lines.append(
                "    Run check_access(spark, POLICY_PATH) to see which sources are readable."
            )
        if self.savings.total is None:
            lines.append("  Savings: unknown (no estimates available)")
        return "\n".join(lines)


def _fmt(value: float | None) -> str:
    return "unknown" if value is None else f"{value:g}"


def _score_line(label: str, score: DimensionScore) -> str:
    coverage = "n/a" if score.coverage is None else f"{score.coverage:.0%}"
    return (
        f"  {label}: {_fmt(score.score)}  ({score.evaluations} checks, {coverage} with known "
        "evidence)"
    )
