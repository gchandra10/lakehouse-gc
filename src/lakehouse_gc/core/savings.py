"""Savings summary. Unknown stays unknown: no estimates means ``total is None``, not zero."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from decimal import Decimal

from lakehouse_gc.core.models import Finding, Recommendation


@dataclass(frozen=True)
class SavingsSummary:
    total: Decimal | None
    currency: str | None
    period: str | None
    findings_with_estimate: int
    findings_without_estimate: int
    resources_with_estimate: int
    notes: tuple[str, ...]


def summarize_savings(findings: Iterable[Finding]) -> SavingsSummary:
    """Sum estimates without double counting: overlapping findings on the same resource
    contribute only their largest estimate. Excluded findings are ignored."""
    relevant = [f for f in findings if f.recommendation is not Recommendation.EXCLUDED]
    estimated = [f for f in relevant if f.estimated_savings is not None]
    without = len(relevant) - len(estimated)
    if not estimated:
        return SavingsSummary(
            total=None,
            currency=None,
            period=None,
            findings_with_estimate=0,
            findings_without_estimate=without,
            resources_with_estimate=0,
            notes=("No findings carry a savings estimate; total savings are unknown.",),
        )

    per_resource: dict[str, Decimal] = {}
    units: set[tuple[str, str]] = set()
    for finding in estimated:
        estimate = finding.estimated_savings
        assert estimate is not None
        units.add((estimate.currency, estimate.period))
        key = finding.resource.key
        per_resource[key] = max(per_resource.get(key, Decimal(0)), estimate.amount)

    notes = [
        "Overlapping estimates on the same resource are not summed; the largest is used.",
    ]
    if without:
        notes.append(f"{without} finding(s) have no estimate; the total covers only the rest.")
    if len(units) > 1:
        notes.append("Estimates use mixed currencies or periods; no total is computed.")
        total, currency, period = None, None, None
    else:
        ((currency, period),) = units
        total = sum(per_resource.values(), Decimal(0))

    return SavingsSummary(
        total=total,
        currency=currency,
        period=period,
        findings_with_estimate=len(estimated),
        findings_without_estimate=without,
        resources_with_estimate=len(per_resource),
        notes=tuple(notes),
    )
