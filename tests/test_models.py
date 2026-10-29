from __future__ import annotations

from decimal import Decimal

import pytest

from lakehouse_gc.core.errors import ModelValidationError
from lakehouse_gc.core.evidence import Evidence, EvidenceStatus, derive_confidence
from lakehouse_gc.core.models import (
    Dimension,
    ExclusionStatus,
    Finding,
    Outcome,
    Recommendation,
    Resource,
    ResourceRef,
    RuleEvaluation,
    SavingsEstimate,
    ScoredEvaluation,
    Severity,
)
from lakehouse_gc.core.savings import summarize_savings

REF = ResourceRef("ws", "table", "t1")
OBS, INF, UNA = EvidenceStatus.OBSERVED, EvidenceStatus.INFERRED, EvidenceStatus.UNAVAILABLE


def test_unavailable_evidence_cannot_carry_value() -> None:
    with pytest.raises(ModelValidationError, match="must not carry a value"):
        Evidence("last_access", UNA, "src", value=0)
    assert Evidence("last_access", UNA, "src").value is None


def test_evidence_requires_source() -> None:
    with pytest.raises(ModelValidationError, match="source"):
        Evidence("k", OBS, "")


@pytest.mark.parametrize(
    ("statuses", "expected"),
    [
        ((), None),
        ((OBS,), 100),
        ((INF,), 50),
        ((UNA,), 0),
        ((OBS, INF), 75),
        ((OBS, UNA, UNA), 33),
        ((OBS, OBS, INF, UNA), 63),  # 62.5 rounds half up
    ],
)
def test_derive_confidence(statuses: tuple[EvidenceStatus, ...], expected: int | None) -> None:
    evidence = [
        Evidence(f"k{i}", s, "src", value=None if s is UNA else 1) for i, s in enumerate(statuses)
    ]
    assert derive_confidence(evidence) == expected


def test_resource_ref_key_and_validation() -> None:
    assert REF.key == "ws/table/t1"
    with pytest.raises(ModelValidationError):
        ResourceRef("ws", "", "t1")


def test_resource_is_immutable() -> None:
    resource = Resource(REF, "main.a.b", metadata={"size": 1})
    with pytest.raises(TypeError):
        resource.metadata["size"] = 2  # type: ignore[index]


@pytest.mark.parametrize("confidence", [-1, 101, True, 50.5])
def test_confidence_bounds(confidence: object) -> None:
    with pytest.raises(ModelValidationError, match="confidence"):
        Finding(REF, "x.rule", Severity.LOW, Recommendation.KEEP, confidence=confidence)  # type: ignore[arg-type]


def test_excluded_requires_exclusion_status() -> None:
    with pytest.raises(ModelValidationError, match="exclusion"):
        Finding(REF, "x.rule", Severity.LOW, Recommendation.EXCLUDED)
    status = ExclusionStatus("table", "*", "reason")
    with pytest.raises(ModelValidationError, match="exclusion"):
        Finding(REF, "x.rule", Severity.LOW, Recommendation.KEEP, exclusion=status)


def test_scored_evaluation_rejects_negative_weight() -> None:
    evaluation = RuleEvaluation(REF, "x.rule", Outcome.PASS)
    with pytest.raises(ModelValidationError, match="non-negative"):
        ScoredEvaluation(evaluation, Severity.LOW, {Dimension.COST: -1}, None)


def test_savings_estimate_requires_assumptions() -> None:
    with pytest.raises(ModelValidationError, match="assumptions"):
        SavingsEstimate(Decimal("10"), "USD", "monthly", (), "full")
    with pytest.raises(ModelValidationError, match="amount"):
        SavingsEstimate(10, "USD", "monthly", ("a",), "full")  # type: ignore[arg-type]


def _finding(resource_id: str, rule: str, amount: str | None, currency: str = "USD") -> Finding:
    estimate = (
        None
        if amount is None
        else SavingsEstimate(Decimal(amount), currency, "monthly", ("list price",), "storage only")
    )
    return Finding(
        ResourceRef("ws", "table", resource_id),
        rule,
        Severity.LOW,
        Recommendation.REVIEW,
        estimated_savings=estimate,
    )


def test_savings_unknown_when_no_estimates() -> None:
    summary = summarize_savings([_finding("a", "x.r", None)])
    assert summary.total is None
    assert summary.findings_without_estimate == 1


def test_savings_not_double_counted_per_resource() -> None:
    summary = summarize_savings(
        [
            _finding("a", "x.r1", "10"),
            _finding("a", "x.r2", "25"),
            _finding("b", "x.r1", "5"),
            _finding("c", "x.r1", None),
        ]
    )
    assert summary.total == Decimal("30")
    assert summary.resources_with_estimate == 2
    assert summary.findings_with_estimate == 3
    assert summary.findings_without_estimate == 1


def test_savings_mixed_currency_has_no_total() -> None:
    summary = summarize_savings([_finding("a", "x.r", "1"), _finding("b", "x.r", "1", "EUR")])
    assert summary.total is None
    assert any("mixed" in note for note in summary.notes)
