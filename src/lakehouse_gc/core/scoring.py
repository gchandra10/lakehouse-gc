"""Deterministic, policy-driven aggregate scoring with a per-evaluation deduction audit trail.

For each dimension ``d`` (governance, cost), over all scored evaluations pooled across
successful scanners whose weight ``w`` for ``d`` is positive::

    p(PASS) = 0, p(FAIL) = 1, p(INDETERMINATE) = unknown_penalty   (0 < unknown_penalty <= 1)
    score_d = 100 * (1 - sum(w * p) / sum(w))                       None if sum(w) == 0
    deduction_i = 100 * w_i * p_i / sum(w)                          sum(deductions) == 100 - score_d

    overall = sum(W_d * score_d) / sum(W_d)   (policy weights, normalized)
    overall is None if any dimension with W_d > 0 has no score.

Arithmetic uses exact fractions on sorted input, so results are reproducible; rounding to
two decimals happens only in the ``score``/``overall`` presentation fields.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from fractions import Fraction
from types import MappingProxyType

from lakehouse_gc.core.models import Dimension, Outcome, ResourceRef, ScoredEvaluation
from lakehouse_gc.core.policy import ScoringConfig


@dataclass(frozen=True)
class Deduction:
    scanner: str
    resource: ResourceRef
    rule_id: str
    dimension: Dimension
    outcome: Outcome
    weight: Fraction
    penalty_factor: Fraction
    points: Fraction
    """Points deducted from 100 for this dimension."""


@dataclass(frozen=True)
class DimensionScore:
    dimension: Dimension
    score: float | None
    exact_score: Fraction | None
    evaluations: int
    determinate: int
    coverage: float | None
    """Share of weighted evaluations with a PASS/FAIL outcome (0-1); None if none."""
    total_weight: Fraction
    deductions: tuple[Deduction, ...]


@dataclass(frozen=True)
class ScoreReport:
    governance: DimensionScore
    cost: DimensionScore
    overall: float | None
    exact_overall: Fraction | None
    grade: str | None
    dimension_weights: Mapping[Dimension, Fraction]
    """Normalized policy weights (sum to 1)."""
    unknown_penalty: Fraction
    complete: bool
    """False if any requested scanner failed; scores then cover successful scanners only."""
    failed_scanners: tuple[str, ...]

    def dimension(self, dimension: Dimension) -> DimensionScore:
        return self.governance if dimension is Dimension.GOVERNANCE else self.cost


def to_fraction(value: float | int | Fraction) -> Fraction:
    """Exact conversion that treats floats as their shortest decimal form (0.1 -> 1/10)."""
    if isinstance(value, Fraction):
        return value
    if isinstance(value, int):
        return Fraction(value)
    return Fraction(repr(value))


def penalty_factor(outcome: Outcome, unknown_penalty: Fraction) -> Fraction:
    if outcome is Outcome.PASS:
        return Fraction(0)
    if outcome is Outcome.FAIL:
        return Fraction(1)
    return unknown_penalty


def _present(value: Fraction, digits: int) -> float:
    return float(round(value, digits))


def _score_dimension(
    dimension: Dimension,
    items: list[tuple[str, ScoredEvaluation]],
    unknown_penalty: Fraction,
) -> DimensionScore:
    weighted = [
        (scanner, scored, to_fraction(scored.weights[dimension]))
        for scanner, scored in items
        if scored.weights.get(dimension, 0) > 0
    ]
    total = sum((weight for _, _, weight in weighted), Fraction(0))
    if total == 0:
        return DimensionScore(dimension, None, None, 0, 0, None, Fraction(0), ())
    deductions = []
    deducted = Fraction(0)
    determinate = 0
    for scanner, scored, weight in weighted:
        outcome = scored.evaluation.outcome
        if outcome is not Outcome.INDETERMINATE:
            determinate += 1
        factor = penalty_factor(outcome, unknown_penalty)
        points = 100 * weight * factor / total
        deducted += points
        if points:
            deductions.append(
                Deduction(
                    scanner=scanner,
                    resource=scored.evaluation.resource,
                    rule_id=scored.evaluation.rule_id,
                    dimension=dimension,
                    outcome=outcome,
                    weight=weight,
                    penalty_factor=factor,
                    points=points,
                )
            )
    exact = 100 - deducted
    return DimensionScore(
        dimension=dimension,
        score=_present(exact, 2),
        exact_score=exact,
        evaluations=len(weighted),
        determinate=determinate,
        coverage=_present(Fraction(determinate, len(weighted)), 4),
        total_weight=total,
        deductions=tuple(deductions),
    )


def grade_for(score: Fraction | None, boundaries: Iterable[tuple[str, float]]) -> str | None:
    if score is None:
        return None
    for grade, minimum in sorted(boundaries, key=lambda bound: bound[1], reverse=True):
        if score >= to_fraction(minimum):
            return grade
    return None


def compute_scores(
    scored: Iterable[tuple[str, ScoredEvaluation]],
    scoring: ScoringConfig,
    failed_scanners: Iterable[str] = (),
) -> ScoreReport:
    """Aggregate ``(scanner_name, scored_evaluation)`` pairs into a ``ScoreReport``."""
    unknown_penalty = to_fraction(scoring.unknown_penalty)
    items = sorted(
        scored,
        key=lambda pair: (pair[0], pair[1].evaluation.resource.key, pair[1].evaluation.rule_id),
    )
    dimensions = {
        dimension: _score_dimension(dimension, items, unknown_penalty) for dimension in Dimension
    }
    raw_weights = {d: to_fraction(w) for d, w in scoring.dimension_weights.items()}
    weight_total = sum(raw_weights.values(), Fraction(0))
    weights = {d: w / weight_total for d, w in raw_weights.items()}

    exact_overall: Fraction | None = Fraction(0)
    for dimension, weight in weights.items():
        if weight == 0:
            continue
        dimension_score = dimensions[dimension].exact_score
        if dimension_score is None or exact_overall is None:
            exact_overall = None
            continue
        exact_overall += weight * dimension_score

    failed = tuple(sorted(set(failed_scanners)))
    return ScoreReport(
        governance=dimensions[Dimension.GOVERNANCE],
        cost=dimensions[Dimension.COST],
        overall=None if exact_overall is None else _present(exact_overall, 2),
        exact_overall=exact_overall,
        grade=grade_for(exact_overall, scoring.grade_boundaries),
        dimension_weights=MappingProxyType(weights),
        unknown_penalty=unknown_penalty,
        complete=not failed,
        failed_scanners=failed,
    )
