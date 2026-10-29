from __future__ import annotations

import random
from fractions import Fraction

import pytest

from lakehouse_gc.core.models import (
    Dimension,
    Outcome,
    ResourceRef,
    RuleEvaluation,
    ScoredEvaluation,
    Severity,
)
from lakehouse_gc.core.policy import ScoringConfig
from lakehouse_gc.core.scoring import compute_scores, grade_for

GOV, COST = Dimension.GOVERNANCE, Dimension.COST


def _scored(
    resource_id: str, outcome: Outcome, weights: dict[Dimension, float], rule: str = "s.rule"
) -> tuple[str, ScoredEvaluation]:
    evaluation = RuleEvaluation(ResourceRef("ws", "t", resource_id), rule, outcome)
    return ("s", ScoredEvaluation(evaluation, Severity.MEDIUM, weights, None))


def test_weighted_pass_rate_with_indeterminate_penalty() -> None:
    items = [
        _scored("a", Outcome.PASS, {GOV: 3}),
        _scored("b", Outcome.FAIL, {GOV: 3}),
        _scored("c", Outcome.INDETERMINATE, {GOV: 3}),
        _scored("d", Outcome.PASS, {GOV: 1}),
    ]
    report = compute_scores(items, ScoringConfig())
    # total weight 10; penalty 3*1 + 3*0.5 = 4.5 -> 55
    assert report.governance.exact_score == 55
    assert report.governance.score == 55.0
    assert report.governance.evaluations == 4
    assert report.governance.determinate == 3
    assert report.governance.coverage == 0.75
    points = {d.resource.resource_id: d.points for d in report.governance.deductions}
    assert points == {"b": 30, "c": 15}


def test_deductions_sum_exactly_to_lost_points() -> None:
    rng = random.Random(7)
    outcomes = list(Outcome)
    items = [
        _scored(f"r{i}", rng.choice(outcomes), {GOV: rng.choice([1, 3, 5]), COST: 0.1})
        for i in range(50)
    ]
    report = compute_scores(items, ScoringConfig(unknown_penalty=0.3))
    for dim in (report.governance, report.cost):
        assert dim.exact_score is not None
        assert sum(d.points for d in dim.deductions) == 100 - dim.exact_score


def test_input_order_does_not_change_result() -> None:
    items = [_scored(f"r{i}", list(Outcome)[i % 3], {GOV: i % 4 + 1, COST: 1}) for i in range(20)]
    shuffled = items[:]
    random.Random(1).shuffle(shuffled)
    assert compute_scores(items, ScoringConfig()) == compute_scores(shuffled, ScoringConfig())


def test_indeterminate_never_scores_as_pass() -> None:
    all_unknown = [_scored(f"r{i}", Outcome.INDETERMINATE, {GOV: 1}) for i in range(3)]
    all_pass = [_scored(f"r{i}", Outcome.PASS, {GOV: 1}) for i in range(3)]
    unknown_report = compute_scores(all_unknown, ScoringConfig(unknown_penalty=0.1))
    pass_report = compute_scores(all_pass, ScoringConfig())
    assert unknown_report.governance.score == 90.0
    assert unknown_report.governance.coverage == 0.0
    assert pass_report.governance.score == 100.0


def test_no_evaluations_means_unknown_not_perfect() -> None:
    report = compute_scores([], ScoringConfig())
    assert report.governance.score is None
    assert report.cost.score is None
    assert report.governance.coverage is None
    assert report.overall is None
    assert report.grade is None


def test_overall_unknown_if_weighted_dimension_missing() -> None:
    report = compute_scores([_scored("a", Outcome.PASS, {GOV: 1})], ScoringConfig())
    assert report.governance.score == 100.0
    assert report.cost.score is None
    assert report.overall is None


def test_zero_weight_dimension_is_ignored_in_overall() -> None:
    config = ScoringConfig(governance_weight=1, cost_weight=0)
    report = compute_scores([_scored("a", Outcome.FAIL, {GOV: 1, COST: 0})], config)
    assert report.overall == 0.0
    assert report.dimension_weights == {GOV: Fraction(1), COST: Fraction(0)}


def test_dimension_weights_are_normalized() -> None:
    items = [_scored("a", Outcome.FAIL, {GOV: 1}), _scored("b", Outcome.PASS, {COST: 1})]
    halves = compute_scores(items, ScoringConfig(governance_weight=0.5, cost_weight=0.5))
    twos = compute_scores(items, ScoringConfig(governance_weight=2, cost_weight=2))
    assert halves.overall == twos.overall == 50.0
    skewed = compute_scores(items, ScoringConfig(governance_weight=1, cost_weight=3))
    assert skewed.exact_overall == 75


def test_zero_weight_evaluations_do_not_count() -> None:
    items = [_scored("a", Outcome.FAIL, {GOV: 0}), _scored("b", Outcome.PASS, {GOV: 2})]
    report = compute_scores(items, ScoringConfig())
    assert report.governance.score == 100.0
    assert report.governance.evaluations == 1


def test_failed_scanners_mark_report_incomplete() -> None:
    report = compute_scores([], ScoringConfig(), failed_scanners=["b", "a", "b"])
    assert not report.complete
    assert report.failed_scanners == ("a", "b")


@pytest.mark.parametrize(
    ("score", "grade"),
    [(Fraction(100), "A"), (Fraction(90), "A"), (Fraction(8999, 100), "B"), (Fraction(0), "F")],
)
def test_grade_boundaries(score: Fraction, grade: str) -> None:
    assert grade_for(score, ScoringConfig().grade_boundaries) == grade


def test_custom_grade_boundaries() -> None:
    bounds = ScoringConfig(grade_boundaries=(("ok", 50), ("bad", 0))).grade_boundaries
    assert grade_for(Fraction(50), bounds) == "ok"
    assert grade_for(Fraction(49), bounds) == "bad"
