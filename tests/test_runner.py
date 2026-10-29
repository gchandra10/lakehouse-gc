from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from conftest import EXAMPLE_POLICY, FIXED_NOW, ROOT, StubScanner, fixed_clock, make_stub
from lakehouse_gc import run
from lakehouse_gc.core.errors import (
    LakehouseGCError,
    PolicyValidationError,
    RegistryError,
    UnknownScannerError,
)
from lakehouse_gc.core.models import (
    Dimension,
    Finding,
    Outcome,
    Phase,
    Recommendation,
    Resource,
    ResourceRef,
    RuleEvaluation,
    RunStatus,
    SavingsEstimate,
    ScannerStatus,
    ScoredEvaluation,
)
from lakehouse_gc.core.policy import Policy, load_policy
from lakehouse_gc.core.registry import ScannerRegistry
from lakehouse_gc.core.results import RunResult
from lakehouse_gc.core.scanner import ScannerContext
from lakehouse_gc.runner import EXCLUDED_RULE_ID


def _registry(*classes: type) -> ScannerRegistry:
    reg = ScannerRegistry()
    for cls in classes:
        reg.register(cls)
    return reg


def _run(*classes: type, policy: Policy | None = None, **kwargs: object) -> RunResult:
    return run(
        policy=policy or Policy(),
        registry=_registry(*classes),
        clock=fixed_clock,
        run_id="test-run",
        **kwargs,  # type: ignore[arg-type]
    )


# --- example scanner end to end ----------------------------------------------------------


def test_example_run_from_config_path() -> None:
    result = run(config_path=str(EXAMPLE_POLICY), scanners=["example"], clock=fixed_clock)
    assert isinstance(result, RunResult)
    assert result.status is RunStatus.SUCCEEDED
    assert result.scanners == ("example",)
    assert result.policy_version == 1
    assert result.started_at == FIXED_NOW
    assert result.errors == ()

    scores = result.scores
    assert (scores.governance.score, scores.cost.score, scores.overall) == (70.0, 50.0, 60.0)
    assert scores.grade == "D"
    assert scores.complete
    assert scores.governance.coverage == 0.8
    assert scores.cost.coverage == 0.8


def test_example_findings_cover_missing_data_exclusions_and_gate() -> None:
    result = run(EXAMPLE_POLICY, scanners=["example"], clock=fixed_clock)
    by_key = {(f.resource.resource_id, f.rule_id): f for f in result.findings}

    assert by_key[("a-001", "example.idle")].recommendation is Recommendation.KEEP
    assert by_key[("a-002", "example.owner_missing")].recommendation is Recommendation.TAG_OWNER
    archive = by_key[("a-002", "example.idle")]
    assert archive.recommendation is Recommendation.ARCHIVE_CANDIDATE
    assert archive.confidence == 100

    unknown = by_key[("a-003", "example.idle")]
    assert unknown.recommendation is Recommendation.REVIEW
    assert unknown.confidence == 0
    assert unknown.evidence[0].value is None

    downgraded = by_key[("a-004", "example.idle")]
    assert downgraded.recommendation is Recommendation.REVIEW
    assert downgraded.downgraded_from is Recommendation.ARCHIVE_CANDIDATE
    assert downgraded.confidence == 50
    assert "below the policy minimum of 70" in downgraded.reasons[-1]

    excluded = by_key[("a-005", EXCLUDED_RULE_ID)]
    assert excluded.recommendation is Recommendation.EXCLUDED
    assert excluded.exclusion is not None and excluded.exclusion.reason == "Retention policy"
    assert result.excluded_findings == (excluded,)
    assert not any(k[0] == "a-005" and k[1] != EXCLUDED_RULE_ID for k in by_key)

    assert ("a-006", "example.idle") in by_key  # expired exclusion not applied
    assert len(result.warnings) == 1 and "expired on 2025-01-01" in result.warnings[0]

    assert all(f.scanner == "example" for f in result.findings)
    assert result.savings.total is None


def test_example_run_is_reproducible() -> None:
    first = run(EXAMPLE_POLICY, scanners=["example"], clock=fixed_clock, run_id="r")
    second = run(EXAMPLE_POLICY, scanners=["example"], clock=fixed_clock, run_id="r")
    assert first == second


def test_example_rules_are_configurable() -> None:
    policy = load_policy(EXAMPLE_POLICY)
    policy = replace(policy, rules={"example": {"require_owner": False, "idle_days": 365}})
    result = run(policy=policy, scanners=["example"], clock=fixed_clock)
    assert result.scores.governance.score is None
    assert {f.rule_id for f in result.findings} == {"example.idle", EXCLUDED_RULE_ID}
    # only old_export (400 days) is idle beyond a year; inferred_idle is 200 days
    failed = [
        d.resource.resource_id for d in result.scores.cost.deductions if d.outcome.value == "fail"
    ]
    assert failed == ["a-002"]


def test_default_scanner_selection_runs_all_registered() -> None:
    result = run(EXAMPLE_POLICY, clock=fixed_clock)
    assert result.scanners == ("example", "uc")
    # no Spark locally: the uc scanner fails in isolation with a clear message
    assert result.status is RunStatus.PARTIAL
    uc_error = result.scanner_result("uc").error
    assert uc_error is not None and "spark" in uc_error.message.lower()
    assert result.scanner_result("example").status is ScannerStatus.SUCCEEDED


# --- configuration errors fail before running ---------------------------------------------


def test_unknown_scanner_fails_before_run() -> None:
    with pytest.raises(UnknownScannerError, match="'ai'"):
        run(EXAMPLE_POLICY, scanners=["example", "ai"])


def test_rules_for_unregistered_scanner_fail_before_run() -> None:
    policy = Policy(rules={"apps": {"idle_days": 30}})
    with pytest.raises(PolicyValidationError, match=r"rules\.apps"):
        _run(StubScanner, policy=policy)


def test_invalid_scanner_rules_fail_before_run() -> None:
    policy = Policy(rules={"example": {"idle_days": "thirty"}})
    with pytest.raises(PolicyValidationError, match=r"rules\.example\.idle_days"):
        run(policy=policy)


@pytest.mark.parametrize(
    "kwargs",
    [{}, {"config_path": str(EXAMPLE_POLICY), "policy": Policy()}],
)
def test_exactly_one_policy_source(kwargs: dict[str, object]) -> None:
    with pytest.raises(LakehouseGCError, match="exactly one"):
        run(**kwargs)  # type: ignore[arg-type]


def test_empty_scanner_list_rejected() -> None:
    with pytest.raises(RegistryError):
        run(EXAMPLE_POLICY, scanners=[])


def test_naive_clock_rejected() -> None:
    with pytest.raises(LakehouseGCError, match="timezone-aware"):
        run(EXAMPLE_POLICY, clock=lambda: datetime(2026, 1, 1))


# --- ordering and isolation -------------------------------------------------------------


def test_scanners_run_in_name_order() -> None:
    zulu, alpha = make_stub("zulu"), make_stub("alpha")
    result = _run(zulu, alpha, scanners=["zulu", "alpha"])
    assert result.scanners == ("alpha", "zulu")
    assert [r.name for r in result.scanner_results] == ["alpha", "zulu"]
    assert [f.scanner for f in result.findings] == ["alpha", "zulu"]


@pytest.mark.parametrize("phase", [Phase.DISCOVER, Phase.ANALYZE, Phase.SCORE, Phase.RECOMMEND])
def test_failure_is_isolated_per_phase(phase: Phase) -> None:
    broken = make_stub("broken", fail_phase=phase, outcomes=(("x", Outcome.FAIL),))
    healthy = make_stub("healthy")
    result = _run(broken, healthy)

    assert result.status is RunStatus.PARTIAL
    broken_result = result.scanner_result("broken")
    assert broken_result.status is ScannerStatus.FAILED
    assert broken_result.findings == () and broken_result.evaluations == ()
    assert broken_result.error is not None
    assert broken_result.error.phase is phase
    assert broken_result.error.error_type == "RuntimeError"
    assert result.errors == (broken_result.error,)

    assert result.scanner_result("healthy").status is ScannerStatus.SUCCEEDED
    assert [f.scanner for f in result.findings] == ["healthy"]
    # broken scanner's FAIL is not scored; scores flag the gap instead of hiding it
    assert result.scores.governance.score == 100.0
    assert not result.scores.complete
    assert result.scores.failed_scanners == ("broken",)


def test_initialization_failure_is_isolated() -> None:
    class Exploding(StubScanner):
        name = "exploding"
        rules = make_stub("exploding").rules

        def __init__(self) -> None:
            raise ValueError("cannot construct")

    result = _run(Exploding, make_stub("ok"))
    error = result.scanner_result("exploding").error
    assert error is not None and error.phase is Phase.INITIALIZE
    assert result.status is RunStatus.PARTIAL


def test_all_scanners_failing_marks_run_failed() -> None:
    result = _run(make_stub("one", fail_phase=Phase.DISCOVER))
    assert result.status is RunStatus.FAILED
    assert result.scores.overall is None


def test_keyboard_interrupt_is_not_swallowed() -> None:
    class Interrupted(StubScanner):
        def discover(self, ctx: ScannerContext) -> Sequence[Resource]:
            raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        _run(Interrupted)


# --- contract violations become scanner failures ------------------------------------------


class _DuplicateResources(StubScanner):
    def discover(self, ctx: ScannerContext) -> Sequence[Resource]:
        ref = ResourceRef("ws", "stub_asset", "r1")
        return [Resource(ref, "r1"), Resource(ref, "r1")]


class _WrongDiscoverType(StubScanner):
    def discover(self, ctx: ScannerContext) -> Sequence[Resource]:
        return None  # type: ignore[return-value]


class _UndeclaredRule(StubScanner):
    def analyze(
        self, ctx: ScannerContext, resources: Sequence[Resource]
    ) -> Sequence[RuleEvaluation]:
        return [RuleEvaluation(resources[0].ref, "stub.other", Outcome.PASS)]


class _EvaluatesUnknownResource(StubScanner):
    def analyze(
        self, ctx: ScannerContext, resources: Sequence[Resource]
    ) -> Sequence[RuleEvaluation]:
        return [
            RuleEvaluation(ResourceRef("ws", "stub_asset", "ghost"), "stub.check", Outcome.PASS)
        ]


class _DropsScored(StubScanner):
    def score(
        self, ctx: ScannerContext, evaluations: Sequence[RuleEvaluation]
    ) -> Sequence[ScoredEvaluation]:
        return []


class _EmitsExcluded(StubScanner):
    def recommend(
        self, ctx: ScannerContext, scored: Sequence[ScoredEvaluation]
    ) -> Sequence[Finding]:
        from lakehouse_gc.core.models import ExclusionStatus, Severity

        return [
            Finding(
                scored[0].evaluation.resource,
                "stub.check",
                Severity.LOW,
                Recommendation.EXCLUDED,
                exclusion=ExclusionStatus("stub_asset", "*", "sneaky"),
            )
        ]


class _FindingWithoutEvaluation(StubScanner):
    def recommend(
        self, ctx: ScannerContext, scored: Sequence[ScoredEvaluation]
    ) -> Sequence[Finding]:
        from lakehouse_gc.core.models import Severity

        ref = ResourceRef("ws", "stub_asset", "elsewhere")
        return [Finding(ref, "stub.check", Severity.LOW, Recommendation.KEEP)]


@pytest.mark.parametrize(
    ("cls", "phase", "match"),
    [
        (_DuplicateResources, Phase.DISCOVER, "duplicate resource"),
        (_WrongDiscoverType, Phase.DISCOVER, "must return a sequence"),
        (_UndeclaredRule, Phase.ANALYZE, "undeclared rule_id"),
        (_EvaluatesUnknownResource, Phase.ANALYZE, "not discovered or is excluded"),
        (_DropsScored, Phase.SCORE, "exactly one result per evaluation"),
        (_EmitsExcluded, Phase.RECOMMEND, "must not emit EXCLUDED"),
        (_FindingWithoutEvaluation, Phase.RECOMMEND, "no matching evaluation"),
    ],
)
def test_contract_violations_fail_scanner(cls: type, phase: Phase, match: str) -> None:
    result = _run(cls)
    error = result.scanner_result("stub").error
    assert error is not None
    assert error.error_type == "ScannerContractError"
    assert error.phase is phase
    assert match in error.message


def test_analyzing_excluded_resource_is_contract_violation() -> None:
    from lakehouse_gc.core.policy import Exclusion

    class AnalyzesEverything(StubScanner):
        def analyze(
            self, ctx: ScannerContext, resources: Sequence[Resource]
        ) -> Sequence[RuleEvaluation]:
            return [
                RuleEvaluation(ResourceRef("ws", "stub_asset", "r1"), "stub.check", Outcome.PASS)
            ]

    policy = Policy(exclusions=(Exclusion("stub_asset", "r1", "protected"),))
    error = _run(AnalyzesEverything, policy=policy).scanner_result("stub").error
    assert error is not None and "excluded" in error.message


# --- candidate gate and savings -----------------------------------------------------------


@pytest.mark.parametrize(
    ("confidence", "expected"),
    [
        (None, Recommendation.REVIEW),
        (69, Recommendation.REVIEW),
        (70, Recommendation.DELETE_CANDIDATE),
    ],
)
def test_candidate_gate(confidence: int | None, expected: Recommendation) -> None:
    class Candidate(StubScanner):
        recommendation = Recommendation.DELETE_CANDIDATE

        def score(
            self, ctx: ScannerContext, evaluations: Sequence[RuleEvaluation]
        ) -> Sequence[ScoredEvaluation]:
            return [replace(s, confidence=confidence) for s in super().score(ctx, evaluations)]

    finding = _run(Candidate).findings[0]
    assert finding.recommendation is expected
    if expected is Recommendation.REVIEW:
        assert finding.downgraded_from is Recommendation.DELETE_CANDIDATE


def test_savings_summary_from_findings() -> None:
    class WithSavings(StubScanner):
        outcomes = (("r1", Outcome.FAIL), ("r2", Outcome.FAIL))

        def recommend(
            self, ctx: ScannerContext, scored: Sequence[ScoredEvaluation]
        ) -> Sequence[Finding]:
            estimate = SavingsEstimate(Decimal("12.50"), "USD", "monthly", ("list",), "storage")
            return [replace(f, estimated_savings=estimate) for f in super().recommend(ctx, scored)]

    savings = _run(WithSavings).savings
    assert savings.total == Decimal("25.00")
    assert savings.resources_with_estimate == 2


def test_dimension_impacts_from_rule_spec() -> None:
    cost_stub = make_stub("coster", dimensions=frozenset({Dimension.COST}))
    result = _run(cost_stub, make_stub("gov"))
    assert result.scores.cost.evaluations == 1
    assert result.scores.governance.evaluations == 1
    assert result.scores.overall == 100.0


# --- example notebooks --------------------------------------------------------------------

EXAMPLES = sorted((ROOT / "examples").glob("[0-9][0-9]_*.ipynb"))


def _notebook(path: Path) -> dict[str, Any]:
    import json

    return json.loads(path.read_text(encoding="utf-8"))


def test_examples_are_numbered_by_level() -> None:
    names = [p.stem for p in EXAMPLES]
    assert len(names) == 8
    levels = [name.split("_")[1] for name in names]
    assert levels == sorted(levels, key=["easy", "intermediate", "advanced"].index)
    assert not list((ROOT / "examples").glob("*.py"))  # notebooks only, one copy


@pytest.mark.parametrize("path", EXAMPLES, ids=lambda p: p.stem)
def test_example_notebook_is_clean(path: Path) -> None:
    nb = _notebook(path)
    assert nb["nbformat"] == 4
    first = nb["cells"][0]
    assert first["cell_type"] == "markdown"
    assert "".join(first["source"]).startswith(f"# {path.stem[:2]} · ")
    for cell in nb["cells"]:
        if cell["cell_type"] == "code":  # never commit outputs: they could leak workspace data
            assert cell["outputs"] == [] and cell["execution_count"] is None, path.name
    pip = [c for c in nb["cells"] if "".join(c["source"]).startswith("%pip install")]
    assert len(pip) == 1 and "lakehouse_gc-0.2.2-py3-none-any.whl" in "".join(pip[0]["source"])


@pytest.mark.parametrize("path", EXAMPLES, ids=lambda p: p.stem)
def test_example_notebook_runs_end_to_end(path: Path) -> None:
    from test_uc import FakeSpark, sample_workspace

    code = "\n".join(
        "".join(line for line in cell["source"] if not line.lstrip().startswith("%"))
        for cell in _notebook(path)["cells"]
        if cell["cell_type"] == "code"
    )
    code = code.replace("/Volumes/main/default/lakehouse_gc/policy.yaml", str(EXAMPLE_POLICY))
    spark = FakeSpark(sample_workspace())
    shown: list[object] = []
    namespace = {"spark": spark, "display": shown.append, "__name__": "__main__"}
    exec(compile(code, path.name, "exec"), namespace)

    for value in namespace.values():
        if isinstance(value, RunResult):
            assert value.status is RunStatus.SUCCEEDED, (path.name, value.summary())
    statements = spark.client.statements
    assert statements
    assert all(s.startswith(("SELECT", "DESCRIBE", "SHOW CATALOGS")) for s in statements)
