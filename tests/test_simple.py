from __future__ import annotations

from typing import Any

import pytest

from conftest import EXAMPLE_POLICY, fixed_clock
from lakehouse_gc import check, define_scanner, fail, ok, run, unknown
from lakehouse_gc.core.errors import (
    DuplicateScannerError,
    InvalidScannerError,
    PolicyValidationError,
    RegistryError,
)
from lakehouse_gc.core.evidence import EvidenceStatus
from lakehouse_gc.core.models import (
    Dimension,
    Outcome,
    Phase,
    Recommendation,
    RunStatus,
    ScannerStatus,
    Severity,
)
from lakehouse_gc.core.policy import Exclusion, Policy
from lakehouse_gc.core.registry import get_default_registry
from lakehouse_gc.core.results import RunResult

ITEMS = [{"name": "a", "v": 1}, {"name": "b", "v": 2}, {"name": "c", "v": None}]


def _run(*scanners: Any, policy: Policy | None = None) -> RunResult:
    return run(policy=policy or Policy(), scanners=list(scanners), clock=fixed_clock)


def _by_resource(result: RunResult, rule_id: str) -> dict[str, Any]:
    return {f.resource.resource_id: f for f in result.findings if f.rule_id == rule_id}


def test_bool_and_none_returns_map_to_outcomes() -> None:
    def positive(item):  # type: ignore[no-untyped-def]
        return None if item["v"] is None else item["v"] > 1

    result = _run(define_scanner("simple", resources=ITEMS, checks=[positive]))
    findings = _by_resource(result, "simple.positive")
    assert findings["a"].recommendation is Recommendation.REVIEW  # default on_fail
    assert findings["b"].recommendation is Recommendation.KEEP
    assert findings["c"].recommendation is Recommendation.REVIEW
    assert findings["c"].evidence[0].status is EvidenceStatus.UNAVAILABLE
    assert (findings["a"].confidence, findings["b"].confidence, findings["c"].confidence) == (
        100,
        100,
        0,
    )
    evaluations = {
        s.evaluation.resource.resource_id: s.evaluation.outcome
        for s in result.scanner_result("simple").evaluations
    }
    assert evaluations == {"a": Outcome.FAIL, "b": Outcome.PASS, "c": Outcome.INDETERMINATE}


def test_result_helpers_carry_reasons_and_inferred_confidence() -> None:
    @check(
        rule="sized",
        title="Has size",
        severity="high",
        dimension="cost",
        on_fail="delete_candidate",
    )
    def sized(item):  # type: ignore[no-untyped-def]
        if item["v"] is None:
            return unknown("size unavailable")
        return fail("too small", inferred=True) if item["v"] < 2 else ok("big enough")

    scanner = define_scanner("sizes", resources=ITEMS, checks=[sized])
    assert scanner.rules[0].severity is Severity.HIGH
    assert scanner.rules[0].dimensions == frozenset({Dimension.COST})
    findings = _by_resource(_run(scanner), "sizes.sized")
    a = findings["a"]
    assert a.reasons[0] == "too small"
    assert a.confidence == 50
    # inferred evidence (50) is below the 70 gate, so the delete candidate is downgraded
    assert a.recommendation is Recommendation.REVIEW
    assert a.downgraded_from is Recommendation.DELETE_CANDIDATE
    assert findings["b"].reasons[0] == "big enough"
    assert findings["c"].reasons[0] == "size unavailable"


def test_bare_decorator_and_default_title() -> None:
    @check
    def has_value(item):  # type: ignore[no-untyped-def]
        return item["v"] is not None

    scanner = define_scanner("bare", resources=ITEMS, checks=[has_value])
    assert scanner.rules[0].rule_id == "bare.has_value"
    assert scanner.rules[0].title == "Has value"
    finding = _by_resource(_run(scanner), "bare.has_value")["a"]
    assert finding.reasons == ("Check 'Has value' passed.",)


def test_config_defaults_and_policy_override() -> None:
    def above(item, config):  # type: ignore[no-untyped-def]
        return None if item["v"] is None else item["v"] > config["threshold"]

    scanner = define_scanner("cfg", resources=ITEMS, checks=[above], config={"threshold": 1})
    default = _by_resource(_run(scanner), "cfg.above")
    assert default["b"].recommendation is Recommendation.KEEP
    policy = Policy(rules={"cfg": {"threshold": 5}})
    overridden = _by_resource(_run(scanner, policy=policy), "cfg.above")
    assert overridden["b"].recommendation is Recommendation.REVIEW


@pytest.mark.parametrize(
    ("rules", "path"),
    [({"threshold": "high"}, "rules.cfg.threshold"), ({"treshold": 1}, "rules.cfg.treshold")],
)
def test_config_from_policy_is_validated(rules: dict[str, object], path: str) -> None:
    scanner = define_scanner("cfg", resources=ITEMS, checks=[_always_true], config={"threshold": 1})
    with pytest.raises(PolicyValidationError) as info:
        _run(scanner, policy=Policy(rules={"cfg": rules}))
    assert info.value.path == path


def _always_true(item: Any) -> bool:
    return True


def test_resources_can_be_a_function_of_config() -> None:
    def load(config):  # type: ignore[no-untyped-def]
        return [{"name": f"t{i}"} for i in range(config["count"])]

    scanner = define_scanner("gen", resources=load, checks=[_always_true], config={"count": 3})
    assert len(_run(scanner).findings) == 3


def test_optional_resource_keys_and_exclusions() -> None:
    items = [
        {"name": "main.finance.ledger", "id": "42", "type": "table", "scope": "ws1", "owner": "x"},
        {"name": "main.sales.orders"},
    ]
    scanner = define_scanner(
        "uc_demo", resources=items, checks=[_always_true], resource_type="table"
    )
    policy = Policy(exclusions=(Exclusion("table", "main.finance.*", "Retention"),))
    result = _run(scanner, policy=policy)
    resources = result.scanner_result("uc_demo").resources
    assert {r.ref.key for r in resources} == {"ws1/table/42", "default/table/main.sales.orders"}
    assert [f.resource.resource_id for f in result.excluded_findings] == ["42"]


def test_mixing_builtin_names_and_classes_with_policy_yaml() -> None:
    scanner = define_scanner("extra", resources=ITEMS, checks=[_always_true])
    result = run(EXAMPLE_POLICY, scanners=["example", scanner], clock=fixed_clock)
    assert result.status is RunStatus.SUCCEEDED
    assert result.scanners == ("example", "extra")
    assert "extra" not in get_default_registry()


@pytest.mark.parametrize(
    ("behaviour", "match"),
    [(lambda item: "yes", "returned str"), (lambda item: 1 / 0, "division by zero")],
)
def test_bad_check_return_or_exception_fails_only_that_scanner(behaviour: Any, match: str) -> None:
    @check(rule="bad")
    def bad(item):  # type: ignore[no-untyped-def]
        return behaviour(item)

    broken = define_scanner("broken", resources=ITEMS, checks=[bad])
    healthy = define_scanner("healthy", resources=ITEMS, checks=[_always_true])
    result = _run(broken, healthy)
    assert result.status is RunStatus.PARTIAL
    error = result.scanner_result("broken").error
    assert error is not None and error.phase is Phase.ANALYZE
    assert match in error.message
    assert result.scanner_result("healthy").status is ScannerStatus.SUCCEEDED


@pytest.mark.parametrize(
    ("resources", "match"),
    [
        ([{"id": "1"}], "needs a non-empty 'name'"),
        (["plain"], "must be mappings"),
        ({"name": "x"}, "list"),
    ],
)
def test_bad_resources_fail_discover(resources: Any, match: str) -> None:
    result = _run(define_scanner("res", resources=resources, checks=[_always_true]))
    error = result.scanner_result("res").error
    assert error is not None and error.phase is Phase.DISCOVER
    assert match in error.message


def test_definition_errors() -> None:
    with pytest.raises(InvalidScannerError, match="must match"):
        define_scanner("Bad-Name", resources=[], checks=[_always_true])
    with pytest.raises(InvalidScannerError, match="at least one check"):
        define_scanner("empty", resources=[], checks=[])
    with pytest.raises(InvalidScannerError, match="lambda"):
        define_scanner("lam", resources=[], checks=[lambda item: True])
    with pytest.raises(InvalidScannerError, match="duplicate check"):
        define_scanner("dup", resources=[], checks=[_always_true, _always_true])
    with pytest.raises(InvalidScannerError, match="must default to"):
        define_scanner("cfg", resources=[], checks=[_always_true], config={"x": {"nested": 1}})
    with pytest.raises(InvalidScannerError, match="invalid severity"):
        check(severity="urgent")
    with pytest.raises(InvalidScannerError, match="on_fail cannot be 'excluded'"):
        check(on_fail="excluded")


def test_name_clash_with_builtin_is_rejected() -> None:
    clash = define_scanner("example", resources=ITEMS, checks=[_always_true])
    with pytest.raises(DuplicateScannerError):
        _run(clash)


def test_invalid_scanner_entries_rejected() -> None:
    with pytest.raises(RegistryError, match="names or Scanner classes"):
        _run(42)
