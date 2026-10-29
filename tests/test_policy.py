from __future__ import annotations

import textwrap
from dataclasses import dataclass
from datetime import date

import pytest

from conftest import EXAMPLE_POLICY, SAMPLE_POLICY
from lakehouse_gc.core.config import build_scanner_config, resolve_scanner_configs
from lakehouse_gc.core.errors import (
    PolicyError,
    PolicyValidationError,
    UnsupportedPolicyVersionError,
)
from lakehouse_gc.core.models import Dimension, Resource, ResourceRef, Severity
from lakehouse_gc.core.policy import (
    ActionsConfig,
    Exclusion,
    Policy,
    ScoringConfig,
    load_policy,
    loads_policy,
)
from lakehouse_gc.scanners.example import ExampleConfig


def _load(text: str) -> Policy:
    return loads_policy(textwrap.dedent(text))


def _error(text: str) -> PolicyValidationError:
    with pytest.raises(PolicyValidationError) as info:
        _load(text)
    return info.value


def test_example_policy_loads() -> None:
    policy = load_policy(EXAMPLE_POLICY)
    assert policy.policy_version == 1
    assert policy.scope.include_catalogs == ("main",)
    assert policy.scope.exclude_catalogs == ("system", "samples", "__databricks_internal")
    assert policy.exclusions[0].pattern == "main.finance.*"
    assert policy.exclusions[-1].expires_on == date(2025, 1, 1)
    assert policy.rules["uc"]["required_tags"] == ["cost_center"]
    assert policy.rules["example"] == {"idle_days": 30, "require_owner": True}
    assert policy.scoring.dimension_weights == {Dimension.GOVERNANCE: 0.5, Dimension.COST: 0.5}
    assert policy.actions.mode == "report_only"
    assert policy.source == str(EXAMPLE_POLICY)


def test_editable_sample_policy_is_valid_and_runs() -> None:
    from lakehouse_gc import run
    from lakehouse_gc.core.results import RunResult

    policy = load_policy(SAMPLE_POLICY)
    assert policy.policy_version == 1
    assert isinstance(run(config_path=str(SAMPLE_POLICY), scanners=["example"]), RunResult)


def test_minimal_policy_uses_defaults() -> None:
    policy = _load("policy_version: 1\n")
    assert policy.exclusions == ()
    assert policy.scoring.unknown_penalty == 0.5
    assert policy.scoring.severity_weights[Severity.HIGH] == 5
    assert policy.actions.min_candidate_confidence == 70
    assert policy.scoring.grade_boundaries[0] == ("A", 90)


def test_missing_file_is_policy_error(tmp_path: object) -> None:
    with pytest.raises(PolicyError, match="cannot read policy file"):
        load_policy("/nonexistent/policy.yaml")


@pytest.mark.parametrize(
    ("text", "match"),
    [
        ("", "is empty"),
        ("- a\n- b\n", "expected a mapping, got list"),
        ("policy_version: [\n", "invalid YAML"),
    ],
)
def test_unparseable_policies(text: str, match: str) -> None:
    with pytest.raises(PolicyError, match=match):
        _load(text)


def test_duplicate_keys_rejected() -> None:
    error = _error(
        """
        policy_version: 1
        actions:
          allow_delete: false
          allow_delete: true
        """
    )
    assert "duplicate key 'allow_delete'" in str(error)


def test_version_is_required() -> None:
    assert _error("scope: {}\n").path == "policy_version"


@pytest.mark.parametrize("version", ["2", "0"])
def test_unsupported_version(version: str) -> None:
    with pytest.raises(UnsupportedPolicyVersionError, match="unsupported policy version"):
        _load(f"policy_version: {version}\n")


@pytest.mark.parametrize("version", ["true", '"1"', "1.0"])
def test_version_must_be_integer(version: str) -> None:
    assert "must be an integer" in str(_error(f"policy_version: {version}\n"))


@pytest.mark.parametrize(
    ("text", "path", "hint"),
    [
        ("policy_version: 1\nscorin: {}\n", "scorin", "did you mean 'scoring'"),
        (
            "policy_version: 1\nscoring:\n  cost_wieght: 1\n",
            "scoring.cost_wieght",
            "did you mean 'cost_weight'",
        ),
        ("policy_version: 1\nactions:\n  dry_run: true\n", "actions.dry_run", "unknown key"),
        (
            "policy_version: 1\nexclusions:\n  - {resource_type: t, pattern: p, reason: r, x: 1}\n",
            "exclusions[0].x",
            "unknown key",
        ),
    ],
)
def test_unknown_keys_report_path(text: str, path: str, hint: str) -> None:
    error = _error(text)
    assert error.path == path
    assert hint in str(error)


@pytest.mark.parametrize(
    ("text", "path"),
    [
        ("policy_version: 1\nactions:\n  allow_delete: true\n", "actions.allow_delete"),
        ("policy_version: 1\nactions:\n  allow_pause: true\n", "actions.allow_pause"),
        ("policy_version: 1\nactions:\n  mode: enforce\n", "actions.mode"),
        ("policy_version: 1\nactions:\n  allow_delete: 'no'\n", "actions.allow_delete"),
        (
            "policy_version: 1\nactions:\n  min_candidate_confidence: 101\n",
            "actions.min_candidate_confidence",
        ),
    ],
)
def test_mutation_and_invalid_actions_rejected(text: str, path: str) -> None:
    assert _error(text).path == path


@pytest.mark.parametrize(
    ("scoring", "path"),
    [
        ("governance_weight: -1", "scoring.governance_weight"),
        ("governance_weight: 0\n  cost_weight: 0", "scoring.governance_weight"),
        ("cost_weight: abc", "scoring.cost_weight"),
        ("cost_weight: true", "scoring.cost_weight"),
        ("unknown_penalty: 0", "scoring.unknown_penalty"),
        ("unknown_penalty: 1.5", "scoring.unknown_penalty"),
        ("severity_weights: {hgh: 1}", "scoring.severity_weights.hgh"),
        ("severity_weights: {high: -2}", "scoring.severity_weights.high"),
        ("grade_boundaries: {A: 90, B: 80}", "scoring.grade_boundaries"),
        ("grade_boundaries: {A: 90, B: 90, F: 0}", "scoring.grade_boundaries"),
        ("grade_boundaries: {A: 120, F: 0}", "scoring.grade_boundaries.A"),
        ("grade_boundaries: {}", "scoring.grade_boundaries"),
    ],
)
def test_invalid_scoring(scoring: str, path: str) -> None:
    assert _error(f"policy_version: 1\nscoring:\n  {scoring}\n").path == path


def test_partial_severity_weights_merge_with_defaults() -> None:
    policy = _load("policy_version: 1\nscoring:\n  severity_weights: {high: 10}\n")
    assert policy.scoring.severity_weights[Severity.HIGH] == 10
    assert policy.scoring.severity_weights[Severity.MEDIUM] == 3


def test_grade_boundaries_sorted_descending() -> None:
    policy = _load("policy_version: 1\nscoring:\n  grade_boundaries: {Fail: 0, Pass: 50}\n")
    assert policy.scoring.grade_boundaries == (("Pass", 50.0), ("Fail", 0.0))


@pytest.mark.parametrize(
    ("item", "path"),
    [
        ("{pattern: p, reason: r}", "exclusions[0].resource_type"),
        ("{resource_type: t, pattern: p}", "exclusions[0].reason"),
        ("{resource_type: t, pattern: p, reason: ''}", "exclusions[0].reason"),
        ("{resource_type: t, pattern: p, reason: r, expires_on: soon}", "exclusions[0].expires_on"),
        (
            "{resource_type: t, pattern: p, reason: r, expires_on: 2026-01-01T00:00:00}",
            "exclusions[0].expires_on",
        ),
    ],
)
def test_invalid_exclusions(item: str, path: str) -> None:
    assert _error(f"policy_version: 1\nexclusions:\n  - {item}\n").path == path


def test_exclusion_date_string_accepted() -> None:
    policy = _load(
        "policy_version: 1\nexclusions:\n"
        "  - {resource_type: t, pattern: p, reason: r, expires_on: '2026-03-01'}\n"
    )
    assert policy.exclusions[0].expires_on == date(2026, 3, 1)


def test_exclusion_matching_and_expiry() -> None:
    exclusion = Exclusion("Table", "MAIN.finance.*", "Retention", date(2026, 1, 31))
    resource = Resource(ResourceRef("ws", "table", "1"), name="main.Finance.ledger")
    other_type = Resource(ResourceRef("ws", "volume", "2"), name="main.finance.ledger")
    assert exclusion.matches(resource)
    assert not exclusion.matches(other_type)
    assert not exclusion.is_expired(date(2026, 1, 31))
    assert exclusion.is_expired(date(2026, 2, 1))


def test_programmatic_configs_are_validated() -> None:
    with pytest.raises(PolicyValidationError):
        ActionsConfig(allow_delete=True)
    with pytest.raises(PolicyValidationError):
        ScoringConfig(unknown_penalty=0)
    with pytest.raises(UnsupportedPolicyVersionError):
        Policy(policy_version=2)


# --- scanner rule sections --------------------------------------------------------------


def test_rules_for_unregistered_scanner_rejected_with_hint() -> None:
    with pytest.raises(PolicyValidationError) as info:
        resolve_scanner_configs({"exampel": {}}, {"example": ExampleConfig})
    assert info.value.path == "rules.exampel"
    assert "did you mean 'example'" in str(info.value)


def test_missing_rules_section_uses_defaults() -> None:
    configs = resolve_scanner_configs({}, {"example": ExampleConfig})
    assert configs == {"example": ExampleConfig()}


@pytest.mark.parametrize(
    ("data", "path"),
    [
        ({"idle_dayz": 3}, "rules.example.idle_dayz"),
        ({"idle_days": "30"}, "rules.example.idle_days"),
        ({"idle_days": True}, "rules.example.idle_days"),
        ({"require_owner": 1}, "rules.example.require_owner"),
        ({"idle_days": 0}, "rules.example"),
    ],
)
def test_invalid_scanner_rules(data: dict[str, object], path: str) -> None:
    with pytest.raises(PolicyValidationError) as info:
        build_scanner_config(ExampleConfig, data, "rules.example")
    assert info.value.path == path


@dataclass(frozen=True)
class _RichConfig:
    required: int
    ratio: float = 1.0
    tags: tuple[str, ...] = ()
    cutoff: date | None = None


def test_scanner_config_types() -> None:
    config = build_scanner_config(
        _RichConfig,
        {"required": 2, "ratio": 3, "tags": ["a", "b"], "cutoff": date(2026, 1, 1)},
        "rules.x",
    )
    assert config == _RichConfig(2, 3.0, ("a", "b"), date(2026, 1, 1))
    with pytest.raises(PolicyValidationError, match="required key is missing"):
        build_scanner_config(_RichConfig, {}, "rules.x")
    with pytest.raises(PolicyValidationError) as info:
        build_scanner_config(_RichConfig, {"required": 1, "tags": ["a", 2]}, "rules.x")
    assert info.value.path == "rules.x.tags[1]"


def test_scanner_without_config_rejects_rules() -> None:
    with pytest.raises(PolicyValidationError, match="does not accept any rules"):
        build_scanner_config(None, {"x": 1}, "rules.bare")
    assert build_scanner_config(None, None, "rules.bare") is None


# --- scope ------------------------------------------------------------------------------


def _scope(text: str) -> Policy:
    return _load(f"policy_version: 1\nscope:\n{textwrap.indent(textwrap.dedent(text), '  ')}")


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("include_catalogs", "main.sales"),
        ("include_schemas", "sales"),
        ("exclude_tables", "main.orders"),
    ],
)
def test_scope_patterns_need_qualified_names(key: str, value: str) -> None:
    error = _error(f"policy_version: 1\nscope:\n  {key}: ['{value}']\n")
    assert error.path == f"scope.{key}[0]"
    assert "part name" in error.message


def test_empty_scope_includes_everything() -> None:
    scope = _load("policy_version: 1\n").scope
    assert scope.includes_catalog("anything")
    assert scope.includes_table("a", "b", "c")


def test_scope_excludes_win_and_are_case_insensitive() -> None:
    scope = _scope(
        """
        include_catalogs: ["main", "prod_*"]
        exclude_catalogs: ["prod_old"]
        exclude_schemas: ["main.scratch"]
        exclude_tables: ["main.sales.tmp_*"]
        """
    ).scope
    assert scope.includes_catalog("MAIN")
    assert scope.includes_catalog("prod_eu")
    assert not scope.includes_catalog("prod_old")
    assert not scope.includes_catalog("dev")
    assert not scope.includes_schema("main", "Scratch")
    assert scope.includes_table("main", "sales", "orders")
    assert not scope.includes_table("main", "sales", "tmp_x")


def test_narrow_includes_narrow_the_levels_above() -> None:
    scope = _scope('include_tables: ["main.sales.orders", "main.sales.fact_*"]').scope
    assert scope.includes_catalog("main")
    assert not scope.includes_catalog("other")
    assert scope.includes_schema("main", "sales")
    assert not scope.includes_schema("main", "hr")
    assert scope.includes_table("main", "sales", "fact_daily")
    assert not scope.includes_table("main", "sales", "customers")
