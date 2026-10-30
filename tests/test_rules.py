from __future__ import annotations

from dataclasses import replace
from datetime import date

import pytest

from conftest import EXAMPLE_POLICY, fixed_clock
from lakehouse_gc import check, define_scanner, describe_rules, rules_dataframe, rules_text, run
from lakehouse_gc.core.errors import InvalidScannerError, PolicyValidationError
from lakehouse_gc.core.models import Dimension, Recommendation, Severity
from lakehouse_gc.core.policy import Exclusion, load_policy
from lakehouse_gc.core.registry import ScannerRegistry
from lakehouse_gc.core.scanner import RuleSpec
from lakehouse_gc.scanners.uc import UnityCatalogScanner
from test_uc import FakeSpark, sample_workspace


def _rows(**kwargs: object) -> dict[str, dict[str, str]]:
    return {row["rule"]: row for row in describe_rules(EXAMPLE_POLICY, **kwargs)}  # type: ignore[arg-type]


def test_describes_every_uc_rule_with_policy_settings() -> None:
    rows = _rows(scanners=["uc"])
    assert set(rows) == {r.rule_id for r in UnityCatalogScanner.rules} | {"framework.excluded"}
    assert all(row["meaning"] for row in rows.values())
    stale = rows["uc.stale_table"]
    assert stale["severity"] == "high (weight 5)"
    assert stale["affects"] == "cost"
    assert stale["on_fail"] == "archive_candidate"
    assert stale["settings"] == (
        "stale_table_days = 180; min_table_size_gb = 10; max_detail_queries = 200"
    )
    assert rows["uc.required_tags_missing"]["settings"] == "required_tags = [cost_center]"
    assert "*.__materialization*" in rows["framework.excluded"]["settings"]


def test_settings_reflect_policy_overrides() -> None:
    policy = load_policy(EXAMPLE_POLICY)
    policy = replace(policy, rules={"uc": {"stale_table_days": 30, "required_tags": []}})
    rows = {r["rule"]: r for r in describe_rules(policy=policy, scanners=["uc"])}
    assert rows["uc.stale_table"]["settings"].startswith("stale_table_days = 30;")
    assert rows["uc.required_tags_missing"]["settings"] == "required_tags = [] (off)"


def test_invalid_policy_fails_like_run() -> None:
    policy = replace(load_policy(EXAMPLE_POLICY), rules={"uc": {"stale_days": 1}})
    with pytest.raises(PolicyValidationError, match=r"rules\.uc\.stale_days"):
        describe_rules(policy=policy, scanners=["uc"])


def test_custom_check_docstring_becomes_meaning() -> None:
    @check(severity="low", on_fail="tag_owner")
    def has_owner(item):  # type: ignore[no-untyped-def]
        """Every dataset needs an owner.

        Longer details are not shown.
        """
        return bool(item.get("owner"))

    tags = define_scanner("tags", resources=[], checks=[has_owner])
    row = _rows(scanners=[tags])["tags.has_owner"]
    assert row["meaning"] == "Every dataset needs an owner."
    assert row["on_fail"] == "tag_owner"
    assert row["severity"] == "low (weight 1)"


def test_dataframe_and_text_helpers() -> None:
    spark = FakeSpark(sample_workspace())
    frame = rules_dataframe(spark, EXAMPLE_POLICY, scanners=["uc"])
    assert frame.schema.startswith("scanner STRING, rule STRING")
    assert len(frame.rows) == len(UnityCatalogScanner.rules) + 1
    assert all(isinstance(value, str) for row in frame.rows for value in row)
    text = rules_text(EXAMPLE_POLICY, scanners=["uc"])
    assert "uc.stale_table - Large table not modified" in text
    assert "settings: stale_table_days = 180" in text


def test_rule_settings_must_exist_in_config() -> None:
    class Bad(UnityCatalogScanner):
        name = "bad"
        rules = (
            RuleSpec(
                "bad.rule",
                "x",
                Severity.LOW,
                frozenset({Dimension.GOVERNANCE}),
                settings=("nope",),
            ),
        )

    with pytest.raises(InvalidScannerError, match="unknown settings: nope"):
        ScannerRegistry().register(Bad)
    with pytest.raises(ValueError, match="on_fail"):
        RuleSpec("x.y", "x", Severity.LOW, frozenset(), on_fail=Recommendation.EXCLUDED)


# --- exclusions with the wrong resource_type ----------------------------------------------


def _workspace_with_internal_table() -> dict[str, object]:
    data = sample_workspace()
    internal = dict(data["tables"]["main"][1])  # type: ignore[index]
    internal["table_name"] = "__materialization_mat_4de0b84e_clickstream_raw_1"
    data["tables"]["main"].append(internal)  # type: ignore[index]
    return data


def _run_with_exclusion(resource_type: str) -> object:
    policy = load_policy(EXAMPLE_POLICY)
    policy = replace(
        policy,
        exclusions=(Exclusion(resource_type, "*.__materialization*", "Internal tables"),),
    )
    return run(
        policy=policy,
        scanners=["uc"],
        spark=FakeSpark(_workspace_with_internal_table()),
        clock=fixed_clock,
    )


def test_internal_table_excluded_with_resource_type_table() -> None:
    result = _run_with_exclusion("table")
    name = "main.sales.__materialization_mat_4de0b84e_clickstream_raw_1"
    rules = {f.rule_id for f in result.findings if f.resource.resource_id == name}  # type: ignore[attr-defined]
    assert rules == {"framework.excluded"}
    assert not any("never matches" in w for w in result.warnings)  # type: ignore[attr-defined]


def test_wrong_resource_type_warns_instead_of_silently_doing_nothing() -> None:
    result = _run_with_exclusion("internal_tables")
    name = "main.sales.__materialization_mat_4de0b84e_clickstream_raw_1"
    assert ("uc.stale_table") in {
        f.rule_id
        for f in result.findings  # type: ignore[attr-defined]
        if f.resource.resource_id == name
    }
    warning = next(w for w in result.warnings if "never matches" in w)  # type: ignore[attr-defined]
    assert "'internal_tables'" in warning
    assert "catalog, example_asset, job, schema, table" in warning
    assert "never matches" in result.summary()  # type: ignore[attr-defined]


def test_sample_policy_has_no_unmatched_exclusions() -> None:
    result = run(EXAMPLE_POLICY, scanners=["example"], clock=fixed_clock)
    assert not any("never matches" in w for w in result.warnings)


def test_expired_exclusions_still_reported() -> None:
    policy = replace(
        load_policy(EXAMPLE_POLICY),
        exclusions=(Exclusion("example_asset", "*", "hold", date(2026, 1, 1)),),
    )
    result = run(policy=policy, scanners=["example"], clock=fixed_clock)
    assert result.excluded_findings == ()
    assert any("expired on 2026-01-01" in w for w in result.warnings)
