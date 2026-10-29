"""Unity Catalog scanner tests against an in-memory fake of information_schema."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import replace
from datetime import datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest

from conftest import EXAMPLE_POLICY, FIXED_NOW, fixed_clock
from lakehouse_gc import run
from lakehouse_gc.core.errors import LakehouseGCError
from lakehouse_gc.core.evidence import EvidenceStatus
from lakehouse_gc.core.models import Phase, Recommendation, RunStatus, ScannerStatus
from lakehouse_gc.core.policy import Policy, load_policy
from lakehouse_gc.core.results import RunResult
from lakehouse_gc.core.sql import as_sql_client, quote_identifier

GB = 1024**3


def _ago(days: int) -> datetime:
    """Naive timestamp, as Spark returns them."""
    return (FIXED_NOW - timedelta(days=days)).replace(tzinfo=None)


def _table(schema: str, name: str, kind: str = "MANAGED", **row: Any) -> dict[str, Any]:
    return {
        "table_schema": schema,
        "table_name": name,
        "table_type": kind,
        "table_owner": row.get("owner", "team-data"),
        "comment": row.get("comment", "A table."),
        "created": _ago(900),
        "last_altered": row.get("last_altered", _ago(400)),
        "data_source_format": "DELTA",
    }


def sample_workspace() -> dict[str, Any]:
    schema_row = {"schema_owner": "team-data", "created": _ago(900), "last_altered": _ago(900)}
    return {
        "catalogs": [
            {
                "catalog_name": "main",
                "catalog_owner": "admins",
                "comment": "Main",
                "created": _ago(999),
                "last_altered": _ago(10),
            },
            {
                "catalog_name": "other",
                "catalog_owner": "admins",
                "comment": None,
                "created": None,
                "last_altered": None,
            },
            {
                "catalog_name": "system",
                "catalog_owner": "System user",
                "comment": None,
                "created": None,
                "last_altered": None,
            },
        ],
        "schemata": {
            "main": [
                {"schema_name": "information_schema", **schema_row, "comment": "built-in"},
                {"schema_name": "default", **schema_row, "comment": None},
                {"schema_name": "sales", **schema_row, "comment": "Sales data"},
                {"schema_name": "finance", **schema_row, "comment": "Finance"},
                {"schema_name": "tmp_scratch", **schema_row, "comment": "Scratch space"},
            ]
        },
        "tables": {
            "main": [
                _table("information_schema", "tables", "VIEW"),
                _table("sales", "orders", last_altered=_ago(5)),
                _table("sales", "legacy_big", comment=None, owner=""),
                _table("sales", "small_old"),
                _table("sales", "orders_bak"),
                _table("sales", "v_orders", "VIEW"),
                _table("sales", "broken", "EXTERNAL"),
                _table("finance", "ledger"),
            ]
        },
        "table_tags": {
            "main": [
                {
                    "schema_name": "sales",
                    "table_name": "orders",
                    "tag_name": "cost_center",
                    "tag_value": "cc1",
                },
            ]
        },
        "volumes": {"main": [{"volume_schema": "tmp_scratch", "volume_name": "files"}]},
        "details": {
            "main.sales.legacy_big": {"lastModified": _ago(400), "sizeInBytes": 50 * GB},
            "main.sales.small_old": {"lastModified": _ago(300), "sizeInBytes": 1 * GB},
            "main.sales.orders_bak": {"lastModified": _ago(10), "sizeInBytes": 20 * GB},
        },
        "detail_errors": {"main.sales.broken"},
    }


class FakeUC:
    """Routes the scanner's read-only statements to in-memory rows."""

    def __init__(self, data: dict[str, Any], fail: set[str] | None = None) -> None:
        self.data = data
        self.fail = fail or set()
        self.statements: list[str] = []
        self.params: list[dict[str, Any]] = []

    def query(
        self, statement: str, params: Mapping[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        self.statements.append(statement)
        self.params.append(dict(params or {}))
        catalog = (params or {}).get("catalog")
        for key in ("current_metastore", "catalogs", "schemata", "table_tags", "tables", "volumes"):
            if key in statement and key in self.fail:
                raise RuntimeError(f"PERMISSION_DENIED on {key}")
        if "current_metastore" in statement:
            return [{"metastore_id": "ms-1"}]
        if "information_schema.catalogs" in statement:
            return list(self.data["catalogs"])
        for key in ("schemata", "table_tags", "tables", "volumes"):
            if f"information_schema.{key}" in statement:
                return list(self.data[key].get(catalog, []))
        if statement.startswith("DESCRIBE DETAIL"):
            name = ".".join(re.findall(r"`((?:[^`]|``)*)`", statement)).replace("``", "`")
            if name in self.data["detail_errors"]:
                raise RuntimeError("DELTA_TABLE_NOT_FOUND")
            detail = self.data["details"].get(name)
            return [detail] if detail else []
        raise AssertionError(f"unexpected statement: {statement}")


class _Row(dict[str, Any]):
    def asDict(self, recursive: bool = False) -> dict[str, Any]:
        return dict(self)


class FakeSpark:
    """Just enough of SparkSession: sql(...).collect() and createDataFrame(...)."""

    def __init__(self, data: dict[str, Any], fail: set[str] | None = None) -> None:
        self.client = FakeUC(data, fail)

    def sql(self, statement: str, args: dict[str, Any] | None = None) -> Any:
        rows = self.client.query(statement, args)
        return SimpleNamespace(collect=lambda: [_Row(r) for r in rows])

    def createDataFrame(self, rows: list[tuple[Any, ...]], schema: str) -> Any:
        return SimpleNamespace(rows=rows, schema=schema)


def _policy(**uc_rules: Any) -> Policy:
    policy = load_policy(EXAMPLE_POLICY)
    rules = {**policy.rules["uc"], **uc_rules}
    return replace(policy, rules={**policy.rules, "uc": rules})


def _scan(spark: Any = None, policy: Policy | None = None, **kwargs: Any) -> tuple[RunResult, Any]:
    spark = spark if spark is not None else FakeSpark(sample_workspace())
    result = run(
        policy=policy or load_policy(EXAMPLE_POLICY),
        scanners=["uc"],
        spark=spark,
        clock=fixed_clock,
        **kwargs,
    )
    return result, spark


def _findings(result: RunResult) -> dict[tuple[str, str], Any]:
    return {(f.resource.resource_id, f.rule_id): f for f in result.findings}


# --- discovery and scope ------------------------------------------------------------------


def test_scan_discovers_scoped_objects_only() -> None:
    result, spark = _scan()
    assert result.status is RunStatus.SUCCEEDED
    resources = {r.ref.resource_id: r for r in result.scanner_result("uc").resources}
    assert set(resources) == {
        "main",
        "main.default",
        "main.sales",
        "main.finance",
        "main.tmp_scratch",
        "main.sales.orders",
        "main.sales.legacy_big",
        "main.sales.small_old",
        "main.sales.orders_bak",
        "main.sales.v_orders",
        "main.sales.broken",
        "main.finance.ledger",
    }
    assert {r.ref.scope_id for r in resources.values()} == {"ms-1"}
    queried_catalogs = {p.get("catalog") for p in spark.client.params} - {None}
    assert queried_catalogs == {"main"}  # "other" not in include_catalogs, "system" excluded


def test_statements_are_read_only_and_identifiers_quoted() -> None:
    _, spark = _scan()
    statements = spark.client.statements
    assert all(s.upper().startswith(("SELECT", "DESCRIBE DETAIL")) for s in statements)
    assert "DESCRIBE DETAIL `main`.`sales`.`legacy_big`" in statements
    assert quote_identifier("we`ird", "s") == "`we``ird`.`s`"


def test_table_scope_narrows_to_listed_tables() -> None:
    policy = load_policy(EXAMPLE_POLICY)
    policy = replace(policy, scope=replace(policy.scope, include_tables=("main.sales.orders",)))
    result, _ = _scan(policy=policy)
    names = {r.ref.resource_id for r in result.scanner_result("uc").resources}
    assert names == {"main", "main.sales", "main.sales.orders"}


def test_policy_exclusion_keeps_table_visible() -> None:
    findings = _findings(_scan()[0])
    excluded = findings[("main.finance.ledger", "framework.excluded")]
    assert excluded.recommendation is Recommendation.EXCLUDED
    assert excluded.exclusion is not None and excluded.exclusion.reason == "Retention policy"
    assert not any(k[0] == "main.finance.ledger" and k[1].startswith("uc.") for k in findings)


# --- governance checks --------------------------------------------------------------------


def test_owner_comment_tags_and_naming() -> None:
    findings = _findings(_scan()[0])
    assert findings[("main.sales.orders", "uc.owner_missing")].recommendation is Recommendation.KEEP
    assert (
        findings[("main.sales.legacy_big", "uc.owner_missing")].recommendation
        is Recommendation.TAG_OWNER
    )
    assert (
        findings[("main.sales.legacy_big", "uc.comment_missing")].recommendation
        is Recommendation.REVIEW
    )
    assert (
        findings[("main.sales.orders", "uc.required_tags_missing")].recommendation
        is Recommendation.KEEP
    )
    missing = findings[("main.sales.small_old", "uc.required_tags_missing")]
    assert missing.recommendation is Recommendation.TAG_OWNER
    assert "cost_center" in missing.reasons[0]

    temp = findings[("main.sales.orders_bak", "uc.temporary_name")]
    assert temp.recommendation is Recommendation.REVIEW
    assert temp.confidence == 50  # naming is a heuristic -> inferred
    assert (
        findings[("main.tmp_scratch", "uc.temporary_name")].recommendation is Recommendation.REVIEW
    )
    assert ("main", "uc.temporary_name") not in findings  # catalogs are not name-checked


def test_empty_containers_are_inferred() -> None:
    findings = _findings(_scan()[0])
    empty = findings[("main.default", "uc.empty_container")]
    assert empty.recommendation is Recommendation.REVIEW
    assert empty.confidence == 50
    assert empty.evidence[0].status is EvidenceStatus.INFERRED
    volume_only = findings[("main.tmp_scratch", "uc.empty_container")]
    assert volume_only.recommendation is Recommendation.KEEP
    assert findings[("main", "uc.empty_container")].recommendation is Recommendation.KEEP


# --- stale tables -------------------------------------------------------------------------


def test_stale_table_outcomes() -> None:
    result, _ = _scan()
    findings = _findings(result)
    big = findings[("main.sales.legacy_big", "uc.stale_table")]
    assert big.recommendation is Recommendation.REVIEW
    assert big.downgraded_from is Recommendation.ARCHIVE_CANDIDATE
    assert big.confidence == 50  # write time is inferred activity, never access
    assert "50.00 GB" in big.reasons[0]
    assert (
        findings[("main.sales.small_old", "uc.stale_table")].recommendation is Recommendation.KEEP
    )
    assert (
        findings[("main.sales.orders_bak", "uc.stale_table")].recommendation is Recommendation.KEEP
    )
    assert findings[("main.sales.orders", "uc.stale_table")].recommendation is Recommendation.KEEP
    broken = findings[("main.sales.broken", "uc.stale_table")]
    assert broken.recommendation is Recommendation.REVIEW
    assert broken.confidence is not None and broken.confidence < 70
    assert "DESCRIBE DETAIL failed" in broken.reasons[0]
    assert ("main.sales.v_orders", "uc.stale_table") not in findings  # views have no data
    deductions = {d.resource.resource_id for d in result.scores.cost.deductions}
    assert deductions == {"main.sales.legacy_big", "main.sales.broken"}


def test_min_size_zero_flags_small_tables() -> None:
    findings = _findings(_scan(policy=_policy(min_table_size_gb=0))[0])
    small = findings[("main.sales.small_old", "uc.stale_table")]
    assert small.downgraded_from is Recommendation.ARCHIVE_CANDIDATE


def test_detail_query_limit_is_reported_not_hidden() -> None:
    result, spark = _scan(policy=_policy(max_detail_queries=1))
    describes = [s for s in spark.client.statements if s.startswith("DESCRIBE")]
    assert len(describes) == 1
    limited = [
        f
        for f in result.findings
        if f.rule_id == "uc.stale_table" and "limit reached" in f.reasons[0]
    ]
    assert len(limited) == 3


def test_naive_spark_timestamps_are_treated_as_utc() -> None:
    result, _ = _scan()
    orders = next(r for r in result.scanner_result("uc").resources if r.name == "main.sales.orders")
    altered = orders.get_evidence("last_altered")
    assert altered is not None and str(altered.value).endswith("+00:00")


# --- missing data and failures -------------------------------------------------------------


def test_unreadable_tags_and_volumes_become_unknown_with_warnings() -> None:
    result, _ = _scan(spark=FakeSpark(sample_workspace(), fail={"table_tags", "volumes"}))
    assert result.status is RunStatus.SUCCEEDED
    findings = _findings(result)
    tags = findings[("main.sales.orders", "uc.required_tags_missing")]
    assert tags.recommendation is Recommendation.REVIEW and tags.confidence == 0
    default = findings[("main.default", "uc.empty_container")]
    assert default.recommendation is Recommendation.REVIEW and default.confidence == 0
    assert any("table tags" in w for w in result.warnings)
    assert any("volumes" in w for w in result.warnings)
    assert result.scores.governance.coverage is not None
    assert result.scores.governance.coverage < 1


def test_metastore_lookup_failure_is_a_warning() -> None:
    result, _ = _scan(spark=FakeSpark(sample_workspace(), fail={"current_metastore"}))
    assert {r.ref.scope_id for r in result.scanner_result("uc").resources} == {"unknown"}
    assert any("metastore" in w for w in result.warnings)


def test_catalog_listing_failure_fails_scanner_in_isolation() -> None:
    spark = FakeSpark(sample_workspace(), fail={"catalogs"})
    result = run(EXAMPLE_POLICY, scanners=["example", "uc"], spark=spark, clock=fixed_clock)
    assert result.status is RunStatus.PARTIAL
    error = result.scanner_result("uc").error
    assert error is not None and error.phase is Phase.DISCOVER
    assert "PERMISSION_DENIED" in error.message
    assert result.scanner_result("example").status is ScannerStatus.SUCCEEDED


def test_without_spark_the_scanner_explains_what_to_do() -> None:
    result = run(EXAMPLE_POLICY, scanners=["uc"], clock=fixed_clock)
    assert result.status is RunStatus.FAILED
    error = result.scanner_result("uc").error
    assert error is not None and "spark" in error.message.lower()


def test_sql_client_can_be_passed_directly() -> None:
    client = FakeUC(sample_workspace())
    result = run(EXAMPLE_POLICY, scanners=["uc"], spark=client, clock=fixed_clock)
    assert result.status is RunStatus.SUCCEEDED
    assert client.statements


def test_invalid_spark_argument_is_rejected_before_running() -> None:
    with pytest.raises(LakehouseGCError, match="SparkSession"):
        run(EXAMPLE_POLICY, scanners=["uc"], spark=object())
    assert as_sql_client(FakeUC(sample_workspace())) is not None


@pytest.mark.parametrize(
    ("rules", "path"),
    [
        ({"stale_table_days": 0}, "rules.uc"),
        ({"required_tags": "cost_center"}, "rules.uc.required_tags"),
        ({"stale_days": 30}, "rules.uc.stale_days"),
    ],
)
def test_invalid_uc_rules(rules: dict[str, Any], path: str) -> None:
    from lakehouse_gc.core.errors import PolicyValidationError

    policy = replace(load_policy(EXAMPLE_POLICY), rules={"uc": rules})
    with pytest.raises(PolicyValidationError) as info:
        _scan(policy=policy)
    assert info.value.path == path


# --- notebook helpers ---------------------------------------------------------------------


def test_summary_and_dataframe_helpers() -> None:
    result, spark = _scan()
    text = result.summary()
    assert "succeeded (uc)" in text
    assert "Governance:" in text and "Cost:" in text and "grade:" in text
    assert "Savings: unknown" in text
    frame = result.to_dataframe(spark)
    assert len(frame.rows) == len(result.findings)
    assert frame.schema.startswith("scanner STRING")
    row = dict(zip([c.split()[0] for c in frame.schema.split(", ")], frame.rows[0], strict=True))
    assert row["scanner"] == "uc"
