"""Milestone A: configurable sources, per-catalog fallback, org views, check_access."""

from __future__ import annotations

from dataclasses import replace
from typing import Any

import pytest

from conftest import EXAMPLE_POLICY, fixed_clock
from lakehouse_gc import check_access, run
from lakehouse_gc.core.errors import InvalidScannerError, PolicyValidationError
from lakehouse_gc.core.models import Outcome, Phase, Recommendation, RunStatus
from lakehouse_gc.core.policy import Policy, load_policy, loads_policy
from lakehouse_gc.core.registry import ScannerRegistry
from lakehouse_gc.core.results import RunResult
from lakehouse_gc.core.sources import SourceColumn, SourceSpec
from lakehouse_gc.scanners.uc import UnityCatalogScanner
from test_uc import FakeSpark, FakeUC, sample_workspace


def _policy(sources: dict[str, Any]) -> Policy:
    return replace(load_policy(EXAMPLE_POLICY), sources=sources)


def _scan(policy: Policy, spark: Any) -> RunResult:
    return run(policy=policy, scanners=["uc"], spark=spark, clock=fixed_clock)


def _findings(result: RunResult) -> list[tuple[str, str, str]]:
    return sorted(
        (f.resource.resource_id, f.rule_id, f.recommendation.value) for f in result.findings
    )


BASELINE = _findings(_scan(load_policy(EXAMPLE_POLICY), FakeSpark(sample_workspace())))


# --- policy validation ----------------------------------------------------------------------


def test_sources_section_parses_from_yaml() -> None:
    policy = loads_policy(
        "policy_version: 1\n"
        "sources:\n"
        "  information_schema: per_catalog\n"
        "  tables:\n"
        "    table: governance.uc_meta.all_tables\n"
        "    columns: {table_owner: owner}\n"
    )
    assert policy.sources["information_schema"] == "per_catalog"
    assert policy.sources["tables"]["columns"] == {"table_owner": "owner"}


@pytest.mark.parametrize(
    ("sources", "path", "message"),
    [
        ({"tabels": "a.b.c"}, "sources.tabels", "did you mean 'tables'"),
        ({"information_schema": "just_a_catalog"}, "sources.information_schema", "2-part"),
        ({"tables": "a.b"}, "sources.tables", "3-part"),
        ({"tables": {"table": "a.b.c", "colums": {}}}, "sources.tables.colums", "unknown key"),
        ({"tables": {"columns": {"owner": "x"}}}, "sources.tables.columns.owner", "unknown column"),
        ({"tables": {"columns": {"table_owner": ""}}}, "sources.tables.columns.table_owner", ""),
        ({"tables": 5}, "sources.tables", "expected a table name or a mapping"),
    ],
)
def test_invalid_sources_fail_before_scanning(
    sources: dict[str, Any], path: str, message: str
) -> None:
    spark = FakeSpark(sample_workspace())
    with pytest.raises(PolicyValidationError) as info:
        _scan(_policy(sources), spark)
    assert info.value.path == path
    assert message in str(info.value)
    assert spark.client.statements == []  # nothing was queried


# --- where the scanner reads from -----------------------------------------------------------


def test_default_reads_system_information_schema() -> None:
    result = _scan(load_policy(EXAMPLE_POLICY), FakeSpark(sample_workspace()))
    table = next(r for r in result.scanner_result("uc").resources if r.name == "main.sales.orders")
    assert table.get_evidence("owner").source == "system.information_schema.tables"  # type: ignore[union-attr]
    assert result.limitations == (
        "DESCRIBE DETAIL failed for 1 table(s) (for example, non-Delta tables or missing "
        "privileges): uc.stale_table is unknown for them.",
    )


def test_per_catalog_works_when_system_catalog_is_blocked() -> None:
    assert _scan(
        load_policy(EXAMPLE_POLICY), FakeSpark(sample_workspace(), fail={"system."})
    ).status is (RunStatus.FAILED)

    spark = FakeSpark(sample_workspace(), fail={"system."})
    result = _scan(_policy({"information_schema": "per_catalog"}), spark)
    assert result.status is RunStatus.SUCCEEDED
    assert _findings(result) == BASELINE
    assert "SHOW CATALOGS" in spark.client.statements
    assert not any("`system`" in s or "FROM system." in s for s in spark.client.statements)


def test_per_catalog_without_show_catalogs_fails_clearly() -> None:
    spark = FakeSpark(sample_workspace(), fail={"system.", "SHOW CATALOGS"})
    result = _scan(_policy({"information_schema": "per_catalog"}), spark)
    error = result.scanner_result("uc").error
    assert error is not None and error.phase is Phase.DISCOVER
    assert "cannot read source 'catalogs' (SHOW CATALOGS)" in error.message


def test_per_catalog_unreadable_catalog_details_are_limitations() -> None:
    spark = FakeSpark(sample_workspace(), fail={"system.", "main.information_schema.catalogs"})
    result = _scan(_policy({"information_schema": "per_catalog"}), spark)
    assert result.status is RunStatus.SUCCEEDED
    owner = next(
        f
        for f in result.findings
        if f.resource.resource_id == "main" and f.rule_id == "uc.owner_missing"
    )
    assert owner.recommendation is Recommendation.REVIEW and owner.confidence == 0
    assert any("Catalog details for 'main'" in item for item in result.limitations)


def test_org_schema_with_views_of_same_shape() -> None:
    data = sample_workspace()
    data["views"] = {
        f"governance.uc_meta.{kind}": {"kind": kind}
        for kind in ("catalogs", "schemata", "tables", "table_tags", "volumes")
    }
    spark = FakeSpark(data, fail={"system."})
    result = _scan(_policy({"information_schema": "governance.uc_meta"}), spark)
    assert result.status is RunStatus.SUCCEEDED
    assert _findings(result) == BASELINE


def test_single_view_with_renamed_columns() -> None:
    data = sample_workspace()
    data["views"] = {
        "finops.meta.all_tables": {
            "kind": "tables",
            "rename": {"table_owner": "owner_principal", "last_altered": "changed_at"},
        }
    }
    spark = FakeSpark(data)
    sources = {
        "tables": {
            "table": "finops.meta.all_tables",
            "columns": {"table_owner": "owner_principal", "last_altered": "changed_at"},
        }
    }
    result = _scan(_policy(sources), spark)
    assert result.status is RunStatus.SUCCEEDED
    assert _findings(result) == BASELINE
    assert any("`owner_principal` AS table_owner" in s for s in spark.client.statements)


def test_view_missing_optional_column_makes_only_that_check_unknown() -> None:
    data = sample_workspace()
    data["views"] = {"org.meta.tables": {"kind": "tables", "drop": ["comment"]}}
    result = _scan(_policy({"tables": "org.meta.tables"}), FakeSpark(data))
    assert result.status is RunStatus.SUCCEEDED
    table_comments = [
        f
        for f in result.findings
        if f.rule_id == "uc.comment_missing" and f.resource.resource_type == "table"
    ]
    assert table_comments and all(f.confidence == 0 for f in table_comments)
    assert all(f.recommendation is Recommendation.REVIEW for f in table_comments)
    evaluations = [
        s.evaluation.outcome
        for s in result.scanner_result("uc").evaluations
        if s.evaluation.rule_id == "uc.comment_missing"
        and s.evaluation.resource.resource_type == "table"
    ]
    assert set(evaluations) == {Outcome.INDETERMINATE}
    assert any("org.meta.tables has no 'comment' column" in item for item in result.limitations)
    # other checks on the same tables are unaffected
    owners = {
        f.resource.resource_id: f.recommendation
        for f in result.findings
        if f.rule_id == "uc.owner_missing" and f.resource.resource_type == "table"
    }
    assert owners["main.sales.orders"] is Recommendation.KEEP


def test_view_missing_required_column_fails_scanner_with_reason() -> None:
    data = sample_workspace()
    data["views"] = {"org.meta.tables": {"kind": "tables", "drop": ["table_name"]}}
    result = _scan(_policy({"tables": "org.meta.tables"}), FakeSpark(data))
    error = result.scanner_result("uc").error
    assert error is not None
    assert "missing required column(s): table_name" in error.message


def test_unreadable_optional_view_is_a_limitation() -> None:
    result = _scan(_policy({"table_tags": "org.meta.no_such_view"}), FakeSpark(sample_workspace()))
    assert result.status is RunStatus.SUCCEEDED
    assert any(
        "Table tags in catalog 'main' could not be read (org.meta.no_such_view" in item
        for item in result.limitations
    )


# --- check_access -----------------------------------------------------------------------------


def _report(policy: Policy, spark: Any) -> Any:
    return check_access(spark, policy=policy, scanners=["uc"])


def test_check_access_all_available() -> None:
    report = _report(load_policy(EXAMPLE_POLICY), FakeSpark(sample_workspace()))
    assert report.all_available and report.can_run("uc")
    assert {c.source for c in report.checks} == {
        "catalogs",
        "schemata",
        "tables",
        "table_tags",
        "volumes",
    }
    assert "can run with full coverage" in report.summary()
    assert "DESCRIBE DETAIL" in report.summary()


def test_check_access_blocked_system_explains_fix() -> None:
    report = _report(load_policy(EXAMPLE_POLICY), FakeSpark(sample_workspace(), fail={"system."}))
    assert not report.can_run("uc")
    tables = next(c for c in report.checks if c.source == "tables")
    assert tables.status == "unavailable" and tables.impact == "uc cannot run"
    assert "sources.information_schema: per_catalog" in tables.how_to_fix
    tags = next(c for c in report.checks if c.source == "table_tags")
    assert tags.impact == "unknown: uc.required_tags_missing"
    assert "cannot run" in report.summary()


def test_check_access_per_catalog_samples_a_scoped_catalog() -> None:
    spark = FakeSpark(sample_workspace(), fail={"system."})
    report = _report(_policy({"information_schema": "per_catalog"}), spark)
    assert report.can_run("uc")
    listing = next(c for c in report.checks if c.source == "catalog list")
    assert listing.status == "available" and "'main'" in listing.impact
    tables = next(c for c in report.checks if c.source == "tables")
    assert tables.object == "main.information_schema.tables (sampled)"


def test_check_access_partial_view_names_affected_rules() -> None:
    data = sample_workspace()
    data["views"] = {"org.meta.tables": {"kind": "tables", "drop": ["table_owner", "created"]}}
    report = _report(_policy({"tables": "org.meta.tables"}), FakeSpark(data))
    tables = next(c for c in report.checks if c.source == "tables")
    assert tables.status == "partial"
    assert tables.configured_by == "policy"
    assert tables.missing_columns == ("table_owner", "created")
    assert tables.impact == "unknown: uc.owner_missing"
    assert report.can_run("uc")


def test_check_access_reads_metadata_only() -> None:
    spark = FakeSpark(sample_workspace())
    _report(load_policy(EXAMPLE_POLICY), spark)
    assert spark.client.statements
    assert all(s.endswith("LIMIT 0") or s == "SHOW CATALOGS" for s in spark.client.statements)


def test_check_access_tables_and_scanners_without_sources() -> None:
    spark = FakeSpark(sample_workspace())
    report = check_access(spark, EXAMPLE_POLICY, scanners=["uc", "example"])
    assert "Access check for 'example':\n  reads no data sources" in report.summary()
    frame = report.to_dataframe(spark)
    assert frame.schema.startswith("scanner STRING, source STRING")
    assert len(frame.rows) == 5
    assert check_access(FakeUC(sample_workspace()), EXAMPLE_POLICY, scanners=["uc"]).all_available


# --- scanner source declarations ----------------------------------------------------------------


def test_conflicting_source_declarations_are_rejected() -> None:
    class Other(UnityCatalogScanner):
        name = "other"
        rules = ()
        sources = (
            SourceSpec(
                "tables",
                "different",
                (SourceColumn("x", required=True),),
                default="a.b.c",
            ),
        )
        source_groups = ()

    registry = ScannerRegistry()
    registry.register(UnityCatalogScanner)
    with pytest.raises(InvalidScannerError, match="conflicts with scanner 'uc'"):
        registry.register(Other)
