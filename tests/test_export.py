"""Sorted findings, JSON text, and saving results only where the user says."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from conftest import EXAMPLE_POLICY, FIXED_NOW, fixed_clock
from lakehouse_gc import run
from lakehouse_gc.core import LakehouseGCError, Severity
from lakehouse_gc.core.results import FINDINGS_COLUMNS, RunResult


class _Writer:
    def __init__(self, calls: list[tuple[str, Any]]) -> None:
        self.calls = calls

    def format(self, name: str) -> _Writer:
        self.calls.append(("format", name))
        return self

    def mode(self, name: str) -> _Writer:
        self.calls.append(("mode", name))
        return self

    def saveAsTable(self, name: str) -> None:
        self.calls.append(("saveAsTable", name))


class _Spark:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []

    def createDataFrame(self, rows: list[tuple[Any, ...]], schema: str) -> Any:
        self.calls.append(("createDataFrame", len(rows)))
        return SimpleNamespace(rows=rows, schema=schema, write=_Writer(self.calls))


@pytest.fixture(scope="module")
def result() -> RunResult:
    return run(EXAMPLE_POLICY, scanners=["example"], clock=fixed_clock)


def test_severity_levels() -> None:
    assert [s.level for s in Severity] == [0, 1, 2, 3]
    assert Severity.HIGH.level == 3 and Severity.INFO.level == 0


def test_rows_are_most_severe_first_and_carry_the_run(result: RunResult) -> None:
    rows = result.to_rows()
    assert len(rows) == len(result.findings)
    assert list(rows[0]) == list(FINDINGS_COLUMNS)
    levels = [row["severity_level"] for row in rows]
    assert levels == sorted(levels, reverse=True)
    assert {row["run_id"] for row in rows} == {result.run_id}
    assert {row["run_time"] for row in rows} == {FIXED_NOW}


def test_to_json_round_trips(result: RunResult) -> None:
    document = json.loads(result.to_json())
    assert document["run_id"] == result.run_id
    assert document["scores"]["overall"] == result.scores.overall
    assert len(document["findings"]) == len(result.findings)
    assert document["findings"][0]["run_time"] == FIXED_NOW.isoformat()


def test_save_needs_a_target(result: RunResult) -> None:
    with pytest.raises(ValueError, match="nothing to save"):
        result.save()


def test_save_json_never_overwrites(result: RunResult, tmp_path: Path) -> None:
    path = tmp_path / "run.json"
    assert result.save(json_path=str(path)) == [f"JSON: {path}"]
    assert json.loads(path.read_text(encoding="utf-8"))["run_id"] == result.run_id
    with pytest.raises(LakehouseGCError, match="never overwrites"):
        result.save(json_path=str(path))
    with pytest.raises(LakehouseGCError, match="does not exist"):
        result.save(json_path=str(tmp_path / "missing" / "run.json"))


def test_save_table_appends_delta(result: RunResult) -> None:
    spark = _Spark()
    written = result.save(spark, table="main.governance.findings")
    assert written == [
        f"Delta table (appended {len(result.findings)} rows): main.governance.findings"
    ]
    assert spark.calls[1:] == [
        ("format", "delta"),
        ("mode", "append"),
        ("saveAsTable", "`main`.`governance`.`findings`"),
    ]


@pytest.mark.parametrize("table", ["findings", "main.findings", "a..b", "a.b.c.d"])
def test_save_table_needs_a_full_name(result: RunResult, table: str) -> None:
    spark = _Spark()
    with pytest.raises(LakehouseGCError, match=r"catalog\.schema\.table"):
        result.save(spark, table=table)
    assert spark.calls == []


def test_save_table_needs_spark(result: RunResult) -> None:
    with pytest.raises(LakehouseGCError, match="needs spark"):
        result.save(table="main.governance.findings")
