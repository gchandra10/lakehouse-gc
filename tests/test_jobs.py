"""Lakeflow jobs scanner against an in-memory stand-in for the two system tables."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime, timedelta
from typing import Any

import pytest

from conftest import FIXED_NOW, fixed_clock
from lakehouse_gc import check_access, run
from lakehouse_gc.core import EvidenceStatus, Outcome, ScannerStatus, loads_policy
from lakehouse_gc.core.results import RunResult

JOBS = "system.lakeflow.jobs"
RUNS = "system.lakeflow.job_run_timeline"
JOB_COLUMNS = [
    "workspace_id",
    "job_id",
    "change_time",
    "name",
    "creator_user_name",
    "creator_id",
    "create_time",
    "delete_time",
    "paused",
]
RUN_COLUMNS = ["workspace_id", "job_id", "period_start_time", "period_end_time", "result_state"]
TODAY = FIXED_NOW.date()


def days_ago(days: float) -> datetime:
    return FIXED_NOW - timedelta(days=days)


def job(job_id: str, created_days: float | None = 60, **extra: Any) -> dict[str, Any]:
    created = None if created_days is None else days_ago(created_days)
    return {
        "workspace_id": "ws1",
        "job_id": job_id,
        "change_time": created or days_ago(90),
        "name": f"job_{job_id}",
        "creator_user_name": "ana@example.com",
        "creator_id": "1",
        "create_time": created,
        "delete_time": None,
        "paused": False,
        **extra,
    }


def run_on(job_id: str, day: date, state: str | None, workspace: str = "ws1") -> dict[str, Any]:
    end = datetime.combine(day, datetime.min.time(), FIXED_NOW.tzinfo) + timedelta(hours=1)
    return {
        "workspace_id": workspace,
        "job_id": job_id,
        "period_start_time": end - timedelta(minutes=30),
        "period_end_time": end,
        "result_state": state,
    }


class FakeLakeflow:
    """Answers the scanner's queries the way Spark would, from plain rows."""

    def __init__(
        self,
        jobs: list[dict[str, Any]],
        runs: list[dict[str, Any]],
        *,
        objects: Mapping[str, tuple[str, list[str]]] | None = None,
    ) -> None:
        self.jobs, self.runs = jobs, runs
        self.objects = dict(
            objects
            if objects is not None
            else {JOBS: ("jobs", JOB_COLUMNS), RUNS: ("runs", RUN_COLUMNS)}
        )
        self.statements: list[str] = []
        self.params: list[Mapping[str, Any]] = []

    def columns(self, object_name: str) -> list[str]:
        if object_name not in self.objects:
            raise PermissionError(f"no SELECT on {object_name}")
        return self.objects[object_name][1]

    def _rows(self, kind: str, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        rows = self.jobs if kind == "jobs" else self.runs
        workspaces = {v for k, v in params.items() if k.startswith("ws")}
        return [r for r in rows if not workspaces or r["workspace_id"] in workspaces]

    def query(self, statement: str, params: Mapping[str, Any] | None = None) -> list[Any]:
        params = params or {}
        self.statements.append(statement)
        self.params.append(params)
        if "ROW_NUMBER()" in statement:
            latest: dict[tuple[str, str], dict[str, Any]] = {}
            first: dict[tuple[str, str], datetime] = {}
            for row in self._rows("jobs", params):
                key = (row["workspace_id"], row["job_id"])
                first[key] = min(first.get(key, row["change_time"]), row["change_time"])
                if key not in latest or row["change_time"] > latest[key]["change_time"]:
                    latest[key] = row
            return [
                {**row, "first_change_time": first[key]}
                for key, row in latest.items()
                if row["delete_time"] is None
            ]
        runs = self._rows("runs", params)
        if "history_start" in statement:
            return [{"history_start": min((r["period_start_time"] for r in runs), default=None)}]
        if "last_run_time" in statement:
            last: dict[tuple[str, str], datetime] = {}
            for r in runs:
                key = (r["workspace_id"], r["job_id"])
                last[key] = max(last.get(key, r["period_start_time"]), r["period_start_time"])
            return [
                {"workspace_id": w, "job_id": j, "last_run_time": t} for (w, j), t in last.items()
            ]
        if "run_day" in statement:
            states = {v for k, v in params.items() if k.startswith("state")}
            daily: dict[tuple[str, str, date], list[int]] = {}
            for r in runs:
                day = r["period_end_time"].date()
                if r["result_state"] is None or day < params["since"]:
                    continue
                counts = daily.setdefault((r["workspace_id"], r["job_id"], day), [0, 0])
                counts[0] += 1
                counts[1] += r["result_state"].upper() in states
            return [
                {"workspace_id": w, "job_id": j, "run_day": d, "finished": f, "failed": x}
                for (w, j, d), (f, x) in daily.items()
            ]
        raise AssertionError(f"unexpected statement: {statement}")

    def createDataFrame(self, rows: list[tuple[Any, ...]], schema: str) -> Any:
        return {"rows": rows, "schema": schema}


def sample_lakeflow() -> FakeLakeflow:
    """A job with no run, a job failing every day, and a healthy job, dated relative to the
    real clock (notebooks don't use the fixed test clock)."""
    failing = [run_on("2", TODAY - timedelta(days=d), "FAILED") for d in (1, 2, 3)]
    healthy = [run_on("3", TODAY - timedelta(days=1), "SUCCEEDED")]
    shift = timedelta(days=(datetime.now(FIXED_NOW.tzinfo).date() - TODAY).days)

    def moved(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {k: v + shift if isinstance(v, datetime) else v for k, v in r.items()} for r in rows
        ]

    return FakeLakeflow(moved([job("1"), job("2"), job("3")]), moved([*failing, *healthy]))


POLICY = "policy_version: 1\n"


def scan(
    client: FakeLakeflow, policy: str = POLICY
) -> tuple[RunResult, dict[tuple[str, str], Any]]:
    result = run(policy=loads_policy(policy), scanners=["jobs"], spark=client, clock=fixed_clock)
    findings = {(f.resource.resource_id.split(" (")[0], f.rule_id): f for f in result.findings}
    return result, findings


def test_job_with_no_observed_run_is_flagged() -> None:
    _, findings = scan(FakeLakeflow([job("1", created_days=60)], []))
    finding = findings[("job_1", "jobs.never_run")]
    assert finding.recommendation.value == "review"
    assert "60 days old and no run was observed" in finding.reasons[0]
    assert finding.resource.resource_id == "job_1 (ws1/1)"
    statuses = {e.key: e.status for e in finding.evidence}
    assert statuses == {"created": EvidenceStatus.OBSERVED, "last_run": EvidenceStatus.INFERRED}


def test_job_with_a_run_or_too_young_passes() -> None:
    client = FakeLakeflow(
        [job("1"), job("2", created_days=5)],
        [run_on("1", TODAY - timedelta(days=200), "SUCCEEDED")],
    )
    _, findings = scan(client)
    assert findings[("job_1", "jobs.never_run")].recommendation.value == "keep"
    young = findings[("job_2", "jobs.never_run")]
    assert young.recommendation.value == "keep" and "only 5 day(s) old" in young.reasons[0]


def test_age_falls_back_to_the_earliest_change_as_inferred() -> None:
    rows = [
        job("1", created_days=None, change_time=days_ago(100)),
        job("1", created_days=None, change_time=days_ago(10), name="renamed"),
    ]
    _, findings = scan(FakeLakeflow(rows, []))
    finding = findings[("renamed", "jobs.never_run")]
    assert "about 100 days old" in finding.reasons[0]
    created = next(e for e in finding.evidence if e.key == "created")
    assert created.status is EvidenceStatus.INFERRED and "may be older" in (created.note or "")


def test_deleted_jobs_are_skipped() -> None:
    rows = [job("1"), job("1", change_time=days_ago(1), delete_time=days_ago(1))]
    result, _ = scan(FakeLakeflow(rows, []))
    assert result.findings == ()


def _streak(states_by_days_ago: Mapping[int, list[str]], **config: Any) -> Any:
    runs = [
        run_on("1", TODAY - timedelta(days=ago), state)
        for ago, states in states_by_days_ago.items()
        for state in states
    ]
    policy = POLICY + "rules:\n  jobs:\n" + "".join(f"    {k}: {v}\n" for k, v in config.items())
    _, findings = scan(FakeLakeflow([job("1")], runs), policy)
    return findings[("job_1", "jobs.failing_streak")]


def test_failing_every_day_is_flagged() -> None:
    finding = _streak({1: ["FAILED"], 2: ["FAILED", "TIMED_OUT"], 3: ["ERROR"]})
    assert finding.recommendation.value == "review"
    assert finding.severity.value == "high"
    assert "each of the last 3 days" in finding.reasons[0]


@pytest.mark.parametrize(
    "history",
    [
        {0: ["SUCCEEDED"], 1: ["FAILED"], 2: ["FAILED"], 3: ["FAILED"]},  # fixed today
        {1: ["FAILED", "SUCCEEDED"], 2: ["FAILED"], 3: ["FAILED"]},  # one success that day
        {1: ["FAILED"], 3: ["FAILED"], 4: ["FAILED"]},  # a day without runs
        {1: ["FAILED"], 2: ["CANCELLED"], 3: ["FAILED"]},  # cancelled is not a failure
        {},  # no runs at all
    ],
)
def test_streak_breaks(history: Mapping[int, list[str]]) -> None:
    assert _streak(history).recommendation.value == "keep"


def test_today_counts_once_it_has_a_finished_run() -> None:
    finding = _streak({0: ["FAILED"], 1: ["FAILED"]}, failing_days=2)
    assert finding.recommendation.value == "review"


def test_failure_states_are_configurable() -> None:
    history = {1: ["CANCELLED"], 2: ["CANCELLED"], 3: ["CANCELLED"]}
    assert _streak(history, failure_states='["CANCELLED"]').recommendation.value == "review"


def test_unreadable_timeline_makes_both_rules_unknown() -> None:
    client = FakeLakeflow([job("1")], [], objects={JOBS: ("jobs", JOB_COLUMNS)})
    result, findings = scan(client)
    assert result.scanner_result("jobs").status is ScannerStatus.SUCCEEDED
    for rule in ("jobs.never_run", "jobs.failing_streak"):
        assert findings[("job_1", rule)].confidence == 0
    assert any("Job runs could not be read" in item for item in result.limitations)
    outcomes = {e.evaluation.outcome for e in result.scanner_result("jobs").evaluations}
    assert outcomes == {Outcome.INDETERMINATE}


def test_missing_result_state_makes_only_the_streak_unknown() -> None:
    columns = [c for c in RUN_COLUMNS if c != "result_state"]
    client = FakeLakeflow(
        [job("1")], [], objects={JOBS: ("jobs", JOB_COLUMNS), RUNS: ("runs", columns)}
    )
    result, findings = scan(client)
    assert findings[("job_1", "jobs.never_run")].recommendation.value == "review"
    evaluations = {e.evaluation.rule_id: e for e in result.scanner_result("jobs").evaluations}
    assert evaluations["jobs.failing_streak"].evaluation.outcome is Outcome.INDETERMINATE
    assert any("'result_state'" in item for item in result.limitations)


def test_unreadable_jobs_table_fails_only_the_scanner() -> None:
    result, _ = scan(FakeLakeflow([], [], objects={}))
    jobs_result = result.scanner_result("jobs")
    assert jobs_result.status is ScannerStatus.FAILED
    assert jobs_result.error is not None and JOBS in jobs_result.error.message


def test_workspace_ids_filter_both_tables() -> None:
    client = FakeLakeflow([job("1"), job("2", workspace_id="ws2")], [])
    _, findings = scan(client, POLICY + "rules:\n  jobs:\n    workspace_ids: [ws2]\n")
    assert {name for name, _ in findings} == {"job_2"}
    assert all(params.get("ws0") == "ws2" for params in client.params)


def test_org_views_and_renamed_columns() -> None:
    views = {
        "gov.lakeflow.jobs": ("jobs", [*JOB_COLUMNS[:3], "job_name", *JOB_COLUMNS[4:]]),
        "gov.lakeflow.job_run_timeline": ("runs", RUN_COLUMNS),
    }
    client = FakeLakeflow([job("1")], [], objects=views)
    policy = POLICY + (
        "sources:\n  lakeflow: gov.lakeflow\n  jobs:\n    columns: {name: job_name}\n"
    )
    _, findings = scan(client, policy)
    assert ("job_1", "jobs.never_run") in findings
    assert "`job_name` AS name" in client.statements[0]
    assert "`gov`.`lakeflow`.`jobs`" in client.statements[0]


def test_queries_are_read_only() -> None:
    client = FakeLakeflow([job("1")], [run_on("1", TODAY, "FAILED")])
    scan(client)
    assert client.statements and all(s.startswith("SELECT") for s in client.statements)


def test_check_access_lists_jobs_sources() -> None:
    report = check_access(FakeLakeflow([], []), policy=loads_policy(POLICY), scanners=["jobs"])
    assert [(c.source, c.object, c.status) for c in report.checks] == [
        ("jobs", JOBS, "available"),
        ("job_run_timeline", RUNS, "available"),
    ]
    assert report.can_run("jobs")
