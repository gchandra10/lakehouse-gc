"""Lakeflow Jobs scanner: jobs with no observed run, and jobs failing every day.

Read-only. It runs SELECT queries on ``system.lakeflow.jobs`` and
``system.lakeflow.job_run_timeline`` through the run's SQL client. Both can be repointed
under ``sources:`` (``lakeflow: <catalog.schema>`` for a schema of views, or a single view
with renamed columns). It never modifies, pauses, or deletes a job.

Evidence limits, reported explicitly instead of hidden:
- System tables keep about a year of history, so "no run" means "no run observed in the
  retained history", never "never ran".
- ``create_time`` is only filled for rows emitted from late November 2025. Without it, a
  job's age comes from its earliest recorded change, which is inferred and can be younger
  than the real job.
- ``jobs`` keeps one row per change; the latest row describes the job, and jobs with a
  ``delete_time`` are skipped.
- The system tables can include other workspaces of the account; ``workspace_ids`` limits
  the scan.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

from lakehouse_gc.core.evidence import Evidence, EvidenceStatus
from lakehouse_gc.core.models import (
    Dimension,
    Finding,
    Outcome,
    Recommendation,
    Resource,
    ResourceRef,
    RuleEvaluation,
    ScoredEvaluation,
    Severity,
)
from lakehouse_gc.core.scanner import RuleSpec, Scanner, ScannerContext
from lakehouse_gc.core.sources import (
    ResolvedSource,
    SourceColumn,
    SourceGroupSpec,
    SourceReader,
    SourceSpec,
    SourceUnavailableError,
    describe_error,
)
from lakehouse_gc.core.sql import Row, utc_timestamp

_NEVER_RUN, _FAILING = "jobs.never_run", "jobs.failing_streak"

LAKEFLOW = SourceGroupSpec(
    "lakeflow",
    default="system.lakeflow",
    description="Lakeflow Jobs system tables, or your organization's schema of views over them.",
)


def _col(name: str, *used_by: str, required: bool = False) -> SourceColumn:
    return SourceColumn(name, required=required, used_by=used_by)


JOBS_SOURCE = SourceSpec(
    "jobs",
    "Jobs, one row per change (the latest row describes the job)",
    (
        _col("workspace_id", required=True),
        _col("job_id", required=True),
        _col("change_time", required=True),
        _col("name"),
        _col("creator_user_name"),
        _col("creator_id"),
        _col("create_time"),
        _col("delete_time"),
        _col("paused"),
    ),
    group="lakeflow",
    view="jobs",
    essential=True,
)
RUNS_SOURCE = SourceSpec(
    "job_run_timeline",
    "Job runs: start and end times and the result of each run",
    (
        _col("workspace_id", required=True),
        _col("job_id", required=True),
        _col("period_start_time", required=True),
        _col("period_end_time", _FAILING),
        _col("result_state", _FAILING),
    ),
    group="lakeflow",
    view="job_run_timeline",
    used_by=(_NEVER_RUN, _FAILING),
)
JOBS_SOURCES = (JOBS_SOURCE, RUNS_SOURCE)

DEFAULT_FAILURE_STATES = ("FAILED", "ERROR", "TIMED_OUT")


@dataclass(frozen=True)
class JobsConfig:
    never_run_days: int = 30
    failing_days: int = 3
    failure_states: tuple[str, ...] = DEFAULT_FAILURE_STATES
    workspace_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.never_run_days < 1:
            raise ValueError("never_run_days must be at least 1")
        if not 1 <= self.failing_days <= 90:
            raise ValueError("failing_days must be from 1 to 90")
        if not self.failure_states:
            raise ValueError("failure_states must not be empty")


NEVER_RUN_RULE = RuleSpec(
    _NEVER_RUN,
    "Job has no observed run",
    Severity.MEDIUM,
    frozenset({Dimension.GOVERNANCE}),
    description=(
        "Jobs at least never_run_days old with no run in job_run_timeline may be leftovers. "
        "The timeline keeps about a year, so this means no run was observed, not that the "
        "job never ran. Without create_time, age comes from the earliest recorded change."
    ),
    settings=("never_run_days", "workspace_ids"),
    on_fail=Recommendation.REVIEW,
)
FAILING_RULE = RuleSpec(
    _FAILING,
    "Job failed every run, every day",
    Severity.HIGH,
    frozenset({Dimension.COST}),
    description=(
        "Fails when each of the last failing_days days (UTC) had at least one finished run "
        "and every finished run that day ended in one of failure_states. A day with no "
        "finished run, or any other result, ends the streak. Today counts once it has a "
        "finished run."
    ),
    settings=("failing_days", "failure_states", "workspace_ids"),
    on_fail=Recommendation.REVIEW,
)

_O, _I, _U = EvidenceStatus.OBSERVED, EvidenceStatus.INFERRED, EvidenceStatus.UNAVAILABLE


class LakeflowJobsScanner(Scanner):
    name = "jobs"
    rules = (NEVER_RUN_RULE, FAILING_RULE)
    config_model = JobsConfig
    resource_types = ("job",)
    sources = JOBS_SOURCES
    source_groups = (LAKEFLOW,)

    def discover(self, ctx: ScannerContext) -> Sequence[Resource]:
        config: JobsConfig = ctx.config
        reader = SourceReader(ctx.get_sql())
        source = _plan(ctx)["jobs"]
        object_name = source.object_for()
        select, missing = reader.select(source, object_name)
        _note_missing(ctx, source, object_name, missing)
        where, params = _workspace_filter(config)
        statement = (
            "SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY workspace_id, job_id "
            "ORDER BY change_time DESC) AS change_rank, MIN(change_time) OVER (PARTITION BY "
            f"workspace_id, job_id) AS first_change_time FROM ({select}) AS j{where}) AS latest "
            "WHERE change_rank = 1 AND delete_time IS NULL"
        )
        try:
            rows = reader.sql.query(statement, params or None)
        except Exception as exc:
            raise SourceUnavailableError("jobs", object_name, describe_error(exc)) from exc
        if "delete_time" in missing:
            ctx.limit(
                f"{object_name} has no 'delete_time' column: deleted jobs cannot be told apart "
                "and may appear in the results."
            )
        resources = [_resource(row, object_name, missing) for row in rows]
        return sorted(resources, key=lambda r: r.ref.key)

    def analyze(
        self, ctx: ScannerContext, resources: Sequence[Resource]
    ) -> Sequence[RuleEvaluation]:
        if not resources:
            return []
        config: JobsConfig = ctx.config
        today = ctx.now().date()
        source = _plan(ctx)["job_run_timeline"]
        runs = _read_runs(ctx, SourceReader(ctx.get_sql()), source, config, today)
        evaluations: list[RuleEvaluation] = []
        for resource in resources:
            evaluations.append(_evaluate_never_run(resource, runs, config, ctx.now()))
            evaluations.append(_evaluate_failing(resource, runs, config, today))
        return evaluations

    def recommend(
        self, ctx: ScannerContext, scored: Sequence[ScoredEvaluation]
    ) -> Sequence[Finding]:
        findings = []
        for item in scored:
            evaluation = item.evaluation
            if evaluation.outcome is Outcome.PASS:
                recommendation = Recommendation.KEEP
            elif evaluation.outcome is Outcome.FAIL:
                recommendation = self.rule_specs()[evaluation.rule_id].on_fail or (
                    Recommendation.REVIEW
                )
            else:
                recommendation = Recommendation.REVIEW
            findings.append(
                Finding(
                    resource=evaluation.resource,
                    rule_id=evaluation.rule_id,
                    severity=item.severity,
                    recommendation=recommendation,
                    reasons=evaluation.reasons,
                    evidence=evaluation.evidence,
                    confidence=item.confidence,
                    impacts=frozenset(d for d, w in item.weights.items() if w > 0),
                )
            )
        return findings


# --- reading -----------------------------------------------------------------------------


@dataclass(frozen=True)
class _Runs:
    """What the run timeline showed, or why it couldn't be read."""

    object_name: str
    unavailable: str | None = None
    """Reason the timeline (or the columns a rule needs) could not be read."""
    history_start: datetime | None = None
    last_run: Mapping[tuple[str, str], datetime] | None = None
    days: Mapping[tuple[str, str], Mapping[date, tuple[int, int]]] | None = None
    """(workspace, job) -> day -> (finished runs, failed runs)."""
    days_unavailable: str | None = None


def _plan(ctx: ScannerContext) -> Mapping[str, ResolvedSource]:
    """This run's resolved sources, or the defaults when the scanner runs standalone."""
    if ctx.sources:
        return ctx.sources
    from lakehouse_gc.core.sources import resolve_sources

    return resolve_sources({}, [("jobs", JOBS_SOURCES, (LAKEFLOW,))])["jobs"]


def _workspace_filter(config: JobsConfig) -> tuple[str, dict[str, Any]]:
    if not config.workspace_ids:
        return "", {}
    params = {f"ws{i}": value for i, value in enumerate(config.workspace_ids)}
    return f" WHERE workspace_id IN ({', '.join(':' + name for name in params)})", params


def _note_missing(
    ctx: ScannerContext, source: ResolvedSource, object_name: str, missing: Sequence[str]
) -> None:
    for column in missing:
        uses = source.spec.column(column).used_by
        if uses:
            ctx.limit(
                f"{object_name} has no '{source.actual(column)}' column: {', '.join(uses)} unknown."
            )


def _read_runs(
    ctx: ScannerContext,
    reader: SourceReader,
    source: ResolvedSource,
    config: JobsConfig,
    today: date,
) -> _Runs:
    object_name = source.object_for()
    try:
        select, missing = reader.select(source, object_name)
        where, params = _workspace_filter(config)
        runs = f"(SELECT * FROM ({select}) AS r{where}) AS runs"
        history = reader.sql.query(
            f"SELECT MIN(period_start_time) AS history_start FROM {runs}", params or None
        )
        latest = reader.sql.query(
            "SELECT workspace_id, job_id, MAX(period_start_time) AS last_run_time "
            f"FROM {runs} GROUP BY workspace_id, job_id",
            params or None,
        )
    except SourceUnavailableError as exc:
        ctx.limit(
            f"Job runs could not be read ({exc.object_name}: {exc.reason}): "
            f"{_NEVER_RUN} and {_FAILING} unknown."
        )
        return _Runs(object_name, unavailable=exc.reason)
    except Exception as exc:
        reason = describe_error(exc)
        ctx.limit(
            f"Job runs could not be read ({object_name}: {reason}): "
            f"{_NEVER_RUN} and {_FAILING} unknown."
        )
        return _Runs(object_name, unavailable=reason)
    _note_missing(ctx, source, object_name, missing)
    last_run = {
        (str(row["workspace_id"]), str(row["job_id"])): stamp
        for row in latest
        if (stamp := utc_timestamp(row["last_run_time"])) is not None
    }
    history_start = utc_timestamp(history[0]["history_start"]) if history else None

    if missing:
        return _Runs(object_name, None, history_start, last_run, None, "columns missing")
    states = {f"state{i}": s.upper() for i, s in enumerate(config.failure_states)}
    since = today - timedelta(days=config.failing_days)
    try:
        daily = reader.sql.query(
            "SELECT workspace_id, job_id, to_date(period_end_time) AS run_day, "
            "COUNT(*) AS finished, SUM(CASE WHEN upper(result_state) IN "
            f"({', '.join(':' + name for name in states)}) THEN 1 ELSE 0 END) AS failed "
            f"FROM {runs} WHERE result_state IS NOT NULL AND to_date(period_end_time) >= :since "
            "GROUP BY workspace_id, job_id, to_date(period_end_time)",
            {**params, **states, "since": since},
        )
    except Exception as exc:
        reason = describe_error(exc)
        ctx.limit(
            f"Daily run results could not be read ({object_name}: {reason}): {_FAILING} unknown."
        )
        return _Runs(object_name, None, history_start, last_run, None, reason)
    days: dict[tuple[str, str], dict[date, tuple[int, int]]] = {}
    for row in daily:
        day = row["run_day"]
        day = day.date() if isinstance(day, datetime) else day
        if isinstance(day, str):
            day = date.fromisoformat(day)
        key = (str(row["workspace_id"]), str(row["job_id"]))
        days.setdefault(key, {})[day] = (int(row["finished"]), int(row["failed"] or 0))
    return _Runs(object_name, None, history_start, last_run, days)


def _text(row: Row, key: str) -> str | None:
    value = row.get(key)
    return str(value) if value is not None and str(value).strip() else None


def _resource(row: Row, source: str, missing: Sequence[str]) -> Resource:
    workspace, job_id = str(row["workspace_id"]), str(row["job_id"])
    name = _text(row, "name") or f"job {job_id}"
    owner = _text(row, "creator_user_name") or _text(row, "creator_id")
    created = utc_timestamp(row.get("create_time"))
    if created is not None:
        created_evidence = Evidence(
            "created", _O, source, value=created.isoformat(), note="Job creation time."
        )
    else:
        first = utc_timestamp(row.get("first_change_time"))
        why = "no create_time column" if "create_time" in missing else "create_time not filled"
        created_evidence = (
            Evidence(
                "created",
                _I,
                source,
                value=first.isoformat(),
                note=f"Earliest recorded change ({why}); the job may be older.",
            )
            if first is not None
            else Evidence("created", _U, source, note=f"Creation time unknown ({why}).")
        )
    paused = row.get("paused")
    return Resource(
        ref=ResourceRef(workspace, "job", f"{name} ({workspace}/{job_id})"),
        name=name,
        owner=owner,
        metadata={
            "workspace_id": workspace,
            "job_id": job_id,
            "paused": paused if isinstance(paused, bool) else None,
        },
        evidence=(
            Evidence("owner", _O if owner else _U, source, value=owner),
            created_evidence,
        ),
    )


# --- rules -------------------------------------------------------------------------------


def _key(resource: Resource) -> tuple[str, str]:
    return str(resource.metadata["workspace_id"]), str(resource.metadata["job_id"])


def _evaluate_never_run(
    resource: Resource, runs: _Runs, config: JobsConfig, now: datetime
) -> RuleEvaluation:
    rule = _NEVER_RUN
    created_evidence = resource.get_evidence("created")
    assert created_evidence is not None
    if runs.unavailable is not None or runs.last_run is None:
        evidence = Evidence("last_run", _U, runs.object_name, note=runs.unavailable)
        return RuleEvaluation(
            resource.ref,
            rule,
            Outcome.INDETERMINATE,
            (evidence,),
            ("Job runs could not be read.",),
        )
    last = runs.last_run.get(_key(resource))
    if last is not None:
        evidence = Evidence("last_run", _O, runs.object_name, value=last.isoformat())
        return RuleEvaluation(
            resource.ref,
            rule,
            Outcome.PASS,
            (evidence,),
            (f"Last run started {last.date().isoformat()}.",),
        )
    created = utc_timestamp(created_evidence.value)
    if created is None:
        return RuleEvaluation(
            resource.ref,
            rule,
            Outcome.INDETERMINATE,
            (created_evidence,),
            ("No run observed, but the job's age is unknown.",),
        )
    age = (now - created).days
    if age < config.never_run_days:
        return RuleEvaluation(
            resource.ref,
            rule,
            Outcome.PASS,
            (created_evidence,),
            (
                f"No run yet, but the job is only {age} day(s) old "
                f"(threshold {config.never_run_days}).",
            ),
        )
    since = max(created, runs.history_start) if runs.history_start else created
    no_run = Evidence(
        "last_run",
        _I,
        runs.object_name,
        note=f"No run observed since {since.date().isoformat()}; the timeline keeps about a "
        "year, so an older run would not show.",
    )
    paused = " It is paused." if resource.metadata.get("paused") is True else ""
    approx = "about " if created_evidence.status is _I else ""
    return RuleEvaluation(
        resource.ref,
        rule,
        Outcome.FAIL,
        (created_evidence, no_run),
        (
            f"Job is {approx}{age} days old and no run was observed since "
            f"{since.date().isoformat()}.{paused} Confirm it is still needed.",
        ),
    )


def _evaluate_failing(
    resource: Resource, runs: _Runs, config: JobsConfig, today: date
) -> RuleEvaluation:
    rule = _FAILING
    if runs.unavailable is not None or runs.days is None:
        reason = runs.unavailable or runs.days_unavailable
        evidence = Evidence("daily_results", _U, runs.object_name, note=reason)
        return RuleEvaluation(
            resource.ref,
            rule,
            Outcome.INDETERMINATE,
            (evidence,),
            ("Daily run results could not be read.",),
        )
    days = runs.days.get(_key(resource), {})
    day = today if today in days else today - timedelta(days=1)
    streak = 0
    while day in days and days[day][0] > 0 and days[day][1] == days[day][0]:
        streak += 1
        day -= timedelta(days=1)
    finished = sum(f for f, _ in days.values())
    failed = sum(x for _, x in days.values())
    evidence = Evidence(
        "daily_results",
        _O,
        runs.object_name,
        value={"failing_days_in_a_row": streak, "finished_runs": finished, "failed_runs": failed},
        note=f"Finished runs since {(today - timedelta(days=config.failing_days)).isoformat()}"
        " (UTC days).",
    )
    if streak >= config.failing_days:
        return RuleEvaluation(
            resource.ref,
            rule,
            Outcome.FAIL,
            (evidence,),
            (
                f"Every finished run failed on each of the last {streak} days "
                f"({failed} failed run(s)). Each failed run still uses compute; investigate "
                "the failures.",
            ),
        )
    if not days:
        why = f"No finished runs in the last {config.failing_days} days."
    else:
        why = (
            f"Not failing every day: {streak} day(s) in a row with only failures "
            f"(threshold {config.failing_days})."
        )
    return RuleEvaluation(resource.ref, rule, Outcome.PASS, (evidence,), (why,))
