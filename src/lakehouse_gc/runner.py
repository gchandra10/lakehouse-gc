"""Run orchestration: resolve scanners, run each lifecycle in isolation, aggregate results.

Configuration problems (invalid policy, unknown scanners, invalid scanner rules) raise before
any scanner runs. Once running, an exception or contract violation in one scanner marks only
that scanner ``FAILED``; other scanners still run and the run status becomes ``PARTIAL``.
"""

from __future__ import annotations

import logging
import os
import uuid
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, replace
from datetime import date, datetime, timezone
from typing import Any, TypeVar

from lakehouse_gc.core.config import resolve_scanner_configs
from lakehouse_gc.core.errors import LakehouseGCError, RegistryError, ScannerContractError
from lakehouse_gc.core.log import get_logger
from lakehouse_gc.core.models import (
    CANDIDATE_RECOMMENDATIONS,
    ExclusionStatus,
    Finding,
    Phase,
    Recommendation,
    Resource,
    RuleEvaluation,
    RunStatus,
    ScannerStatus,
    ScoredEvaluation,
    Severity,
)
from lakehouse_gc.core.policy import Exclusion, Policy, load_policy
from lakehouse_gc.core.registry import ScannerRegistry, get_default_registry
from lakehouse_gc.core.results import RunResult, ScannerError, ScannerResult
from lakehouse_gc.core.savings import summarize_savings
from lakehouse_gc.core.scanner import Scanner, ScannerContext
from lakehouse_gc.core.scoring import compute_scores
from lakehouse_gc.core.sql import SqlClient, as_sql_client

Clock = Callable[[], datetime]

EXCLUDED_RULE_ID = "framework.excluded"

_logger = get_logger(__name__)
_T = TypeVar("_T")


def run(
    config_path: str | os.PathLike[str] | None = None,
    *,
    policy: Policy | None = None,
    scanners: Iterable[str | type[Scanner]] | None = None,
    spark: Any = None,
    registry: ScannerRegistry | None = None,
    clock: Clock | None = None,
    run_id: str | None = None,
) -> RunResult:
    """Run the requested scanners (all registered scanners if ``None``) under a policy.

    ``scanners`` may mix registered names and scanner classes (e.g. from ``define_scanner``).
    ``spark`` is a SparkSession (or any ``SqlClient``) for scanners that read Unity Catalog;
    by default they use the active Spark session.
    Classes are added to a copy of the registry for this run only. Provide exactly one of
    ``config_path`` or ``policy``. ``registry``, ``clock`` (returning timezone-aware
    datetimes), and ``run_id`` are injectable for tests and embedding.
    """
    prepared = prepare(config_path, policy=policy, scanners=scanners, registry=registry)
    resolved_policy, scanner_classes, configs = (
        prepared.policy,
        prepared.scanner_classes,
        prepared.configs,
    )
    sql_client = as_sql_client(spark) if spark is not None else None
    clock_fn = clock if clock is not None else _system_clock
    resolved_run_id = run_id or uuid.uuid4().hex
    started_at = _now(clock_fn)
    names = tuple(cls.name for cls in scanner_classes)
    _logger.info(
        "Run started",
        extra={"event": "run.started", "run_id": resolved_run_id, "scanners": list(names)},
    )

    results = tuple(
        _run_scanner(
            scanner_cls,
            policy=resolved_policy,
            config=configs.get(scanner_cls.name),
            run_id=resolved_run_id,
            clock=clock_fn,
            today=started_at.date(),
            sql=sql_client,
        )
        for scanner_cls in scanner_classes
    )

    succeeded = [r for r in results if r.status is ScannerStatus.SUCCEEDED]
    failed = [r.name for r in results if r.status is ScannerStatus.FAILED]
    if not failed:
        status = RunStatus.SUCCEEDED
    elif succeeded:
        status = RunStatus.PARTIAL
    else:
        status = RunStatus.FAILED
    findings = tuple(f for r in results for f in r.findings)
    scores = compute_scores(
        ((r.name, scored) for r in succeeded for scored in r.evaluations),
        resolved_policy.scoring,
        failed_scanners=failed,
    )
    finished_at = _now(clock_fn)
    _logger.info(
        "Run finished",
        extra={
            "event": "run.finished",
            "run_id": resolved_run_id,
            "status": status.value,
            "failed_scanners": failed,
        },
    )
    return RunResult(
        run_id=resolved_run_id,
        started_at=started_at,
        finished_at=finished_at,
        status=status,
        policy_version=resolved_policy.policy_version,
        scanners=names,
        scanner_results=results,
        findings=findings,
        errors=tuple(r.error for r in results if r.error is not None),
        warnings=(
            *(w for r in results for w in r.warnings),
            *_unmatched_exclusion_warnings(resolved_policy, prepared.registry, results),
        ),
        scores=scores,
        savings=summarize_savings(findings),
    )


@dataclass(frozen=True)
class PreparedRun:
    """Validated inputs shared by ``run`` and ``describe_rules``."""

    policy: Policy
    registry: ScannerRegistry
    scanner_classes: tuple[type[Scanner], ...]
    configs: Mapping[str, Any]


def prepare(
    config_path: str | os.PathLike[str] | None = None,
    *,
    policy: Policy | None = None,
    scanners: Iterable[str | type[Scanner]] | None = None,
    registry: ScannerRegistry | None = None,
) -> PreparedRun:
    """Load and validate the policy, resolve scanners, and build their configs.

    Raises on any configuration problem, before anything is scanned.
    """
    if (config_path is None) == (policy is None):
        raise LakehouseGCError("provide exactly one of config_path or policy")
    resolved_policy = load_policy(config_path) if policy is None else policy  # type: ignore[arg-type]
    base_registry = registry if registry is not None else get_default_registry()
    resolved_registry, requested = _with_scanner_classes(base_registry, scanners)
    scanner_classes = resolved_registry.resolve(requested)
    configs = resolve_scanner_configs(
        resolved_policy.rules,
        {name: resolved_registry.get(name).config_model for name in resolved_registry.names()},
    )
    return PreparedRun(resolved_policy, resolved_registry, scanner_classes, configs)


def _unmatched_exclusion_warnings(
    policy: Policy, registry: ScannerRegistry, results: tuple[ScannerResult, ...]
) -> list[str]:
    """Flag exclusions whose resource_type no scanner declares or produced: they can never
    match, which is almost always a typo (e.g. ``internal_tables`` instead of ``table``)."""
    known = {t.lower() for name in registry.names() for t in registry.get(name).resource_types}
    known |= {r.ref.resource_type.lower() for result in results for r in result.resources}
    warnings = []
    for index, exclusion in enumerate(policy.exclusions):
        if exclusion.resource_type.lower() not in known:
            warnings.append(
                f"Exclusion {index + 1} (pattern '{exclusion.pattern}') uses resource_type "
                f"'{exclusion.resource_type}', which no scanner produces, so it never matches. "
                f"Known resource types: {', '.join(sorted(known)) or '(none)'}."
            )
    return warnings


def _with_scanner_classes(
    registry: ScannerRegistry, scanners: Iterable[str | type[Scanner]] | None
) -> tuple[ScannerRegistry, list[str] | None]:
    if scanners is None or isinstance(scanners, str):
        return registry, scanners  # type: ignore[return-value]  # resolve() rejects a str
    names: list[str] = []
    classes: list[type[Scanner]] = []
    for item in scanners:
        if isinstance(item, str):
            names.append(item)
        elif isinstance(item, type) and issubclass(item, Scanner):
            classes.append(item)
            names.append(getattr(item, "name", ""))
        else:
            raise RegistryError(f"scanners must be names or Scanner classes, got {item!r}")
    if not classes:
        return registry, names
    extended = registry.copy()
    for scanner_cls in classes:
        if scanner_cls.name not in extended or extended.get(scanner_cls.name) is not scanner_cls:
            extended.register(scanner_cls)
    return extended, names


def _system_clock() -> datetime:
    return datetime.now(timezone.utc)


def _now(clock: Clock) -> datetime:
    value = clock()
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise LakehouseGCError("clock must return a timezone-aware datetime")
    return value.astimezone(timezone.utc)


def _run_scanner(
    scanner_cls: type[Scanner],
    *,
    policy: Policy,
    config: Any,
    run_id: str,
    clock: Clock,
    today: date,
    sql: SqlClient | None,
) -> ScannerResult:
    name = scanner_cls.name
    log_extra = {"run_id": run_id, "scanner": name}
    started_at = _now(clock)
    warnings: list[str] = []
    phase = Phase.INITIALIZE
    _logger.info("Scanner started", extra={**log_extra, "event": "scanner.started"})
    try:
        scanner = scanner_cls()
        ctx = ScannerContext(
            run_id=run_id,
            scanner_name=name,
            policy=policy,
            config=config,
            logger=logging.getLogger(f"lakehouse_gc.scanners.{name}"),
            clock=lambda: _now(clock),
            sql=sql,
            warnings=warnings,
        )
        specs = scanner_cls.rule_specs()

        phase = Phase.DISCOVER
        resources = _as_tuple(scanner.discover(ctx), Resource, "discover")
        _check_unique_resources(resources)
        included, excluded = _apply_exclusions(resources, policy.exclusions, today, name)
        warnings.extend(excluded.warnings)

        phase = Phase.ANALYZE
        evaluations = _as_tuple(scanner.analyze(ctx, included), RuleEvaluation, "analyze")
        _check_evaluations(evaluations, included, set(specs))

        phase = Phase.SCORE
        scored = _as_tuple(scanner.score(ctx, evaluations), ScoredEvaluation, "score")
        _check_scored(scored, evaluations)

        phase = Phase.RECOMMEND
        findings = _as_tuple(scanner.recommend(ctx, scored), Finding, "recommend")
        _check_findings(findings, evaluations)
    except Exception as exc:
        _logger.error(
            "Scanner failed",
            exc_info=True,
            extra={
                **log_extra,
                "event": "scanner.failed",
                "phase": phase.value,
                "error_type": type(exc).__name__,
            },
        )
        return ScannerResult(
            name=name,
            status=ScannerStatus.FAILED,
            started_at=started_at,
            finished_at=_now(clock),
            warnings=tuple(warnings),
            error=ScannerError(
                scanner=name, phase=phase, error_type=type(exc).__name__, message=str(exc)
            ),
        )

    min_confidence = policy.actions.min_candidate_confidence
    gated = [
        replace(_apply_candidate_gate(finding, min_confidence), scanner=name)
        for finding in findings
    ]
    all_findings = tuple(sorted([*gated, *excluded.findings], key=lambda f: f.key))
    for warning in warnings:
        _logger.warning(warning, extra={**log_extra, "event": "scanner.warning"})
    _logger.info(
        "Scanner succeeded",
        extra={
            **log_extra,
            "event": "scanner.succeeded",
            "resources": len(resources),
            "excluded": len(excluded.findings),
            "findings": len(all_findings),
        },
    )
    return ScannerResult(
        name=name,
        status=ScannerStatus.SUCCEEDED,
        started_at=started_at,
        finished_at=_now(clock),
        resources=tuple(sorted(resources, key=lambda r: r.ref)),
        evaluations=tuple(sorted(scored, key=lambda s: s.evaluation.key)),
        findings=all_findings,
        warnings=tuple(warnings),
    )


def _as_tuple(value: object, item_type: type[_T], method: str) -> tuple[_T, ...]:
    if not isinstance(value, Iterable) or isinstance(value, (str, bytes, Mapping)):
        raise ScannerContractError(f"{method}() must return a sequence of {item_type.__name__}")
    items = tuple(value)
    for item in items:
        if not isinstance(item, item_type):
            raise ScannerContractError(
                f"{method}() returned {type(item).__name__}; expected {item_type.__name__}"
            )
    return items


def _check_unique_resources(resources: tuple[Resource, ...]) -> None:
    seen: set[str] = set()
    for resource in resources:
        if resource.ref.key in seen:
            raise ScannerContractError(f"discover() returned duplicate resource {resource.ref}")
        seen.add(resource.ref.key)


class _Excluded:
    def __init__(self) -> None:
        self.findings: list[Finding] = []
        self.warnings: list[str] = []


def _apply_exclusions(
    resources: tuple[Resource, ...],
    exclusions: tuple[Exclusion, ...],
    today: date,
    scanner_name: str,
) -> tuple[tuple[Resource, ...], _Excluded]:
    included: list[Resource] = []
    excluded = _Excluded()
    for resource in resources:
        active: Exclusion | None = None
        for exclusion in exclusions:
            if not exclusion.matches(resource):
                continue
            if exclusion.is_expired(today):
                assert exclusion.expires_on is not None
                excluded.warnings.append(
                    f"Exclusion '{exclusion.pattern}' ({exclusion.resource_type}) expired on "
                    f"{exclusion.expires_on.isoformat()}; not applied to {resource.ref}."
                )
                continue
            active = exclusion
            break
        if active is None:
            included.append(resource)
            continue
        excluded.findings.append(
            Finding(
                resource=resource.ref,
                rule_id=EXCLUDED_RULE_ID,
                severity=Severity.INFO,
                recommendation=Recommendation.EXCLUDED,
                reasons=(f"Excluded by policy: {active.reason}",),
                exclusion=ExclusionStatus(
                    resource_type=active.resource_type,
                    pattern=active.pattern,
                    reason=active.reason,
                    expires_on=active.expires_on,
                ),
                scanner=scanner_name,
            )
        )
    return tuple(included), excluded


def _check_evaluations(
    evaluations: tuple[RuleEvaluation, ...],
    included: tuple[Resource, ...],
    rule_ids: set[str],
) -> None:
    allowed = {resource.ref.key for resource in included}
    seen: set[tuple[str, str]] = set()
    for evaluation in evaluations:
        if evaluation.resource.key not in allowed:
            raise ScannerContractError(
                f"analyze() evaluated {evaluation.resource}, which was not discovered or is "
                "excluded"
            )
        if evaluation.rule_id not in rule_ids:
            raise ScannerContractError(f"analyze() used undeclared rule_id '{evaluation.rule_id}'")
        if evaluation.key in seen:
            raise ScannerContractError(
                f"analyze() evaluated rule '{evaluation.rule_id}' more than once for "
                f"{evaluation.resource}"
            )
        seen.add(evaluation.key)


def _check_scored(
    scored: tuple[ScoredEvaluation, ...], evaluations: tuple[RuleEvaluation, ...]
) -> None:
    expected = {evaluation.key: evaluation for evaluation in evaluations}
    seen: set[tuple[str, str]] = set()
    for item in scored:
        key = item.evaluation.key
        if expected.get(key) != item.evaluation or key in seen:
            raise ScannerContractError(
                f"score() returned an evaluation not produced by analyze(): {key}"
            )
        seen.add(key)
    if len(seen) != len(expected):
        raise ScannerContractError("score() must return exactly one result per evaluation")


def _check_findings(findings: tuple[Finding, ...], evaluations: tuple[RuleEvaluation, ...]) -> None:
    evaluated = {evaluation.key for evaluation in evaluations}
    seen: set[tuple[str, str]] = set()
    for finding in findings:
        if finding.recommendation is Recommendation.EXCLUDED:
            raise ScannerContractError(
                "recommend() must not emit EXCLUDED; exclusions are applied by the framework"
            )
        if finding.key not in evaluated:
            raise ScannerContractError(
                f"recommend() emitted a finding with no matching evaluation: {finding.key}"
            )
        if finding.key in seen:
            raise ScannerContractError(f"recommend() emitted duplicate finding {finding.key}")
        seen.add(finding.key)


def _apply_candidate_gate(finding: Finding, min_confidence: int) -> Finding:
    """Downgrade archive/pause/delete candidates to REVIEW unless confidence is known and
    meets the policy minimum."""
    if finding.recommendation not in CANDIDATE_RECOMMENDATIONS:
        return finding
    if finding.confidence is not None and finding.confidence >= min_confidence:
        return finding
    why = (
        "confidence is unknown"
        if finding.confidence is None
        else f"confidence {finding.confidence} is below the policy minimum of {min_confidence}"
    )
    return replace(
        finding,
        recommendation=Recommendation.REVIEW,
        downgraded_from=finding.recommendation,
        reasons=(
            *finding.reasons,
            f"Downgraded from {finding.recommendation.value} to review: {why}.",
        ),
    )
