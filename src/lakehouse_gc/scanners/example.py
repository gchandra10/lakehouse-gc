"""Deterministic example scanner backed by an in-memory fixture. Needs no credentials.

The fixture is illustrative mock data (source ``example-fixture``), not telemetry. Activity
timestamps are relative to the run clock so results are stable across days. It covers every
evidence status and rule outcome so the framework's behavior on missing data is visible.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta

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

FIXTURE_SOURCE = "example-fixture"
SCOPE_ID = "example-workspace"
RESOURCE_TYPE = "example_asset"

OWNER_RULE = RuleSpec(
    rule_id="example.owner_missing",
    title="Resource has no owner",
    severity=Severity.MEDIUM,
    dimensions=frozenset({Dimension.GOVERNANCE}),
    description="Demo rule on mock data: fails when an asset has no owner.",
    settings=("require_owner",),
    on_fail=Recommendation.TAG_OWNER,
)
IDLE_RULE = RuleSpec(
    rule_id="example.idle",
    title="No activity within the idle window",
    severity=Severity.HIGH,
    dimensions=frozenset({Dimension.COST}),
    description="Demo rule on mock data: fails when the last activity is older than idle_days.",
    settings=("idle_days",),
    on_fail=Recommendation.ARCHIVE_CANDIDATE,
)

_O, _I, _U = EvidenceStatus.OBSERVED, EvidenceStatus.INFERRED, EvidenceStatus.UNAVAILABLE


@dataclass(frozen=True)
class ExampleConfig:
    idle_days: int = 30
    require_owner: bool = True

    def __post_init__(self) -> None:
        if self.idle_days < 1:
            raise ValueError("idle_days must be at least 1")


@dataclass(frozen=True)
class _Asset:
    asset_id: str
    name: str
    owner_status: EvidenceStatus
    owner: str | None
    activity_status: EvidenceStatus
    activity_days_ago: int | None


_FIXTURE: tuple[_Asset, ...] = (
    _Asset("a-001", "sales_dashboard", _O, "team-sales@example.com", _O, 3),
    _Asset("a-002", "old_export", _O, None, _O, 400),
    _Asset("a-003", "mystery_asset", _U, None, _U, None),
    _Asset("a-004", "inferred_idle", _O, "team-data@example.com", _I, 200),
    _Asset("a-005", "finance_ledger", _O, "team-finance@example.com", _O, 500),
    _Asset("a-006", "tmp_scratch", _O, "team-ml@example.com", _O, 10),
)


class ExampleScanner(Scanner):
    name = "example"
    rules = (OWNER_RULE, IDLE_RULE)
    config_model = ExampleConfig
    resource_types = (RESOURCE_TYPE,)

    def discover(self, ctx: ScannerContext) -> Sequence[Resource]:
        now = ctx.now()
        return [_to_resource(asset, now) for asset in _FIXTURE]

    def analyze(
        self, ctx: ScannerContext, resources: Sequence[Resource]
    ) -> Sequence[RuleEvaluation]:
        config: ExampleConfig = ctx.config
        now = ctx.now()
        evaluations = []
        for resource in resources:
            if config.require_owner:
                evaluations.append(_evaluate_owner(resource))
            evaluations.append(_evaluate_idle(resource, now, config.idle_days))
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
                    impacts=frozenset(d for d, weight in item.weights.items() if weight > 0),
                )
            )
        return findings


def _to_resource(asset: _Asset, now: datetime) -> Resource:
    owner_evidence = Evidence(
        key="owner",
        status=asset.owner_status,
        source=FIXTURE_SOURCE,
        value=asset.owner if asset.owner_status is not _U else None,
        note="Owner metadata is not available." if asset.owner_status is _U else None,
    )
    if asset.activity_status is _U or asset.activity_days_ago is None:
        activity_evidence = Evidence(
            key="last_activity_at",
            status=_U,
            source=FIXTURE_SOURCE,
            note="Activity telemetry is unavailable; no observed activity is not zero activity.",
        )
    else:
        activity_evidence = Evidence(
            key="last_activity_at",
            status=asset.activity_status,
            source=FIXTURE_SOURCE,
            value=(now - timedelta(days=asset.activity_days_ago)).isoformat(),
            note="Inferred from last modification time." if asset.activity_status is _I else None,
        )
    return Resource(
        ref=ResourceRef(SCOPE_ID, RESOURCE_TYPE, asset.asset_id),
        name=asset.name,
        owner=asset.owner if asset.owner_status is _O else None,
        evidence=(owner_evidence, activity_evidence),
    )


def _evaluate_owner(resource: Resource) -> RuleEvaluation:
    evidence = resource.get_evidence("owner")
    rule_id = OWNER_RULE.rule_id
    if evidence is None or evidence.status is _U:
        return RuleEvaluation(
            resource.ref,
            rule_id,
            Outcome.INDETERMINATE,
            evidence=() if evidence is None else (evidence,),
            reasons=("Ownership could not be determined.",),
        )
    if evidence.value:
        return RuleEvaluation(
            resource.ref, rule_id, Outcome.PASS, (evidence,), (f"Owner is {evidence.value}.",)
        )
    return RuleEvaluation(
        resource.ref, rule_id, Outcome.FAIL, (evidence,), ("No owner is assigned.",)
    )


def _evaluate_idle(resource: Resource, now: datetime, idle_days: int) -> RuleEvaluation:
    evidence = resource.get_evidence("last_activity_at")
    rule_id = IDLE_RULE.rule_id
    if evidence is None or evidence.status is _U or not isinstance(evidence.value, str):
        return RuleEvaluation(
            resource.ref,
            rule_id,
            Outcome.INDETERMINATE,
            evidence=() if evidence is None else (evidence,),
            reasons=("Activity is unknown; absence of observed activity is not inactivity.",),
        )
    idle_for = now - datetime.fromisoformat(evidence.value)
    if idle_for > timedelta(days=idle_days):
        return RuleEvaluation(
            resource.ref,
            rule_id,
            Outcome.FAIL,
            (evidence,),
            (f"Last activity {idle_for.days} days ago exceeds the {idle_days}-day window.",),
        )
    return RuleEvaluation(
        resource.ref,
        rule_id,
        Outcome.PASS,
        (evidence,),
        (f"Last activity {idle_for.days} days ago is within the {idle_days}-day window.",),
    )
