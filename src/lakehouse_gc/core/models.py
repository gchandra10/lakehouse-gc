"""Core immutable models shared by scanners, scoring, and the runner."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from enum import Enum
from types import MappingProxyType

from lakehouse_gc.core._validation import check_confidence, is_number, require_text
from lakehouse_gc.core.errors import ModelValidationError
from lakehouse_gc.core.evidence import Evidence, JsonValue


class Severity(str, Enum):
    INFO = "info"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class Dimension(str, Enum):
    GOVERNANCE = "governance"
    COST = "cost"


class Outcome(str, Enum):
    PASS = "pass"
    FAIL = "fail"
    INDETERMINATE = "indeterminate"
    """Required evidence was unavailable or inconclusive; not treated as a pass."""


class Recommendation(str, Enum):
    """Advisory recommendations. None of these authorize any action on a resource."""

    KEEP = "keep"
    REVIEW = "review"
    TAG_OWNER = "tag_owner"
    ARCHIVE_CANDIDATE = "archive_candidate"
    PAUSE_CANDIDATE = "pause_candidate"
    DELETE_CANDIDATE = "delete_candidate"
    EXCLUDED = "excluded"


CANDIDATE_RECOMMENDATIONS: frozenset[Recommendation] = frozenset(
    {
        Recommendation.ARCHIVE_CANDIDATE,
        Recommendation.PAUSE_CANDIDATE,
        Recommendation.DELETE_CANDIDATE,
    }
)


class Phase(str, Enum):
    INITIALIZE = "initialize"
    DISCOVER = "discover"
    ANALYZE = "analyze"
    SCORE = "score"
    RECOMMEND = "recommend"


class ScannerStatus(str, Enum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class RunStatus(str, Enum):
    SUCCEEDED = "succeeded"
    PARTIAL = "partial"
    FAILED = "failed"


def _require_enum(value: object, enum_type: type[Enum], field_name: str) -> None:
    if not isinstance(value, enum_type):
        raise ModelValidationError(f"{field_name} must be a {enum_type.__name__}")


def _require_text_tuple(values: tuple[str, ...], field_name: str) -> None:
    for value in values:
        require_text(value, field_name)


def _require_evidence(values: tuple[Evidence, ...], field_name: str) -> None:
    for value in values:
        if not isinstance(value, Evidence):
            raise ModelValidationError(f"{field_name} must contain only Evidence")


@dataclass(frozen=True, order=True)
class ResourceRef:
    """Stable resource identity: workspace/account scope, resource type, and ID."""

    scope_id: str
    resource_type: str
    resource_id: str

    def __post_init__(self) -> None:
        require_text(self.scope_id, "ResourceRef.scope_id")
        require_text(self.resource_type, "ResourceRef.resource_type")
        require_text(self.resource_id, "ResourceRef.resource_id")

    @property
    def key(self) -> str:
        return f"{self.scope_id}/{self.resource_type}/{self.resource_id}"

    def __str__(self) -> str:
        return self.key


@dataclass(frozen=True)
class Resource:
    """A discovered resource. ``name`` should be the fully qualified, human-readable name;
    policy exclusion patterns match against it."""

    ref: ResourceRef
    name: str
    owner: str | None = None
    metadata: Mapping[str, JsonValue] = field(default_factory=dict)
    evidence: tuple[Evidence, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.ref, ResourceRef):
            raise ModelValidationError("Resource.ref must be a ResourceRef")
        require_text(self.name, "Resource.name")
        if self.owner is not None:
            require_text(self.owner, "Resource.owner")
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))
        object.__setattr__(self, "evidence", tuple(self.evidence))
        _require_evidence(self.evidence, "Resource.evidence")

    def get_evidence(self, key: str) -> Evidence | None:
        return next((item for item in self.evidence if item.key == key), None)


@dataclass(frozen=True)
class RuleEvaluation:
    """Outcome of evaluating one rule against one resource (output of ``analyze``)."""

    resource: ResourceRef
    rule_id: str
    outcome: Outcome
    evidence: tuple[Evidence, ...] = ()
    reasons: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.resource, ResourceRef):
            raise ModelValidationError("RuleEvaluation.resource must be a ResourceRef")
        require_text(self.rule_id, "RuleEvaluation.rule_id")
        _require_enum(self.outcome, Outcome, "RuleEvaluation.outcome")
        object.__setattr__(self, "evidence", tuple(self.evidence))
        object.__setattr__(self, "reasons", tuple(self.reasons))
        _require_evidence(self.evidence, "RuleEvaluation.evidence")
        _require_text_tuple(self.reasons, "RuleEvaluation.reasons")

    @property
    def key(self) -> tuple[str, str]:
        return (self.resource.key, self.rule_id)


@dataclass(frozen=True)
class ScoredEvaluation:
    """A rule evaluation with severity, per-dimension weights, and confidence (output of
    ``score``). Aggregate scores are computed by the framework, not by scanners."""

    evaluation: RuleEvaluation
    severity: Severity
    weights: Mapping[Dimension, float]
    confidence: int | None

    def __post_init__(self) -> None:
        if not isinstance(self.evaluation, RuleEvaluation):
            raise ModelValidationError("ScoredEvaluation.evaluation must be a RuleEvaluation")
        _require_enum(self.severity, Severity, "ScoredEvaluation.severity")
        weights = dict(self.weights)
        for dimension, weight in weights.items():
            _require_enum(dimension, Dimension, "ScoredEvaluation.weights key")
            if not is_number(weight) or weight < 0:
                raise ModelValidationError(
                    "ScoredEvaluation.weights values must be non-negative numbers"
                )
        object.__setattr__(self, "weights", MappingProxyType(weights))
        check_confidence(self.confidence, "ScoredEvaluation.confidence")


@dataclass(frozen=True)
class SavingsEstimate:
    """An estimated saving. Assumptions and coverage are mandatory; when they cannot be
    stated, scanners must omit the estimate (unknown) instead."""

    amount: Decimal
    currency: str
    period: str
    assumptions: tuple[str, ...]
    coverage: str

    def __post_init__(self) -> None:
        if not isinstance(self.amount, Decimal) or not self.amount.is_finite() or self.amount < 0:
            raise ModelValidationError("SavingsEstimate.amount must be a finite Decimal >= 0")
        require_text(self.currency, "SavingsEstimate.currency")
        require_text(self.period, "SavingsEstimate.period")
        object.__setattr__(self, "assumptions", tuple(self.assumptions))
        if not self.assumptions:
            raise ModelValidationError("SavingsEstimate.assumptions must not be empty")
        _require_text_tuple(self.assumptions, "SavingsEstimate.assumptions")
        require_text(self.coverage, "SavingsEstimate.coverage")


@dataclass(frozen=True)
class ExclusionStatus:
    """Why a resource was excluded by policy. Excluded resources stay visible in results."""

    resource_type: str
    pattern: str
    reason: str
    expires_on: date | None = None


@dataclass(frozen=True)
class Finding:
    """An advisory finding about a resource. Recommendations never authorize action."""

    resource: ResourceRef
    rule_id: str
    severity: Severity
    recommendation: Recommendation
    reasons: tuple[str, ...] = ()
    evidence: tuple[Evidence, ...] = ()
    confidence: int | None = None
    impacts: frozenset[Dimension] = frozenset()
    estimated_savings: SavingsEstimate | None = None
    exclusion: ExclusionStatus | None = None
    downgraded_from: Recommendation | None = None
    scanner: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.resource, ResourceRef):
            raise ModelValidationError("Finding.resource must be a ResourceRef")
        require_text(self.rule_id, "Finding.rule_id")
        _require_enum(self.severity, Severity, "Finding.severity")
        _require_enum(self.recommendation, Recommendation, "Finding.recommendation")
        object.__setattr__(self, "reasons", tuple(self.reasons))
        object.__setattr__(self, "evidence", tuple(self.evidence))
        object.__setattr__(self, "impacts", frozenset(self.impacts))
        _require_text_tuple(self.reasons, "Finding.reasons")
        _require_evidence(self.evidence, "Finding.evidence")
        for impact in self.impacts:
            _require_enum(impact, Dimension, "Finding.impacts")
        check_confidence(self.confidence, "Finding.confidence")
        if self.estimated_savings is not None and not isinstance(
            self.estimated_savings, SavingsEstimate
        ):
            raise ModelValidationError("Finding.estimated_savings must be a SavingsEstimate")
        if (self.recommendation is Recommendation.EXCLUDED) != (self.exclusion is not None):
            raise ModelValidationError(
                "Finding.exclusion must be set if and only if recommendation is EXCLUDED"
            )
        if (
            self.downgraded_from is not None
            and self.downgraded_from not in CANDIDATE_RECOMMENDATIONS
        ):
            raise ModelValidationError("Finding.downgraded_from must be a candidate recommendation")

    @property
    def key(self) -> tuple[str, str]:
        return (self.resource.key, self.rule_id)
