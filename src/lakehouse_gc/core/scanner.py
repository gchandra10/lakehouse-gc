"""Scanner contract: ``discover -> analyze -> score -> recommend``.

State flows explicitly between phases as immutable values; scanners should not keep mutable
state between phases. A fresh scanner instance is created for every run. Exclusions,
contract validation, the candidate-confidence gate, and aggregate scoring are applied by the
framework, not by scanners.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, ClassVar

from lakehouse_gc.core._validation import require_text
from lakehouse_gc.core.errors import ModelValidationError, ScannerContractError
from lakehouse_gc.core.evidence import derive_confidence
from lakehouse_gc.core.models import (
    Dimension,
    Finding,
    Recommendation,
    Resource,
    RuleEvaluation,
    ScoredEvaluation,
    Severity,
)
from lakehouse_gc.core.policy import Policy
from lakehouse_gc.core.sql import SqlClient, default_sql_client


@dataclass(frozen=True)
class RuleSpec:
    """Declares a rule a scanner evaluates. ``rule_id`` must be prefixed ``<scanner>.``.

    ``dimensions`` are the scores the rule affects; its weight in each is the policy's
    severity weight for ``severity``. ``description``, ``settings`` (names of
    ``config_model`` fields the rule uses), and ``on_fail`` document the rule for
    ``describe_rules``.
    """

    rule_id: str
    title: str
    severity: Severity
    dimensions: frozenset[Dimension]
    description: str = ""
    settings: tuple[str, ...] = ()
    on_fail: Recommendation | None = None

    def __post_init__(self) -> None:
        require_text(self.rule_id, "RuleSpec.rule_id")
        require_text(self.title, "RuleSpec.title")
        if not isinstance(self.severity, Severity):
            raise ModelValidationError("RuleSpec.severity must be a Severity")
        object.__setattr__(self, "dimensions", frozenset(self.dimensions))
        if not all(isinstance(dimension, Dimension) for dimension in self.dimensions):
            raise ModelValidationError("RuleSpec.dimensions must contain only Dimension values")
        if not isinstance(self.description, str):
            raise ModelValidationError("RuleSpec.description must be a string")
        object.__setattr__(self, "settings", tuple(self.settings))
        for setting in self.settings:
            require_text(setting, "RuleSpec.settings")
        if self.on_fail is not None and (
            not isinstance(self.on_fail, Recommendation)
            or self.on_fail in (Recommendation.KEEP, Recommendation.EXCLUDED)
        ):
            raise ModelValidationError("RuleSpec.on_fail must be a non-keep, non-excluded value")


@dataclass(frozen=True)
class ScannerContext:
    """Read-only inputs available to every lifecycle phase."""

    run_id: str
    scanner_name: str
    policy: Policy
    config: Any
    logger: logging.Logger
    clock: Callable[[], datetime]
    sql: SqlClient | None = None
    warnings: list[str] = field(default_factory=list)

    def now(self) -> datetime:
        """Current timezone-aware UTC time from the run's clock."""
        return self.clock()

    def get_sql(self) -> SqlClient:
        """The run's SQL client, else the active Spark session. Raises outside Databricks."""
        return self.sql if self.sql is not None else default_sql_client()

    def warn(self, message: str) -> None:
        """Record a non-fatal limitation; it appears in ``RunResult.warnings``."""
        self.warnings.append(message)
        self.logger.warning(message, extra={"event": "scanner.warning"})


class Scanner(ABC):
    """Base class for scanners. Subclasses set ``name`` and ``rules``, optionally
    ``config_model``, and implement ``discover``, ``analyze``, and ``recommend``."""

    name: ClassVar[str]
    rules: ClassVar[tuple[RuleSpec, ...]] = ()
    config_model: ClassVar[type | None] = None
    resource_types: ClassVar[tuple[str, ...]] = ()
    """Resource types this scanner emits; used to flag policy exclusions that can't match."""

    @classmethod
    def rule_specs(cls) -> Mapping[str, RuleSpec]:
        return {rule.rule_id: rule for rule in cls.rules}

    @abstractmethod
    def discover(self, ctx: ScannerContext) -> Sequence[Resource]:
        """Return the resources in scope. Must not mutate anything."""

    @abstractmethod
    def analyze(
        self, ctx: ScannerContext, resources: Sequence[Resource]
    ) -> Sequence[RuleEvaluation]:
        """Evaluate rules against non-excluded resources. Use ``Outcome.INDETERMINATE`` when
        required evidence is unavailable; never substitute a guessed value."""

    def score(
        self, ctx: ScannerContext, evaluations: Sequence[RuleEvaluation]
    ) -> Sequence[ScoredEvaluation]:
        """Attach severity, per-dimension weights, and confidence to each evaluation.

        The default derives weights from the policy's severity weights and confidence from
        evidence status (see ``derive_confidence``). Override only to refine confidence.
        """
        specs = self.rule_specs()
        severity_weights = ctx.policy.scoring.severity_weights
        scored = []
        for evaluation in evaluations:
            spec = specs.get(evaluation.rule_id)
            if spec is None:
                raise ScannerContractError(f"unknown rule_id '{evaluation.rule_id}'")
            weight = severity_weights[spec.severity]
            scored.append(
                ScoredEvaluation(
                    evaluation=evaluation,
                    severity=spec.severity,
                    weights={dimension: weight for dimension in spec.dimensions},
                    confidence=derive_confidence(evaluation.evidence),
                )
            )
        return scored

    @abstractmethod
    def recommend(
        self, ctx: ScannerContext, scored: Sequence[ScoredEvaluation]
    ) -> Sequence[Finding]:
        """Turn scored evaluations into advisory findings (at most one per evaluation)."""
