from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, ClassVar

import pytest

from lakehouse_gc.core.evidence import Evidence, EvidenceStatus
from lakehouse_gc.core.models import (
    Dimension,
    Finding,
    Outcome,
    Phase,
    Recommendation,
    Resource,
    ResourceRef,
    RuleEvaluation,
    ScoredEvaluation,
    Severity,
)
from lakehouse_gc.core.registry import ScannerRegistry
from lakehouse_gc.core.scanner import RuleSpec, Scanner, ScannerContext
from lakehouse_gc.scanners.example import ExampleScanner

ROOT = Path(__file__).resolve().parent.parent
EXAMPLE_POLICY = ROOT / "examples" / "policy.yaml"
FIXED_NOW = datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc)


def fixed_clock() -> datetime:
    return FIXED_NOW


class StubScanner(Scanner):
    """Configurable scanner for runner tests. ``outcomes`` maps resource id -> outcome."""

    name = "stub"
    rules = (
        RuleSpec("stub.check", "Stub check", Severity.MEDIUM, frozenset({Dimension.GOVERNANCE})),
    )
    outcomes: ClassVar[tuple[tuple[str, Outcome], ...]] = (("r1", Outcome.PASS),)
    fail_phase: ClassVar[Phase | None] = None
    recommendation: ClassVar[Recommendation] = Recommendation.KEEP

    def _maybe_fail(self, phase: Phase) -> None:
        if self.fail_phase is phase:
            raise RuntimeError(f"boom in {phase.value}")

    def discover(self, ctx: ScannerContext) -> Sequence[Resource]:
        self._maybe_fail(Phase.DISCOVER)
        return [
            Resource(ResourceRef("ws", "stub_asset", rid), name=rid) for rid, _ in self.outcomes
        ]

    def analyze(
        self, ctx: ScannerContext, resources: Sequence[Resource]
    ) -> Sequence[RuleEvaluation]:
        self._maybe_fail(Phase.ANALYZE)
        outcomes = dict(self.outcomes)
        status = EvidenceStatus.OBSERVED
        return [
            RuleEvaluation(
                r.ref,
                self.rules[0].rule_id,
                outcomes[r.ref.resource_id],
                evidence=(Evidence("signal", status, "stub", value=1),),
            )
            for r in resources
        ]

    def score(
        self, ctx: ScannerContext, evaluations: Sequence[RuleEvaluation]
    ) -> Sequence[ScoredEvaluation]:
        self._maybe_fail(Phase.SCORE)
        return super().score(ctx, evaluations)

    def recommend(
        self, ctx: ScannerContext, scored: Sequence[ScoredEvaluation]
    ) -> Sequence[Finding]:
        self._maybe_fail(Phase.RECOMMEND)
        return [
            Finding(
                resource=s.evaluation.resource,
                rule_id=s.evaluation.rule_id,
                severity=s.severity,
                recommendation=self.recommendation,
                confidence=s.confidence,
            )
            for s in scored
        ]


def make_stub(name: str, base: type[StubScanner] = StubScanner, **attrs: Any) -> type[Scanner]:
    """Create a stub scanner class named ``name`` with a correctly prefixed rule id."""
    dimensions = attrs.pop("dimensions", frozenset({Dimension.GOVERNANCE}))
    severity = attrs.pop("severity", Severity.MEDIUM)
    namespace = {
        "name": name,
        "rules": (RuleSpec(f"{name}.check", "Stub check", severity, dimensions),),
        **attrs,
    }
    return type(f"{name.title()}Scanner", (base,), namespace)


@pytest.fixture
def registry() -> ScannerRegistry:
    reg = ScannerRegistry()
    reg.register(ExampleScanner)
    return reg
