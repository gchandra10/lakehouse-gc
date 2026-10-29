"""Framework core: models, scanner contract, policy, registry, and scoring."""

from __future__ import annotations

from lakehouse_gc.core.errors import (
    DuplicateScannerError,
    InvalidScannerError,
    LakehouseGCError,
    ModelValidationError,
    PolicyError,
    PolicyValidationError,
    RegistryError,
    ScannerContractError,
    UnknownScannerError,
    UnsupportedPolicyVersionError,
)
from lakehouse_gc.core.evidence import Evidence, EvidenceStatus, derive_confidence
from lakehouse_gc.core.models import (
    Dimension,
    Finding,
    Outcome,
    Phase,
    Recommendation,
    Resource,
    ResourceRef,
    RuleEvaluation,
    RunStatus,
    SavingsEstimate,
    ScannerStatus,
    ScoredEvaluation,
    Severity,
)
from lakehouse_gc.core.policy import Policy, load_policy, loads_policy
from lakehouse_gc.core.registry import (
    ScannerRegistry,
    get_default_registry,
    register_scanner,
)
from lakehouse_gc.core.results import RunResult, ScannerError, ScannerResult
from lakehouse_gc.core.scanner import RuleSpec, Scanner, ScannerContext
from lakehouse_gc.core.scoring import ScoreReport, compute_scores

__all__ = [
    "Dimension",
    "DuplicateScannerError",
    "Evidence",
    "EvidenceStatus",
    "Finding",
    "InvalidScannerError",
    "LakehouseGCError",
    "ModelValidationError",
    "Outcome",
    "Phase",
    "Policy",
    "PolicyError",
    "PolicyValidationError",
    "Recommendation",
    "RegistryError",
    "Resource",
    "ResourceRef",
    "RuleEvaluation",
    "RuleSpec",
    "RunResult",
    "RunStatus",
    "SavingsEstimate",
    "Scanner",
    "ScannerContext",
    "ScannerContractError",
    "ScannerError",
    "ScannerRegistry",
    "ScannerResult",
    "ScannerStatus",
    "ScoreReport",
    "ScoredEvaluation",
    "Severity",
    "UnknownScannerError",
    "UnsupportedPolicyVersionError",
    "compute_scores",
    "derive_confidence",
    "get_default_registry",
    "load_policy",
    "loads_policy",
    "register_scanner",
]
