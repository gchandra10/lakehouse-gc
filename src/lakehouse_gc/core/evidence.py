"""Evidence model: every signal a scanner relies on carries explicit status and provenance.

``UNAVAILABLE`` evidence means the signal could not be obtained. It is never equivalent to a
zero or empty observation: "no activity observed" is not "zero activity".
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from fractions import Fraction
from typing import Union

from lakehouse_gc.core._validation import require_text
from lakehouse_gc.core.errors import ModelValidationError

JsonValue = Union[  # noqa: UP007 - recursive alias must stay a runtime Union
    None, bool, int, float, str, Sequence["JsonValue"], Mapping[str, "JsonValue"]
]


class EvidenceStatus(str, Enum):
    OBSERVED = "observed"
    """Directly measured from a named source."""
    INFERRED = "inferred"
    """Derived indirectly (e.g. from modification time); weaker than observed."""
    UNAVAILABLE = "unavailable"
    """The signal could not be obtained; its value is unknown."""


@dataclass(frozen=True)
class Evidence:
    key: str
    status: EvidenceStatus
    source: str
    value: JsonValue = None
    note: str | None = None

    def __post_init__(self) -> None:
        require_text(self.key, "Evidence.key")
        if not isinstance(self.status, EvidenceStatus):
            raise ModelValidationError("Evidence.status must be an EvidenceStatus")
        require_text(self.source, "Evidence.source")
        if self.status is EvidenceStatus.UNAVAILABLE and self.value is not None:
            raise ModelValidationError(
                f"Evidence '{self.key}' is unavailable and must not carry a value"
            )


STATUS_CONFIDENCE: Mapping[EvidenceStatus, int] = {
    EvidenceStatus.OBSERVED: 100,
    EvidenceStatus.INFERRED: 50,
    EvidenceStatus.UNAVAILABLE: 0,
}


def derive_confidence(evidence: Iterable[Evidence]) -> int | None:
    """Default confidence: mean of per-item status scores, rounded half up.

    Returns ``None`` (unknown) when there is no evidence at all. Confidence reflects evidence
    quality only; it is not a probability that acting on a recommendation is safe.
    """
    items = tuple(evidence)
    if not items:
        return None
    mean = Fraction(sum(STATUS_CONFIDENCE[item.status] for item in items), len(items))
    return int(mean + Fraction(1, 2))
