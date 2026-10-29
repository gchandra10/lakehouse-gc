"""Exception hierarchy for lakehouse_gc."""

from __future__ import annotations


class LakehouseGCError(Exception):
    """Base class for all lakehouse_gc errors."""


class ModelValidationError(LakehouseGCError, ValueError):
    """A model was constructed with invalid field values."""


class PolicyError(LakehouseGCError):
    """A policy could not be read or parsed."""


class PolicyValidationError(PolicyError):
    """A policy is structurally or semantically invalid.

    ``path`` is the dotted location of the offending value, e.g. ``scoring.cost_weight``.
    """

    def __init__(self, path: str, message: str) -> None:
        self.path = path
        self.message = message
        super().__init__(f"{path}: {message}" if path else message)


class UnsupportedPolicyVersionError(PolicyValidationError):
    """The policy declares a ``policy_version`` this release does not support."""


class RegistryError(LakehouseGCError):
    """Base class for scanner registry errors."""


class DuplicateScannerError(RegistryError):
    """A scanner name is already registered."""


class UnknownScannerError(RegistryError):
    """A requested scanner name is not registered."""


class InvalidScannerError(RegistryError):
    """A class does not satisfy the scanner contract and cannot be registered."""


class ScannerContractError(LakehouseGCError):
    """A scanner returned output that violates the scanner contract."""
