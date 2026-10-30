"""Built-in scanners: the credential-free example scanner, Unity Catalog, and Lakeflow Jobs."""

from __future__ import annotations

from lakehouse_gc.core.scanner import Scanner
from lakehouse_gc.scanners.example import ExampleScanner
from lakehouse_gc.scanners.jobs import LakeflowJobsScanner
from lakehouse_gc.scanners.uc import UnityCatalogScanner

BUILTIN_SCANNERS: tuple[type[Scanner], ...] = (
    ExampleScanner,
    UnityCatalogScanner,
    LakeflowJobsScanner,
)

__all__ = ["BUILTIN_SCANNERS", "ExampleScanner", "LakeflowJobsScanner", "UnityCatalogScanner"]
