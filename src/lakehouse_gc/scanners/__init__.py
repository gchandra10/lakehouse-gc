"""Built-in scanners: the credential-free example scanner and the Unity Catalog scanner."""

from __future__ import annotations

from lakehouse_gc.core.scanner import Scanner
from lakehouse_gc.scanners.example import ExampleScanner
from lakehouse_gc.scanners.uc import UnityCatalogScanner

BUILTIN_SCANNERS: tuple[type[Scanner], ...] = (ExampleScanner, UnityCatalogScanner)

__all__ = ["BUILTIN_SCANNERS", "ExampleScanner", "UnityCatalogScanner"]
