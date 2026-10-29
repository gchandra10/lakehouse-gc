from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import pytest

from conftest import StubScanner, make_stub
from lakehouse_gc.core.errors import (
    DuplicateScannerError,
    InvalidScannerError,
    RegistryError,
    UnknownScannerError,
)
from lakehouse_gc.core.models import Dimension, Resource, Severity
from lakehouse_gc.core.registry import ScannerRegistry, get_default_registry, register_builtins
from lakehouse_gc.core.scanner import RuleSpec, Scanner, ScannerContext
from lakehouse_gc.scanners.example import ExampleScanner


def test_register_and_get(registry: ScannerRegistry) -> None:
    assert registry.get("example") is ExampleScanner
    assert "example" in registry
    assert registry.names() == ("example",)


def test_register_works_as_decorator() -> None:
    reg = ScannerRegistry()

    @reg.register
    class Decorated(StubScanner):
        name = "stub"

    assert reg.get("stub") is Decorated


def test_duplicate_name_rejected(registry: ScannerRegistry) -> None:
    with pytest.raises(DuplicateScannerError, match="already registered"):
        registry.register(make_stub("example"))


def test_unknown_scanner_suggests(registry: ScannerRegistry) -> None:
    with pytest.raises(UnknownScannerError, match="did you mean 'example'"):
        registry.get("exmple")
    with pytest.raises(UnknownScannerError, match=r"'ai'.*'uc'"):
        registry.resolve(["example", "uc", "ai"])


@pytest.mark.parametrize("name", ["", "Example", "1abc", "has-dash", None])
def test_invalid_names_rejected(name: object) -> None:
    cls = type("Bad", (StubScanner,), {"name": name})
    with pytest.raises(InvalidScannerError, match="must match"):
        ScannerRegistry().register(cls)


def test_non_scanner_rejected() -> None:
    with pytest.raises(InvalidScannerError, match="not a Scanner subclass"):
        ScannerRegistry().register(object)  # type: ignore[type-var]


def test_abstract_scanner_rejected() -> None:
    class Incomplete(Scanner):
        name = "incomplete"

        def discover(self, ctx: ScannerContext) -> Sequence[Resource]:
            return []

    with pytest.raises(InvalidScannerError, match="abstract"):
        ScannerRegistry().register(Incomplete)


@pytest.mark.parametrize(
    "rules",
    [
        (RuleSpec("other.check", "x", Severity.LOW, frozenset({Dimension.COST})),),
        (
            RuleSpec("bad.check", "x", Severity.LOW, frozenset()),
            RuleSpec("bad.check", "y", Severity.LOW, frozenset()),
        ),
        ["not-a-tuple"],
    ],
)
def test_invalid_rules_rejected(rules: object) -> None:
    cls = type("Bad", (StubScanner,), {"name": "bad", "rules": rules})
    with pytest.raises(InvalidScannerError):
        ScannerRegistry().register(cls)


def test_unsupported_config_model_rejected() -> None:
    @dataclass(frozen=True)
    class BadConfig:
        mapping: dict[str, int]

    cls = make_stub("cfg", config_model=BadConfig)
    with pytest.raises(InvalidScannerError, match="unsupported type"):
        ScannerRegistry().register(cls)


def test_resolve_is_sorted_and_deduplicated() -> None:
    reg = ScannerRegistry()
    b, a = make_stub("bravo"), make_stub("alpha")
    reg.register(b)
    reg.register(a)
    assert reg.resolve(["bravo", "alpha", "bravo"]) == (a, b)
    assert reg.resolve(None) == (a, b)


def test_resolve_rejects_bad_requests(registry: ScannerRegistry) -> None:
    with pytest.raises(RegistryError, match="no scanners requested"):
        registry.resolve([])
    with pytest.raises(RegistryError, match="not a single string"):
        registry.resolve("example")
    with pytest.raises(RegistryError, match="no scanners are registered"):
        ScannerRegistry().resolve(None)


def test_default_registry_has_builtins_and_is_idempotent() -> None:
    reg = get_default_registry()
    assert reg is get_default_registry()
    assert reg.get("example") is ExampleScanner
    register_builtins(reg)
    assert len(reg) >= 1
