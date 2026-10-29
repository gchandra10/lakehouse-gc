"""Scanner registry. The runner resolves scanners by name here; it never hard-codes them."""

from __future__ import annotations

import dataclasses
import inspect
import re
import threading
from collections.abc import Iterable
from typing import TypeVar

from lakehouse_gc.core._validation import close_match_hint
from lakehouse_gc.core.config import validate_config_model
from lakehouse_gc.core.errors import (
    DuplicateScannerError,
    InvalidScannerError,
    RegistryError,
    UnknownScannerError,
)
from lakehouse_gc.core.scanner import RuleSpec, Scanner

SCANNER_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")

_S = TypeVar("_S", bound=type[Scanner])


class ScannerRegistry:
    def __init__(self) -> None:
        self._scanners: dict[str, type[Scanner]] = {}

    def register(self, scanner_cls: _S) -> _S:
        """Register a scanner class. Usable as a decorator. Returns the class unchanged."""
        if not (isinstance(scanner_cls, type) and issubclass(scanner_cls, Scanner)):
            raise InvalidScannerError(f"{scanner_cls!r} is not a Scanner subclass")
        name = getattr(scanner_cls, "name", None)
        if not isinstance(name, str) or not SCANNER_NAME_PATTERN.fullmatch(name):
            raise InvalidScannerError(
                f"{scanner_cls.__qualname__}.name must match {SCANNER_NAME_PATTERN.pattern}, "
                f"got {name!r}"
            )
        if inspect.isabstract(scanner_cls):
            missing = ", ".join(sorted(scanner_cls.__abstractmethods__))
            raise InvalidScannerError(f"scanner '{name}' is abstract; missing: {missing}")
        _validate_rules(name, scanner_cls.rules)
        validate_config_model(scanner_cls.config_model, name)
        _validate_rule_settings(name, scanner_cls)
        existing = self._scanners.get(name)
        if existing is not None:
            raise DuplicateScannerError(
                f"scanner '{name}' is already registered by "
                f"{existing.__module__}.{existing.__qualname__}"
            )
        self._scanners[name] = scanner_cls
        return scanner_cls

    def copy(self) -> ScannerRegistry:
        """Independent registry with the same registrations."""
        clone = ScannerRegistry()
        clone._scanners = dict(self._scanners)
        return clone

    def get(self, name: str) -> type[Scanner]:
        try:
            return self._scanners[name]
        except KeyError:
            raise UnknownScannerError(self._unknown_message([name])) from None

    def names(self) -> tuple[str, ...]:
        return tuple(sorted(self._scanners))

    def __contains__(self, name: object) -> bool:
        return name in self._scanners

    def __len__(self) -> int:
        return len(self._scanners)

    def resolve(self, names: Iterable[str] | None = None) -> tuple[type[Scanner], ...]:
        """Resolve requested names (all registered if ``None``) in deterministic name order.

        Duplicates are ignored. Unknown names fail before anything runs.
        """
        if names is None:
            selected = list(self.names())
            if not selected:
                raise RegistryError("no scanners are registered")
        else:
            if isinstance(names, str):
                raise RegistryError("scanners must be a list of names, not a single string")
            requested = list(names)
            if not requested:
                raise RegistryError("no scanners requested")
            unknown = sorted({name for name in requested if name not in self._scanners})
            if unknown:
                raise UnknownScannerError(self._unknown_message(unknown))
            selected = sorted(set(requested))
        return tuple(self._scanners[name] for name in selected)

    def _unknown_message(self, unknown: list[str]) -> str:
        parts = [f"'{name}'{close_match_hint(name, self._scanners)}" for name in unknown]
        available = ", ".join(self.names()) or "(none)"
        return f"unknown scanner(s): {', '.join(parts)}; registered scanners: {available}"


def _validate_rules(name: str, rules: object) -> None:
    if not isinstance(rules, tuple):
        raise InvalidScannerError(f"scanner '{name}': rules must be a tuple of RuleSpec")
    seen: set[str] = set()
    for rule in rules:
        if not isinstance(rule, RuleSpec):
            raise InvalidScannerError(f"scanner '{name}': rules must contain only RuleSpec")
        if not rule.rule_id.startswith(f"{name}."):
            raise InvalidScannerError(
                f"scanner '{name}': rule_id '{rule.rule_id}' must start with '{name}.'"
            )
        if rule.rule_id in seen:
            raise InvalidScannerError(f"scanner '{name}': duplicate rule_id '{rule.rule_id}'")
        seen.add(rule.rule_id)


def _validate_rule_settings(name: str, scanner_cls: type[Scanner]) -> None:
    model = scanner_cls.config_model
    fields = {f.name for f in dataclasses.fields(model)} if model is not None else set()
    for rule in scanner_cls.rules:
        unknown = sorted(set(rule.settings) - fields)
        if unknown:
            raise InvalidScannerError(
                f"scanner '{name}': rule '{rule.rule_id}' lists unknown settings: "
                f"{', '.join(unknown)}"
            )
    types = scanner_cls.resource_types
    if not isinstance(types, tuple) or not all(isinstance(t, str) and t for t in types):
        raise InvalidScannerError(f"scanner '{name}': resource_types must be a tuple of str")


_default_registry: ScannerRegistry | None = None
_default_lock = threading.Lock()


def register_builtins(registry: ScannerRegistry) -> None:
    """Register the built-in scanners. Idempotent for the same classes."""
    from lakehouse_gc.scanners import BUILTIN_SCANNERS

    for scanner_cls in BUILTIN_SCANNERS:
        if registry._scanners.get(scanner_cls.name) is not scanner_cls:
            registry.register(scanner_cls)


def get_default_registry() -> ScannerRegistry:
    """Process-wide registry, pre-populated with built-in scanners on first use."""
    global _default_registry
    with _default_lock:
        if _default_registry is None:
            registry = ScannerRegistry()
            register_builtins(registry)
            _default_registry = registry
        return _default_registry


def register_scanner(scanner_cls: _S) -> _S:
    """Decorator registering a scanner in the default registry."""
    return get_default_registry().register(scanner_cls)
