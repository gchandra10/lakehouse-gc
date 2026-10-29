"""Versioned policy model and strict YAML loader.

Unknown keys, wrong types, duplicate YAML keys, and unsupported versions all fail with a
``PolicyValidationError`` naming the dotted path of the offending value. Policy objects
validate their invariants on construction, so programmatically built policies are held to
the same rules as YAML ones.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date, datetime
from fnmatch import fnmatchcase
from pathlib import Path
from types import MappingProxyType
from typing import Any, TypeVar

import yaml

from lakehouse_gc.core._validation import close_match_hint, is_number, parse_date, type_name
from lakehouse_gc.core.errors import (
    PolicyError,
    PolicyValidationError,
    UnsupportedPolicyVersionError,
)
from lakehouse_gc.core.models import Dimension, Resource, Severity

POLICY_VERSION = 1
REPORT_ONLY = "report_only"

DEFAULT_SEVERITY_WEIGHTS: Mapping[Severity, float] = MappingProxyType(
    {
        Severity.INFO: 0,
        Severity.LOW: 1,
        Severity.MEDIUM: 3,
        Severity.HIGH: 5,
        Severity.CRITICAL: 8,
    }
)
DEFAULT_GRADE_BOUNDARIES: tuple[tuple[str, float], ...] = (
    ("A", 90),
    ("B", 80),
    ("C", 70),
    ("D", 60),
    ("F", 0),
)
DEFAULT_MIN_CANDIDATE_CONFIDENCE = 70


def _require_policy_text(value: object, path: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise PolicyValidationError(path, "must be a non-empty string")


_SCOPE_PARTS = {
    "include_catalogs": 1,
    "exclude_catalogs": 1,
    "include_schemas": 2,
    "exclude_schemas": 2,
    "include_tables": 3,
    "exclude_tables": 3,
}
_SCOPE_EXAMPLES = {1: "main", 2: "main.sales", 3: "main.sales.orders"}


def _matches(name: str, patterns: tuple[str, ...]) -> bool:
    lowered = name.lower()
    return any(fnmatchcase(lowered, pattern.lower()) for pattern in patterns)


def _prefixes(patterns: tuple[str, ...], parts: int) -> tuple[str, ...]:
    return tuple(".".join(pattern.split(".")[:parts]) for pattern in patterns)


@dataclass(frozen=True)
class Scope:
    """Which Unity Catalog objects scanners read.

    Patterns are case-insensitive globs on fully qualified names: catalogs ``main``, schemas
    ``main.sales``, tables ``main.sales.orders``. An empty include list means "all". Excludes
    always win. Narrower includes also narrow the levels above them: ``include_tables:
    [main.sales.orders]`` alone scans only catalog ``main`` and schema ``main.sales``.
    """

    include_catalogs: tuple[str, ...] = ()
    exclude_catalogs: tuple[str, ...] = ()
    include_schemas: tuple[str, ...] = ()
    exclude_schemas: tuple[str, ...] = ()
    include_tables: tuple[str, ...] = ()
    exclude_tables: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for name, parts in _SCOPE_PARTS.items():
            values = tuple(getattr(self, name))
            for index, value in enumerate(values):
                _require_policy_text(value, f"{name}[{index}]")
                if len(value.split(".")) != parts:
                    raise PolicyValidationError(
                        f"{name}[{index}]",
                        f"expected a {parts}-part name like '{_SCOPE_EXAMPLES[parts]}', "
                        f"got '{value}'",
                    )
            object.__setattr__(self, name, values)

    def includes_catalog(self, catalog: str) -> bool:
        if _matches(catalog, self.exclude_catalogs):
            return False
        constraints = (
            self.include_catalogs,
            _prefixes(self.include_schemas, 1),
            _prefixes(self.include_tables, 1),
        )
        return all(not c or _matches(catalog, c) for c in constraints)

    def includes_schema(self, catalog: str, schema: str) -> bool:
        name = f"{catalog}.{schema}"
        if not self.includes_catalog(catalog) or _matches(name, self.exclude_schemas):
            return False
        constraints = (self.include_schemas, _prefixes(self.include_tables, 2))
        return all(not c or _matches(name, c) for c in constraints)

    def includes_table(self, catalog: str, schema: str, table: str) -> bool:
        name = f"{catalog}.{schema}.{table}"
        if not self.includes_schema(catalog, schema) or _matches(name, self.exclude_tables):
            return False
        return not self.include_tables or _matches(name, self.include_tables)


@dataclass(frozen=True)
class Exclusion:
    """Protects matching resources from analysis. Matching is a case-insensitive glob on
    ``Resource.name`` for the given ``resource_type``. ``expires_on`` is inclusive: the
    exclusion applies through that date and is ignored (with a warning) afterwards."""

    resource_type: str
    pattern: str
    reason: str
    expires_on: date | None = None

    def __post_init__(self) -> None:
        _require_policy_text(self.resource_type, "resource_type")
        _require_policy_text(self.pattern, "pattern")
        _require_policy_text(self.reason, "reason")
        if self.expires_on is not None and (
            isinstance(self.expires_on, datetime) or not isinstance(self.expires_on, date)
        ):
            raise PolicyValidationError("expires_on", "must be a date or null")

    def matches(self, resource: Resource) -> bool:
        return resource.ref.resource_type.lower() == self.resource_type.lower() and fnmatchcase(
            resource.name.lower(), self.pattern.lower()
        )

    def is_expired(self, today: date) -> bool:
        return self.expires_on is not None and today > self.expires_on


@dataclass(frozen=True)
class ScoringConfig:
    governance_weight: float = 0.5
    cost_weight: float = 0.5
    unknown_penalty: float = 0.5
    severity_weights: Mapping[Severity, float] = field(
        default_factory=lambda: dict(DEFAULT_SEVERITY_WEIGHTS)
    )
    grade_boundaries: tuple[tuple[str, float], ...] = DEFAULT_GRADE_BOUNDARIES

    def __post_init__(self) -> None:
        for name in ("governance_weight", "cost_weight"):
            value = getattr(self, name)
            if not is_number(value) or value < 0:
                raise PolicyValidationError(name, "must be a non-negative number")
        if self.governance_weight + self.cost_weight <= 0:
            raise PolicyValidationError(
                "governance_weight", "governance_weight and cost_weight must not both be zero"
            )
        if not is_number(self.unknown_penalty) or not 0 < self.unknown_penalty <= 1:
            raise PolicyValidationError(
                "unknown_penalty",
                "must be greater than 0 and at most 1; a zero penalty would reward missing "
                "visibility",
            )
        self._validate_severity_weights()
        self._validate_grade_boundaries()

    def _validate_severity_weights(self) -> None:
        weights = dict(self.severity_weights)
        for severity, weight in weights.items():
            if not isinstance(severity, Severity):
                raise PolicyValidationError("severity_weights", f"unknown severity {severity!r}")
            if not is_number(weight) or weight < 0:
                raise PolicyValidationError(
                    f"severity_weights.{severity.value}", "must be a non-negative number"
                )
        missing = [severity.value for severity in Severity if severity not in weights]
        if missing:
            raise PolicyValidationError(
                "severity_weights", f"missing weights for: {', '.join(missing)}"
            )
        object.__setattr__(self, "severity_weights", MappingProxyType(weights))

    def _validate_grade_boundaries(self) -> None:
        bounds = tuple((grade, minimum) for grade, minimum in self.grade_boundaries)
        if not bounds:
            raise PolicyValidationError("grade_boundaries", "must define at least one grade")
        grades = [grade for grade, _ in bounds]
        for grade, minimum in bounds:
            _require_policy_text(grade, "grade_boundaries")
            if not is_number(minimum) or not 0 <= minimum <= 100:
                raise PolicyValidationError(
                    f"grade_boundaries.{grade}", "must be a number from 0 to 100"
                )
        if len(set(grades)) != len(grades):
            raise PolicyValidationError("grade_boundaries", "grade names must be unique")
        minimums = [minimum for _, minimum in bounds]
        if len(set(minimums)) != len(minimums):
            raise PolicyValidationError("grade_boundaries", "minimum scores must be unique")
        if 0 not in minimums:
            raise PolicyValidationError(
                "grade_boundaries", "one grade must have a minimum of 0 so every score is graded"
            )
        object.__setattr__(
            self,
            "grade_boundaries",
            tuple(sorted(bounds, key=lambda bound: bound[1], reverse=True)),
        )

    @property
    def dimension_weights(self) -> Mapping[Dimension, float]:
        return MappingProxyType(
            {Dimension.GOVERNANCE: self.governance_weight, Dimension.COST: self.cost_weight}
        )


@dataclass(frozen=True)
class ActionsConfig:
    """V1 is report-only. Any configuration that would permit mutation is rejected."""

    mode: str = REPORT_ONLY
    allow_delete: bool = False
    allow_pause: bool = False
    min_candidate_confidence: int = DEFAULT_MIN_CANDIDATE_CONFIDENCE

    def __post_init__(self) -> None:
        if self.mode != REPORT_ONLY:
            raise PolicyValidationError(
                "mode", f"only '{REPORT_ONLY}' is supported; lakehouse-gc never mutates resources"
            )
        for name in ("allow_delete", "allow_pause"):
            if getattr(self, name) is not False:
                raise PolicyValidationError(
                    name, "must be false; lakehouse-gc is report-only and never mutates resources"
                )
        value = self.min_candidate_confidence
        if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 100:
            raise PolicyValidationError(
                "min_candidate_confidence", "must be an integer from 0 to 100"
            )


@dataclass(frozen=True)
class Policy:
    policy_version: int = POLICY_VERSION
    scope: Scope = field(default_factory=Scope)
    exclusions: tuple[Exclusion, ...] = ()
    rules: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
    sources: Mapping[str, Any] = field(default_factory=dict)
    """Where scanners read from; validated against declared sources at run time."""
    scoring: ScoringConfig = field(default_factory=ScoringConfig)
    actions: ActionsConfig = field(default_factory=ActionsConfig)
    source: str | None = None

    def __post_init__(self) -> None:
        version = self.policy_version
        if isinstance(version, bool) or not isinstance(version, int):
            raise PolicyValidationError(
                "policy_version", f"must be an integer, got {type_name(version)}"
            )
        if version != POLICY_VERSION:
            raise UnsupportedPolicyVersionError(
                "policy_version",
                f"unsupported policy version {version}; this release supports {POLICY_VERSION}",
            )
        exclusions = tuple(self.exclusions)
        for index, exclusion in enumerate(exclusions):
            if not isinstance(exclusion, Exclusion):
                raise PolicyValidationError(f"exclusions[{index}]", "must be an Exclusion")
        object.__setattr__(self, "exclusions", exclusions)
        rules: dict[str, Mapping[str, Any]] = {}
        for name, section in self.rules.items():
            if not isinstance(section, Mapping):
                raise PolicyValidationError(f"rules.{name}", "must be a mapping")
            rules[name] = MappingProxyType(dict(section))
        object.__setattr__(self, "rules", MappingProxyType(rules))
        if not isinstance(self.sources, Mapping):
            raise PolicyValidationError("sources", "must be a mapping")
        object.__setattr__(self, "sources", MappingProxyType(dict(self.sources)))


# --- YAML loading -----------------------------------------------------------------------


class _StrictLoader(yaml.SafeLoader):
    """SafeLoader that rejects duplicate mapping keys instead of silently keeping the last."""


def _construct_strict_mapping(loader: _StrictLoader, node: yaml.MappingNode) -> dict[Any, Any]:
    loader.flatten_mapping(node)
    seen: set[Any] = set()
    for key_node, _ in node.value:
        key = loader.construct_object(key_node, deep=True)
        line = key_node.start_mark.line + 1
        try:
            duplicate = key in seen
        except TypeError:
            raise PolicyValidationError("", f"unsupported mapping key at line {line}") from None
        if duplicate:
            raise PolicyValidationError("", f"duplicate key '{key}' at line {line}")
        seen.add(key)
    return loader.construct_mapping(node, deep=True)


_StrictLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_strict_mapping,
)


def load_policy(path: str | os.PathLike[str]) -> Policy:
    """Load and validate a policy YAML file."""
    policy_path = Path(path)
    try:
        text = policy_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise PolicyError(f"cannot read policy file '{policy_path}': {exc}") from exc
    return loads_policy(text, source=str(policy_path))


def loads_policy(text: str, source: str = "<string>") -> Policy:
    """Parse and validate a policy from YAML text."""
    try:
        data = yaml.load(text, Loader=_StrictLoader)
    except yaml.YAMLError as exc:
        raise PolicyError(f"invalid YAML in policy '{source}': {exc}") from None
    if data is None:
        raise PolicyValidationError("", f"policy '{source}' is empty")
    return policy_from_mapping(data, source=source)


_TOP_KEYS = frozenset(
    {"policy_version", "scope", "exclusions", "rules", "sources", "scoring", "actions"}
)
_SCOPE_KEYS = frozenset(_SCOPE_PARTS)
_EXCLUSION_KEYS = frozenset({"resource_type", "pattern", "reason", "expires_on"})
_SCORING_KEYS = frozenset(
    {"governance_weight", "cost_weight", "unknown_penalty", "severity_weights", "grade_boundaries"}
)
_ACTIONS_KEYS = frozenset({"mode", "allow_delete", "allow_pause", "min_candidate_confidence"})

_T = TypeVar("_T")


def policy_from_mapping(data: Any, source: str | None = None) -> Policy:
    """Validate an already-parsed mapping (e.g. from YAML or JSON) into a ``Policy``."""
    root = _mapping(data, "")
    _check_keys(root, _TOP_KEYS, "")
    if "policy_version" not in root:
        raise PolicyValidationError("policy_version", "required key is missing")
    return _construct(
        Policy,
        "",
        policy_version=root["policy_version"],
        scope=_parse_scope(root.get("scope"), "scope"),
        exclusions=_parse_exclusions(root.get("exclusions"), "exclusions"),
        rules=_parse_rules(root.get("rules"), "rules"),
        sources=_optional_mapping(root.get("sources"), "sources"),
        scoring=_parse_scoring(root.get("scoring"), "scoring"),
        actions=_parse_actions(root.get("actions"), "actions"),
        source=source,
    )


def _join(prefix: str, path: str) -> str:
    if not prefix:
        return path
    if not path:
        return prefix
    return f"{prefix}{path}" if path.startswith("[") else f"{prefix}.{path}"


def _construct(cls: type[_T], prefix: str, **kwargs: Any) -> _T:
    try:
        return cls(**kwargs)
    except PolicyValidationError as exc:
        if not prefix:
            raise
        raise type(exc)(_join(prefix, exc.path), exc.message) from None


def _mapping(value: Any, path: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise PolicyValidationError(path or "policy", f"expected a mapping, got {type_name(value)}")
    for key in value:
        if not isinstance(key, str):
            raise PolicyValidationError(path or "policy", f"keys must be strings, got {key!r}")
    return value


def _optional_mapping(value: Any, path: str) -> dict[str, Any]:
    return {} if value is None else _mapping(value, path)


def _check_keys(data: Mapping[str, Any], allowed: frozenset[str], path: str) -> None:
    for key in sorted(set(data) - allowed):
        hint = close_match_hint(key, allowed)
        raise PolicyValidationError(
            _join(path, key), f"unknown key{hint}; allowed keys: {', '.join(sorted(allowed))}"
        )


def _number(value: Any, path: str) -> float:
    if not is_number(value):
        raise PolicyValidationError(path, f"expected a number, got {type_name(value)}")
    return float(value)


def _bool(value: Any, path: str) -> bool:
    if not isinstance(value, bool):
        raise PolicyValidationError(path, f"expected a boolean, got {type_name(value)}")
    return value


def _int(value: Any, path: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise PolicyValidationError(path, f"expected an integer, got {type_name(value)}")
    return value


def _str(value: Any, path: str) -> str:
    if not isinstance(value, str):
        raise PolicyValidationError(path, f"expected a string, got {type_name(value)}")
    return value


def _str_list(value: Any, path: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        raise PolicyValidationError(path, f"expected a list, got {type_name(value)}")
    return tuple(_str(item, f"{path}[{index}]") for index, item in enumerate(value))


def _parse_scope(value: Any, path: str) -> Scope:
    data = _optional_mapping(value, path)
    _check_keys(data, _SCOPE_KEYS, path)
    kwargs = {key: _str_list(item, _join(path, key)) for key, item in data.items()}
    return _construct(Scope, path, **kwargs)


def _parse_exclusions(value: Any, path: str) -> tuple[Exclusion, ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        raise PolicyValidationError(path, f"expected a list, got {type_name(value)}")
    exclusions = []
    for index, item in enumerate(value):
        item_path = f"{path}[{index}]"
        data = _mapping(item, item_path)
        _check_keys(data, _EXCLUSION_KEYS, item_path)
        for required in ("resource_type", "pattern", "reason"):
            if required not in data:
                raise PolicyValidationError(_join(item_path, required), "required key is missing")
        expires = data.get("expires_on")
        exclusions.append(
            _construct(
                Exclusion,
                item_path,
                resource_type=_str(data["resource_type"], _join(item_path, "resource_type")),
                pattern=_str(data["pattern"], _join(item_path, "pattern")),
                reason=_str(data["reason"], _join(item_path, "reason")),
                expires_on=None
                if expires is None
                else parse_date(expires, _join(item_path, "expires_on")),
            )
        )
    return tuple(exclusions)


def _parse_rules(value: Any, path: str) -> dict[str, Mapping[str, Any]]:
    data = _optional_mapping(value, path)
    return {name: _optional_mapping(section, _join(path, name)) for name, section in data.items()}


def _parse_scoring(value: Any, path: str) -> ScoringConfig:
    data = _optional_mapping(value, path)
    _check_keys(data, _SCORING_KEYS, path)
    kwargs: dict[str, Any] = {}
    for key in ("governance_weight", "cost_weight", "unknown_penalty"):
        if key in data:
            kwargs[key] = _number(data[key], _join(path, key))
    if "severity_weights" in data:
        weights_path = _join(path, "severity_weights")
        raw = _optional_mapping(data["severity_weights"], weights_path)
        valid = {severity.value for severity in Severity}
        weights = dict(DEFAULT_SEVERITY_WEIGHTS)
        for name, weight in raw.items():
            if name not in valid:
                raise PolicyValidationError(
                    _join(weights_path, name),
                    f"unknown severity{close_match_hint(name, valid)}; "
                    f"allowed: {', '.join(sorted(valid))}",
                )
            weights[Severity(name)] = _number(weight, _join(weights_path, name))
        kwargs["severity_weights"] = weights
    if "grade_boundaries" in data:
        bounds_path = _join(path, "grade_boundaries")
        raw = _optional_mapping(data["grade_boundaries"], bounds_path)
        kwargs["grade_boundaries"] = tuple(
            (grade, _number(minimum, _join(bounds_path, grade))) for grade, minimum in raw.items()
        )
    return _construct(ScoringConfig, path, **kwargs)


def _parse_actions(value: Any, path: str) -> ActionsConfig:
    data = _optional_mapping(value, path)
    _check_keys(data, _ACTIONS_KEYS, path)
    kwargs: dict[str, Any] = {}
    if "mode" in data:
        kwargs["mode"] = _str(data["mode"], _join(path, "mode"))
    for key in ("allow_delete", "allow_pause"):
        if key in data:
            kwargs[key] = _bool(data[key], _join(path, key))
    if "min_candidate_confidence" in data:
        kwargs["min_candidate_confidence"] = _int(
            data["min_candidate_confidence"], _join(path, "min_candidate_confidence")
        )
    return _construct(ActionsConfig, path, **kwargs)
