"""Function-based scanners: build a scanner from a list of resources and plain check functions.

A check receives one resource (the mapping you supplied) and, optionally, the scanner config
from ``rules.<scanner>`` in the policy. It returns:

- ``True`` / ``ok(reason)``      the resource passes
- ``False`` / ``fail(reason)``   the resource fails
- ``None`` / ``unknown(reason)`` the data needed is unavailable (never treated as a pass)

``ok``/``fail`` accept ``inferred=True`` when the verdict rests on indirect evidence, which
lowers confidence. The resulting class is a regular ``Scanner``; the framework still applies
exclusions, contract checks, the candidate-confidence gate, and scoring.
"""

from __future__ import annotations

import dataclasses
import inspect
import re
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, ClassVar, TypeVar, overload

from lakehouse_gc.core.errors import InvalidScannerError, ScannerContractError
from lakehouse_gc.core.evidence import Evidence, EvidenceStatus
from lakehouse_gc.core.models import (
    Dimension,
    Finding,
    Outcome,
    Recommendation,
    Resource,
    ResourceRef,
    RuleEvaluation,
    ScoredEvaluation,
    Severity,
)
from lakehouse_gc.core.scanner import RuleSpec, Scanner, ScannerContext

DEFAULT_SCOPE_ID = "default"
_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")
_E = TypeVar("_E", Severity, Dimension, Recommendation)
_F = TypeVar("_F", bound=Callable[..., Any])


@dataclass(frozen=True)
class CheckResult:
    outcome: Outcome
    reason: str | None = None
    inferred: bool = False


def ok(reason: str | None = None, *, inferred: bool = False) -> CheckResult:
    return CheckResult(Outcome.PASS, reason, inferred)


def fail(reason: str | None = None, *, inferred: bool = False) -> CheckResult:
    return CheckResult(Outcome.FAIL, reason, inferred)


def unknown(reason: str | None = None) -> CheckResult:
    return CheckResult(Outcome.INDETERMINATE, reason)


@dataclass(frozen=True)
class _CheckSpec:
    fn: Callable[..., Any]
    rule: str | None
    title: str | None
    description: str | None
    severity: Severity
    dimensions: frozenset[Dimension]
    on_fail: Recommendation


_CHECK_ATTR = "__lakehouse_gc_check__"


def _as_enum(enum_type: type[_E], value: object, field_name: str) -> _E:
    if isinstance(value, enum_type):
        return value
    try:
        return enum_type(value)
    except ValueError:
        allowed = ", ".join(member.value for member in enum_type)
        raise InvalidScannerError(f"invalid {field_name} {value!r}; allowed: {allowed}") from None


@overload
def check(fn: _F, /) -> _F: ...
@overload
def check(
    *,
    rule: str | None = None,
    title: str | None = None,
    description: str | None = None,
    severity: Severity | str = Severity.MEDIUM,
    dimension: Dimension | str | Iterable[Dimension | str] = Dimension.GOVERNANCE,
    on_fail: Recommendation | str = Recommendation.REVIEW,
) -> Callable[[_F], _F]: ...
def check(
    fn: Callable[..., Any] | None = None,
    /,
    *,
    rule: str | None = None,
    title: str | None = None,
    description: str | None = None,
    severity: Severity | str = Severity.MEDIUM,
    dimension: Dimension | str | Iterable[Dimension | str] = Dimension.GOVERNANCE,
    on_fail: Recommendation | str = Recommendation.REVIEW,
) -> Any:
    """Annotate a check function. Usable bare (``@check``) or with options.

    ``rule`` defaults to the function name, ``title`` to a readable form of it, and
    ``description`` (shown by ``describe_rules``) to the docstring's first paragraph.
    ``on_fail`` is the recommendation for a failing resource (default ``review``).
    """
    dims = [dimension] if isinstance(dimension, (str, Dimension)) else list(dimension)
    recommendation = _as_enum(Recommendation, on_fail, "on_fail")
    if recommendation in (Recommendation.EXCLUDED, Recommendation.KEEP):
        raise InvalidScannerError(f"on_fail cannot be '{recommendation.value}'")
    spec_options = {
        "rule": rule,
        "title": title,
        "description": description,
        "severity": _as_enum(Severity, severity, "severity"),
        "dimensions": frozenset(_as_enum(Dimension, d, "dimension") for d in dims),
        "on_fail": recommendation,
    }

    def decorate(func: _F) -> _F:
        setattr(func, _CHECK_ATTR, _CheckSpec(fn=func, **spec_options))  # type: ignore[arg-type]
        return func

    return decorate(fn) if fn is not None else decorate


def _spec_for(fn: Callable[..., Any]) -> _CheckSpec:
    if not callable(fn):
        raise InvalidScannerError(f"checks must be functions, got {fn!r}")
    spec = getattr(fn, _CHECK_ATTR, None)
    if isinstance(spec, _CheckSpec):
        return spec
    return _CheckSpec(
        fn=fn,
        rule=None,
        title=None,
        description=None,
        severity=Severity.MEDIUM,
        dimensions=frozenset({Dimension.GOVERNANCE}),
        on_fail=Recommendation.REVIEW,
    )


def _call(fn: Callable[..., Any], *args: Any) -> Any:
    """Call ``fn`` with as many leading positional ``args`` as it accepts."""
    try:
        parameters = inspect.signature(fn).parameters.values()
    except (TypeError, ValueError):
        return fn(*args)
    if any(p.kind is inspect.Parameter.VAR_POSITIONAL for p in parameters):
        return fn(*args)
    positional = [
        p
        for p in parameters
        if p.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    ]
    return fn(*args[: len(positional)])


def _config_model(scanner_name: str, defaults: Mapping[str, Any] | None) -> type | None:
    """Build a frozen config dataclass whose field types come from the default values."""
    if defaults is None:
        return None
    fields: list[tuple[str, Any, Any]] = []
    for key, value in defaults.items():
        if not isinstance(key, str) or not key.isidentifier():
            raise InvalidScannerError(f"scanner '{scanner_name}': invalid config key {key!r}")
        field_type: Any
        if isinstance(value, (bool, int, float, str)):
            field_type = type(value)
        elif isinstance(value, (list, tuple)) and all(isinstance(v, str) for v in value):
            field_type, value = tuple[str, ...], tuple(value)
        else:
            raise InvalidScannerError(
                f"scanner '{scanner_name}': config '{key}' must default to a bool, int, float, "
                "str, or list of str"
            )
        fields.append((key, field_type, dataclasses.field(default=value)))
    class_name = "".join(part.title() for part in scanner_name.split("_")) + "Config"
    return dataclasses.make_dataclass(class_name, fields, frozen=True)


def _to_resource(item: object, default_type: str) -> Resource:
    if not isinstance(item, Mapping):
        raise ScannerContractError(
            f"resources must be mappings (e.g. dicts), got {type(item).__name__}"
        )
    name = item.get("name")
    if not isinstance(name, str) or not name.strip():
        raise ScannerContractError(f"each resource needs a non-empty 'name': {dict(item)!r}")
    owner = item.get("owner")
    return Resource(
        ref=ResourceRef(
            scope_id=str(item.get("scope", DEFAULT_SCOPE_ID)),
            resource_type=str(item.get("type", default_type)),
            resource_id=str(item.get("id", name)),
        ),
        name=name,
        owner=owner if isinstance(owner, str) and owner.strip() else None,
        metadata=item,
    )


def _normalize(value: object, rule_id: str) -> CheckResult:
    if isinstance(value, CheckResult):
        return value
    if value is True:
        return ok()
    if value is False:
        return fail()
    if value is None:
        return unknown()
    raise ScannerContractError(
        f"check '{rule_id}' returned {type(value).__name__}; return True, False, None, "
        "ok(), fail(), or unknown()"
    )


_DEFAULT_REASON = {
    Outcome.PASS: "Check '{title}' passed.",
    Outcome.FAIL: "Check '{title}' failed.",
    Outcome.INDETERMINATE: "Check '{title}' could not be evaluated; required data is unavailable.",
}


class FunctionScanner(Scanner):
    """Base for scanners created by ``define_scanner``. Not registered by itself."""

    _resources: ClassVar[tuple[Any]]  # 1-tuple so a function is not bound as a method
    _checks: ClassVar[Mapping[str, _CheckSpec]]
    _default_type: ClassVar[str]

    def _config(self, ctx: ScannerContext) -> Mapping[str, Any]:
        if ctx.config is None:
            return MappingProxyType({})
        return MappingProxyType(dataclasses.asdict(ctx.config))

    def discover(self, ctx: ScannerContext) -> Sequence[Resource]:
        source = self._resources[0]
        items = _call(source, self._config(ctx)) if callable(source) else source
        if items is None or isinstance(items, (str, bytes, Mapping)):
            raise ScannerContractError("resources must be a list of mappings")
        return [_to_resource(item, self._default_type) for item in items]

    def analyze(
        self, ctx: ScannerContext, resources: Sequence[Resource]
    ) -> Sequence[RuleEvaluation]:
        config = self._config(ctx)
        evaluations = []
        for resource in resources:
            for rule_id, spec in self._checks.items():
                result = _normalize(_call(spec.fn, resource.metadata, config), rule_id)
                title = self.rule_specs()[rule_id].title
                reason = result.reason or _DEFAULT_REASON[result.outcome].format(title=title)
                if result.outcome is Outcome.INDETERMINATE:
                    status = EvidenceStatus.UNAVAILABLE
                elif result.inferred:
                    status = EvidenceStatus.INFERRED
                else:
                    status = EvidenceStatus.OBSERVED
                evidence = Evidence(rule_id, status, source=f"{self.name} check", note=reason)
                evaluations.append(
                    RuleEvaluation(resource.ref, rule_id, result.outcome, (evidence,), (reason,))
                )
        return evaluations

    def recommend(
        self, ctx: ScannerContext, scored: Sequence[ScoredEvaluation]
    ) -> Sequence[Finding]:
        findings = []
        for item in scored:
            evaluation = item.evaluation
            spec = self._checks[evaluation.rule_id]
            if evaluation.outcome is Outcome.PASS:
                recommendation = Recommendation.KEEP
            elif evaluation.outcome is Outcome.FAIL:
                recommendation = spec.on_fail
            else:
                recommendation = Recommendation.REVIEW
            findings.append(
                Finding(
                    resource=evaluation.resource,
                    rule_id=evaluation.rule_id,
                    severity=item.severity,
                    recommendation=recommendation,
                    reasons=evaluation.reasons,
                    evidence=evaluation.evidence,
                    confidence=item.confidence,
                    impacts=spec.dimensions,
                )
            )
        return findings


def define_scanner(
    name: str,
    *,
    resources: Iterable[Mapping[str, Any]] | Callable[..., Iterable[Mapping[str, Any]]],
    checks: Sequence[Callable[..., Any]],
    config: Mapping[str, Any] | None = None,
    resource_type: str | None = None,
) -> type[Scanner]:
    """Create a scanner from resources and check functions; pass it to ``run(scanners=...)``.

    ``resources`` is a list of mappings, or a function returning one (it may take the config
    mapping as its argument). Each mapping needs ``name``; optional keys are ``id`` (default:
    name), ``type`` (default: ``resource_type`` or the scanner name), ``scope`` (default
    ``"default"``), and ``owner``. ``config`` holds defaults for ``rules.<name>`` in the
    policy; the policy may override them and types are validated.
    """
    if not isinstance(name, str) or not _NAME_PATTERN.fullmatch(name):
        raise InvalidScannerError(f"scanner name must match {_NAME_PATTERN.pattern}, got {name!r}")
    if not checks:
        raise InvalidScannerError(f"scanner '{name}' needs at least one check")
    by_rule: dict[str, _CheckSpec] = {}
    rules = []
    for fn in checks:
        spec = _spec_for(fn)
        rule_name = spec.rule or getattr(fn, "__name__", "")
        if not rule_name or rule_name == "<lambda>":
            raise InvalidScannerError(f"scanner '{name}': lambda checks need @check(rule=...)")
        rule_id = f"{name}.{rule_name}"
        title = spec.title or rule_name.replace("_", " ").capitalize()
        if rule_id in by_rule:
            raise InvalidScannerError(f"scanner '{name}': duplicate check '{rule_name}'")
        by_rule[rule_id] = spec
        doc = (inspect.getdoc(fn) or "").split("\n\n", 1)[0].replace("\n", " ")
        rules.append(
            RuleSpec(
                rule_id,
                title,
                spec.severity,
                spec.dimensions,
                description=spec.description or doc,
                on_fail=spec.on_fail,
            )
        )
    class_name = "".join(part.title() for part in name.split("_")) + "Scanner"
    namespace = {
        "name": name,
        "rules": tuple(rules),
        "config_model": _config_model(name, config),
        "_resources": (resources,),
        "_checks": MappingProxyType(by_rule),
        "_default_type": resource_type or name,
        "resource_types": (resource_type or name,),
        "__module__": __name__,
    }
    return type(class_name, (FunctionScanner,), namespace)
