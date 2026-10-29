"""Validation of scanner-specific ``rules.<scanner>`` policy sections into typed configs.

A scanner declares ``config_model``: a dataclass whose fields use only ``bool``, ``int``,
``float``, ``str``, ``date``, ``tuple[<scalar>, ...]``, or ``Optional`` of those. Unknown
keys, missing required keys, and type mismatches are rejected with the dotted policy path.
"""

from __future__ import annotations

import dataclasses
import types
from collections.abc import Mapping
from datetime import date
from typing import Any, Union, get_args, get_origin, get_type_hints

from lakehouse_gc.core._validation import close_match_hint, is_number, parse_date, type_name
from lakehouse_gc.core.errors import InvalidScannerError, PolicyValidationError

_SCALARS: tuple[type, ...] = (bool, int, float, str, date)


def _unwrap_optional(hint: Any) -> tuple[bool, Any]:
    if get_origin(hint) in (Union, types.UnionType):
        args = get_args(hint)
        non_none = [arg for arg in args if arg is not type(None)]
        if len(args) == 2 and len(non_none) == 1:
            return True, non_none[0]
    return False, hint


def _is_supported(hint: Any) -> bool:
    _, inner = _unwrap_optional(hint)
    if inner in _SCALARS:
        return True
    if get_origin(inner) is tuple:
        args = get_args(inner)
        return len(args) == 2 and args[1] is Ellipsis and args[0] in _SCALARS
    return False


def validate_config_model(model: object, scanner_name: str) -> None:
    """Raise ``InvalidScannerError`` if ``model`` is not a supported config dataclass."""
    if model is None:
        return
    if not (isinstance(model, type) and dataclasses.is_dataclass(model)):
        raise InvalidScannerError(
            f"scanner '{scanner_name}': config_model must be a dataclass type or None"
        )
    hints = get_type_hints(model)
    for config_field in dataclasses.fields(model):
        if config_field.init and not _is_supported(hints[config_field.name]):
            raise InvalidScannerError(
                f"scanner '{scanner_name}': config field '{config_field.name}' has unsupported "
                f"type {hints[config_field.name]!r}"
            )


def build_scanner_config(model: type | None, data: Mapping[str, Any] | None, path: str) -> Any:
    """Build a config instance from a policy section, or ``None`` if the scanner has none."""
    data = data or {}
    if model is None:
        if data:
            raise PolicyValidationError(path, "this scanner does not accept any rules")
        return None
    fields = {f.name: f for f in dataclasses.fields(model) if f.init}
    for key in sorted(set(data) - set(fields)):
        raise PolicyValidationError(
            f"{path}.{key}",
            f"unknown key{close_match_hint(key, fields)}; allowed keys: "
            f"{', '.join(sorted(fields)) or '(none)'}",
        )
    hints = get_type_hints(model)
    kwargs: dict[str, Any] = {}
    for name, config_field in fields.items():
        if name in data:
            kwargs[name] = _coerce(data[name], hints[name], f"{path}.{name}")
        elif (
            config_field.default is dataclasses.MISSING
            and config_field.default_factory is dataclasses.MISSING
        ):
            raise PolicyValidationError(f"{path}.{name}", "required key is missing")
    try:
        return model(**kwargs)
    except PolicyValidationError:
        raise
    except (ValueError, TypeError) as exc:
        raise PolicyValidationError(path, str(exc)) from None


def resolve_scanner_configs(
    rules: Mapping[str, Mapping[str, Any]], config_models: Mapping[str, type | None]
) -> dict[str, Any]:
    """Validate every ``rules`` section against the registered scanners' config models.

    A section naming an unregistered scanner is an error (catches typos). Returns a config
    for every registered scanner, using defaults when its section is absent.
    """
    for name in sorted(rules):
        if name not in config_models:
            registered = ", ".join(sorted(config_models)) or "(none)"
            raise PolicyValidationError(
                f"rules.{name}",
                f"no registered scanner named '{name}'{close_match_hint(name, config_models)}; "
                f"registered scanners: {registered}",
            )
    return {
        name: build_scanner_config(model, rules.get(name), f"rules.{name}")
        for name, model in sorted(config_models.items())
    }


def _coerce(value: Any, hint: Any, path: str) -> Any:
    optional, inner = _unwrap_optional(hint)
    if value is None:
        if optional:
            return None
        raise PolicyValidationError(path, "must not be null")
    if get_origin(inner) is tuple:
        if not isinstance(value, list):
            raise PolicyValidationError(path, f"expected a list, got {type_name(value)}")
        item_type = get_args(inner)[0]
        return tuple(
            _coerce_scalar(item, item_type, f"{path}[{index}]") for index, item in enumerate(value)
        )
    return _coerce_scalar(value, inner, path)


def _coerce_scalar(value: Any, hint: type, path: str) -> Any:
    if hint is date:
        return parse_date(value, path)
    if hint is bool:
        if isinstance(value, bool):
            return value
    elif hint is int:
        if isinstance(value, int) and not isinstance(value, bool):
            return value
    elif hint is float:
        if is_number(value):
            return float(value)
    elif hint is str and isinstance(value, str):
        return value
    raise PolicyValidationError(path, f"expected {hint.__name__}, got {type_name(value)}")
