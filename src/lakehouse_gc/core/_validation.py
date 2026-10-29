"""Small shared validation helpers (internal)."""

from __future__ import annotations

import difflib
import math
from collections.abc import Iterable
from datetime import date, datetime
from typing import Any

from lakehouse_gc.core.errors import ModelValidationError, PolicyValidationError


def require_text(value: object, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ModelValidationError(f"{field_name} must be a non-empty string")


def check_confidence(value: object, field_name: str) -> None:
    if value is None:
        return
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 100:
        raise ModelValidationError(f"{field_name} must be an integer from 0 to 100, or None")


def is_number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def type_name(value: object) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, dict):
        return "mapping"
    if isinstance(value, list):
        return "list"
    return type(value).__name__


def close_match_hint(name: str, candidates: Iterable[str]) -> str:
    matches = difflib.get_close_matches(name, list(candidates), n=1)
    return f" (did you mean '{matches[0]}'?)" if matches else ""


def parse_date(value: Any, path: str) -> date:
    if isinstance(value, datetime):
        raise PolicyValidationError(path, "expected a date (YYYY-MM-DD), got a timestamp")
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError:
            raise PolicyValidationError(
                path, f"invalid date '{value}'; expected YYYY-MM-DD"
            ) from None
    raise PolicyValidationError(path, f"expected a date (YYYY-MM-DD), got {type_name(value)}")
