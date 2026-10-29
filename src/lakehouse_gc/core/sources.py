"""Configurable, read-only data sources.

Scanners declare the tables or views they read (``SourceSpec``) and the logical columns they
need. The policy's ``sources:`` section can repoint them, for organizations that block the
``system`` catalog or publish curated views over system tables:

    sources:
      information_schema: per_catalog          # or a schema such as governance.uc_meta
      tables:                                  # repoint one view, optionally renaming columns
        table: governance.uc_meta.all_tables
        columns: {table_owner: owner}

A missing optional column makes only the checks that use it unknown. A missing required
column or an unreadable object makes the source unavailable. Neither is ever treated as a
pass.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

from lakehouse_gc.core._validation import close_match_hint, type_name
from lakehouse_gc.core.errors import LakehouseGCError, PolicyValidationError
from lakehouse_gc.core.sql import Row, SqlClient, quote_identifier, table_columns

PER_CATALOG = "per_catalog"


@dataclass(frozen=True)
class SourceColumn:
    name: str
    required: bool = False
    used_by: tuple[str, ...] = ()
    """Rule IDs (or features) that become unknown when this column is unavailable."""


@dataclass(frozen=True)
class SourceGroupSpec:
    """A schema holding several sources, e.g. ``information_schema``."""

    name: str
    default: str
    description: str
    allow_per_catalog: bool = False


@dataclass(frozen=True)
class SourceSpec:
    """A table or view a scanner reads. Either ``group`` and ``view`` (default object is
    ``<group base>.<view>``), or ``default`` (a full object name)."""

    name: str
    description: str
    columns: tuple[SourceColumn, ...]
    group: str | None = None
    view: str | None = None
    default: str | None = None
    essential: bool = False
    """The scanner cannot run without this source."""
    used_by: tuple[str, ...] = ()
    """Rule IDs (or features) that become unknown when the whole source is unavailable."""

    def column(self, name: str) -> SourceColumn:
        return next(c for c in self.columns if c.name == name)


@dataclass(frozen=True)
class ResolvedSource:
    spec: SourceSpec
    object_name: str | None
    per_catalog: bool
    columns: Mapping[str, str] = field(default_factory=dict)
    """Logical column -> actual column, from the policy."""
    origin: str = "default"
    """``default`` or ``policy``."""

    @property
    def name(self) -> str:
        return self.spec.name

    def object_for(self, catalog: str | None = None) -> str:
        if not self.per_catalog:
            assert self.object_name is not None
            return self.object_name
        if catalog is None:
            raise LakehouseGCError(f"source '{self.name}' is per catalog; a catalog is needed")
        return f"{catalog}.information_schema.{self.spec.view}"

    @property
    def display_name(self) -> str:
        return self.object_for("<catalog>") if self.per_catalog else self.object_for()

    def actual(self, column: str) -> str:
        return self.columns.get(column, column)


class SourceUnavailableError(LakehouseGCError):
    """A source could not be read, or lacks a required column."""

    def __init__(self, source: str, object_name: str, reason: str) -> None:
        self.source = source
        self.object_name = object_name
        self.reason = reason
        super().__init__(f"cannot read source '{source}' ({object_name}): {reason}")


@dataclass(frozen=True)
class SourceRows:
    rows: list[Row]
    object_name: str
    missing_columns: tuple[str, ...]
    """Optional logical columns not present in the object (returned as None)."""


def describe_error(exc: BaseException) -> str:
    """Short, single-line description of a probe failure."""
    message = " ".join(str(exc).split())
    if len(message) > 160:
        message = message[:157] + "..."
    return f"{type(exc).__name__}: {message}" if message else type(exc).__name__


class SourceReader:
    """Reads resolved sources through a SQL client, probing and caching their columns."""

    def __init__(self, sql: SqlClient) -> None:
        self.sql = sql
        self._columns: dict[str, frozenset[str] | SourceUnavailableError] = {}

    def columns(self, source: ResolvedSource, object_name: str) -> frozenset[str]:
        cached = self._columns.get(object_name)
        if cached is None:
            try:
                cached = frozenset(c.lower() for c in table_columns(self.sql, object_name))
            except Exception as exc:
                cached = SourceUnavailableError(source.name, object_name, describe_error(exc))
            self._columns[object_name] = cached
        if isinstance(cached, SourceUnavailableError):
            raise cached
        return cached

    def missing(self, source: ResolvedSource, object_name: str) -> tuple[list[str], list[str]]:
        """(missing required, missing optional) logical columns."""
        available = self.columns(source, object_name)
        required: list[str] = []
        optional: list[str] = []
        for column in source.spec.columns:
            if source.actual(column.name).lower() not in available:
                (required if column.required else optional).append(column.name)
        return required, optional

    def read(
        self,
        source: ResolvedSource,
        *,
        catalog: str | None = None,
        where: Mapping[str, str] | None = None,
        params: Mapping[str, Any] | None = None,
    ) -> SourceRows:
        object_name = source.object_for(catalog)
        missing_required, missing_optional = self.missing(source, object_name)
        if missing_required:
            raise SourceUnavailableError(
                source.name,
                object_name,
                "missing required column(s): "
                + ", ".join(source.actual(c) for c in missing_required),
            )
        items = [
            f"NULL AS {c.name}"
            if c.name in missing_optional
            else f"{quote_identifier(source.actual(c.name))} AS {c.name}"
            for c in source.spec.columns
        ]
        statement = f"SELECT {', '.join(items)} FROM {quote_identifier(*object_name.split('.'))}"
        if where:
            statement += " WHERE " + " AND ".join(
                f"{quote_identifier(source.actual(column))} = :{param}"
                for column, param in where.items()
            )
        try:
            rows = self.sql.query(statement, params)
        except Exception as exc:
            raise SourceUnavailableError(source.name, object_name, describe_error(exc)) from exc
        return SourceRows(rows, object_name, tuple(missing_optional))


# --- resolution from policy -------------------------------------------------------------


def _check_name(value: object, parts: int, path: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise PolicyValidationError(path, f"expected a name, got {type_name(value)}")
    pieces = value.split(".")
    if len(pieces) != parts or any(not p.strip() or "`" in p for p in pieces):
        example = "catalog.schema" if parts == 2 else "catalog.schema.table"
        raise PolicyValidationError(
            path, f"expected a {parts}-part name like '{example}', got '{value}'"
        )
    return value


def resolve_sources(
    raw: Mapping[str, Any],
    scanners: Iterable[tuple[str, tuple[SourceSpec, ...], tuple[SourceGroupSpec, ...]]],
) -> dict[str, dict[str, ResolvedSource]]:
    """Validate the policy ``sources`` section and resolve every declared source.

    Returns ``{scanner_name: {source_name: ResolvedSource}}``.
    """
    scanner_list = list(scanners)
    specs: dict[str, SourceSpec] = {}
    groups: dict[str, SourceGroupSpec] = {}
    for _, scanner_specs, scanner_groups in scanner_list:
        specs.update({s.name: s for s in scanner_specs})
        groups.update({g.name: g for g in scanner_groups})

    known = sorted({*specs, *groups})
    overrides: dict[str, tuple[str | None, dict[str, str]]] = {}
    group_values: dict[str, str] = {}
    for key, value in raw.items():
        path = f"sources.{key}"
        if key in groups:
            group = groups[key]
            if value == PER_CATALOG and group.allow_per_catalog:
                group_values[key] = PER_CATALOG
            else:
                group_values[key] = _check_name(value, 2, path)
        elif key in specs:
            overrides[key] = _parse_override(specs[key], value, path)
        else:
            raise PolicyValidationError(
                path,
                f"unknown source{close_match_hint(key, known)}; known sources: "
                f"{', '.join(known) or '(none)'}",
            )

    resolved: dict[str, dict[str, ResolvedSource]] = {}
    for scanner_name, scanner_specs, _ in scanner_list:
        plan: dict[str, ResolvedSource] = {}
        for spec in scanner_specs:
            table, columns = overrides.get(spec.name, (None, {}))
            origin = "policy" if spec.name in overrides else "default"
            if table is not None:
                plan[spec.name] = ResolvedSource(spec, table, False, columns, "policy")
            elif spec.group is not None:
                base = group_values.get(spec.group, groups[spec.group].default)
                if spec.group in group_values:
                    origin = "policy"
                if base == PER_CATALOG:
                    plan[spec.name] = ResolvedSource(spec, None, True, columns, origin)
                else:
                    plan[spec.name] = ResolvedSource(
                        spec, f"{base}.{spec.view}", False, columns, origin
                    )
            else:
                plan[spec.name] = ResolvedSource(spec, spec.default, False, columns, origin)
        resolved[scanner_name] = plan
    return resolved


def _parse_override(spec: SourceSpec, value: Any, path: str) -> tuple[str | None, dict[str, str]]:
    if isinstance(value, str):
        return _check_name(value, 3, path), {}
    if not isinstance(value, Mapping):
        raise PolicyValidationError(
            path, f"expected a table name or a mapping, got {type_name(value)}"
        )
    for key in value:
        if key not in ("table", "columns"):
            raise PolicyValidationError(
                f"{path}.{key}",
                f"unknown key{close_match_hint(str(key), ['table', 'columns'])}; "
                "allowed keys: columns, table",
            )
    table = value.get("table")
    if table is not None:
        table = _check_name(table, 3, f"{path}.table")
    columns_raw = value.get("columns") or {}
    if not isinstance(columns_raw, Mapping):
        raise PolicyValidationError(
            f"{path}.columns", f"expected a mapping, got {type_name(columns_raw)}"
        )
    names = [c.name for c in spec.columns]
    columns: dict[str, str] = {}
    for logical, actual in columns_raw.items():
        if logical not in names:
            raise PolicyValidationError(
                f"{path}.columns.{logical}",
                f"unknown column{close_match_hint(str(logical), names)}; "
                f"columns of '{spec.name}': {', '.join(names)}",
            )
        if not isinstance(actual, str) or not actual.strip() or "`" in actual:
            raise PolicyValidationError(f"{path}.columns.{logical}", "expected a column name")
        columns[logical] = actual
    return table, columns


def frozen(plan: Mapping[str, ResolvedSource]) -> Mapping[str, ResolvedSource]:
    return MappingProxyType(dict(plan))
