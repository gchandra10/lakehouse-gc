"""Unity Catalog scanner: catalogs, schemas, and tables selected by the policy ``scope``.

Read-only. It runs SELECT queries on Unity Catalog ``information_schema`` views and, for
stale-table candidates, ``DESCRIBE DETAIL``, through the run's SQL client (the notebook's
Spark session by default). It never modifies anything.

Where it reads from is configurable under ``sources:`` in the policy. The default is
``system.information_schema``. ``per_catalog`` reads each catalog's own
``information_schema`` (for organizations that block the ``system`` catalog), and single
views can be repointed or have their columns renamed.

Evidence limits, reported explicitly instead of hidden:
- Only objects the running principal can see are visible, so "empty" is inferred.
- ``last_altered`` tracks metadata changes and ``lastModified`` from ``DESCRIBE DETAIL``
  tracks data writes. Neither is a read or access time, so staleness is inferred.
  Confidence stays below the candidate gate and results in REVIEW.
- Unreadable sources or missing columns make only the affected checks unknown, and they
  are listed in ``RunResult.limitations``.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from fnmatch import fnmatchcase
from typing import Any

from lakehouse_gc.core.evidence import Evidence, EvidenceStatus, derive_confidence
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
from lakehouse_gc.core.sources import (
    ResolvedSource,
    SourceColumn,
    SourceGroupSpec,
    SourceReader,
    SourceRows,
    SourceSpec,
    SourceUnavailableError,
    describe_error,
)
from lakehouse_gc.core.sql import Row, SqlClient, quote_identifier, utc_timestamp

DETAIL_SOURCE = "DESCRIBE DETAIL"
NAME_SOURCE = "object name"
BYTES_PER_GB = 1024**3
DATA_TABLE_TYPES = frozenset({"MANAGED", "EXTERNAL"})
SKIPPED_SCHEMAS = frozenset({"information_schema"})
METASTORE_SQL = "SELECT current_metastore() AS metastore_id"
SHOW_CATALOGS_SQL = "SHOW CATALOGS"

_OWNER, _COMMENT, _TAGS = "uc.owner_missing", "uc.comment_missing", "uc.required_tags_missing"
_EMPTY, _STALE = "uc.empty_container", "uc.stale_table"

INFORMATION_SCHEMA = SourceGroupSpec(
    "information_schema",
    default="system.information_schema",
    description=(
        "Unity Catalog metadata views. Use per_catalog to read each catalog's own "
        "information_schema when the system catalog is not available."
    ),
    allow_per_catalog=True,
)


def _col(name: str, *used_by: str, required: bool = False) -> SourceColumn:
    return SourceColumn(name, required=required, used_by=used_by)


CATALOGS_SOURCE = SourceSpec(
    "catalogs",
    "Catalog list with owner and comment",
    (
        _col("catalog_name", required=True),
        _col("catalog_owner", _OWNER),
        _col("comment", _COMMENT),
        _col("created"),
        _col("last_altered"),
    ),
    group="information_schema",
    view="catalogs",
    essential=True,
)
SCHEMATA_SOURCE = SourceSpec(
    "schemata",
    "Schemas with owner and comment",
    (
        _col("catalog_name", required=True),
        _col("schema_name", required=True),
        _col("schema_owner", _OWNER),
        _col("comment", _COMMENT),
        _col("created"),
        _col("last_altered"),
    ),
    group="information_schema",
    view="schemata",
    essential=True,
)
TABLES_SOURCE = SourceSpec(
    "tables",
    "Tables and views with owner, comment, type, and change times",
    (
        _col("table_catalog", required=True),
        _col("table_schema", required=True),
        _col("table_name", required=True),
        _col("table_type", _STALE),
        _col("table_owner", _OWNER),
        _col("comment", _COMMENT),
        _col("created"),
        _col("last_altered", _STALE),
        _col("data_source_format"),
    ),
    group="information_schema",
    view="tables",
    essential=True,
)
TABLE_TAGS_SOURCE = SourceSpec(
    "table_tags",
    "Tags on tables",
    (
        _col("catalog_name", required=True),
        _col("schema_name", required=True),
        _col("table_name", required=True),
        _col("tag_name", required=True),
        _col("tag_value"),
    ),
    group="information_schema",
    view="table_tags",
    used_by=(_TAGS,),
)
VOLUMES_SOURCE = SourceSpec(
    "volumes",
    "Volumes, counted when deciding whether a schema is empty",
    (
        _col("volume_catalog", required=True),
        _col("volume_schema", required=True),
        _col("volume_name", required=True),
    ),
    group="information_schema",
    view="volumes",
    used_by=(_EMPTY,),
)
UC_SOURCES = (CATALOGS_SOURCE, SCHEMATA_SOURCE, TABLES_SOURCE, TABLE_TAGS_SOURCE, VOLUMES_SOURCE)

DEFAULT_TEMP_PATTERNS = (
    "tmp_*",
    "temp_*",
    "test_*",
    "scratch_*",
    "*_tmp",
    "*_temp",
    "*_bak",
    "*_backup",
    "*_old",
)


@dataclass(frozen=True)
class UCConfig:
    stale_table_days: int = 180
    min_table_size_gb: float = 10.0
    required_tags: tuple[str, ...] = ()
    require_comment: bool = True
    temp_name_patterns: tuple[str, ...] = DEFAULT_TEMP_PATTERNS
    check_empty_containers: bool = True
    max_detail_queries: int = 200

    def __post_init__(self) -> None:
        if self.stale_table_days < 1:
            raise ValueError("stale_table_days must be at least 1")
        if self.min_table_size_gb < 0:
            raise ValueError("min_table_size_gb must not be negative")
        if self.max_detail_queries < 0:
            raise ValueError("max_detail_queries must not be negative")


_GOV = frozenset({Dimension.GOVERNANCE})
OWNER_RULE = RuleSpec(
    "uc.owner_missing",
    "Object has no owner",
    Severity.MEDIUM,
    _GOV,
    description=(
        "Every catalog, schema, and table should have an owner who is accountable for it. "
        "Fails when Unity Catalog reports no owner."
    ),
    on_fail=Recommendation.TAG_OWNER,
)
COMMENT_RULE = RuleSpec(
    "uc.comment_missing",
    "Object has no description",
    Severity.LOW,
    _GOV,
    description=(
        "Catalogs, schemas, and tables should have a comment explaining what they hold. "
        "Fails when the comment is empty."
    ),
    settings=("require_comment",),
    on_fail=Recommendation.REVIEW,
)
TAGS_RULE = RuleSpec(
    "uc.required_tags_missing",
    "Table is missing required tags",
    Severity.MEDIUM,
    _GOV,
    description=(
        "Tables must carry every tag in required_tags (for example cost_center) for cost "
        "allocation and governance. Unknown if tags cannot be read. Skipped when "
        "required_tags is empty."
    ),
    settings=("required_tags",),
    on_fail=Recommendation.TAG_OWNER,
)
TEMP_RULE = RuleSpec(
    "uc.temporary_name",
    "Name looks temporary",
    Severity.LOW,
    _GOV,
    description=(
        "Schema or table names such as tmp_*, *_bak, or *_old suggest leftovers. This is a "
        "naming heuristic only (inferred evidence); confirm before acting."
    ),
    settings=("temp_name_patterns",),
    on_fail=Recommendation.REVIEW,
)
EMPTY_RULE = RuleSpec(
    "uc.empty_container",
    "Catalog or schema has no visible objects",
    Severity.LOW,
    _GOV,
    description=(
        "Catalogs or schemas with no tables or volumes may be unused. Only objects visible "
        "to the user running the scan are counted, so this is inferred, not proof."
    ),
    settings=("check_empty_containers",),
    on_fail=Recommendation.REVIEW,
)
STALE_RULE = RuleSpec(
    "uc.stale_table",
    "Large table not modified within the stale window",
    Severity.HIGH,
    frozenset({Dimension.COST}),
    description=(
        "Managed or external tables with no data written for stale_table_days and at least "
        "min_table_size_gb in size may be archive candidates. Write time is not read time, "
        "so confidence stays below the candidate threshold and the result is review. "
        "Views are not checked."
    ),
    settings=("stale_table_days", "min_table_size_gb", "max_detail_queries"),
    on_fail=Recommendation.ARCHIVE_CANDIDATE,
)

_ACTIVITY_KEYS = frozenset({"last_altered", "last_modified"})

_O, _I, _U = EvidenceStatus.OBSERVED, EvidenceStatus.INFERRED, EvidenceStatus.UNAVAILABLE


class UnityCatalogScanner(Scanner):
    name = "uc"
    rules = (OWNER_RULE, COMMENT_RULE, TAGS_RULE, TEMP_RULE, EMPTY_RULE, STALE_RULE)
    config_model = UCConfig
    resource_types = ("catalog", "schema", "table")
    sources = UC_SOURCES
    source_groups = (INFORMATION_SCHEMA,)
    access_notes = (
        "stale-table checks also run DESCRIBE DETAIL on candidate tables; failures are "
        "listed as limitations after a run.",
    )

    # --- discover ------------------------------------------------------------------------

    def discover(self, ctx: ScannerContext) -> Sequence[Resource]:
        reader = SourceReader(ctx.get_sql())
        plan = _plan(ctx)
        scope = ctx.policy.scope
        metastore = _metastore_id(reader.sql, ctx)
        resources: list[Resource] = []
        for catalog_row in _list_catalogs(ctx, reader, plan["catalogs"]):
            if scope.includes_catalog(str(catalog_row["catalog_name"])):
                resources.extend(self._discover_catalog(ctx, reader, plan, metastore, catalog_row))
        return resources

    def _discover_catalog(
        self,
        ctx: ScannerContext,
        reader: SourceReader,
        plan: Mapping[str, ResolvedSource],
        metastore: str,
        catalog_row: Row,
    ) -> list[Resource]:
        scope = ctx.policy.scope
        catalog = str(catalog_row["catalog_name"])
        params = {"catalog": catalog}
        schemata_read = reader.read(
            plan["schemata"], catalog=catalog, where={"catalog_name": "catalog"}, params=params
        )
        tables_read = reader.read(
            plan["tables"], catalog=catalog, where={"table_catalog": "catalog"}, params=params
        )
        _note_missing_columns(ctx, plan["schemata"], schemata_read)
        _note_missing_columns(ctx, plan["tables"], tables_read)
        schemata = [r for r in schemata_read.rows if r["schema_name"] not in SKIPPED_SCHEMAS]
        tables = [r for r in tables_read.rows if r["table_schema"] not in SKIPPED_SCHEMAS]
        tags = _read_optional(
            ctx,
            reader,
            plan["table_tags"],
            catalog,
            {"catalog_name": "catalog"},
            f"Table tags in catalog '{catalog}'",
        )
        volumes = _read_optional(
            ctx,
            reader,
            plan["volumes"],
            catalog,
            {"volume_catalog": "catalog"},
            f"Volumes in catalog '{catalog}'",
        )

        table_counts: dict[str, int] = defaultdict(int)
        for row in tables:
            table_counts[row["table_schema"]] += 1
        volume_counts: dict[str, int] | None = None
        if volumes is not None:
            volume_counts = defaultdict(int)
            for row in volumes.rows:
                volume_counts[row["volume_schema"]] += 1
        tags_by_table: dict[tuple[str, str], dict[str, str]] | None = None
        if tags is not None:
            tags_by_table = defaultdict(dict)
            for row in tags.rows:
                key = (row["schema_name"], row["table_name"])
                tags_by_table[key][str(row["tag_name"])] = str(row["tag_value"] or "")

        def object_count(schema: str) -> int | None:
            if table_counts.get(schema, 0) > 0:
                return table_counts[schema]
            if volume_counts is None:
                return None
            return volume_counts.get(schema, 0)

        schema_counts = {row["schema_name"]: object_count(row["schema_name"]) for row in schemata}
        known = [count for count in schema_counts.values() if count is not None]
        catalog_count: int | None = sum(known)
        if not catalog_count and len(known) < len(schema_counts):
            catalog_count = None
        volume_source = volumes.object_name if volumes else plan["volumes"].display_name

        resources = [
            _resource(
                metastore,
                "catalog",
                catalog,
                catalog_row,
                source=str(catalog_row.get("__source__") or plan["catalogs"].display_name),
                missing=frozenset(catalog_row.get("__missing__") or ()),
                owner_key="catalog_owner",
                extra=(_count_evidence(catalog_count, volume_source),),
                metadata={"catalog": catalog},
            )
        ]
        for row in sorted(schemata, key=lambda r: str(r["schema_name"])):
            schema = str(row["schema_name"])
            if not scope.includes_schema(catalog, schema):
                continue
            resources.append(
                _resource(
                    metastore,
                    "schema",
                    f"{catalog}.{schema}",
                    row,
                    source=schemata_read.object_name,
                    missing=frozenset(schemata_read.missing_columns),
                    owner_key="schema_owner",
                    extra=(_count_evidence(schema_counts[schema], volume_source),),
                    metadata={"catalog": catalog, "schema": schema},
                )
            )
        for row in sorted(tables, key=lambda r: (str(r["table_schema"]), str(r["table_name"]))):
            schema, table = str(row["table_schema"]), str(row["table_name"])
            if not scope.includes_table(catalog, schema, table):
                continue
            if tags_by_table is None or tags is None:
                tag_evidence = Evidence(
                    "tags",
                    _U,
                    plan["table_tags"].display_name,
                    note="Table tags could not be read.",
                )
            else:
                tag_evidence = Evidence(
                    "tags", _O, tags.object_name, value=dict(tags_by_table.get((schema, table), {}))
                )
            resources.append(
                _resource(
                    metastore,
                    "table",
                    f"{catalog}.{schema}.{table}",
                    row,
                    source=tables_read.object_name,
                    missing=frozenset(tables_read.missing_columns),
                    owner_key="table_owner",
                    extra=(tag_evidence,),
                    metadata={
                        "catalog": catalog,
                        "schema": schema,
                        "table": table,
                        "table_type": str(row.get("table_type") or ""),
                        "table_type_known": "table_type" not in tables_read.missing_columns,
                        "data_source_format": str(row.get("data_source_format") or ""),
                    },
                )
            )
        return resources

    # --- analyze -------------------------------------------------------------------------

    def analyze(
        self, ctx: ScannerContext, resources: Sequence[Resource]
    ) -> Sequence[RuleEvaluation]:
        config: UCConfig = ctx.config
        now = ctx.now()
        detail_budget = [config.max_detail_queries]
        detail_stats = {"skipped": 0, "failed": 0}
        evaluations: list[RuleEvaluation] = []
        for resource in resources:
            kind = resource.ref.resource_type
            evaluations.append(_evaluate_owner(resource))
            if config.require_comment:
                evaluations.append(_evaluate_comment(resource))
            if kind in ("schema", "table") and config.temp_name_patterns:
                evaluations.append(_evaluate_temp_name(resource, config.temp_name_patterns))
            if kind in ("catalog", "schema") and config.check_empty_containers:
                evaluations.append(_evaluate_empty(resource))
            if kind == "table":
                if config.required_tags:
                    evaluations.append(_evaluate_tags(resource, config.required_tags))
                type_known = resource.metadata.get("table_type_known", True)
                if not type_known or resource.metadata.get("table_type") in DATA_TABLE_TYPES:
                    evaluations.append(
                        _evaluate_stale(ctx, resource, config, now, detail_budget, detail_stats)
                    )
        if detail_stats["skipped"]:
            ctx.limit(
                f"{detail_stats['skipped']} stale-table candidate(s) were not checked because "
                f"max_detail_queries ({config.max_detail_queries}) was reached: "
                f"{_STALE} is unknown for them."
            )
        if detail_stats["failed"]:
            ctx.limit(
                f"DESCRIBE DETAIL failed for {detail_stats['failed']} table(s) (for example, "
                f"non-Delta tables or missing privileges): {_STALE} is unknown for them."
            )
        return evaluations

    def score(
        self, ctx: ScannerContext, evaluations: Sequence[RuleEvaluation]
    ) -> Sequence[ScoredEvaluation]:
        scored = list(super().score(ctx, evaluations))
        for index, item in enumerate(scored):
            if item.evaluation.rule_id == STALE_RULE.rule_id:
                # Staleness confidence rests on the activity signal alone: the data-write
                # time when known (inferred: a write is not a read), else the metadata time.
                # Size doesn't make a modification time any closer to an access time.
                evidence = item.evaluation.evidence
                activity = [e for e in evidence if e.key == "last_modified"] or [
                    e for e in evidence if e.key in _ACTIVITY_KEYS
                ]
                scored[index] = replace(item, confidence=derive_confidence(activity))
        return scored

    def recommend(
        self, ctx: ScannerContext, scored: Sequence[ScoredEvaluation]
    ) -> Sequence[Finding]:
        findings = []
        for item in scored:
            evaluation = item.evaluation
            if evaluation.outcome is Outcome.PASS:
                recommendation = Recommendation.KEEP
            elif evaluation.outcome is Outcome.FAIL:
                recommendation = self.rule_specs()[evaluation.rule_id].on_fail or (
                    Recommendation.REVIEW
                )
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
                    impacts=frozenset(d for d, w in item.weights.items() if w > 0),
                )
            )
        return findings


# --- helpers -----------------------------------------------------------------------------


def _plan(ctx: ScannerContext) -> Mapping[str, ResolvedSource]:
    """This run's resolved sources, or the defaults when the scanner runs standalone."""
    if ctx.sources:
        return ctx.sources
    from lakehouse_gc.core.sources import resolve_sources

    return resolve_sources({}, [("uc", UC_SOURCES, (INFORMATION_SCHEMA,))])["uc"]


def _metastore_id(sql: SqlClient, ctx: ScannerContext) -> str:
    try:
        rows = sql.query(METASTORE_SQL)
        value = rows[0]["metastore_id"] if rows else None
    except Exception as exc:
        ctx.limit(
            f"The metastore ID could not be read ({describe_error(exc)}); resource IDs use "
            "'unknown' as their scope."
        )
        return "unknown"
    return str(value) if value else "unknown"


def _list_catalogs(ctx: ScannerContext, reader: SourceReader, source: ResolvedSource) -> list[Row]:
    """Catalog rows, sorted by name. Per-catalog mode lists names with SHOW CATALOGS and
    then reads each catalog's own ``information_schema.catalogs`` for owner and comment."""
    if not source.per_catalog:
        result = reader.read(source)
        _note_missing_columns(ctx, source, result)
        rows = [{**row, "__source__": result.object_name} for row in result.rows]
        return sorted(rows, key=lambda row: str(row["catalog_name"]))

    try:
        listed = reader.sql.query(SHOW_CATALOGS_SQL)
    except Exception as exc:
        raise SourceUnavailableError("catalogs", SHOW_CATALOGS_SQL, describe_error(exc)) from exc
    names = sorted({str(row.get("catalog") or next(iter(row.values()), "")) for row in listed})
    catalog_rows: list[Row] = []
    for name in (n for n in names if n and ctx.policy.scope.includes_catalog(n)):
        try:
            result = reader.read(
                source, catalog=name, where={"catalog_name": "catalog"}, params={"catalog": name}
            )
        except SourceUnavailableError as exc:
            ctx.limit(
                f"Catalog details for '{name}' could not be read ({exc.object_name}: "
                f"{exc.reason}): {_OWNER} and {_COMMENT} are unknown for that catalog."
            )
            catalog_rows.append(
                {
                    "catalog_name": name,
                    "__source__": SHOW_CATALOGS_SQL,
                    "__missing__": ("catalog_owner", "comment", "created", "last_altered"),
                }
            )
            continue
        _note_missing_columns(ctx, source, result)
        match = next((r for r in result.rows if r["catalog_name"] == name), {"catalog_name": name})
        catalog_rows.append(
            {**match, "__source__": result.object_name, "__missing__": result.missing_columns}
        )
    return catalog_rows


def _read_optional(
    ctx: ScannerContext,
    reader: SourceReader,
    source: ResolvedSource,
    catalog: str,
    where: Mapping[str, str],
    what: str,
) -> SourceRows | None:
    try:
        result = reader.read(source, catalog=catalog, where=where, params={"catalog": catalog})
    except SourceUnavailableError as exc:
        rules = ", ".join(source.spec.used_by)
        ctx.limit(f"{what} could not be read ({exc.object_name}: {exc.reason}): {rules} unknown.")
        return None
    _note_missing_columns(ctx, source, result)
    return result


def _note_missing_columns(ctx: ScannerContext, source: ResolvedSource, result: SourceRows) -> None:
    for column in result.missing_columns:
        uses = source.spec.column(column).used_by
        if uses:
            ctx.limit(
                f"{result.object_name} has no '{source.actual(column)}' column: "
                f"{', '.join(uses)} unknown where it is needed."
            )


def _iso(value: object) -> str | None:
    parsed = utc_timestamp(value)
    return parsed.isoformat() if parsed else None


def _resource(
    metastore: str,
    kind: str,
    name: str,
    row: Row,
    *,
    source: str,
    missing: frozenset[str],
    owner_key: str,
    extra: tuple[Evidence, ...],
    metadata: dict[str, Any],
) -> Resource:
    def text(key: str) -> Evidence:
        label = "owner" if key == owner_key else key
        if key in missing:
            return Evidence(label, _U, source, note=f"Column not available in {source}.")
        value = row.get(key)
        value = value if isinstance(value, str) and value.strip() else None
        return Evidence(label, _O, source, value=value)

    def time(key: str, note: str) -> Evidence:
        value = None if key in missing else _iso(row.get(key))
        if value is None:
            return Evidence(key, _U, source, note=f"{note} Not reported.")
        return Evidence(key, _O, source, value=value, note=note)

    owner = text(owner_key)
    evidence = (
        owner,
        text("comment"),
        time("created", "Creation time."),
        time("last_altered", "Metadata change time."),
        *extra,
    )
    owner_name = owner.value if isinstance(owner.value, str) else None
    return Resource(
        ref=ResourceRef(metastore, kind, name),
        name=name,
        owner=owner_name,
        metadata=metadata,
        evidence=evidence,
    )


def _count_evidence(count: int | None, source: str) -> Evidence:
    if count is None:
        return Evidence(
            "visible_objects", _U, source, note="Volumes could not be read; count unknown."
        )
    if count > 0:
        return Evidence("visible_objects", _O, source, value=count)
    return Evidence(
        "visible_objects",
        _I,
        source,
        value=0,
        note="Counts only tables and volumes visible to the running principal.",
    )


def _required(resource: Resource, key: str) -> Evidence:
    evidence = resource.get_evidence(key)
    assert evidence is not None
    return evidence


def _evaluate_owner(resource: Resource) -> RuleEvaluation:
    evidence = _required(resource, "owner")
    if evidence.status is _U:
        return RuleEvaluation(
            resource.ref,
            OWNER_RULE.rule_id,
            Outcome.INDETERMINATE,
            (evidence,),
            ("Owner is not available from the configured source.",),
        )
    if evidence.value:
        return RuleEvaluation(
            resource.ref,
            OWNER_RULE.rule_id,
            Outcome.PASS,
            (evidence,),
            (f"Owner is {evidence.value}.",),
        )
    return RuleEvaluation(
        resource.ref, OWNER_RULE.rule_id, Outcome.FAIL, (evidence,), ("No owner is set.",)
    )


def _evaluate_comment(resource: Resource) -> RuleEvaluation:
    evidence = _required(resource, "comment")
    kind = resource.ref.resource_type
    if evidence.status is _U:
        return RuleEvaluation(
            resource.ref,
            COMMENT_RULE.rule_id,
            Outcome.INDETERMINATE,
            (evidence,),
            ("Comment is not available from the configured source.",),
        )
    if evidence.value:
        return RuleEvaluation(
            resource.ref, COMMENT_RULE.rule_id, Outcome.PASS, (evidence,), ("Has a description.",)
        )
    return RuleEvaluation(
        resource.ref,
        COMMENT_RULE.rule_id,
        Outcome.FAIL,
        (evidence,),
        (f"The {kind} has no comment describing its purpose.",),
    )


def _evaluate_temp_name(resource: Resource, patterns: tuple[str, ...]) -> RuleEvaluation:
    short_name = resource.name.rsplit(".", 1)[-1].lower()
    matched = next((p for p in patterns if fnmatchcase(short_name, p.lower())), None)
    if matched is None:
        evidence = Evidence("name", _O, NAME_SOURCE, value=resource.name)
        return RuleEvaluation(
            resource.ref, TEMP_RULE.rule_id, Outcome.PASS, (evidence,), ("Name looks permanent.",)
        )
    evidence = Evidence(
        "name", _I, NAME_SOURCE, value=resource.name, note="Naming is a heuristic signal only."
    )
    return RuleEvaluation(
        resource.ref,
        TEMP_RULE.rule_id,
        Outcome.FAIL,
        (evidence,),
        (f"Name matches temporary pattern '{matched}'; confirm it is still needed.",),
    )


def _evaluate_empty(resource: Resource) -> RuleEvaluation:
    evidence = _required(resource, "visible_objects")
    kind = resource.ref.resource_type
    if evidence.status is _U:
        return RuleEvaluation(
            resource.ref,
            EMPTY_RULE.rule_id,
            Outcome.INDETERMINATE,
            (evidence,),
            (f"Could not determine whether the {kind} contains objects.",),
        )
    if evidence.value:
        return RuleEvaluation(
            resource.ref,
            EMPTY_RULE.rule_id,
            Outcome.PASS,
            (evidence,),
            (f"Contains {evidence.value} visible table(s) or volume(s).",),
        )
    return RuleEvaluation(
        resource.ref,
        EMPTY_RULE.rule_id,
        Outcome.FAIL,
        (evidence,),
        (
            f"No tables or volumes are visible in this {kind}. Objects you lack privileges on, "
            "and functions or models, are not counted.",
        ),
    )


def _evaluate_tags(resource: Resource, required: tuple[str, ...]) -> RuleEvaluation:
    evidence = _required(resource, "tags")
    if evidence.status is _U or not isinstance(evidence.value, Mapping):
        return RuleEvaluation(
            resource.ref,
            TAGS_RULE.rule_id,
            Outcome.INDETERMINATE,
            (evidence,),
            ("Table tags could not be read.",),
        )
    present = {str(name).lower() for name in evidence.value}
    missing = [tag for tag in required if tag.lower() not in present]
    if missing:
        return RuleEvaluation(
            resource.ref,
            TAGS_RULE.rule_id,
            Outcome.FAIL,
            (evidence,),
            (f"Missing required tag(s): {', '.join(missing)}.",),
        )
    return RuleEvaluation(
        resource.ref, TAGS_RULE.rule_id, Outcome.PASS, (evidence,), ("All required tags are set.",)
    )


def _evaluate_stale(
    ctx: ScannerContext,
    resource: Resource,
    config: UCConfig,
    now: datetime,
    detail_budget: list[int],
    detail_stats: dict[str, int],
) -> RuleEvaluation:
    rule_id = STALE_RULE.rule_id
    cutoff = now - timedelta(days=config.stale_table_days)
    altered_evidence = _required(resource, "last_altered")
    altered = utc_timestamp(altered_evidence.value)
    if altered is not None and altered > cutoff:
        return RuleEvaluation(
            resource.ref,
            rule_id,
            Outcome.PASS,
            (altered_evidence,),
            (f"Metadata changed within the last {config.stale_table_days} days.",),
        )

    if detail_budget[0] <= 0:
        detail_stats["skipped"] += 1
        unavailable = Evidence(
            "last_modified",
            _U,
            DETAIL_SOURCE,
            note=f"Skipped: max_detail_queries ({config.max_detail_queries}) reached.",
        )
        return RuleEvaluation(
            resource.ref,
            rule_id,
            Outcome.INDETERMINATE,
            (altered_evidence, unavailable),
            ("Data modification time was not checked (detail query limit reached).",),
        )
    detail_budget[0] -= 1
    catalog, schema, table = (str(resource.metadata[k]) for k in ("catalog", "schema", "table"))
    try:
        rows = ctx.get_sql().query(f"DESCRIBE DETAIL {quote_identifier(catalog, schema, table)}")
        detail = rows[0] if rows else {}
    except Exception as exc:
        detail = {}
        detail_stats["failed"] += 1
        reason = f"DESCRIBE DETAIL failed ({type(exc).__name__})."
    else:
        reason = "DESCRIBE DETAIL returned no modification time."

    modified = utc_timestamp(detail.get("lastModified"))
    size = detail.get("sizeInBytes")
    size = size if isinstance(size, int) and not isinstance(size, bool) else None
    if modified is None:
        unavailable = Evidence("last_modified", _U, DETAIL_SOURCE, note=reason)
        return RuleEvaluation(
            resource.ref,
            rule_id,
            Outcome.INDETERMINATE,
            (altered_evidence, unavailable),
            (f"Data modification time is unknown: {reason}",),
        )
    modified_evidence = Evidence(
        "last_modified",
        _I,
        DETAIL_SOURCE,
        value=modified.isoformat(),
        note="Last data write; not a read or access time.",
    )
    if modified > cutoff:
        return RuleEvaluation(
            resource.ref,
            rule_id,
            Outcome.PASS,
            (altered_evidence, modified_evidence),
            (f"Data written within the last {config.stale_table_days} days.",),
        )
    idle_days = (now - modified).days
    min_bytes = int(config.min_table_size_gb * BYTES_PER_GB)
    if min_bytes > 0:
        if size is None:
            unavailable = Evidence("size_bytes", _U, DETAIL_SOURCE, note="Size not reported.")
            return RuleEvaluation(
                resource.ref,
                rule_id,
                Outcome.INDETERMINATE,
                (altered_evidence, modified_evidence, unavailable),
                (f"No data written for {idle_days} days, but table size is unknown.",),
            )
        if size < min_bytes:
            size_evidence = Evidence("size_bytes", _O, DETAIL_SOURCE, value=size)
            return RuleEvaluation(
                resource.ref,
                rule_id,
                Outcome.PASS,
                (altered_evidence, modified_evidence, size_evidence),
                (
                    f"No data written for {idle_days} days, but {_gb(size)} is below the "
                    f"{config.min_table_size_gb:g} GB threshold.",
                ),
            )
    size_note = f" and holds {_gb(size)}" if size is not None else ""
    evidence: tuple[Evidence, ...] = (altered_evidence, modified_evidence)
    if size is not None:
        evidence = (*evidence, Evidence("size_bytes", _O, DETAIL_SOURCE, value=size))
    return RuleEvaluation(
        resource.ref,
        rule_id,
        Outcome.FAIL,
        evidence,
        (
            f"No data written for {idle_days} days{size_note}. A write time is not a read "
            "time; confirm with lineage or audit logs before archiving.",
        ),
    )


def _gb(size: int | None) -> str:
    return "unknown size" if size is None else f"{size / BYTES_PER_GB:.2f} GB"
