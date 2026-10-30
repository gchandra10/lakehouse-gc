"""Saving run results: JSON text, a new JSON file, or an appended Delta table.

This is the only module that writes anything, and it writes only results, only to a
location the user names. It never changes a scanned resource.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any

from lakehouse_gc.core.errors import LakehouseGCError
from lakehouse_gc.core.sql import quote_identifier

if TYPE_CHECKING:
    from lakehouse_gc.core.results import RunResult
    from lakehouse_gc.core.scoring import DimensionScore


def _dimension(score: DimensionScore) -> dict[str, Any]:
    return {"score": score.score, "checks": score.evaluations, "coverage": score.coverage}


def run_to_json(result: RunResult, indent: int | None = 2) -> str:
    """The run as JSON text. Timestamps are ISO 8601; unknown values are ``null``."""
    rows = [{**row, "run_time": row["run_time"].isoformat()} for row in result.to_rows()]
    document = {
        "run_id": result.run_id,
        "started_at": result.started_at.isoformat(),
        "finished_at": result.finished_at.isoformat(),
        "status": result.status.value,
        "policy_version": result.policy_version,
        "scanners": list(result.scanners),
        "scores": {
            "governance": _dimension(result.scores.governance),
            "cost": _dimension(result.scores.cost),
            "overall": result.scores.overall,
            "grade": result.scores.grade,
            "complete": result.scores.complete,
        },
        "limitations": list(result.limitations),
        "warnings": list(result.warnings),
        "errors": [
            {
                "scanner": error.scanner,
                "phase": error.phase.value,
                "type": error.error_type,
                "message": error.message,
            }
            for error in result.errors
        ],
        "findings": rows,
    }
    return json.dumps(document, indent=indent)


def _table_name(table: str) -> str:
    parts = table.split(".")
    if len(parts) != 3 or not all(part.strip() for part in parts):
        raise LakehouseGCError(f"table must be catalog.schema.table; got {table!r}")
    return quote_identifier(*parts)


def save(
    result: RunResult,
    spark: Any = None,
    *,
    table: str | None = None,
    json_path: str | None = None,
) -> list[str]:
    """Write the results where asked; return a line per target written."""
    if not table and not json_path:
        raise ValueError(
            "nothing to save: pass table='catalog.schema.table' and/or "
            "json_path='/Volumes/.../run.json', or just display(result.to_dataframe(spark))"
        )
    written = []
    if table:
        name = _table_name(table)  # validate before writing anything
        if spark is None:
            raise LakehouseGCError("saving to a table needs spark: result.save(spark, table=...)")
    if json_path:
        path = Path(json_path)
        try:
            with path.open("x", encoding="utf-8") as handle:
                handle.write(result.to_json())
        except FileExistsError:
            raise LakehouseGCError(
                f"{json_path} already exists; lakehouse-gc never overwrites files. Choose a new "
                "path, for example one that includes the run_id."
            ) from None
        except FileNotFoundError:
            raise LakehouseGCError(f"folder {path.parent} does not exist") from None
        written.append(f"JSON: {json_path}")
    if table:
        result.to_dataframe(spark).write.format("delta").mode("append").saveAsTable(name)
        written.append(f"Delta table (appended {len(result.findings)} rows): {table}")
    return written
