"""Read-only SQL access for scanners.

Scanners query through a ``SqlClient``: anything with ``query(statement, params) -> rows``.
In Databricks the default wraps the notebook's Spark session; tests pass a fake. pyspark is
never a dependency of this package: it is looked up at runtime only when needed.
"""

from __future__ import annotations

import importlib
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any, Protocol, runtime_checkable

from lakehouse_gc.core.errors import LakehouseGCError

Row = dict[str, Any]


@runtime_checkable
class SqlClient(Protocol):
    def query(self, statement: str, params: Mapping[str, Any] | None = None) -> list[Row]:
        """Run a read-only statement with named ``:param`` markers; return rows as dicts."""
        ...


class SparkSqlClient:
    """``SqlClient`` backed by a SparkSession (named parameters need DBR 13.3 LTS+)."""

    def __init__(self, spark: Any) -> None:
        self._spark = spark

    def query(self, statement: str, params: Mapping[str, Any] | None = None) -> list[Row]:
        frame = (
            self._spark.sql(statement, args=dict(params)) if params else self._spark.sql(statement)
        )
        return [row.asDict(recursive=True) for row in frame.collect()]

    def columns(self, object_name: str) -> list[str]:
        """Column names of a table or view, without reading any rows."""
        statement = f"SELECT * FROM {quote_identifier(*object_name.split('.'))} LIMIT 0"
        return list(self._spark.sql(statement).columns)


def as_sql_client(value: Any) -> SqlClient:
    """Accept a ``SqlClient`` or a SparkSession-like object (anything with ``.sql``)."""
    if isinstance(value, SqlClient):
        return value
    if callable(getattr(value, "sql", None)):
        return SparkSqlClient(value)
    raise LakehouseGCError(
        f"spark must be a SparkSession or an object with query(statement, params); got "
        f"{type(value).__name__}"
    )


def default_sql_client() -> SqlClient:
    """Wrap the active Spark session, or fail with a clear message outside Databricks."""
    try:
        spark_sql = importlib.import_module("pyspark.sql")
    except ImportError:
        raise LakehouseGCError(
            "pyspark is not available; run inside Databricks or pass spark=... to run()"
        ) from None
    session = spark_sql.SparkSession.getActiveSession()
    if session is None:
        raise LakehouseGCError("no active Spark session; pass spark=spark to run()")
    return SparkSqlClient(session)


def quote_identifier(*parts: str) -> str:
    """Backtick-quote a multi-part name, escaping embedded backticks."""
    return ".".join("`" + part.replace("`", "``") + "`" for part in parts)


def table_columns(client: SqlClient, object_name: str) -> list[str]:
    """Column names of ``object_name``. Uses the client's ``columns()`` when it has one,
    otherwise ``DESCRIBE TABLE``. Raises if the object can't be read."""
    columns = getattr(client, "columns", None)
    if callable(columns):
        return list(columns(object_name))
    rows = client.query(f"DESCRIBE TABLE {quote_identifier(*object_name.split('.'))}")
    names = []
    for row in rows:
        name = str(row.get("col_name") or "").strip()
        if not name or name.startswith("#"):
            break
        names.append(name)
    return names


def utc_timestamp(value: object) -> datetime | None:
    """Normalize Spark timestamps (naive values are UTC on Databricks drivers) or ISO text."""
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value)
        except ValueError:
            return None
    if not isinstance(value, datetime):
        return None
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
