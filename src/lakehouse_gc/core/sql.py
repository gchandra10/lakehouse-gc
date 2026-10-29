"""Read-only SQL access for scanners.

Scanners query through a ``SqlClient``: anything with ``query(statement, params) -> rows``.
In Databricks the default wraps the notebook's Spark session; tests pass a fake. pyspark is
never a dependency of this package: it is looked up at runtime only when needed.
"""

from __future__ import annotations

import importlib
from collections.abc import Mapping
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
