"""Structured logging helpers built on the standard library.

The library never configures handlers on the root logger. Structured fields are passed via
``extra=`` (``event``, ``run_id``, ``scanner``, ``phase``, ...). Use ``JsonFormatter`` to
emit them as JSON lines.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

LOGGER_NAME = "lakehouse_gc"

_RESERVED = frozenset(vars(logging.LogRecord("", 0, "", 0, "", (), None))) | {
    "message",
    "asctime",
}


def get_logger(name: str | None = None) -> logging.Logger:
    if not name or name == LOGGER_NAME:
        return logging.getLogger(LOGGER_NAME)
    if name.startswith(f"{LOGGER_NAME}."):
        return logging.getLogger(name)
    return logging.getLogger(f"{LOGGER_NAME}.{name}")


class JsonFormatter(logging.Formatter):
    """Formats records as single-line JSON including any ``extra`` fields."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in vars(record).items():
            if key not in _RESERVED and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str, sort_keys=True)
