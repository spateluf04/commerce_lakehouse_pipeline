"""Structured (JSON lines) logging shared by every job.

Each record is one JSON object on stdout, so Airflow task logs (or any log shipper) can be
searched by field instead of by regex. Pass context with `extra`:

    log.info("wrote table", extra={"table": "silver_orders", "rows_out": 1234})
"""
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone

_ROOT = "lakehouse"
# Attributes every LogRecord has; anything else on a record came from `extra`.
_RESERVED = set(vars(logging.LogRecord("", 0, "", 0, "", None, None))) | {"message", "asctime"}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        payload.update({k: v for k, v in vars(record).items() if k not in _RESERVED})
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def get_logger(name: str) -> logging.Logger:
    """Return a logger under the `lakehouse` hierarchy, which writes JSON to stdout."""
    root = logging.getLogger(_ROOT)
    # Look for our handler specifically. Tools like pytest or Airflow may attach their own handlers too.
    if not any(isinstance(h.formatter, JsonFormatter) for h in root.handlers):
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        root.addHandler(handler)
        root.setLevel(os.environ.get("LAKEHOUSE_LOG_LEVEL", "INFO").upper())
        root.propagate = False  # don't log every line twice through the root logger (e.g. Airflow's)
    return logging.getLogger(f"{_ROOT}.{name}")
