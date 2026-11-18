import json
import logging
import sys
from pathlib import Path

from lakehouse_common.log import JsonFormatter, get_logger


def _record(msg="wrote %s rows", args=(5,), exc_info=None):
    return logging.LogRecord("lakehouse.test", logging.INFO, __file__, 1, msg, args, exc_info)


def test_formatter_emits_json_with_extra_fields():
    record = _record()
    record.table = "silver_orders"
    record.path = Path("data/x")  # not JSON-serializable, so it's written as a string
    out = json.loads(JsonFormatter().format(record))
    assert out["msg"] == "wrote 5 rows"
    assert out["level"] == "INFO"
    assert out["logger"] == "lakehouse.test"
    assert out["table"] == "silver_orders"
    assert out["path"] == str(Path("data/x"))
    assert out["ts"].endswith("+00:00")
    assert "args" not in out and "lineno" not in out  # standard LogRecord attributes are left out


def test_formatter_includes_exception():
    try:
        raise ValueError("boom")
    except ValueError:
        record = _record("failed", (), sys.exc_info())
    out = json.loads(JsonFormatter().format(record))
    assert "ValueError: boom" in out["exc_info"]


def test_get_logger_configures_one_handler():
    a = get_logger("jobs.a")
    get_logger("jobs.b")
    assert a.name == "lakehouse.jobs.a"
    handlers = logging.getLogger("lakehouse").handlers
    assert sum(isinstance(h.formatter, JsonFormatter) for h in handlers) == 1
