import json
from types import SimpleNamespace

import pytest

from lakehouse_common import alerts


def _context():
    ti = SimpleNamespace(dag_id="commerce_lakehouse_daily", task_id="silver_transform", run_id="scheduled__2026-03-01",
                         try_number=3, log_url="http://localhost:8080/log")
    return {"ti": ti, "ds": "2026-03-01", "exception": ValueError("bad data")}


@pytest.fixture
def posts(monkeypatch):
    """Capture Slack webhook requests instead of sending them."""
    sent = []

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def read(self):
            return b"ok"

    def fake_urlopen(request, timeout):
        sent.append({"url": request.full_url, "body": json.loads(request.data), "timeout": timeout})
        return Response()

    monkeypatch.setattr(alerts.urllib.request, "urlopen", fake_urlopen)
    return sent


def test_failure_details_and_message():
    details = alerts.failure_details(_context())
    assert details["task_id"] == "silver_transform"
    assert details["exception"] == "ValueError('bad data')"
    message = alerts.format_message(details)
    assert "commerce_lakehouse_daily.silver_transform* failed for ds=2026-03-01" in message
    assert "try 3" in message and "View logs" in message


def test_alert_only_logs_without_a_webhook(monkeypatch, posts):
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    alerts.alert_on_failure(_context())
    assert posts == []


def test_alert_posts_to_slack_when_configured(monkeypatch, posts):
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/T/B/X")
    alerts.alert_on_failure(_context())
    [post] = posts
    assert post["url"] == "https://hooks.slack.com/services/T/B/X"
    assert "silver_transform" in post["body"]["text"]


def test_a_broken_webhook_never_raises(monkeypatch):
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/T/B/X")

    def fail(*args, **kwargs):
        raise OSError("network down")

    monkeypatch.setattr(alerts.urllib.request, "urlopen", fail)
    alerts.alert_on_failure(_context())  # must not raise
