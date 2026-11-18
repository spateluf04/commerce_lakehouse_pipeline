"""Failure alerts: always written to the log, and posted to Slack when SLACK_WEBHOOK_URL is set.

Standard library only (urllib), so the Airflow DAG can import this without pulling in a Slack SDK.
"""
from __future__ import annotations

import json
import os
import urllib.request
from collections.abc import Mapping
from typing import Any

from lakehouse_common.log import get_logger

log = get_logger(__name__)


def failure_details(context: Mapping[str, Any]) -> dict:
    """Pull out of an Airflow task context the fields worth putting in an alert."""
    ti = context.get("ti") or context.get("task_instance")
    return {
        "dag_id": getattr(ti, "dag_id", None),
        "task_id": getattr(ti, "task_id", None),
        "run_id": getattr(ti, "run_id", None),
        "ds": context.get("ds"),
        "try_number": getattr(ti, "try_number", None),
        "log_url": getattr(ti, "log_url", None),
        "exception": repr(context.get("exception")) if context.get("exception") else None,
    }


def format_message(details: Mapping[str, Any]) -> str:
    lines = [
        f":red_circle: *{details['dag_id']}.{details['task_id']}* failed for ds={details['ds']} "
        f"(try {details['try_number']}, run {details['run_id']})",
    ]
    if details.get("exception"):
        lines.append(f"```{details['exception'][:500]}```")
    if details.get("log_url"):
        lines.append(f"<{details['log_url']}|View logs>")
    return "\n".join(lines)


def send_slack(webhook_url: str, text: str, *, timeout: float = 10) -> None:
    body = json.dumps({"text": text}).encode()
    request = urllib.request.Request(webhook_url, data=body, headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310 - the URL comes from our own env
        response.read()


def alert_on_failure(context: Mapping[str, Any]) -> None:
    """Airflow on_failure_callback. It runs once a task has failed its last retry.

    It never raises: a broken alert channel must not hide the task's real error.
    """
    details = failure_details(context)
    log.error("pipeline task failed", extra={"alert": details})
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook_url:
        return
    try:
        send_slack(webhook_url, format_message(details))
    except Exception:
        log.exception("could not send Slack alert", extra={"dag_id": details["dag_id"], "task_id": details["task_id"]})
