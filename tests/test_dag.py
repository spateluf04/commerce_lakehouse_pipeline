"""The DAG loads and has the intended shape. Skipped when Airflow isn't installed."""
import importlib.util
from datetime import timedelta
from pathlib import Path

import pytest

pytest.importorskip("airflow")

DAG_FILE = Path(__file__).parents[1] / "dags" / "commerce_lakehouse_daily.py"


@pytest.fixture(scope="module")
def dag():
    spec = importlib.util.spec_from_file_location("commerce_lakehouse_daily", DAG_FILE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.dag


def test_tasks_run_in_order(dag):
    order = ["generate_source_data", "bronze_ingest", "silver_transform", "quality_gate", "gold_build"]
    assert sorted(dag.task_ids) == sorted(order)
    for upstream, downstream in zip(order, order[1:]):
        assert dag.get_task(upstream).downstream_task_ids == {downstream}


def test_schedule_processes_the_previous_full_day(dag):
    from airflow.timetables.interval import CronDataIntervalTimetable

    assert isinstance(dag.timetable, CronDataIntervalTimetable)
    assert dag.max_active_runs == 1
    assert dag.catchup is False


def test_every_task_retries_alerts_and_uses_ds(dag):
    from lakehouse_common.alerts import alert_on_failure

    for task in dag.tasks:
        assert task.retries == 2
        assert task.retry_delay == timedelta(minutes=1)
        assert task.on_failure_callback in (alert_on_failure, [alert_on_failure])
        assert "{{ ds }}" in task.bash_command
        assert task.cwd == "/opt/project"
