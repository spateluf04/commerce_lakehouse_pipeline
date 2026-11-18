"""Daily commerce lakehouse pipeline: generate -> bronze -> silver -> data quality -> gold.

Every task is one job process (`python -m ...`) that runs Spark in local mode, so each task gets a
fresh JVM, much as spark-submit would. The DAG only orchestrates. All the logic is in src/, which
is tested without Airflow.

Scheduling: a run for ds=2026-03-01 covers the interval 2026-03-01 00:00 to 2026-03-02 00:00 UTC
and starts once that interval has ended. Every job is idempotent for its ds, so retries, manual
re-runs and backfills are all safe.
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG
from airflow.timetables.interval import CronDataIntervalTimetable

from lakehouse_common.alerts import alert_on_failure

PROJECT_DIR = "/opt/project"

default_args = {
    "owner": "commerce-data",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": alert_on_failure,  # fires only after the last retry has failed
}


def job(task_id: str, command: str) -> BashOperator:
    # BashOperator runs in a temporary directory by default. Run from the repo so relative paths resolve.
    return BashOperator(task_id=task_id, bash_command=command, cwd=PROJECT_DIR)


with DAG(
    dag_id="commerce_lakehouse_daily",
    description="Commerce medallion lakehouse: raw files to Delta bronze/silver/gold",
    # In Airflow 3, schedule="@daily" means a CronTriggerTimetable, where ds is the moment the run
    # starts (today, which has no data yet). CronDataIntervalTimetable sets ds to the start of
    # the interval that just ended, i.e. yesterday, the day whose data is complete.
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone="UTC"),
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,  # history is loaded with an explicit backfill (see README), not on first deploy
    max_active_runs=1,  # one writer at a time: open-source Delta on S3 is only safe with a single writer
    default_args=default_args,
    tags=["commerce", "lakehouse", "delta"],
) as dag:
    generate = job(
        "generate_source_data",
        # Simulates the source system's daily drop, then copies it to the S3 landing zone (a no-op locally).
        "python -m generator --date {{ ds }} && python -m jobs.upload_landing --ds {{ ds }}",
    )
    bronze = job("bronze_ingest", "python -m jobs.bronze_ingest --ds {{ ds }}")
    silver = job("silver_transform", "python -m jobs.silver_transform --ds {{ ds }}")
    quality = job("quality_gate", "python -m jobs.quality_gate --ds {{ ds }}")
    gold = job("gold_build", "python -m jobs.gold_build --ds {{ ds }}")

    generate >> bronze >> silver >> quality >> gold
