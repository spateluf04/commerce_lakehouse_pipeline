"""Command-line entry point shared by every Spark job: `python -m jobs.<name> --ds YYYY-MM-DD`.

It parses the business date, builds the config and SparkSession, runs the job, and always (even
when the job fails) writes the job's metrics plus its duration and status to
ops.pipeline_run_metrics, then stops Spark. The job modules contain only their transformation logic.
"""
from __future__ import annotations

import argparse
import time
from collections.abc import Callable, Iterable
from dataclasses import asdict
from datetime import date

from pyspark.sql import SparkSession

from lakehouse_common.config import LakehouseConfig
from lakehouse_common.log import get_logger
from lakehouse_common.metrics import Metric, RunContext, check_metric, write_metrics
from lakehouse_common.quality import DataQualityError
from lakehouse_common.spark import get_spark

log = get_logger(__name__)

Job = Callable[[SparkSession, LakehouseConfig, date], Iterable[Metric]]


def run_job(name: str, job: Job, argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog=f"python -m jobs.{name}")
    parser.add_argument("--ds", required=True, type=date.fromisoformat, help="business date, YYYY-MM-DD (Airflow's {{ ds }})")
    args = parser.parse_args(argv)

    config = LakehouseConfig.from_env()
    ctx = RunContext.from_env(name, args.ds)
    context = {"job": name, "ds": args.ds.isoformat(), "run_id": ctx.run_id, "storage_root": config.storage_root}
    log.info("job started", extra=context)
    started = time.monotonic()
    spark = get_spark(name, config)
    metrics: list[Metric] = []
    succeeded = False
    try:
        metrics = list(job(spark, config, args.ds))
        succeeded = True
    except Exception as exc:
        if isinstance(exc, DataQualityError):
            metrics = [check_metric(r) for r in exc.results]  # a failed gate still records every check
        log.exception("job failed", extra=context)
        raise
    finally:
        duration = round(time.monotonic() - started, 1)
        metrics += [
            Metric("duration_seconds", duration),
            Metric("job_status", 1 if succeeded else 0, status="success" if succeeded else "failed"),
        ]
        _record(spark, config, ctx, metrics)
        spark.stop()
    log.info("job finished", extra={**context, "duration_s": duration, "metrics": [asdict(m) for m in metrics]})
    return 0


def _record(spark, config, ctx, metrics) -> None:
    # Metrics are best effort: failing to record them must never hide or replace the job's own result.
    try:
        write_metrics(spark, config, ctx, metrics)
    except Exception:
        log.exception("could not write run metrics", extra={"job": ctx.job, "run_id": ctx.run_id})
