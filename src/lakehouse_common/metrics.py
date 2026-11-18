"""Run metrics, appended to the ops.pipeline_run_metrics Delta table on every job run.

The table is in long format (one row per measurement: metric, table_name, value), not one column
per metric, so adding a metric never needs a schema change. It's append-only. Every attempt,
including failed ones and retries, is recorded, which is the history you want when debugging.

Metric names:
    rows_in                     rows read from upstream for table_name
    rows_out                    rows written to table_name
    rows_quarantined            rows rejected for table_name
    rows_quarantined_by_reason  the same, broken down by reason (in detail)
    check                       a data quality check: value = failed rows, status = pass/fail
    duration_seconds            wall-clock time of the job
    job_status                  1 = success, 0 = failed (status says which)
"""
from __future__ import annotations

import os
import uuid
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import types as T

from lakehouse_common.config import LakehouseConfig
from lakehouse_common.quality import CheckResult

METRICS_TABLE = "pipeline_run_metrics"
SCHEMA = T.StructType([
    T.StructField("run_id", T.StringType(), False),
    T.StructField("job", T.StringType(), False),
    T.StructField("ds", T.DateType(), False),
    T.StructField("try_number", T.IntegerType()),
    T.StructField("metric", T.StringType(), False),
    T.StructField("table_name", T.StringType()),
    T.StructField("value", T.DoubleType()),
    T.StructField("status", T.StringType()),
    T.StructField("detail", T.StringType()),
    T.StructField("recorded_at", T.TimestampType(), False),
])


@dataclass(frozen=True)
class Metric:
    metric: str
    value: float
    table_name: str | None = None
    status: str | None = None
    detail: str | None = None


def check_metric(result: CheckResult) -> Metric:
    return Metric("check", result.failed_rows, result.table, "pass" if result.passed else "fail", str(result))


@dataclass(frozen=True)
class RunContext:
    job: str
    ds: date
    run_id: str
    try_number: int | None = None

    @classmethod
    def from_env(cls, job: str, ds: date, environ: Mapping[str, str] | None = None) -> RunContext:
        """Pick up the Airflow run ID and try number, which Airflow exports to every task as AIRFLOW_CTX_*."""
        env = os.environ if environ is None else environ
        try_number = env.get("AIRFLOW_CTX_TRY_NUMBER")
        return cls(
            job=job,
            ds=ds,
            run_id=env.get("AIRFLOW_CTX_DAG_RUN_ID") or f"local__{uuid.uuid4().hex[:12]}",
            try_number=int(try_number) if try_number else None,
        )


def write_metrics(spark: SparkSession, config: LakehouseConfig, ctx: RunContext, metrics: Iterable[Metric]) -> None:
    recorded_at = datetime.now(timezone.utc)
    rows = [
        (ctx.run_id, ctx.job, ctx.ds, ctx.try_number, m.metric, m.table_name, float(m.value), m.status, m.detail, recorded_at)
        for m in metrics
    ]
    (
        spark.createDataFrame(rows, SCHEMA)
        .write.format("delta").mode("append").partitionBy("ds")
        .save(config.table_path("ops", METRICS_TABLE))
    )
