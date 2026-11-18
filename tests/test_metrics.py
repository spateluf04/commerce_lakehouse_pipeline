from datetime import date

from lakehouse_common.delta import read_delta
from lakehouse_common.metrics import METRICS_TABLE, Metric, RunContext, check_metric, write_metrics
from lakehouse_common.quality import WARN, CheckResult

DS = date(2026, 3, 1)


def test_run_context_reads_airflow_env():
    ctx = RunContext.from_env("silver_transform", DS, {"AIRFLOW_CTX_DAG_RUN_ID": "scheduled__x", "AIRFLOW_CTX_TRY_NUMBER": "3"})
    assert (ctx.run_id, ctx.try_number) == ("scheduled__x", 3)


def test_run_context_outside_airflow_gets_a_local_run_id():
    ctx = RunContext.from_env("silver_transform", DS, {})
    assert ctx.run_id.startswith("local__") and ctx.try_number is None


def test_check_metric():
    m = check_metric(CheckResult("not_null", "silver.customers", ("email",), WARN, False, 4, 100))
    assert (m.metric, m.value, m.table_name, m.status) == ("check", 4, "silver.customers", "fail")
    assert "not_null(silver.customers: email) FAIL 4/100" in m.detail


def test_write_metrics_appends_every_run(spark, lake):
    first = RunContext("bronze_ingest", DS, "run_1", 1)
    retry = RunContext("bronze_ingest", DS, "run_1", 2)
    write_metrics(spark, lake, first, [Metric("rows_out", 10, "bronze.orders"), Metric("job_status", 0, status="failed")])
    write_metrics(spark, lake, retry, [Metric("rows_out", 10, "bronze.orders"), Metric("job_status", 1, status="success")])

    df = read_delta(spark, lake.table_path("ops", METRICS_TABLE))
    assert df.columns == ["run_id", "job", "ds", "try_number", "metric", "table_name", "value", "status", "detail", "recorded_at"]
    statuses = sorted((r.try_number, r.status) for r in df.where("metric = 'job_status'").collect())
    assert statuses == [(1, "failed"), (2, "success")]  # both attempts are kept, as history
    assert df.where("metric = 'rows_out'").count() == 2
