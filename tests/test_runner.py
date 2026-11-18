from datetime import date

import pytest

from lakehouse_common import runner
from lakehouse_common.metrics import Metric
from lakehouse_common.quality import ERROR, CheckResult, DataQualityError


class FakeSpark:
    stopped = False

    def stop(self):
        self.stopped = True


@pytest.fixture
def fake_spark(monkeypatch):
    fake = FakeSpark()
    monkeypatch.setattr(runner, "get_spark", lambda name, config: fake)
    return fake


@pytest.fixture
def recorded(monkeypatch):
    """Capture what the runner would write to pipeline_run_metrics."""
    calls = []
    monkeypatch.setattr(runner, "write_metrics", lambda spark, config, ctx, metrics: calls.append((ctx, list(metrics))))
    return calls


def _by_name(metrics):
    return {m.metric: m for m in metrics}


def test_run_job_passes_the_date_records_metrics_and_stops_spark(fake_spark, recorded, monkeypatch):
    monkeypatch.setenv("AIRFLOW_CTX_DAG_RUN_ID", "scheduled__2026-03-01")
    monkeypatch.setenv("AIRFLOW_CTX_TRY_NUMBER", "2")
    seen = {}

    def job(spark, config, ds):
        seen.update(spark=spark, ds=ds)
        return [Metric("rows_out", 5, "silver.orders")]

    assert runner.run_job("demo", job, ["--ds", "2026-03-01"]) == 0
    assert seen == {"spark": fake_spark, "ds": date(2026, 3, 1)}
    assert fake_spark.stopped

    [(ctx, metrics)] = recorded
    assert (ctx.job, ctx.ds, ctx.run_id, ctx.try_number) == ("demo", date(2026, 3, 1), "scheduled__2026-03-01", 2)
    by_name = _by_name(metrics)
    assert by_name["rows_out"].value == 5
    assert by_name["job_status"].status == "success"
    assert by_name["duration_seconds"].value >= 0


def test_run_job_records_failure_reraises_and_still_stops_spark(fake_spark, recorded):
    def job(spark, config, ds):
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        runner.run_job("demo", job, ["--ds", "2026-03-01"])
    assert fake_spark.stopped
    [(_, metrics)] = recorded
    assert _by_name(metrics)["job_status"].status == "failed"


def test_failed_quality_gate_still_records_every_check(fake_spark, recorded):
    results = [
        CheckResult("not_null", "silver.orders", ("order_id",), ERROR, True, 0, 10),
        CheckResult("unique", "silver.orders", ("order_id",), ERROR, False, 2, 10),
    ]

    def job(spark, config, ds):
        raise DataQualityError([results[1]], results)

    with pytest.raises(DataQualityError):
        runner.run_job("quality_gate", job, ["--ds", "2026-03-01"])
    [(_, metrics)] = recorded
    assert [(m.metric, m.status) for m in metrics if m.metric == "check"] == [("check", "pass"), ("check", "fail")]


def test_a_metrics_write_failure_does_not_fail_the_job(fake_spark, monkeypatch):
    def broken(*args):
        raise OSError("disk full")

    monkeypatch.setattr(runner, "write_metrics", broken)
    assert runner.run_job("demo", lambda spark, config, ds: [], ["--ds", "2026-03-01"]) == 0


def test_run_job_rejects_a_bad_date(fake_spark):
    with pytest.raises(SystemExit):
        runner.run_job("demo", lambda *a: [], ["--ds", "03/01/2026"])
