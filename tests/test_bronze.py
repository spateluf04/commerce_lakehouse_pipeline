from datetime import date
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from conftest import SMALL_BATCH
from generator.generate import GeneratorConfig, write_batch
from jobs import bronze_ingest
from lakehouse_common.delta import read_delta

D1, D2 = date(2026, 3, 1), date(2026, 3, 2)


def _land(lake, ds):
    return write_batch(GeneratorConfig(run_date=ds, **SMALL_BATCH), lake.landing_dir)


def test_bronze_lands_every_record_with_metadata(spark, lake):
    manifest = _land(lake, D1)
    metrics = bronze_ingest.run(spark, lake, D1)

    expected = {}
    for t in ("customers", "products", "orders"):
        expected[("rows_in", f"bronze.{t}")] = expected[("rows_out", f"bronze.{t}")] = manifest["row_counts"][t]
    assert {(m.metric, m.table_name): m.value for m in metrics} == expected
    orders = read_delta(spark, lake.table_path("bronze", "orders"))
    assert orders.columns == ["raw_record", "_source_file", "_ingested_at", "ingest_date"]
    assert orders.where(F.col("_ingested_at").isNull() | F.col("_source_file").isNull()).count() == 0
    assert {r.ingest_date for r in orders.select("ingest_date").distinct().collect()} == {D1}
    assert orders.first()._source_file.endswith("orders/dt=2026-03-01/orders.jsonl")


def test_bronze_keeps_records_exactly_as_received(spark, lake):
    manifest = _land(lake, D1)
    bronze_ingest.run(spark, lake, D1)

    landed = (Path(lake.landing_dir) / manifest["files"]["orders"]).read_text().splitlines()
    stored = [r.raw_record for r in read_delta(spark, lake.table_path("bronze", "orders")).collect()]
    assert sorted(stored) == sorted(landed)

    customers = read_delta(spark, lake.table_path("bronze", "customers"))
    assert all(t == "string" for c, t in customers.dtypes if not c.startswith("_") and c != "ingest_date")


def test_bronze_rerun_is_idempotent_and_dates_are_independent(spark, lake):
    _land(lake, D1)
    _land(lake, D2)
    first = bronze_ingest.run(spark, lake, D1)
    bronze_ingest.run(spark, lake, D2)
    again = bronze_ingest.run(spark, lake, D1)  # re-run D1

    assert again == first
    orders = read_delta(spark, lake.table_path("bronze", "orders"))
    per_day = {r.ingest_date: r["count"] for r in orders.groupBy("ingest_date").count().collect()}
    [d1_rows] = [m.value for m in first if (m.metric, m.table_name) == ("rows_out", "bronze.orders")]
    assert per_day[D1] == d1_rows and set(per_day) == {D1, D2}


def test_bronze_fails_loudly_when_the_landing_files_are_missing(spark, lake):
    with pytest.raises(Exception, match="(?i)path does not exist"):
        bronze_ingest.run(spark, lake, D1)
