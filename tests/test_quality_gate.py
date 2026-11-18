from datetime import date

import pytest
from pyspark.sql import functions as F

from jobs import quality_gate
from lakehouse_common.delta import read_delta
from lakehouse_common.quality import DataQualityError

DS = date(2026, 3, 1)


def test_gate_passes_on_clean_silver(spark, pipeline):
    results = quality_gate.run_checks(spark, pipeline.lake, DS)
    assert all(r.passed for r in results), [str(r) for r in results if not r.passed]
    metrics = quality_gate.run(spark, pipeline.lake, DS)
    assert len(metrics) == len(results)
    assert {m.status for m in metrics} == {"pass"}


def test_gate_fails_on_a_day_with_no_orders(spark, pipeline):
    with pytest.raises(DataQualityError) as excinfo:
        quality_gate.run(spark, pipeline.lake, date(2026, 3, 9))  # this day was never loaded
    assert {f.check for f in excinfo.value.failures} == {"row_count"}


def test_gate_fails_on_duplicate_orders_in_silver(spark, lake, load_day):
    load_day(DS, orders=40)
    path = lake.table_path("silver", "orders")
    read_delta(spark, path).where(F.col("order_date") == F.lit(DS)).limit(1).write.format("delta").mode("append").save(path)

    with pytest.raises(DataQualityError) as excinfo:
        quality_gate.run(spark, lake, DS)
    assert [(f.check, f.table) for f in excinfo.value.failures] == [("unique", "silver.orders")]
