"""End to end: generator -> bronze -> silver -> quality gate -> gold, using the shared `pipeline` lakehouse."""
from pyspark.sql import functions as F

from conftest import PIPELINE_DATES, run_day
from jobs import silver_transform
from lakehouse_common.delta import read_delta

D1, D2 = PIPELINE_DATES


def _snapshot(spark, lake):
    """Everything a re-run must leave unchanged."""
    def table(layer, name):
        return read_delta(spark, lake.table_path(layer, name))

    counts = {
        f"{layer}.{name}": table(layer, name).count()
        for layer, names in {
            "bronze": ("orders", "customers", "products"),
            "silver": ("orders", "order_lines", "customers", "products", "quarantine"),
            "gold": ("daily_category_revenue", "top_products_daily", "customer_lifetime_value"),
        }.items()
        for name in names
    }
    revenue = table("gold", "daily_category_revenue").groupBy("order_date").agg(F.sum("revenue").alias("revenue"))
    return {
        "counts": counts,
        "silver_summary": {ds: silver_transform.summarize(spark, lake, ds) for ds in PIPELINE_DATES},
        "revenue_by_day": {r.order_date: r.revenue for r in revenue.collect()},
        "clv": sorted(tuple(r) for r in table("gold", "customer_lifetime_value").collect()),
    }


def test_rerunning_a_day_changes_nothing(spark, pipeline):
    before = _snapshot(spark, pipeline.lake)
    run_day(spark, pipeline.lake, D1)  # re-run (or backfill) a day that was already loaded
    assert _snapshot(spark, pipeline.lake) == before
    assert set(before["revenue_by_day"]) == {D1, D2}


def test_gold_revenue_reconciles_with_silver(spark, pipeline):
    lake = pipeline.lake
    gold = (
        read_delta(spark, lake.table_path("gold", "daily_category_revenue"))
        .where(F.col("order_date") == F.lit(D1)).agg(F.sum("revenue")).first()[0]
    )
    silver = (
        read_delta(spark, lake.table_path("silver", "order_lines"))
        .where((F.col("order_date") == F.lit(D1)) & ~F.col("order_status").isin("cancelled", "returned"))
        .agg(F.sum("line_total")).first()[0]
    )
    assert gold == silver and gold > 0


def test_top_products_has_ten_ranked_rows_per_day(spark, pipeline):
    top = read_delta(spark, pipeline.lake.table_path("gold", "top_products_daily"))
    for ds in PIPELINE_DATES:
        ranks = [r.revenue_rank for r in top.where(F.col("order_date") == F.lit(ds)).orderBy("revenue_rank").collect()]
        assert ranks == list(range(1, 11))
