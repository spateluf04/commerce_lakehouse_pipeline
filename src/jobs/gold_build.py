"""Gold: business tables built with plain Spark SQL from the silver tables.

The SQL lives in jobs/sql/gold/<table>.sql, so the business logic can be read and reviewed without
reading any Python. Each query can use the :ds parameter (the business date).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse_common.config import LakehouseConfig
from lakehouse_common.delta import overwrite_partition, overwrite_table, read_delta
from lakehouse_common.metrics import Metric
from lakehouse_common.runner import run_job

SQL_DIR = Path(__file__).parent / "sql" / "gold"
SILVER_TABLES = ("orders", "order_lines", "customers", "products")


@dataclass(frozen=True)
class GoldTable:
    name: str
    daily: bool  # True: rebuild only the order_date = ds partition. False: rebuild the whole table.


GOLD_TABLES = (
    GoldTable("daily_category_revenue", daily=True),
    GoldTable("top_products_daily", daily=True),
    GoldTable("customer_lifetime_value", daily=False),
)


def register_silver_views(spark: SparkSession, config: LakehouseConfig) -> None:
    """Expose the silver tables to SQL as silver_<table>, so queries don't hard-code storage paths."""
    for table in SILVER_TABLES:
        read_delta(spark, config.table_path("silver", table)).createOrReplaceTempView(f"silver_{table}")


def build(spark: SparkSession, table: GoldTable, ds: date) -> DataFrame:
    sql = (SQL_DIR / f"{table.name}.sql").read_text(encoding="utf-8")
    # A named parameter (:ds) instead of string formatting: Spark binds it as a literal value.
    return spark.sql(sql, args={"ds": ds.isoformat()})


def run(spark: SparkSession, config: LakehouseConfig, ds: date) -> list[Metric]:
    register_silver_views(spark, config)
    day_lines = spark.table("silver_order_lines").where(F.col("order_date") == F.lit(ds)).count()
    all_orders = spark.table("silver_orders").count()
    metrics = []
    for table in GOLD_TABLES:
        path = config.table_path("gold", table.name)
        df = build(spark, table, ds)
        if table.daily:
            overwrite_partition(df, path, replace_where=f"order_date = '{ds.isoformat()}'", partition_by=["order_date"])
            rows_in, rows_out = day_lines, read_delta(spark, path).where(F.col("order_date") == F.lit(ds)).count()
        else:
            overwrite_table(df, path)
            rows_in, rows_out = all_orders, read_delta(spark, path).count()
        metrics += [Metric("rows_in", rows_in, f"gold.{table.name}"), Metric("rows_out", rows_out, f"gold.{table.name}")]
    return metrics


if __name__ == "__main__":
    raise SystemExit(run_job("gold_build", run))
