"""Quality gate: dataset-level checks on silver for one date. It runs before gold is built.

Silver has already quarantined bad rows one at a time. This gate checks properties of whole tables
that a row-by-row check can't see: the day isn't empty, keys are unique across the table, and
every foreign key resolves. If any "error" check fails, the job raises, Airflow marks the task
failed, and gold is not built from bad data.
"""
from __future__ import annotations

from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from lakehouse_common.config import LakehouseConfig
from lakehouse_common.delta import read_delta
from lakehouse_common.log import get_logger
from lakehouse_common.metrics import Metric, check_metric
from lakehouse_common.quality import (
    WARN,
    CheckResult,
    check_not_null,
    check_referential_integrity,
    check_row_count,
    check_unique,
    check_value_range,
    raise_on_failure,
)
from lakehouse_common.runner import run_job

log = get_logger(__name__)


def run_checks(spark: SparkSession, config: LakehouseConfig, ds: date) -> list[CheckResult]:
    def silver(table: str):
        return read_delta(spark, config.table_path("silver", table))

    orders = silver("orders").where(F.col("order_date") == F.lit(ds))
    lines = silver("order_lines").where(F.col("order_date") == F.lit(ds))
    customers = silver("customers")
    products = silver("products")

    return [
        check_row_count(orders, table="silver.orders"),
        check_not_null(orders, ["order_id", "customer_id", "order_ts", "order_date", "order_total"], table="silver.orders"),
        check_unique(orders, ["order_id"], table="silver.orders"),
        check_value_range(orders, "order_total", table="silver.orders", min_value=0),
        check_referential_integrity(orders, "customer_id", customers, "customer_id", table="silver.orders", ref_table="silver.customers"),
        check_row_count(lines, table="silver.order_lines"),
        check_unique(lines, ["order_id", "line_number"], table="silver.order_lines"),
        check_value_range(lines, "quantity", table="silver.order_lines", min_value=1),
        check_value_range(lines, "unit_price", table="silver.order_lines", min_value=0),
        check_referential_integrity(lines, "order_id", orders, "order_id", table="silver.order_lines", ref_table="silver.orders"),
        check_referential_integrity(lines, "product_id", products, "product_id", table="silver.order_lines", ref_table="silver.products"),
        check_unique(customers, ["customer_id"], table="silver.customers"),
        check_not_null(customers, ["email"], table="silver.customers", severity=WARN),
        check_unique(products, ["product_id"], table="silver.products"),
    ]


def run(spark: SparkSession, config: LakehouseConfig, ds: date) -> list[Metric]:
    results = run_checks(spark, config, ds)
    for r in results:
        log.info(str(r), extra={"check": r.check, "table": r.table, "passed": r.passed, "severity": r.severity,
                                "failed_rows": r.failed_rows, "total_rows": r.total_rows})
    raise_on_failure(results)  # on failure the runner records every result from the exception
    return [check_metric(r) for r in results]


if __name__ == "__main__":
    raise SystemExit(run_job("quality_gate", run))
