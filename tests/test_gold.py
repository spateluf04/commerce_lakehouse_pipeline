from datetime import date
from decimal import Decimal

import pytest

from jobs.gold_build import GOLD_TABLES, build

D1, D0 = date(2026, 3, 1), date(2026, 2, 28)
TABLES = {t.name: t for t in GOLD_TABLES}


@pytest.fixture(scope="module")
def silver_views(spark):
    """Hand-built silver tables with numbers that are easy to check by hand."""
    spark.createDataFrame(
        [
            ("O1", "C1", D1, "delivered", Decimal("30.00")),
            ("O2", "C1", D1, "cancelled", Decimal("100.00")),
            ("O3", "C2", D1, "shipped", Decimal("15.00")),
            ("O4", "C2", D0, "delivered", Decimal("50.00")),
        ],
        "order_id STRING, customer_id STRING, order_date DATE, status STRING, order_total DECIMAL(12,2)",
    ).createOrReplaceTempView("silver_orders")
    spark.createDataFrame(
        [
            ("O1", D1, "delivered", "P1", "Pro Mouse", "Electronics", 1, Decimal("10.00")),
            ("O1", D1, "delivered", "P2", "Classic Novel", "Books", 2, Decimal("20.00")),
            ("O2", D1, "cancelled", "P1", "Pro Mouse", "Electronics", 10, Decimal("100.00")),
            ("O3", D1, "shipped", "P2", "Classic Novel", "Books", 1, Decimal("15.00")),
            ("O4", D0, "delivered", "P1", "Pro Mouse", "Electronics", 5, Decimal("50.00")),
        ],
        "order_id STRING, order_date DATE, order_status STRING, product_id STRING, product_name STRING, "
        "category STRING, quantity INT, line_total DECIMAL(12,2)",
    ).createOrReplaceTempView("silver_order_lines")
    spark.createDataFrame(
        [("C1", "Ava", "Lee", "TX"), ("C2", "Wei", "Chen", "WA")],
        "customer_id STRING, first_name STRING, last_name STRING, state STRING",
    ).createOrReplaceTempView("silver_customers")


def _collect(spark, name, ds, order_by):
    return [tuple(r) for r in build(spark, TABLES[name], ds).orderBy(*order_by).collect()]


def test_daily_category_revenue_excludes_cancelled_orders(spark, silver_views):
    assert _collect(spark, "daily_category_revenue", D1, ["category"]) == [
        (D1, "Books", 2, 3, Decimal("35.00")),
        (D1, "Electronics", 1, 1, Decimal("10.00")),
    ]


def test_daily_category_revenue_only_covers_the_requested_day(spark, silver_views):
    assert _collect(spark, "daily_category_revenue", D0, ["category"]) == [(D0, "Electronics", 1, 5, Decimal("50.00"))]


def test_top_products_daily_ranks_by_revenue(spark, silver_views):
    assert _collect(spark, "top_products_daily", D1, ["revenue_rank"]) == [
        (D1, 1, "P2", "Classic Novel", "Books", 3, Decimal("35.00")),
        (D1, 2, "P1", "Pro Mouse", "Electronics", 1, Decimal("10.00")),
    ]


def test_customer_lifetime_value_spans_all_days(spark, silver_views):
    expected = [
        ("C1", "Ava", "Lee", "TX", D1, D1, 1, Decimal("30.00"), Decimal("30.00")),
        ("C2", "Wei", "Chen", "WA", D0, D1, 2, Decimal("65.00"), Decimal("32.50")),
    ]
    assert _collect(spark, "customer_lifetime_value", D1, ["customer_id"]) == expected
    assert _collect(spark, "customer_lifetime_value", D0, ["customer_id"]) == expected  # same whatever ds is
