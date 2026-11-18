import json
from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import functions as F

from conftest import SMALL_BATCH
from generator.generate import GeneratorConfig, generate_customers
from jobs.silver_transform import transform_customers, transform_orders
from lakehouse_common.delta import read_delta

DS = date(2026, 3, 1)
INGESTED = datetime(2026, 3, 2, 1, 0)
SOURCE = "file:/landing/x"


def _quarantined(df):
    """[(record_key, sorted reasons)] sorted, for easy comparison."""
    return sorted(((r.record_key, sorted(r.reject_reasons)) for r in df.collect()), key=lambda t: (str(t[0]), t[1]))


# --- transform_orders on fixture rows -----------------------------------------

def _order(**overrides):
    order = {
        "order_id": "O1", "customer_id": "C1", "order_ts": "2026-03-01T10:00:00Z", "status": "delivered",
        "payment_method": "paypal", "currency": "USD",
        "line_items": [{"line_number": 1, "product_id": "P1", "quantity": 2, "unit_price": 12.5}],
    }
    order.update(overrides)
    return json.dumps(order)


def _bad_price():
    return [{"line_number": 1, "product_id": "P1", "quantity": 1, "unit_price": -3.0}]


def test_transform_orders_routes_each_defect_to_quarantine(spark):
    records = [
        _order(),
        _order(),  # sent twice
        _order(order_id="O2", customer_id=None),
        _order(order_id="O3", customer_id="C404"),
        _order(order_id="O4", line_items=_bad_price()),
        _order(order_id="O5", order_ts="nope"),
        _order(order_id="O6", order_ts="2026-02-27T10:00:00Z"),
        _order(order_id="O7", line_items=[{"line_number": 1, "product_id": "P404", "quantity": 1, "unit_price": 1.0}]),
        _order(order_id="O8", line_items=[]),
        _order(order_id=None),
        _order(order_id="O10", customer_id=None, line_items=_bad_price()),  # two defects at once
        '{"order_id": "O11", not json',
    ]
    bronze = spark.createDataFrame([(r, SOURCE, INGESTED) for r in records], "raw_record STRING, _source_file STRING, _ingested_at TIMESTAMP")
    customers = spark.createDataFrame([("C1", "CA")], "customer_id STRING, state STRING")
    products = spark.createDataFrame([("P1", "Classic Novel", "Books")], "product_id STRING, product_name STRING, category STRING")

    out = transform_orders(bronze, customers, products, DS)

    assert _quarantined(out.quarantine) == [
        (None, ["malformed_json"]),
        (None, ["null_order_id"]),
        ("O1", ["duplicate_order_id"]),
        ("O10", ["invalid_line_item", "null_customer_id"]),
        ("O2", ["null_customer_id"]),
        ("O3", ["unknown_customer_id"]),
        ("O4", ["invalid_line_item"]),
        ("O5", ["invalid_order_ts"]),
        ("O6", ["order_date_outside_batch"]),
        ("O7", ["unknown_product_id"]),
        ("O8", ["no_line_items"]),
    ]
    assert {r.source_table for r in out.quarantine.collect()} == {"orders"}
    assert out.quarantine.where(F.col("raw_record").isNull()).count() == 0  # raw form kept for debugging

    [order] = out.orders.collect()
    assert (order.order_id, order.order_date, order.item_count, order.order_total) == ("O1", DS, 1, Decimal("25.00"))
    [line] = out.order_lines.collect()
    assert (line.product_name, line.category, line.customer_state, line.line_total) == ("Classic Novel", "Books", "CA", Decimal("25.00"))


# --- transform_customers on fixture rows --------------------------------------

def test_transform_customers_dedupes_to_latest_and_quarantines_bad_rows(spark):
    def row(cid, city, updated, signup="2025-01-01"):
        return (cid, "Ava", "Lee", " Ava@Example.com ", city, "TX", signup, updated, SOURCE)

    bronze = spark.createDataFrame(
        [
            row("C1", "Austin", "2026-01-01"),
            row("C1", "Austin", "2026-01-01"),   # exact duplicate
            row("C2", "Austin", "2026-01-01"),   # older version...
            row("C2", "Denver", "2026-02-01"),   # ...and the newer one that should win
            row(None, "Austin", "2026-01-01"),
            row("C3", "Austin", "2026-01-01", signup="2026-13-45"),
        ],
        "customer_id STRING, first_name STRING, last_name STRING, email STRING, city STRING, state STRING, "
        "signup_date STRING, updated_date STRING, _source_file STRING",
    )
    out = transform_customers(bronze, DS)

    clean = {r.customer_id: r for r in out.clean.collect()}
    assert set(clean) == {"C1", "C2"}
    assert clean["C2"].city == "Denver"
    assert clean["C1"].email == "ava@example.com"
    assert clean["C1"].signup_date == date(2025, 1, 1)
    assert _quarantined(out.quarantine) == [
        ("C1", ["duplicate_customer_id"]),
        ("C2", ["duplicate_customer_id"]),
        ("C3", ["invalid_signup_date"]),
        (None, ["null_customer_id"]),
    ]


# --- end to end: generator -> bronze -> silver --------------------------------

# Which quarantine reason each injected generator defect should produce.
DEFECT_REASON = {
    "null_customer_id": "null_customer_id",
    "orphan_customer_id": "unknown_customer_id",
    "negative_unit_price": "invalid_line_item",
    "invalid_order_ts": "invalid_order_ts",
    "duplicate_order_id": "duplicate_order_id",
}


def test_every_bronze_row_lands_in_silver_or_quarantine(spark, pipeline):
    lake, manifest = pipeline.lake, pipeline.manifests[DS]

    quarantine = read_delta(spark, lake.table_path("silver", "quarantine")).where(F.col("ingest_date") == F.lit(DS))
    reasons = {
        (r.source_table, r.reason): r["count"]
        for r in quarantine.select("source_table", F.explode("reject_reasons").alias("reason")).groupBy("source_table", "reason").count().collect()
    }
    defects = manifest["injected_defects"]
    expected = {("orders", DEFECT_REASON[d]): n for d, n in defects["orders"].items()}
    expected[("customers", "duplicate_customer_id")] = defects["customers"]["duplicate_customer_id"]
    assert reasons == expected

    orders = read_delta(spark, lake.table_path("silver", "orders")).where(F.col("order_date") == F.lit(DS))
    lines = read_delta(spark, lake.table_path("silver", "order_lines")).where(F.col("order_date") == F.lit(DS))
    assert orders.count() + quarantine.where("source_table = 'orders'").count() == manifest["row_counts"]["orders"]
    assert lines.count() == orders.agg(F.sum("item_count")).first()[0]
    customers = read_delta(spark, lake.table_path("silver", "customers"))
    assert customers.count() == customers.select("customer_id").distinct().count()


def test_silver_metrics_account_for_every_row(spark, pipeline):
    from jobs.silver_transform import summarize

    m = {(x.metric, x.table_name): x.value for x in summarize(spark, pipeline.lake, DS) if x.detail is None}
    for entity in ("customers", "products", "orders"):
        table = f"silver.{entity}"
        assert m[("rows_in", table)] == m[("rows_out", table)] + m[("rows_quarantined", table)]
    assert m[("rows_in", "silver.orders")] == pipeline.manifests[DS]["row_counts"]["orders"]


def test_customer_merge_applies_moves_and_ignores_older_snapshots(spark, lake, load_day):
    late, early = date(2026, 12, 31), date(2026, 3, 1)
    load_day(late, orders=20)
    load_day(early, orders=20)  # a backfill of an earlier date, after the later one

    expected = {c["customer_id"]: c for c in generate_customers(GeneratorConfig(run_date=late, **SMALL_BATCH))[0]}
    moved_after_early = [c for c in expected.values() if c["updated_date"] > early.isoformat() and c["signup_date"] <= early.isoformat()]
    assert moved_after_early, "fixture should include customers who moved between the two dates"

    silver = {r.customer_id: r for r in read_delta(spark, lake.table_path("silver", "customers")).collect()}
    assert set(silver) == set(expected)
    for cid, c in expected.items():
        assert (silver[cid].city, silver[cid].updated_date.isoformat()) == (c["city"], c["updated_date"])
