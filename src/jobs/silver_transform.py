"""Silver: typed, validated, deduplicated and joined tables, plus a quarantine table.

For one business date (ds):
  customers, products   that day's full snapshots, MERGEd (upserted) into silver
  orders                one row per order, overwriting the order_date = ds partition
  order_lines           one row per line item, joined with product and customer attributes
  quarantine            every rejected row, its raw form, and all the reasons it was rejected

No row is dropped silently. Every bronze row ends up either in a silver table or in quarantine,
and the tests check that the counts reconcile with the generator's manifest.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from lakehouse_common.config import LakehouseConfig
from lakehouse_common.delta import merge_upsert, overwrite_partition, read_delta
from lakehouse_common.metrics import Metric
from lakehouse_common.runner import run_job

MONEY = T.DecimalType(12, 2)  # money is decimal, never float: 0.1 + 0.2 != 0.3 in floating point
TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'"
ORDER_SCHEMA = T.StructType([
    T.StructField("order_id", T.StringType()),
    T.StructField("customer_id", T.StringType()),
    T.StructField("order_ts", T.StringType()),
    T.StructField("status", T.StringType()),
    T.StructField("payment_method", T.StringType()),
    T.StructField("currency", T.StringType()),
    T.StructField("line_items", T.ArrayType(T.StructType([
        T.StructField("line_number", T.IntegerType()),
        T.StructField("product_id", T.StringType()),
        T.StructField("quantity", T.IntegerType()),
        T.StructField("unit_price", T.DecimalType(10, 2)),
    ]))),
])
CUSTOMER_COLUMNS = ("customer_id", "first_name", "last_name", "email", "city", "state", "signup_date", "updated_date")
PRODUCT_COLUMNS = ("product_id", "product_name", "category", "list_price")
ORDER_COLUMNS = ("order_id", "customer_id", "order_ts", "order_date", "status", "payment_method", "currency", "item_count", "order_total")
# Update a product only if something about it changed. <=> is null-safe equality.
PRODUCT_CHANGED = "NOT (t.product_name <=> s.product_name AND t.category <=> s.category AND t.list_price <=> s.list_price)"


@dataclass
class SilverOutput:
    clean: DataFrame
    quarantine: DataFrame


@dataclass
class SilverOrders:
    orders: DataFrame
    order_lines: DataFrame
    quarantine: DataFrame


def _reasons(*checks: tuple[Column, str]) -> Column:
    """An array of the reason codes whose condition is true. A clean row gets an empty array."""
    return F.array_compact(F.array(*[F.when(condition, F.lit(reason)) for condition, reason in checks]))


def _mark_duplicates(df: DataFrame, key: str, order_by: list, reason: str) -> DataFrame:
    """Among rows that are otherwise clean, keep the first row per key and reject the rest with `reason`."""
    is_clean = F.size("reject_reasons") == 0
    rank = F.row_number().over(Window.partitionBy(F.col(key), is_clean).orderBy(*order_by))
    return df.withColumn("reject_reasons", F.when(is_clean & (rank > 1), F.array(F.lit(reason))).otherwise(F.col("reject_reasons")))


def _split(flagged: DataFrame, source_table: str, key: str, ds: date) -> tuple[DataFrame, DataFrame]:
    """Split rows into (clean, quarantine-shaped rejected)."""
    clean = flagged.where(F.size("reject_reasons") == 0)
    rejected = flagged.where(F.size("reject_reasons") > 0).select(
        F.lit(source_table).alias("source_table"),
        F.col(key).alias("record_key"),
        "reject_reasons",
        "raw_record",
        "_source_file",
        F.lit(ds).alias("ingest_date"),
        F.current_timestamp().alias("_quarantined_at"),
    )
    return clean, rejected


def transform_customers(bronze: DataFrame, ds: date) -> SilverOutput:
    typed = bronze.select(
        "customer_id", "first_name", "last_name",
        F.lower(F.trim("email")).alias("email"),
        "city", "state",
        F.to_date("signup_date", "yyyy-MM-dd").alias("signup_date"),
        F.to_date("updated_date", "yyyy-MM-dd").alias("updated_date"),
        F.to_json(F.struct(*CUSTOMER_COLUMNS)).alias("raw_record"),
        "_source_file",
    )
    flagged = typed.withColumn("reject_reasons", _reasons(
        (F.col("customer_id").isNull(), "null_customer_id"),
        (F.col("signup_date").isNull(), "invalid_signup_date"),
        (F.col("updated_date").isNull(), "invalid_updated_date"),
    ))
    # If a customer appears more than once, keep the most recently updated version.
    flagged = _mark_duplicates(flagged, "customer_id", [F.col("updated_date").desc(), "raw_record"], "duplicate_customer_id")
    clean, rejected = _split(flagged, "customers", "customer_id", ds)
    return SilverOutput(clean.select(*CUSTOMER_COLUMNS, F.current_timestamp().alias("_loaded_at")), rejected)


def transform_products(bronze: DataFrame, ds: date) -> SilverOutput:
    typed = bronze.select(
        "product_id", "product_name", "category",
        F.col("list_price").cast(T.DecimalType(10, 2)).alias("list_price"),
        F.to_json(F.struct(*PRODUCT_COLUMNS)).alias("raw_record"),
        "_source_file",
    )
    flagged = typed.withColumn("reject_reasons", _reasons(
        (F.col("product_id").isNull(), "null_product_id"),
        (F.col("category").isNull(), "null_category"),
        (F.col("list_price").isNull() | (F.col("list_price") < 0), "invalid_list_price"),
    ))
    flagged = _mark_duplicates(flagged, "product_id", ["raw_record"], "duplicate_product_id")
    clean, rejected = _split(flagged, "products", "product_id", ds)
    return SilverOutput(clean.select(*PRODUCT_COLUMNS, F.current_timestamp().alias("_loaded_at")), rejected)


def transform_orders(bronze: DataFrame, customers: DataFrame, products: DataFrame, ds: date) -> SilverOrders:
    """Validate one day's orders against silver customers and products.

    `customers` and `products` are the full silver tables, so referential integrity is checked
    against every customer and product known so far, not just today's snapshot.
    """
    o = F.col("o")
    parsed = bronze.select(
        "raw_record", "_source_file", "_ingested_at",
        # get_json_object returns null when the line isn't valid JSON at all.
        F.get_json_object("raw_record", "$").isNull().alias("_malformed"),
        F.from_json("raw_record", ORDER_SCHEMA).alias("o"),
    ).select(
        "raw_record", "_source_file", "_ingested_at", "_malformed",
        o.order_id.alias("order_id"),
        o.customer_id.alias("customer_id"),
        F.to_timestamp(o.order_ts, TS_FORMAT).alias("order_ts"),
        o.status.alias("status"),
        o.payment_method.alias("payment_method"),
        o.currency.alias("currency"),
        o.line_items.alias("line_items"),
    ).withColumn("order_date", F.to_date("order_ts"))

    # Orders with at least one line item for a product that isn't in silver products.
    unknown_product = (
        parsed.select("order_id", F.explode("line_items.product_id").alias("product_id"))
        .where(F.col("product_id").isNotNull())
        .join(products.select("product_id"), "product_id", "left_anti")
        .select("order_id").distinct()
        .withColumn("_unknown_product", F.lit(True))
    )
    known_customer = customers.select("customer_id", F.lit(True).alias("_known_customer"))
    enriched = parsed.join(unknown_product, "order_id", "left").join(known_customer, "customer_id", "left")

    parsed_ok = ~F.col("_malformed")  # a malformed line gets only the one reason, malformed_json
    bad_line = F.exists(
        "line_items",
        lambda li: li.product_id.isNull() | li.quantity.isNull() | (li.quantity <= 0) | li.unit_price.isNull() | (li.unit_price < 0),
    )
    flagged = enriched.withColumn("reject_reasons", _reasons(
        (F.col("_malformed"), "malformed_json"),
        (parsed_ok & F.col("order_id").isNull(), "null_order_id"),
        (parsed_ok & F.col("customer_id").isNull(), "null_customer_id"),
        (F.col("customer_id").isNotNull() & F.col("_known_customer").isNull(), "unknown_customer_id"),
        (parsed_ok & F.col("order_ts").isNull(), "invalid_order_ts"),
        # The replaceWhere overwrite below only accepts rows dated ds. Late or early orders are rejected, not loaded.
        (F.col("order_date") != F.lit(ds), "order_date_outside_batch"),
        (parsed_ok & (F.col("line_items").isNull() | (F.size("line_items") == 0)), "no_line_items"),
        (bad_line, "invalid_line_item"),
        (F.col("_unknown_product").isNotNull(), "unknown_product_id"),
    ))
    # The same order sent twice: keep the first copy received, quarantine the others.
    flagged = _mark_duplicates(flagged, "order_id", ["_ingested_at", "raw_record"], "duplicate_order_id")
    clean, rejected = _split(flagged, "orders", "order_id", ds)

    li = F.col("li")
    order_lines = (
        clean.select("order_id", "order_date", "order_ts", F.col("status").alias("order_status"), "customer_id",
                     F.explode("line_items").alias("li"))
        .select(
            "order_id", li.line_number.alias("line_number"), "order_date", "order_ts", "order_status", "customer_id",
            li.product_id.alias("product_id"), li.quantity.alias("quantity"), li.unit_price.alias("unit_price"),
            (li.quantity * li.unit_price).cast(MONEY).alias("line_total"),
        )
        # Inner joins are safe here: orders with unknown products or customers were rejected above.
        .join(products.select("product_id", "product_name", "category"), "product_id")
        .join(customers.select("customer_id", F.col("state").alias("customer_state")), "customer_id")
        .select(
            "order_id", "line_number", "order_date", "order_ts", "order_status", "customer_id", "customer_state",
            "product_id", "product_name", "category", "quantity", "unit_price", "line_total",
        )
    )
    totals = order_lines.groupBy("order_id").agg(
        F.count(F.lit(1)).alias("item_count"),
        F.sum("line_total").cast(MONEY).alias("order_total"),
    )
    orders = clean.join(totals, "order_id").select(
        *ORDER_COLUMNS, "_source_file", "_ingested_at", F.current_timestamp().alias("_processed_at"),
    )
    return SilverOrders(orders, order_lines, rejected)


def run(spark: SparkSession, config: LakehouseConfig, ds: date) -> list[Metric]:
    day = f"'{ds.isoformat()}'"

    def bronze(table: str) -> DataFrame:
        return read_delta(spark, config.table_path("bronze", table)).where(F.col("ingest_date") == F.lit(ds))

    customers_path = config.table_path("silver", "customers")
    products_path = config.table_path("silver", "products")

    customers = transform_customers(bronze("customers"), ds)
    merge_upsert(spark, customers.clean, customers_path, ["customer_id"], update_condition="s.updated_date > t.updated_date")
    products = transform_products(bronze("products"), ds)
    merge_upsert(spark, products.clean, products_path, ["product_id"], update_condition=PRODUCT_CHANGED)

    orders = transform_orders(bronze("orders"), read_delta(spark, customers_path), read_delta(spark, products_path), ds)
    overwrite_partition(orders.orders, config.table_path("silver", "orders"), replace_where=f"order_date = {day}", partition_by=["order_date"])
    overwrite_partition(orders.order_lines, config.table_path("silver", "order_lines"), replace_where=f"order_date = {day}", partition_by=["order_date"])

    quarantine = customers.quarantine.unionByName(products.quarantine).unionByName(orders.quarantine)
    overwrite_partition(quarantine, config.table_path("silver", "quarantine"), replace_where=f"ingest_date = {day}", partition_by=["ingest_date"])
    return summarize(spark, config, ds)


def summarize(spark: SparkSession, config: LakehouseConfig, ds: date) -> list[Metric]:
    """Row counts for ds, read back from the tables that were written.

    For customers, products and orders, rows_in = rows_out + rows_quarantined: every bronze row is
    accounted for. Customers and products are compared with that day's snapshot, not with the
    whole MERGEd table.
    """
    def table(layer: str, name: str):
        return read_delta(spark, config.table_path(layer, name))

    def on_ds(df, col):
        return df.where(F.col(col) == F.lit(ds))

    quarantine = on_ds(table("silver", "quarantine"), "ingest_date")
    quarantined = {r.source_table: r["count"] for r in quarantine.groupBy("source_table").count().collect()}
    by_reason = (
        quarantine.select("source_table", F.explode("reject_reasons").alias("reason"))
        .groupBy("source_table", "reason").count().collect()
    )

    metrics = []
    for entity in ("customers", "products", "orders"):
        rows_in = on_ds(table("bronze", entity), "ingest_date").count()
        rejected = quarantined.get(entity, 0)
        if entity == "orders":
            rows_out = on_ds(table("silver", "orders"), "order_date").count()
        else:
            rows_out = rows_in - rejected  # MERGE source rows; the target table also holds earlier days
        metrics += [
            Metric("rows_in", rows_in, f"silver.{entity}"),
            Metric("rows_out", rows_out, f"silver.{entity}"),
            Metric("rows_quarantined", rejected, f"silver.{entity}"),
        ]
    metrics.append(Metric("rows_out", on_ds(table("silver", "order_lines"), "order_date").count(), "silver.order_lines"))
    metrics += [Metric("rows_quarantined_by_reason", r["count"], f"silver.{r.source_table}", detail=r.reason) for r in by_reason]
    return sorted(metrics, key=lambda m: (m.metric, m.table_name or "", m.detail or ""))


if __name__ == "__main__":
    raise SystemExit(run_job("silver_transform", run))
