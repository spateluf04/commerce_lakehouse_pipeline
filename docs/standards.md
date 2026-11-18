# Data standards

The conventions every job in this repo follows. They're enforced in code wherever that's possible.

## Table naming

| Rule | Example | Enforced by |
|---|---|---|
| Tables are named `<layer>.<entity>`, stored at `<storage_root>/<layer>/<entity>` | `silver.order_lines` → `s3a://bucket/lakehouse/silver/order_lines` | `LakehouseConfig.table_path` |
| Layers are `bronze`, `silver`, `gold`, plus `ops` for pipeline metadata | `ops.pipeline_run_metrics` | `config.LAYERS` |
| Entity names are lower snake_case, plural nouns for entities | `customers`, `order_lines` | regex in `table_path` |
| Gold tables are named after the business question they answer, with their grain as a suffix | `daily_category_revenue`, `top_products_daily` | review |
| In SQL, silver tables are temp views named `silver_<entity>`, never storage paths | `FROM silver_order_lines` | `gold_build.register_silver_views` |

Column conventions:

- Business keys keep their source names: `order_id`, `customer_id`, `product_id`.
- Columns added by the pipeline start with `_`: `_source_file`, `_ingested_at`, `_loaded_at`, `_processed_at`, `_quarantined_at`.
  The exception is partition columns. Spark treats directories whose names start with `_` as hidden, so `ingest_date` has no underscore.
- Dates are `DATE` and named `*_date`. Timestamps are UTC `TIMESTAMP` and named `*_ts` or `*_at`.
- Money is `DECIMAL(12,2)` in silver and `DECIMAL(18,2)` for gold aggregates. Never float.

## Partitioning

| Table | Partition column | Why |
|---|---|---|
| bronze.* | `ingest_date` | One partition per daily batch. A re-run replaces exactly that batch. |
| silver.orders, silver.order_lines | `order_date` | Most queries filter by business date, and each daily load rewrites one partition. |
| silver.quarantine | `ingest_date` | Reviewed one batch at a time; each re-run replaces its own batch. |
| silver.customers, silver.products | none | Small dimension tables, updated with MERGE. Partitioning them would only create small files. |
| gold.daily_category_revenue, gold.top_products_daily | `order_date` | Daily grain, rebuilt one day at a time. |
| gold.customer_lifetime_value | none | Rebuilt in full every run. |
| ops.pipeline_run_metrics | `ds` | Append-only history, filtered by run date. |

Rules:

- Partition on a low-cardinality date column only. Never partition on an ID: that creates millions of tiny directories.
- At this volume a partition holds about one file. At production scale, aim for files of roughly 128 MB to 1 GB and run `OPTIMIZE` on a schedule.
- Every write is idempotent:
  - Facts are written with `replaceWhere` on the partition column. Delta rejects any row outside the predicate, so a bug can't overwrite other dates.
  - Dimensions use `MERGE` on the business key, guarded by a condition such as `s.updated_date > t.updated_date`, so an out-of-order backfill can't regress newer data.

## Data quality rules

There are two layers, and every rejected row is kept along with its reasons.

**1. Row-level rules in silver.** A row that breaks a rule goes to `silver.quarantine` with all of its
`reject_reasons` and its `raw_record`, never silently dropped. `rows_in = rows_out + rows_quarantined`
is recorded for every table, every run.

| Entity | Reason codes |
|---|---|
| orders | `malformed_json`, `null_order_id`, `null_customer_id`, `unknown_customer_id`, `invalid_order_ts`, `order_date_outside_batch`, `no_line_items`, `invalid_line_item` (null product, quantity ≤ 0, or negative price), `unknown_product_id`, `duplicate_order_id` |
| customers | `null_customer_id`, `invalid_signup_date`, `invalid_updated_date`, `duplicate_customer_id` (the most recently updated copy is kept) |
| products | `null_product_id`, `null_category`, `invalid_list_price`, `duplicate_product_id` |

**2. Dataset-level checks in the quality gate**, which runs before gold (`jobs/quality_gate.py`):

| Check | Applied to | Severity |
|---|---|---|
| `row_count ≥ 1` | silver.orders and silver.order_lines for ds | error |
| `not_null` | order keys, timestamps, totals | error |
| `unique` | `orders.order_id`, `order_lines.(order_id, line_number)`, `customers.customer_id`, `products.product_id` | error |
| `referential_integrity` | orders → customers, order_lines → orders, order_lines → products | error |
| `value_range` | `quantity ≥ 1`, `unit_price ≥ 0`, `order_total ≥ 0` | error |
| `not_null(email)` | silver.customers | warn |

- An `error` failure fails the Airflow task, so gold isn't built from bad data, and the failure alert fires.
- A `warn` failure is recorded in `ops.pipeline_run_metrics` but doesn't stop the run.
- Every check result, passed or failed, is written to the metrics table.

**Business rule:** revenue excludes orders with status `cancelled` or `returned`.
