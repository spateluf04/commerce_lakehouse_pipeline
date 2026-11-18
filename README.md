# Commerce Data Lakehouse Pipeline

A daily batch pipeline for synthetic e-commerce data: a Python generator, a Bronze/Silver/Gold
Delta Lake medallion on S3 (or the local filesystem), PySpark jobs orchestrated by Airflow,
and Terraform for the AWS resources.

> Work in progress, built in stages. Stage 1 so far: the data generator, the shared library and tests.

## Prerequisites

- Docker Desktop with the WSL2 backend. Spark, Airflow and the tests all run inside one image.

## Build the image

```powershell
docker build -f docker/Dockerfile -t commerce-lakehouse:dev .
```

## Run the tests

```powershell
docker run --rm -v "${PWD}:/opt/project" commerce-lakehouse:dev python -m pytest
```

## Generate a day of data

```powershell
docker run --rm -v "${PWD}:/opt/project" commerce-lakehouse:dev python -m generator --date 2026-03-01
```

This writes to `data/landing/` (gitignored):

```
data/landing/
├── customers/dt=2026-03-01/customers.csv     full customer snapshot
├── products/dt=2026-03-01/products.csv       product catalog
├── orders/dt=2026-03-01/orders.jsonl         the day's orders, line items nested
└── _manifests/dt=2026-03-01/manifest.json    row counts and the defects that were injected
```

Volume is configurable: `--orders`, `--base-customers`, `--new-customers-per-day`, `--products`,
`--bad-rate` (see `--help`). Output is deterministic: re-running a date rewrites identical files.

About 2% of records have a deliberate defect, so the data quality layer has something to catch:
null customer IDs, orders that point to customers who don't exist, negative unit prices,
timestamps that won't parse, orders sent twice, and duplicate customer rows.
