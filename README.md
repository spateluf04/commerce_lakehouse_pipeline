# Commerce Data Lakehouse Pipeline

A daily batch pipeline for e-commerce data, built on a medallion (bronze/silver/gold) lakehouse:

- **PySpark + Delta Lake** for processing and storage
- **Airflow** for orchestration, with retries, alerting and backfills
- **AWS S3** for storage, provisioned with **Terraform** (or the local filesystem for development)
- **Data quality checks**, a **quarantine** table, and per-run **metrics**, all through one shared Python library

Everything runs on a laptop in Docker. The only cloud resources are an S3 bucket and an IAM user,
which cost close to nothing (see [Cost](#cost-and-teardown)).

## Architecture

```mermaid
flowchart LR
    subgraph Airflow["Airflow DAG: commerce_lakehouse_daily (daily, backfill-capable)"]
        direction LR
        G[generate_source_data] --> B[bronze_ingest] --> S[silver_transform] --> Q{quality_gate} --> GD[gold_build]
    end

    GEN["Python generator<br/>(stdlib only)"] -->|"CSV + JSON Lines<br/>dt=YYYY-MM-DD"| LZ[("Landing zone<br/>local dir or s3a://…/landing")]
    LZ --> BR

    subgraph Lakehouse["Delta Lake on S3 (s3a://…/lakehouse) or local filesystem"]
        BR[("bronze<br/>orders · customers · products<br/>raw + _source_file, _ingested_at")]
        SV[("silver<br/>orders · order_lines · customers · products")]
        QT[("silver.quarantine<br/>rejected rows + reasons")]
        GL[("gold<br/>daily_category_revenue<br/>top_products_daily<br/>customer_lifetime_value")]
        OPS[("ops.pipeline_run_metrics")]
        BR -->|"type · validate · dedupe · join"| SV
        BR -->|bad rows| QT
        SV -->|Spark SQL| GL
    end

    Q -. "error check fails → stop + alert" .-> AL["on_failure_callback<br/>log + optional Slack"]
    Airflow -. "every job: rows in/out, quarantined,<br/>check results, duration" .-> OPS
```

Every task runs `python -m <job> --ds {{ ds }}` and uses the shared library in
[`src/lakehouse_common/`](src/lakehouse_common/):

| Module | What it does |
|---|---|
| `config.py` | Builds every table and landing path from `LAKEHOUSE_STORAGE_ROOT` / `LAKEHOUSE_LANDING_ROOT` |
| `spark.py` | The only SparkSession builder: Delta, UTC, S3A, jars |
| `delta.py` | Idempotent writes: `overwrite_partition` (replaceWhere) and `merge_upsert` (MERGE) |
| `quality.py` | `not_null`, `unique`, `referential_integrity`, `value_range`, `row_count` |
| `metrics.py` | Writes `ops.pipeline_run_metrics` |
| `runner.py` | Job entry point: parse `--ds`, time the run, record metrics, stop Spark |
| `log.py` | JSON-lines structured logging |
| `alerts.py` | Failure alerts: log, plus Slack if a webhook is set |

## Stack and versions

| Component | Version | Note |
|---|---|---|
| Python | 3.11 | |
| Apache Spark (PySpark) | 3.5.9 | local mode, Scala 2.12, bundles Hadoop 3.3.4 |
| Delta Lake (`delta-spark`) | 3.3.3 | built for Spark 3.5 |
| `hadoop-aws` / `aws-java-sdk-bundle` | 3.3.4 / 1.12.262 | must match Spark's bundled Hadoop exactly |
| Apache Airflow | 3.3.2 | LocalExecutor, Postgres 16 for metadata |
| Java | Temurin 17 | |
| Terraform / AWS provider | ≥ 1.6 / ~> 6.0 | |

The versions are pinned in `requirements.txt`, `docker/Dockerfile` and `infra/terraform/versions.tf`.

## Repository layout

```
├── dags/commerce_lakehouse_daily.py   Airflow DAG (orchestration only)
├── src/
│   ├── generator/                     synthetic source data (stdlib only)
│   ├── jobs/                          bronze_ingest, silver_transform, quality_gate, gold_build, upload_landing
│   │   └── sql/gold/*.sql             gold business logic as plain Spark SQL
│   └── lakehouse_common/              shared library (see above)
├── tests/                             pytest, local filesystem only, no AWS needed
├── infra/terraform/                   S3 bucket + least-privilege IAM
├── docker/Dockerfile                  one image for Airflow, Spark jobs and tests
├── docker-compose.yml                 Postgres + Airflow
└── docs/standards.md                  naming, partitioning and data quality rules
```

## Setup

You only need Docker Desktop (with the WSL2 backend on Windows). Terraform and the AWS CLI are
only needed for the S3 setup.

```powershell
git clone <repo> ; cd commerce-lakehouse-pipeline
copy .env.example .env          # defaults to the local filesystem
docker build -f docker/Dockerfile -t commerce-lakehouse:dev .
```

## Tests

```powershell
docker run --rm -v "${PWD}:/opt/project" commerce-lakehouse:dev python -m pytest                       # everything (~10 min)
docker run --rm -v "${PWD}:/opt/project" commerce-lakehouse:dev python -m pytest -m "not integration"  # quick (~3 min)
```

The tests use small fixture DataFrames and the local filesystem only. They never touch AWS. They cover:

- the generator: deterministic output, volumes, exact counts of injected defects
- every data quality check
- each transformation: bronze → silver (every defect goes to quarantine), silver → gold (hand-checked numbers)
- idempotent reruns and MERGE behaviour on backfills
- the DAG's structure, the alert callback, metrics and S3A settings

The `integration` tests run whole pipeline days through Spark, so they're slow.

## Run locally

### With Airflow (the normal way)

```powershell
docker compose up -d --build    # wait ~1 min, then open http://localhost:8080 (no login locally)
```

Turn on `commerce_lakehouse_daily` in the UI, or trigger one day from the CLI:

```powershell
docker compose exec airflow airflow dags trigger commerce_lakehouse_daily --logical-date 2026-03-01T00:00:00+00:00
```

### Without Airflow (one job at a time)

```powershell
$ds = "2026-03-01"
docker run --rm -v "${PWD}:/opt/project" commerce-lakehouse:dev python -m generator --date $ds
foreach ($job in "bronze_ingest", "silver_transform", "quality_gate", "gold_build") {
    docker run --rm -v "${PWD}:/opt/project" commerce-lakehouse:dev python -m jobs.$job --ds $ds
}
```

The output lands in `data/landing/` and `data/lakehouse/<layer>/<table>/`, both gitignored.

## Run on S3

1. **Provision the bucket and IAM user** (needs admin credentials for your own account, used only for Terraform):
   ```powershell
   cd infra/terraform
   copy terraform.tfvars.example terraform.tfvars
   terraform init ; terraform apply
   terraform output
   ```
2. **Create an access key for the pipeline user.** This isn't done in Terraform, because the secret would end up in the state file. Store the key in a profile:
   ```powershell
   aws iam create-access-key --user-name commerce-lakehouse-pipeline
   aws configure --profile commerce-lakehouse   # paste the key ID and secret
   ```
3. **Point the pipeline at S3** in `.env`, using the values from `terraform output`:
   ```
   LAKEHOUSE_STORAGE_ROOT=s3a://commerce-lakehouse-<account-id>/lakehouse
   LAKEHOUSE_LANDING_ROOT=s3a://commerce-lakehouse-<account-id>/landing
   AWS_PROFILE=commerce-lakehouse
   AWS_REGION=us-east-1
   ```
4. Run `docker compose up -d` and trigger the DAG as above. Docker Compose mounts `~/.aws` read-only, and no credential is ever copied into the image or the repo.

How S3 access works:

- Spark talks to S3 through Hadoop's **S3A** connector (`s3a://`, not `s3://`), configured in [`spark.py`](src/lakehouse_common/spark.py).
- `hadoop-aws` must be the same version as the Hadoop that's built into PySpark (3.3.4). It was compiled against one exact AWS SDK bundle (1.12.262).
- Credentials come from the AWS SDK's default chain: environment variables, then the `AWS_PROFILE` profile, then instance or container roles.
- Delta on S3 uses a single-writer log store. That's why the DAG sets `max_active_runs=1`. Running several writers at once would need the DynamoDB-backed log store.

## Backfills

The DAG uses `CronDataIntervalTimetable`, so each run processes one complete day: `ds` is the start
of the interval, and the run starts after the interval ends. `catchup=False`, so deploying the DAG
doesn't automatically start runs for every day since `start_date`. History is loaded on purpose:

```powershell
# Load 1–7 March 2026, one day at a time and in order (max_active_runs=1)
docker compose exec airflow airflow backfill create --dag-id commerce_lakehouse_daily --from-date 2026-03-01 --to-date 2026-03-07

# Re-process days that already succeeded, for example after fixing a silver bug
docker compose exec airflow airflow backfill create --dag-id commerce_lakehouse_daily --from-date 2026-03-01 --to-date 2026-03-07 --reprocess-behavior completed
```

Backfills and re-runs are safe because every write is idempotent for its `ds`:

- Partitions are replaced with `replaceWhere`.
- Dimension tables use MERGE, guarded so an older snapshot can't overwrite newer data. You can backfill March after December without losing changes.
- `customer_lifetime_value` is rebuilt from all of silver every time.

## Naming conventions and partitioning

The details are in [docs/standards.md](docs/standards.md). In short:

- Tables are `<layer>.<entity>` at `<root>/<layer>/<entity>`, in lower snake_case. This is enforced by `LakehouseConfig.table_path`.
- Columns added by the pipeline start with `_`. Money is `DECIMAL`. Timestamps are UTC.
- Fact tables are partitioned by date: `ingest_date` in bronze and quarantine, `order_date` in silver and gold.
- Dimension tables aren't partitioned, and are updated with MERGE.

## Observability

Every job appends to `ops.pipeline_run_metrics`, one row per measurement:

- `rows_in`, `rows_out`, `rows_quarantined` (plus a breakdown by reason)
- one row per quality `check`, with pass/fail
- `duration_seconds` and `job_status`

The Airflow run ID and try number are recorded too, so retries show up as separate attempts.
Failed runs record their metrics as well.

```powershell
docker run --rm -v "${PWD}:/opt/project" commerce-lakehouse:dev python -c "from lakehouse_common.spark import get_spark; s = get_spark('metrics'); s.read.format('delta').load('data/lakehouse/ops/pipeline_run_metrics').where(\"ds = '2026-03-01'\").orderBy('recorded_at').select('job','metric','table_name','value','status','detail').show(100, False)"
```

Alerting:

- Each task retries twice, with exponential backoff.
- After the last retry fails, `on_failure_callback` logs a structured alert.
- If `SLACK_WEBHOOK_URL` is set, the alert is also posted to Slack. A broken webhook never hides the real error.

## Cost and teardown

The lakehouse is a few MB per simulated day. To keep the bill at roughly zero:

- **Lifecycle rules:** landing files expire after 30 days, old object versions after 7 days, and failed multipart uploads after 1 day.
- **No paid services:** no NAT, no compute. Spark runs on your laptop. Encryption is SSE-S3 (free).
- **Optional budget alert:** set `budget_alert_email` in `terraform.tfvars` to get an email if spend is forecast to go over $1/month.
- **Free-tier terms vary** by account and sign-up date, so check yours in the AWS console.

When you're finished, delete everything:

```powershell
cd infra/terraform
terraform destroy        # force_destroy=true also empties the bucket, including old versions
```

Delete the pipeline user's access key first (`aws iam delete-access-key ...`), or Terraform can't delete the user.

## Design notes

- **Bronze stores raw data.** CSV columns stay strings and orders stay raw JSON lines. A silver bug is fixed by re-running from bronze, not by going back to the source.
- **Quarantine, not drop.** Every bronze row ends up in silver or in quarantine, with its reasons and raw form. The tests reconcile the quarantine counts against the generator's manifest of injected defects.
- **Two layers of data quality.** Silver applies row rules. The gate checks table-level properties (empty days, uniqueness, referential integrity) and stops gold from being built on bad data.
- **SQL for business logic.** Gold queries are `.sql` files with a `:ds` named parameter. Analysts can review them without reading Python.
- **One process per task.** Each Airflow task starts its own JVM, much like `spark-submit` on a cluster. The scheduler never loads Spark.
- **Known limits.** Spark runs in local mode with a single writer, and there's no schema evolution or SCD2 history. These are the next steps at production scale: a Databricks or EMR cluster, the DynamoDB log store, `OPTIMIZE`/`VACUUM` jobs, and SCD2 customers.
