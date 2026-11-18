"""Bronze: land one day's raw files in Delta exactly as received, plus ingestion metadata.

Nothing is parsed, typed or filtered here. CSV columns stay strings and each order stays its raw
JSON line. If silver has a bug, it can be fixed and re-run from bronze without going back to the
source system.
"""
from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse_common.config import LakehouseConfig
from lakehouse_common.delta import overwrite_partition, read_delta
from lakehouse_common.metrics import Metric
from lakehouse_common.runner import run_job

SOURCES = {"customers": "csv", "products": "csv", "orders": "jsonl"}


def read_landing(spark: SparkSession, landing_root: str, entity: str, ds: date) -> DataFrame:
    path = f"{landing_root}/{entity}/dt={ds.isoformat()}"
    if SOURCES[entity] == "jsonl":
        reader = spark.read.text(path)  # one row per line, column "value"
        return reader.select(F.col("value").alias("raw_record"), F.col("_metadata.file_path").alias("_source_file"))
    # No inferSchema: every column is read as the string in the file, and silver does the typing.
    reader = spark.read.option("header", True).csv(path)
    return reader.select("*", F.col("_metadata.file_path").alias("_source_file"))


def to_bronze(df: DataFrame, ds: date) -> DataFrame:
    return df.withColumn("_ingested_at", F.current_timestamp()).withColumn("ingest_date", F.lit(ds))


def run(spark: SparkSession, config: LakehouseConfig, ds: date) -> list[Metric]:
    metrics = []
    for entity in SOURCES:
        path = config.table_path("bronze", entity)
        landed = read_landing(spark, config.landing_root, entity, ds)
        overwrite_partition(to_bronze(landed, ds), path, replace_where=f"ingest_date = '{ds.isoformat()}'", partition_by=["ingest_date"])
        table = f"bronze.{entity}"
        metrics += [
            Metric("rows_in", landed.count(), table),
            Metric("rows_out", read_delta(spark, path).where(F.col("ingest_date") == F.lit(ds)).count(), table),
        ]
    return metrics


if __name__ == "__main__":
    raise SystemExit(run_job("bronze_ingest", run))
