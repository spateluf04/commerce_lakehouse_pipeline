"""Delta Lake read/write patterns shared by every job.

Every write here is idempotent: running it again with the same input leaves the table in the same
state. That is what makes Airflow retries and backfills safe.
"""
from __future__ import annotations

from collections.abc import Sequence

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession


def read_delta(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.format("delta").load(path)


def overwrite_partition(df: DataFrame, path: str, *, replace_where: str, partition_by: Sequence[str]) -> None:
    """Atomically replace the rows that match replace_where with df.

    This is what makes a daily load idempotent: re-running a date replaces that date's rows instead
    of appending a second copy. Delta also checks that every row in df matches replace_where, so a
    bug can't silently overwrite other dates. The table is created on the first write.
    """
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", replace_where)
        .partitionBy(*partition_by)
        .save(path)
    )


def overwrite_table(df: DataFrame, path: str) -> None:
    """Replace the whole table. Fine for small tables that are rebuilt from scratch on every run."""
    df.write.format("delta").mode("overwrite").save(path)


def merge_upsert(
    spark: SparkSession,
    df: DataFrame,
    path: str,
    keys: Sequence[str],
    *,
    update_condition: str | None = None,
) -> None:
    """Upsert df into the table at path: update rows whose keys match, insert the rest.

    update_condition (SQL, `t` = target and `s` = source) limits which matched rows get updated.
    For example, "s.updated_date > t.updated_date" stops an old snapshot from overwriting newer data
    when dates are backfilled out of order. df must have at most one row per key: Delta refuses to
    MERGE when several source rows match the same target row.
    """
    if not DeltaTable.isDeltaTable(spark, path):
        df.write.format("delta").save(path)
        return
    on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    (
        DeltaTable.forPath(spark, path).alias("t")
        .merge(df.alias("s"), on)
        .whenMatchedUpdateAll(condition=update_condition)
        .whenNotMatchedInsertAll()
        .execute()
    )
