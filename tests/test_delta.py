from datetime import date

import pytest

from lakehouse_common.delta import merge_upsert, overwrite_partition, read_delta

SCHEMA = "id INT, v STRING, day DATE"
D1, D2 = date(2026, 3, 1), date(2026, 3, 2)


def _rows(spark, path):
    return sorted(tuple(r) for r in read_delta(spark, path).collect())


def test_overwrite_partition_replaces_only_that_partition(spark, tmp_path):
    path = str(tmp_path / "t")
    write = lambda rows, day: overwrite_partition(
        spark.createDataFrame(rows, SCHEMA), path, replace_where=f"day = '{day}'", partition_by=["day"]
    )
    write([(1, "a", D1), (2, "b", D1)], D1)  # creates the table
    write([(3, "c", D2)], D2)
    write([(1, "A", D1)], D1)  # re-run D1 with different data

    assert _rows(spark, path) == [(1, "A", D1), (3, "c", D2)]


def test_overwrite_partition_is_idempotent(spark, tmp_path):
    path = str(tmp_path / "t")
    df = spark.createDataFrame([(1, "a", D1), (2, "b", D1)], SCHEMA)
    for _ in range(3):
        overwrite_partition(df, path, replace_where=f"day = '{D1}'", partition_by=["day"])
    assert len(_rows(spark, path)) == 2


def test_overwrite_partition_rejects_rows_outside_the_predicate(spark, tmp_path):
    path = str(tmp_path / "t")
    overwrite_partition(spark.createDataFrame([(1, "a", D1)], SCHEMA), path, replace_where=f"day = '{D1}'", partition_by=["day"])
    # Delta enforces replaceWhere like a CHECK constraint on the rows being written.
    with pytest.raises(Exception, match="DELTA_VIOLATE_CONSTRAINT"):
        overwrite_partition(spark.createDataFrame([(2, "b", D2)], SCHEMA), path, replace_where=f"day = '{D1}'", partition_by=["day"])
    assert _rows(spark, path) == [(1, "a", D1)]  # the failed write changed nothing


def test_merge_upsert_inserts_updates_and_respects_condition(spark, tmp_path):
    path = str(tmp_path / "t")
    schema = "id INT, v STRING, version INT"
    merge_upsert(spark, spark.createDataFrame([(1, "a", 1), (2, "b", 1)], schema), path, ["id"])  # first load creates the table

    updates = spark.createDataFrame([(1, "a2", 2), (2, "stale", 0), (3, "c", 1)], schema)
    merge_upsert(spark, updates, path, ["id"], update_condition="s.version > t.version")
    assert _rows(spark, path) == [(1, "a2", 2), (2, "b", 1), (3, "c", 1)]  # id 2 kept: the update was older

    merge_upsert(spark, updates, path, ["id"], update_condition="s.version > t.version")  # re-run: no change
    assert _rows(spark, path) == [(1, "a2", 2), (2, "b", 1), (3, "c", 1)]
