"""Checks that the pinned pyspark, delta-spark and jar versions actually work together."""
from delta.tables import DeltaTable


def test_session_settings(spark):
    assert spark.version.startswith("3.5.")
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"
    assert spark.conf.get("spark.sql.shuffle.partitions") == "1"


def test_delta_write_read_and_merge(spark, tmp_path):
    path = str(tmp_path / "t")
    spark.createDataFrame([(1, "a"), (2, "b")], "id INT, v STRING").write.format("delta").save(path)

    updates = spark.createDataFrame([(2, "B"), (3, "c")], "id INT, v STRING")
    (
        DeltaTable.forPath(spark, path).alias("t")
        .merge(updates.alias("s"), "t.id = s.id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    rows = sorted(tuple(r) for r in spark.read.format("delta").load(path).collect())
    assert rows == [(1, "a"), (2, "B"), (3, "c")]
