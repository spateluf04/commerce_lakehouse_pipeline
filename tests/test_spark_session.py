"""Checks that the pinned pyspark, delta-spark and jar versions actually work together."""
from delta.tables import DeltaTable

from lakehouse_common.config import LakehouseConfig
from lakehouse_common.spark import spark_conf


def test_s3a_settings_only_when_a_root_is_on_s3(tmp_path):
    local = LakehouseConfig(storage_root=str(tmp_path), landing_dir=str(tmp_path))
    assert not any(k.startswith("spark.hadoop.fs.s3a") for k in spark_conf(local, {}))

    s3 = LakehouseConfig(storage_root="s3a://bucket/lakehouse", landing_dir=str(tmp_path))
    conf = spark_conf(s3, {"AWS_REGION": "us-west-2"})
    assert conf["spark.hadoop.fs.s3a.aws.credentials.provider"] == "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
    assert conf["spark.hadoop.fs.s3a.endpoint.region"] == "us-west-2"
    assert not any("secret" in k.lower() or "access.key" in k.lower() for k in conf)  # no credentials in config


def test_jar_source(tmp_path):
    cfg = LakehouseConfig(storage_root=str(tmp_path), landing_dir=str(tmp_path))
    assert "spark.jars.packages" in spark_conf(cfg, {})
    conf = spark_conf(cfg, {"SPARK_EXTRA_JARS": "/opt/spark-jars/a.jar"})
    assert conf["spark.jars"] == "/opt/spark-jars/a.jar" and "spark.jars.packages" not in conf


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
