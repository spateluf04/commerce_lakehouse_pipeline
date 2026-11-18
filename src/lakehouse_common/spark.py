"""The one place a SparkSession is built, so every job gets the same Delta and storage settings."""
from __future__ import annotations

import os

from pyspark.sql import SparkSession

from lakehouse_common.config import LakehouseConfig

# Must match requirements.txt (pyspark/delta-spark) and the jar ARGs in docker/Dockerfile.
DELTA_VERSION = "3.3.3"
HADOOP_AWS_VERSION = "3.3.4"  # = the Hadoop version bundled inside pyspark 3.5
# Only used when the jars aren't already on disk. The Docker image sets SPARK_EXTRA_JARS, so this
# is a fallback for running outside it. hadoop-aws pulls in aws-java-sdk-bundle transitively.
_MAVEN_PACKAGES = (
    f"io.delta:delta-spark_2.12:{DELTA_VERSION}",
    f"org.apache.hadoop:hadoop-aws:{HADOOP_AWS_VERSION}",
)


def get_spark(
    app_name: str,
    config: LakehouseConfig | None = None,
    *,
    master: str | None = None,
    extra_conf: dict[str, str] | None = None,
) -> SparkSession:
    config = config or LakehouseConfig.from_env()
    builder = (
        SparkSession.builder.appName(app_name)
        .master(master or os.environ.get("SPARK_MASTER", "local[*]"))
        # These two settings turn on Delta: SQL syntax such as MERGE, and the Delta table catalog.
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Store and display timestamps in UTC, whatever time zone the machine running this is in.
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(config.shuffle_partitions))
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "2g"))
    )
    jars = os.environ.get("SPARK_EXTRA_JARS")
    if jars:
        builder = builder.config("spark.jars", jars)
    else:
        builder = builder.config("spark.jars.packages", ",".join(_MAVEN_PACKAGES))
    for key, value in (extra_conf or {}).items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
