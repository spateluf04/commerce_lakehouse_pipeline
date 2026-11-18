"""The one place a SparkSession is built, so every job gets the same Delta and storage settings."""
from __future__ import annotations

import os
from collections.abc import Mapping

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


def s3a_conf(environ: Mapping[str, str]) -> dict[str, str]:
    """Settings for Hadoop's S3A connector, which is what makes s3a:// paths work."""
    region = environ.get("AWS_REGION") or environ.get("AWS_DEFAULT_REGION") or "us-east-1"
    return {
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        # The AWS SDK's default chain tries, in order: env vars (AWS_ACCESS_KEY_ID...), Java system
        # properties, the ~/.aws profile named by AWS_PROFILE, then container or EC2 instance roles.
        # Hadoop's own default list leaves out profiles, so it's set explicitly. Credentials are
        # never written into config or code.
        "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        "spark.hadoop.fs.s3a.endpoint": f"s3.{region}.amazonaws.com",
        "spark.hadoop.fs.s3a.endpoint.region": region,
    }


def spark_conf(config: LakehouseConfig, environ: Mapping[str, str]) -> dict[str, str]:
    conf = {
        # These two settings turn on Delta: SQL syntax such as MERGE, and the Delta table catalog.
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        # Store and display timestamps in UTC, whatever time zone the machine running this is in.
        "spark.sql.session.timeZone": "UTC",
        # Dates and timestamps that don't parse become null (silver then quarantines them). The
        # default, EXCEPTION, makes some bad inputs crash the job instead.
        "spark.sql.legacy.timeParserPolicy": "CORRECTED",
        "spark.sql.shuffle.partitions": str(config.shuffle_partitions),
        # Delta rebuilds a table's state from its _delta_log using this many tasks, 50 by default.
        # That's sized for clusters. On a laptop it makes every small-table read and write slow.
        "spark.databricks.delta.snapshotPartitions": str(config.shuffle_partitions),
        "spark.driver.memory": environ.get("SPARK_DRIVER_MEMORY", "2g"),
    }
    jars = environ.get("SPARK_EXTRA_JARS")
    if jars:
        conf["spark.jars"] = jars
    else:
        conf["spark.jars.packages"] = ",".join(_MAVEN_PACKAGES)
    if config.uses_s3:
        conf.update(s3a_conf(environ))
    return conf


def get_spark(
    app_name: str,
    config: LakehouseConfig | None = None,
    *,
    master: str | None = None,
    extra_conf: dict[str, str] | None = None,
) -> SparkSession:
    config = config or LakehouseConfig.from_env()
    builder = SparkSession.builder.appName(app_name).master(master or os.environ.get("SPARK_MASTER", "local[*]"))
    for key, value in {**spark_conf(config, os.environ), **(extra_conf or {})}.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
