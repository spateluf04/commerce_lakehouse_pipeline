import pytest


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    """One local SparkSession for the whole test run. Starting a JVM takes several seconds.

    PySpark is imported inside the fixture, so the stdlib-only tests (generator, config, log)
    can run without Java.
    """
    from lakehouse_common.config import LakehouseConfig
    from lakehouse_common.spark import get_spark

    root = tmp_path_factory.mktemp("lakehouse")
    config = LakehouseConfig(storage_root=root.as_uri(), landing_dir=str(tmp_path_factory.mktemp("landing")), shuffle_partitions=1)
    session = get_spark(
        "tests",
        config,
        master="local[1]",
        extra_conf={"spark.ui.enabled": "false", "spark.sql.warehouse.dir": str(root / "warehouse")},
    )
    yield session
    session.stop()
