import logging
from datetime import date
from types import SimpleNamespace

import pytest

# Small enough to keep the tests fast. The 5% bad rate injects 3 of each order defect.
SMALL_BATCH = dict(orders=300, base_customers=100, new_customers_per_day=2, products=40, bad_rate=0.05)
PIPELINE_DATES = (date(2026, 3, 1), date(2026, 3, 2))


def pytest_collection_modifyitems(items):
    """Mark tests that run whole pipeline days as `integration`. `pytest -m "not integration"` skips them for a quick run."""
    for item in items:
        if {"pipeline", "load_day"} & set(item.fixturenames):
            item.add_marker(pytest.mark.integration)


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    """One local SparkSession for the whole test run. Starting a JVM takes several seconds.

    PySpark is imported inside the fixture, so the stdlib-only tests (generator, config, log)
    can run without Java.
    """
    from lakehouse_common.spark import get_spark

    root = tmp_path_factory.mktemp("spark")
    session = get_spark(
        "tests",
        _make_lake(root),
        master="local[2]",
        extra_conf={"spark.ui.enabled": "false", "spark.sql.warehouse.dir": str(root / "warehouse")},
    )
    yield session
    session.stop()
    # py4j logs "Closing down clientserver connection" at interpreter exit, after pytest has closed
    # the stream the root logger writes to. Raising its level keeps that from printing a traceback.
    logging.getLogger("py4j").setLevel(logging.WARNING)


def _make_lake(root):
    from lakehouse_common.config import LakehouseConfig

    return LakehouseConfig(storage_root=(root / "lakehouse").as_uri(), landing_dir=str(root / "landing"), shuffle_partitions=1)


def run_day(spark, lake, ds, *, through_gold=True, **overrides):
    """Generate one day of data and run it through the pipeline. Returns the generator manifest."""
    from generator.generate import GeneratorConfig, write_batch
    from jobs import bronze_ingest, gold_build, quality_gate, silver_transform

    manifest = write_batch(GeneratorConfig(run_date=ds, **{**SMALL_BATCH, **overrides}), lake.landing_dir)
    bronze_ingest.run(spark, lake, ds)
    silver_transform.run(spark, lake, ds)
    if through_gold:
        quality_gate.run(spark, lake, ds)
        gold_build.run(spark, lake, ds)
    return manifest


@pytest.fixture
def lake(tmp_path):
    """A fresh, empty lakehouse and landing zone for a test that needs its own."""
    return _make_lake(tmp_path)


@pytest.fixture
def load_day(spark, lake):
    """Return load(ds, **generator_overrides), which runs one day through bronze and silver in `lake`."""
    return lambda ds, **overrides: run_day(spark, lake, ds, through_gold=False, **overrides)


@pytest.fixture(scope="session")
def pipeline(spark, tmp_path_factory):
    """A lakehouse with PIPELINE_DATES run end to end, from the generator to gold, built once per session.

    Tests must leave it as they found it. The only one that writes to it re-runs a day, which is
    idempotent, so other tests still see the same data.
    """
    lake = _make_lake(tmp_path_factory.mktemp("pipeline"))
    manifests = {ds: run_day(spark, lake, ds) for ds in PIPELINE_DATES}
    return SimpleNamespace(lake=lake, manifests=manifests)
