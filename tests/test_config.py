from pathlib import Path

import pytest

from lakehouse_common.config import LakehouseConfig


def test_defaults_use_local_filesystem():
    cfg = LakehouseConfig.from_env({})
    assert cfg.storage_root == Path("data/lakehouse").resolve().as_uri()
    assert cfg.landing_dir == "data/landing"
    assert cfg.shuffle_partitions == 8
    assert not cfg.is_s3


def test_env_overrides():
    cfg = LakehouseConfig.from_env({
        "LAKEHOUSE_STORAGE_ROOT": "s3a://my-bucket/lakehouse/",
        "LAKEHOUSE_LANDING_DIR": "/tmp/landing",
        "SPARK_SHUFFLE_PARTITIONS": "4",
    })
    assert cfg.storage_root == "s3a://my-bucket/lakehouse"  # trailing slash removed
    assert cfg.is_s3
    assert cfg.landing_dir == "/tmp/landing"
    assert cfg.shuffle_partitions == 4


def test_plain_path_becomes_file_uri(tmp_path):
    assert LakehouseConfig(storage_root=str(tmp_path), landing_dir="x").storage_root == tmp_path.resolve().as_uri()


def test_s3_scheme_is_rejected_with_hint():
    with pytest.raises(ValueError, match="s3a://"):
        LakehouseConfig(storage_root="s3://my-bucket", landing_dir="x")


def test_unsupported_scheme_is_rejected():
    with pytest.raises(ValueError, match="file:// or s3a://"):
        LakehouseConfig(storage_root="hdfs://namenode/lake", landing_dir="x")


def test_landing_root_defaults_to_the_local_landing_dir(tmp_path):
    cfg = LakehouseConfig(storage_root="s3a://bucket/lakehouse", landing_dir=str(tmp_path))
    assert cfg.landing_root == tmp_path.resolve().as_uri()
    assert cfg.is_s3 and cfg.uses_s3


def test_landing_root_on_s3():
    cfg = LakehouseConfig.from_env({"LAKEHOUSE_LANDING_ROOT": "s3a://bucket/landing", "LAKEHOUSE_STORAGE_ROOT": "/tmp/lake"})
    assert cfg.landing_root == "s3a://bucket/landing"
    assert not cfg.is_s3 and cfg.uses_s3


def test_table_path():
    cfg = LakehouseConfig(storage_root="s3a://bucket/lakehouse", landing_dir="x")
    assert cfg.table_path("silver", "orders") == "s3a://bucket/lakehouse/silver/orders"


@pytest.mark.parametrize("layer, table", [("platinum", "orders"), ("silver", "Orders"), ("silver", "orders-v2")])
def test_table_path_enforces_naming(layer, table):
    cfg = LakehouseConfig(storage_root="s3a://bucket", landing_dir="x")
    with pytest.raises(ValueError):
        cfg.table_path(layer, table)
