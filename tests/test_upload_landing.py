from datetime import date

import pytest

from conftest import SMALL_BATCH
from generator.generate import GeneratorConfig, write_batch
from jobs import upload_landing

DS = date(2026, 3, 1)


class FakeS3:
    def __init__(self):
        self.uploads = []

    def upload_file(self, filename, bucket, key):
        self.uploads.append((bucket, key))


def test_parse_s3a():
    assert upload_landing.parse_s3a("s3a://bucket/landing/") == ("bucket", "landing")
    assert upload_landing.parse_s3a("s3a://bucket") == ("bucket", "")
    with pytest.raises(ValueError):
        upload_landing.parse_s3a("s3://bucket")


def test_upload_batch_mirrors_the_local_layout(tmp_path):
    write_batch(GeneratorConfig(run_date=DS, **SMALL_BATCH), tmp_path)
    s3 = FakeS3()
    keys = upload_landing.upload_batch(s3, str(tmp_path), "s3a://my-bucket/landing", DS)
    assert sorted(keys) == [
        "landing/_manifests/dt=2026-03-01/manifest.json",
        "landing/customers/dt=2026-03-01/customers.csv",
        "landing/orders/dt=2026-03-01/orders.jsonl",
        "landing/products/dt=2026-03-01/products.csv",
    ]
    assert {bucket for bucket, _ in s3.uploads} == {"my-bucket"}


def test_upload_batch_fails_when_the_day_was_not_generated(tmp_path):
    with pytest.raises(FileNotFoundError):
        upload_landing.upload_batch(FakeS3(), str(tmp_path), "s3a://my-bucket/landing", DS)


def test_main_is_a_no_op_for_a_local_landing_root(tmp_path, monkeypatch):
    monkeypatch.setenv("LAKEHOUSE_LANDING_DIR", str(tmp_path))
    monkeypatch.delenv("LAKEHOUSE_LANDING_ROOT", raising=False)
    assert upload_landing.main(["--ds", "2026-03-01"]) == 0  # nothing generated, nothing uploaded, no error
