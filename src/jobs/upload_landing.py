"""Copy one day's raw files from the local landing directory to the S3 landing zone.

The generator writes locally (it's standard library only). When LAKEHOUSE_LANDING_ROOT is an
s3a:// URI, this uploads that day's files so bronze reads them from S3, like a real source system
dropping files into a bucket. When the landing root is local, there is nothing to do.
Re-uploading overwrites the same keys, so re-runs are safe.

    python -m jobs.upload_landing --ds 2026-03-01
"""
from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
from typing import Any

from lakehouse_common.config import LakehouseConfig
from lakehouse_common.log import get_logger

log = get_logger(__name__)

ENTITIES = ("customers", "products", "orders", "_manifests")


def parse_s3a(uri: str) -> tuple[str, str]:
    """'s3a://bucket/some/prefix' -> ('bucket', 'some/prefix')"""
    if not uri.startswith("s3a://"):
        raise ValueError(f"not an s3a:// URI: {uri}")
    bucket, _, prefix = uri.removeprefix("s3a://").partition("/")
    return bucket, prefix.strip("/")


def batch_files(landing_dir: str, ds: date) -> list[Path]:
    root = Path(landing_dir)
    files = sorted(
        p for entity in ENTITIES for p in (root / entity / f"dt={ds.isoformat()}").glob("*")
        if p.is_file() and not p.name.startswith(".")
    )
    if not files:
        raise FileNotFoundError(f"no landing files for {ds} under {root}")
    return files


def upload_batch(client: Any, landing_dir: str, landing_root: str, ds: date) -> list[str]:
    """Upload the day's files with a boto3 S3 client. Returns the keys written."""
    bucket, prefix = parse_s3a(landing_root)
    keys = []
    for path in batch_files(landing_dir, ds):
        key = "/".join(part for part in (prefix, path.relative_to(landing_dir).as_posix()) if part)
        client.upload_file(str(path), bucket, key)
        keys.append(key)
    return keys


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m jobs.upload_landing")
    parser.add_argument("--ds", required=True, type=date.fromisoformat)
    args = parser.parse_args(argv)

    config = LakehouseConfig.from_env()
    if not config.landing_root.startswith("s3a://"):
        log.info("landing root is local; nothing to upload", extra={"landing_root": config.landing_root})
        return 0

    import boto3  # only needed when running against S3. It uses the same credential chain as S3A.

    keys = upload_batch(boto3.client("s3"), config.landing_dir, config.landing_root, args.ds)
    log.info("uploaded landing files", extra={"ds": args.ds.isoformat(), "landing_root": config.landing_root, "files": len(keys)})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
