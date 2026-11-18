"""Pipeline configuration, read from environment variables.

The one setting that matters most is LAKEHOUSE_STORAGE_ROOT. Every table path is built from it,
so the same code writes to the local filesystem (tests, local dev) or to S3 (s3a://bucket/prefix)
without any other change.
"""
from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

LAYERS = ("bronze", "silver", "gold", "ops")  # "ops" holds pipeline metadata such as run metrics
_TABLE_NAME = re.compile(r"^[a-z][a-z0-9_]*$")


@dataclass(frozen=True)
class LakehouseConfig:
    storage_root: str  # URI root for Delta tables: file:///... or s3a://bucket/prefix
    landing_dir: str  # local directory the generator writes raw daily files to
    shuffle_partitions: int = 8

    def __post_init__(self) -> None:
        root = self.storage_root.rstrip("/")
        if root.startswith("s3://"):
            # Open-source Hadoop's S3 connector is S3A; s3:// only works on EMR/Databricks.
            raise ValueError(f"use s3a:// instead of s3:// for storage_root: {self.storage_root}")
        if "://" not in root and not root.startswith("file:"):
            root = Path(root).resolve().as_uri()  # a plain path means the local filesystem
        if not root.startswith(("file:", "s3a://")):
            raise ValueError(f"storage_root must be a file:// or s3a:// URI: {self.storage_root}")
        object.__setattr__(self, "storage_root", root)

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> LakehouseConfig:
        env = os.environ if environ is None else environ
        return cls(
            storage_root=env.get("LAKEHOUSE_STORAGE_ROOT") or "data/lakehouse",
            landing_dir=env.get("LAKEHOUSE_LANDING_DIR") or "data/landing",
            shuffle_partitions=int(env.get("SPARK_SHUFFLE_PARTITIONS") or 8),
        )

    @property
    def is_s3(self) -> bool:
        return self.storage_root.startswith("s3a://")

    def table_path(self, layer: str, table: str) -> str:
        """Location of a Delta table: <storage_root>/<layer>/<table>."""
        if layer not in LAYERS:
            raise ValueError(f"unknown layer {layer!r}; expected one of {LAYERS}")
        if not _TABLE_NAME.match(table):
            raise ValueError(f"table names must be lower snake_case: {table!r}")
        return f"{self.storage_root}/{layer}/{table}"
