"""Pipeline configuration, read from environment variables.

Two roots decide where everything lives. Every table path and landing path is built from them, so
the same code runs against the local filesystem (tests, local dev) or S3 (s3a://bucket/prefix)
without any other change:

    LAKEHOUSE_STORAGE_ROOT  Delta tables            file:///... or s3a://bucket/lakehouse
    LAKEHOUSE_LANDING_ROOT  raw files bronze reads  defaults to LAKEHOUSE_LANDING_DIR
"""
from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

LAYERS = ("bronze", "silver", "gold", "ops")  # "ops" holds pipeline metadata such as run metrics
_TABLE_NAME = re.compile(r"^[a-z][a-z0-9_]*$")


def _normalize_uri(value: str, name: str) -> str:
    uri = value.rstrip("/")
    if uri.startswith("s3://"):
        # Open-source Hadoop's S3 connector is S3A; s3:// only works on EMR/Databricks.
        raise ValueError(f"use s3a:// instead of s3:// for {name}: {value}")
    if "://" not in uri and not uri.startswith("file:"):
        uri = Path(uri).resolve().as_uri()  # a plain path means the local filesystem
    if not uri.startswith(("file:", "s3a://")):
        raise ValueError(f"{name} must be a file:// or s3a:// URI: {value}")
    return uri


@dataclass(frozen=True)
class LakehouseConfig:
    storage_root: str  # URI root for Delta tables
    landing_dir: str  # local directory the generator writes raw daily files to
    shuffle_partitions: int = 8
    landing_root: str = ""  # URI bronze reads raw files from. Empty means landing_dir.

    def __post_init__(self) -> None:
        object.__setattr__(self, "storage_root", _normalize_uri(self.storage_root, "storage_root"))
        object.__setattr__(self, "landing_root", _normalize_uri(self.landing_root or self.landing_dir, "landing_root"))

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> LakehouseConfig:
        env = os.environ if environ is None else environ
        return cls(
            storage_root=env.get("LAKEHOUSE_STORAGE_ROOT") or "data/lakehouse",
            landing_dir=env.get("LAKEHOUSE_LANDING_DIR") or "data/landing",
            shuffle_partitions=int(env.get("SPARK_SHUFFLE_PARTITIONS") or 8),
            landing_root=env.get("LAKEHOUSE_LANDING_ROOT") or "",
        )

    @property
    def is_s3(self) -> bool:
        return self.storage_root.startswith("s3a://")

    @property
    def uses_s3(self) -> bool:
        """True if Spark needs the S3A connector to read or write either root."""
        return self.is_s3 or self.landing_root.startswith("s3a://")

    def table_path(self, layer: str, table: str) -> str:
        """Location of a Delta table: <storage_root>/<layer>/<table>."""
        if layer not in LAYERS:
            raise ValueError(f"unknown layer {layer!r}; expected one of {LAYERS}")
        if not _TABLE_NAME.match(table):
            raise ValueError(f"table names must be lower snake_case: {table!r}")
        return f"{self.storage_root}/{layer}/{table}"
