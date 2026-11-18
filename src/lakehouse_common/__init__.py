"""Shared library imported by every pipeline job: config, Spark session, logging, data quality.

Submodules are imported explicitly (e.g. `from lakehouse_common.quality import check_unique`) so
that stdlib-only code like the generator can use config and logging without importing PySpark.
"""
