"""Data quality checks shared by every job.

Each check returns a CheckResult instead of raising. A job can therefore run all of its checks,
record every result as a metric, and only then decide whether to fail (raise_on_failure).
Checks with severity "warn" are recorded but never fail a run.

Null handling: unique, referential_integrity and value_range skip nulls. Pair them with
not_null, so that each kind of problem is counted by exactly one check.
"""
from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from functools import reduce
from operator import and_, or_

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

ERROR = "error"
WARN = "warn"


@dataclass(frozen=True)
class CheckResult:
    check: str
    table: str
    columns: tuple[str, ...]
    severity: str
    passed: bool
    failed_rows: int
    total_rows: int
    detail: str = ""

    def __post_init__(self) -> None:
        if self.severity not in (ERROR, WARN):
            raise ValueError(f"severity must be {ERROR!r} or {WARN!r}, got {self.severity!r}")

    def __str__(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        text = f"{self.check}({self.table}: {','.join(self.columns)}) {status} {self.failed_rows}/{self.total_rows} failed"
        return f"{text}; {self.detail}" if self.detail else text


class DataQualityError(Exception):
    def __init__(self, failures: Sequence[CheckResult]):
        self.failures = list(failures)
        lines = "\n".join(f"  - {f}" for f in self.failures)
        super().__init__(f"{len(self.failures)} data quality check(s) failed:\n{lines}")


def _result(check, table, columns, severity, failed, total, detail="") -> CheckResult:
    return CheckResult(check, table, tuple(columns), severity, failed == 0, failed, total, detail)


def _count_where(df: DataFrame, condition: Column) -> tuple[int, int]:
    """Return (rows where condition is true, total rows), computed in one pass."""
    row = df.agg(
        F.sum(F.when(condition, 1).otherwise(0)).alias("failed"),
        F.count(F.lit(1)).alias("total"),
    ).first()
    return row["failed"] or 0, row["total"]


def check_not_null(df: DataFrame, columns: Sequence[str], *, table: str, severity: str = ERROR) -> CheckResult:
    """Fail for every row where any of `columns` is null."""
    failed, total = _count_where(df, reduce(or_, [F.col(c).isNull() for c in columns]))
    return _result("not_null", table, columns, severity, failed, total)


def check_unique(df: DataFrame, columns: Sequence[str], *, table: str, severity: str = ERROR) -> CheckResult:
    """Fail for every row whose key (one or more columns) appears more than once."""
    keyed = df.where(reduce(and_, [F.col(c).isNotNull() for c in columns]))
    dupes = keyed.groupBy(*columns).agg(F.count(F.lit(1)).alias("_n")).where(F.col("_n") > 1)
    row = dupes.agg(F.count(F.lit(1)).alias("keys"), F.sum("_n").alias("rows")).first()
    failed = row["rows"] or 0
    detail = f"{row['keys']} duplicated key(s)" if failed else ""
    return _result("unique", table, columns, severity, failed, df.count(), detail)


def check_referential_integrity(
    df: DataFrame,
    column: str,
    ref_df: DataFrame,
    ref_column: str,
    *,
    table: str,
    ref_table: str,
    severity: str = ERROR,
) -> CheckResult:
    """Fail for every row whose non-null `column` value has no match in ref_df.ref_column."""
    ref_keys = ref_df.select(F.col(ref_column).alias(column))
    # A left anti join keeps only the rows on the left that have no match on the right.
    orphans = df.where(F.col(column).isNotNull()).join(ref_keys, on=column, how="left_anti")
    return _result(
        "referential_integrity", table, [column], severity, orphans.count(), df.count(),
        f"references {ref_table}.{ref_column}",
    )


def check_value_range(
    df: DataFrame,
    column: str,
    *,
    table: str,
    min_value: float | None = None,
    max_value: float | None = None,
    severity: str = ERROR,
) -> CheckResult:
    """Fail for every row where `column` is outside [min_value, max_value]. Both bounds are inclusive."""
    if min_value is None and max_value is None:
        raise ValueError("give min_value, max_value, or both")
    conditions = []
    if min_value is not None:
        conditions.append(F.col(column) < min_value)
    if max_value is not None:
        conditions.append(F.col(column) > max_value)
    failed, total = _count_where(df, reduce(or_, conditions))
    low = "-inf" if min_value is None else min_value
    high = "inf" if max_value is None else max_value
    return _result("value_range", table, [column], severity, failed, total, f"expected [{low}, {high}]")


def check_row_count(df: DataFrame, *, table: str, min_rows: int = 1, severity: str = ERROR) -> CheckResult:
    """Fail if there are fewer than min_rows rows, which catches an empty or missing batch.

    failed_rows is the shortfall: how many rows short of min_rows the data is.
    """
    total = df.count()
    return _result("row_count", table, [], severity, max(0, min_rows - total), total, f"expected >= {min_rows} rows")


def raise_on_failure(results: Iterable[CheckResult]) -> None:
    """Raise DataQualityError if any check with severity "error" failed. Failed warnings are ignored."""
    failures = [r for r in results if not r.passed and r.severity == ERROR]
    if failures:
        raise DataQualityError(failures)
