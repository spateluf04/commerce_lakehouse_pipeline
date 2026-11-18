import pytest

from lakehouse_common.quality import (
    ERROR,
    WARN,
    CheckResult,
    DataQualityError,
    check_not_null,
    check_referential_integrity,
    check_row_count,
    check_unique,
    check_value_range,
    raise_on_failure,
)


@pytest.fixture(scope="module")
def orders(spark):
    return spark.createDataFrame(
        [
            ("O1", "C1", 10.0),
            ("O2", "C2", 0.0),
            ("O2", "C2", 5.0),   # duplicate order_id
            ("O3", None, 7.5),   # null customer_id
            ("O4", "C9", -1.0),  # orphan customer, negative amount
            (None, "C1", 3.0),   # null order_id
        ],
        "order_id STRING, customer_id STRING, amount DOUBLE",
    )


@pytest.fixture(scope="module")
def customers(spark):
    return spark.createDataFrame([("C1",), ("C2",)], "customer_id STRING")


# --- not_null --------------------------------------------------------------

def test_not_null_counts_rows_with_any_null(orders):
    result = check_not_null(orders, ["order_id", "customer_id"], table="orders")
    assert (result.passed, result.failed_rows, result.total_rows) == (False, 2, 6)
    assert result.columns == ("order_id", "customer_id")


def test_not_null_passes(orders):
    assert check_not_null(orders, ["amount"], table="orders").passed


def test_not_null_on_empty_dataframe_passes(spark):
    empty = spark.createDataFrame([], "id STRING")
    result = check_not_null(empty, ["id"], table="empty")
    assert (result.passed, result.failed_rows, result.total_rows) == (True, 0, 0)


# --- unique ----------------------------------------------------------------

def test_unique_counts_every_row_of_a_duplicated_key(orders):
    result = check_unique(orders, ["order_id"], table="orders")
    assert (result.passed, result.failed_rows, result.total_rows) == (False, 2, 6)
    assert result.detail == "1 duplicated key(s)"


def test_unique_ignores_null_keys(spark):
    df = spark.createDataFrame([(None,), (None,), ("a",)], "id STRING")
    assert check_unique(df, ["id"], table="t").passed


def test_unique_composite_key(spark):
    df = spark.createDataFrame([("O1", 1), ("O1", 2), ("O2", 1)], "order_id STRING, line INT")
    assert check_unique(df, ["order_id", "line"], table="t").passed
    assert not check_unique(df, ["order_id"], table="t").passed


# --- referential integrity -------------------------------------------------

def test_referential_integrity_finds_orphans_and_ignores_nulls(orders, customers):
    result = check_referential_integrity(
        orders, "customer_id", customers, "customer_id", table="orders", ref_table="customers"
    )
    assert (result.passed, result.failed_rows, result.total_rows) == (False, 1, 6)  # only C9; the null is not_null's job
    assert result.detail == "references customers.customer_id"


def test_referential_integrity_with_different_column_names(spark, customers):
    df = spark.createDataFrame([("C1",), ("C2",)], "buyer STRING")
    assert check_referential_integrity(df, "buyer", customers, "customer_id", table="t", ref_table="customers").passed


# --- value range -----------------------------------------------------------

def test_value_range_min_only(orders):
    result = check_value_range(orders, "amount", table="orders", min_value=0)
    assert (result.passed, result.failed_rows) == (False, 1)  # 0.0 is allowed: bounds are inclusive
    assert result.detail == "expected [0, inf]"


def test_value_range_both_bounds(orders):
    result = check_value_range(orders, "amount", table="orders", min_value=0, max_value=7.5)
    assert result.failed_rows == 2  # -1.0 and 10.0


def test_value_range_ignores_nulls(spark):
    df = spark.createDataFrame([(None,), (1.0,)], "x DOUBLE")
    assert check_value_range(df, "x", table="t", min_value=0).passed


def test_value_range_requires_a_bound(orders):
    with pytest.raises(ValueError):
        check_value_range(orders, "amount", table="orders")


# --- row count -------------------------------------------------------------

def test_row_count(orders, spark):
    assert check_row_count(orders, table="orders", min_rows=6).passed
    result = check_row_count(spark.createDataFrame([], "id STRING"), table="empty")
    assert (result.passed, result.failed_rows, result.total_rows) == (False, 1, 0)


# --- severity and raise_on_failure -----------------------------------------

def test_invalid_severity_rejected(orders):
    with pytest.raises(ValueError, match="severity"):
        check_not_null(orders, ["amount"], table="orders", severity="critical")


def test_raise_on_failure_raises_for_failed_errors(orders):
    results = [
        check_not_null(orders, ["amount"], table="orders"),
        check_unique(orders, ["order_id"], table="orders", severity=ERROR),
    ]
    with pytest.raises(DataQualityError) as excinfo:
        raise_on_failure(results)
    assert [f.check for f in excinfo.value.failures] == ["unique"]
    assert "unique(orders: order_id) FAIL 2/6 failed" in str(excinfo.value)


def test_raise_on_failure_ignores_failed_warnings(orders):
    raise_on_failure([check_unique(orders, ["order_id"], table="orders", severity=WARN)])


def test_check_result_str():
    r = CheckResult("not_null", "t", ("a",), ERROR, True, 0, 10)
    assert str(r) == "not_null(t: a) PASS 0/10 failed"
