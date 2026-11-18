import csv
import json
from datetime import date, datetime, timedelta

import pytest

from generator.generate import (
    ORDER_DEFECTS,
    ORPHAN_CUSTOMER_ID,
    TS_FORMAT,
    GeneratorConfig,
    customer_count,
    generate_customers,
    generate_orders,
    generate_products,
    main,
    write_batch,
)

RUN_DATE = date(2026, 3, 1)


def make_config(**overrides):
    params = dict(run_date=RUN_DATE, orders=500, base_customers=200, new_customers_per_day=3, products=50, bad_rate=0.05)
    params.update(overrides)
    return GeneratorConfig(**params)


def build(cfg):
    customers, _ = generate_customers(cfg)
    products = generate_products(cfg)
    orders, defects = generate_orders(cfg, customers, products)
    return customers, products, orders, defects


def _valid_ts(value):
    try:
        datetime.strptime(value, TS_FORMAT)
        return True
    except ValueError:
        return False


def test_same_inputs_produce_identical_files(tmp_path):
    cfg = make_config()
    write_batch(cfg, tmp_path / "a")
    write_batch(cfg, tmp_path / "b")
    files_a = sorted(p.relative_to(tmp_path / "a") for p in (tmp_path / "a").rglob("*") if p.is_file())
    files_b = sorted(p.relative_to(tmp_path / "b") for p in (tmp_path / "b").rglob("*") if p.is_file())
    assert files_a == files_b and len(files_a) == 4
    for rel in files_a:
        assert (tmp_path / "a" / rel).read_bytes() == (tmp_path / "b" / rel).read_bytes(), rel


def test_different_dates_produce_different_orders():
    _, _, day1, _ = build(make_config())
    _, _, day2, _ = build(make_config(run_date=RUN_DATE + timedelta(days=1)))
    assert {o["order_id"] for o in day1}.isdisjoint(o["order_id"] for o in day2)
    assert [o["customer_id"] for o in day1] != [o["customer_id"] for o in day2]


def test_order_volume_and_ids():
    cfg = make_config()
    _, _, orders, defects = build(cfg)
    ids = [o["order_id"] for o in orders]
    assert len(ids) == cfg.orders + defects["duplicate_order_id"]
    assert len(set(ids)) == cfg.orders
    assert all(i.startswith("O20260301") for i in ids)


def test_defect_counts_match_content():
    cfg = make_config()
    customers, _, orders, defects = build(cfg)
    customer_ids = {c["customer_id"] for c in customers}

    assert set(defects) == set(ORDER_DEFECTS)
    assert sum(defects.values()) == round(cfg.orders * cfg.bad_rate)
    assert all(n > 0 for n in defects.values())
    assert sum(o["customer_id"] is None for o in orders) == defects["null_customer_id"]
    assert sum(o["customer_id"] == ORPHAN_CUSTOMER_ID for o in orders) == defects["orphan_customer_id"]
    assert ORPHAN_CUSTOMER_ID not in customer_ids
    assert sum(any(i["unit_price"] < 0 for i in o["line_items"]) for o in orders) == defects["negative_unit_price"]
    assert sum(not _valid_ts(o["order_ts"]) for o in orders) == defects["invalid_order_ts"]
    assert len(orders) - len({o["order_id"] for o in orders}) == defects["duplicate_order_id"]


def test_duplicate_orders_are_exact_copies():
    _, _, orders, _ = build(make_config())
    by_id = {}
    for o in orders:
        by_id.setdefault(o["order_id"], []).append(o)
    dupes = [group for group in by_id.values() if len(group) > 1]
    assert dupes and all(group[0] == group[1] for group in dupes)


def test_zero_bad_rate_produces_clean_data():
    cfg = make_config(bad_rate=0)
    customers, products, orders, defects = build(cfg)
    customer_ids = {c["customer_id"] for c in customers}
    product_ids = {p["product_id"] for p in products}

    assert sum(defects.values()) == 0
    assert len(customers) == len(customer_ids)
    assert len({o["order_id"] for o in orders}) == len(orders) == cfg.orders
    for o in orders:
        assert o["customer_id"] in customer_ids
        assert datetime.strptime(o["order_ts"], TS_FORMAT).date() == RUN_DATE
        assert 1 <= len(o["line_items"]) <= 5
        assert len({i["product_id"] for i in o["line_items"]}) == len(o["line_items"])
        for item in o["line_items"]:
            assert item["product_id"] in product_ids
            assert item["unit_price"] > 0 and item["quantity"] >= 1


def test_orders_only_reference_customers_signed_up_by_run_date():
    customers, _, orders, _ = build(make_config(bad_rate=0, run_date=date(2026, 1, 3)))
    signup = {c["customer_id"]: c["signup_date"] for c in customers}
    assert all(signup[o["customer_id"]] <= "2026-01-03" for o in orders)


def test_customer_snapshot_grows_and_is_stable():
    cfg = make_config(bad_rate=0)
    later = make_config(bad_rate=0, run_date=RUN_DATE + timedelta(days=10))
    today, _ = generate_customers(cfg)
    future, _ = generate_customers(later)

    assert len(today) == customer_count(cfg) == 200 + 59 * 3  # 59 days since the epoch
    assert len(future) == len(today) + 10 * 3
    future_by_id = {c["customer_id"]: c for c in future}
    for c in today:  # identity attributes never change between snapshots
        f = future_by_id[c["customer_id"]]
        assert (f["email"], f["signup_date"]) == (c["email"], c["signup_date"])


def test_customer_moves_show_up_from_the_move_date():
    year_end, _ = generate_customers(make_config(bad_rate=0, run_date=date(2026, 12, 31)))
    moved = [c for c in year_end if c["updated_date"] != c["signup_date"]]
    assert moved, "expected some customers to have moved"

    c = moved[0]
    move_date = date.fromisoformat(c["updated_date"])
    before, _ = generate_customers(make_config(bad_rate=0, run_date=move_date - timedelta(days=1)))
    on_the_day, _ = generate_customers(make_config(bad_rate=0, run_date=move_date))
    old = next(x for x in before if x["customer_id"] == c["customer_id"])
    new = next(x for x in on_the_day if x["customer_id"] == c["customer_id"])
    assert old["updated_date"] == old["signup_date"]
    assert new == c


def test_customer_duplicates_are_injected():
    cfg = make_config()
    customers, defects = generate_customers(cfg)
    assert defects["duplicate_customer_id"] == round(customer_count(cfg) * cfg.bad_rate) > 0
    assert len(customers) - len({c["customer_id"] for c in customers}) == defects["duplicate_customer_id"]


def test_write_batch_layout_and_manifest(tmp_path):
    manifest = write_batch(make_config(), tmp_path)
    assert manifest["files"] == {
        "customers": "customers/dt=2026-03-01/customers.csv",
        "products": "products/dt=2026-03-01/products.csv",
        "orders": "orders/dt=2026-03-01/orders.jsonl",
    }
    orders = [json.loads(line) for line in (tmp_path / manifest["files"]["orders"]).read_text().splitlines()]
    with open(tmp_path / manifest["files"]["customers"], newline="") as f:
        customers = list(csv.DictReader(f))
    assert len(orders) == manifest["row_counts"]["orders"]
    assert len(customers) == manifest["row_counts"]["customers"]
    assert sum(len(o["line_items"]) for o in orders) == manifest["row_counts"]["order_line_items"]
    assert json.loads((tmp_path / "_manifests/dt=2026-03-01/manifest.json").read_text()) == manifest
    assert not list(tmp_path.rglob(".*.tmp"))  # no temp files left behind


def test_rerun_overwrites_the_same_partition(tmp_path):
    write_batch(make_config(orders=50), tmp_path)
    write_batch(make_config(orders=20), tmp_path)
    lines = (tmp_path / "orders/dt=2026-03-01/orders.jsonl").read_text().splitlines()
    assert len({json.loads(line)["order_id"] for line in lines}) == 20


def test_cli(tmp_path):
    assert main(["--date", "2026-03-01", "--out", str(tmp_path), "--orders", "10", "--base-customers", "20", "--products", "5"]) == 0
    manifest = json.loads((tmp_path / "_manifests/dt=2026-03-01/manifest.json").read_text())
    assert manifest["config"]["orders"] == 10
    assert manifest["row_counts"]["products"] == 5


def test_cli_uses_landing_dir_from_env(tmp_path, monkeypatch):
    monkeypatch.setenv("LAKEHOUSE_LANDING_DIR", str(tmp_path))
    main(["--date", "2026-03-01", "--orders", "5", "--base-customers", "5", "--products", "5"])
    assert (tmp_path / "orders/dt=2026-03-01/orders.jsonl").exists()


@pytest.mark.parametrize("overrides", [{"bad_rate": 0.9}, {"orders": -1}, {"products": 0}])
def test_invalid_config_rejected(overrides):
    with pytest.raises(ValueError):
        make_config(**overrides)
