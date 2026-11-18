"""Deterministic generator for one day of e-commerce source data (standard library only).

Each run writes one daily batch to the landing zone:

    <out>/customers/dt=YYYY-MM-DD/customers.csv    full customer snapshot
    <out>/products/dt=YYYY-MM-DD/products.csv      full product catalog
    <out>/orders/dt=YYYY-MM-DD/orders.jsonl        that day's orders, line items nested
    <out>/_manifests/dt=YYYY-MM-DD/manifest.json   row counts and the defects that were injected

All randomness is seeded from (seed, entity, key), so re-running a date reproduces exactly the
same bytes. That is what makes the pipeline's idempotency testable end to end.
"""
from __future__ import annotations

import argparse
import copy
import csv
import io
import json
import os
import random
from dataclasses import asdict, dataclass, fields
from datetime import date, datetime, time, timedelta, timezone
from itertools import accumulate
from pathlib import Path

from generator import catalog
from lakehouse_common.config import LakehouseConfig
from lakehouse_common.log import get_logger

log = get_logger(__name__)

# Defects injected into orders, so that silver's quarantine and the data quality checks have something to catch.
ORDER_DEFECTS = (
    "null_customer_id",
    "orphan_customer_id",  # points to a customer that doesn't exist
    "negative_unit_price",
    "invalid_order_ts",  # won't parse as a timestamp
    "duplicate_order_id",  # the source sent the same order twice (at-least-once delivery)
)
ORPHAN_CUSTOMER_ID = "C9999999"
TS_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
CUSTOMER_FIELDS = ("customer_id", "first_name", "last_name", "email", "city", "state", "signup_date", "updated_date")
PRODUCT_FIELDS = ("product_id", "product_name", "category", "list_price")


@dataclass(frozen=True)
class GeneratorConfig:
    run_date: date
    orders: int = 2_000
    base_customers: int = 1_000  # customers who signed up before the epoch
    new_customers_per_day: int = 5  # customers who join on each day after the epoch
    products: int = 200
    bad_rate: float = 0.02  # fraction of records given a defect
    seed: int = 42
    epoch: date = date(2026, 1, 1)

    def __post_init__(self) -> None:
        if self.orders < 0 or self.new_customers_per_day < 0:
            raise ValueError("orders and new_customers_per_day must be >= 0")
        if self.base_customers < 1 or self.products < 1:
            raise ValueError("base_customers and products must be >= 1")
        if not 0 <= self.bad_rate <= 0.5:
            raise ValueError("bad_rate must be between 0 and 0.5")


def _rng(seed: int, *parts: object) -> random.Random:
    # A string seed is hashed with SHA-512, so this is stable across runs and machines
    # (unlike hash(), which changes with PYTHONHASHSEED).
    return random.Random(":".join(str(p) for p in (seed, *parts)))


def customer_count(cfg: GeneratorConfig) -> int:
    """How many distinct customers have signed up by cfg.run_date."""
    days = max(0, (cfg.run_date - cfg.epoch).days)
    return cfg.base_customers + days * cfg.new_customers_per_day


def generate_customers(cfg: GeneratorConfig) -> tuple[list[dict], dict[str, int]]:
    """Full snapshot of every customer who has signed up by run_date, plus some duplicate rows.

    Each customer's attributes come from that customer's own seed, so a customer looks the same in
    every snapshot. About 10% move house once. From the move date on, the snapshot shows the new
    address and updated_date, which gives silver's MERGE real updates to apply.
    """
    rows = []
    for i in range(1, customer_count(cfg) + 1):
        rng = _rng(cfg.seed, "customer", i)
        first = rng.choice(catalog.FIRST_NAMES)
        last = rng.choice(catalog.LAST_NAMES)
        city, state = rng.choice(catalog.LOCATIONS)
        if i <= cfg.base_customers:
            signup = cfg.epoch - timedelta(days=rng.randint(1, 730))
        else:
            signup = cfg.epoch + timedelta(days=(i - cfg.base_customers - 1) // cfg.new_customers_per_day + 1)
        updated = signup
        if rng.random() < 0.10:
            move_date = signup + timedelta(days=rng.randint(30, 365))
            new_city, new_state = rng.choice(catalog.LOCATIONS)
            if move_date <= cfg.run_date:
                city, state, updated = new_city, new_state, move_date
        rows.append({
            "customer_id": f"C{i:07d}",
            "first_name": first,
            "last_name": last,
            "email": f"{first}.{last}.{i}@example.com".lower(),
            "city": city,
            "state": state,
            "signup_date": signup.isoformat(),
            "updated_date": updated.isoformat(),
        })

    rng = _rng(cfg.seed, "customers", cfg.run_date)
    dupes = [dict(rows[i]) for i in rng.sample(range(len(rows)), round(len(rows) * cfg.bad_rate))]
    for row in dupes:
        rows.insert(rng.randrange(len(rows) + 1), row)
    return rows, {"duplicate_customer_id": len(dupes)}


def generate_products(cfg: GeneratorConfig) -> list[dict]:
    """The product catalog. It doesn't depend on run_date: it's the same every day."""
    categories = list(catalog.CATEGORIES)
    rows = []
    for i in range(1, cfg.products + 1):
        rng = _rng(cfg.seed, "product", i)
        category = categories[(i - 1) % len(categories)]
        nouns, (low, high) = catalog.CATEGORIES[category]
        rows.append({
            "product_id": f"P{i:05d}",
            "product_name": f"{rng.choice(catalog.ADJECTIVES)} {rng.choice(nouns)}",
            "category": category,
            "list_price": round(rng.uniform(low, high), 2),
        })
    return rows


def generate_orders(cfg: GeneratorConfig, customers: list[dict], products: list[dict]) -> tuple[list[dict], dict[str, int]]:
    """One day of orders with nested line items, some of them with defects."""
    run_day = cfg.run_date.isoformat()
    rng = _rng(cfg.seed, "orders", run_day)
    eligible = sorted({c["customer_id"] for c in customers if c["signup_date"] <= run_day})
    if cfg.orders and not eligible:
        raise ValueError(f"no customers have signed up by {run_day}")

    # A few heavy buyers and a long tail of occasional ones, like real customers.
    customer_cum = list(accumulate(_rng(cfg.seed, "activity", cid).paretovariate(1.5) for cid in eligible))
    # Sales fall off with product rank (roughly Zipf), so "top products" in gold means something.
    product_cum = list(accumulate(1 / rank**1.1 for rank in range(1, len(products) + 1)))
    hour_cum = list(accumulate(catalog.HOUR_WEIGHTS))
    statuses, status_weights = zip(*catalog.ORDER_STATUSES)
    payments, payment_weights = zip(*catalog.PAYMENT_METHODS)
    day_start = datetime.combine(cfg.run_date, time.min, tzinfo=timezone.utc)

    orders = []
    for seq in range(1, cfg.orders + 1):
        ts = day_start + timedelta(hours=rng.choices(range(24), cum_weights=hour_cum)[0], seconds=rng.randrange(3600))
        n_items = min(rng.choices((1, 2, 3, 4, 5), weights=(45, 25, 15, 10, 5))[0], len(products))
        picked: list[dict] = []  # a list, not a set, so the draw order (and the output) is stable
        while len(picked) < n_items:
            product = rng.choices(products, cum_weights=product_cum)[0]
            if product not in picked:
                picked.append(product)
        orders.append({
            "order_id": f"O{cfg.run_date:%Y%m%d}{seq:06d}",
            "customer_id": rng.choices(eligible, cum_weights=customer_cum)[0],
            "order_ts": ts.strftime(TS_FORMAT),
            "status": rng.choices(statuses, weights=status_weights)[0],
            "payment_method": rng.choices(payments, weights=payment_weights)[0],
            "currency": "USD",
            "line_items": [
                {
                    "line_number": n,
                    "product_id": p["product_id"],
                    "quantity": rng.choices((1, 2, 3, 4), weights=(70, 20, 7, 3))[0],
                    "unit_price": round(p["list_price"] * (1 - rng.choice(catalog.DISCOUNTS)), 2),
                }
                for n, p in enumerate(picked, start=1)
            ],
        })
    return orders, _inject_order_defects(rng, orders, cfg.bad_rate)


def _inject_order_defects(rng: random.Random, orders: list[dict], bad_rate: float) -> dict[str, int]:
    """Give a sample of orders one defect each, cycling through ORDER_DEFECTS. Edits `orders` in place.

    Because each chosen order gets exactly one defect, the counts in the manifest are exact.
    """
    counts = dict.fromkeys(ORDER_DEFECTS, 0)
    resent = []
    for k, idx in enumerate(rng.sample(range(len(orders)), round(len(orders) * bad_rate))):
        defect = ORDER_DEFECTS[k % len(ORDER_DEFECTS)]
        order = orders[idx]
        if defect == "null_customer_id":
            order["customer_id"] = None
        elif defect == "orphan_customer_id":
            order["customer_id"] = ORPHAN_CUSTOMER_ID
        elif defect == "negative_unit_price":
            item = rng.choice(order["line_items"])
            item["unit_price"] = -item["unit_price"]
        elif defect == "invalid_order_ts":
            order["order_ts"] = "not-a-timestamp"
        else:
            resent.append(copy.deepcopy(order))
        counts[defect] += 1
    for order in resent:
        orders.insert(rng.randrange(len(orders) + 1), order)
    return counts


def _to_csv(rows: list[dict], fieldnames: tuple[str, ...]) -> str:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue()


def _atomic_write(path: Path, content: str) -> None:
    """Write to a hidden temp file, then rename it over the target.

    A reader never sees a half-written file. Spark skips files whose names start with "." if it
    lists the directory mid-write.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(content, encoding="utf-8", newline="")
    os.replace(tmp, path)


def write_batch(cfg: GeneratorConfig, out_dir: str | Path) -> dict:
    """Generate one day's batch, write it under out_dir, and return the manifest."""
    out = Path(out_dir)
    partition = f"dt={cfg.run_date.isoformat()}"
    customers, customer_defects = generate_customers(cfg)
    products = generate_products(cfg)
    orders, order_defects = generate_orders(cfg, customers, products)

    files = {
        "customers": out / "customers" / partition / "customers.csv",
        "products": out / "products" / partition / "products.csv",
        "orders": out / "orders" / partition / "orders.jsonl",
    }
    _atomic_write(files["customers"], _to_csv(customers, CUSTOMER_FIELDS))
    _atomic_write(files["products"], _to_csv(products, PRODUCT_FIELDS))
    _atomic_write(files["orders"], "".join(json.dumps(o, separators=(",", ":")) + "\n" for o in orders))

    manifest = {
        "run_date": cfg.run_date.isoformat(),
        "config": {k: v.isoformat() if isinstance(v, date) else v for k, v in asdict(cfg).items()},
        "files": {name: path.relative_to(out).as_posix() for name, path in files.items()},
        "row_counts": {
            "customers": len(customers),
            "products": len(products),
            "orders": len(orders),
            "order_line_items": sum(len(o["line_items"]) for o in orders),
        },
        "injected_defects": {"customers": customer_defects, "orders": order_defects},
    }
    _atomic_write(out / "_manifests" / partition / "manifest.json", json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return manifest


def main(argv: list[str] | None = None) -> int:
    defaults = {f.name: f.default for f in fields(GeneratorConfig) if f.name != "run_date"}
    parser = argparse.ArgumentParser(prog="python -m generator", description="Generate one day of e-commerce source data.")
    parser.add_argument("--date", required=True, type=date.fromisoformat, help="business date, YYYY-MM-DD")
    parser.add_argument("--out", help="landing directory (default: $LAKEHOUSE_LANDING_DIR, or data/landing)")
    parser.add_argument("--orders", type=int, default=defaults["orders"])
    parser.add_argument("--base-customers", type=int, default=defaults["base_customers"])
    parser.add_argument("--new-customers-per-day", type=int, default=defaults["new_customers_per_day"])
    parser.add_argument("--products", type=int, default=defaults["products"])
    parser.add_argument("--bad-rate", type=float, default=defaults["bad_rate"])
    parser.add_argument("--seed", type=int, default=defaults["seed"])
    args = parser.parse_args(argv)

    cfg = GeneratorConfig(
        run_date=args.date,
        orders=args.orders,
        base_customers=args.base_customers,
        new_customers_per_day=args.new_customers_per_day,
        products=args.products,
        bad_rate=args.bad_rate,
        seed=args.seed,
    )
    out = args.out or LakehouseConfig.from_env().landing_dir
    manifest = write_batch(cfg, out)
    log.info(
        "generated daily batch",
        extra={"run_date": manifest["run_date"], "landing_dir": str(out),
               "row_counts": manifest["row_counts"], "injected_defects": manifest["injected_defects"]},
    )
    return 0
