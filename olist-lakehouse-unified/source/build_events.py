"""Build the time-ordered event table the simulator replays.

One row per event: ts (business time), kind, order_id, status (target status), review_idx.
Kinds: PURCHASE, APPROVED, SHIPPED, DELIVERED, REVIEW, TERMINAL (see plan_source_ingestion.md section 3.3).
"""
import os

import numpy as np
import pandas as pd

DATA = os.environ.get("DATA_DIR", "/data")
OUT = os.environ.get("EVENTS_PATH", "/app/events.parquet")

RANK = {s: i for i, s in enumerate(["created", "approved", "invoiced", "processing", "shipped", "delivered"])}
PRIORITY = {"PURCHASE": 0, "APPROVED": 1, "SHIPPED": 2, "DELIVERED": 3, "REVIEW": 4, "TERMINAL": 5}
DATE_COLS = ["order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date",
             "order_delivered_customer_date", "order_estimated_delivery_date"]


def simulate_status(g):
    """Replay rank-guarded status moves for events of one order (same rule the simulator applies in SQL)."""
    cur = None
    for kind, status in zip(g.kind, g.status):
        if kind == "PURCHASE":
            cur = "created"
        elif kind in ("APPROVED", "SHIPPED", "DELIVERED"):
            if RANK[status] > RANK[cur]:
                cur = status
        elif kind == "TERMINAL":
            cur = status
    return cur


def main():
    o = pd.read_csv(f"{DATA}/olist_orders_dataset.csv", parse_dates=DATE_COLS)
    r = pd.read_csv(f"{DATA}/olist_order_reviews_dataset.csv", parse_dates=["review_answer_timestamp"])

    parts = []

    def add(df, ts_col, kind, status=None):
        d = df[df[ts_col].notna()][["order_id", ts_col]].rename(columns={ts_col: "ts"})
        d["kind"], d["status"], d["review_idx"] = kind, status, pd.NA
        parts.append(d)

    add(o, "order_purchase_timestamp", "PURCHASE", "created")
    add(o, "order_approved_at", "APPROVED", "approved")
    add(o, "order_delivered_carrier_date", "SHIPPED", "shipped")

    # delivered orders missing the delivery date fall back to the estimated date (plan decision 3)
    o["_deliv_ts"] = o.order_delivered_customer_date.where(
        o.order_delivered_customer_date.notna() | (o.order_status != "delivered"),
        o.order_estimated_delivery_date)
    add(o, "_deliv_ts", "DELIVERED", "delivered")

    rv = r[["order_id", "review_answer_timestamp"]].rename(columns={"review_answer_timestamp": "ts"})
    rv["kind"], rv["status"], rv["review_idx"] = "REVIEW", None, r.index.astype("Int64")
    parts.append(rv)

    ev = pd.concat(parts, ignore_index=True)
    ev["prio"] = ev.kind.map(PRIORITY)
    ev = ev.sort_values(["order_id", "ts", "prio"])

    # no event may precede its purchase (some CSV rows have approved_at < purchase); clamp them
    purchase = o.set_index("order_id").order_purchase_timestamp
    ev["ts"] = np.maximum(ev.ts, ev.order_id.map(purchase))
    print("events clamped to purchase time:", int((ev.ts == ev.order_id.map(purchase)).sum() - len(o)))

    # final status must match the CSV: add a forcing TERMINAL event where the rank-guarded replay differs
    ev = ev.sort_values(["order_id", "ts", "prio"])
    sim = ev[ev.kind != "REVIEW"].groupby("order_id")[["kind", "status"]].apply(simulate_status)
    want = o.set_index("order_id").order_status
    bad = want[sim.reindex(want.index) != want]
    last_ts = ev.groupby("order_id").ts.max()
    term = pd.DataFrame({"order_id": bad.index, "ts": last_ts.reindex(bad.index).values + pd.Timedelta(seconds=1),
                         "kind": "TERMINAL", "status": bad.values, "review_idx": pd.NA, "prio": 5})
    ev = pd.concat([ev, term], ignore_index=True).sort_values(["ts", "prio", "order_id"]).reset_index(drop=True)
    ev["review_idx"] = ev.review_idx.astype("Int64")

    # --- self-checks ---
    assert (ev.kind == "PURCHASE").sum() == len(o) == ev[ev.kind == "PURCHASE"].order_id.nunique()
    assert (ev.kind == "REVIEW").sum() == len(r)
    final = ev[ev.kind != "REVIEW"].groupby("order_id")[["kind", "status"]].apply(simulate_status)
    assert (final.reindex(want.index) == want).all(), "final status mismatch"
    first = ev.groupby("order_id").ts.min()
    assert (first.reindex(purchase.index) >= purchase).all(), "event before purchase"

    ev[["ts", "kind", "order_id", "status", "review_idx"]].to_parquet(OUT, index=False)
    print(f"events: {len(ev):,}  range {ev.ts.min()} -> {ev.ts.max()}")
    print(ev.kind.value_counts().to_dict())
    print(f"forced TERMINAL events: {len(term):,}")


if __name__ == "__main__":
    main()
