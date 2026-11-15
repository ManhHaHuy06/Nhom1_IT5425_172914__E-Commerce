"""Build the time-ordered event table the simulator replays.

One row per event: ts (business time), kind, order_id, status (target status), review_idx.
Kinds: PURCHASE, APPROVED, SHIPPED, DELIVERED, REVIEW, TERMINAL (see plan_source_ingestion.md section 3.3).
"""
import numpy as np
import pandas as pd

from olist_data import EVENT_PRIORITY, EVENTS_PATH, MILESTONES, STATUS_RANK, read_csv, read_orders


def simulate_status(g):
    """Replay rank-guarded status moves for events of one order (same rule the simulator applies in SQL)."""
    cur = None
    for kind, status in zip(g.kind, g.status):
        if kind == "PURCHASE":
            cur = "created"
        elif kind in MILESTONES:
            if STATUS_RANK[status] > STATUS_RANK[cur]:
                cur = status
        elif kind == "TERMINAL":
            cur = status
    return cur


def final_status(ev):
    return ev[ev.kind != "REVIEW"].groupby("order_id")[["kind", "status"]].apply(simulate_status)


def main():
    o = read_orders()
    r = read_csv("order_reviews", parse_dates=["review_answer_timestamp"])

    parts = []

    def add(df, ts_col, kind, status=None):
        d = df[df[ts_col].notna()][["order_id", ts_col]].rename(columns={ts_col: "ts"})
        d["kind"], d["status"], d["review_idx"] = kind, status, pd.NA
        parts.append(d)

    add(o, "order_purchase_timestamp", "PURCHASE", "created")
    for kind in ("APPROVED", "SHIPPED"):
        col, status = MILESTONES[kind]
        add(o, col, kind, status)

    # delivered orders missing the delivery date fall back to the estimated date (plan decision 3)
    deliv_col, deliv_status = MILESTONES["DELIVERED"]
    o["_deliv_ts"] = o[deliv_col].where(o[deliv_col].notna() | (o.order_status != "delivered"),
                                        o.order_estimated_delivery_date)
    add(o, "_deliv_ts", "DELIVERED", deliv_status)

    rv = r[["order_id", "review_answer_timestamp"]].rename(columns={"review_answer_timestamp": "ts"})
    rv["kind"], rv["status"], rv["review_idx"] = "REVIEW", None, r.index.astype("Int64")
    parts.append(rv)

    ev = pd.concat(parts, ignore_index=True)
    ev["prio"] = ev.kind.map(EVENT_PRIORITY)
    ev = ev.sort_values(["order_id", "ts", "prio"])

    # no event may precede its purchase (some CSV rows have approved_at < purchase); clamp them
    purchase = o.set_index("order_id").order_purchase_timestamp
    ev["ts"] = np.maximum(ev.ts, ev.order_id.map(purchase))
    print("events clamped to purchase time:", int((ev.ts == ev.order_id.map(purchase)).sum() - len(o)))

    # final status must match the CSV: add a forcing TERMINAL event where the rank-guarded replay differs
    ev = ev.sort_values(["order_id", "ts", "prio"])
    want = o.set_index("order_id").order_status
    bad = want[final_status(ev).reindex(want.index) != want]
    last_ts = ev.groupby("order_id").ts.max()
    term = pd.DataFrame({"order_id": bad.index, "ts": last_ts.reindex(bad.index).values + pd.Timedelta(seconds=1),
                         "kind": "TERMINAL", "status": bad.values, "review_idx": pd.NA,
                         "prio": EVENT_PRIORITY["TERMINAL"]})
    ev = pd.concat([ev, term], ignore_index=True).sort_values(["ts", "prio", "order_id"]).reset_index(drop=True)
    ev["review_idx"] = ev.review_idx.astype("Int64")

    # --- self-checks ---
    assert (ev.kind == "PURCHASE").sum() == len(o) == ev[ev.kind == "PURCHASE"].order_id.nunique()
    assert (ev.kind == "REVIEW").sum() == len(r)
    assert (final_status(ev).reindex(want.index) == want).all(), "final status mismatch"
    first = ev.groupby("order_id").ts.min()
    assert (first.reindex(purchase.index) >= purchase).all(), "event before purchase"

    ev[["ts", "kind", "order_id", "status", "review_idx"]].to_parquet(EVENTS_PATH, index=False)
    print(f"events: {len(ev):,}  range {ev.ts.min()} -> {ev.ts.max()}")
    print(ev.kind.value_counts().to_dict())
    print(f"forced TERMINAL events: {len(term):,}")


if __name__ == "__main__":
    main()
