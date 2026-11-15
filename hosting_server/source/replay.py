"""Applies replay events to the OLTP DB. Every write is idempotent (INSERT IGNORE / rank-guarded UPDATE)."""
import pandas as pd

from db import insert_ignore, py
from olist_data import DATA_DIR, EVENT_KINDS, MILESTONES, ORDER_DATES, STATUS_LIFECYCLE, read_csv

STATUS_SQL_LIST = ",".join(f"'{s}'" for s in STATUS_LIFECYCLE)

CUSTOMER_COLS = ["customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"]
ORDER_COLS = ["order_id", "customer_id", "order_status"] + ORDER_DATES
ITEM_COLS = ["order_id", "order_item_id", "product_id", "seller_id", "shipping_limit_date", "price", "freight_value"]
PAYMENT_COLS = ["order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value"]
REVIEW_COLS = ["review_id", "order_id", "review_score", "review_comment_title", "review_comment_message",
               "review_creation_date", "review_answer_timestamp"]
# Set only by later milestone events, so a freshly purchased order starts without them
LATER_DATES = [col for col, _ in MILESTONES.values()]


class Replayer:
    def __init__(self, data_dir=DATA_DIR):
        self.orders = read_csv("orders", data_dir, parse_dates=ORDER_DATES).set_index("order_id")
        self.customers = read_csv("customers", data_dir,
                                  dtype={"customer_zip_code_prefix": str}).set_index("customer_id")
        self.items = read_csv("order_items", data_dir, parse_dates=["shipping_limit_date"])
        self.items = self.items.set_index("order_id").sort_index()
        self.payments = read_csv("order_payments", data_dir).set_index("order_id").sort_index()
        self.reviews = read_csv("order_reviews", data_dir,
                                parse_dates=["review_creation_date", "review_answer_timestamp"])

    def apply(self, cur, ev):
        """Apply a slice of the event table (already sorted by ts) using an open cursor."""
        for kind in EVENT_KINDS:
            e = ev[ev.kind == kind]
            if e.empty:
                continue
            if kind in MILESTONES:
                self._milestone(cur, e, *MILESTONES[kind])
            else:
                getattr(self, f"_{kind.lower()}")(cur, e)

    @staticmethod
    def _sub(df, ids):
        return df.loc[df.index.intersection(ids)].reset_index()

    def _purchase(self, cur, e):
        ids = e.order_id.tolist()
        o = self.orders.loc[ids].reset_index()
        cust = self.customers.loc[o.customer_id.unique()].reset_index()
        insert_ignore(cur, "customers", cust, CUSTOMER_COLS)
        o["order_status"] = "created"
        o[LATER_DATES] = pd.NaT
        insert_ignore(cur, "orders", o, ORDER_COLS)
        insert_ignore(cur, "order_items", self._sub(self.items, ids), ITEM_COLS)
        insert_ignore(cur, "order_payments", self._sub(self.payments, ids), PAYMENT_COLS)

    def _milestone(self, cur, e, column, status):
        """Set a business timestamp from the CSV and move the status forward only (never backwards)."""
        vals = self.orders.loc[e.order_id, column]
        data = [(py(v), oid) for oid, v in zip(e.order_id, vals)]
        cur.executemany(
            f"UPDATE orders SET {column} = %s, order_status = IF(FIELD(order_status,{STATUS_SQL_LIST})"
            f" < FIELD('{status}',{STATUS_SQL_LIST}), '{status}', order_status) WHERE order_id = %s", data)

    def _review(self, cur, e):
        insert_ignore(cur, "order_reviews", self.reviews.loc[e.review_idx.astype(int).tolist()], REVIEW_COLS)

    def _terminal(self, cur, e):
        cur.executemany("UPDATE orders SET order_status = %s WHERE order_id = %s",
                        list(zip(e.status, e.order_id)))
