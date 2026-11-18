"""Applies replay events to the OLTP DB. Every write is idempotent (INSERT IGNORE / rank-guarded UPDATE)."""
import os

import numpy as np
import pandas as pd

DATA = os.environ.get("DATA_DIR", "/data")
STATUS_ORDER = "'created','approved','invoiced','processing','shipped','delivered'"
KIND_ORDER = ["PURCHASE", "APPROVED", "SHIPPED", "DELIVERED", "REVIEW", "TERMINAL"]
ORDER_DATES = ["order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date",
               "order_delivered_customer_date", "order_estimated_delivery_date"]


def py(v):
    """numpy / pandas scalar -> plain python value pymysql can encode (NaN/NaT -> None)."""
    if v is None or v is pd.NaT:
        return None
    if isinstance(v, float) and np.isnan(v):
        return None
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime()
    if isinstance(v, np.generic):
        return v.item()
    return v


def rows(df, cols):
    return [tuple(py(v) for v in r) for r in df[cols].astype(object).itertuples(index=False, name=None)]


class Replayer:
    def __init__(self, data_dir=DATA):
        self.orders = pd.read_csv(f"{data_dir}/olist_orders_dataset.csv", parse_dates=ORDER_DATES).set_index("order_id")
        self.customers = pd.read_csv(f"{data_dir}/olist_customers_dataset.csv",
                                     dtype={"customer_zip_code_prefix": str}).set_index("customer_id")
        self.items = pd.read_csv(f"{data_dir}/olist_order_items_dataset.csv", parse_dates=["shipping_limit_date"])
        self.items = self.items.set_index("order_id").sort_index()
        self.payments = pd.read_csv(f"{data_dir}/olist_order_payments_dataset.csv").set_index("order_id").sort_index()
        self.reviews = pd.read_csv(f"{data_dir}/olist_order_reviews_dataset.csv",
                                   parse_dates=["review_creation_date", "review_answer_timestamp"])

    def _sub(self, df, ids):
        return df.loc[df.index.intersection(ids)].reset_index()

    def apply(self, cur, ev):
        """Apply a slice of the event table (already sorted by ts) using an open cursor."""
        for kind in KIND_ORDER:
            e = ev[ev.kind == kind]
            if e.empty:
                continue
            getattr(self, f"_{kind.lower()}")(cur, e)

    def _purchase(self, cur, e):
        ids = e.order_id.tolist()
        o = self.orders.loc[ids].reset_index()
        cust = self.customers.loc[o.customer_id.unique()].reset_index()
        cur.executemany(
            "INSERT IGNORE INTO customers (customer_id, customer_unique_id, customer_zip_code_prefix,"
            " customer_city, customer_state) VALUES (%s,%s,%s,%s,%s)",
            rows(cust, ["customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city",
                        "customer_state"]))
        o["order_status"] = "created"
        for c in ("order_approved_at", "order_delivered_carrier_date", "order_delivered_customer_date"):
            o[c] = pd.NaT
        cur.executemany(
            "INSERT IGNORE INTO orders (order_id, customer_id, order_status, order_purchase_timestamp,"
            " order_approved_at, order_delivered_carrier_date, order_delivered_customer_date,"
            " order_estimated_delivery_date) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
            rows(o, ["order_id", "customer_id", "order_status", "order_purchase_timestamp", "order_approved_at",
                     "order_delivered_carrier_date", "order_delivered_customer_date",
                     "order_estimated_delivery_date"]))
        it = self._sub(self.items, ids)
        if len(it):
            cur.executemany(
                "INSERT IGNORE INTO order_items (order_id, order_item_id, product_id, seller_id,"
                " shipping_limit_date, price, freight_value) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                rows(it, ["order_id", "order_item_id", "product_id", "seller_id", "shipping_limit_date", "price",
                          "freight_value"]))
        pay = self._sub(self.payments, ids)
        if len(pay):
            cur.executemany(
                "INSERT IGNORE INTO order_payments (order_id, payment_sequential, payment_type,"
                " payment_installments, payment_value) VALUES (%s,%s,%s,%s,%s)",
                rows(pay, ["order_id", "payment_sequential", "payment_type", "payment_installments",
                           "payment_value"]))

    def _set_field(self, cur, e, column, status):
        """Set a business timestamp from the CSV and move the status forward only (never backwards)."""
        vals = self.orders.loc[e.order_id, column]
        data = [(py(v), oid) for oid, v in zip(e.order_id, vals)]
        cur.executemany(
            f"UPDATE orders SET {column} = %s, order_status = IF(FIELD(order_status,{STATUS_ORDER})"
            f" < FIELD('{status}',{STATUS_ORDER}), '{status}', order_status) WHERE order_id = %s", data)

    def _approved(self, cur, e):
        self._set_field(cur, e, "order_approved_at", "approved")

    def _shipped(self, cur, e):
        self._set_field(cur, e, "order_delivered_carrier_date", "shipped")

    def _delivered(self, cur, e):
        self._set_field(cur, e, "order_delivered_customer_date", "delivered")

    def _review(self, cur, e):
        rv = self.reviews.loc[e.review_idx.astype(int).tolist()]
        cur.executemany(
            "INSERT IGNORE INTO order_reviews (review_id, order_id, review_score, review_comment_title,"
            " review_comment_message, review_creation_date, review_answer_timestamp)"
            " VALUES (%s,%s,%s,%s,%s,%s,%s)",
            rows(rv, ["review_id", "order_id", "review_score", "review_comment_title", "review_comment_message",
                      "review_creation_date", "review_answer_timestamp"]))

    def _terminal(self, cur, e):
        cur.executemany("UPDATE orders SET order_status = %s WHERE order_id = %s",
                        list(zip(e.status, e.order_id)))
