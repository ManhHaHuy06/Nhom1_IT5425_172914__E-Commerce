"""Reconcile the OLTP DB against the original CSVs (plan step 5)."""
import os

import pandas as pd

import db

DATA = os.environ.get("DATA_DIR", "/data")
conn = db.connect()
q = lambda sql: pd.read_sql(sql, conn)
ok = True


def check(name, cond, detail=""):
    global ok
    ok &= bool(cond)
    print(("PASS" if cond else "FAIL"), name, detail)


csv = {
    "customers": "olist_customers_dataset", "orders": "olist_orders_dataset",
    "order_items": "olist_order_items_dataset", "order_payments": "olist_order_payments_dataset",
    "order_reviews": "olist_order_reviews_dataset",
}
for t, f in csv.items():
    n_csv = len(pd.read_csv(f"{DATA}/{f}.csv"))
    n_db = int(q(f"SELECT COUNT(*) c FROM {t}").c[0])
    check(f"rowcount {t}", n_csv == n_db, f"csv={n_csv:,} db={n_db:,}")

o_csv = pd.read_csv(f"{DATA}/olist_orders_dataset.csv", parse_dates=[3, 4, 5, 6, 7]).set_index("order_id").sort_index()
o_db = q("SELECT * FROM orders").set_index("order_id").sort_index()
check("order_status identical", (o_csv.order_status == o_db.order_status).all(),
      str(o_db.order_status.value_counts().to_dict()))
for c in ["order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date",
          "order_delivered_customer_date", "order_estimated_delivery_date"]:
    same = ((o_csv[c] == pd.to_datetime(o_db[c])) | (o_csv[c].isna() & o_db[c].isna())).all()
    check(f"orders.{c} identical", same)

i_csv = pd.read_csv(f"{DATA}/olist_order_items_dataset.csv")
i_db = q("SELECT SUM(price) p, SUM(freight_value) f FROM order_items")
check("sum(price)/sum(freight)", abs(i_csv.price.sum() - float(i_db.p[0])) < 0.01 and
      abs(i_csv.freight_value.sum() - float(i_db.f[0])) < 0.01, f"{i_csv.price.sum():.2f} / {i_csv.freight_value.sum():.2f}")
p_csv = pd.read_csv(f"{DATA}/olist_order_payments_dataset.csv").payment_value.sum()
check("sum(payment_value)", abs(p_csv - float(q("SELECT SUM(payment_value) s FROM order_payments").s[0])) < 0.01)
r_csv = pd.read_csv(f"{DATA}/olist_order_reviews_dataset.csv").review_score.value_counts().sort_index().to_dict()
r_db = q("SELECT review_score, COUNT(*) c FROM order_reviews GROUP BY 1").set_index("review_score").c.sort_index().to_dict()
check("review_score distribution", r_csv == r_db, str(r_db))
print("ALL PASS" if ok else "SOME FAILED")
