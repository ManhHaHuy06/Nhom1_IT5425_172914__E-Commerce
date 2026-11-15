"""Reconcile the OLTP DB against the original CSVs (plan step 5). Exit code 1 if any check fails."""
import sys
import warnings

import pandas as pd

import db
from olist_data import ORDER_DATES, TRANSACTION_TABLES, read_csv, read_orders

MONEY_TOLERANCE = 0.01
# pandas warns on every read_sql with a raw DBAPI connection; plain SELECTs on pymysql work fine
warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy connectable")


class Checker:
    def __init__(self):
        self.ok = True

    def __call__(self, name, cond, detail=""):
        self.ok &= bool(cond)
        print("PASS" if cond else "FAIL", name, detail)


def main():
    conn = db.connect()
    q = lambda sql: pd.read_sql(sql, conn)
    check = Checker()

    for t in TRANSACTION_TABLES:
        n_csv = len(read_csv(t))
        n_db = int(q(f"SELECT COUNT(*) c FROM {t}").c[0])
        check(f"rowcount {t}", n_csv == n_db, f"csv={n_csv:,} db={n_db:,}")

    o_csv = read_orders().set_index("order_id").sort_index()
    o_db = q("SELECT * FROM orders").set_index("order_id").sort_index()
    check("order_status identical", (o_csv.order_status == o_db.order_status).all(),
          str(o_db.order_status.value_counts().to_dict()))
    for c in ORDER_DATES:
        same = ((o_csv[c] == pd.to_datetime(o_db[c])) | (o_csv[c].isna() & o_db[c].isna())).all()
        check(f"orders.{c} identical", same)

    i_csv = read_csv("order_items")
    i_db = q("SELECT SUM(price) p, SUM(freight_value) f FROM order_items")
    check("sum(price)/sum(freight)",
          abs(i_csv.price.sum() - float(i_db.p[0])) < MONEY_TOLERANCE
          and abs(i_csv.freight_value.sum() - float(i_db.f[0])) < MONEY_TOLERANCE,
          f"{i_csv.price.sum():.2f} / {i_csv.freight_value.sum():.2f}")

    p_csv = read_csv("order_payments").payment_value.sum()
    p_db = float(q("SELECT SUM(payment_value) s FROM order_payments").s[0])
    check("sum(payment_value)", abs(p_csv - p_db) < MONEY_TOLERANCE)

    r_csv = read_csv("order_reviews").review_score.value_counts().sort_index().to_dict()
    r_db = (q("SELECT review_score, COUNT(*) c FROM order_reviews GROUP BY 1")
            .set_index("review_score").c.sort_index().to_dict())
    check("review_score distribution", r_csv == r_db, str(r_db))

    conn.close()
    print("ALL PASS" if check.ok else "SOME FAILED")
    return 0 if check.ok else 1


if __name__ == "__main__":
    sys.exit(main())
