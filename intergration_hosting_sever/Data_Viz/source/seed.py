"""Seed reference tables (products, sellers, geolocation, category translation) from the Olist CSVs.
Idempotent: INSERT IGNORE for small tables; geolocation is skipped if already populated."""
import os

import numpy as np
import pandas as pd

import db

DATA = os.environ.get("DATA_DIR", "/data")
CHUNK = 20_000


def rows(df):
    df = df.astype(object).where(df.notna(), None)
    return [tuple(r) for r in df.itertuples(index=False, name=None)]


def load(cur, table, df):
    cols = list(df.columns)
    sql = f"INSERT IGNORE INTO {table} ({','.join(cols)}) VALUES ({','.join(['%s'] * len(cols))})"
    data = rows(df)
    for i in range(0, len(data), CHUNK):
        cur.executemany(sql, data[i:i + CHUNK])
    return len(data)


def main():
    conn = db.connect()
    cur = conn.cursor()

    cat = pd.read_csv(f"{DATA}/product_category_name_translation.csv", encoding="utf-8-sig")
    print("product_category_name_translation", load(cur, "product_category_name_translation", cat))

    prod = pd.read_csv(f"{DATA}/olist_products_dataset.csv")
    int_cols = [c for c in prod.columns if c not in ("product_id", "product_category_name")]
    prod[int_cols] = prod[int_cols].astype("Int64")
    print("products", load(cur, "products", prod))

    sel = pd.read_csv(f"{DATA}/olist_sellers_dataset.csv", dtype={"seller_zip_code_prefix": str})
    print("sellers", load(cur, "sellers", sel))

    cur.execute("SELECT COUNT(*) FROM geolocation")
    if cur.fetchone()[0] == 0:
        geo = pd.read_csv(f"{DATA}/olist_geolocation_dataset.csv", dtype={"geolocation_zip_code_prefix": str})
        print("geolocation", load(cur, "geolocation", geo))
    else:
        print("geolocation already seeded, skipped")

    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
