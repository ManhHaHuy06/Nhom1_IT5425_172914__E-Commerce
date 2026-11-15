"""Seed reference tables (products, sellers, geolocation, category translation) from the Olist CSVs.
Idempotent: INSERT IGNORE for small tables; geolocation is skipped if already populated."""
import db
from olist_data import read_csv

CHUNK = 20_000


def main():
    conn = db.connect()
    cur = conn.cursor()

    cat = read_csv("product_category_name_translation", encoding="utf-8-sig")
    print("product_category_name_translation",
          db.insert_ignore(cur, "product_category_name_translation", cat, chunk=CHUNK))

    prod = read_csv("products")
    int_cols = [c for c in prod.columns if c not in ("product_id", "product_category_name")]
    prod[int_cols] = prod[int_cols].astype("Int64")
    print("products", db.insert_ignore(cur, "products", prod, chunk=CHUNK))

    sel = read_csv("sellers", dtype={"seller_zip_code_prefix": str})
    print("sellers", db.insert_ignore(cur, "sellers", sel, chunk=CHUNK))

    cur.execute("SELECT COUNT(*) FROM geolocation")
    if cur.fetchone()[0] == 0:
        geo = read_csv("geolocation", dtype={"geolocation_zip_code_prefix": str})
        print("geolocation", db.insert_ignore(cur, "geolocation", geo, chunk=CHUNK))
    else:
        print("geolocation already seeded, skipped")

    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
