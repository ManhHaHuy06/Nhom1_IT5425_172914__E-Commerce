"""Create deterministic Olist-like reference and historical OLTP data.

The uploaded project archives do not include the original Olist CSV dataset. This
seed therefore creates clearly synthetic but relationally consistent demo data.
It is idempotent: if orders already exist, it leaves the OLTP database untouched.
"""
from __future__ import annotations

import os
import random
import uuid
from datetime import datetime, timedelta

import pymysql

HOST = os.getenv("OLTP_HOST", "oltp-mysql")
PORT = int(os.getenv("OLTP_PORT", "3306"))
DB = os.getenv("OLTP_DATABASE", "shop")
USER = os.environ["OLTP_SIM_USER"]
PASSWORD = os.environ["OLTP_SIM_PASSWORD"]
SEED = int(os.getenv("SIM_SEED", "42"))
INITIAL_ORDERS = int(os.getenv("SIM_INITIAL_ORDERS", "12000"))
BATCH = 1000

CITIES = [
    ("sao paulo", "SP", -23.5505, -46.6333), ("rio de janeiro", "RJ", -22.9068, -43.1729),
    ("belo horizonte", "MG", -19.9167, -43.9345), ("brasilia", "DF", -15.7939, -47.8828),
    ("curitiba", "PR", -25.4284, -49.2733), ("porto alegre", "RS", -30.0346, -51.2177),
    ("salvador", "BA", -12.9777, -38.5016), ("recife", "PE", -8.0476, -34.8770),
    ("fortaleza", "CE", -3.7319, -38.5267), ("goiania", "GO", -16.6869, -49.2648),
]
CATEGORIES = [
    "beleza_saude", "esporte_lazer", "informatica_acessorios", "moveis_decoracao",
    "utilidades_domesticas", "relogios_presentes", "telefonia", "cama_mesa_banho",
    "brinquedos", "automotivo", "perfumaria", "ferramentas_jardim",
    "eletrodomesticos", "fashion_bolsas_e_acessorios", "unknown",
]
PAYMENTS = ["credit_card", "boleto", "voucher", "debit_card"]


def ident():
    return uuid.uuid4().hex


def chunks(rows, size=BATCH):
    for start in range(0, len(rows), size):
        yield rows[start:start + size]


def bulk(cur, sql, rows):
    for part in chunks(rows):
        cur.executemany(sql, part)


def connect():
    return pymysql.connect(host=HOST, port=PORT, user=USER, password=PASSWORD,
                           database=DB, charset="utf8mb4", autocommit=False,
                           connect_timeout=20)


def main():
    rng = random.Random(SEED)
    conn = connect()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM orders")
    existing = int(cur.fetchone()[0])
    if existing:
        print(f"[SEED] Existing OLTP data detected ({existing:,} orders); preserving database.")
        cur.execute("INSERT IGNORE INTO sim_state(id, virtual_ts, speed, status) VALUES (1, '2019-01-01 00:00:00', 60, 'running')")
        conn.commit()
        conn.close()
        return

    print("[SEED] Creating synthetic reference data...")
    geo_rows, zip_locations = [], []
    for idx in range(1200):
        city, state, lat, lng = CITIES[idx % len(CITIES)]
        zip_code = f"{10000 + idx:05d}"
        zip_locations.append((zip_code, city, state, lat, lng))
        for point in range(1 + (idx % 3)):
            geo_rows.append((zip_code, lat + rng.uniform(-0.025, 0.025),
                             lng + rng.uniform(-0.025, 0.025), city, state))
    categories = [(name, name.replace("_", " ")) for name in CATEGORIES]
    bulk(cur, "INSERT INTO product_category_name_translation(product_category_name, product_category_name_english) VALUES (%s,%s)", categories)

    sellers = []
    for i in range(100):
        z, city, state, _, _ = rng.choice(zip_locations)
        sellers.append((ident(), z, city, state))
    bulk(cur, "INSERT INTO sellers(seller_id,seller_zip_code_prefix,seller_city,seller_state) VALUES (%s,%s,%s,%s)", sellers)

    products = []
    for i in range(500):
        products.append((ident(), rng.choice(CATEGORIES), rng.randint(10, 80), rng.randint(100, 2500),
                         rng.randint(1, 8), rng.randint(100, 12000), rng.randint(5, 100),
                         rng.randint(2, 80), rng.randint(2, 80)))
    bulk(cur, """INSERT INTO products(product_id,product_category_name,product_name_lenght,
        product_description_lenght,product_photos_qty,product_weight_g,product_length_cm,
        product_height_cm,product_width_cm) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""", products)
    bulk(cur, """INSERT INTO geolocation(geolocation_zip_code_prefix,geolocation_lat,
        geolocation_lng,geolocation_city,geolocation_state) VALUES (%s,%s,%s,%s,%s)""", geo_rows)

    customers = []
    for i in range(5000):
        z, city, state, _, _ = rng.choice(zip_locations)
        unique_group = i % 3200
        unique_id = f"{unique_group:032x}"
        customers.append((ident(), unique_id, z, city, state))
    bulk(cur, """INSERT INTO customers(customer_id,customer_unique_id,customer_zip_code_prefix,
        customer_city,customer_state) VALUES (%s,%s,%s,%s,%s)""", customers)

    print(f"[SEED] Creating {INITIAL_ORDERS:,} historical orders and child rows...")
    seller_ids = [x[0] for x in sellers]
    product_ids = [x[0] for x in products]
    customer_ids = [x[0] for x in customers]
    orders, items, payments, reviews = [], [], [], []
    base = datetime(2016, 1, 1)
    for n in range(INITIAL_ORDERS):
        order_id = ident()
        customer_id = rng.choice(customer_ids)
        purchase = base + timedelta(minutes=rng.randint(0, 365 * 3 * 24 * 60))
        status = rng.choices(
            ["delivered", "shipped", "processing", "approved", "created", "canceled"],
            weights=[68, 8, 6, 5, 4, 9], k=1
        )[0]
        approved = purchase + timedelta(minutes=rng.randint(2, 240)) if status != "created" else None
        carrier = None
        delivered = None
        if status in ("shipped", "delivered"):
            carrier = (approved or purchase) + timedelta(days=rng.randint(1, 4))
        if status == "delivered":
            delivered = carrier + timedelta(days=rng.randint(1, 12))
        estimated = purchase + timedelta(days=rng.randint(12, 28))
        orders.append((order_id, customer_id, status, purchase, approved, carrier, delivered, estimated))

        item_count = rng.randint(1, 3)
        total = 0.0
        for item_no in range(1, item_count + 1):
            price = round(rng.uniform(15, 650), 2)
            freight = round(rng.uniform(4, 45), 2)
            total += price + freight
            items.append((order_id, item_no, rng.choice(product_ids), rng.choice(seller_ids),
                          purchase + timedelta(days=rng.randint(1, 7)), price, freight))
        payments.append((order_id, 1, rng.choice(PAYMENTS), rng.randint(1, 10), round(total, 2)))
        if status == "delivered" and rng.random() < 0.68:
            review_ts = delivered + timedelta(days=rng.randint(1, 15))
            reviews.append((ident(), order_id, rng.choices([1, 2, 3, 4, 5], [4, 6, 15, 30, 45])[0],
                            None, None, review_ts, review_ts + timedelta(days=1)))

    bulk(cur, """INSERT INTO orders(order_id,customer_id,order_status,order_purchase_timestamp,
        order_approved_at,order_delivered_carrier_date,order_delivered_customer_date,
        order_estimated_delivery_date) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""", orders)
    bulk(cur, """INSERT INTO order_items(order_id,order_item_id,product_id,seller_id,
        shipping_limit_date,price,freight_value) VALUES (%s,%s,%s,%s,%s,%s,%s)""", items)
    bulk(cur, """INSERT INTO order_payments(order_id,payment_sequential,payment_type,
        payment_installments,payment_value) VALUES (%s,%s,%s,%s,%s)""", payments)
    bulk(cur, """INSERT INTO order_reviews(review_id,order_id,review_score,review_comment_title,
        review_comment_message,review_creation_date,review_answer_timestamp)
        VALUES (%s,%s,%s,%s,%s,%s,%s)""", reviews)
    cur.execute("INSERT INTO sim_state(id,virtual_ts,speed,status) VALUES (1,'2019-01-01 00:00:00',60,'running')")
    conn.commit()
    print(f"[SEED] customers={len(customers):,} products={len(products):,} sellers={len(sellers):,} "
          f"geolocation={len(geo_rows):,} orders={len(orders):,} items={len(items):,} "
          f"payments={len(payments):,} reviews={len(reviews):,}")
    conn.close()


if __name__ == "__main__":
    main()
