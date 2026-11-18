"""Virtual-time Olist shop simulator.

Creates new orders and advances order lifecycle state in MySQL. MySQL updated_at
columns are wall-clock timestamps used by Airflow incremental extraction, while
business timestamps follow the accelerated virtual clock stored in sim_state.
"""
from __future__ import annotations

import argparse
import os
import random
import time
import uuid
from datetime import datetime, timedelta

import pymysql

HOST = os.getenv("OLTP_HOST", "oltp-mysql")
PORT = int(os.getenv("OLTP_PORT", "3306"))
DB = os.getenv("OLTP_DATABASE", "shop")
USER = os.environ["OLTP_SIM_USER"]
PASSWORD = os.environ["OLTP_SIM_PASSWORD"]
SPEED = float(os.getenv("SIM_SPEED", "60"))
INTERVAL = float(os.getenv("SIM_INTERVAL_SECONDS", "30"))
ORDERS_PER_TICK = int(os.getenv("SIM_ORDERS_PER_TICK", "2"))
rng = random.Random()


def ident():
    return uuid.uuid4().hex


def connect():
    return pymysql.connect(host=HOST, port=PORT, user=USER, password=PASSWORD,
                           database=DB, charset="utf8mb4", autocommit=False,
                           connect_timeout=20)


def current_state(cur):
    cur.execute("SELECT virtual_ts,speed,status FROM sim_state WHERE id=1")
    row = cur.fetchone()
    if not row:
        raise RuntimeError("sim_state missing; run the OLTP seed service first")
    return row


def create_order(cur, virtual_now):
    if rng.random() < 0.08:
        cur.execute("SELECT customer_zip_code_prefix,customer_city,customer_state FROM customers ORDER BY RAND() LIMIT 1")
        zip_code, city, state = cur.fetchone()
        customer_id = ident()
        cur.execute(
            "INSERT INTO customers(customer_id,customer_unique_id,customer_zip_code_prefix,customer_city,customer_state) "
            "VALUES (%s,%s,%s,%s,%s)",
            (customer_id, ident(), zip_code, city, state),
        )
    else:
        cur.execute("SELECT customer_id FROM customers ORDER BY RAND() LIMIT 1")
        customer_id = cur.fetchone()[0]
    cur.execute("SELECT product_id FROM products ORDER BY RAND() LIMIT 30")
    products = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT seller_id FROM sellers ORDER BY RAND() LIMIT 30")
    sellers = [r[0] for r in cur.fetchall()]
    if not products or not sellers:
        raise RuntimeError("Reference products/sellers are empty")

    order_id = ident()
    cur.execute(
        """INSERT INTO orders(order_id,customer_id,order_status,order_purchase_timestamp,
           order_estimated_delivery_date) VALUES (%s,%s,'created',%s,%s)""",
        (order_id, customer_id, virtual_now, virtual_now + timedelta(days=rng.randint(10, 28))),
    )
    total = 0.0
    for item_no in range(1, rng.randint(2, 4)):
        price, freight = round(rng.uniform(20, 500), 2), round(rng.uniform(5, 40), 2)
        total += price + freight
        cur.execute(
            """INSERT INTO order_items(order_id,order_item_id,product_id,seller_id,
               shipping_limit_date,price,freight_value) VALUES (%s,%s,%s,%s,%s,%s,%s)""",
            (order_id, item_no, rng.choice(products), rng.choice(sellers),
             virtual_now + timedelta(days=3), price, freight),
        )
    cur.execute(
        """INSERT INTO order_payments(order_id,payment_sequential,payment_type,
           payment_installments,payment_value) VALUES (%s,1,%s,%s,%s)""",
        (order_id, rng.choice(["credit_card", "boleto", "voucher", "debit_card"]),
         rng.randint(1, 6), round(total, 2)),
    )
    return order_id


def advance_orders(cur, virtual_now):
    # Move a bounded number of orders forward; timestamps remain monotonic.
    cur.execute(
        """SELECT order_id,order_status,order_purchase_timestamp,order_approved_at,
                  order_delivered_carrier_date
           FROM orders
           WHERE order_status IN ('created','approved','processing','shipped')
           ORDER BY updated_at ASC LIMIT 40"""
    )
    rows = cur.fetchall()
    changed = 0
    for order_id, status, purchase, approved, carrier in rows:
        age = virtual_now - purchase
        if status == "created" and age >= timedelta(minutes=20):
            if rng.random() < 0.025:
                cur.execute("UPDATE orders SET order_status='canceled' WHERE order_id=%s", (order_id,))
            else:
                cur.execute("UPDATE orders SET order_status='approved',order_approved_at=%s WHERE order_id=%s",
                            (max(virtual_now, purchase + timedelta(minutes=1)), order_id))
            changed += 1
        elif status == "approved" and age >= timedelta(hours=1):
            cur.execute("UPDATE orders SET order_status='processing' WHERE order_id=%s", (order_id,))
            if rng.random() < 0.12:
                cur.execute(
                    "INSERT IGNORE INTO order_payments(order_id,payment_sequential,payment_type,payment_installments,payment_value) "
                    "VALUES (%s,2,%s,%s,%s)",
                    (order_id, "credit_card", 1, round(rng.uniform(5, 80), 2)),
                )
            changed += 1
        elif status == "processing" and age >= timedelta(days=1):
            carrier_ts = max(virtual_now, approved + timedelta(minutes=1))
            cur.execute("UPDATE orders SET order_status='shipped',order_delivered_carrier_date=%s WHERE order_id=%s",
                        (carrier_ts, order_id))
            changed += 1
        elif status == "shipped" and age >= timedelta(days=3):
            delivered_ts = max(virtual_now, carrier + timedelta(minutes=1))
            cur.execute("UPDATE orders SET order_status='delivered',order_delivered_customer_date=%s WHERE order_id=%s",
                        (delivered_ts, order_id))
            changed += 1
            if rng.random() < 0.75:
                review_ts = delivered_ts + timedelta(days=1)
                cur.execute(
                    """INSERT IGNORE INTO order_reviews(review_id,order_id,review_score,
                       review_comment_title,review_comment_message,review_creation_date,
                       review_answer_timestamp) VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                    (ident(), order_id, rng.choices([1, 2, 3, 4, 5], [5, 7, 14, 30, 44])[0],
                     None, None, review_ts, review_ts + timedelta(hours=8)),
                )
    return changed


def tick(cur, virtual_now):
    new_ids = [create_order(cur, virtual_now + timedelta(seconds=i)) for i in range(ORDERS_PER_TICK)]
    advanced = advance_orders(cur, virtual_now)
    cur.execute("UPDATE sim_state SET virtual_ts=%s,speed=%s,status='running' WHERE id=1",
                (virtual_now, SPEED))
    return new_ids, advanced


def run():
    conn = connect()
    anchor_wall = time.monotonic()
    cur = conn.cursor()
    v0, speed, status = current_state(cur)
    speed = SPEED
    print(f"[SIM] starting virtual_time={v0} speed={speed}x interval={INTERVAL}s", flush=True)
    while True:
        cur = conn.cursor()
        v0, stored_speed, status = current_state(cur)
        if status == "paused":
            conn.commit()
            time.sleep(2)
            anchor_wall = time.monotonic()
            continue
        elapsed = max(0.0, time.monotonic() - anchor_wall)
        virtual_now = v0 + timedelta(seconds=elapsed * speed)
        try:
            new_ids, advanced = tick(cur, virtual_now)
            conn.commit()
            anchor_wall = time.monotonic()
            print(f"[SIM] virtual={virtual_now:%Y-%m-%d %H:%M:%S} new_orders={len(new_ids)} "
                  f"status_updates={advanced}", flush=True)
        except Exception:
            conn.rollback()
            raise
        time.sleep(INTERVAL)


def control(action):
    conn = connect()
    cur = conn.cursor()
    if action == "status":
        cur.execute("SELECT virtual_ts,speed,status FROM sim_state WHERE id=1")
        print(cur.fetchone())
        for table in ("customers", "orders", "order_items", "order_payments", "order_reviews"):
            cur.execute(f"SELECT COUNT(*) FROM `{table}`")
            print(f"{table}: {cur.fetchone()[0]:,}")
    else:
        cur.execute("UPDATE sim_state SET status=%s WHERE id=1", (action,))
        conn.commit()
        print(f"sim_state.status={action}")
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["run", "pause", "resume", "status"])
    args = parser.parse_args()
    if args.command == "run":
        run()
    elif args.command == "pause":
        control("paused")
    elif args.command == "resume":
        control("running")
    else:
        control("status")
