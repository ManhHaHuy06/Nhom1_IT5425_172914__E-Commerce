"""Olist CSV access and order-lifecycle constants shared by the simulator scripts."""
import os

import pandas as pd

DATA_DIR = os.environ.get("DATA_DIR", "/data")
EVENTS_PATH = os.environ.get("EVENTS_PATH", "/app/events.parquet")

# OLTP table -> CSV file stem
CSV_FILES = {
    "customers": "olist_customers_dataset",
    "orders": "olist_orders_dataset",
    "order_items": "olist_order_items_dataset",
    "order_payments": "olist_order_payments_dataset",
    "order_reviews": "olist_order_reviews_dataset",
    "products": "olist_products_dataset",
    "sellers": "olist_sellers_dataset",
    "geolocation": "olist_geolocation_dataset",
    "product_category_name_translation": "product_category_name_translation",
}
TRANSACTION_TABLES = ["customers", "orders", "order_items", "order_payments", "order_reviews"]

ORDER_DATES = ["order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date",
               "order_delivered_customer_date", "order_estimated_delivery_date"]

# Forward-only status lifecycle; canceled/unavailable/... are set by TERMINAL events.
STATUS_LIFECYCLE = ["created", "approved", "invoiced", "processing", "shipped", "delivered"]
STATUS_RANK = {s: i for i, s in enumerate(STATUS_LIFECYCLE)}

# Event kinds in apply order; the index is also the tie-break priority for events with the same ts.
EVENT_KINDS = ["PURCHASE", "APPROVED", "SHIPPED", "DELIVERED", "REVIEW", "TERMINAL"]
EVENT_PRIORITY = {k: i for i, k in enumerate(EVENT_KINDS)}
# Milestone event -> (orders timestamp column it sets, status it moves the order to)
MILESTONES = {
    "APPROVED": ("order_approved_at", "approved"),
    "SHIPPED": ("order_delivered_carrier_date", "shipped"),
    "DELIVERED": ("order_delivered_customer_date", "delivered"),
}


def read_csv(table, data_dir=None, **kwargs):
    return pd.read_csv(f"{data_dir or DATA_DIR}/{CSV_FILES[table]}.csv", **kwargs)


def read_orders(data_dir=None):
    return read_csv("orders", data_dir, parse_dates=ORDER_DATES)
