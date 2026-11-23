"""Load the Olist CSVs into one order-level table (grain: ONE ROW PER ORDER).

Follows the ETL rule "aggregate order_items / payments / reviews first, then join", so there is no fan-out.
`original_only=True` drops the ~50k generated orders that live in `olist_augmented_data/` (their order_id is a
36-char UUID, the original ids have 32 chars): the models are trained and evaluated on real orders only.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

DATE_COLS = ["order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date",
             "order_delivered_customer_date", "order_estimated_delivery_date"]
NON_SALE = ("canceled", "unavailable")


def haversine_km(lat1, lon1, lat2, lon2):
    p = np.pi / 180
    a = 0.5 - np.cos((lat2 - lat1) * p) / 2 + np.cos(lat1 * p) * np.cos(lat2 * p) * (1 - np.cos((lon2 - lon1) * p)) / 2
    return 12742 * np.arcsin(np.sqrt(a))


def add_features(df: pd.DataFrame) -> pd.DataFrame:
    """Derived columns from the raw order columns. Shared by the CSV loader and the ClickHouse/Spark export."""
    df = df.copy()
    P = df.order_purchase_timestamp
    df["distance_km"] = haversine_km(df.c_lat, df.c_lng, df.s_lat, df.s_lng)
    df["same_state"] = (df.customer_state == df.seller_state).astype(int)
    df["freight_ratio"] = df.freight / df.price.clip(lower=1)
    df["hour"], df["dow"], df["month"] = P.dt.hour, P.dt.dayofweek, P.dt.month
    df["est_days"] = (df.order_estimated_delivery_date - P).dt.total_seconds() / 86400
    df["delivery_days"] = (df.order_delivered_customer_date - P).dt.total_seconds() / 86400
    df["late"] = (df.order_delivered_customer_date > df.order_estimated_delivery_date).astype(float).where(df.order_delivered_customer_date.notna())
    df["date"] = P.dt.normalize()
    return df


def load_orders(data_dir: str | Path, original_only: bool = True) -> pd.DataFrame:
    d = Path(data_dir)
    rd = lambda f, **k: pd.read_csv(d / f, **k)
    orders = rd("olist_orders_dataset.csv", parse_dates=DATE_COLS)
    if original_only:
        orders = orders[orders.order_id.str.len() == 32]
    cust = rd("olist_customers_dataset.csv")
    items = rd("olist_order_items_dataset.csv")
    prod = rd("olist_products_dataset.csv")
    sell = rd("olist_sellers_dataset.csv")
    pay = rd("olist_order_payments_dataset.csv")
    rev = rd("olist_order_reviews_dataset.csv", usecols=["order_id", "review_score"])
    trans = rd("product_category_name_translation.csv")
    geo = (rd("olist_geolocation_dataset.csv", usecols=["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng"])
           .groupby("geolocation_zip_code_prefix").agg(lat=("geolocation_lat", "mean"), lng=("geolocation_lng", "mean")))

    it = items.merge(prod[["product_id", "product_category_name", "product_weight_g", "product_length_cm",
                           "product_height_cm", "product_width_cm"]], on="product_id", how="left")
    it = it.merge(trans, on="product_category_name", how="left")
    it["vol"] = it.product_length_cm * it.product_height_cm * it.product_width_cm
    main = (it.sort_values("price", ascending=False).drop_duplicates("order_id")
              [["order_id", "seller_id", "product_category_name_english"]]
              .rename(columns={"product_category_name_english": "category"}))
    agg = it.groupby("order_id").agg(n_items=("order_item_id", "size"), price=("price", "sum"), freight=("freight_value", "sum"),
                                     n_sellers=("seller_id", "nunique"), weight_g=("product_weight_g", "sum"),
                                     vol_cm3=("vol", "sum")).reset_index()
    pm = pay.sort_values("payment_value", ascending=False).drop_duplicates("order_id")[["order_id", "payment_type"]]
    inst = pay.groupby("order_id").payment_installments.max().rename("installments").reset_index()
    rv = rev.groupby("order_id").review_score.mean().reset_index()

    df = (orders.merge(cust, on="customer_id").merge(agg, on="order_id").merge(main, on="order_id")
          .merge(sell[["seller_id", "seller_zip_code_prefix", "seller_state"]], on="seller_id")
          .merge(pm, on="order_id", how="left").merge(inst, on="order_id", how="left").merge(rv, on="order_id", how="left"))
    df = df.join(geo.rename(columns=lambda c: "c_" + c), on="customer_zip_code_prefix")
    df = df.join(geo.rename(columns=lambda c: "s_" + c), on="seller_zip_code_prefix")

    return add_features(df).reset_index(drop=True)


def daily_series(df: pd.DataFrame, target: str = "orders") -> pd.Series:
    """Daily count of orders (or revenue = sum of item prices) for orders that became sales."""
    v = df[~df.order_status.isin(NON_SALE)]
    s = v.groupby("date").size() if target == "orders" else v.groupby("date").price.sum()
    s = s.reindex(pd.date_range(s.index.min(), s.index.max(), freq="D"), fill_value=0).astype(float)
    s.index.name = "date"
    return s.rename(target)
