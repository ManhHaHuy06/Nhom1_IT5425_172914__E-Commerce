"""Stage `predict`: Silver + trọng số ML (MinIO) -> gold.order_predictions + gold.forecast_daily_orders (+ export Parquet).

Spark chỉ lo vào/ra dữ liệu; toàn bộ tính toán ML nằm trong package olist-ml (`olist_ml.predict_core`, thuần pandas, có unit test).
Cách chạy: suy luận trên driver bằng pandas (~100k đơn, vài giây). Import `olist_ml` được hoãn đến khi stage chạy để các stage khác
không phụ thuộc scikit-learn. Chi tiết: docs/ML_PREDICTIONS.md.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from layers.gold.build_marts import GOLD, SILVER, conform, export_table

MODELS_ROOT = os.getenv("OLIST_MODELS_ROOT", "s3a://lakehouse/models/late_risk")
STRICT = os.getenv("OLIST_PREDICT_STRICT", "false").lower() in ("1", "true", "yes")


class ModelMissing(Exception):
    """Chưa có trọng số trên MinIO (models/late_risk/current.json)."""


def _read_text(spark: SparkSession, uri: str) -> str:
    return spark.read.text(uri, wholetext=True).first()[0]


def _read_bytes(spark: SparkSession, uri: str) -> bytes:
    return bytes(spark.read.format("binaryFile").load(uri).first()["content"])


def read_artifacts(spark: SparkSession):
    from olist_ml.predict_core import load_artifacts
    from olist_ml.registry import version_mismatches

    try:
        version = json.loads(_read_text(spark, f"{MODELS_ROOT}/current.json"))["version"]
        root = f"{MODELS_ROOT}/{version}"
        card_json = _read_text(spark, f"{root}/model_card.json")
    except Exception as exc:  # noqa: BLE001 - thiếu file/đường dẫn đều quy về "chưa có trọng số"
        raise ModelMissing(f"không đọc được {MODELS_ROOT}/current.json: {str(exc)[:200]}") from exc
    bad = version_mismatches(json.loads(card_json))
    if bad:  # lệch phiên bản thư viện: dừng hẳn, không dùng pickle sai phiên bản
        raise RuntimeError("Lệch phiên bản thư viện giữa lúc train và lúc chạy: " + "; ".join(bad)
                           + ". Train lại trong container Spark (olist-ml/train_model.bat) rồi publish lại.")
    art = load_artifacts(_read_bytes(spark, f"{root}/late_risk.joblib"), _read_text(spark, f"{root}/forecast_bands.json"),
                         _read_text(spark, f"{root}/thresholds.json"), card_json, version)
    print(f"Model {version} loaded from {root}")
    return art


def order_features(spark: SparkSession) -> DataFrame:
    """1 dòng / đơn, cột giống `olist_ml.data.load_orders` (đặc trưng biết được tại thời điểm đặt hàng)."""
    orders = spark.table(f"{SILVER}.fact_orders")
    items = spark.table(f"{SILVER}.fact_order_items")
    products = spark.table(f"{SILVER}.dim_product").select(
        "product_key", "category_key", "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm")
    categories = spark.table(f"{SILVER}.dim_category").select(
        "category_key", F.col("product_category_name_english").alias("category"))
    sellers = spark.table(f"{SILVER}.dim_seller").select(
        "seller_key", "seller_id", F.col("state").alias("seller_state"), F.col("geolocation_key").alias("s_geo"))
    customers = spark.table(f"{SILVER}.dim_customer").select(
        "customer_key", F.col("state").alias("customer_state"), F.col("geolocation_key").alias("c_geo"))
    geo = spark.table(f"{SILVER}.dim_geolocation").select("geolocation_key", "latitude", "longitude")
    payments = spark.table(f"{SILVER}.fact_payments")

    it = (items.join(products, "product_key", "left").join(categories, "category_key", "left")
          .withColumn("vol", F.col("product_length_cm") * F.col("product_height_cm") * F.col("product_width_cm")))
    agg = it.groupBy("order_id").agg(
        F.count(F.lit(1)).cast("int").alias("n_items"), F.sum("price").alias("price"), F.sum("freight_value").alias("freight"),
        F.countDistinct("seller_key").cast("int").alias("n_sellers"), F.sum("product_weight_g").alias("weight_g"),
        F.sum("vol").alias("vol_cm3"))
    # món đắt nhất quyết định seller và category của đơn (cùng quy tắc với bản CSV)
    w = Window.partitionBy("order_id").orderBy(F.col("price").desc(), F.col("order_item_id"))
    main = (it.withColumn("rn", F.row_number().over(w)).filter("rn = 1").select("order_id", "seller_key", "category")
            .join(sellers, "seller_key", "left")
            .join(geo.select(F.col("geolocation_key").alias("s_geo"), F.col("latitude").alias("s_lat"),
                             F.col("longitude").alias("s_lng")), "s_geo", "left")
            .select("order_id", "seller_id", "seller_state", "s_lat", "s_lng", "category"))
    cust = customers.join(geo.select(F.col("geolocation_key").alias("c_geo"), F.col("latitude").alias("c_lat"),
                                     F.col("longitude").alias("c_lng")), "c_geo", "left") \
        .select("customer_key", "customer_state", "c_lat", "c_lng")
    pw = Window.partitionBy("order_id").orderBy(F.col("payment_value").desc(), F.col("payment_sequential"))
    pay_main = payments.withColumn("rn", F.row_number().over(pw)).filter("rn = 1").select("order_id", "payment_type")
    pay_inst = payments.groupBy("order_id").agg(F.max("payment_installments").alias("installments"))

    return (orders.join(agg, "order_id", "inner")      # đơn không có món hàng thì không phải đơn bán
            .join(main, "order_id", "left").join(cust, "customer_key", "left")
            .join(pay_main, "order_id", "left").join(pay_inst, "order_id", "left")
            .select("order_id", "order_key", "order_status", "order_purchase_timestamp", "order_approved_at",
                    "order_delivered_carrier_date", "order_delivered_customer_date", "order_estimated_delivery_date",
                    "customer_state", "c_lat", "c_lng", "seller_id", "seller_state", "s_lat", "s_lng", "category",
                    "payment_type", "installments", "price", "freight", "n_items", "n_sellers", "weight_g", "vol_cm3"))


def _write(spark: SparkSession, table: str, pdf, columns: list) -> None:
    from olist_ml.predict_core import to_rows

    full = f"{GOLD}.{table}"
    schema = spark.table(full).schema
    df = spark.createDataFrame(to_rows(pdf[columns]), schema=schema)
    conform(df, schema).writeTo(full).overwrite(F.lit(True))   # rebuild toàn bộ, giống các mart khác (idempotent)
    print(f"{full}: {spark.table(full).count()} rows")
    export_table(spark, table)


def build_predictions(spark: SparkSession) -> None:
    import pandas as pd
    from olist_ml.predict_core import (FORECAST_COLUMNS, ORDER_PRED_COLUMNS, daily_orders_series, forecast_orders,
                                       predict_orders)

    scored_at = datetime.now(timezone.utc).replace(tzinfo=None)
    try:
        art = read_artifacts(spark)
    except ModelMissing as exc:
        if STRICT:
            raise
        print(f"WARNING: {exc}\n  -> ghi 2 bảng dự đoán RỖNG (đúng schema) để bước nạp ClickHouse không vỡ. "
              "Publish trọng số rồi chạy lại stage predict (docs/ML_PREDICTIONS.md). Đặt OLIST_PREDICT_STRICT=true để dừng thay vì cảnh báo.")
        art = None
    if art is None:
        orders_pdf, fc_pdf = pd.DataFrame(columns=ORDER_PRED_COLUMNS), pd.DataFrame(columns=FORECAST_COLUMNS)
    else:
        raw = order_features(spark).toPandas()
        orders_pdf = predict_orders(raw, art, scored_at)
        fc_pdf = forecast_orders(daily_orders_series(raw), art, scored_at, complete_through=os.getenv("OLIST_COMPLETE_THROUGH") or None)
        n = max(len(orders_pdf), 1)
        print(f"Scored {len(orders_pdf)} orders; flagged {orders_pdf.is_flagged.sum()} ({orders_pdf.is_flagged.sum() / n:.1%}), "
              f"escalated {orders_pdf.is_escalated.sum()}; forecast rows {len(fc_pdf)}")
    _write(spark, "order_predictions", orders_pdf, ORDER_PRED_COLUMNS)
    _write(spark, "forecast_daily_orders", fc_pdf, FORECAST_COLUMNS)
