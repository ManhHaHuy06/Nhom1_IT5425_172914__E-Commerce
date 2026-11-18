"""Build optimized Olist Gold marts from Iceberg Silver using PySpark.

Reads iceberg.silver.* tables, writes iceberg.gold.* through
common.spark_utils.write_table, then exports Parquet to MinIO.
"""
from __future__ import annotations

from typing import Dict
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from common.spark_utils import write_table

GOLD_NAMESPACE = "iceberg.gold"
EXPORT_ROOT = "s3a://lakehouse/exports/gold"

GOLD_TABLES = (
    "sales_detail", "orders", "payments_by_order", "reviews",
    "seller_order", "customer_order_bridge",
    "sales_performance_daily", "sales_performance_monthly",
    "category_performance_daily", "category_performance_monthly",
    "product_performance_monthly",
    "geography_performance_daily", "geography_performance_monthly",
    "order_status_daily", "order_status_monthly",
    "delivery_performance_daily", "delivery_performance_monthly",
    "order_hourly_daily", "order_hourly_monthly", "order_weekday_monthly",
    "review_score_daily", "review_score_monthly",
    "review_geography_monthly", "review_category_monthly",
    "customer_lifetime", "customer_activity_monthly", "customer_activity_daily",
)


def silver(spark: SparkSession, name: str) -> DataFrame:
    return spark.table(f"iceberg.silver.{name}")


def write_gold(df: DataFrame, name: str) -> None:
    target = f"{GOLD_NAMESPACE}.{name}"
    write_table(df, target)
    print(f"[GOLD] Written {target}")


def export_table(spark: SparkSession, name: str) -> None:
    source = f"{GOLD_NAMESPACE}.{name}"
    path = f"{EXPORT_ROOT}/{name}"
    spark.table(source).write.mode("overwrite").format("parquet").save(path)
    print(f"[EXPORT] {source} -> {path}")


def build_base(spark: SparkSession) -> Dict[str, DataFrame]:
    o = silver(spark, "fact_orders")
    i = silver(spark, "fact_order_items")
    s = silver(spark, "dim_seller").select(
        "seller_key", "seller_id",
        F.col("city").alias("seller_city"), F.col("state").alias("seller_state")
    ).dropDuplicates(["seller_key"])
    p = silver(spark, "dim_product").select(
        "product_key", "product_id", "category_key", "product_category_name"
    ).dropDuplicates(["product_key"])
    c = silver(spark, "dim_category").select(
        "category_key", "product_category_name_english"
    ).dropDuplicates(["category_key"])
    cu = silver(spark, "dim_customer").select(
        "customer_key", "customer_unique_id",
        F.col("city").alias("customer_city"),
        F.col("state").alias("customer_state"),
        F.col("geolocation_key").alias("customer_geolocation_key"),
    ).dropDuplicates(["customer_key"])
    g = silver(spark, "dim_geolocation").select(
        "geolocation_key", "latitude", "longitude"
    ).dropDuplicates(["geolocation_key"])

    orders = o.select(
        "order_key", "order_id", "customer_key", "purchase_date_key",
        "order_status", "order_purchase_timestamp", "order_approved_at",
        "order_delivered_carrier_date", "order_delivered_customer_date",
        "order_estimated_delivery_date", "delivery_days",
        "estimated_delivery_days", "delivery_delay_days",
        "is_delivered", "is_canceled",
    ).dropDuplicates(["order_key", "order_id"])
    orders = (
        orders
        .withColumn("purchase_date", F.to_date("order_purchase_timestamp"))
        .withColumn("month", F.trunc("order_purchase_timestamp", "month").cast("date"))
        .withColumn("purchase_hour", F.hour("order_purchase_timestamp"))
        .withColumn("weekday_num", ((F.dayofweek("order_purchase_timestamp") + 5) % 7).cast("int"))
        .withColumn("order_status", F.coalesce("order_status", F.lit("unknown")))
    )

    detail = (
        i.select(
            "item_key", "order_key", "order_id", "order_item_id",
            "product_key", "seller_key", "date_key", "shipping_limit_date",
            "price", "freight_value", "item_total",
        )
        .join(orders, ["order_key", "order_id"], "inner")
        .join(s, "seller_key", "left")
        .join(p, "product_key", "left")
        .join(c, "category_key", "left")
        .join(cu, "customer_key", "left")
        .join(g, F.col("customer_geolocation_key") == F.col("geolocation_key"), "left")
        .drop("geolocation_key", "customer_geolocation_key")
        .withColumn(
            "product_category_name_english",
            F.coalesce("product_category_name_english", "product_category_name", F.lit("unknown")),
        )
        .withColumn("seller_id", F.coalesce("seller_id", F.lit("unknown")))
        .withColumn("seller_state", F.coalesce("seller_state", F.lit("unknown")))
        .withColumn("customer_state", F.coalesce("customer_state", F.lit("unknown")))
        .withColumn("revenue", F.coalesce(F.col("price").cast("double"), F.lit(0.0)))
        .withColumn("item_total", F.coalesce(F.col("item_total").cast("double"), F.col("revenue")))
    )

    seller_order = detail.groupBy("seller_id", "order_id").agg(
        F.first("order_key", True).alias("order_key"),
        F.first("customer_key", True).alias("customer_key"),
        F.first("customer_unique_id", True).alias("customer_unique_id"),
        F.first("customer_state", True).alias("customer_state"),
        F.first("order_purchase_timestamp", True).alias("order_purchase_timestamp"),
        F.first("purchase_date", True).alias("purchase_date"),
        F.first("month", True).alias("month"),
        F.first("purchase_hour", True).alias("purchase_hour"),
        F.first("weekday_num", True).alias("weekday_num"),
        F.first("order_status", True).alias("order_status"),
        F.max("is_delivered").cast("long").alias("is_delivered"),
        F.max("is_canceled").cast("long").alias("is_canceled"),
        F.first("delivery_days", True).alias("delivery_days"),
        F.first("estimated_delivery_days", True).alias("estimated_delivery_days"),
        F.first("delivery_delay_days", True).alias("delivery_delay_days"),
        F.first("seller_state", True).alias("seller_state"),
        F.sum("revenue").cast("double").alias("revenue"),
        F.countDistinct("item_key").cast("long").alias("item_count"),
    )
    return {"orders": orders, "detail": detail, "seller_order": seller_order}


def build_payments(spark: SparkSession) -> DataFrame:
    return silver(spark, "fact_payments").groupBy("order_id").agg(
        F.sum("payment_value").cast("double").alias("payment_value"),
        F.count("*").cast("long").alias("payment_transaction_count"),
        F.countDistinct("payment_type").cast("long").alias("payment_type_count"),
    )


def build_reviews(spark: SparkSession) -> DataFrame:
    w = Window.partitionBy("order_id").orderBy(
        F.col("review_creation_date").desc_nulls_last(),
        F.col("review_key").desc_nulls_last(),
    )
    return (
        silver(spark, "fact_reviews")
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1).drop("_rn")
        .select(
            "review_key", "order_key", "order_id", "review_id", "review_score",
            "review_comment_title", "review_comment_message",
            "review_creation_date", "review_answer_timestamp", "is_positive", "is_negative",
        )
    )


def build_marts(spark: SparkSession) -> Dict[str, DataFrame]:
    base = build_base(spark)
    d, o, so = base["detail"], base["orders"], base["seller_order"]
    pay, rev = build_payments(spark), build_reviews(spark)

    detail = d.join(
        rev.select(
            "order_id", "review_score", "is_positive", "is_negative",
            "review_comment_title", "review_comment_message",
            "review_creation_date", "review_answer_timestamp",
        ), "order_id", "left"
    )
    orders_gold = o.join(pay, "order_id", "left").join(
        rev.select("order_id", "review_score", "is_positive", "is_negative", "review_creation_date"),
        "order_id", "left"
    )

    sales_daily = d.groupBy("seller_id", "purchase_date").agg(
        F.countDistinct("order_id").cast("long").alias("order_count"),
        F.countDistinct("item_key").cast("long").alias("item_count"),
        F.sum("revenue").cast("double").alias("total_revenue"),
        F.sum("freight_value").cast("double").alias("total_freight"),
        F.avg("freight_value").cast("double").alias("avg_freight"),
    )
    sales_monthly = d.groupBy("seller_id", "month").agg(
        F.countDistinct("order_id").cast("long").alias("order_count"),
        F.countDistinct("item_key").cast("long").alias("item_count"),
        F.sum("revenue").cast("double").alias("total_revenue"),
        F.sum("freight_value").cast("double").alias("total_freight"),
        F.avg("freight_value").cast("double").alias("avg_freight"),
        F.avg("price").cast("double").alias("avg_item_price"),
    )

    category_daily = d.groupBy("seller_id", "purchase_date", "product_category_name_english").agg(
        F.countDistinct("order_id").cast("long").alias("order_count"),
        F.countDistinct("item_key").cast("long").alias("item_count"),
        F.sum("revenue").cast("double").alias("total_revenue"),
    )
    category_monthly = d.groupBy("seller_id", "month", "product_category_name_english").agg(
        F.countDistinct("order_id").cast("long").alias("order_count"),
        F.countDistinct("item_key").cast("long").alias("item_count"),
        F.sum("revenue").cast("double").alias("total_revenue"),
    )
    product_monthly = d.groupBy(
        "seller_id", "month", "product_key", "product_id", "product_category_name_english"
    ).agg(
        F.countDistinct("order_id").cast("long").alias("order_count"),
        F.countDistinct("item_key").cast("long").alias("item_count"),
        F.sum("revenue").cast("double").alias("total_revenue"),
        F.avg("price").cast("double").alias("avg_item_price"),
    )

    geography_daily = d.groupBy("seller_id", "purchase_date", "customer_state").agg(
        F.countDistinct("order_id").cast("long").alias("order_count"),
        F.countDistinct("customer_unique_id").cast("long").alias("customer_count"),
        F.sum("revenue").cast("double").alias("total_revenue"),
    )
    geography_monthly = d.groupBy("seller_id", "month", "customer_state").agg(
        F.countDistinct("order_id").cast("long").alias("order_count"),
        F.countDistinct("customer_unique_id").cast("long").alias("customer_count"),
        F.sum("revenue").cast("double").alias("total_revenue"),
    )

    status_daily = so.groupBy("seller_id", "purchase_date", "order_status").agg(
        F.countDistinct("order_id").cast("long").alias("order_count")
    )
    status_monthly = so.groupBy("seller_id", "month", "order_status").agg(
        F.countDistinct("order_id").cast("long").alias("order_count")
    )

    def delivery_mart(keys: list[str]) -> DataFrame:
        x = so.groupBy(*keys).agg(
            F.countDistinct("order_id").cast("long").alias("order_count"),
            F.sum(F.coalesce(F.col("is_delivered"), F.lit(0))).cast("long").alias("delivered_orders"),
            F.sum(F.coalesce(F.col("is_canceled"), F.lit(0))).cast("long").alias("canceled_orders"),
            F.sum(F.when((F.col("is_delivered") == 1) & (F.col("delivery_delay_days") > 0), 1).otherwise(0)).cast("long").alias("late_orders"),
            F.sum("delivery_days").cast("double").alias("delivery_days_sum"),
            F.count("delivery_days").cast("long").alias("delivery_days_count"),
            F.sum("delivery_delay_days").cast("double").alias("delay_days_sum"),
            F.count("delivery_delay_days").cast("long").alias("delay_days_count"),
        )
        return x.withColumn(
            "late_rate",
            F.when(F.col("delivered_orders") > 0,
                   F.col("late_orders").cast("double") / F.col("delivered_orders"))
             .otherwise(F.lit(0.0))
        )

    delivery_daily = delivery_mart(["seller_id", "purchase_date"])
    delivery_monthly = delivery_mart(["seller_id", "month"])

    hourly_daily = so.groupBy("seller_id", "purchase_date", "purchase_hour").agg(
        F.countDistinct("order_id").cast("long").alias("order_count")
    )
    hourly_monthly = so.groupBy("seller_id", "month", "purchase_hour").agg(
        F.countDistinct("order_id").cast("long").alias("order_count")
    )
    weekday_monthly = so.groupBy("seller_id", "month", "weekday_num").agg(
        F.countDistinct("order_id").cast("long").alias("order_count")
    )

    review_order = so.join(
        rev.select("order_id", "review_score"), "order_id", "inner"
    )
    review_score_daily = review_order.groupBy("seller_id", "purchase_date", "review_score").agg(
        F.countDistinct("order_id").cast("long").alias("reviewed_orders")
    )
    review_score_monthly = review_order.groupBy("seller_id", "month", "review_score").agg(
        F.countDistinct("order_id").cast("long").alias("reviewed_orders")
    )
    review_geography_monthly = review_order.groupBy(
        "seller_id", "month", "customer_state", "review_score"
    ).agg(F.countDistinct("order_id").cast("long").alias("reviewed_orders"))

    order_category = d.select(
        "seller_id", "order_id", "month", "product_category_name_english"
    ).dropDuplicates(["seller_id", "order_id", "product_category_name_english"])
    review_category_monthly = order_category.join(
        rev.select("order_id", "review_score"), "order_id", "inner"
    ).groupBy("seller_id", "month", "product_category_name_english", "review_score").agg(
        F.countDistinct("order_id").cast("long").alias("reviewed_orders")
    )

    customer_lifetime = d.groupBy("seller_id", "customer_unique_id").agg(
        F.countDistinct("order_id").cast("long").alias("order_count"),
        F.countDistinct("item_key").cast("long").alias("item_count"),
        F.sum("revenue").cast("double").alias("total_revenue"),
        F.min("purchase_date").alias("first_purchase_date"),
        F.max("purchase_date").alias("last_purchase_date"),
    )
    customer_monthly = so.filter(F.col("customer_unique_id").isNotNull()).groupBy(
        "seller_id", "month", "customer_unique_id"
    ).agg(
        F.countDistinct("order_id").cast("long").alias("order_count"),
        F.sum("revenue").cast("double").alias("total_revenue"),
        F.first("customer_state", True).alias("customer_state"),
    )
    customer_daily = so.filter(F.col("customer_unique_id").isNotNull()).groupBy(
        "seller_id", "purchase_date", "customer_unique_id"
    ).agg(
        F.countDistinct("order_id").cast("long").alias("order_count"),
        F.sum("revenue").cast("double").alias("total_revenue"),
    )

    return {
        "sales_detail": detail,
        "orders": orders_gold,
        "payments_by_order": pay,
        "reviews": rev,
        "seller_order": so,
        "customer_order_bridge": so.select(
            "seller_id", "customer_unique_id", "order_id", "purchase_date",
            "month", "revenue", "item_count"
        ),
        "sales_performance_daily": sales_daily,
        "sales_performance_monthly": sales_monthly,
        "category_performance_daily": category_daily,
        "category_performance_monthly": category_monthly,
        "product_performance_monthly": product_monthly,
        "geography_performance_daily": geography_daily,
        "geography_performance_monthly": geography_monthly,
        "order_status_daily": status_daily,
        "order_status_monthly": status_monthly,
        "delivery_performance_daily": delivery_daily,
        "delivery_performance_monthly": delivery_monthly,
        "order_hourly_daily": hourly_daily,
        "order_hourly_monthly": hourly_monthly,
        "order_weekday_monthly": weekday_monthly,
        "review_score_daily": review_score_daily,
        "review_score_monthly": review_score_monthly,
        "review_geography_monthly": review_geography_monthly,
        "review_category_monthly": review_category_monthly,
        "customer_lifetime": customer_lifetime,
        "customer_activity_monthly": customer_monthly,
        "customer_activity_daily": customer_daily,
    }


def build_gold(spark: SparkSession) -> None:
    print(f"[START] Gold namespace={GOLD_NAMESPACE}; export={EXPORT_ROOT}")
    marts = build_marts(spark)

    # Write Iceberg Gold tables; DDL must be applied before this step.
    for name in GOLD_TABLES:
        if name not in marts:
            raise RuntimeError(f"Missing Gold DataFrame: {name}")
        print(f"[WRITE] {name}")
        write_gold(marts[name], name)

    # Export committed Iceberg tables to MinIO for ClickHouse.
    for name in GOLD_TABLES:
        export_table(spark, name)

    print(f"[DONE] {len(GOLD_TABLES)} Gold marts written and exported.")


# Giữ cách tích hợp giống job Gold cũ của dự án:
# entrypoint/orchestrator hiện tại tạo SparkSession rồi gọi:
#     from build_gold_marts import build_gold
#     build_gold(spark)
