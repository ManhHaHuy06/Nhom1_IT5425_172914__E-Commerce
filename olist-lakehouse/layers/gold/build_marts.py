"""Build simple Gold marts from Silver and export Parquet for ClickHouse."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common.spark_utils import write_table

EXPORT_ROOT = "s3a://lakehouse/exports/gold"
GOLD_TABLES = (
    "sales_performance_daily",
    "delivery_delay_analysis",
    "seller_scorecard",
)


def export_table(spark: SparkSession, table: str) -> None:
    path = f"{EXPORT_ROOT}/{table}"
    spark.table(f"iceberg.gold.{table}").write.mode("overwrite").parquet(path)
    print(f"Exported iceberg.gold.{table} -> {path}")


def build_sales_performance_daily(spark: SparkSession) -> DataFrame:
    """Revenue / freight by purchase date, category (EN), customer state."""
    items = spark.table("iceberg.silver.fact_order_items").alias("i")
    orders = spark.table("iceberg.silver.fact_orders").alias("o")
    products = spark.table("iceberg.silver.dim_product").alias("p")
    categories = spark.table("iceberg.silver.dim_category").alias("c")
    customers = spark.table("iceberg.silver.dim_customer").alias("cu")
    dates = spark.table("iceberg.silver.dim_date").alias("d")

    return (
        items.join(orders, F.col("i.order_key") == F.col("o.order_key"), "inner")
        .join(products, F.col("i.product_key") == F.col("p.product_key"), "left")
        .join(categories, F.col("p.category_key") == F.col("c.category_key"), "left")
        .join(customers, F.col("o.customer_key") == F.col("cu.customer_key"), "left")
        .join(dates, F.col("i.date_key") == F.col("d.date_key"), "left")
        .groupBy(
            F.col("d.date").alias("sale_date"),
            F.coalesce(F.col("c.product_category_name_english"), F.lit("unknown")).alias(
                "product_category_name_english"
            ),
            F.coalesce(F.col("cu.state"), F.lit("unknown")).alias("customer_state"),
        )
        .agg(
            F.countDistinct("o.order_id").cast("long").alias("order_count"),
            F.count(F.lit(1)).cast("long").alias("item_count"),
            F.sum("i.price").cast("double").alias("total_revenue"),
            F.sum("i.freight_value").cast("double").alias("total_freight"),
            F.avg("i.freight_value").cast("double").alias("avg_freight"),
        )
        .filter(F.col("sale_date").isNotNull())
    )


def build_delivery_delay_analysis(spark: SparkSession) -> DataFrame:
    """Late delivery rate by customer state x seller (delivered orders only)."""
    orders = spark.table("iceberg.silver.fact_orders").filter(F.col("is_delivered") == 1)
    items = (
        spark.table("iceberg.silver.fact_order_items")
        .select("order_key", "seller_key")
        .dropDuplicates(["order_key", "seller_key"])
    )
    customers = spark.table("iceberg.silver.dim_customer").select("customer_key", "state")
    sellers = spark.table("iceberg.silver.dim_seller").select("seller_key", "seller_id")

    base = (
        orders.join(items, "order_key", "inner")
        .join(customers, "customer_key", "left")
        .join(sellers, "seller_key", "left")
        .withColumn("is_late", (F.col("delivery_delay_days") > 0).cast("int"))
    )

    return (
        base.groupBy(
            F.coalesce(F.col("state"), F.lit("unknown")).alias("customer_state"),
            F.coalesce(F.col("seller_id"), F.lit("unknown")).alias("seller_id"),
        )
        .agg(
            F.count(F.lit(1)).cast("long").alias("delivered_orders"),
            F.sum("is_late").cast("long").alias("late_orders"),
            F.avg("delivery_delay_days").cast("double").alias("avg_delay_days"),
            F.avg("delivery_days").cast("double").alias("avg_delivery_days"),
        )
        .withColumn(
            "late_rate",
            F.when(
                F.col("delivered_orders") > 0,
                F.col("late_orders").cast("double") / F.col("delivered_orders"),
            ).otherwise(F.lit(0.0)),
        )
        .select(
            "customer_state",
            "seller_id",
            "delivered_orders",
            "late_orders",
            "late_rate",
            "avg_delay_days",
            "avg_delivery_days",
        )
    )


def build_seller_scorecard(spark: SparkSession) -> DataFrame:
    """Per-seller revenue, cancel rate, review score, carrier handoff days."""
    items = spark.table("iceberg.silver.fact_order_items").alias("i")
    orders = spark.table("iceberg.silver.fact_orders").alias("o")
    sellers = spark.table("iceberg.silver.dim_seller").alias("s")
    reviews = spark.table("iceberg.silver.fact_reviews").alias("r")

    item_base = (
        items.join(orders, F.col("i.order_key") == F.col("o.order_key"), "inner")
        .join(sellers, F.col("i.seller_key") == F.col("s.seller_key"), "left")
        .withColumn(
            "carrier_handoff_days",
            (
                (F.unix_timestamp("o.order_delivered_carrier_date") - F.unix_timestamp("o.order_purchase_timestamp"))
                / F.lit(86400.0)
            ).cast("double"),
        )
    )

    by_seller = item_base.groupBy(
        F.coalesce(F.col("s.seller_id"), F.lit("unknown")).alias("seller_id"),
        F.coalesce(F.col("s.state"), F.lit("unknown")).alias("seller_state"),
    ).agg(
        F.countDistinct("o.order_id").cast("long").alias("order_count"),
        F.count(F.lit(1)).cast("long").alias("item_count"),
        F.sum("i.price").cast("double").alias("total_revenue"),
        F.countDistinct(F.when(F.col("o.is_canceled") == 1, F.col("o.order_id"))).cast("long").alias(
            "canceled_orders"
        ),
        F.avg("carrier_handoff_days").cast("double").alias("avg_carrier_handoff_days"),
    )

    review_by_seller = (
        items.join(reviews, F.col("i.order_key") == F.col("r.order_key"), "inner")
        .join(sellers, F.col("i.seller_key") == F.col("s.seller_key"), "left")
        .groupBy(F.coalesce(F.col("s.seller_id"), F.lit("unknown")).alias("seller_id"))
        .agg(F.avg("r.review_score").cast("double").alias("avg_review_score"))
    )

    return (
        by_seller.join(review_by_seller, "seller_id", "left")
        .withColumn(
            "cancel_rate",
            F.when(
                F.col("order_count") > 0,
                F.col("canceled_orders").cast("double") / F.col("order_count"),
            ).otherwise(F.lit(0.0)),
        )
        .select(
            "seller_id",
            "seller_state",
            "order_count",
            "item_count",
            "total_revenue",
            "canceled_orders",
            "cancel_rate",
            "avg_review_score",
            "avg_carrier_handoff_days",
        )
    )


def build_marts(spark: SparkSession) -> None:
    write_table(build_sales_performance_daily(spark), "iceberg.gold.sales_performance_daily")
    write_table(build_delivery_delay_analysis(spark), "iceberg.gold.delivery_delay_analysis")
    write_table(build_seller_scorecard(spark), "iceberg.gold.seller_scorecard")

    for table in GOLD_TABLES:
        export_table(spark, table)
    print("Gold marts + Parquet export complete")
