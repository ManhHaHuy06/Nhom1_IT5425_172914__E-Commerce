"""Build Silver fact tables from Bronze and Silver dimensions (aligned with schema.sql)."""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common.spark_utils import latest, read_bronze, write_table


def days_between(later: str, earlier: str) -> F.Column:
    """Whole days as INT (schema.sql delivery_*_days INT)."""
    return ((F.unix_timestamp(later) - F.unix_timestamp(earlier)) / F.lit(86400.0)).cast("int")


def earlier_than(left: str, right: str) -> F.Column:
    """True when both timestamps exist and left < right (invalid lifecycle order)."""
    return (
        F.col(left).isNotNull()
        & F.col(right).isNotNull()
        & (F.col(left) < F.col(right))
    )


# Drop orders that violate purchase -> approved -> carrier -> customer timeline.
ORDER_TIMELINE_RULES = (
    ("A1", "order_approved_at", "order_purchase_timestamp"),
    ("A2", "order_delivered_carrier_date", "order_purchase_timestamp"),
    ("A3", "order_delivered_carrier_date", "order_approved_at"),
    ("A4", "order_delivered_customer_date", "order_delivered_carrier_date"),
    ("A5", "order_delivered_customer_date", "order_purchase_timestamp"),
    ("A6", "order_delivered_customer_date", "order_approved_at"),
)


def drop_invalid_order_timeline(orders):
    """Remove orders with inverted timestamps; null dates are allowed and kept."""
    invalid = F.lit(False)
    for code, left, right in ORDER_TIMELINE_RULES:
        rule = earlier_than(left, right)
        print(f"Orders dropped by {code} ({left} < {right}): {orders.filter(rule).count()}")
        invalid = invalid | rule
    kept = orders.filter(~invalid)
    print(f"Orders kept after A1-A6 timeline filter: {kept.count()}")
    return kept


def customer_id_to_key(spark: SparkSession):
    """Resolve order-level customer_id -> customer_key via dim_customer (no map_customer table)."""
    dim = spark.table("iceberg.silver.dim_customer").select("customer_unique_id", "customer_key")
    customers = latest(read_bronze(spark, "olist_customers"), ["customer_id"]).select(
        "customer_id", "customer_unique_id"
    )
    return customers.join(dim, "customer_unique_id", "inner").select("customer_id", "customer_key")


def build_facts(spark: SparkSession) -> None:
    customers = customer_id_to_key(spark)
    products = spark.table("iceberg.silver.dim_product").select("product_id", "product_key")
    sellers = spark.table("iceberg.silver.dim_seller").select("seller_id", "seller_key")

    orders = latest(read_bronze(spark, "olist_orders"), ["order_id"]).filter(F.col("order_id").isNotNull())
    for column in (
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ):
        orders = orders.withColumn(column, F.to_timestamp(column))
    orders = orders.filter(F.col("order_purchase_timestamp").isNotNull())
    orders = orders.join(customers, "customer_id", "left")
    orphan_orders = orders.filter(F.col("customer_key").isNull()).count()
    print(f"Orphan orders without customer_id mapping: {orphan_orders}")
    orders = orders.filter(F.col("customer_key").isNotNull())
    orders = drop_invalid_order_timeline(orders)
    fact_orders = orders.select(
        F.xxhash64("order_id").alias("order_key"),
        "order_id",
        "customer_key",
        F.date_format("order_purchase_timestamp", "yyyyMMdd").cast("int").alias("purchase_date_key"),
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
        days_between("order_delivered_customer_date", "order_purchase_timestamp").alias("delivery_days"),
        days_between("order_estimated_delivery_date", "order_purchase_timestamp").alias(
            "estimated_delivery_days"
        ),
        days_between("order_delivered_customer_date", "order_estimated_delivery_date").alias(
            "delivery_delay_days"
        ),
        (F.col("order_status") == F.lit("delivered")).cast("int").alias("is_delivered"),
        (F.col("order_status") == F.lit("canceled")).cast("int").alias("is_canceled"),
    )
    write_table(fact_orders, "iceberg.silver.fact_orders")
    if spark.table("iceberg.silver.fact_orders").count() == 0:
        raise RuntimeError("fact_orders is empty; refusing to build downstream facts")

    order_keys = spark.table("iceberg.silver.fact_orders").select(
        "order_id", "order_key", "purchase_date_key", "order_purchase_timestamp"
    )
    items = latest(read_bronze(spark, "olist_order_items"), ["order_id", "order_item_id"])
    items = items.filter(F.col("order_id").isNotNull() & F.col("order_item_id").isNotNull())
    items = items.withColumn("order_item_id", F.col("order_item_id").cast("int"))
    items = items.withColumn("price", F.col("price").cast("double")).withColumn(
        "freight_value", F.col("freight_value").cast("double")
    )
    linked = (
        items.join(order_keys, "order_id", "left")
        .join(products, "product_id", "left")
        .join(sellers, "seller_id", "left")
    )
    print(f"Orphan order items without fact_orders: {linked.filter(F.col('order_key').isNull()).count()}")
    print(f"Order items without product: {linked.filter(F.col('product_key').isNull()).count()}")
    print(f"Order items without seller: {linked.filter(F.col('seller_key').isNull()).count()}")
    fact_items = linked.filter(
        F.col("order_key").isNotNull()
        & F.col("product_key").isNotNull()
        & F.col("seller_key").isNotNull()
    ).select(
        F.xxhash64("order_id", "order_item_id").alias("item_key"),
        "order_key",
        "order_id",
        "order_item_id",
        "product_key",
        "seller_key",
        F.col("purchase_date_key").alias("date_key"),
        F.to_timestamp("shipping_limit_date").alias("shipping_limit_date"),
        "price",
        "freight_value",
        (F.col("price") + F.col("freight_value")).cast("double").alias("item_total"),
    )
    write_table(fact_items, "iceberg.silver.fact_order_items")

    payments = latest(read_bronze(spark, "olist_order_payments"), ["order_id", "payment_sequential"])
    payments = payments.filter(F.col("order_id").isNotNull() & F.col("payment_sequential").isNotNull())
    payments = payments.join(order_keys.select("order_id", "order_key"), "order_id", "left")
    print(f"Orphan payments without fact_orders: {payments.filter(F.col('order_key').isNull()).count()}")
    fact_payments = payments.filter(F.col("order_key").isNotNull()).select(
        F.xxhash64("order_id", "payment_sequential").alias("payment_key"),
        "order_key",
        "order_id",
        F.col("payment_sequential").cast("int").alias("payment_sequential"),
        "payment_type",
        F.col("payment_installments").cast("int").alias("payment_installments"),
        F.col("payment_value").cast("double").alias("payment_value"),
    )
    write_table(fact_payments, "iceberg.silver.fact_payments")

    reviews = latest(read_bronze(spark, "olist_order_reviews"), ["review_id"]).filter(
        F.col("review_id").isNotNull()
    )
    reviews = reviews.join(order_keys.select("order_id", "order_key"), "order_id", "left")
    print(f"Orphan reviews without fact_orders: {reviews.filter(F.col('order_key').isNull()).count()}")
    score = F.col("review_score").cast("int")
    fact_reviews = reviews.filter(F.col("order_key").isNotNull()).select(
        F.xxhash64("review_id").alias("review_key"),
        "order_key",
        "order_id",
        "review_id",
        score.alias("review_score"),
        "review_comment_title",
        "review_comment_message",
        F.to_timestamp("review_creation_date").alias("review_creation_date"),
        F.to_timestamp("review_answer_timestamp").alias("review_answer_timestamp"),
        (score >= F.lit(4)).cast("int").alias("is_positive"),
        (score <= F.lit(2)).cast("int").alias("is_negative"),
    )
    write_table(fact_reviews, "iceberg.silver.fact_reviews")

    # fact_customer_order (cumulative snapshot per customer_key)
    orders_f = spark.table("iceberg.silver.fact_orders")
    spent = (
        spark.table("iceberg.silver.fact_payments")
        .join(orders_f.select("order_key", "customer_key"), "order_key", "inner")
        .groupBy("customer_key")
        .agg(F.sum("payment_value").cast("double").alias("total_spent"))
    )
    per_customer = orders_f.groupBy("customer_key").agg(
        F.count(F.lit(1)).cast("long").alias("order_count"),
        F.sum("is_delivered").cast("long").alias("delivered_order_count"),
        F.sum("is_canceled").cast("long").alias("canceled_order_count"),
        F.min("order_purchase_timestamp").alias("first_order_timestamp"),
        F.max("order_purchase_timestamp").alias("last_order_timestamp"),
        F.avg(F.when(F.col("is_delivered") == 1, F.col("delivery_days"))).cast("double").alias(
            "average_delivery_days"
        ),
    )
    fact_customer_order = (
        per_customer.join(spent, "customer_key", "left")
        .withColumn("total_spent", F.coalesce(F.col("total_spent"), F.lit(0.0)))
        .withColumn(
            "average_order_value",
            F.when(F.col("order_count") > 0, F.col("total_spent") / F.col("order_count")).otherwise(
                F.lit(0.0)
            ),
        )
        .select(
            F.col("customer_key").alias("customer_order_key"),
            "customer_key",
            "order_count",
            "delivered_order_count",
            "canceled_order_count",
            "total_spent",
            F.col("average_order_value").cast("double").alias("average_order_value"),
            "first_order_timestamp",
            "last_order_timestamp",
            "average_delivery_days",
        )
    )
    write_table(fact_customer_order, "iceberg.silver.fact_customer_order")

    missing_dates = spark.sql(
        """
        SELECT COUNT(*) AS missing_dates
        FROM iceberg.silver.fact_orders o
        LEFT ANTI JOIN iceberg.silver.dim_date d ON o.purchase_date_key = d.date_key
        """
    ).collect()[0].missing_dates
    if missing_dates:
        raise RuntimeError(f"fact_orders has {missing_dates} rows whose purchase_date_key is not in dim_date")
    print("Silver fact referential check passed")
