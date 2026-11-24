"""Build Silver 3NF transactional entities from Bronze with Data Quality Quarantine."""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common.spark_utils import latest, latest_bronze, read_bronze, write_table
from layers.silver.quarantine import build_rejects, write_quarantine

# Drop and quarantine orders that violate purchase -> approved -> carrier -> customer timeline.
ORDER_TIMELINE_RULES = (
    ("A1", "order_approved_at", "order_purchase_timestamp"),
    ("A2", "order_delivered_carrier_date", "order_purchase_timestamp"),
    ("A3", "order_delivered_carrier_date", "order_approved_at"),
    ("A4", "order_delivered_customer_date", "order_delivered_carrier_date"),
    ("A5", "order_delivered_customer_date", "order_purchase_timestamp"),
    ("A6", "order_delivered_customer_date", "order_approved_at"),
)


def days_between(later: str, earlier: str) -> F.Column:
    """Whole days as INT."""
    return ((F.unix_timestamp(later) - F.unix_timestamp(earlier)) / F.lit(86400.0)).cast("int")


def earlier_than(left: str, right: str) -> F.Column:
    """True when both timestamps exist and left < right (invalid lifecycle order)."""
    return F.col(left).isNotNull() & F.col(right).isNotNull() & (F.col(left) < F.col(right))


def customer_id_to_unique_id(spark: SparkSession):
    """Resolve order-level customer_id -> customer_unique_id present in silver.customers."""
    valid_people = spark.table("iceberg.silver.customers").select("customer_unique_id")
    cust_map = latest_bronze(spark, "olist_customers", ["customer_id"]).select(
        "customer_id", "customer_unique_id"
    )
    return cust_map.join(valid_people, "customer_unique_id", "inner")


def _split_timeline_violations(orders, batch_id: str):
    """Split valid orders vs timeline-violating orders (A1-A6), attaching combined rule codes & reasons."""
    code_cols = [
        F.when(earlier_than(left, right), F.lit(code))
        for code, left, right in ORDER_TIMELINE_RULES
    ]
    reason_cols = [
        F.when(earlier_than(left, right), F.lit(f"{code}: {left} < {right}"))
        for code, left, right in ORDER_TIMELINE_RULES
    ]
    annotated = orders.withColumn("_rule_code", F.concat_ws(",", *code_cols)).withColumn(
        "_reason", F.concat_ws("; ", *reason_cols)
    )
    bad = annotated.filter(F.col("_rule_code") != "")
    good = annotated.filter(F.col("_rule_code") == "").drop("_rule_code", "_reason")
    payload = [
        "order_id", "customer_id", "customer_unique_id", "order_status",
        "order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date",
        "order_delivered_customer_date", "order_estimated_delivery_date",
    ]
    rejects = build_rejects(
        bad, "olist_orders", "silver.orders", F.col("order_id"),
        F.col("_rule_code"), F.col("_reason"), batch_id, payload,
    )
    return good, rejects


def build_transactions(spark: SparkSession, batch_id: str) -> None:
    cust_map = customer_id_to_unique_id(spark)
    valid_products = spark.table("iceberg.silver.products").select(F.col("product_id").alias("_valid_product_id"))
    valid_sellers = spark.table("iceberg.silver.sellers").select(F.col("seller_id").alias("_valid_seller_id"))

    # 1. orders (quarantine NULL PKs before latest() so multiple NULL rows are never collapsed)
    orders_bronze = read_bronze(spark, "olist_orders")
    order_payload = [
        "order_id", "customer_id", "order_status", "order_purchase_timestamp",
        "order_approved_at", "order_delivered_carrier_date",
        "order_delivered_customer_date", "order_estimated_delivery_date",
    ]
    orders_null_pk = build_rejects(
        orders_bronze.filter(F.col("order_id").isNull()),
        "olist_orders", "silver.orders", F.lit("<null>"),
        "NULL_PK", "order_id is null", batch_id, order_payload,
    )
    orders = latest(orders_bronze.filter(F.col("order_id").isNotNull()), ["order_id"])
    for column in (
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ):
        orders = orders.withColumn(column, F.to_timestamp(column))

    orders_null_ts = build_rejects(
        orders.filter(F.col("order_purchase_timestamp").isNull()),
        "olist_orders", "silver.orders", F.col("order_id"),
        "NULL_PURCHASE_TS", "order_purchase_timestamp is null or unparseable", batch_id, order_payload,
    )
    orders = orders.filter(F.col("order_purchase_timestamp").isNotNull())

    orders = orders.join(cust_map, "customer_id", "left")
    orders_orphan_cust = build_rejects(
        orders.filter(F.col("customer_unique_id").isNull()),
        "olist_orders", "silver.orders", F.col("order_id"),
        "ORPHAN_CUSTOMER", "customer_id has no matching customer in silver.customers", batch_id, order_payload,
    )
    orders = orders.filter(F.col("customer_unique_id").isNotNull())

    orders, orders_timeline_bad = _split_timeline_violations(orders, batch_id)
    write_quarantine(
        spark,
        [orders_null_pk, orders_null_ts, orders_orphan_cust, orders_timeline_bad],
        "silver.orders",
    )

    silver_orders = orders.select(
        "order_id",
        "customer_id",
        "customer_unique_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
        days_between("order_delivered_customer_date", "order_purchase_timestamp").alias("delivery_days"),
        days_between("order_estimated_delivery_date", "order_purchase_timestamp").alias("estimated_delivery_days"),
        days_between("order_delivered_customer_date", "order_estimated_delivery_date").alias("delivery_delay_days"),
        F.coalesce(F.col("order_status") == F.lit("delivered"), F.lit(False)).cast("int").alias("is_delivered"),
        F.coalesce(F.col("order_status") == F.lit("canceled"), F.lit(False)).cast("int").alias("is_canceled"),
    )
    write_table(silver_orders, "iceberg.silver.orders")
    if spark.table("iceberg.silver.orders").isEmpty():
        raise RuntimeError("silver.orders is empty; refusing to build downstream transactional entities")

    valid_orders = spark.table("iceberg.silver.orders").select(F.col("order_id").alias("_valid_order_id"))

    # 2. order_items
    items_bronze = read_bronze(spark, "olist_order_items")
    item_payload = [
        "order_id", "order_item_id", "product_id", "seller_id",
        "shipping_limit_date", "price", "freight_value",
    ]
    items_null_pk = build_rejects(
        items_bronze.filter(F.col("order_id").isNull() | F.col("order_item_id").isNull()),
        "olist_order_items", "silver.order_items",
        F.concat_ws("|", F.coalesce("order_id", F.lit("<null>")), F.coalesce("order_item_id", F.lit("<null>"))),
        "NULL_PK", "order_id or order_item_id is null", batch_id, item_payload,
    )
    items = (
        latest(items_bronze.filter(F.col("order_id").isNotNull() & F.col("order_item_id").isNotNull()), ["order_id", "order_item_id"])
        .withColumn("order_item_id", F.col("order_item_id").cast("int"))
        .withColumn("price", F.col("price").cast("double"))
        .withColumn("freight_value", F.col("freight_value").cast("double"))
        .join(valid_orders, F.col("order_id") == F.col("_valid_order_id"), "left")
        .join(valid_products, F.col("product_id") == F.col("_valid_product_id"), "left")
        .join(valid_sellers, F.col("seller_id") == F.col("_valid_seller_id"), "left")
    )
    item_rule = F.concat_ws(
        ",",
        F.when(F.col("_valid_order_id").isNull(), F.lit("ORPHAN_ORDER")),
        F.when(F.col("_valid_product_id").isNull(), F.lit("ORPHAN_PRODUCT")),
        F.when(F.col("_valid_seller_id").isNull(), F.lit("ORPHAN_SELLER")),
    )
    items_annotated = items.withColumn("_rule_code", item_rule)
    items_orphan = build_rejects(
        items_annotated.filter(F.col("_rule_code") != ""),
        "olist_order_items", "silver.order_items",
        F.concat_ws("|", F.col("order_id"), F.col("order_item_id").cast("string")),
        F.col("_rule_code"),
        F.concat(F.lit("Missing parent entity: "), F.col("_rule_code")),
        batch_id, item_payload,
    )
    write_quarantine(spark, [items_null_pk, items_orphan], "silver.order_items")

    silver_items = items_annotated.filter(F.col("_rule_code") == "").select(
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        F.to_timestamp("shipping_limit_date").alias("shipping_limit_date"),
        "price",
        "freight_value",
        (F.col("price") + F.col("freight_value")).cast("double").alias("item_total"),
    )
    write_table(silver_items, "iceberg.silver.order_items")

    # 3. order_payments
    pay_bronze = read_bronze(spark, "olist_order_payments")
    pay_payload = ["order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value"]
    pay_null_pk = build_rejects(
        pay_bronze.filter(F.col("order_id").isNull() | F.col("payment_sequential").isNull()),
        "olist_order_payments", "silver.order_payments",
        F.concat_ws("|", F.coalesce("order_id", F.lit("<null>")), F.coalesce("payment_sequential", F.lit("<null>"))),
        "NULL_PK", "order_id or payment_sequential is null", batch_id, pay_payload,
    )
    payments = latest(
        pay_bronze.filter(F.col("order_id").isNotNull() & F.col("payment_sequential").isNotNull()),
        ["order_id", "payment_sequential"],
    ).join(valid_orders, F.col("order_id") == F.col("_valid_order_id"), "left")
    pay_orphan = build_rejects(
        payments.filter(F.col("_valid_order_id").isNull()),
        "olist_order_payments", "silver.order_payments",
        F.concat_ws("|", F.col("order_id"), F.col("payment_sequential")),
        "ORPHAN_ORDER", "order_id not found in silver.orders", batch_id, pay_payload,
    )
    write_quarantine(spark, [pay_null_pk, pay_orphan], "silver.order_payments")

    silver_payments = payments.filter(F.col("_valid_order_id").isNotNull()).select(
        "order_id",
        F.col("payment_sequential").cast("int").alias("payment_sequential"),
        "payment_type",
        F.col("payment_installments").cast("int").alias("payment_installments"),
        F.col("payment_value").cast("double").alias("payment_value"),
    )
    write_table(silver_payments, "iceberg.silver.order_payments")

    # 4. order_reviews (dedup on review_id to stay aligned with GOLD_DESIGN.md)
    rev_bronze = read_bronze(spark, "olist_order_reviews")
    rev_payload = [
        "review_id", "order_id", "review_score", "review_comment_title",
        "review_comment_message", "review_creation_date", "review_answer_timestamp",
    ]
    rev_null_pk = build_rejects(
        rev_bronze.filter(F.col("review_id").isNull()),
        "olist_order_reviews", "silver.order_reviews",
        F.coalesce("order_id", F.lit("<null>")),
        "NULL_PK", "review_id is null", batch_id, rev_payload,
    )
    reviews = latest(rev_bronze.filter(F.col("review_id").isNotNull()), ["review_id"]).join(
        valid_orders, F.col("order_id") == F.col("_valid_order_id"), "left"
    )
    rev_orphan = build_rejects(
        reviews.filter(F.col("_valid_order_id").isNull()),
        "olist_order_reviews", "silver.order_reviews",
        F.col("review_id"),
        "ORPHAN_ORDER", "order_id not found in silver.orders", batch_id, rev_payload,
    )
    write_quarantine(spark, [rev_null_pk, rev_orphan], "silver.order_reviews")

    score = F.col("review_score").cast("int")
    silver_reviews = reviews.filter(F.col("_valid_order_id").isNotNull()).select(
        "review_id",
        "order_id",
        score.alias("review_score"),
        "review_comment_title",
        "review_comment_message",
        F.to_timestamp("review_creation_date").alias("review_creation_date"),
        F.to_timestamp("review_answer_timestamp").alias("review_answer_timestamp"),
        (score >= F.lit(4)).cast("int").alias("is_positive"),
        (score <= F.lit(2)).cast("int").alias("is_negative"),
    )
    write_table(silver_reviews, "iceberg.silver.order_reviews")
