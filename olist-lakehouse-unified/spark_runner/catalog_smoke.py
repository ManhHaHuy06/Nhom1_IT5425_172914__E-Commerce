"""Read-only smoke test for the existing Lakehouse REST Catalog; creates nothing."""
from pyspark.sql import SparkSession

EXPECTED = {
    "bronze": {"olist_orders", "olist_order_items", "olist_order_payments", "olist_order_reviews", "olist_customers", "olist_products", "olist_sellers", "olist_geolocation", "product_category_name_translation"},
    "silver": {"dim_geolocation", "dim_category", "dim_customer", "dim_seller", "dim_product", "dim_date", "fact_orders", "fact_order_items", "fact_payments", "fact_reviews", "fact_customer_order"},
    "gold": {"sales_detail", "orders", "payments_by_order", "reviews", "seller_order", "customer_order_bridge", "sales_performance_daily", "sales_performance_monthly", "category_performance_daily", "category_performance_monthly", "product_performance_monthly", "geography_performance_daily", "geography_performance_monthly", "order_status_daily", "order_status_monthly", "delivery_performance_daily", "delivery_performance_monthly", "order_hourly_daily", "order_hourly_monthly", "order_weekday_monthly", "review_score_daily", "review_score_monthly", "review_geography_monthly", "review_category_monthly", "customer_lifetime", "customer_activity_monthly", "customer_activity_daily"},
}

spark = SparkSession.builder.appName("olist-integration-catalog-smoke").getOrCreate()
try:
    namespaces = {r.namespace for r in spark.sql("SHOW NAMESPACES IN iceberg").collect()}
    missing_ns = set(EXPECTED) - namespaces
    if missing_ns:
        raise RuntimeError(f"Existing Lakehouse namespaces missing: {sorted(missing_ns)}")
    for namespace, expected in EXPECTED.items():
        rows = spark.sql(f"SHOW TABLES IN iceberg.{namespace}").collect()
        actual = {r.tableName for r in rows}
        missing = expected - actual
        if missing:
            raise RuntimeError(f"Existing Lakehouse tables missing in {namespace}: {sorted(missing)}")
        print(f"[CATALOG OK] iceberg.{namespace}: {len(actual)} tables; required={len(expected)}")
    print("[CATALOG OK] Read-only check passed. No DDL or data writes were executed.")
finally:
    spark.stop()
