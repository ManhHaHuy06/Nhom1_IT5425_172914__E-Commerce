-- Serving tables for Gold examples. Spark exports Parquet to MinIO, then load_gold.sql loads them.
CREATE DATABASE IF NOT EXISTS olist_gold;

CREATE TABLE IF NOT EXISTS olist_gold.sales_performance_daily (
    sale_date Date,
    product_category_name_english String,
    customer_state String,
    order_count Int64,
    item_count Int64,
    total_revenue Float64,
    total_freight Float64,
    avg_freight Float64
) ENGINE = MergeTree
ORDER BY (sale_date, customer_state, product_category_name_english);

CREATE TABLE IF NOT EXISTS olist_gold.delivery_delay_analysis (
    customer_state String,
    seller_id String,
    delivered_orders Int64,
    late_orders Int64,
    late_rate Float64,
    avg_delay_days Float64,
    avg_delivery_days Float64
) ENGINE = MergeTree
ORDER BY (customer_state, seller_id);

CREATE TABLE IF NOT EXISTS olist_gold.seller_scorecard (
    seller_id String,
    seller_state String,
    order_count Int64,
    item_count Int64,
    total_revenue Float64,
    canceled_orders Int64,
    cancel_rate Float64,
    avg_review_score Float64,
    avg_carrier_handoff_days Float64
) ENGINE = MergeTree
ORDER BY seller_id;
