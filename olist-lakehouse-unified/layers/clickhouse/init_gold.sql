-- ClickHouse serving layer for optimized Olist Gold marts.
-- Source: Iceberg Gold tables exported by build_gold_marts.py to MinIO Parquet.
-- Naming convention: olist_gold.<snake_case_table>, matching iceberg.gold.<table>.
-- ClickHouse types are mapped from the supplied Iceberg Gold DDL:
--   STRING -> String, DATE -> Date, TIMESTAMP -> DateTime64(6),
--   INT -> Int32, BIGINT -> Int64, DOUBLE -> Float64.
-- Grain and ORDER BY keys follow each Gold mart's declared grain.

CREATE DATABASE IF NOT EXISTS olist_gold;

-- ============================================================
-- BASE / DETAIL
-- ============================================================

-- Grain: one row per order item.
CREATE TABLE IF NOT EXISTS olist_gold.sales_detail
(
    item_key Int64,
    order_key Int64,
    order_id String,
    order_item_id Int32,
    product_key Int64,
    seller_key Int64,
    date_key Int32,
    shipping_limit_date DateTime64(6),
    price Float64,
    freight_value Float64,
    item_total Float64,
    customer_key Int64,
    purchase_date_key Int32,
    order_status String,
    order_purchase_timestamp DateTime64(6),
    order_approved_at Nullable(DateTime64(6)),
    order_delivered_carrier_date Nullable(DateTime64(6)),
    order_delivered_customer_date Nullable(DateTime64(6)),
    order_estimated_delivery_date Nullable(DateTime64(6)),
    delivery_days Nullable(Int32),
    estimated_delivery_days Nullable(Int32),
    delivery_delay_days Nullable(Int32),
    is_delivered Int32,
    is_canceled Int32,
    purchase_date Date,
    month Date,
    purchase_hour Int32,
    weekday_num Int32,
    seller_id String,
    seller_city String,
    seller_state String,
    product_id String,
    category_key Int64,
    product_category_name String,
    product_category_name_english String,
    customer_unique_id String,
    customer_city String,
    customer_state String,
    latitude Nullable(Float64),
    longitude Nullable(Float64),
    revenue Float64,
    review_score Nullable(Int32),
    is_positive Nullable(Int32),
    is_negative Nullable(Int32),
    review_comment_title Nullable(String),
    review_comment_message Nullable(String),
    review_creation_date Nullable(DateTime64(6)),
    review_answer_timestamp Nullable(DateTime64(6))
)
ENGINE = MergeTree
ORDER BY (seller_id, purchase_date, order_id, order_item_id);

-- Grain: one row per order.
CREATE TABLE IF NOT EXISTS olist_gold.orders
(
    order_key Int64,
    order_id String,
    customer_key Int64,
    purchase_date_key Int32,
    order_status String,
    order_purchase_timestamp DateTime64(6),
    order_approved_at Nullable(DateTime64(6)),
    order_delivered_carrier_date Nullable(DateTime64(6)),
    order_delivered_customer_date Nullable(DateTime64(6)),
    order_estimated_delivery_date Nullable(DateTime64(6)),
    delivery_days Nullable(Int32),
    estimated_delivery_days Nullable(Int32),
    delivery_delay_days Nullable(Int32),
    is_delivered Int32,
    is_canceled Int32,
    purchase_date Date,
    month Date,
    purchase_hour Int32,
    weekday_num Int32,
    payment_value Float64,
    payment_transaction_count Int64,
    payment_type_count Int64,
    review_score Nullable(Int32),
    is_positive Nullable(Int32),
    is_negative Nullable(Int32),
    review_creation_date Nullable(DateTime64(6))
)
ENGINE = MergeTree
ORDER BY (order_id);

-- Grain: one row per order.
CREATE TABLE IF NOT EXISTS olist_gold.payments_by_order
(
    order_id String,
    payment_value Float64,
    payment_transaction_count Int64,
    payment_type_count Int64
)
ENGINE = MergeTree
ORDER BY (order_id);

-- Grain: latest review per order.
CREATE TABLE IF NOT EXISTS olist_gold.reviews
(
    review_key Int64,
    order_key Int64,
    order_id String,
    review_id String,
    review_score Int32,
    review_comment_title Nullable(String),
    review_comment_message Nullable(String),
    review_creation_date DateTime64(6),
    review_answer_timestamp Nullable(DateTime64(6)),
    is_positive Int32,
    is_negative Int32
)
ENGINE = MergeTree
ORDER BY (order_id, review_creation_date);

-- Grain: one row per seller + order.
CREATE TABLE IF NOT EXISTS olist_gold.seller_order
(
    seller_id String,
    order_id String,
    order_key Int64,
    customer_key Int64,
    customer_unique_id String,
    customer_state String,
    order_purchase_timestamp DateTime64(6),
    purchase_date Date,
    month Date,
    purchase_hour Int32,
    weekday_num Int32,
    order_status String,
    is_delivered Int64,
    is_canceled Int64,
    delivery_days Nullable(Int32),
    estimated_delivery_days Nullable(Int32),
    delivery_delay_days Nullable(Int32),
    seller_state String,
    revenue Float64,
    item_count Int64
)
ENGINE = MergeTree
ORDER BY (seller_id, purchase_date, order_id);

-- Grain: one row per seller + customer + order.
CREATE TABLE IF NOT EXISTS olist_gold.customer_order_bridge
(
    seller_id String,
    customer_unique_id String,
    order_id String,
    purchase_date Date,
    month Date,
    revenue Float64,
    item_count Int64
)
ENGINE = MergeTree
ORDER BY (seller_id, customer_unique_id, purchase_date, order_id);

-- ============================================================
-- SALES / CATEGORY / PRODUCT / GEOGRAPHY
-- ============================================================

-- Grain: seller + purchase date.
CREATE TABLE IF NOT EXISTS olist_gold.sales_performance_daily
(
    seller_id String,
    purchase_date Date,
    order_count Int64,
    item_count Int64,
    total_revenue Float64,
    total_freight Float64,
    avg_freight Float64
)
ENGINE = MergeTree
ORDER BY (seller_id, purchase_date);

-- Grain: seller + month.
CREATE TABLE IF NOT EXISTS olist_gold.sales_performance_monthly
(
    seller_id String,
    month Date,
    order_count Int64,
    item_count Int64,
    total_revenue Float64,
    total_freight Float64,
    avg_freight Float64,
    avg_item_price Float64
)
ENGINE = MergeTree
ORDER BY (seller_id, month);

-- Grain: seller + date + category.
CREATE TABLE IF NOT EXISTS olist_gold.category_performance_daily
(
    seller_id String,
    purchase_date Date,
    product_category_name_english String,
    order_count Int64,
    item_count Int64,
    total_revenue Float64
)
ENGINE = MergeTree
ORDER BY (seller_id, purchase_date, product_category_name_english);

-- Grain: seller + month + category.
CREATE TABLE IF NOT EXISTS olist_gold.category_performance_monthly
(
    seller_id String,
    month Date,
    product_category_name_english String,
    order_count Int64,
    item_count Int64,
    total_revenue Float64
)
ENGINE = MergeTree
ORDER BY (seller_id, month, product_category_name_english);

-- Grain: seller + month + product.
CREATE TABLE IF NOT EXISTS olist_gold.product_performance_monthly
(
    seller_id String,
    month Date,
    product_key Int64,
    product_id String,
    product_category_name_english String,
    order_count Int64,
    item_count Int64,
    total_revenue Float64,
    avg_item_price Float64
)
ENGINE = MergeTree
ORDER BY (seller_id, month, product_id);

-- Grain: seller + date + customer state.
CREATE TABLE IF NOT EXISTS olist_gold.geography_performance_daily
(
    seller_id String,
    purchase_date Date,
    customer_state String,
    order_count Int64,
    customer_count Int64,
    total_revenue Float64
)
ENGINE = MergeTree
ORDER BY (seller_id, purchase_date, customer_state);

-- Grain: seller + month + customer state.
CREATE TABLE IF NOT EXISTS olist_gold.geography_performance_monthly
(
    seller_id String,
    month Date,
    customer_state String,
    order_count Int64,
    customer_count Int64,
    total_revenue Float64
)
ENGINE = MergeTree
ORDER BY (seller_id, month, customer_state);

-- ============================================================
-- ORDER / OPERATIONS
-- ============================================================

-- Grain: seller + date + order status.
CREATE TABLE IF NOT EXISTS olist_gold.order_status_daily
(
    seller_id String,
    purchase_date Date,
    order_status String,
    order_count Int64
)
ENGINE = MergeTree
ORDER BY (seller_id, purchase_date, order_status);

-- Grain: seller + month + order status.
CREATE TABLE IF NOT EXISTS olist_gold.order_status_monthly
(
    seller_id String,
    month Date,
    order_status String,
    order_count Int64
)
ENGINE = MergeTree
ORDER BY (seller_id, month, order_status);

-- Grain: seller + date.
CREATE TABLE IF NOT EXISTS olist_gold.delivery_performance_daily
(
    seller_id String,
    purchase_date Date,
    order_count Int64,
    delivered_orders Int64,
    canceled_orders Int64,
    late_orders Int64,
    delivery_days_sum Float64,
    delivery_days_count Int64,
    delay_days_sum Float64,
    delay_days_count Int64,
    late_rate Float64
)
ENGINE = MergeTree
ORDER BY (seller_id, purchase_date);

-- Grain: seller + month.
CREATE TABLE IF NOT EXISTS olist_gold.delivery_performance_monthly
(
    seller_id String,
    month Date,
    order_count Int64,
    delivered_orders Int64,
    canceled_orders Int64,
    late_orders Int64,
    delivery_days_sum Float64,
    delivery_days_count Int64,
    delay_days_sum Float64,
    delay_days_count Int64,
    late_rate Float64
)
ENGINE = MergeTree
ORDER BY (seller_id, month);

-- Grain: seller + date + purchase hour.
CREATE TABLE IF NOT EXISTS olist_gold.order_hourly_daily
(
    seller_id String,
    purchase_date Date,
    purchase_hour Int32,
    order_count Int64
)
ENGINE = MergeTree
ORDER BY (seller_id, purchase_date, purchase_hour);

-- Grain: seller + month + purchase hour.
CREATE TABLE IF NOT EXISTS olist_gold.order_hourly_monthly
(
    seller_id String,
    month Date,
    purchase_hour Int32,
    order_count Int64
)
ENGINE = MergeTree
ORDER BY (seller_id, month, purchase_hour);

-- Grain: seller + month + weekday (Monday=0 ... Sunday=6).
CREATE TABLE IF NOT EXISTS olist_gold.order_weekday_monthly
(
    seller_id String,
    month Date,
    weekday_num Int32,
    order_count Int64
)
ENGINE = MergeTree
ORDER BY (seller_id, month, weekday_num);

-- ============================================================
-- REVIEWS
-- ============================================================

CREATE TABLE IF NOT EXISTS olist_gold.review_score_daily
(
    seller_id String,
    purchase_date Date,
    review_score Int32,
    reviewed_orders Int64
)
ENGINE = MergeTree
ORDER BY (seller_id, purchase_date, review_score);

CREATE TABLE IF NOT EXISTS olist_gold.review_score_monthly
(
    seller_id String,
    month Date,
    review_score Int32,
    reviewed_orders Int64
)
ENGINE = MergeTree
ORDER BY (seller_id, month, review_score);

CREATE TABLE IF NOT EXISTS olist_gold.review_geography_monthly
(
    seller_id String,
    month Date,
    customer_state String,
    review_score Int32,
    reviewed_orders Int64
)
ENGINE = MergeTree
ORDER BY (seller_id, month, customer_state, review_score);

-- Category counts represent order-category membership; not additive across categories.
CREATE TABLE IF NOT EXISTS olist_gold.review_category_monthly
(
    seller_id String,
    month Date,
    product_category_name_english String,
    review_score Int32,
    reviewed_orders Int64
)
ENGINE = MergeTree
ORDER BY (seller_id, month, product_category_name_english, review_score);

-- ============================================================
-- CUSTOMER
-- ============================================================

-- Grain: seller + customer across all history.
CREATE TABLE IF NOT EXISTS olist_gold.customer_lifetime
(
    seller_id String,
    customer_unique_id String,
    order_count Int64,
    item_count Int64,
    total_revenue Float64,
    first_purchase_date Date,
    last_purchase_date Date
)
ENGINE = MergeTree
ORDER BY (seller_id, customer_unique_id);

-- Grain: seller + month + customer.
CREATE TABLE IF NOT EXISTS olist_gold.customer_activity_monthly
(
    seller_id String,
    month Date,
    customer_unique_id String,
    order_count Int64,
    total_revenue Float64,
    customer_state String
)
ENGINE = MergeTree
ORDER BY (seller_id, month, customer_unique_id);

-- Grain: seller + date + customer.
CREATE TABLE IF NOT EXISTS olist_gold.customer_activity_daily
(
    seller_id String,
    purchase_date Date,
    customer_unique_id String,
    order_count Int64,
    total_revenue Float64
)
ENGINE = MergeTree
ORDER BY (seller_id, purchase_date, customer_unique_id);
