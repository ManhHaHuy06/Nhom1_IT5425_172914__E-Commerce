-- GOLD LAYER DDL - optimized Olist BI marts from Iceberg Silver.
-- Naming convention follows the existing project:
--   iceberg.gold.<snake_case_table>
-- Iceberg types: STRING, TIMESTAMP, DATE, INT, BIGINT, DOUBLE.
-- No PK/FK enforcement in Iceberg. Grain is documented per table.
-- build_gold_marts.py exports each table to:
--   s3a://lakehouse/exports/gold/<table_name>

CREATE NAMESPACE IF NOT EXISTS iceberg.gold;

-- ============================================================
-- BASE / DETAIL
-- ============================================================

-- Grain: one row per order item.
CREATE TABLE IF NOT EXISTS iceberg.gold.sales_detail (
    item_key BIGINT, order_key BIGINT, order_id STRING, order_item_id INT,
    product_key BIGINT, seller_key BIGINT, date_key INT, shipping_limit_date TIMESTAMP,
    price DOUBLE, freight_value DOUBLE, item_total DOUBLE,
    customer_key BIGINT, purchase_date_key INT, order_status STRING,
    order_purchase_timestamp TIMESTAMP, order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP, order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    delivery_days INT, estimated_delivery_days INT, delivery_delay_days INT,
    is_delivered INT, is_canceled INT,
    purchase_date DATE, month DATE, purchase_hour INT, weekday_num INT,
    seller_id STRING, seller_city STRING, seller_state STRING,
    product_id STRING, category_key BIGINT, product_category_name STRING,
    product_category_name_english STRING,
    customer_unique_id STRING, customer_city STRING, customer_state STRING,
    latitude DOUBLE, longitude DOUBLE, revenue DOUBLE,
    review_score INT, is_positive INT, is_negative INT,
    review_comment_title STRING, review_comment_message STRING,
    review_creation_date TIMESTAMP, review_answer_timestamp TIMESTAMP
) USING iceberg TBLPROPERTIES ('write.format.default' = 'parquet');

-- Grain: one row per order.
CREATE TABLE IF NOT EXISTS iceberg.gold.orders (
    order_key BIGINT, order_id STRING, customer_key BIGINT, purchase_date_key INT,
    order_status STRING, order_purchase_timestamp TIMESTAMP, order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP, order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    delivery_days INT, estimated_delivery_days INT, delivery_delay_days INT,
    is_delivered INT, is_canceled INT,
    purchase_date DATE, month DATE, purchase_hour INT, weekday_num INT,
    payment_value DOUBLE, payment_transaction_count BIGINT, payment_type_count BIGINT,
    review_score INT, is_positive INT, is_negative INT, review_creation_date TIMESTAMP
) USING iceberg TBLPROPERTIES ('write.format.default' = 'parquet');

-- Grain: one row per order.
CREATE TABLE IF NOT EXISTS iceberg.gold.payments_by_order (
    order_id STRING, payment_value DOUBLE,
    payment_transaction_count BIGINT, payment_type_count BIGINT
) USING iceberg TBLPROPERTIES ('write.format.default' = 'parquet');

-- Grain: latest review per order.
CREATE TABLE IF NOT EXISTS iceberg.gold.reviews (
    review_key BIGINT, order_key BIGINT, order_id STRING, review_id STRING,
    review_score INT, review_comment_title STRING, review_comment_message STRING,
    review_creation_date TIMESTAMP, review_answer_timestamp TIMESTAMP,
    is_positive INT, is_negative INT
) USING iceberg TBLPROPERTIES ('write.format.default' = 'parquet');

-- Grain: one row per seller + order.
CREATE TABLE IF NOT EXISTS iceberg.gold.seller_order (
    seller_id STRING, order_id STRING, order_key BIGINT, customer_key BIGINT,
    customer_unique_id STRING, customer_state STRING,
    order_purchase_timestamp TIMESTAMP, purchase_date DATE, month DATE,
    purchase_hour INT, weekday_num INT, order_status STRING,
    is_delivered BIGINT, is_canceled BIGINT,
    delivery_days INT, estimated_delivery_days INT, delivery_delay_days INT,
    seller_state STRING, revenue DOUBLE, item_count BIGINT
) USING iceberg TBLPROPERTIES ('write.format.default' = 'parquet');

-- Grain: one row per seller + customer + order.
CREATE TABLE IF NOT EXISTS iceberg.gold.customer_order_bridge (
    seller_id STRING, customer_unique_id STRING, order_id STRING,
    purchase_date DATE, month DATE, revenue DOUBLE, item_count BIGINT
) USING iceberg TBLPROPERTIES ('write.format.default' = 'parquet');

-- ============================================================
-- SALES / CATEGORY / PRODUCT / GEOGRAPHY
-- ============================================================

-- Grain: seller + purchase date.
CREATE TABLE IF NOT EXISTS iceberg.gold.sales_performance_daily (
    seller_id STRING, purchase_date DATE, order_count BIGINT, item_count BIGINT,
    total_revenue DOUBLE, total_freight DOUBLE, avg_freight DOUBLE
) USING iceberg TBLPROPERTIES ('write.format.default' = 'parquet');

-- Grain: seller + month.
CREATE TABLE IF NOT EXISTS iceberg.gold.sales_performance_monthly (
    seller_id STRING, month DATE, order_count BIGINT, item_count BIGINT,
    total_revenue DOUBLE, total_freight DOUBLE, avg_freight DOUBLE, avg_item_price DOUBLE
) USING iceberg TBLPROPERTIES ('write.format.default' = 'parquet');

-- Grain: seller + date + category.
CREATE TABLE IF NOT EXISTS iceberg.gold.category_performance_daily (
    seller_id STRING, purchase_date DATE, product_category_name_english STRING,
    order_count BIGINT, item_count BIGINT, total_revenue DOUBLE
) USING iceberg TBLPROPERTIES ('write.format.default' = 'parquet');

-- Grain: seller + month + category.
CREATE TABLE IF NOT EXISTS iceberg.gold.category_performance_monthly (
    seller_id STRING, month DATE, product_category_name_english STRING,
    order_count BIGINT, item_count BIGINT, total_revenue DOUBLE
) USING iceberg TBLPROPERTIES ('write.format.default' = 'parquet');

-- Grain: seller + month + product; intentionally no daily product expansion.
CREATE TABLE IF NOT EXISTS iceberg.gold.product_performance_monthly (
    seller_id STRING, month DATE, product_key BIGINT, product_id STRING,
    product_category_name_english STRING,
    order_count BIGINT, item_count BIGINT, total_revenue DOUBLE, avg_item_price DOUBLE
) USING iceberg TBLPROPERTIES ('write.format.default' = 'parquet');

-- Grain: seller + date + customer state.
CREATE TABLE IF NOT EXISTS iceberg.gold.geography_performance_daily (
    seller_id STRING, purchase_date DATE, customer_state STRING,
    order_count BIGINT, customer_count BIGINT, total_revenue DOUBLE
) USING iceberg TBLPROPERTIES ('write.format.default' = 'parquet');

-- Grain: seller + month + customer state.
CREATE TABLE IF NOT EXISTS iceberg.gold.geography_performance_monthly (
    seller_id STRING, month DATE, customer_state STRING,
    order_count BIGINT, customer_count BIGINT, total_revenue DOUBLE
) USING iceberg TBLPROPERTIES ('write.format.default' = 'parquet');

-- ============================================================
-- ORDER / OPERATIONS
-- ============================================================

-- Grain: seller + date + order status.
CREATE TABLE IF NOT EXISTS iceberg.gold.order_status_daily (
    seller_id STRING, purchase_date DATE, order_status STRING, order_count BIGINT
) USING iceberg TBLPROPERTIES ('write.format.default' = 'parquet');

-- Grain: seller + month + order status.
CREATE TABLE IF NOT EXISTS iceberg.gold.order_status_monthly (
    seller_id STRING, month DATE, order_status STRING, order_count BIGINT
) USING iceberg TBLPROPERTIES ('write.format.default' = 'parquet');

-- Grain: seller + date. Store sums/counts to calculate weighted averages later.
CREATE TABLE IF NOT EXISTS iceberg.gold.delivery_performance_daily (
    seller_id STRING, purchase_date DATE, order_count BIGINT,
    delivered_orders BIGINT, canceled_orders BIGINT, late_orders BIGINT,
    delivery_days_sum DOUBLE, delivery_days_count BIGINT,
    delay_days_sum DOUBLE, delay_days_count BIGINT, late_rate DOUBLE
) USING iceberg TBLPROPERTIES ('write.format.default' = 'parquet');

-- Grain: seller + month.
CREATE TABLE IF NOT EXISTS iceberg.gold.delivery_performance_monthly (
    seller_id STRING, month DATE, order_count BIGINT,
    delivered_orders BIGINT, canceled_orders BIGINT, late_orders BIGINT,
    delivery_days_sum DOUBLE, delivery_days_count BIGINT,
    delay_days_sum DOUBLE, delay_days_count BIGINT, late_rate DOUBLE
) USING iceberg TBLPROPERTIES ('write.format.default' = 'parquet');

-- Grain: seller + date + purchase hour.
CREATE TABLE IF NOT EXISTS iceberg.gold.order_hourly_daily (
    seller_id STRING, purchase_date DATE, purchase_hour INT, order_count BIGINT
) USING iceberg TBLPROPERTIES ('write.format.default' = 'parquet');

-- Grain: seller + month + purchase hour.
CREATE TABLE IF NOT EXISTS iceberg.gold.order_hourly_monthly (
    seller_id STRING, month DATE, purchase_hour INT, order_count BIGINT
) USING iceberg TBLPROPERTIES ('write.format.default' = 'parquet');

-- Grain: seller + month + weekday (Monday=0 ... Sunday=6).
CREATE TABLE IF NOT EXISTS iceberg.gold.order_weekday_monthly (
    seller_id STRING, month DATE, weekday_num INT, order_count BIGINT
) USING iceberg TBLPROPERTIES ('write.format.default' = 'parquet');

-- ============================================================
-- REVIEWS
-- ============================================================

CREATE TABLE IF NOT EXISTS iceberg.gold.review_score_daily (
    seller_id STRING, purchase_date DATE, review_score INT, reviewed_orders BIGINT
) USING iceberg TBLPROPERTIES ('write.format.default' = 'parquet');

CREATE TABLE IF NOT EXISTS iceberg.gold.review_score_monthly (
    seller_id STRING, month DATE, review_score INT, reviewed_orders BIGINT
) USING iceberg TBLPROPERTIES ('write.format.default' = 'parquet');

CREATE TABLE IF NOT EXISTS iceberg.gold.review_geography_monthly (
    seller_id STRING, month DATE, customer_state STRING,
    review_score INT, reviewed_orders BIGINT
) USING iceberg TBLPROPERTIES ('write.format.default' = 'parquet');

-- Category counts represent order-category membership; not additive across categories.
CREATE TABLE IF NOT EXISTS iceberg.gold.review_category_monthly (
    seller_id STRING, month DATE, product_category_name_english STRING,
    review_score INT, reviewed_orders BIGINT
) USING iceberg TBLPROPERTIES ('write.format.default' = 'parquet');

-- ============================================================
-- CUSTOMER
-- ============================================================

-- Grain: seller + customer across all history.
CREATE TABLE IF NOT EXISTS iceberg.gold.customer_lifetime (
    seller_id STRING, customer_unique_id STRING,
    order_count BIGINT, item_count BIGINT, total_revenue DOUBLE,
    first_purchase_date DATE, last_purchase_date DATE
) USING iceberg TBLPROPERTIES ('write.format.default' = 'parquet');

-- Grain: seller + month + customer. Distinct customers are non-additive across months.
CREATE TABLE IF NOT EXISTS iceberg.gold.customer_activity_monthly (
    seller_id STRING, month DATE, customer_unique_id STRING,
    order_count BIGINT, total_revenue DOUBLE, customer_state STRING
) USING iceberg TBLPROPERTIES ('write.format.default' = 'parquet');

-- Grain: seller + date + customer.
CREATE TABLE IF NOT EXISTS iceberg.gold.customer_activity_daily (
    seller_id STRING, purchase_date DATE, customer_unique_id STRING,
    order_count BIGINT, total_revenue DOUBLE
) USING iceberg TBLPROPERTIES ('write.format.default' = 'parquet');
